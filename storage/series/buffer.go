// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package series

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/storage/block"
	m3dberrors "github.com/m3db/m3db/storage/errors"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3db/x/xio"
	"github.com/m3db/m3x/context"
	xtime "github.com/m3db/m3x/time"
)

var (
	errMoreThanOneStreamAfterMerge = errors.New("buffer has more than one stream after merge")
	errNoAvailableBuckets          = errors.New("[invariant violated] buffer has no available buckets")
	timeZero                       time.Time
)

const (
	// bucketsLen is three to contain the following buckets:
	// 1. Bucket before current window, can be drained or not-yet-drained
	// 2. Bucket currently taking writes
	// 3. Bucket for the future that can be taking writes that is head of
	// the current block if write is for the future within bounds
	bucketsLen = 3
)

type computeBucketIdxOp int

const (
	computeBucketIdx computeBucketIdxOp = iota
	computeAndResetBucketIdx
)

type databaseBuffer interface {
	Write(
		ctx context.Context,
		timestamp time.Time,
		value float64,
		unit xtime.Unit,
		annotation []byte,
	) error

	Snapshot(ctx context.Context, blockStart time.Time) (xio.SegmentReader, error)

	ReadEncoded(
		ctx context.Context,
		start, end time.Time,
	) [][]xio.BlockReader

	FetchBlocks(
		ctx context.Context,
		starts []time.Time,
	) []block.FetchBlockResult

	FetchBlocksMetadata(
		ctx context.Context,
		start, end time.Time,
		opts FetchBlocksMetadataOptions,
	) block.FetchBlockMetadataResults

	IsEmpty() bool

	Stats() bufferStats

	// MinMax returns the minimum and maximum blockstarts for the buckets
	// that are contained within the buffer. These ranges exclude buckets
	// that have already been drained (as those buckets are no longer in use.)
	MinMax() (time.Time, time.Time, error)

	Tick() bufferTickResult

	NeedsDrain() bool

	DrainAndReset() drainAndResetResult

	Bootstrap(bl block.DatabaseBlock) error

	Reset(opts Options)
}

type bufferStats struct {
	openBlocks  int
	wiredBlocks int
}

type drainAndResetResult struct {
	mergedOutOfOrderBlocks int
}

type bufferTickResult struct {
	mergedOutOfOrderBlocks int
}

type dbBuffer struct {
	opts              Options
	nowFn             clock.NowFn
	drainFn           databaseBufferDrainFn
	pastMostBucketIdx int
	buckets           [bucketsLen]dbBufferBucket
	blockSize         time.Duration
	bufferPast        time.Duration
	bufferFuture      time.Duration
}

type databaseBufferDrainFn func(b block.DatabaseBlock)

// NB(prateek): databaseBuffer.Reset(...) must be called upon the returned
// object prior to use.
func newDatabaseBuffer(drainFn databaseBufferDrainFn) databaseBuffer {
	b := &dbBuffer{
		drainFn: drainFn,
	}
	return b
}

func (b *dbBuffer) Reset(opts Options) {
	b.opts = opts
	b.nowFn = opts.ClockOptions().NowFn()
	ropts := opts.RetentionOptions()
	b.blockSize = ropts.BlockSize()
	b.bufferPast = ropts.BufferPast()
	b.bufferFuture = ropts.BufferFuture()
	// Avoid capturing any variables with callback
	b.computedForEachBucketAsc(computeAndResetBucketIdx, bucketResetStart)
}

func bucketResetStart(now time.Time, b *dbBuffer, idx int, start time.Time) int {
	b.buckets[idx].opts = b.opts
	b.buckets[idx].resetTo(start)
	return 1
}

func (b *dbBuffer) MinMax() (time.Time, time.Time, error) {
	var min, max time.Time
	for i := range b.buckets {
		if (min.IsZero() || b.buckets[i].start.Before(min)) && !b.buckets[i].drained {
			min = b.buckets[i].start
		}
		if max.IsZero() || b.buckets[i].start.After(max) && !b.buckets[i].drained {
			max = b.buckets[i].start
		}
	}

	if min.IsZero() || max.IsZero() {
		// Should never happen
		return time.Time{}, time.Time{}, errNoAvailableBuckets
	}
	return min, max, nil
}

func (b *dbBuffer) Write(
	ctx context.Context,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	now := b.nowFn()
	futureLimit := now.Add(1 * b.bufferFuture)
	pastLimit := now.Add(-1 * b.bufferPast)
	if !futureLimit.After(timestamp) {
		return m3dberrors.ErrTooFuture
	}
	if !pastLimit.Before(timestamp) {
		return m3dberrors.ErrTooPast
	}

	bucketStart := timestamp.Truncate(b.blockSize)
	idx := b.writableBucketIdx(timestamp)
	if b.buckets[idx].needsReset(bucketStart) {
		// Needs reset
		b.DrainAndReset()
	}

	return b.buckets[idx].write(timestamp, value, unit, annotation)
}

func (b *dbBuffer) writableBucketIdx(t time.Time) int {
	return int(t.Truncate(b.blockSize).UnixNano() / int64(b.blockSize) % bucketsLen)
}

func (b *dbBuffer) IsEmpty() bool {
	canReadAny := false
	for i := range b.buckets {
		canReadAny = canReadAny || b.buckets[i].canRead()
	}
	return !canReadAny
}

func (b *dbBuffer) Stats() bufferStats {
	var stats bufferStats
	writableIdx := b.writableBucketIdx(b.nowFn())
	for i := range b.buckets {
		if !b.buckets[i].canRead() {
			continue
		}
		if i == writableIdx {
			stats.openBlocks++
		}
		stats.wiredBlocks++
	}
	return stats
}

func (b *dbBuffer) NeedsDrain() bool {
	// Avoid capturing any variables with callback
	return b.computedForEachBucketAsc(computeBucketIdx, bucketNeedsDrain) > 0
}

func bucketNeedsDrain(now time.Time, b *dbBuffer, idx int, start time.Time) int {
	if b.buckets[idx].needsDrain(now, start) {
		return 1
	}
	return 0
}

func (b *dbBuffer) Tick() bufferTickResult {
	// Avoid capturing any variables with callback
	mergedOutOfOrder := b.computedForEachBucketAsc(computeAndResetBucketIdx, bucketTick)
	return bufferTickResult{
		mergedOutOfOrderBlocks: mergedOutOfOrder,
	}
}

func bucketTick(now time.Time, b *dbBuffer, idx int, start time.Time) int {
	// Perform a drain and reset if necessary
	mergedOutOfOrderBlocks := bucketDrainAndReset(now, b, idx, start)

	// Try to merge any out of order encoders to amortize the cost of a drain
	r, err := b.buckets[idx].merge()
	if err != nil {
		log := b.opts.InstrumentOptions().Logger()
		log.Errorf("buffer merge encode error: %v", err)
	}
	if r.merges > 0 {
		mergedOutOfOrderBlocks++
	}

	return mergedOutOfOrderBlocks
}

func (b *dbBuffer) DrainAndReset() drainAndResetResult {
	// Avoid capturing any variables with callback
	mergedOutOfOrder := b.computedForEachBucketAsc(computeAndResetBucketIdx, bucketDrainAndReset)
	return drainAndResetResult{
		mergedOutOfOrderBlocks: mergedOutOfOrder,
	}
}

func bucketDrainAndReset(now time.Time, b *dbBuffer, idx int, start time.Time) int {
	mergedOutOfOrderBlocks := 0

	if b.buckets[idx].needsDrain(now, start) {
		// Rotate the buffer to a block, merging if required
		result, err := b.buckets[idx].discardMerged()
		if err != nil {
			log := b.opts.InstrumentOptions().Logger()
			log.Errorf("buffer merge encode error: %v", err)
		} else {
			if result.merges > 0 {
				mergedOutOfOrderBlocks++
			}
			if !(result.block.Len() > 0) {
				log := b.opts.InstrumentOptions().Logger()
				log.Errorf("buffer drain tried to drain empty stream for bucket: %v",
					start.String())
			} else {
				// If this block was read mark it as such
				if lastRead := b.buckets[idx].lastRead(); !lastRead.IsZero() {
					result.block.SetLastReadTime(lastRead)
				}
				b.drainFn(result.block)
			}
		}

		b.buckets[idx].drained = true
	}

	if b.buckets[idx].needsReset(start) {
		// Reset bucket
		b.buckets[idx].resetTo(start)
	}

	return mergedOutOfOrderBlocks
}

func (b *dbBuffer) Bootstrap(bl block.DatabaseBlock) error {
	blockStart := bl.StartTime()
	bootstrapped := false
	for i := range b.buckets {
		if b.buckets[i].start.Equal(blockStart) {
			if b.buckets[i].drained {
				return fmt.Errorf(
					"block at %s cannot be bootstrapped by buffer because its already drained",
					blockStart.String(),
				)
			}
			b.buckets[i].bootstrap(bl)
			bootstrapped = true
			break
		}
	}
	if !bootstrapped {
		return fmt.Errorf("block at %s not contained by buffer", blockStart.String())
	}
	return nil
}

// forEachBucketAsc iterates over the buckets in time ascending order
// to read bucket data
func (b *dbBuffer) forEachBucketAsc(fn func(*dbBufferBucket)) {
	for i := 0; i < bucketsLen; i++ {
		idx := (b.pastMostBucketIdx + i) % bucketsLen
		fn(&b.buckets[idx])
	}
}

// computedForEachBucketAsc performs a fn on the buckets in time ascending order
// and returns the sum of the number returned by each fn
func (b *dbBuffer) computedForEachBucketAsc(
	op computeBucketIdxOp,
	fn func(now time.Time, b *dbBuffer, idx int, bucketStart time.Time) int,
) int {
	now := b.nowFn()
	pastMostBucketStart := now.Truncate(b.blockSize).Add(-1 * b.blockSize)
	bucketNum := (pastMostBucketStart.UnixNano() / int64(b.blockSize)) % bucketsLen
	result := 0
	for i := int64(0); i < bucketsLen; i++ {
		idx := int((bucketNum + i) % bucketsLen)
		curr := pastMostBucketStart.Add(time.Duration(i) * b.blockSize)
		result += fn(now, b, idx, curr)
	}
	if op == computeAndResetBucketIdx {
		b.pastMostBucketIdx = int(bucketNum)
	}
	return result
}

func (b *dbBuffer) Snapshot(ctx context.Context, blockStart time.Time) (xio.SegmentReader, error) {
	var (
		res xio.SegmentReader
		err error
	)

	b.forEachBucketAsc(func(bucket *dbBufferBucket) {
		if err != nil {
			// Something already went wrong and we want to return the error to the caller
			// as soon as possible instead of continuing to do work.
			return
		}

		if !bucket.canRead() {
			return
		}

		if !blockStart.Equal(bucket.start) {
			return
		}

		// We need to merge all the bootstrapped blocks / encoders into a single stream for
		// the sake of being able to persist it to disk as a single encoded stream.
		_, err = bucket.merge()
		if err != nil {
			return
		}

		// This operation is safe because all of the underlying resources will respect the
		// lifecycle of the context in one way or another. The "bootstrapped blocks" that
		// we stream from will mark their internal context as dependent on that of the passed
		// context, and the Encoder's that we stream from actually perform a data copy and
		// don't share a reference.
		streams := bucket.streams(ctx)
		if len(streams) != 1 {
			// Should never happen as the call to merge above should result in only a single
			// stream being present.
			err = errMoreThanOneStreamAfterMerge
			return
		}

		// Direct indexing is safe because canRead guarantees us at least one stream
		res = streams[0]
	})

	return res, err
}

func (b *dbBuffer) ReadEncoded(ctx context.Context, start, end time.Time) [][]xio.BlockReader {
	// TODO(r): pool these results arrays
	var res [][]xio.BlockReader
	b.forEachBucketAsc(func(bucket *dbBufferBucket) {
		if !bucket.canRead() {
			return
		}
		if !start.Before(bucket.start.Add(b.blockSize)) {
			return
		}
		if !bucket.start.Before(end) {
			return
		}

		res = append(res, bucket.streams(ctx))

		// NB(r): Store the last read time, should not set this when
		// calling FetchBlocks as a read is differentiated from
		// a FetchBlocks call. One is initiated by an external
		// entity and the other is used for streaming blocks between
		// the storage nodes. This distinction is important as this
		// data is important for use with understanding access patterns, etc.
		bucket.setLastRead(b.nowFn())
	})

	return res
}

func (b *dbBuffer) FetchBlocks(ctx context.Context, starts []time.Time) []block.FetchBlockResult {
	var res []block.FetchBlockResult

	b.forEachBucketAsc(func(bucket *dbBufferBucket) {
		if !bucket.canRead() {
			return
		}
		found := false
		// starts have only a few items, linear search should be okay time-wise to
		// avoid allocating a map here.
		for _, start := range starts {
			if start.Equal(bucket.start) {
				found = true
				break
			}
		}
		if !found {
			return
		}

		streams := bucket.streams(ctx)
		res = append(res, block.NewFetchBlockResult(bucket.start, streams, nil))
	})

	return res
}

func (b *dbBuffer) FetchBlocksMetadata(
	ctx context.Context,
	start, end time.Time,
	opts FetchBlocksMetadataOptions,
) block.FetchBlockMetadataResults {
	blockSize := b.opts.RetentionOptions().BlockSize()
	res := b.opts.FetchBlockMetadataResultsPool().Get()
	b.forEachBucketAsc(func(bucket *dbBufferBucket) {
		if !bucket.canRead() {
			return
		}
		if !start.Before(bucket.start.Add(blockSize)) || !bucket.start.Before(end) {
			return
		}
		size := int64(bucket.streamsLen())
		// If we have no data in this bucket, return early without appending it to the result.
		if size == 0 {
			return
		}
		var resultSize int64
		if opts.IncludeSizes {
			resultSize = size
		}
		var resultLastRead time.Time
		if opts.IncludeLastRead {
			resultLastRead = bucket.lastRead()
		}
		// NB(r): Ignore if opts.IncludeChecksum because we avoid
		// calculating checksum since block is open and is being mutated
		res.Add(block.FetchBlockMetadataResult{
			Start:    bucket.start,
			Size:     resultSize,
			LastRead: resultLastRead,
		})
	})

	return res
}

type dbBufferBucket struct {
	opts              Options
	start             time.Time
	encoders          []inOrderEncoder
	bootstrapped      []block.DatabaseBlock
	lastReadUnixNanos int64
	empty             bool
	drained           bool
}

type inOrderEncoder struct {
	lastWriteAt time.Time
	encoder     encoding.Encoder
}

func (b *dbBufferBucket) resetTo(
	start time.Time,
) {
	// Close the old context if we're resetting for use
	b.finalize()

	bopts := b.opts.DatabaseBlockOptions()
	encoder := bopts.EncoderPool().Get()
	encoder.Reset(start, bopts.DatabaseBlockAllocSize())

	b.start = start
	b.encoders = append(b.encoders, inOrderEncoder{
		lastWriteAt: timeZero,
		encoder:     encoder,
	})
	b.bootstrapped = nil
	atomic.StoreInt64(&b.lastReadUnixNanos, 0)
	b.empty = true
	b.drained = false
}

func (b *dbBufferBucket) finalize() {
	b.resetEncoders()
	b.resetBootstrapped()
}

func (b *dbBufferBucket) canRead() bool {
	return !b.drained && !b.empty
}

func (b *dbBufferBucket) needsReset(
	targetStart time.Time,
) bool {
	return !b.start.Equal(targetStart)
}

func (b *dbBufferBucket) needsDrain(
	now time.Time,
	targetStart time.Time,
) bool {
	retentionOpts := b.opts.RetentionOptions()
	blockSize := retentionOpts.BlockSize()
	bufferPast := retentionOpts.BufferPast()

	return b.canRead() && (b.needsReset(targetStart) ||
		b.start.Add(blockSize).Before(now.Add(-bufferPast)))
}

func (b *dbBufferBucket) bootstrap(
	bl block.DatabaseBlock,
) {
	b.empty = false
	b.bootstrapped = append(b.bootstrapped, bl)
}

func (b *dbBufferBucket) write(
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	idx := -1
	for i := range b.encoders {
		if timestamp.Equal(b.encoders[i].lastWriteAt) {
			// NB(xichen): We discard datapoints with the same timestamps as the
			// ones we've already encoded. Immutable/first-write-wins semantics.
			return nil
		}
		if timestamp.After(b.encoders[i].lastWriteAt) {
			idx = i
			break
		}
	}
	if idx == -1 {
		b.opts.Stats().IncCreatedEncoders()
		bopts := b.opts.DatabaseBlockOptions()
		blockSize := b.opts.RetentionOptions().BlockSize()
		blockAllocSize := bopts.DatabaseBlockAllocSize()
		encoder := bopts.EncoderPool().Get()
		encoder.Reset(timestamp.Truncate(blockSize), blockAllocSize)
		next := inOrderEncoder{encoder: encoder}
		b.encoders = append(b.encoders, next)
		idx = len(b.encoders) - 1
	}

	if err := b.encoders[idx].encoder.Encode(ts.Datapoint{
		Timestamp: timestamp,
		Value:     value,
	}, unit, annotation); err != nil {
		return err
	}
	b.encoders[idx].lastWriteAt = timestamp
	b.empty = false
	return nil
}

func (b *dbBufferBucket) streams(ctx context.Context) []xio.BlockReader {
	streams := make([]xio.BlockReader, 0, len(b.bootstrapped)+len(b.encoders))

	for i := range b.bootstrapped {
		if b.bootstrapped[i].Len() == 0 {
			continue
		}
		if s, err := b.bootstrapped[i].Stream(ctx); err == nil && s.IsNotEmpty() {
			// NB(r): block stream method will register the stream closer already
			streams = append(streams, s)
		}
	}
	for i := range b.encoders {
		start := b.start
		if s := b.encoders[i].encoder.Stream(); s != nil {
			br := xio.BlockReader{
				SegmentReader: s,
				Start:         start,
				BlockSize:     b.opts.RetentionOptions().BlockSize(),
			}
			ctx.RegisterFinalizer(s)
			streams = append(streams, br)
		}
	}

	return streams
}

func (b *dbBufferBucket) streamsLen() int {
	length := 0
	for i := range b.bootstrapped {
		length += b.bootstrapped[i].Len()
	}
	for i := range b.encoders {
		length += b.encoders[i].encoder.Len()
	}
	return length
}

func (b *dbBufferBucket) setLastRead(value time.Time) {
	atomic.StoreInt64(&b.lastReadUnixNanos, value.UnixNano())
}

func (b *dbBufferBucket) lastRead() time.Time {
	return time.Unix(0, atomic.LoadInt64(&b.lastReadUnixNanos))
}

func (b *dbBufferBucket) resetEncoders() {
	var zeroed inOrderEncoder
	for i := range b.encoders {
		// Register when this bucket resets we close the encoder
		encoder := b.encoders[i].encoder
		encoder.Close()
		b.encoders[i] = zeroed
	}
	b.encoders = b.encoders[:0]
}

func (b *dbBufferBucket) resetBootstrapped() {
	for i := range b.bootstrapped {
		bl := b.bootstrapped[i]
		bl.Close()
	}
	b.bootstrapped = nil
}

func (b *dbBufferBucket) needsMerge() bool {
	return b.canRead() && !(b.hasJustSingleEncoder() || b.hasJustSingleBootstrappedBlock())
}

func (b *dbBufferBucket) hasJustSingleEncoder() bool {
	return len(b.encoders) == 1 && len(b.bootstrapped) == 0
}

func (b *dbBufferBucket) hasJustSingleBootstrappedBlock() bool {
	encodersEmpty := len(b.encoders) == 0 ||
		(len(b.encoders) == 1 &&
			b.encoders[0].encoder.Len() == 0)
	return encodersEmpty && len(b.bootstrapped) == 1
}

type mergeResult struct {
	merges int
}

func (b *dbBufferBucket) merge() (mergeResult, error) {
	if !b.needsMerge() {
		// Save unnecessary work
		return mergeResult{}, nil
	}

	merges := 0
	bopts := b.opts.DatabaseBlockOptions()
	encoder := bopts.EncoderPool().Get()
	encoder.Reset(b.start, bopts.DatabaseBlockAllocSize())

	// If we have to merge bootstrapped from disk during a merge then this
	// can make ticking very slow, ensure to notify this bug
	if len(b.bootstrapped) > 0 {
		unretrieved := 0
		for i := range b.bootstrapped {
			if !b.bootstrapped[i].IsRetrieved() {
				unretrieved++
			}
		}
		if unretrieved > 0 {
			log := b.opts.InstrumentOptions().Logger()
			log.Warnf("buffer merging %d unretrieved blocks", unretrieved)
		}
	}

	start := b.start
	readers := make([]xio.SegmentReader, 0, len(b.encoders)+len(b.bootstrapped))
	streams := make([]xio.SegmentReader, 0, len(b.encoders))
	for i := range b.encoders {
		if s := b.encoders[i].encoder.Stream(); s != nil {
			merges++
			readers = append(readers, s)
			streams = append(streams, s)
		}
	}

	var lastWriteAt time.Time
	iter := b.opts.MultiReaderIteratorPool().Get()
	ctx := b.opts.ContextPool().Get()
	defer func() {
		iter.Close()
		ctx.Close()
		// NB(r): Only need to close the mutable encoder streams as
		// the context we created for reading the bootstrap blocks
		// when closed will close those streams.
		for _, stream := range streams {
			stream.Finalize()
		}
	}()

	for i := range b.bootstrapped {
		block, err := b.bootstrapped[i].Stream(ctx)
		if err == nil && block.SegmentReader != nil {
			merges++
			readers = append(readers, block.SegmentReader)
		}
	}

	iter.Reset(readers, start, b.opts.RetentionOptions().BlockSize())
	for iter.Next() {
		dp, unit, annotation := iter.Current()
		if err := encoder.Encode(dp, unit, annotation); err != nil {
			return mergeResult{}, err
		}
		lastWriteAt = dp.Timestamp
	}
	if err := iter.Err(); err != nil {
		return mergeResult{}, err
	}

	b.resetEncoders()
	b.resetBootstrapped()

	b.encoders = append(b.encoders, inOrderEncoder{
		lastWriteAt: lastWriteAt,
		encoder:     encoder,
	})

	return mergeResult{merges: merges}, nil
}

type discardMergedResult struct {
	block  block.DatabaseBlock
	merges int
}

func (b *dbBufferBucket) discardMerged() (discardMergedResult, error) {
	if b.hasJustSingleEncoder() {
		// Already merged as a single encoder
		encoder := b.encoders[0].encoder
		newBlock := b.opts.DatabaseBlockOptions().DatabaseBlockPool().Get()
		blockSize := b.opts.RetentionOptions().BlockSize()
		newBlock.Reset(b.start, blockSize, encoder.Discard())

		// The single encoder is already discarded, no need to call resetEncoders
		// just remove it from the list of encoders
		b.encoders = b.encoders[:0]
		b.resetBootstrapped()
		b.empty = true

		return discardMergedResult{newBlock, 0}, nil
	}

	if b.hasJustSingleBootstrappedBlock() {
		// Already merged just a single bootstrapped block
		existingBlock := b.bootstrapped[0]

		// Need to reset encoders but do not want to finalize the block as we
		// are passing ownership of it to the caller
		b.resetEncoders()
		b.bootstrapped = nil
		b.empty = true

		return discardMergedResult{existingBlock, 0}, nil
	}

	result, err := b.merge()
	if err != nil {
		b.resetEncoders()
		b.resetBootstrapped()
		b.empty = true
		return discardMergedResult{}, err
	}

	merged := b.encoders[0].encoder

	newBlock := b.opts.DatabaseBlockOptions().DatabaseBlockPool().Get()
	blockSize := b.opts.RetentionOptions().BlockSize()
	newBlock.Reset(b.start, blockSize, merged.Discard())

	// The merged encoder is already discarded, no need to call resetEncoders
	// just remove it from the list of encoders
	b.encoders = b.encoders[:0]
	b.resetBootstrapped()
	b.empty = true

	return discardMergedResult{newBlock, result.merges}, nil
}
