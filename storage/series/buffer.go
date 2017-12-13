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
	"io"
	"sync/atomic"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/ts"
	xio "github.com/m3db/m3db/x/io"
	xerrors "github.com/m3db/m3x/errors"
	xtime "github.com/m3db/m3x/time"
)

var (
	errTooFuture = errors.New("datapoint is too far in the future")
	errTooPast   = errors.New("datapoint is too far in the past")
	timeZero     time.Time
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

	ReadEncoded(
		ctx context.Context,
		start, end time.Time,
	) [][]xio.SegmentReader

	FetchBlocks(
		ctx context.Context,
		starts []time.Time,
	) []block.FetchBlockResult

	FetchBlocksMetadata(
		ctx context.Context,
		start, end time.Time,
		opts block.FetchBlocksMetadataOptions,
	) block.FetchBlockMetadataResults

	IsEmpty() bool

	Stats() bufferStats

	MinMax() (time.Time, time.Time)

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

func (b *dbBuffer) MinMax() (time.Time, time.Time) {
	var min, max time.Time
	for i := range b.buckets {
		if min.IsZero() || b.buckets[i].start.Before(min) {
			min = b.buckets[i].start
		}
		if max.IsZero() || b.buckets[i].start.After(max) {
			max = b.buckets[i].start
		}
	}
	return min, max
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
		return xerrors.NewInvalidParamsError(errTooFuture)
	}
	if !pastLimit.Before(timestamp) {
		return xerrors.NewInvalidParamsError(errTooPast)
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
	return int((t.UnixNano() / int64(b.blockSize)) % bucketsLen)
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

	if b.buckets[idx].needsMerge() {
		// Try to merge any out of order encoders to amortize the cost of a drain
		r, err := b.buckets[idx].merge()
		if err != nil {
			log := b.opts.InstrumentOptions().Logger()
			log.Errorf("buffer merge encode error: %v", err)
		}
		if r.merges > 0 {
			mergedOutOfOrderBlocks++
		}
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

func (b *dbBuffer) ReadEncoded(ctx context.Context, start, end time.Time) [][]xio.SegmentReader {
	// TODO(r): pool these results arrays
	var res [][]xio.SegmentReader
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
		res = append(res, block.NewFetchBlockResult(bucket.start, streams, nil, nil))
	})

	return res
}

func (b *dbBuffer) FetchBlocksMetadata(
	ctx context.Context,
	start, end time.Time,
	opts block.FetchBlocksMetadataOptions,
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
	ctx               context.Context
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

	ctx := b.opts.ContextPool().Get()

	bopts := b.opts.DatabaseBlockOptions()
	encoder := bopts.EncoderPool().Get()
	encoder.Reset(start, bopts.DatabaseBlockAllocSize())

	b.ctx = ctx
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
	if b.ctx != nil {
		// Close the old context
		b.ctx.Close()
	}
	b.ctx = nil
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
	var target *inOrderEncoder
	for i := range b.encoders {
		if timestamp.Equal(b.encoders[i].lastWriteAt) {
			// NB(xichen): We discard datapoints with the same timestamps as the
			// ones we've already encoded. Immutable/first-write-wins semantics.
			return nil
		}
		if timestamp.After(b.encoders[i].lastWriteAt) {
			target = &b.encoders[i]
			break
		}
	}
	if target == nil {
		bopts := b.opts.DatabaseBlockOptions()
		blockSize := b.opts.RetentionOptions().BlockSize()
		blockAllocSize := bopts.DatabaseBlockAllocSize()
		encoder := bopts.EncoderPool().Get()
		encoder.Reset(timestamp.Truncate(blockSize), blockAllocSize)
		next := inOrderEncoder{encoder: encoder}
		b.encoders = append(b.encoders, next)
		target = &b.encoders[len(b.encoders)-1]
	}

	datapoint := ts.Datapoint{
		Timestamp: timestamp,
		Value:     value,
	}
	if err := target.encoder.Encode(datapoint, unit, annotation); err != nil {
		return err
	}
	target.lastWriteAt = timestamp
	b.empty = false
	return nil
}

func (b *dbBufferBucket) streams(ctx context.Context) []xio.SegmentReader {
	// NB(r): Ensure we don't call any closers before the operation
	// started by the passed context completes.
	b.ctx.DependsOn(ctx)

	streams := make([]xio.SegmentReader, 0, len(b.bootstrapped)+len(b.encoders))

	for i := range b.bootstrapped {
		if b.bootstrapped[i].Len() == 0 {
			continue
		}
		if s, err := b.bootstrapped[i].Stream(ctx); err == nil && s != nil {
			// NB(r): block stream method will register the stream closer already
			streams = append(streams, s)
		}
	}
	for i := range b.encoders {
		if s := b.encoders[i].encoder.Stream(); s != nil {
			ctx.RegisterFinalizer(s)
			streams = append(streams, s)
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
		length += b.encoders[i].encoder.StreamLen()
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
		if b.ctx != nil {
			b.ctx.RegisterFinalizer(context.FinalizerFn(encoder.Close))
		}
		b.encoders[i] = zeroed
	}
	b.encoders = b.encoders[:0]
}

func (b *dbBufferBucket) resetBootstrapped() {
	for i := range b.bootstrapped {
		bl := b.bootstrapped[i]
		if b.ctx != nil {
			b.ctx.RegisterFinalizer(context.FinalizerFn(bl.Close))
		}
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
			b.encoders[0].encoder.StreamLen() == 0)
	return encodersEmpty && len(b.bootstrapped) == 1
}

type mergeResult struct {
	merges int
}

func (b *dbBufferBucket) merge() (mergeResult, error) {
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

	readers := make([]io.Reader, 0, len(b.encoders)+len(b.bootstrapped))
	for i := range b.encoders {
		if stream := b.encoders[i].encoder.Stream(); stream != nil {
			merges++
			readers = append(readers, stream)
		}
	}

	for i := range b.bootstrapped {
		stream, err := b.bootstrapped[i].Stream(b.ctx)
		if err == nil && stream != nil {
			merges++
			readers = append(readers, stream)
		}
	}

	var lastWriteAt time.Time
	iter := b.opts.MultiReaderIteratorPool().Get()
	iter.Reset(readers)

	defer func() {
		iter.Close()
		for _, stream := range readers {
			if resource, ok := stream.(context.Finalizer); ok {
				resource.Finalize()
			}
		}
	}()

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
	defer func() {
		b.resetEncoders()
		b.resetBootstrapped()
		b.empty = true
	}()

	if b.hasJustSingleEncoder() {
		// Already merged as a single encoder
		encoder := b.encoders[0].encoder
		newBlock := b.opts.DatabaseBlockOptions().DatabaseBlockPool().Get()
		newBlock.Reset(b.start, encoder.Discard())
		b.encoders = b.encoders[:0]
		return discardMergedResult{newBlock, 0}, nil
	}

	if b.hasJustSingleBootstrappedBlock() {
		// Already merged just a single bootstrapped block
		existingBlock := b.bootstrapped[0]
		b.bootstrapped = b.bootstrapped[:0]
		return discardMergedResult{existingBlock, 0}, nil
	}

	result, err := b.merge()
	if err != nil {
		return discardMergedResult{}, err
	}

	merged := b.encoders[0].encoder

	newBlock := b.opts.DatabaseBlockOptions().DatabaseBlockPool().Get()
	newBlock.Reset(b.start, merged.Discard())

	return discardMergedResult{newBlock, result.merges}, nil
}
