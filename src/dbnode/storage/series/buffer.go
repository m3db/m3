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

	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"
)

var (
	errMoreThanOneStreamAfterMerge = errors.New("buffer has more than one stream after merge")
	errNoAvailableBuckets          = errors.New("[invariant violated] buffer has no available buckets")
	errAnyWriteTimeNotEnabled      = errors.New("non-realtime metrics not enabled")
	timeZero                       time.Time
)

const (
	// bucketsLen is three to contain the following buckets:
	// 1. Bucket before current window, can be drained or not-yet-drained
	// 2. Bucket currently taking writes
	// 3. Bucket for the future that can be taking writes that is head of
	// the current block if write is for the future within bounds
	bucketsLen = 3

	// TODO: make sure this is a good pool size or make it customizable
	defaultBufferBucketPoolSize = 16
)

type computeBucketIdxOp int
type bucketType int

const (
	computeBucketIdx computeBucketIdxOp = iota
	computeAndResetBucketIdx

	bucketTypeAll bucketType = iota
	bucketTypeRealTime
	bucketTypeNotRealTime
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
	opts               Options
	nowFn              clock.NowFn
	drainFn            databaseBufferDrainFn
	pastMostBucketIdx  int
	bucketsRealTime    [bucketsLen]dbBufferBucket
	bucketsNotRealTime map[xtime.UnixNano]*dbBufferBucket
	bucketPool         *dbBufferBucketPool
	blockSize          time.Duration
	bufferPast         time.Duration
	bufferFuture       time.Duration
}

type bucketID struct {
	isRealTime bool
	// idx is the index of a realtime bucket in the array
	idx int
	// key is the key of a non-realtime bucket in the map
	key xtime.UnixNano
}

func bucketIDRealtime(idx int) bucketID {
	return bucketID{
		isRealTime: true,
		idx:        idx,
	}
}

func bucketIDNotRealtime(key xtime.UnixNano) bucketID {
	return bucketID{
		isRealTime: false,
		key:        key,
	}
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

	if ropts.AnyWriteTimeEnabled() {
		bucketPoolOpts := pool.NewObjectPoolOptions().SetSize(defaultBufferBucketPoolSize)
		b.bucketPool = newDBBufferBucketPool(bucketPoolOpts)
		b.bucketsNotRealTime = make(map[xtime.UnixNano]*dbBufferBucket)
		b.removeBucketsNotRealTime()
	}

	// Avoid capturing any variables with callback
	b.computedForEachBucketAsc(computeAndResetBucketIdx, bucketTypeRealTime, bucketResetStart)
}

func (b *dbBuffer) removeBucketsNotRealTime() {
	for key := range b.bucketsNotRealTime {
		b.removeBucket(key)
	}
}

func bucketResetStart(now time.Time, b *dbBuffer, id bucketID, start time.Time) int {
	bucket := b.bucketAtIdx(id)

	bucket.opts = b.opts
	bucket.resetTo(start, true)
	return 1
}

func (b *dbBuffer) bucketAtIdx(id bucketID) *dbBufferBucket {
	if id.isRealTime {
		return &b.bucketsRealTime[id.idx]
	}

	return b.bucketsNotRealTime[id.key]
}

func (b *dbBuffer) MinMax() (time.Time, time.Time, error) {
	var min, max time.Time
	for i := range b.bucketsRealTime {
		if (min.IsZero() || b.bucketsRealTime[i].start.Before(min)) && !b.bucketsRealTime[i].drained {
			min = b.bucketsRealTime[i].start
		}
		if max.IsZero() || b.bucketsRealTime[i].start.After(max) && !b.bucketsRealTime[i].drained {
			max = b.bucketsRealTime[i].start
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
	bucketStart := timestamp.Truncate(b.blockSize)
	key := b.writableBucketKey(timestamp)

	if b.isRealTime(timestamp) {
		idx := b.writableBucketIdx(timestamp)

		if b.bucketsRealTime[idx].needsReset(now, bucketStart) {
			b.DrainAndReset()
		}
		// If there was previously a non-realtime metric written to this bucket
		// already, then use that bucket.
		if _, ok := b.bucketsNotRealTime[key]; ok {
			b.makeBucketRealTime(key, idx)
		}

		return b.bucketsRealTime[idx].write(now, timestamp, value, unit, annotation)
	}

	if !b.opts.RetentionOptions().AnyWriteTimeEnabled() {
		return errAnyWriteTimeNotEnabled
	}

	if _, ok := b.bucketsNotRealTime[key]; !ok {
		b.initializeBucket(key)
	}

	if b.bucketsNotRealTime[key].needsDrain(now, bucketStart) {
		b.bucketDrain(now, bucketIDNotRealtime(key), bucketStart)

		if b.bucketsNotRealTime[key].isStale(now) {
			b.removeBucket(key)
		}
	}

	return b.bucketsNotRealTime[key].write(now, timestamp, value, unit, annotation)
}

func (b *dbBuffer) isRealTime(timestamp time.Time) bool {
	now := b.nowFn()
	futureLimit := now.Add(1 * b.bufferFuture)
	pastLimit := now.Add(-1 * b.bufferPast)
	return pastLimit.Before(timestamp) && futureLimit.After(timestamp)
}

func (b *dbBuffer) writableBucketIdx(t time.Time) int {
	return int(t.Truncate(b.blockSize).UnixNano() / int64(b.blockSize) % bucketsLen)
}

func (b *dbBuffer) writableBucketKey(t time.Time) xtime.UnixNano {
	return xtime.ToUnixNano(t.Truncate(b.blockSize))
}

func (b *dbBuffer) initializeBucket(key xtime.UnixNano) {
	b.bucketsNotRealTime[key] = b.bucketPool.Get()
	b.bucketsNotRealTime[key].opts = b.opts
	b.bucketsNotRealTime[key].resetTo(key.ToTime(), false)
}

func (b *dbBuffer) removeBucket(idx xtime.UnixNano) {
	b.bucketPool.Put(b.bucketsNotRealTime[idx])
	delete(b.bucketsNotRealTime, idx)
}

func (b *dbBuffer) IsEmpty() bool {
	for i := range b.bucketsRealTime {
		if b.bucketsRealTime[i].canRead() {
			return false
		}
	}

	for key := range b.bucketsNotRealTime {
		if b.bucketsNotRealTime[key].canRead() {
			return false
		}
	}

	return true
}

func (b *dbBuffer) Stats() bufferStats {
	var stats bufferStats
	writableIdx := b.writableBucketIdx(b.nowFn())
	for i := range b.bucketsRealTime {
		if !b.bucketsRealTime[i].canRead() {
			continue
		}
		if i == writableIdx {
			stats.openBlocks++
		}
		stats.wiredBlocks++
	}

	for key := range b.bucketsNotRealTime {
		if !b.bucketsNotRealTime[key].canRead() {
			continue
		}
		stats.openBlocks++
		stats.wiredBlocks++
	}

	return stats
}

func (b *dbBuffer) NeedsDrain() bool {
	// Avoid capturing any variables with callback
	return b.computedForEachBucketAsc(computeBucketIdx, bucketTypeAll, bucketNeedsDrain) > 0
}

func bucketNeedsDrain(now time.Time, b *dbBuffer, id bucketID, start time.Time) int {
	bucket := b.bucketAtIdx(id)
	if bucket.needsDrain(now, start) {
		return 1
	}
	return 0
}

func (b *dbBuffer) Tick() bufferTickResult {
	// Avoid capturing any variables with callback
	mergedOutOfOrder := b.computedForEachBucketAsc(computeAndResetBucketIdx, bucketTypeAll, bucketTick)
	return bufferTickResult{
		mergedOutOfOrderBlocks: mergedOutOfOrder,
	}
}

func bucketTick(now time.Time, b *dbBuffer, id bucketID, start time.Time) int {
	// Perform a drain and reset if necessary
	mergedOutOfOrderBlocks := bucketDrainAndReset(now, b, id, start)

	bucket := b.bucketAtIdx(id)

	// Try to merge any out of order encoders to amortize the cost of a drain
	r, err := bucket.merge()
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
	mergedOutOfOrder := b.computedForEachBucketAsc(computeAndResetBucketIdx, bucketTypeRealTime, bucketDrainAndReset)
	return drainAndResetResult{
		mergedOutOfOrderBlocks: mergedOutOfOrder,
	}
}

func bucketDrainAndReset(now time.Time, b *dbBuffer, id bucketID, start time.Time) int {
	mergedOutOfOrderBlocks := 0
	bucket := b.bucketAtIdx(id)

	if bucket.needsDrain(now, start) {
		mergedOutOfOrderBlocks += b.bucketDrain(now, id, start)
	}

	if bucket.needsReset(now, start) {
		bucket.resetTo(start, true)
	}

	if bucket.isStale(now) {
		b.removeBucket(id.key)
	}

	return mergedOutOfOrderBlocks
}

func (b *dbBuffer) bucketDrain(now time.Time, id bucketID, start time.Time) int {
	mergedOutOfOrderBlocks := 0

	bucket := b.bucketAtIdx(id)
	// Rotate the buffer to a block, merging if required
	result, err := bucket.discardMerged()
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
			if lastRead := bucket.lastRead(); !lastRead.IsZero() {
				result.block.SetLastReadTime(lastRead)
			}
			if b.drainFn != nil {
				b.drainFn(result.block)
			}
		}
	}

	bucket.drained = true
	bucket.setLastWrite(now)
	bucket.resetNumWrites()
	if bucket.isStale(now) {
		b.removeBucket(id.key)
	}

	return mergedOutOfOrderBlocks
}

func (b *dbBuffer) makeBucketRealTime(fromKey xtime.UnixNano, toIdx int) {
	bucket, ok := b.bucketsNotRealTime[fromKey]
	if !ok {
		return
	}

	bucket.isRealTime = true
	b.bucketsRealTime[toIdx] = *bucket
	b.removeBucket(fromKey)
}

func (b *dbBuffer) Bootstrap(bl block.DatabaseBlock) error {
	blockStart := bl.StartTime()
	bootstrapped := false
	for i := range b.bucketsRealTime {
		if b.bucketsRealTime[i].start.Equal(blockStart) {
			if b.bucketsRealTime[i].drained {
				return fmt.Errorf(
					"block at %s cannot be bootstrapped by buffer because its already drained",
					blockStart.String(),
				)
			}
			b.bucketsRealTime[i].bootstrap(bl)
			bootstrapped = true
			break
		}
	}
	if bucket, ok := b.bucketsNotRealTime[xtime.ToUnixNano(blockStart)]; !bootstrapped && ok {
		if bucket.drained {
			return fmt.Errorf(
				"block at %s cannot be bootstrapped by buffer because its already drained",
				blockStart.String(),
			)
		}
		bucket.bootstrap(bl)
		bootstrapped = true
	}
	if !bootstrapped {
		return fmt.Errorf("block at %s not contained by buffer", blockStart.String())
	}
	return nil
}

// forEachBucketAsc iterates over the buckets in time ascending order
// to read bucket data
func (b *dbBuffer) forEachBucketAsc(
	bType bucketType,
	fn func(*dbBufferBucket),
) {
	if bType == bucketTypeAll || bType == bucketTypeRealTime {
		for i := 0; i < bucketsLen; i++ {
			idx := (b.pastMostBucketIdx + i) % bucketsLen
			fn(&b.bucketsRealTime[idx])
		}
	}

	if bType == bucketTypeAll || bType == bucketTypeNotRealTime {
		for key := range b.bucketsNotRealTime {
			fn(b.bucketsNotRealTime[key])
		}
	}
}

// computedForEachBucketAsc performs a fn on the buckets in time ascending order
// and returns the sum of the number returned by each fn
func (b *dbBuffer) computedForEachBucketAsc(
	op computeBucketIdxOp,
	bType bucketType,
	fn func(now time.Time, b *dbBuffer, id bucketID, bucketStart time.Time) int,
) int {
	now := b.nowFn()
	pastMostBucketStart := now.Truncate(b.blockSize).Add(-1 * b.blockSize)
	bucketNum := (pastMostBucketStart.UnixNano() / int64(b.blockSize)) % bucketsLen
	result := 0

	if bType == bucketTypeAll || bType == bucketTypeRealTime {
		for i := int64(0); i < bucketsLen; i++ {
			idx := int((bucketNum + i) % bucketsLen)
			curr := pastMostBucketStart.Add(time.Duration(i) * b.blockSize)
			result += fn(now, b, bucketIDRealtime(idx), curr)
		}
		if op == computeAndResetBucketIdx {
			b.pastMostBucketIdx = int(bucketNum)
		}
	}

	if bType == bucketTypeAll || bType == bucketTypeNotRealTime {
		for key := range b.bucketsNotRealTime {
			result += fn(now, b, bucketIDNotRealtime(key), key.ToTime())
		}
	}
	return result
}

func (b *dbBuffer) Snapshot(ctx context.Context, blockStart time.Time) (xio.SegmentReader, error) {
	var (
		res xio.SegmentReader
		err error
	)

	b.forEachBucketAsc(bucketTypeAll, func(bucket *dbBufferBucket) {
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
	b.forEachBucketAsc(bucketTypeAll, func(bucket *dbBufferBucket) {
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

	b.forEachBucketAsc(bucketTypeAll, func(bucket *dbBufferBucket) {
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
	b.forEachBucketAsc(bucketTypeAll, func(bucket *dbBufferBucket) {
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
	opts               Options
	start              time.Time
	encoders           []inOrderEncoder
	bootstrapped       []block.DatabaseBlock
	lastReadUnixNanos  int64
	lastWriteUnixNanos int64
	undrainedWrites    uint64
	empty              bool
	drained            bool
	isRealTime         bool
}

type inOrderEncoder struct {
	lastWriteAt time.Time
	encoder     encoding.Encoder
}

func (b *dbBufferBucket) resetTo(
	start time.Time,
	isRealTime bool,
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
	atomic.StoreInt64(&b.lastWriteUnixNanos, 0)
	b.empty = true
	b.drained = false
	b.resetNumWrites()
	b.isRealTime = isRealTime
}

func (b *dbBufferBucket) finalize() {
	b.resetEncoders()
	b.resetBootstrapped()
}

func (b *dbBufferBucket) canRead() bool {
	return !b.drained && !b.empty
}

func (b *dbBufferBucket) needsReset(
	now time.Time,
	targetStart time.Time,
) bool {
	if !b.isRealTime {
		return false
	}

	return !b.start.Equal(targetStart)
}

func (b *dbBufferBucket) isStale(now time.Time) bool {
	if b.isRealTime {
		return false
	}

	return b.numWrites() > 0 && now.Sub(b.lastWrite()) > b.opts.RetentionOptions().FlushAfterNoMetricPeriod()
}

func (b *dbBufferBucket) isFull() bool {
	if b.isRealTime {
		return false
	}

	return b.numWrites() >= b.opts.RetentionOptions().MaxWritesBeforeFlush()
}

func (b *dbBufferBucket) needsDrain(
	now time.Time,
	targetStart time.Time,
) bool {
	if !b.isRealTime {
		return b.isStale(now) || b.isFull()
	}

	retentionOpts := b.opts.RetentionOptions()
	blockSize := retentionOpts.BlockSize()
	bufferPast := retentionOpts.BufferPast()
	return b.canRead() && (b.needsReset(now, targetStart) ||
		b.start.Add(blockSize).Before(now.Add(-bufferPast)))
}

func (b *dbBufferBucket) bootstrap(
	bl block.DatabaseBlock,
) {
	b.empty = false
	b.bootstrapped = append(b.bootstrapped, bl)
}

func (b *dbBufferBucket) write(
	// `now` represents the time the metric came in and not the
	// time of the metric itself.
	now time.Time,
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
	b.setLastWrite(now)
	b.incNumWrites()
	// Required for non-realtime buckets
	b.drained = false
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

func (b *dbBufferBucket) setLastWrite(value time.Time) {
	atomic.StoreInt64(&b.lastWriteUnixNanos, value.UnixNano())
}

func (b *dbBufferBucket) incNumWrites() {
	atomic.AddUint64(&b.undrainedWrites, 1)
}

func (b *dbBufferBucket) resetNumWrites() {
	atomic.StoreUint64(&b.undrainedWrites, uint64(0))
}

func (b *dbBufferBucket) lastRead() time.Time {
	return time.Unix(0, atomic.LoadInt64(&b.lastReadUnixNanos))
}

func (b *dbBufferBucket) lastWrite() time.Time {
	return time.Unix(0, atomic.LoadInt64(&b.lastWriteUnixNanos))
}

func (b *dbBufferBucket) numWrites() uint64 {
	return atomic.LoadUint64(&b.undrainedWrites)
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

type dbBufferBucketPool struct {
	pool pool.ObjectPool
}

// newDBBufferBucketPool creates a new dbBufferBucketPool
func newDBBufferBucketPool(opts pool.ObjectPoolOptions) *dbBufferBucketPool {
	p := &dbBufferBucketPool{pool: pool.NewObjectPool(opts)}
	p.pool.Init(func() interface{} {
		return &dbBufferBucket{}
	})
	return p
}

func (p *dbBufferBucketPool) Get() *dbBufferBucket {
	return p.pool.Get().(*dbBufferBucket)
}

func (p *dbBufferBucketPool) Put(bucket *dbBufferBucket) {
	p.pool.Put(*bucket)
}
