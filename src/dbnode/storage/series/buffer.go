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
	"sort"
	"sync/atomic"
	"time"

	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/storage/block"
	m3dberrors "github.com/m3db/m3/src/dbnode/storage/errors"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
	xtime "github.com/m3db/m3/src/x/time"

	"go.uber.org/zap"
)

const (
	errBucketMapCacheNotInSync    = "bucket map keys do not match sorted keys cache"
	errBucketMapCacheNotInSyncFmt = errBucketMapCacheNotInSync + ", blockStart: %d"
)

var (
	timeZero           time.Time
	errIncompleteMerge = errors.New("bucket merge did not result in only one encoder")
)

const (
	bucketsCacheSize = 2

	writableBucketVer = 0
)

type databaseBuffer interface {
	Write(
		ctx context.Context,
		timestamp time.Time,
		value float64,
		unit xtime.Unit,
		annotation []byte,
		wOpts WriteOptions,
	) (bool, error)

	Snapshot(
		ctx context.Context,
		blockStart time.Time,
	) (xio.SegmentReader, error)

	Flush(
		ctx context.Context,
		blockStart time.Time,
		id ident.ID,
		tags ident.Tags,
		persistFn persist.DataFn,
		version int,
	) (FlushOutcome, error)

	ReadEncoded(
		ctx context.Context,
		start, end time.Time,
	) ([][]xio.BlockReader, error)

	FetchBlocks(
		ctx context.Context,
		starts []time.Time,
	) []block.FetchBlockResult

	FetchBlocksMetadata(
		ctx context.Context,
		start, end time.Time,
		opts FetchBlocksMetadataOptions,
	) (block.FetchBlockMetadataResults, error)

	IsEmpty() bool

	Stats() bufferStats

	Tick(versions map[xtime.UnixNano]BlockState) bufferTickResult

	Bootstrap(bl block.DatabaseBlock)

	Reset(opts Options)
}

type bufferStats struct {
	wiredBlocks int
}

type bufferTickResult struct {
	mergedOutOfOrderBlocks int
	evictedBuckets         int
}

type dbBuffer struct {
	opts  Options
	nowFn clock.NowFn

	bucketsMap map[xtime.UnixNano]*BufferBucketVersions
	// Cache of buckets to avoid map lookup of above.
	bucketVersionsCache [bucketsCacheSize]*BufferBucketVersions
	// This is an in order slice of the block starts in the bucketsMap.
	// We maintain this to avoid sorting the map keys adhoc when we want to
	// perform operations in chronological order.
	inOrderBlockStarts []time.Time
	bucketVersionsPool *BufferBucketVersionsPool
	bucketPool         *BufferBucketPool

	blockSize             time.Duration
	bufferPast            time.Duration
	bufferFuture          time.Duration
	coldWritesEnabled     bool
	retentionPeriod       time.Duration
	futureRetentionPeriod time.Duration
}

// NB(prateek): databaseBuffer.Reset(...) must be called upon the returned
// object prior to use.
func newDatabaseBuffer() databaseBuffer {
	b := &dbBuffer{
		bucketsMap:         make(map[xtime.UnixNano]*BufferBucketVersions),
		inOrderBlockStarts: make([]time.Time, 0, bucketsCacheSize),
	}
	return b
}

func (b *dbBuffer) Reset(opts Options) {
	b.opts = opts
	b.nowFn = opts.ClockOptions().NowFn()
	ropts := opts.RetentionOptions()
	b.bucketPool = opts.BufferBucketPool()
	b.bucketVersionsPool = opts.BufferBucketVersionsPool()
	b.blockSize = ropts.BlockSize()
	b.bufferPast = ropts.BufferPast()
	b.bufferFuture = ropts.BufferFuture()
	b.coldWritesEnabled = opts.ColdWritesEnabled()
	b.retentionPeriod = ropts.RetentionPeriod()
	b.futureRetentionPeriod = ropts.FutureRetentionPeriod()
}

// ResolveWriteType returns whether a write is a cold write or warm write.
func (b *dbBuffer) ResolveWriteType(
	timestamp time.Time,
	now time.Time,
) WriteType {
	pastLimit := now.Add(-1 * b.bufferPast)
	futureLimit := now.Add(b.bufferFuture)
	if !pastLimit.Before(timestamp) || !futureLimit.After(timestamp) {
		return ColdWrite
	}

	return WarmWrite
}

func (b *dbBuffer) Write(
	ctx context.Context,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
	wOpts WriteOptions,
) (bool, error) {
	now := b.nowFn()
	wType := b.ResolveWriteType(timestamp, now)

	if wType == ColdWrite {
		if !b.coldWritesEnabled {
			return false, m3dberrors.ErrColdWritesNotEnabled
		}

		if now.Add(-b.retentionPeriod).After(timestamp) {
			return false, m3dberrors.ErrTooPast
		}

		if !now.Add(b.futureRetentionPeriod).Add(b.blockSize).After(timestamp) {
			return false, m3dberrors.ErrTooFuture
		}
	}

	blockStart := timestamp.Truncate(b.blockSize)
	buckets := b.bucketVersionsAtCreate(blockStart)
	b.putBucketVersionsInCache(buckets)
	return buckets.write(timestamp, value, unit, annotation, wType)
}

func (b *dbBuffer) IsEmpty() bool {
	// A buffer can only be empty if there are no buckets in its map, since
	// buckets are only created when a write for a new block start is done, and
	// buckets are removed from the map when they are evicted from memory.
	return len(b.bucketsMap) == 0
}

func (b *dbBuffer) Stats() bufferStats {
	return bufferStats{
		wiredBlocks: len(b.bucketsMap),
	}
}

func (b *dbBuffer) Tick(blockStates map[xtime.UnixNano]BlockState) bufferTickResult {
	mergedOutOfOrder := 0
	evictedBuckets := 0
	for tNano, buckets := range b.bucketsMap {
		// The blockStates map is never be written to after creation, so this
		// read access is safe. Since this version map is a snapshot of the
		// versions, the real block flush versions may be higher. This is okay
		// here because it's safe to:
		// 1) not remove a bucket that's actually retrievable, or
		// 2) remove a lower versioned bucket.
		// Retrievable and higher versioned buckets will be left to be
		// collected in the next tick.
		blockState := blockStates[tNano]
		if !blockState.Retrievable {
			continue
		}

		buckets.removeBucketsUpToVersion(blockState.Version)

		if buckets.streamsLen() == 0 {
			// All underlying buckets have been flushed successfully, so we can
			// just remove the buckets from the bucketsMap.

			// TODO(juchan): in order to support cold writes, the buffer needs
			// to tell the series that these buckets were evicted from the
			// buffer so that the cached block in the series can either:
			//   1) be evicted, or
			//   2) be merged with the new data.
			// This needs to happen because the buffer just flushed new data
			// to disk, which the cached block does not have, and is
			// therefore invalid. This was fine while the missing data was still
			// in memory, but once we evict it from the buffer we need to make
			// sure that we bust the cache as well.
			b.removeBucketVersionsAt(tNano.ToTime())
			evictedBuckets++
			continue
		}

		// Once we've evicted all eligible buckets, we merge duplicate encoders
		// in the remaining ones to try and reclaim memory.
		merges, err := buckets.merge(WarmWrite)
		if err != nil {
			log := b.opts.InstrumentOptions().Logger()
			log.Error("buffer merge encode error", zap.Error(err))
		}
		if merges > 0 {
			mergedOutOfOrder++
		}
	}
	return bufferTickResult{
		mergedOutOfOrderBlocks: mergedOutOfOrder,
		evictedBuckets:         evictedBuckets,
	}
}

func (b *dbBuffer) Bootstrap(bl block.DatabaseBlock) {
	blockStart := bl.StartTime()
	buckets := b.bucketVersionsAtCreate(blockStart)
	buckets.bootstrap(bl)
}

func (b *dbBuffer) Snapshot(
	ctx context.Context,
	blockStart time.Time,
) (xio.SegmentReader, error) {
	buckets, exists := b.bucketVersionsAt(blockStart)
	if !exists {
		return nil, nil
	}

	// We only snapshot warm writes since cold writes get force merged every
	// tick anyway.
	_, err := buckets.merge(WarmWrite)
	if err != nil {
		return nil, err
	}

	streams := buckets.streams(ctx, streamsOptions{filterWriteType: true, writeType: WarmWrite})
	// We may need to merge again here because the regular merge method does
	// not merge buckets that have different versions.
	numStreams := len(streams)
	if numStreams == 1 {
		return streams[0], nil
	}

	sr := make([]xio.SegmentReader, 0, numStreams)
	for _, stream := range streams {
		sr = append(sr, stream)
	}

	bopts := b.opts.DatabaseBlockOptions()
	encoder := bopts.EncoderPool().Get()
	encoder.Reset(blockStart, bopts.DatabaseBlockAllocSize())
	iter := b.opts.MultiReaderIteratorPool().Get()
	defer func() {
		encoder.Close()
		iter.Close()
	}()
	iter.Reset(sr, blockStart, b.opts.RetentionOptions().BlockSize())

	for iter.Next() {
		dp, unit, annotation := iter.Current()
		if err := encoder.Encode(dp, unit, annotation); err != nil {
			return nil, err
		}
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}

	return encoder.Stream(), nil
}

func (b *dbBuffer) Flush(
	ctx context.Context,
	blockStart time.Time,
	id ident.ID,
	tags ident.Tags,
	persistFn persist.DataFn,
	version int,
) (FlushOutcome, error) {
	buckets, exists := b.bucketVersionsAt(blockStart)
	if !exists {
		return FlushOutcomeBlockDoesNotExist, nil
	}

	streams, err := buckets.toStreams(ctx)
	if err != nil {
		return FlushOutcomeErr, err
	}

	var stream xio.SegmentReader
	if numStreams := len(streams); numStreams == 1 {
		stream = streams[0]
	} else {
		// In the majority of cases, there will only be one stream to persist
		// here. Only when a previous flush fails midway through a shard will
		// there be buckets for previous versions. In this case, we need to try
		// to flush them again, so we merge them together to one stream and
		// persist it.
		encoder, _, err := mergeStreamsToEncoder(blockStart, streams, b.opts)
		if err != nil {
			return FlushOutcomeErr, err
		}

		stream = encoder.Stream()
		encoder.Close()
	}

	segment, err := stream.Segment()
	if err != nil {
		return FlushOutcomeErr, err
	}

	checksum := digest.SegmentChecksum(segment)

	err = persistFn(id, tags, segment, checksum)
	if err != nil {
		return FlushOutcomeErr, err
	}

	if bucket, exists := buckets.writableBucket(WarmWrite); exists {
		bucket.version = version
	}

	return FlushOutcomeFlushedToDisk, nil
}

func (b *dbBuffer) ReadEncoded(
	ctx context.Context,
	start time.Time,
	end time.Time,
) ([][]xio.BlockReader, error) {
	// TODO(r): pool these results arrays
	var res [][]xio.BlockReader

	for _, blockStart := range b.inOrderBlockStarts {
		if !blockStart.Before(end) || !start.Before(blockStart.Add(b.blockSize)) {
			continue
		}

		bv, exists := b.bucketVersionsAt(blockStart)
		if !exists {
			// Invariant violated. This means the keys in the bucket map does
			// not match the sorted keys cache, which should never happen.
			instrument.EmitAndLogInvariantViolation(
				b.opts.InstrumentOptions(), func(l *zap.Logger) {
					l.Error(errBucketMapCacheNotInSync, zap.Int64("blockStart", blockStart.UnixNano()))
				})
			return nil, instrument.InvariantErrorf(
				errBucketMapCacheNotInSyncFmt, blockStart.UnixNano())
		}

		if streams := bv.streams(ctx, streamsOptions{filterWriteType: false}); len(streams) > 0 {
			res = append(res, streams)
		}

		// NB(r): Store the last read time, should not set this when
		// calling FetchBlocks as a read is differentiated from
		// a FetchBlocks call. One is initiated by an external
		// entity and the other is used for streaming blocks between
		// the storage nodes. This distinction is important as this
		// data is important for use with understanding access patterns, etc.
		bv.setLastRead(b.nowFn())
	}

	return res, nil
}

func (b *dbBuffer) FetchBlocks(ctx context.Context, starts []time.Time) []block.FetchBlockResult {
	var res []block.FetchBlockResult

	for _, start := range starts {
		buckets, ok := b.bucketVersionsAt(start)
		if !ok {
			continue
		}

		if streams := buckets.streams(ctx, streamsOptions{filterWriteType: false}); len(streams) > 0 {
			res = append(res, block.NewFetchBlockResult(start, streams, nil))
		}
	}

	// Result should be sorted in ascending order.
	sort.Slice(res, func(i, j int) bool { return res[i].Start.Before(res[j].Start) })

	return res
}

func (b *dbBuffer) FetchBlocksMetadata(
	ctx context.Context,
	start, end time.Time,
	opts FetchBlocksMetadataOptions,
) (block.FetchBlockMetadataResults, error) {
	blockSize := b.opts.RetentionOptions().BlockSize()
	res := b.opts.FetchBlockMetadataResultsPool().Get()

	for _, blockStart := range b.inOrderBlockStarts {
		if !blockStart.Before(end) || !start.Before(blockStart.Add(blockSize)) {
			continue
		}

		bv, exists := b.bucketVersionsAt(blockStart)
		if !exists {
			// Invariant violated. This means the keys in the bucket map does
			// not match the sorted keys cache, which should never happen.
			instrument.EmitAndLogInvariantViolation(
				b.opts.InstrumentOptions(), func(l *zap.Logger) {
					l.Error(errBucketMapCacheNotInSync, zap.Int64("blockStart", blockStart.UnixNano()))
				})
			return nil, instrument.InvariantErrorf(errBucketMapCacheNotInSyncFmt, blockStart.UnixNano())
		}

		size := int64(bv.streamsLen())
		// If we have no data in this bucket, skip early without appending it to the result.
		if size == 0 {
			continue
		}
		var resultSize int64
		if opts.IncludeSizes {
			resultSize = size
		}
		var resultLastRead time.Time
		if opts.IncludeLastRead {
			resultLastRead = bv.lastRead()
		}
		// NB(r): Ignore if opts.IncludeChecksum because we avoid
		// calculating checksum since block is open and is being mutated
		res.Add(block.FetchBlockMetadataResult{
			Start:    bv.start,
			Size:     resultSize,
			LastRead: resultLastRead,
		})
	}

	return res, nil
}

func (b *dbBuffer) bucketVersionsAt(
	t time.Time,
) (*BufferBucketVersions, bool) {
	// First check LRU cache.
	for _, buckets := range b.bucketVersionsCache {
		if buckets == nil {
			continue
		}
		if buckets.start.Equal(t) {
			return buckets, true
		}
	}

	// Then check the map.
	if buckets, exists := b.bucketsMap[xtime.ToUnixNano(t)]; exists {
		return buckets, true
	}

	return nil, false
}

func (b *dbBuffer) bucketVersionsAtCreate(
	t time.Time,
) *BufferBucketVersions {
	if buckets, exists := b.bucketVersionsAt(t); exists {
		return buckets
	}

	buckets := b.bucketVersionsPool.Get()
	buckets.resetTo(t, b.opts, b.bucketPool)
	b.bucketsMap[xtime.ToUnixNano(t)] = buckets
	b.inOrderBlockStartsAdd(t)

	return buckets
}

func (b *dbBuffer) putBucketVersionsInCache(newBuckets *BufferBucketVersions) {
	replaceIdx := bucketsCacheSize - 1
	for i, buckets := range b.bucketVersionsCache {
		// Check if we have the same pointer in cache.
		if buckets == newBuckets {
			replaceIdx = i
		}
	}

	for i := replaceIdx; i > 0; i-- {
		b.bucketVersionsCache[i] = b.bucketVersionsCache[i-1]
	}

	b.bucketVersionsCache[0] = newBuckets
}

func (b *dbBuffer) removeBucketVersionsInCache(oldBuckets *BufferBucketVersions) {
	nilIdx := -1
	for i, buckets := range b.bucketVersionsCache {
		if buckets == oldBuckets {
			nilIdx = i
		}
	}
	if nilIdx == -1 {
		return
	}

	for i := nilIdx; i < bucketsCacheSize-1; i++ {
		b.bucketVersionsCache[i] = b.bucketVersionsCache[i+1]
	}

	b.bucketVersionsCache[bucketsCacheSize-1] = nil
}

func (b *dbBuffer) removeBucketVersionsAt(blockStart time.Time) {
	buckets, exists := b.bucketVersionsAt(blockStart)
	if !exists {
		return
	}
	delete(b.bucketsMap, xtime.ToUnixNano(blockStart))
	b.removeBucketVersionsInCache(buckets)
	b.inOrderBlockStartsRemove(blockStart)
	// nil out pointers.
	buckets.resetTo(timeZero, nil, nil)
	b.bucketVersionsPool.Put(buckets)
}

func (b *dbBuffer) inOrderBlockStartsAdd(newTime time.Time) {
	starts := b.inOrderBlockStarts
	idx := len(starts)
	// There shouldn't be that many starts here, so just linear search through.
	for i, t := range starts {
		if t.After(newTime) {
			idx = i
			break
		}
	}
	// Insert new time without allocating new slice.
	b.inOrderBlockStarts = append(starts, timeZero)
	// Update to new slice
	starts = b.inOrderBlockStarts
	copy(starts[idx+1:], starts[idx:])
	starts[idx] = newTime
}

func (b *dbBuffer) inOrderBlockStartsRemove(removeTime time.Time) {
	starts := b.inOrderBlockStarts
	// There shouldn't be that many starts here, so just linear search through.
	for i, t := range starts {
		if t.Equal(removeTime) {
			b.inOrderBlockStarts = append(starts[:i], starts[i+1:]...)
			return
		}
	}
}

// BufferBucketVersions is a container for different versions (from
// different flushes to disk) of buffer buckets in the database.
type BufferBucketVersions struct {
	buckets           []*BufferBucket
	start             time.Time
	opts              Options
	lastReadUnixNanos int64
	bucketPool        *BufferBucketPool
}

func (b *BufferBucketVersions) resetTo(
	start time.Time,
	opts Options,
	bucketPool *BufferBucketPool,
) {
	// nil all elements so that they get GC'd.
	for i := range b.buckets {
		b.buckets[i] = nil
	}
	b.buckets = b.buckets[:0]
	b.start = start
	b.opts = opts
	atomic.StoreInt64(&b.lastReadUnixNanos, 0)
	b.bucketPool = bucketPool
}

func (b *BufferBucketVersions) streams(ctx context.Context, opts streamsOptions) []xio.BlockReader {
	var res []xio.BlockReader
	for _, bucket := range b.buckets {
		if !opts.filterWriteType || bucket.wType == opts.writeType {
			res = append(res, bucket.streams(ctx)...)
		}
	}

	return res
}

func (b *BufferBucketVersions) streamsLen() int {
	res := 0
	for _, bucket := range b.buckets {
		res += bucket.streamsLen()
	}
	return res
}

func (b *BufferBucketVersions) write(
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
	wType WriteType,
) (bool, error) {
	return b.writableBucketCreate(wType).write(timestamp, value, unit, annotation)
}

func (b *BufferBucketVersions) merge(wType WriteType) (int, error) {
	res := 0
	for _, bucket := range b.buckets {
		// Only makes sense to merge buckets that are writable.
		if bucket.version == writableBucketVer && wType == bucket.wType {
			merges, err := bucket.merge()
			if err != nil {
				return res, nil
			}
			res += merges
		}
	}

	return res, nil
}

func (b *BufferBucketVersions) removeBucketsUpToVersion(version int) {
	// TODO(juchan): in order to support ColdWrites, we need to keep track of
	// separate bucket versions for ColdWrites and WarmWrites, since they have
	// different persist cycles. This will involve storing that state in the
	// shard flush state management. That state will need to be passed down
	// here so that this function will know which WriteType/version is safe to
	// remove.

	// Avoid allocating a new backing array.
	nonEvictedBuckets := b.buckets[:0]

	for _, bucket := range b.buckets {
		bVersion := bucket.version
		if bucket.wType == WarmWrite && bVersion != writableBucketVer &&
			bVersion <= version {
			// We no longer need to keep any version which is equal to
			// or less than the retrievable version, since that means
			// that the version has successfully persisted to disk.
			// Bucket gets reset before use.
			b.bucketPool.Put(bucket)
			continue
		}

		nonEvictedBuckets = append(nonEvictedBuckets, bucket)
	}

	b.buckets = nonEvictedBuckets
}

func (b *BufferBucketVersions) setLastRead(value time.Time) {
	atomic.StoreInt64(&b.lastReadUnixNanos, value.UnixNano())
}

func (b *BufferBucketVersions) lastRead() time.Time {
	return time.Unix(0, atomic.LoadInt64(&b.lastReadUnixNanos))
}

func (b *BufferBucketVersions) bootstrap(bl block.DatabaseBlock) {
	// TODO(juchan): what is a "cold" bootstrap?
	bucket := b.writableBucketCreate(WarmWrite)
	bucket.bootstrapped = append(bucket.bootstrapped, bl)
}

func (b *BufferBucketVersions) writableBucket(wType WriteType) (*BufferBucket, bool) {
	for _, bucket := range b.buckets {
		if bucket.version == writableBucketVer && bucket.wType == wType {
			return bucket, true
		}
	}

	return nil, false
}

func (b *BufferBucketVersions) writableBucketCreate(wType WriteType) *BufferBucket {
	bucket, exists := b.writableBucket(wType)

	if exists {
		return bucket
	}

	newBucket := b.bucketPool.Get()
	newBucket.resetTo(b.start, wType, b.opts)
	b.buckets = append(b.buckets, newBucket)
	return newBucket
}

func (b *BufferBucketVersions) toStreams(ctx context.Context) ([]xio.SegmentReader, error) {
	buckets := b.buckets
	res := make([]xio.SegmentReader, 0, len(buckets))

	for _, bucket := range buckets {
		stream, err := bucket.toStream(ctx)
		if err != nil {
			return nil, err
		}
		res = append(res, stream)
	}

	return res, nil
}

type streamsOptions struct {
	filterWriteType bool
	writeType       WriteType
}

// BufferBucket is a bucket in the buffer.
type BufferBucket struct {
	opts         Options
	start        time.Time
	encoders     []inOrderEncoder
	bootstrapped []block.DatabaseBlock
	version      int
	wType        WriteType
}

type inOrderEncoder struct {
	encoder     encoding.Encoder
	lastWriteAt time.Time
}

func (b *BufferBucket) resetTo(
	start time.Time,
	wType WriteType,
	opts Options,
) {
	// Close the old context if we're resetting for use.
	b.reset()
	b.opts = opts
	b.start = start
	bopts := b.opts.DatabaseBlockOptions()
	encoder := bopts.EncoderPool().Get()
	encoder.Reset(start, bopts.DatabaseBlockAllocSize())
	b.encoders = append(b.encoders, inOrderEncoder{
		encoder: encoder,
	})
	b.bootstrapped = nil
	// We would only ever create a bucket for it to be writable.
	b.version = writableBucketVer
	b.wType = wType
}

func (b *BufferBucket) reset() {
	b.resetEncoders()
	b.resetBootstrapped()
}

func (b *BufferBucket) write(
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) (bool, error) {
	datapoint := ts.Datapoint{
		Timestamp: timestamp,
		Value:     value,
	}

	// Find the correct encoder to write to
	idx := -1
	for i := range b.encoders {
		lastWriteAt := b.encoders[i].lastWriteAt
		if timestamp.Equal(lastWriteAt) {
			last, err := b.encoders[i].encoder.LastEncoded()
			if err != nil {
				return false, err
			}
			if last.Value == value {
				// No-op since matches the current value. Propagates up to callers that
				// no value was written.
				return false, nil
			}
			continue
		}

		if timestamp.After(lastWriteAt) {
			idx = i
			break
		}
	}

	// Upsert/last-write-wins semantics.
	// NB(r): We push datapoints with the same timestamp but differing
	// value into a new encoder later in the stack of in order encoders
	// since an encoder is immutable.
	// The encoders pushed later will surface their values first.
	if idx != -1 {
		return true, b.writeToEncoderIndex(idx, datapoint, unit, annotation)
	}

	// Need a new encoder, we didn't find an encoder to write to
	b.opts.Stats().IncCreatedEncoders()
	bopts := b.opts.DatabaseBlockOptions()
	blockSize := b.opts.RetentionOptions().BlockSize()
	blockAllocSize := bopts.DatabaseBlockAllocSize()

	encoder := b.opts.EncoderPool().Get()
	encoder.Reset(timestamp.Truncate(blockSize), blockAllocSize)

	b.encoders = append(b.encoders, inOrderEncoder{
		encoder:     encoder,
		lastWriteAt: timestamp,
	})

	idx = len(b.encoders) - 1
	err := b.writeToEncoderIndex(idx, datapoint, unit, annotation)
	if err != nil {
		encoder.Close()
		b.encoders = b.encoders[:idx]
		return false, err
	}
	return true, nil
}

func (b *BufferBucket) writeToEncoderIndex(
	idx int,
	datapoint ts.Datapoint,
	unit xtime.Unit,
	annotation []byte,
) error {
	err := b.encoders[idx].encoder.Encode(datapoint, unit, annotation)
	if err != nil {
		return err
	}

	b.encoders[idx].lastWriteAt = datapoint.Timestamp
	return nil
}

func (b *BufferBucket) streams(ctx context.Context) []xio.BlockReader {
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

func (b *BufferBucket) streamsLen() int {
	length := 0
	for i := range b.bootstrapped {
		length += b.bootstrapped[i].Len()
	}
	for i := range b.encoders {
		length += b.encoders[i].encoder.Len()
	}
	return length
}

func (b *BufferBucket) resetEncoders() {
	var zeroed inOrderEncoder
	for i := range b.encoders {
		// Register when this bucket resets we close the encoder.
		encoder := b.encoders[i].encoder
		encoder.Close()
		b.encoders[i] = zeroed
	}
	b.encoders = b.encoders[:0]
}

func (b *BufferBucket) resetBootstrapped() {
	for i := range b.bootstrapped {
		bl := b.bootstrapped[i]
		bl.Close()
	}
	b.bootstrapped = nil
}

func (b *BufferBucket) needsMerge() bool {
	return !(b.hasJustSingleEncoder() || b.hasJustSingleBootstrappedBlock())
}

func (b *BufferBucket) hasJustSingleEncoder() bool {
	return len(b.encoders) == 1 && len(b.bootstrapped) == 0
}

func (b *BufferBucket) hasJustSingleBootstrappedBlock() bool {
	encodersEmpty := len(b.encoders) == 0 ||
		(len(b.encoders) == 1 && b.encoders[0].encoder.Len() == 0)
	return encodersEmpty && len(b.bootstrapped) == 1
}

func (b *BufferBucket) merge() (int, error) {
	if !b.needsMerge() {
		// Save unnecessary work
		return 0, nil
	}

	var (
		start   = b.start
		readers = make([]xio.SegmentReader, 0, len(b.encoders)+len(b.bootstrapped))
		streams = make([]xio.SegmentReader, 0, len(b.encoders))
		ctx     = b.opts.ContextPool().Get()
		merges  = 0
	)
	defer func() {
		ctx.Close()
		// NB(r): Only need to close the mutable encoder streams as
		// the context we created for reading the bootstrap blocks
		// when closed will close those streams.
		for _, stream := range streams {
			stream.Finalize()
		}
	}()

	// Rank bootstrapped blocks as data that has appeared before data that
	// arrived locally in the buffer
	for i := range b.bootstrapped {
		block, err := b.bootstrapped[i].Stream(ctx)
		if err == nil && block.SegmentReader != nil {
			merges++
			readers = append(readers, block.SegmentReader)
		}
	}

	for i := range b.encoders {
		if s := b.encoders[i].encoder.Stream(); s != nil {
			merges++
			readers = append(readers, s)
			streams = append(streams, s)
		}
	}

	encoder, lastWriteAt, err := mergeStreamsToEncoder(start, readers, b.opts)
	if err != nil {
		return 0, err
	}

	b.resetEncoders()
	b.resetBootstrapped()

	b.encoders = append(b.encoders, inOrderEncoder{
		encoder:     encoder,
		lastWriteAt: lastWriteAt,
	})

	return merges, nil
}

// mergeStreamsToEncoder merges streams to an encoder and returns the last
// write time. It is the responsibility of the caller to close the returned
// encoder when appropriate.
func mergeStreamsToEncoder(
	blockStart time.Time,
	streams []xio.SegmentReader,
	opts Options,
) (encoding.Encoder, time.Time, error) {
	bopts := opts.DatabaseBlockOptions()
	encoder := opts.EncoderPool().Get()
	encoder.Reset(blockStart, bopts.DatabaseBlockAllocSize())
	iter := bopts.MultiReaderIteratorPool().Get()
	defer iter.Close()

	var lastWriteAt time.Time
	iter.Reset(streams, blockStart, opts.RetentionOptions().BlockSize())
	for iter.Next() {
		dp, unit, annotation := iter.Current()
		if err := encoder.Encode(dp, unit, annotation); err != nil {
			encoder.Close()
			return nil, timeZero, err
		}
		lastWriteAt = dp.Timestamp
	}
	if err := iter.Err(); err != nil {
		encoder.Close()
		return nil, timeZero, err
	}

	return encoder, lastWriteAt, nil
}

func (b *BufferBucket) toStream(ctx context.Context) (xio.SegmentReader, error) {
	if b.hasJustSingleEncoder() {
		b.resetBootstrapped()
		// Already merged as a single encoder.
		stream := b.encoders[0].encoder.Stream()
		ctx.RegisterFinalizer(stream)
		return stream, nil
	}

	if b.hasJustSingleBootstrappedBlock() {
		// Need to reset encoders but do not want to finalize the block as we
		// are passing ownership of it to the caller.
		b.resetEncoders()
		return b.bootstrapped[0].Stream(ctx)
	}

	_, err := b.merge()
	if err != nil {
		b.resetEncoders()
		b.resetBootstrapped()
		return nil, err
	}

	// After a successful merge, encoders and bootstrapped blocks will be
	// reset, and the merged encoder appended as the only encoder in the
	// bucket.
	if !b.hasJustSingleEncoder() {
		return nil, errIncompleteMerge
	}

	stream := b.encoders[0].encoder.Stream()
	ctx.RegisterFinalizer(stream)
	return stream, nil
}

// BufferBucketVersionsPool provides a pool for BufferBucketVersions.
type BufferBucketVersionsPool struct {
	pool pool.ObjectPool
}

// NewBufferBucketVersionsPool creates a new BufferBucketVersionsPool.
func NewBufferBucketVersionsPool(opts pool.ObjectPoolOptions) *BufferBucketVersionsPool {
	p := &BufferBucketVersionsPool{pool: pool.NewObjectPool(opts)}
	p.pool.Init(func() interface{} {
		return &BufferBucketVersions{}
	})
	return p
}

// Get gets a BufferBucketVersions from the pool.
func (p *BufferBucketVersionsPool) Get() *BufferBucketVersions {
	return p.pool.Get().(*BufferBucketVersions)
}

// Put puts a BufferBucketVersions back into the pool.
func (p *BufferBucketVersionsPool) Put(buckets *BufferBucketVersions) {
	p.pool.Put(buckets)
}

// BufferBucketPool provides a pool for BufferBuckets.
type BufferBucketPool struct {
	pool pool.ObjectPool
}

// NewBufferBucketPool creates a new BufferBucketPool.
func NewBufferBucketPool(opts pool.ObjectPoolOptions) *BufferBucketPool {
	p := &BufferBucketPool{pool: pool.NewObjectPool(opts)}
	p.pool.Init(func() interface{} {
		return &BufferBucket{}
	})
	return p
}

// Get gets a BufferBucket from the pool.
func (p *BufferBucketPool) Get() *BufferBucket {
	return p.pool.Get().(*BufferBucket)
}

// Put puts a BufferBucket back into the pool.
func (p *BufferBucketPool) Put(bucket *BufferBucket) {
	p.pool.Put(bucket)
}
