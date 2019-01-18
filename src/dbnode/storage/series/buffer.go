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
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/storage/block"
	m3dberrors "github.com/m3db/m3/src/dbnode/storage/errors"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"
)

var (
	errNoAvailableBuckets = errors.New("[invariant violated] buffer has no available buckets")
	timeZero              time.Time
)

const (
	bucketsCacheSize = 2
	// In the most common case, there would only be one bucket in a
	// buckets slice, i.e. it gets written to, flushed, and then the buckets
	// get evicted from the map.
	defaultBufferBucketPoolSize         = 2
	defaultBufferBucketVersionsPoolSize = defaultBufferBucketPoolSize

	writableBucketVer = 0
)

type databaseBuffer interface {
	Write(
		ctx context.Context,
		timestamp time.Time,
		value float64,
		unit xtime.Unit,
		annotation []byte,
		wopts WriteOptions,
	) error

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

	Tick(versions map[xtime.UnixNano]BlockState) bufferTickResult

	Bootstrap(bl block.DatabaseBlock)

	Reset(opts Options)
}

type bufferStats struct {
	wiredBlocks int
}

type bufferTickResult struct {
	mergedOutOfOrderBlocks int
}

type dbBuffer struct {
	opts  Options
	nowFn clock.NowFn

	bucketsMap map[xtime.UnixNano]*BufferBucketVersions
	// Cache of buckets to avoid map lookup of above.
	bucketsCache       [bucketsCacheSize]*BufferBucketVersions
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
		bucketsMap: make(map[xtime.UnixNano]*BufferBucketVersions),
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

func (b *dbBuffer) Write(
	ctx context.Context,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
	wopts WriteOptions,
) error {
	now := b.nowFn()
	wType := wopts.WriteType
	if wType == UndefinedWriteType {
		wType = b.writeType(timestamp, now)
	}

	if wType == ColdWrite {
		if !b.coldWritesEnabled {
			return m3dberrors.ErrColdWritesNotEnabled
		}

		if now.Add(-b.retentionPeriod).Truncate(b.blockSize).After(timestamp) {
			return m3dberrors.ErrTooPast
		}

		if now.Add(b.futureRetentionPeriod).Truncate(b.blockSize).Add(b.blockSize).Before(timestamp) {
			return m3dberrors.ErrTooFuture
		}
	}

	blockStart := timestamp.Truncate(b.blockSize)
	buckets := b.bucketsAtCreate(blockStart)
	b.putBucketInCache(buckets)
	return buckets.Write(timestamp, value, unit, annotation, wType)
}

func (b *dbBuffer) writeType(timestamp time.Time, now time.Time) WriteType {
	ropts := b.opts.RetentionOptions()
	futureLimit := now.Add(ropts.BufferFuture())
	pastLimit := now.Add(-1 * ropts.BufferPast())

	if pastLimit.Before(timestamp) && futureLimit.After(timestamp) {
		return WarmWrite
	}

	return ColdWrite
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
			b.removeBucketsAt(tNano.ToTime())
			continue
		}

		// Once we've evicted all eligible buckets, we merge duplicate encoders
		// in the remaining ones to try and reclaim memory.
		merges, err := buckets.merge(WarmWrite)
		if err != nil {
			log := b.opts.InstrumentOptions().Logger()
			log.Errorf("buffer merge encode error: %v", err)
		}
		if merges > 0 {
			mergedOutOfOrder++
		}
	}
	return bufferTickResult{
		mergedOutOfOrderBlocks: mergedOutOfOrder,
	}
}

func (b *dbBuffer) Bootstrap(bl block.DatabaseBlock) {
	blockStart := bl.StartTime()
	buckets := b.bucketsAtCreate(blockStart)
	buckets.bootstrap(bl)
}

func (b *dbBuffer) Snapshot(
	ctx context.Context,
	blockStart time.Time,
) (xio.SegmentReader, error) {
	buckets, exists := b.bucketsAt(blockStart)
	if !exists {
		return nil, nil
	}

	// A call to snapshot can only be for warm writes.
	_, err := buckets.merge(WarmWrite)
	if err != nil {
		return nil, err
	}

	streams := buckets.streams(ctx, WarmWrite)
	// We may need to merge again here because the regular merge method does
	// not merge buckets that have different versions.
	numStreams := len(streams)
	if numStreams == 1 {
		return streams[0], nil
	}

	sr := make([]xio.SegmentReader, 0, numStreams)
	for _, stream := range streams {
		sr = append(sr, stream.SegmentReader)
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
	buckets, exists := b.bucketsAt(blockStart)
	if !exists {
		return FlushOutcomeBlockDoesNotExist, nil
	}

	// A call to flush can only be for warm writes.
	blocks, err := buckets.toBlocks()
	if err != nil {
		return FlushOutcomeErr, err
	}

	// In the majority of cases, there will only be one block to persist here.
	// Only when a flush fails
	for _, block := range blocks {
		if block == nil {
			return FlushOutcomeBlockDoesNotExist, nil
		}

		stream, err := block.Stream(ctx)
		if err != nil {
			return FlushOutcomeErr, err
		}

		segment, err := stream.Segment()
		if err != nil {
			return FlushOutcomeErr, err
		}

		checksum, err := block.Checksum()
		if err != nil {
			return FlushOutcomeErr, err
		}

		err = persistFn(id, tags, segment, checksum)
		if err != nil {
			return FlushOutcomeErr, err
		}
	}

	if bucket, exists := buckets.writableBucket(WarmWrite); exists {
		bucket.version = version
	}

	return FlushOutcomeFlushedToDisk, nil
}

func (b *dbBuffer) ReadEncoded(ctx context.Context, start, end time.Time) [][]xio.BlockReader {
	// TODO(r): pool these results arrays
	var res [][]xio.BlockReader

	b.forEachBucketAsc(func(bv *BufferBucketVersions) {
		if !bv.start.Before(end) || !start.Before(bv.start.Add(b.blockSize)) {
			return
		}

		res = append(res, bv.streams(ctx, WarmWrite))
		res = append(res, bv.streams(ctx, ColdWrite))

		// NB(r): Store the last read time, should not set this when
		// calling FetchBlocks as a read is differentiated from
		// a FetchBlocks call. One is initiated by an external
		// entity and the other is used for streaming blocks between
		// the storage nodes. This distinction is important as this
		// data is important for use with understanding access patterns, etc.
		bv.setLastRead(b.nowFn())
	})

	return res
}

func (b *dbBuffer) forEachBucketAsc(fn func(*BufferBucketVersions)) {
	buckets := b.bucketsMap

	// Handle these differently to avoid allocating a slice in these cases.
	if len(buckets) == 0 || len(buckets) == 1 {
		for key := range buckets {
			fn(buckets[key])
		}
		return
	}

	keys := make([]xtime.UnixNano, 0, len(buckets))
	for k := range buckets {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].Before(keys[j])
	})

	for _, key := range keys {
		// No need to check for existence since we just got the list of keys.
		bucket, _ := b.bucketsAt(key.ToTime())
		fn(bucket)
	}
}

func (b *dbBuffer) FetchBlocks(ctx context.Context, starts []time.Time) []block.FetchBlockResult {
	var res []block.FetchBlockResult

	for _, start := range starts {
		buckets, ok := b.bucketsAt(start)
		if !ok {
			continue
		}

		if streams := buckets.streams(ctx, WarmWrite); len(streams) > 0 {
			res = append(res, block.NewFetchBlockResult(start, streams, nil))
		}

		if streams := buckets.streams(ctx, ColdWrite); len(streams) > 0 {
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
) block.FetchBlockMetadataResults {
	blockSize := b.opts.RetentionOptions().BlockSize()
	res := b.opts.FetchBlockMetadataResultsPool().Get()

	b.forEachBucketAsc(func(bv *BufferBucketVersions) {
		if !bv.start.Before(end) || !start.Before(bv.start.Add(blockSize)) {
			return
		}

		size := int64(bv.streamsLen())
		// If we have no data in this bucket, skip early without appending it to the result.
		if size == 0 {
			return
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
	})

	return res
}

func (b *dbBuffer) bucketsAt(
	t time.Time,
) (*BufferBucketVersions, bool) {
	// First check LRU cache.
	for _, buckets := range b.bucketsCache {
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

func (b *dbBuffer) bucketsAtCreate(
	t time.Time,
) *BufferBucketVersions {
	if buckets, exists := b.bucketsAt(t); exists {
		return buckets
	}

	buckets := b.bucketVersionsPool.Get()
	buckets.resetTo(t, b.opts, b.bucketPool)
	b.bucketsMap[xtime.ToUnixNano(t)] = buckets
	return buckets
}

func (b *dbBuffer) putBucketInCache(newBuckets *BufferBucketVersions) {
	replaceIdx := bucketsCacheSize - 1
	for i, buckets := range b.bucketsCache {
		// Check if we have the same pointer in cache.
		if buckets == newBuckets {
			replaceIdx = i
		}
	}

	for i := replaceIdx; i > 0; i-- {
		b.bucketsCache[i] = b.bucketsCache[i-1]
	}

	b.bucketsCache[0] = newBuckets
}

func (b *dbBuffer) removeBucketsAt(blockStart time.Time) {
	buckets, exists := b.bucketsAt(blockStart)
	if !exists {
		return
	}
	// nil out pointers.
	buckets.resetTo(timeZero, nil, nil)
	b.bucketVersionsPool.Put(buckets)
	delete(b.bucketsMap, xtime.ToUnixNano(blockStart))
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

func (b *BufferBucketVersions) streams(ctx context.Context, wType WriteType) []xio.BlockReader {
	var res []xio.BlockReader
	for _, bucket := range b.buckets {
		if bucket.wType == wType {
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

func (b *BufferBucketVersions) Write(
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
	wType WriteType,
) error {
	return b.writableBucketCreate(wType).Write(timestamp, value, unit, annotation)
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
	// Avoid allocating a new backing array.
	nonEvictedBuckets := b.buckets[:0]

	for _, bucket := range b.buckets {
		bVersion := bucket.version
		// TODO(juchan): deal with ColdWrite too.
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
	b.writableBucketCreate(WarmWrite).addBlock(bl)
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

func (b *BufferBucketVersions) toBlocks() ([]block.DatabaseBlock, error) {
	buckets := b.buckets
	res := make([]block.DatabaseBlock, 0, len(buckets))

	for _, bucket := range buckets {
		block, err := bucket.toBlock()
		if err != nil {
			return nil, err
		}
		res = append(res, block)
	}

	return res, nil
}

// BufferBucket is a bucket in the buffer.
type BufferBucket struct {
	opts     Options
	start    time.Time
	encoders []inOrderEncoder
	blocks   []block.DatabaseBlock
	version  int
	wType    WriteType
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
	b.wType = wType
	b.opts = opts
	bopts := b.opts.DatabaseBlockOptions()
	encoder := bopts.EncoderPool().Get()
	encoder.Reset(start, bopts.DatabaseBlockAllocSize())

	b.start = start
	b.encoders = append(b.encoders, inOrderEncoder{
		encoder: encoder,
	})
	b.blocks = nil
}

func (b *BufferBucket) reset() {
	b.resetEncoders()
	b.resetBlocks()
}

func (b *BufferBucket) addBlock(
	bl block.DatabaseBlock,
) {
	b.blocks = append(b.blocks, bl)
}

func (b *BufferBucket) Write(
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
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
				return err
			}
			if last.Value == value {
				// No-op since matches the current value
				// TODO(r): in the future we could return some metadata that
				// this result was a no-op and hence does not need to be written
				// to the commit log, otherwise high frequency write volumes
				// that are using M3DB as a cache-like index of things seen
				// in a time window will still cause a flood of disk/CPU resource
				// usage writing values to the commit log, even if the memory
				// profile is lean as a side effect of this write being a no-op.
				return nil
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
		return b.writeToEncoderIndex(idx, datapoint, unit, annotation)
	}

	// Need a new encoder, we didn't find an encoder to write to
	b.opts.Stats().IncCreatedEncoders()
	bopts := b.opts.DatabaseBlockOptions()
	blockSize := b.opts.RetentionOptions().BlockSize()
	blockAllocSize := bopts.DatabaseBlockAllocSize()

	encoder := bopts.EncoderPool().Get()
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
		return err
	}
	return nil
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
	streams := make([]xio.BlockReader, 0, len(b.blocks)+len(b.encoders))

	for i := range b.blocks {
		if b.blocks[i].Len() == 0 {
			continue
		}
		if s, err := b.blocks[i].Stream(ctx); err == nil && s.IsNotEmpty() {
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
	for i := range b.blocks {
		length += b.blocks[i].Len()
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

func (b *BufferBucket) resetBlocks() {
	for i := range b.blocks {
		bl := b.blocks[i]
		bl.Close()
	}
	b.blocks = nil
}

func (b *BufferBucket) needsMerge() bool {
	return !(b.hasJustSingleEncoder() || b.hasJustSingleBootstrappedBlock())
}

func (b *BufferBucket) hasJustSingleEncoder() bool {
	return len(b.encoders) == 1 && len(b.blocks) == 0
}

func (b *BufferBucket) hasJustSingleBootstrappedBlock() bool {
	encodersEmpty := len(b.encoders) == 0 ||
		(len(b.encoders) == 1 && b.encoders[0].encoder.Len() == 0)
	return encodersEmpty && len(b.blocks) == 1
}

func (b *BufferBucket) merge() (int, error) {
	if !b.needsMerge() {
		// Save unnecessary work
		return 0, nil
	}

	merges := 0
	bopts := b.opts.DatabaseBlockOptions()
	encoder := bopts.EncoderPool().Get()
	encoder.Reset(b.start, bopts.DatabaseBlockAllocSize())

	var (
		start   = b.start
		readers = make([]xio.SegmentReader, 0, len(b.encoders)+len(b.blocks))
		streams = make([]xio.SegmentReader, 0, len(b.encoders))
		iter    = b.opts.MultiReaderIteratorPool().Get()
		ctx     = b.opts.ContextPool().Get()
	)
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

	// Rank bootstrapped blocks as data that has appeared before data that
	// arrived locally in the buffer
	for i := range b.blocks {
		block, err := b.blocks[i].Stream(ctx)
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

	var lastWriteAt time.Time
	iter.Reset(readers, start, b.opts.RetentionOptions().BlockSize())
	for iter.Next() {
		dp, unit, annotation := iter.Current()
		if err := encoder.Encode(dp, unit, annotation); err != nil {
			return 0, err
		}
		lastWriteAt = dp.Timestamp
	}
	if err := iter.Err(); err != nil {
		return 0, err
	}

	b.resetEncoders()
	b.resetBlocks()

	b.encoders = append(b.encoders, inOrderEncoder{
		encoder:     encoder,
		lastWriteAt: lastWriteAt,
	})

	return merges, nil
}

func (b *BufferBucket) toBlock() (block.DatabaseBlock, error) {
	if b.hasJustSingleEncoder() {
		// Already merged as a single encoder
		encoder := b.encoders[0].encoder
		newBlock := b.opts.DatabaseBlockOptions().DatabaseBlockPool().Get()
		blockSize := b.opts.RetentionOptions().BlockSize()
		newBlock.Reset(b.start, blockSize, encoder.Discard())

		// The single encoder is already discarded, no need to call resetEncoders
		// just remove it from the list of encoders
		b.encoders = b.encoders[:0]
		b.resetBlocks()
		// We need to retain this block otherwise the data contained cannot be
		// read, since it won't be in the buffer and we don't know when the
		// flush will fully complete. Blocks are also quicker to stream from
		// compared to from encoders, so keep the block.
		b.addBlock(newBlock)
		return newBlock, nil
	}

	if b.hasJustSingleBootstrappedBlock() {
		// Need to reset encoders but do not want to finalize the block as we
		// are passing ownership of it to the caller
		b.resetEncoders()
		return b.blocks[0], nil
	}

	_, err := b.merge()
	if err != nil {
		b.resetEncoders()
		b.resetBlocks()
		return nil, err
	}

	merged := b.encoders[0].encoder

	newBlock := b.opts.DatabaseBlockOptions().DatabaseBlockPool().Get()
	blockSize := b.opts.RetentionOptions().BlockSize()
	newBlock.Reset(b.start, blockSize, merged.Discard())

	// The merged encoder is already discarded, no need to call resetEncoders
	// just remove it from the list of encoders
	b.encoders = b.encoders[:0]
	b.resetBlocks()
	// We need to retain this block otherwise the data contained cannot be
	// read, since it won't be in the buffer and we don't know when the
	// flush will fully complete. It's also quicker to stream from blocks
	// compared to from encoders, so keep the block instead.
	b.addBlock(newBlock)
	return newBlock, nil
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
