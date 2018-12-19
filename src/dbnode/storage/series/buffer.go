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
	bucketsCacheSize       = 2
	defaultBucketsPoolSize = 2
	// In the most common case, there would only be one bucket in a
	// buckets slice, i.e. it gets written to, flushed, and then the buckets
	// get evicted from the map
	defaultBucketPoolSize = defaultBucketsPoolSize

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

	Tick() bufferTickResult

	Bootstrap(bl block.DatabaseBlock)

	Reset(blockRetriever QueryableBlockRetriever, opts Options)
}

type bufferStats struct {
	wiredBlocks int
}

type bufferTickResult struct {
	mergedOutOfOrderBlocks int
}

type dbBuffer struct {
	opts           Options
	nowFn          clock.NowFn
	blockRetriever QueryableBlockRetriever

	bucketsMap map[xtime.UnixNano]*dbBufferBuckets
	// Cache of buckets to avoid map lookup of above
	bucketsCache [bucketsCacheSize]*dbBufferBuckets
	bucketsPool  *dbBufferBucketsPool
	bucketPool   *dbBufferBucketPool

	blockSize         time.Duration
	bufferPast        time.Duration
	bufferFuture      time.Duration
	coldWritesEnabled bool
}

// NB(prateek): databaseBuffer.Reset(...) must be called upon the returned
// object prior to use.
func newDatabaseBuffer() databaseBuffer {
	b := &dbBuffer{
		bucketsMap: make(map[xtime.UnixNano]*dbBufferBuckets),
	}
	return b
}

func (b *dbBuffer) Reset(blockRetriever QueryableBlockRetriever, opts Options) {
	b.opts = opts
	b.nowFn = opts.ClockOptions().NowFn()
	ropts := opts.RetentionOptions()
	b.blockRetriever = blockRetriever
	bucketsPoolOpts := pool.NewObjectPoolOptions().SetSize(defaultBucketsPoolSize)
	b.bucketsPool = newDBBufferBucketsPool(bucketsPoolOpts)
	bucketPoolOpts := pool.NewObjectPoolOptions().SetSize(defaultBucketPoolSize)
	b.bucketPool = newDBBufferBucketPool(bucketPoolOpts)
	b.blockSize = ropts.BlockSize()
	b.bufferPast = ropts.BufferPast()
	b.bufferFuture = ropts.BufferFuture()
	b.coldWritesEnabled = opts.ColdWritesEnabled()
}

func (b *dbBuffer) Write(
	ctx context.Context,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
	wopts WriteOptions,
) error {
	wType := wopts.WriteType
	if wType == UndefinedWriteType {
		wType = b.writeType(timestamp, b.nowFn())
	}

	if wType == ColdWrite && !b.coldWritesEnabled {
		return m3dberrors.ErrColdWritesNotEnabled
	}

	blockStart := timestamp.Truncate(b.blockSize)
	buckets := b.bucketsAtCreate(blockStart)
	b.putBucketInCache(buckets)
	return buckets.write(timestamp, value, unit, annotation, wType)
}

func (b *dbBuffer) writeType(timestamp time.Time, writeTime time.Time) WriteType {
	ropts := b.opts.RetentionOptions()
	futureLimit := writeTime.Add(1 * ropts.BufferFuture())
	pastLimit := writeTime.Add(-1 * ropts.BufferPast())

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

func (b *dbBuffer) Tick() bufferTickResult {
	mergedOutOfOrder := 0
	retriever := b.blockRetriever
	for tNano, buckets := range b.bucketsMap {
		blockStart := tNano.ToTime()
		if !retriever.IsBlockRetrievable(blockStart) {
			continue
		}

		// Avoid allocating a new backing array
		nonEvictedBuckets := buckets.buckets[:0]
		version := retriever.RetrievableBlockVersion(blockStart)
		for _, bucket := range buckets.buckets {
			bVersion := bucket.version
			// TODO(juchan): deal with ColdWrite too
			if bucket.wType == WarmWrite && bVersion != writableBucketVer &&
				bVersion <= version {
				// We no longer need to keep any version which is equal to
				// or less than the retrievable version, since that means
				// that the version has successfully persisted to disk
				bucket.finalize()
				b.bucketPool.Put(bucket)
				continue
			}

			nonEvictedBuckets = append(nonEvictedBuckets, bucket)
		}

		// All underlying buckets have been flushed successfully, so we can
		// just remove the buckets from the bucketsMap
		if len(nonEvictedBuckets) == 0 {
			// TODO(juchan): in order to support cold writes, the buffer needs
			// to tell the series that these buckets were evicted from the
			// buffer so that the cached block in the series can either:
			//   1) be evicted, or
			//   2) be merged with the new data.
			// This needs to happen because the buffer just flushed new data
			// to disk, which the cached block does not have, and is
			// therefore invalid.
			b.removeBucketsAt(blockStart)
			continue
		}

		buckets.buckets = nonEvictedBuckets

		r, err := buckets.merge(WarmWrite)
		if err != nil {
			log := b.opts.InstrumentOptions().Logger()
			log.Errorf("buffer merge encode error: %v", err)
		}
		if r.merges > 0 {
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

	// A call to snapshot can only be for warm writes
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
	block, err := buckets.toBlock(WarmWrite)
	if err != nil {
		return FlushOutcomeErr, err
	}
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

	// Don't need to check error here because a non-nil block means that a
	// writable bucket exists
	bucket, _ := buckets.writableBucket(WarmWrite)
	bucket.version = version

	return FlushOutcomeFlushedToDisk, nil
}

func (b *dbBuffer) ReadEncoded(ctx context.Context, start, end time.Time) [][]xio.BlockReader {
	// TODO(r): pool these results arrays
	var res [][]xio.BlockReader

	keys := b.sortedBucketsKeys(true)
	for _, key := range keys {
		buckets, exists := b.bucketsAt(key.ToTime())
		if !exists || !buckets.start.Before(end) ||
			!start.Before(buckets.start.Add(b.blockSize)) {
			continue
		}

		res = append(res, buckets.streams(ctx, WarmWrite))
		res = append(res, buckets.streams(ctx, ColdWrite))

		// NB(r): Store the last read time, should not set this when
		// calling FetchBlocks as a read is differentiated from
		// a FetchBlocks call. One is initiated by an external
		// entity and the other is used for streaming blocks between
		// the storage nodes. This distinction is important as this
		// data is important for use with understanding access patterns, etc.
		buckets.setLastRead(b.nowFn())
	}

	return res
}

func (b *dbBuffer) sortedBucketsKeys(ascending bool) []xtime.UnixNano {
	buckets := b.bucketsMap
	keys := make([]xtime.UnixNano, len(buckets))
	i := 0
	for k := range buckets {
		keys[i] = k
		i++
	}
	if ascending {
		sort.Slice(keys, func(i, j int) bool {
			return keys[i].Before(keys[j])
		})
	} else {
		sort.Slice(keys, func(i, j int) bool {
			return keys[i].After(keys[j])
		})
	}
	return keys
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

	// Result should be sorted in ascending order
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
	keys := b.sortedBucketsKeys(true)
	for _, key := range keys {
		bucket, exists := b.bucketsAt(key.ToTime())
		if !exists || !bucket.start.Before(end) ||
			!start.Before(bucket.start.Add(blockSize)) {
			continue
		}

		size := int64(bucket.streamsLen())
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
			resultLastRead = bucket.lastRead()
		}
		// NB(r): Ignore if opts.IncludeChecksum because we avoid
		// calculating checksum since block is open and is being mutated
		res.Add(block.FetchBlockMetadataResult{
			Start:    bucket.start,
			Size:     resultSize,
			LastRead: resultLastRead,
		})
	}

	return res
}

func (b *dbBuffer) newBucketsAt(
	t time.Time,
) *dbBufferBuckets {
	buckets := b.bucketsPool.Get()
	buckets.resetTo(t, b.opts, b.bucketPool)
	b.bucketsMap[xtime.ToUnixNano(t)] = buckets
	return buckets
}

func (b *dbBuffer) bucketsAt(
	t time.Time,
) (*dbBufferBuckets, bool) {
	// First check LRU cache
	for _, buckets := range b.bucketsCache {
		if buckets == nil {
			continue
		}
		if buckets.start.Equal(t) {
			return buckets, true
		}
	}

	// Then check the map
	if buckets, exists := b.bucketsMap[xtime.ToUnixNano(t)]; exists {
		return buckets, true
	}

	return nil, false
}

func (b *dbBuffer) bucketsAtCreate(
	t time.Time,
) *dbBufferBuckets {
	if buckets, exists := b.bucketsAt(t); exists {
		return buckets
	}

	return b.newBucketsAt(t)
}

func (b *dbBuffer) putBucketInCache(newBuckets *dbBufferBuckets) {
	replaceIdx := bucketsCacheSize - 1
	for i, buckets := range b.bucketsCache {
		// Check if we have the same pointer in cache
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

	b.bucketsPool.Put(buckets)
	delete(b.bucketsMap, xtime.ToUnixNano(blockStart))
}

type dbBufferBuckets struct {
	buckets           []*dbBufferBucket
	start             time.Time
	opts              Options
	lastReadUnixNanos int64
	bucketPool        *dbBufferBucketPool
}

func (b *dbBufferBuckets) resetTo(
	start time.Time,
	opts Options,
	bucketPool *dbBufferBucketPool,
) {
	// nil all elements so that they get GC'd
	for i := range b.buckets {
		b.buckets[i] = nil
	}
	b.buckets = b.buckets[:0]
	b.start = start
	b.opts = opts
	atomic.StoreInt64(&b.lastReadUnixNanos, 0)
	b.bucketPool = bucketPool
}

func (b *dbBufferBuckets) streams(ctx context.Context, wType WriteType) []xio.BlockReader {
	var res []xio.BlockReader
	for _, bucket := range b.buckets {
		if bucket.wType == wType {
			res = append(res, bucket.streams(ctx)...)
		}
	}

	return res
}

func (b *dbBufferBuckets) streamsLen() int {
	res := 0
	for _, bucket := range b.buckets {
		res += bucket.streamsLen()
	}
	return res
}

func (b *dbBufferBuckets) write(
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
	wType WriteType,
) error {
	return b.writableBucketCreate(wType).write(timestamp, value, unit, annotation)
}

func (b *dbBufferBuckets) merge(wType WriteType) (mergeResult, error) {
	var res mergeResult
	for _, bucket := range b.buckets {
		// Only makes sense to merge buckets that are writable
		if bucket.version == writableBucketVer && wType == bucket.wType {
			mergeRes, err := bucket.merge()
			if err != nil {
				return res, nil
			}
			res.merges += mergeRes.merges
		}
	}

	return res, nil
}

func (b *dbBufferBuckets) setLastRead(value time.Time) {
	atomic.StoreInt64(&b.lastReadUnixNanos, value.UnixNano())
}

func (b *dbBufferBuckets) lastRead() time.Time {
	return time.Unix(0, atomic.LoadInt64(&b.lastReadUnixNanos))
}

func (b *dbBufferBuckets) bootstrap(bl block.DatabaseBlock) {
	// TODO(juchan): what is a "cold" bootstrap?
	b.writableBucketCreate(WarmWrite).addBlock(bl)
}

func (b *dbBufferBuckets) writableBucket(wType WriteType) (*dbBufferBucket, bool) {
	for _, bucket := range b.buckets {
		if bucket.version == writableBucketVer && bucket.wType == wType {
			return bucket, true
		}
	}

	return nil, false
}

func (b *dbBufferBuckets) writableBucketCreate(wType WriteType) *dbBufferBucket {
	bucket, exists := b.writableBucket(wType)

	if exists {
		return bucket
	}

	return b.newBucketAt(b.start, wType)
}

func (b *dbBufferBuckets) newBucketAt(
	t time.Time,
	wType WriteType,
) *dbBufferBucket {
	bucket := b.bucketPool.Get()
	bucket.resetTo(t, wType, b.opts)
	b.buckets = append([]*dbBufferBucket{bucket}, b.buckets...)
	return bucket
}

func (b *dbBufferBuckets) toBlock(wType WriteType) (block.DatabaseBlock, error) {
	bucket, exists := b.writableBucket(wType)
	if !exists {
		return nil, nil
	}

	return bucket.toBlock()
}

type dbBufferBucket struct {
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

func (b *dbBufferBucket) resetTo(
	start time.Time,
	wType WriteType,
	opts Options,
) {
	// Close the old context if we're resetting for use
	b.finalize()
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

func (b *dbBufferBucket) finalize() {
	b.resetEncoders()
	b.resetBlocks()
}

func (b *dbBufferBucket) addBlock(
	bl block.DatabaseBlock,
) {
	b.blocks = append(b.blocks, bl)
}

func (b *dbBufferBucket) write(
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

func (b *dbBufferBucket) writeToEncoderIndex(
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

func (b *dbBufferBucket) streams(ctx context.Context) []xio.BlockReader {
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

func (b *dbBufferBucket) streamsLen() int {
	length := 0
	for i := range b.blocks {
		length += b.blocks[i].Len()
	}
	for i := range b.encoders {
		length += b.encoders[i].encoder.Len()
	}
	return length
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

func (b *dbBufferBucket) resetBlocks() {
	for i := range b.blocks {
		bl := b.blocks[i]
		bl.Close()
	}
	b.blocks = nil
}

func (b *dbBufferBucket) needsMerge() bool {
	return !(b.hasJustSingleEncoder() || b.hasJustSingleBootstrappedBlock())
}

func (b *dbBufferBucket) hasJustSingleEncoder() bool {
	return len(b.encoders) == 1 && len(b.blocks) == 0
}

func (b *dbBufferBucket) hasJustSingleBootstrappedBlock() bool {
	encodersEmpty := len(b.encoders) == 0 ||
		(len(b.encoders) == 1 && b.encoders[0].encoder.Len() == 0)
	return encodersEmpty && len(b.blocks) == 1
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
			return mergeResult{}, err
		}
		lastWriteAt = dp.Timestamp
	}
	if err := iter.Err(); err != nil {
		return mergeResult{}, err
	}

	b.resetEncoders()
	b.resetBlocks()

	b.encoders = append(b.encoders, inOrderEncoder{
		encoder:     encoder,
		lastWriteAt: lastWriteAt,
	})

	return mergeResult{merges: merges}, nil
}

func (b *dbBufferBucket) toBlock() (block.DatabaseBlock, error) {
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

type dbBufferBucketsPool struct {
	pool pool.ObjectPool
}

// newDBBufferBucketsPool creates a new dbBufferBucketsPool
func newDBBufferBucketsPool(opts pool.ObjectPoolOptions) *dbBufferBucketsPool {
	p := &dbBufferBucketsPool{pool: pool.NewObjectPool(opts)}
	p.pool.Init(func() interface{} {
		return &dbBufferBuckets{}
	})
	return p
}

func (p *dbBufferBucketsPool) Get() *dbBufferBuckets {
	return p.pool.Get().(*dbBufferBuckets)
}

func (p *dbBufferBucketsPool) Put(buckets *dbBufferBuckets) {
	p.pool.Put(buckets)
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
	p.pool.Put(bucket)
}
