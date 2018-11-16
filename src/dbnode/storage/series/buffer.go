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
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"
)

var (
	errInvalidMetricType = errors.New("invalid metric type for context")

	timeZero time.Time
)

const (
	cacheSize = 2

	defaultBucketContainerPoolSize = 2

	numMetricTypes = 2
)

type metricType int

const (
	realtimeType metricType = iota
	outOfOrderType
	allMetricTypes
)

type databaseBuffer interface {
	Write(
		ctx context.Context,
		timestamp time.Time,
		value float64,
		unit xtime.Unit,
		annotation []byte,
	) error

	Snapshot(
		ctx context.Context,
		mType metricType,
		blockStart time.Time,
	) (xio.SegmentReader, error)

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

	Flush(
		ctx context.Context,
		blockStart time.Time,
		id ident.ID,
		tags ident.Tags,
		persistFn persist.DataFn,
	) (FlushOutcome, error)

	Reset(blockRetriever QueryableBlockRetriever, opts Options)
}

type bufferStats struct {
	wiredBlocks int
}

type bufferTickResult struct {
	mergedOutOfOrderBlocks int
	bucketsRemoved         []time.Time
}

type dbBuffer struct {
	opts           Options
	nowFn          clock.NowFn
	blockRetriever QueryableBlockRetriever

	buckets     map[xtime.UnixNano]*dbBufferBucket
	bucketCache [cacheSize]*dbBufferBucket
	bucketPool  *dbBufferBucketPool

	blockSize    time.Duration
	bufferPast   time.Duration
	bufferFuture time.Duration
}

// NB(prateek): databaseBuffer.Reset(...) must be called upon the returned
// object prior to use.
func newDatabaseBuffer() databaseBuffer {
	b := &dbBuffer{
		buckets: make(map[xtime.UnixNano]*dbBufferBucket),
	}
	return b
}

func (b *dbBuffer) Reset(blockRetriever QueryableBlockRetriever, opts Options) {
	b.opts = opts
	b.nowFn = opts.ClockOptions().NowFn()
	b.blockRetriever = blockRetriever
	bucketPoolOpts := pool.NewObjectPoolOptions().SetSize(defaultBucketContainerPoolSize)
	b.bucketPool = newDBBufferBucketPool(bucketPoolOpts)
	ropts := opts.RetentionOptions()
	b.blockSize = ropts.BlockSize()
	b.bufferPast = ropts.BufferPast()
	b.bufferFuture = ropts.BufferFuture()
}

func (b *dbBuffer) Write(
	ctx context.Context,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	mType := b.isRealtime(b.nowFn(), timestamp)

	blockStart := timestamp.Truncate(b.blockSize)
	bucket, ok := b.bucketAt(blockStart)
	if !ok {
		bucket = b.newBucketAt(blockStart)
	}
	b.putBucketInCache(bucket)
	return bucket.write(mType, timestamp, value, unit, annotation)
}

func (b *dbBuffer) IsEmpty() bool {
	for _, bucket := range b.buckets {
		if !bucket.isEmpty() {
			return false
		}
	}
	return true
}

func (b *dbBuffer) Stats() bufferStats {
	var stats bufferStats
	for _, bucket := range b.buckets {
		if bucket.isEmpty() {
			continue
		}

		stats.wiredBlocks++
	}
	return stats
}

func (b *dbBuffer) Tick() bufferTickResult {
	var (
		res       bufferTickResult
		retriever = b.blockRetriever
	)

	for startNano, bucket := range b.buckets {
		start := startNano.ToTime()
		// We can only remove the bucket from the map if:
		// 1) This bucket is empty
		// 2) The blockStart is retrievable
		// 3) the last success is after the last persist. This is necessary
		//    because there may have been an existing file on disk from a previous
		//    flush.
		if bucket.isEmpty() && retriever.IsBlockRetrievable(start) &&
			retriever.BlockLastSuccess(start).After(bucket.lastPersist()) {
			b.removeBucketAt(start)
			res.bucketsRemoved = append(res.bucketsRemoved, start)
			continue
		}

		for mType := 0; mType < numMetricTypes; mType++ {
			r, err := bucket.merge(metricType(mType))
			if err != nil {
				log := b.opts.InstrumentOptions().Logger()
				log.Errorf("buffer merge encode error: %v", err)
			}
			if r.merges > 0 {
				res.mergedOutOfOrderBlocks++
			}
		}
	}

	return res
}

func (b *dbBuffer) Bootstrap(bl block.DatabaseBlock) {
	blockStart := bl.StartTime()
	bucket, ok := b.bucketAt(blockStart)
	if !ok {
		bucket = b.newBucketAt(blockStart)
	}

	// TODO(juchan): what is a "realtime" bootstrap vs "out of order" bootstrap?
	bucket.bootstrap(bl, realtimeType)
	// min, max := b.minMaxRealtimeBlockStarts(b.nowFn())
	// isRealtime := !blockStart.Before(min) && !blockStart.After(max)
	// if isRealtime {
	// 	bucket.bootstrap(bl, realtimeType)
	// } else {
	// 	bucket.bootstrap(bl, outOfOrderType)
	// }
}

// func (b *dbBuffer) minMaxRealtimeBlockStarts(now time.Time) (time.Time, time.Time) {
// 	min := now.Add(-b.bufferPast).Truncate(b.blockSize)
// 	max := now.Add(b.bufferFuture).Truncate(b.blockSize)
// 	return min, max
// }

func (b *dbBuffer) Snapshot(
	ctx context.Context,
	mType metricType,
	blockStart time.Time,
) (xio.SegmentReader, error) {
	if mType == allMetricTypes {
		return nil, errInvalidMetricType
	}

	if bucket, ok := b.bucketAt(blockStart); ok {
		return bucket.stream(ctx, mType)
	}

	// Not having data for a bucket is not an error
	return nil, nil
}

func (b *dbBuffer) ReadEncoded(ctx context.Context, start, end time.Time) [][]xio.BlockReader {
	// TODO(r): pool these results arrays
	var res [][]xio.BlockReader

	keys := b.sortedBucketKeys(true)
	for _, key := range keys {
		bucket := b.buckets[key]
		if bucket.isEmpty() || !start.Before(bucket.start.Add(b.blockSize)) ||
			!bucket.start.Before(end) {
			continue
		}

		res = append(res, bucket.streams(ctx, allMetricTypes))

		// NB(r): Store the last read time, should not set this when
		// calling FetchBlocks as a read is differentiated from
		// a FetchBlocks call. One is initiated by an external
		// entity and the other is used for streaming blocks between
		// the storage nodes. This distinction is important as this
		// data is important for use with understanding access patterns, etc.
		bucket.setLastRead(b.nowFn())
	}

	return res
}

func (b *dbBuffer) FetchBlocks(ctx context.Context, starts []time.Time) []block.FetchBlockResult {
	var res []block.FetchBlockResult

	for _, start := range starts {
		bucket, ok := b.bucketAt(start)
		if !ok {
			continue
		}

		streams := bucket.streams(ctx, allMetricTypes)
		res = append(res, block.NewFetchBlockResult(bucket.start, streams, nil))
	}

	return res
}

func (b *dbBuffer) FetchBlocksMetadata(
	ctx context.Context,
	start, end time.Time,
	opts FetchBlocksMetadataOptions,
) block.FetchBlockMetadataResults {
	blockSize := b.opts.RetentionOptions().BlockSize()
	res := b.opts.FetchBlockMetadataResultsPool().Get()

	keys := b.sortedBucketKeys(true)
	for _, key := range keys {
		bucket := b.buckets[key]
		if bucket.isEmpty() || !start.Before(bucket.start.Add(blockSize)) ||
			!bucket.start.Before(end) {
			continue
		}
		size := int64(bucket.streamsLen())
		// If we have no data in this bucket, return early without appending it to the result.
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

func (b *dbBuffer) newBucketAt(t time.Time) *dbBufferBucket {
	bucket := b.bucketPool.Get()
	bucket.resetTo(t, b.opts)
	b.buckets[xtime.ToUnixNano(t)] = bucket
	return bucket
}

func (b *dbBuffer) bucketAt(t time.Time) (*dbBufferBucket, bool) {
	// First check LRU cache
	for _, bucket := range b.buckets {
		if bucket == nil {
			continue
		}

		if bucket.start.Equal(t) {
			return bucket, true
		}
	}

	// Then check the map
	if bg, ok := b.buckets[xtime.ToUnixNano(t)]; ok {
		return bg, true
	}

	return nil, false
}

func (b *dbBuffer) putBucketInCache(bg *dbBufferBucket) {
	b.bucketCache[b.lruBucketIdxInCache()] = bg
}

func (b *dbBuffer) lruBucketIdxInCache() int {
	idx := -1
	var lastReadTime time.Time

	for i, bucket := range b.bucketCache {
		if bucket == nil {
			// An empty slot in the cache is older than any existing bucket
			return i
		}

		curLastRead := bucket.lastRead()
		if idx == -1 || curLastRead.Before(lastReadTime) {
			lastReadTime = curLastRead
			idx = i
		}
	}

	return idx
}

func (b *dbBuffer) removeBucketAt(blockStart time.Time) {
	tNano := xtime.ToUnixNano(blockStart)
	bucket, exists := b.bucketAt(blockStart)
	if !exists {
		return
	}

	bucket.resetEncoders(allMetricTypes)
	bucket.resetBootstrapped(allMetricTypes)

	b.bucketPool.Put(bucket)
	delete(b.buckets, tNano)
}

func (b *dbBuffer) isRealtime(now time.Time, timestamp time.Time) metricType {
	futureLimit := now.Add(1 * b.bufferFuture)
	pastLimit := now.Add(-1 * b.bufferPast)
	isRealtime := pastLimit.Before(timestamp) && futureLimit.After(timestamp)

	if isRealtime {
		return realtimeType
	}

	return outOfOrderType
}

func (b *dbBuffer) Flush(
	ctx context.Context,
	blockStart time.Time,
	id ident.ID,
	tags ident.Tags,
	persistFn persist.DataFn,
) (FlushOutcome, error) {
	bucket, exists := b.bucketAt(blockStart)
	if !exists {
		return FlushOutcomeBlockDoesNotExist, nil
	}
	// By virtue of calling this function, we know we are only interested in
	// realtime writes. Out of order writes are merged and written directly
	// by the compactor.
	block, err := bucket.discardMerged(realtimeType)
	if err != nil {
		return FlushOutcomeErr, err
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

	bucket.setLastPersist(b.nowFn())

	return FlushOutcomeFlushedToDisk, nil
}

func (b *dbBuffer) sortedBucketKeys(ascending bool) []xtime.UnixNano {
	buckets := b.buckets
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

type dbBufferBucket struct {
	opts                 Options
	start                time.Time
	encoders             [numMetricTypes][]inOrderEncoder
	bootstrapped         [numMetricTypes][]block.DatabaseBlock
	lastReadUnixNanos    int64
	lastPersistUnixNanos int64
}

type inOrderEncoder struct {
	encoder     encoding.Encoder
	lastWriteAt time.Time
}

func (b *dbBufferBucket) resetTo(
	start time.Time,
	opts Options,
) {
	// Close the old context if we're resetting for use
	b.finalize()

	b.opts = opts
	bopts := b.opts.DatabaseBlockOptions()
	b.start = start

	for i := 0; i < numMetricTypes; i++ {
		encoder := bopts.EncoderPool().Get()
		encoder.Reset(start, bopts.DatabaseBlockAllocSize())

		b.encoders[i] = append(b.encoders[i], inOrderEncoder{
			encoder: encoder,
		})
		b.bootstrapped[i] = nil
	}

	atomic.StoreInt64(&b.lastReadUnixNanos, 0)
}

func (b *dbBufferBucket) finalize() {
	b.resetEncoders(allMetricTypes)
	b.resetBootstrapped(allMetricTypes)
}

func (b *dbBufferBucket) isEmpty() bool {
	for i := 0; i < numMetricTypes; i++ {
		for _, block := range b.bootstrapped[i] {
			if block.Len() > 0 {
				return false
			}
		}
		for _, elem := range b.encoders[i] {
			if elem.encoder != nil && elem.encoder.NumEncoded() > 0 {
				return false
			}
		}
	}
	return true
}

func (b *dbBufferBucket) bootstrap(
	bl block.DatabaseBlock,
	mType metricType,
) {
	b.bootstrapped[mType] = append(b.bootstrapped[mType], bl)
}

func (b *dbBufferBucket) write(
	mType metricType,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	if mType == allMetricTypes {
		return errInvalidMetricType
	}

	datapoint := ts.Datapoint{
		Timestamp: timestamp,
		Value:     value,
	}

	// Find the correct encoder to write to
	idx := -1

	for i := range b.encoders[mType] {
		lastWriteAt := b.encoders[mType][i].lastWriteAt
		if timestamp.Equal(lastWriteAt) {
			last, err := b.encoders[mType][i].encoder.LastEncoded()
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
		return b.writeToEncoderIndex(mType, idx, datapoint, unit, annotation)
	}

	// Need a new encoder, we didn't find an encoder to write to
	b.opts.Stats().IncCreatedEncoders()
	bopts := b.opts.DatabaseBlockOptions()
	blockSize := b.opts.RetentionOptions().BlockSize()
	blockAllocSize := bopts.DatabaseBlockAllocSize()

	encoder := bopts.EncoderPool().Get()
	encoder.Reset(timestamp.Truncate(blockSize), blockAllocSize)

	b.encoders[mType] = append(b.encoders[mType], inOrderEncoder{
		encoder:     encoder,
		lastWriteAt: timestamp,
	})
	idx = len(b.encoders[mType]) - 1
	err := b.writeToEncoderIndex(mType, idx, datapoint, unit, annotation)
	if err != nil {
		encoder.Close()
		b.encoders[mType] = b.encoders[mType][:idx]
		return err
	}
	return nil
}

func (b *dbBufferBucket) writeToEncoderIndex(
	mType metricType,
	idx int,
	datapoint ts.Datapoint,
	unit xtime.Unit,
	annotation []byte,
) error {
	if mType == allMetricTypes {
		return errInvalidMetricType
	}

	err := b.encoders[mType][idx].encoder.Encode(datapoint, unit, annotation)
	if err != nil {
		return err
	}

	b.encoders[mType][idx].lastWriteAt = datapoint.Timestamp
	return nil
}

func (b *dbBufferBucket) streams(ctx context.Context, mType metricType) []xio.BlockReader {
	streamsCap := 0
	for mt := 0; mt < numMetricTypes; mt++ {
		if mType != metricType(mt) && mType != allMetricTypes {
			continue
		}

		streamsCap += len(b.bootstrapped[mt])
		streamsCap += len(b.encoders[mt])
	}

	streams := make([]xio.BlockReader, 0, streamsCap)

	for mt := 0; mt < numMetricTypes; mt++ {
		if mType != metricType(mt) && mType != allMetricTypes {
			continue
		}

		for i := range b.bootstrapped[mt] {
			if b.bootstrapped[mt][i].Len() == 0 {
				continue
			}
			if s, err := b.bootstrapped[mt][i].Stream(ctx); err == nil && s.IsNotEmpty() {
				// NB(r): block stream method will register the stream closer already
				streams = append(streams, s)
			}
		}
		for i := range b.encoders[mt] {
			start := b.start
			if s := b.encoders[mt][i].encoder.Stream(); s != nil {
				br := xio.BlockReader{
					SegmentReader: s,
					Start:         start,
					BlockSize:     b.opts.RetentionOptions().BlockSize(),
				}
				ctx.RegisterFinalizer(s)
				streams = append(streams, br)
			}
		}
	}

	return streams
}

func (b *dbBufferBucket) streamsLen() int {
	length := 0

	for mt := 0; mt < numMetricTypes; mt++ {
		for i := range b.bootstrapped[mt] {
			length += b.bootstrapped[mt][i].Len()
		}
		for i := range b.encoders[mt] {
			length += b.encoders[mt][i].encoder.Len()
		}
	}
	return length
}

func (b *dbBufferBucket) setLastRead(value time.Time) {
	atomic.StoreInt64(&b.lastReadUnixNanos, value.UnixNano())
}

func (b *dbBufferBucket) lastRead() time.Time {
	return time.Unix(0, atomic.LoadInt64(&b.lastReadUnixNanos))
}

func (b *dbBufferBucket) setLastPersist(value time.Time) {
	atomic.StoreInt64(&b.lastPersistUnixNanos, value.UnixNano())
}

func (b *dbBufferBucket) lastPersist() time.Time {
	return time.Unix(0, atomic.LoadInt64(&b.lastPersistUnixNanos))
}

func (b *dbBufferBucket) resetEncoders(mt metricType) {
	var zeroed inOrderEncoder
	for mType := 0; mType < numMetricTypes; mType++ {
		if mt != metricType(mType) && mt != allMetricTypes {
			continue
		}

		for i := range b.encoders[mType] {
			// Register when this bucket resets we close the encoder
			encoder := b.encoders[mType][i].encoder
			encoder.Close()
			b.encoders[mType][i] = zeroed
		}
		b.encoders[mType] = b.encoders[mType][:0]
	}
}

func (b *dbBufferBucket) resetBootstrapped(mt metricType) {
	for mType := 0; mType < numMetricTypes; mType++ {
		if mt != metricType(mType) && mt != allMetricTypes {
			continue
		}

		for i := range b.bootstrapped[mType] {
			bl := b.bootstrapped[mType][i]
			bl.Close()
		}
		b.bootstrapped[mType] = nil
	}
}

type mergeResult struct {
	merges int
}

func (b *dbBufferBucket) merge(mType metricType) (mergeResult, error) {
	if !b.needsMerge(mType) {
		// Save unnecessary work
		return mergeResult{}, nil
	}

	merges := 0
	start := b.start
	bopts := b.opts.DatabaseBlockOptions()

	var (
		readers = make([]xio.SegmentReader, 0, len(b.encoders[mType])+len(b.bootstrapped[mType]))
		streams = make([]xio.SegmentReader, 0, len(b.encoders[mType]))
		iter    = b.opts.MultiReaderIteratorPool().Get()
		ctx     = b.opts.ContextPool().Get()
		encoder = bopts.EncoderPool().Get()
	)
	encoder.Reset(start, bopts.DatabaseBlockAllocSize())
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
	for i := range b.bootstrapped[mType] {
		block, err := b.bootstrapped[mType][i].Stream(ctx)
		if err == nil && block.SegmentReader != nil {
			merges++
			readers = append(readers, block.SegmentReader)
		}
	}

	for i := range b.encoders[mType] {
		if s := b.encoders[mType][i].encoder.Stream(); s != nil {
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

	b.resetEncoders(metricType(mType))
	b.resetBootstrapped(metricType(mType))

	b.encoders[mType] = append(b.encoders[mType], inOrderEncoder{
		encoder:     encoder,
		lastWriteAt: lastWriteAt,
	})

	return mergeResult{merges: merges}, nil
}

func (b *dbBufferBucket) needsMerge(mType metricType) bool {
	return !(b.hasJustSingleEncoder(mType) || b.hasJustSingleBootstrappedBlock(mType))
}

func (b *dbBufferBucket) hasJustSingleEncoder(mType metricType) bool {
	return len(b.encoders[mType]) == 1 && len(b.bootstrapped[mType]) == 0
}

func (b *dbBufferBucket) encodersEmpty(mType metricType) bool {
	return len(b.encoders[mType]) == 0 ||
		(len(b.encoders[mType]) == 1 &&
			b.encoders[mType][0].encoder.Len() == 0)
}

func (b *dbBufferBucket) hasJustSingleBootstrappedBlock(mType metricType) bool {
	return b.encodersEmpty(mType) && len(b.bootstrapped[mType]) == 1
}

func (b *dbBufferBucket) discardMerged(mType metricType) (block.DatabaseBlock, error) {
	if b.hasJustSingleEncoder(mType) {
		// Already merged as a single encoder
		encoder := b.encoders[mType][0].encoder
		newBlock := b.opts.DatabaseBlockOptions().DatabaseBlockPool().Get()
		blockSize := b.opts.RetentionOptions().BlockSize()
		newBlock.Reset(b.start, blockSize, encoder.Discard())

		// The single encoder is already discarded, no need to call resetEncoders
		// just remove it from the list of encoders
		b.encoders[mType] = b.encoders[mType][:0]
		b.resetBootstrapped(mType)

		return newBlock, nil
	}

	if b.hasJustSingleBootstrappedBlock(mType) {
		// Already merged just a single bootstrapped block
		existingBlock := b.bootstrapped[mType][0]

		// Need to reset encoders but do not want to finalize the block as we
		// are passing ownership of it to the caller
		b.resetEncoders(mType)
		b.bootstrapped[mType] = nil

		return existingBlock, nil
	}

	_, err := b.merge(mType)
	if err != nil {
		b.resetEncoders(allMetricTypes)
		b.resetBootstrapped(allMetricTypes)
		return nil, err
	}

	mergedEncoder := b.encoders[mType][0].encoder
	newBlock := b.opts.DatabaseBlockOptions().DatabaseBlockPool().Get()
	blockSize := b.opts.RetentionOptions().BlockSize()
	newBlock.Reset(b.start, blockSize, mergedEncoder.Discard())
	if lastRead := b.lastRead(); !lastRead.IsZero() {
		newBlock.SetLastReadTime(lastRead)
	}

	// The merged encoder is already discarded, no need to call resetEncoders
	// just remove it from the list of encoders
	b.encoders[mType] = b.encoders[mType][:0]
	b.resetBootstrapped(mType)

	return newBlock, nil
}

func (b *dbBufferBucket) stream(ctx context.Context, mType metricType) (xio.BlockReader, error) {
	if mType == allMetricTypes {
		return xio.EmptyBlockReader, errInvalidMetricType
	}

	if b.isEmpty() {
		return xio.EmptyBlockReader, nil
	}

	// We need to merge all the bootstrapped blocks / encoders into a single stream for
	// the sake of being able to persist it to disk as a single encoded stream.
	_, err := b.merge(mType)
	if err != nil {
		return xio.EmptyBlockReader, err
	}

	if s := b.encoders[mType][0].encoder.Stream(); s != nil {
		br := xio.BlockReader{
			SegmentReader: s,
			Start:         b.start,
			BlockSize:     b.opts.RetentionOptions().BlockSize(),
		}
		ctx.RegisterFinalizer(s)
		return br, nil
	}

	return xio.EmptyBlockReader, nil
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
