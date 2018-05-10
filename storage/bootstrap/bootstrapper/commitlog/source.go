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

package commitlog

import (
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/persist/fs/commitlog"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/storage/index/convert"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3db/x/xio"
	"github.com/m3db/m3x/ident"
	xlog "github.com/m3db/m3x/log"
	xsync "github.com/m3db/m3x/sync"
	xtime "github.com/m3db/m3x/time"
)

const encoderChanBufSize = 1000

type newIteratorFn func(opts commitlog.IteratorOpts) (commitlog.Iterator, error)

type commitLogSource struct {
	opts            Options
	inspection      fs.Inspection
	log             xlog.Logger
	newIteratorFn   newIteratorFn
	cachedShardData []shardData
}

type encoder struct {
	lastWriteAt time.Time
	enc         encoding.Encoder
}

func newCommitLogSource(opts Options, inspection fs.Inspection) bootstrap.Source {
	return &commitLogSource{
		opts:          opts,
		inspection:    inspection,
		log:           opts.ResultOptions().InstrumentOptions().Logger(),
		newIteratorFn: commitlog.NewIterator,
	}
}

func (s *commitLogSource) Can(strategy bootstrap.Strategy) bool {
	switch strategy {
	case bootstrap.BootstrapSequential:
		return true
	}
	return false
}

func (s *commitLogSource) AvailableData(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
) result.ShardTimeRanges {
	// Commit log bootstrapper is a last ditch effort, so fulfill all
	// time ranges requested even if not enough data, just to succeed
	// the bootstrap
	return shardsTimeRanges
}

func (s *commitLogSource) ReadData(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	_ bootstrap.RunOptions,
) (result.DataBootstrapResult, error) {
	if shardsTimeRanges.IsEmpty() {
		return result.NewDataBootstrapResult(), nil
	}

	readCommitLogPredicate := newReadCommitLogPredicate(
		ns, shardsTimeRanges, s.opts, s.inspection)
	readSeriesPredicate := newReadSeriesPredicate(ns)
	iterOpts := commitlog.IteratorOpts{
		CommitLogOptions:      s.opts.CommitLogOptions(),
		FileFilterPredicate:   readCommitLogPredicate,
		SeriesFilterPredicate: readSeriesPredicate,
	}
	iter, err := s.newIteratorFn(iterOpts)
	if err != nil {
		return nil, fmt.Errorf("unable to create commit log iterator: %v", err)
	}

	defer iter.Close()

	var (
		// +1 so we can use the shard number as an index throughout without constantly
		// remembering to subtract 1 to convert to zero-based indexing
		numShards   = s.findHighestShard(shardsTimeRanges) + 1
		numConc     = s.opts.EncodingConcurrency()
		bopts       = s.opts.ResultOptions()
		blopts      = bopts.DatabaseBlockOptions()
		blockSize   = ns.Options().RetentionOptions().BlockSize()
		encoderPool = bopts.DatabaseBlockOptions().EncoderPool()
		workerErrs  = make([]int, numConc)
	)

	unmerged := make([]shardData, numShards)
	for shard := range shardsTimeRanges {
		unmerged[shard] = shardData{
			encodersBySeries: make(map[uint64]encodersByTime),
			ranges:           shardsTimeRanges[shard],
		}
	}

	encoderChans := make([]chan encoderArg, numConc)
	for i := 0; i < numConc; i++ {
		encoderChans[i] = make(chan encoderArg, encoderChanBufSize)
	}

	// Spin up numConc background go-routines to handle M3TSZ encoding. This must
	// happen before we start reading to prevent infinitely blocking writes to
	// the encoderChans.
	wg := &sync.WaitGroup{}
	for workerNum, encoderChan := range encoderChans {
		wg.Add(1)
		go s.startM3TSZEncodingWorker(
			workerNum,
			encoderChan,
			unmerged,
			encoderPool,
			workerErrs,
			blopts,
			wg,
		)
	}

	for iter.Next() {
		series, dp, unit, annotation := iter.Current()
		if !s.shouldEncodeForData(unmerged, blockSize, series, dp.Timestamp) {
			continue
		}

		// Distribute work such that each encoder goroutine is responsible for
		// approximately numShards / numConc shards. This also means that all
		// datapoints for a given shard/series will be processed in a serialized
		// manner.
		// We choose to distribute work by shard instead of series.UniqueIndex
		// because it means that all accesses to the unmerged slice don't need
		// to be synchronized because each index belongs to a single shard so it
		// will only be accessed serially from a single worker routine.
		encoderChans[series.Shard%uint32(numConc)] <- encoderArg{
			series:     series,
			dp:         dp,
			unit:       unit,
			annotation: annotation,
			blockStart: dp.Timestamp.Truncate(blockSize),
		}
	}

	for _, encoderChan := range encoderChans {
		close(encoderChan)
	}

	// Block until all data has been read and encoded by the worker goroutines
	wg.Wait()
	s.logEncodingOutcome(workerErrs, iter)

	result := s.mergeShards(int(numShards), bopts, blockSize, blopts, encoderPool, unmerged)
	// After mergingShards, its safe to cache the shardData (which involves some mutation).
	if s.opts.ShouldCacheSeriesMetadata() {
		s.cacheShardData(unmerged)
	}
	return result, nil
}

func (s *commitLogSource) startM3TSZEncodingWorker(
	workerNum int,
	ec <-chan encoderArg,
	unmerged []shardData,
	encoderPool encoding.EncoderPool,
	workerErrs []int,
	blopts block.Options,
	wg *sync.WaitGroup,
) {
	for arg := range ec {
		var (
			series     = arg.series
			dp         = arg.dp
			unit       = arg.unit
			annotation = arg.annotation
			blockStart = arg.blockStart
		)

		unmergedShard := unmerged[series.Shard].encodersBySeries
		unmergedSeries, ok := unmergedShard[series.UniqueIndex]
		if !ok {
			unmergedSeries = encodersByTime{
				id:       series.ID,
				tags:     series.Tags,
				encoders: make(map[xtime.UnixNano]encoders)}
			unmergedShard[series.UniqueIndex] = unmergedSeries
		}

		var (
			err            error
			blockStartNano = xtime.ToUnixNano(blockStart)
			unmergedBlock  = unmergedSeries.encoders[blockStartNano]
			wroteExisting  = false
		)
		for i := range unmergedBlock {
			if unmergedBlock[i].lastWriteAt.Before(dp.Timestamp) {
				unmergedBlock[i].lastWriteAt = dp.Timestamp
				err = unmergedBlock[i].enc.Encode(dp, unit, annotation)
				wroteExisting = true
				break
			}
		}
		if !wroteExisting {
			enc := encoderPool.Get()
			enc.Reset(blockStart, blopts.DatabaseBlockAllocSize())

			err = enc.Encode(dp, unit, annotation)
			if err == nil {
				unmergedBlock = append(unmergedBlock, encoder{
					lastWriteAt: dp.Timestamp,
					enc:         enc,
				})
				unmergedSeries.encoders[blockStartNano] = unmergedBlock
			}
		}
		if err != nil {
			workerErrs[workerNum]++
		}
	}
	wg.Done()
}

func (s *commitLogSource) shouldEncodeForData(
	unmerged []shardData,
	dataBlockSize time.Duration,
	series commitlog.Series,
	timestamp time.Time,
) bool {
	// Check if the shard number is higher the amount of space we pre-allocated.
	// If it is, then it's not one of the shards we're trying to bootstrap
	if series.Shard > uint32(len(unmerged)-1) {
		return false
	}

	// Check if the shard is one of the shards we're trying to bootstrap
	ranges := unmerged[series.Shard].ranges
	if ranges.IsEmpty() {
		// Did not allocate map for this shard so not expecting data for it
		return false
	}

	// Check if the block corresponds to the time-range that we're trying to bootstrap
	blockStart := timestamp.Truncate(dataBlockSize)
	blockEnd := blockStart.Add(dataBlockSize)
	blockRange := xtime.Range{
		Start: blockStart,
		End:   blockEnd,
	}

	return ranges.Overlaps(blockRange)
}

func (s *commitLogSource) shouldIncludeInIndex(
	shard uint32,
	ts time.Time,
	highestShard uint32,
	indexBlockSize time.Duration,
	bootstrapRangesByShard []xtime.Ranges,
) bool {
	if shard > highestShard {
		// Not trying to bootstrap this shard
		return false
	}

	rangesToBootstrap := bootstrapRangesByShard[shard]
	if rangesToBootstrap.IsEmpty() {
		// No ShardTimeRanges were provided for this shard, so we're not
		// bootstrapping it.
		return false
	}

	// Check if the timestamp corresponds to one of the index blocks we're
	// trying to bootstrap.
	indexBlockStart := ts.Truncate(indexBlockSize)
	indexBlockEnd := indexBlockStart.Add(indexBlockSize)
	indexBlockRange := xtime.Range{
		Start: indexBlockStart,
		End:   indexBlockEnd,
	}

	return rangesToBootstrap.Overlaps(indexBlockRange)
}

func (s *commitLogSource) mergeShards(
	numShards int,
	bopts result.Options,
	blockSize time.Duration,
	blopts block.Options,
	encoderPool encoding.EncoderPool,
	unmerged []shardData,
) result.DataBootstrapResult {
	var (
		shardErrs               = make([]int, numShards)
		shardEmptyErrs          = make([]int, numShards)
		bootstrapResult         = result.NewDataBootstrapResult()
		blocksPool              = bopts.DatabaseBlockOptions().DatabaseBlockPool()
		multiReaderIteratorPool = blopts.MultiReaderIteratorPool()
		// Controls how many shards can be merged in parallel
		workerPool          = xsync.NewWorkerPool(s.opts.MergeShardsConcurrency())
		bootstrapResultLock sync.Mutex
		wg                  sync.WaitGroup
	)

	workerPool.Init()

	for shard, unmergedShard := range unmerged {
		if unmergedShard.encodersBySeries == nil {
			continue
		}
		wg.Add(1)
		shard, unmergedShard := shard, unmergedShard
		mergeShardFunc := func() {
			var shardResult result.ShardResult
			shardResult, shardEmptyErrs[shard], shardErrs[shard] = s.mergeShard(
				shard, unmergedShard, blocksPool, multiReaderIteratorPool, encoderPool, blockSize, blopts)

			if shardResult != nil && shardResult.NumSeries() > 0 {
				// Prevent race conditions while updating bootstrapResult from multiple go-routines
				bootstrapResultLock.Lock()
				// Shard is a slice index so conversion to uint32 is safe
				bootstrapResult.Add(uint32(shard), shardResult, xtime.Ranges{})
				bootstrapResultLock.Unlock()
			}
			wg.Done()
		}
		workerPool.Go(mergeShardFunc)
	}

	// Wait for all merge goroutines to complete
	wg.Wait()
	s.logMergeShardsOutcome(shardErrs, shardEmptyErrs)
	return bootstrapResult
}

func (s *commitLogSource) mergeShard(
	shard int,
	unmergedShard shardData,
	blocksPool block.DatabaseBlockPool,
	multiReaderIteratorPool encoding.MultiReaderIteratorPool,
	encoderPool encoding.EncoderPool,
	blockSize time.Duration,
	blopts block.Options,
) (result.ShardResult, int, int) {
	var shardResult result.ShardResult
	var numShardEmptyErrs int
	var numErrs int

	for _, unmergedBlocks := range unmergedShard.encodersBySeries {
		seriesBlocks, numSeriesEmptyErrs, numSeriesErrs := s.mergeSeries(
			unmergedBlocks,
			blocksPool,
			multiReaderIteratorPool,
			encoderPool,
			blockSize,
			blopts,
		)

		if seriesBlocks != nil && seriesBlocks.Len() > 0 {
			if shardResult == nil {
				shardResult = result.NewShardResult(len(unmergedShard.encodersBySeries), s.opts.ResultOptions())
			}
			shardResult.AddSeries(unmergedBlocks.id, unmergedBlocks.tags, seriesBlocks)
		}

		numShardEmptyErrs += numSeriesEmptyErrs
		numErrs += numSeriesErrs
	}
	return shardResult, numShardEmptyErrs, numErrs
}

func (s *commitLogSource) mergeSeries(
	unmergedBlocks encodersByTime,
	blocksPool block.DatabaseBlockPool,
	multiReaderIteratorPool encoding.MultiReaderIteratorPool,
	encoderPool encoding.EncoderPool,
	blockSize time.Duration,
	blopts block.Options,
) (block.DatabaseSeriesBlocks, int, int) {
	var seriesBlocks block.DatabaseSeriesBlocks
	var numEmptyErrs int
	var numErrs int

	for startNano, encoders := range unmergedBlocks.encoders {
		start := startNano.ToTime()

		if len(encoders) == 0 {
			numEmptyErrs++
			continue
		}

		if len(encoders) == 1 {
			pooledBlock := blocksPool.Get()
			pooledBlock.Reset(start, blockSize, encoders[0].enc.Discard())
			if seriesBlocks == nil {
				seriesBlocks = block.NewDatabaseSeriesBlocks(len(unmergedBlocks.encoders))
			}
			seriesBlocks.AddBlock(pooledBlock)
			continue
		}

		// Convert encoders to readers so we can use iteration helpers
		readers := encoders.newReaders()
		iter := multiReaderIteratorPool.Get()
		iter.Reset(readers, time.Time{}, 0)

		var err error
		enc := encoderPool.Get()
		enc.Reset(start, blopts.DatabaseBlockAllocSize())
		for iter.Next() {
			dp, unit, annotation := iter.Current()
			encodeErr := enc.Encode(dp, unit, annotation)
			if encodeErr != nil {
				err = encodeErr
				numErrs++
				break
			}
		}

		if iterErr := iter.Err(); iterErr != nil {
			if err == nil {
				err = iter.Err()
			}
			numErrs++
		}

		// Automatically returns iter to the pool
		iter.Close()
		encoders.close()
		readers.close()

		if err != nil {
			continue
		}

		pooledBlock := blocksPool.Get()
		pooledBlock.Reset(start, blockSize, enc.Discard())
		if seriesBlocks == nil {
			seriesBlocks = block.NewDatabaseSeriesBlocks(len(unmergedBlocks.encoders))
		}
		seriesBlocks.AddBlock(pooledBlock)
	}
	return seriesBlocks, numEmptyErrs, numErrs
}

func (s *commitLogSource) findHighestShard(shardsTimeRanges result.ShardTimeRanges) uint32 {
	var max uint32
	for shard := range shardsTimeRanges {
		if shard > max {
			max = shard
		}
	}
	return max
}

func (s *commitLogSource) logEncodingOutcome(workerErrs []int, iter commitlog.Iterator) {
	errSum := 0
	for _, numErrs := range workerErrs {
		errSum += numErrs
	}
	if errSum > 0 {
		s.log.Errorf("error bootstrapping from commit log: %d block encode errors", errSum)
	}
	if err := iter.Err(); err != nil {
		s.log.Errorf("error reading commit log: %v", err)
	}
}

func (s *commitLogSource) logMergeShardsOutcome(shardErrs []int, shardEmptyErrs []int) {
	errSum := 0
	for _, numErrs := range shardErrs {
		errSum += numErrs
	}
	if errSum > 0 {
		s.log.Errorf("error bootstrapping from commit log: %d merge out of order errors", errSum)
	}

	emptyErrSum := 0
	for _, numEmptyErr := range shardEmptyErrs {
		emptyErrSum += numEmptyErr
	}
	if emptyErrSum > 0 {
		s.log.Errorf("error bootstrapping from commit log: %d empty unmerged blocks errors", emptyErrSum)
	}
}

func (s *commitLogSource) cacheShardData(allShardData []shardData) {
	for shard, currShardData := range allShardData {
		for _, seriesData := range currShardData.encodersBySeries {
			for blockStart := range seriesData.encoders {
				seriesData.encoders[blockStart] = nil
			}
		}

		if int(shard) >= len(s.cachedShardData) {
			// Extend the slice if necessary (could happen if different calls to
			// ReadData() bootstrap different shards.)
			for len(s.cachedShardData) != int(shard)+1 {
				s.cachedShardData = append(s.cachedShardData, shardData{
					encodersBySeries: make(map[uint64]encodersByTime),
				})
			}
		}

		s.cachedShardData[shard].ranges = s.cachedShardData[shard].ranges.AddRanges(currShardData.ranges)

		// TODO: Rename encodersBySeries to series
		currSeries := currShardData.encodersBySeries
		cachedSeries := s.cachedShardData[shard].encodersBySeries
		for uniqueIdx, seriesData := range currSeries {
			// If its not already there, just add it
			cachedSeriesData, ok := cachedSeries[uniqueIdx]
			if !ok {
				cachedSeries[uniqueIdx] = seriesData
				continue
			}

			// If it is there, merge blockStart times
			for blockStart := range seriesData.encoders {
				cachedSeriesData.encoders[blockStart] = nil
			}
		}
	}
}

func (s *commitLogSource) AvailableIndex(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
) result.ShardTimeRanges {
	// Commit log bootstrapper is a last ditch effort, so fulfill all
	// time ranges requested even if not enough data, just to succeed
	// the bootstrap
	return shardsTimeRanges
}

func (s *commitLogSource) ReadIndex(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	opts bootstrap.RunOptions,
) (result.IndexBootstrapResult, error) {
	// Also: it's now possible to cache the metadata and data
	// for all namespaces in the first call to ReadData(...) since
	// the source is used across all namespaces and then discarded after.
	// Finally, we can also probably improve performance significantly by
	// implementing a new interface in the Commitlog Reader/Iterator that
	// only returns a given series once per file, although there is some
	// trickiness there involving bufferpast and buffer future values.
	if shardsTimeRanges.IsEmpty() {
		return result.NewIndexBootstrapResult(), nil
	}

	cachedShardsTimeRanges := result.ShardTimeRanges{}
	for shard, shardData := range s.cachedShardData {
		cachedShardsTimeRanges[uint32(shard)] = shardData.ranges
	}

	filteredShardsTimeRanges := shardsTimeRanges.Copy()
	filteredShardsTimeRanges.Subtract(cachedShardsTimeRanges)

	// Setup predicates for skipping files / series at iterator and reader level.
	readCommitLogPredicate := newReadCommitLogPredicate(
		ns, filteredShardsTimeRanges, s.opts, s.inspection)
	readSeriesPredicate := newReadSeriesPredicate(ns)
	iterOpts := commitlog.IteratorOpts{
		CommitLogOptions:      s.opts.CommitLogOptions(),
		FileFilterPredicate:   readCommitLogPredicate,
		SeriesFilterPredicate: readSeriesPredicate,
	}

	// Create the commitlog iterator
	iter, err := s.newIteratorFn(iterOpts)
	if err != nil {
		return nil, fmt.Errorf("unable to create commit log iterator: %v", err)
	}
	defer iter.Close()

	highestShard := s.findHighestShard(shardsTimeRanges)
	// +1 so we can use the shard number as an index throughout without constantly
	// remembering to subtract 1 to convert to zero-based indexing.
	numShards := highestShard + 1
	// Convert the map to a slice for faster lookups
	bootstrapRangesByShard := make([]xtime.Ranges, numShards)
	for shard, ranges := range shardsTimeRanges {
		bootstrapRangesByShard[shard] = ranges
	}

	var (
		indexResult    = result.NewIndexBootstrapResult()
		indexResults   = indexResult.IndexResults()
		indexOptions   = ns.Options().IndexOptions()
		indexBlockSize = indexOptions.BlockSize()
		resultOptions  = s.opts.ResultOptions()
	)

	// Start by reading all the commit log files that we couldn't eliminate due to the
	// cached metadata.
	for iter.Next() {
		series, dp, _, _ := iter.Current()
		s.maybeAddToIndex(
			series.ID, series.Tags, series.Shard, highestShard, dp.Timestamp, bootstrapRangesByShard,
			indexResults, indexOptions, indexBlockSize, resultOptions)
	}

	// Add in all the data that was cached by a previous run of ReadData() (if any).
	for shard, shardData := range s.cachedShardData {
		for _, series := range shardData.encodersBySeries {
			for dataBlockStart := range series.encoders {
				s.maybeAddToIndex(
					series.id, series.tags, uint32(shard), highestShard, dataBlockStart.ToTime(), bootstrapRangesByShard,
					indexResults, indexOptions, indexBlockSize, resultOptions)
			}
		}
	}

	return indexResult, nil
}

func (s commitLogSource) maybeAddToIndex(
	id ident.ID,
	tags ident.Tags,
	shard uint32,
	highestShard uint32,
	blockStart time.Time,
	bootstrapRangesByShard []xtime.Ranges,
	indexResults result.IndexResults,
	indexOptions namespace.IndexOptions,
	indexBlockSize time.Duration,
	resultOptions result.Options,
) error {
	if !s.shouldIncludeInIndex(
		uint32(shard), blockStart, highestShard, indexBlockSize, bootstrapRangesByShard) {
		return nil
	}

	segment, err := indexResults.GetOrAddSegment(blockStart, indexOptions, resultOptions)
	if err != nil {
		return err
	}

	exists, err := segment.ContainsID(id.Data().Bytes())
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	d, err := convert.FromMetric(id, tags)
	if err != nil {
		return err
	}

	_, err = segment.Insert(d)
	if err != nil {
		return err
	}

	return nil
}

func newReadCommitLogPredicate(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	opts Options,
	inspection fs.Inspection,
) commitlog.FileFilterPredicate {
	// Minimum and maximum times for which we want to bootstrap
	shardMin, shardMax := shardsTimeRanges.MinMax()
	shardRange := xtime.Range{
		Start: shardMin,
		End:   shardMax,
	}

	// How far into the past or future a commitlog might contain a write for a
	// previous or future block
	bufferPast := ns.Options().RetentionOptions().BufferPast()
	bufferFuture := ns.Options().RetentionOptions().BufferFuture()

	// commitlogFilesPresentBeforeStart is a set of all the commitlog files that were
	// on disk before the node started.
	commitlogFilesPresentBeforeStart := inspection.CommitLogFilesSet()

	return func(name string, entryTime time.Time, entryDuration time.Duration) bool {
		_, ok := commitlogFilesPresentBeforeStart[name]
		if !ok {
			// If the file wasn't on disk before the node started then it only contains
			// writes that are already in memory (and in-fact the file may be actively
			// being written to.)
			return false
		}

		// If there is any amount of overlap between the commitlog range and the
		// shardRange then we need to read the commitlog file
		return xtime.Range{
			Start: entryTime.Add(-bufferPast),
			End:   entryTime.Add(entryDuration).Add(bufferFuture),
		}.Overlaps(shardRange)
	}
}

func newReadSeriesPredicate(ns namespace.Metadata) commitlog.SeriesFilterPredicate {
	nsID := ns.ID()
	return func(id ident.ID, namespace ident.ID) bool {
		return nsID.Equal(namespace)
	}
}

type shardData struct {
	encodersBySeries map[uint64]encodersByTime
	ranges           xtime.Ranges
}

type encodersByTime struct {
	id   ident.ID
	tags ident.Tags
	// int64 instead of time.Time because there is an optimized map access pattern
	// for i64's
	encoders map[xtime.UnixNano]encoders
}

// encoderArg contains all the information a worker go-routine needs to encode
// a data point as M3TSZ
type encoderArg struct {
	series     commitlog.Series
	dp         ts.Datapoint
	unit       xtime.Unit
	annotation ts.Annotation
	blockStart time.Time
}

type encoders []encoder

type ioReaders []xio.SegmentReader

func (e encoders) newReaders() ioReaders {
	readers := make(ioReaders, len(e))
	for i := range e {
		readers[i] = e[i].enc.Stream()
	}
	return readers
}

func (e encoders) close() {
	for i := range e {
		e[i].enc.Close()
	}
}

func (ir ioReaders) close() {
	for _, r := range ir {
		r.(xio.SegmentReader).Finalize()
	}
}
