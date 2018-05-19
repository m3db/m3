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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3db/src/dbnode/encoding"
	"github.com/m3db/m3db/src/dbnode/persist/fs"
	"github.com/m3db/m3db/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3db/src/dbnode/storage/block"
	"github.com/m3db/m3db/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3db/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3db/src/dbnode/storage/index/convert"
	"github.com/m3db/m3db/src/dbnode/storage/namespace"
	"github.com/m3db/m3db/src/dbnode/ts"
	"github.com/m3db/m3db/src/dbnode/x/xio"
	"github.com/m3db/m3x/ident"
	xlog "github.com/m3db/m3x/log"
	xsync "github.com/m3db/m3x/sync"
	xtime "github.com/m3db/m3x/time"
)

var (
	errIndexingNotEnableForNamespace = errors.New("indexing not enabled for namespace")
)

const encoderChanBufSize = 1000

type newIteratorFn func(opts commitlog.IteratorOpts) (commitlog.Iterator, error)

type commitLogSource struct {
	opts                Options
	inspection          fs.Inspection
	log                 xlog.Logger
	newIteratorFn       newIteratorFn
	cachedShardDataByNS map[string]*cachedShardData
}

type encoder struct {
	lastWriteAt time.Time
	enc         encoding.Encoder
}

func newCommitLogSource(opts Options, inspection fs.Inspection) bootstrap.Source {
	return &commitLogSource{
		opts:                opts,
		inspection:          inspection,
		log:                 opts.ResultOptions().InstrumentOptions().Logger(),
		newIteratorFn:       commitlog.NewIterator,
		cachedShardDataByNS: map[string]*cachedShardData{},
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
	runOpts bootstrap.RunOptions,
) (result.DataBootstrapResult, error) {
	if shardsTimeRanges.IsEmpty() {
		return result.NewDataBootstrapResult(), nil
	}

	readCommitLogPredicate := newReadCommitLogPredicate(
		ns, shardsTimeRanges, s.opts, s.inspection)

	// TODO(rartoul): When we implement caching data across namespaces, this will need
	// to be commitlog.ReadAllSeriesPredicate() if CacheSeriesMetadata() is enabled
	// because we'll need to read data for all namespaces, not just the one we're currently
	// bootstrapping.
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

	shardDataByShard := make([]shardData, numShards)
	for shard := range shardsTimeRanges {
		shardDataByShard[shard] = shardData{
			series: make(map[uint64]metadataAndEncodersByTime),
			ranges: shardsTimeRanges[shard],
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
			ns, runOpts, workerNum, encoderChan, shardDataByShard, encoderPool, workerErrs, blopts, wg)
	}

	for iter.Next() {
		series, dp, unit, annotation := iter.Current()
		if !s.shouldEncodeForData(shardDataByShard, blockSize, series, dp.Timestamp) {
			continue
		}

		// Distribute work such that each encoder goroutine is responsible for
		// approximately numShards / numConc shards. This also means that all
		// datapoints for a given shard/series will be processed in a serialized
		// manner.
		// We choose to distribute work by shard instead of series.UniqueIndex
		// because it means that all accesses to the shardDataByShard slice don't need
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

	result := s.mergeShards(int(numShards), bopts, blockSize, blopts, encoderPool, shardDataByShard)
	// After merging shards, its safe to cache the shardData (which involves some mutation).
	if s.shouldCacheSeriesMetadata(runOpts, ns) {
		s.cacheShardData(ns, shardDataByShard)
	}
	return result, nil
}

func (s *commitLogSource) startM3TSZEncodingWorker(
	ns namespace.Metadata,
	runOpts bootstrap.RunOptions,
	workerNum int,
	ec <-chan encoderArg,
	unmerged []shardData,
	encoderPool encoding.EncoderPool,
	workerErrs []int,
	blopts block.Options,
	wg *sync.WaitGroup,
) {
	shouldCacheSeriesMetadata := s.shouldCacheSeriesMetadata(runOpts, ns)
	for arg := range ec {
		var (
			series     = arg.series
			dp         = arg.dp
			unit       = arg.unit
			annotation = arg.annotation
			blockStart = arg.blockStart
		)

		unmergedShard := unmerged[series.Shard].series
		unmergedSeries, ok := unmergedShard[series.UniqueIndex]
		if !ok {
			if shouldCacheSeriesMetadata {
				// If we're going to cache the IDs and Tags on the commitlog source, then
				// we need to make sure that they won't get finalized by anything else in
				// the code base. Specifically, since series.Tags is a struct (not a pointer
				// to a struct), we need to call NoFinalize() on it as early in the code-path
				// as possible so that the NoFinalize() state is propagated everywhere (since
				// the struct will get copied repeatedly.)
				//
				// This is also the "ideal" spot to mark the IDs as NoFinalize(), because it
				// only occurs once per series per run. So if we end up allocating the IDs/Tags
				// multiple times during the bootstrap, we'll only mark the first appearance as
				// NoFinalize, and all subsequent occurrences can be finalized per usual.
				series.ID.NoFinalize()
				series.Tags.NoFinalize()
			}

			unmergedSeries = metadataAndEncodersByTime{
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
		if unmergedShard.series == nil {
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

	for _, unmergedBlocks := range unmergedShard.series {
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
				shardResult = result.NewShardResult(len(unmergedShard.series), s.opts.ResultOptions())
			}
			shardResult.AddSeries(unmergedBlocks.id, unmergedBlocks.tags, seriesBlocks)
		}

		numShardEmptyErrs += numSeriesEmptyErrs
		numErrs += numSeriesErrs
	}
	return shardResult, numShardEmptyErrs, numErrs
}

func (s *commitLogSource) mergeSeries(
	unmergedBlocks metadataAndEncodersByTime, blocksPool block.DatabaseBlockPool,
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

// cacheShardData caches the shardData from a call to ReadData() on the source so that subsequent calls
// to ReadIndex() for the same time period don't have to read the commit log files again.
//
// In order for the subsequent call to ReadIndex() to avoid reading the same commit log files, we need
// to cache three pieces of information for every series:
// 		1) The ID (so it can be indexed)
// 		2) The tags (so they can be indexed)
// 		3) The block starts for which the series had a datapoint (so that we know which index blocks
//         / segments the series needs to be included in)
//
// In addition, for each shard we will need to store the ranges which we have already read commit log
// files for, so that the ReadIndex() call can easily filter commit log files down to those which
// have not already been read by a previous call to ReadData().
//
// Its safe to cache the series IDs and Tags because we mark them both as NoFinalize() if the caching
// path is enabled.
func (s *commitLogSource) cacheShardData(ns namespace.Metadata, allShardData []shardData) {
	nsString := ns.ID().String()
	nsCache, ok := s.cachedShardDataByNS[nsString]
	if !ok {
		nsShardData := &cachedShardData{
			shardData: make([]shardData, len(allShardData)),
		}
		s.cachedShardDataByNS[nsString] = nsShardData
		nsCache = nsShardData
	}

	for shard, currShardData := range allShardData {
		for _, seriesData := range currShardData.series {
			for blockStart := range seriesData.encoders {
				// Nil out any references to the encoders (which should be closed already anyways),
				// so that they can be GC'd.
				seriesData.encoders[blockStart] = nil
			}
		}

		for shard >= len(nsCache.shardData) {
			// Extend the slice if necessary (could happen if different calls to
			// ReadData() bootstrap different shards.)
			nsCache.shardData = append(nsCache.shardData, shardData{})
		}

		nsCache.shardData[shard].ranges = nsCache.shardData[shard].ranges.AddRanges(currShardData.ranges)

		currSeries := currShardData.series
		cachedSeries := nsCache.shardData[shard].series

		// If there are no existing series, just set what we have.
		if len(cachedSeries) == 0 {
			if currSeries != nil {
				nsCache.shardData[shard].series = currSeries
			} else {
				nsCache.shardData[shard].series = make(map[uint64]metadataAndEncodersByTime)
			}
			continue
		}

		// If there are existing series, then add any new series that we have, and merge block starts.
		for uniqueIdx, seriesData := range currSeries {
			// If its not already there, just add it
			cachedSeriesData, ok := cachedSeries[uniqueIdx]
			if ok {
			}
			if !ok {
				cachedSeries[uniqueIdx] = seriesData
				continue
			}

			// If it is there, merge blockStart times
			for blockStart := range seriesData.encoders {
				// The existence of a key in the map is indicative of its presence in this case,
				// so assigning nil is equivalent to adding an item to a set. This is counter-intuitive,
				// but we do it so that we can re-use the existing datastructures that have already been
				// allocated by the bootstrapping process, otherwise we'd have to perform millions of
				// additional allocations.
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
	if !ns.Options().IndexOptions().Enabled() {
		return result.NewIndexBootstrapResult(), errIndexingNotEnableForNamespace
	}

	if shardsTimeRanges.IsEmpty() {
		return result.NewIndexBootstrapResult(), nil
	}

	var (
		nsCache                        = s.cachedShardDataByNS[ns.ID().String()]
		shardsTimeRangesToReadFromDisk = shardsTimeRanges.Copy()
	)
	if nsCache != nil {
		cachedShardsTimeRanges := result.ShardTimeRanges{}
		for shard, shardData := range nsCache.shardData {
			cachedShardsTimeRanges[uint32(shard)] = shardData.ranges
		}
		shardsTimeRangesToReadFromDisk.Subtract(cachedShardsTimeRanges)
	}

	// Setup predicates for skipping files / series at iterator and reader level.
	readCommitLogPredicate := newReadCommitLogPredicate(
		ns, shardsTimeRangesToReadFromDisk, s.opts, s.inspection)
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
	if nsCache != nil {
		for shard, shardData := range nsCache.shardData {
			for _, series := range shardData.series {
				for dataBlockStart := range series.encoders {
					s.maybeAddToIndex(
						series.id, series.tags, uint32(shard), highestShard, dataBlockStart.ToTime(), bootstrapRangesByShard,
						indexResults, indexOptions, indexBlockSize, resultOptions)
				}
			}
		}
	}

	// If all successfull then we mark each index block as fulfilled
	for _, block := range indexResult.IndexResults() {
		blockRange := xtime.Range{
			Start: block.BlockStart(),
			End:   block.BlockStart().Add(indexOptions.BlockSize()),
		}
		fulfilled := result.ShardTimeRanges{}
		for shard, timeRanges := range shardsTimeRanges {
			iter := timeRanges.Iter()
			for iter.Next() {
				curr := iter.Value()
				intersection, intersects := curr.Intersect(blockRange)
				if intersects {
					fulfilled[shard] = fulfilled[shard].AddRange(intersection)
				}
			}
		}
		// Now mark as much of the block that we fulfilled
		err := indexResult.IndexResults().MarkFulfilled(blockRange.Start,
			fulfilled, indexOptions, resultOptions)
		if err != nil {
			return nil, err
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
		shard, blockStart, highestShard, indexBlockSize, bootstrapRangesByShard) {
		return nil
	}

	segment, err := indexResults.GetOrAddSegment(blockStart, indexOptions, resultOptions)
	if err != nil {
		return err
	}

	exists, err := segment.ContainsID(id.Bytes())
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	// We can use the NoClone variant here because the cached IDs/Tags are marked NoFinalize
	// by the ReadData() path when it reads from the commitlog files, and the IDs/Tags read
	// from the commit log files by the ReadIndex() method won't be finalized because this
	// code path doesn't finalize them.
	d, err := convert.FromMetricNoClone(id, tags)
	if err != nil {
		return err
	}

	_, err = segment.Insert(d)
	return err
}

func (s commitLogSource) shouldCacheSeriesMetadata(runOpts bootstrap.RunOptions, nsMeta namespace.Metadata) bool {
	return runOpts.CacheSeriesMetadata() && nsMeta.Options().IndexOptions().Enabled()
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
	series map[uint64]metadataAndEncodersByTime
	ranges xtime.Ranges
}

type metadataAndEncodersByTime struct {
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

type cachedShardData struct {
	shardData []shardData
}
