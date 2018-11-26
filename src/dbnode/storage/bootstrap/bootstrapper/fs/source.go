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

package fs

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	xlog "github.com/m3db/m3x/log"
	"github.com/m3db/m3x/pool"
	xsync "github.com/m3db/m3x/sync"
	xtime "github.com/m3db/m3x/time"

	"github.com/uber-go/tally"
)

type runType int

const (
	bootstrapDataRunType runType = iota
	bootstrapIndexRunType
)

type newDataFileSetReaderFn func(
	bytesPool pool.CheckedBytesPool,
	opts fs.Options,
) (fs.DataFileSetReader, error)

type fileSystemSource struct {
	opts              Options
	fsopts            fs.Options
	log               xlog.Logger
	idPool            ident.Pool
	newReaderFn       newDataFileSetReaderFn
	newReaderPoolOpts newReaderPoolOptions
	dataProcessors    xsync.WorkerPool
	indexProcessors   xsync.WorkerPool
	persistManager    persistManager
	metrics           fileSystemSourceMetrics
}

type persistManager struct {
	sync.Mutex
	mgr persist.Manager
}

type fileSystemSourceMetrics struct {
	persistedIndexBlocksRead  tally.Counter
	persistedIndexBlocksWrite tally.Counter
}

func newFileSystemSource(opts Options) bootstrap.Source {
	iopts := opts.InstrumentOptions()
	scope := iopts.MetricsScope().SubScope("fs-bootstrapper")
	iopts = iopts.SetMetricsScope(scope)
	opts = opts.SetInstrumentOptions(iopts)

	dataProcessors := xsync.NewWorkerPool(opts.BoostrapDataNumProcessors())
	dataProcessors.Init()
	indexProcessors := xsync.NewWorkerPool(opts.BoostrapIndexNumProcessors())
	indexProcessors.Init()

	s := &fileSystemSource{
		opts:            opts,
		fsopts:          opts.FilesystemOptions(),
		log:             iopts.Logger(),
		idPool:          opts.IdentifierPool(),
		newReaderFn:     fs.NewReader,
		dataProcessors:  dataProcessors,
		indexProcessors: indexProcessors,
		persistManager: persistManager{
			mgr: opts.PersistManager(),
		},
		metrics: fileSystemSourceMetrics{
			persistedIndexBlocksRead:  scope.Counter("persist-index-blocks-read"),
			persistedIndexBlocksWrite: scope.Counter("persist-index-blocks-write"),
		},
	}
	s.newReaderPoolOpts.alloc = s.newReader

	return s
}

func (s *fileSystemSource) Can(strategy bootstrap.Strategy) bool {
	switch strategy {
	case bootstrap.BootstrapSequential:
		return true
	}
	return false
}

func (s *fileSystemSource) AvailableData(
	md namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) (result.ShardTimeRanges, error) {
	return s.availability(md, shardsTimeRanges)
}

func (s *fileSystemSource) ReadData(
	md namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) (result.DataBootstrapResult, error) {
	r, err := s.read(md, shardsTimeRanges, bootstrapDataRunType, runOpts)
	if err != nil {
		return nil, err
	}
	return r.data, nil
}

func (s *fileSystemSource) AvailableIndex(
	md namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) (result.ShardTimeRanges, error) {
	return s.availability(md, shardsTimeRanges)
}

func (s *fileSystemSource) ReadIndex(
	md namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) (result.IndexBootstrapResult, error) {
	r, err := s.read(md, shardsTimeRanges, bootstrapIndexRunType, runOpts)
	if err != nil {
		return nil, err
	}
	return r.index, nil
}

func (s *fileSystemSource) availability(
	md namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
) (result.ShardTimeRanges, error) {
	result := make(map[uint32]xtime.Ranges)
	for shard, ranges := range shardsTimeRanges {
		result[shard] = s.shardAvailability(md.ID(), shard, ranges)
	}
	return result, nil
}

func (s *fileSystemSource) shardAvailability(
	namespace ident.ID,
	shard uint32,
	targetRangesForShard xtime.Ranges,
) xtime.Ranges {
	if targetRangesForShard.IsEmpty() {
		return xtime.Ranges{}
	}

	readInfoFilesResults := fs.ReadInfoFiles(s.fsopts.FilePathPrefix(),
		namespace, shard, s.fsopts.InfoReaderBufferSize(), s.fsopts.DecodingOptions())

	var tr xtime.Ranges
	for i := 0; i < len(readInfoFilesResults); i++ {
		result := readInfoFilesResults[i]
		if result.Err.Error() != nil {
			s.log.WithFields(
				xlog.NewField("shard", shard),
				xlog.NewField("namespace", namespace.String()),
				xlog.NewField("error", result.Err.Error()),
				xlog.NewField("targetRangesForShard", targetRangesForShard),
				xlog.NewField("filepath", result.Err.Filepath()),
			).Error("unable to read info files in shardAvailability")
			continue
		}
		info := result.Info
		t := xtime.FromNanoseconds(info.BlockStart)
		w := time.Duration(info.BlockSize)
		currRange := xtime.Range{Start: t, End: t.Add(w)}
		if targetRangesForShard.Overlaps(currRange) {
			tr = tr.AddRange(currRange)
		}
	}
	return tr
}

func (s *fileSystemSource) enqueueReaders(
	ns namespace.Metadata,
	run runType,
	runOpts bootstrap.RunOptions,
	shardsTimeRanges result.ShardTimeRanges,
	readerPool *readerPool,
	readersCh chan<- timeWindowReaders,
) {
	// Close the readers ch if and only if all readers are enqueued
	defer close(readersCh)

	shouldPersistIndexBootstrap := run == bootstrapIndexRunType && s.shouldPersist(runOpts)
	if !shouldPersistIndexBootstrap {
		// Normal run, open readers
		s.enqueueReadersGroupedByBlockSize(ns, run, runOpts,
			shardsTimeRanges, readerPool, readersCh)
		return
	}

	// If the run is an index bootstrap with the persist configuration enabled
	// then we need to write out the metadata into FSTs that we store on disk,
	// to avoid creating any one single huge FST at once we bucket the
	// shards into number of buckets
	runtimeOpts := s.opts.RuntimeOptionsManager().Get()
	numSegmentsPerBlock := runtimeOpts.FlushIndexBlockNumSegments()

	buckets := make([]result.ShardTimeRanges, numSegmentsPerBlock)
	for i := range buckets {
		buckets[i] = make(result.ShardTimeRanges)
	}

	i := 0
	for shard, timeRanges := range shardsTimeRanges {
		idx := i % int(numSegmentsPerBlock)
		buckets[idx][shard] = timeRanges
		i++
	}

	for _, bucket := range buckets {
		if len(bucket) == 0 {
			// Skip potentially empty buckets if num of segments per block is
			// greater than the number of shards
			continue
		}
		s.enqueueReadersGroupedByBlockSize(ns, run, runOpts,
			bucket, readerPool, readersCh)
	}
}

func (s *fileSystemSource) enqueueReadersGroupedByBlockSize(
	ns namespace.Metadata,
	run runType,
	runOpts bootstrap.RunOptions,
	shardTimeRanges result.ShardTimeRanges,
	readerPool *readerPool,
	readersCh chan<- timeWindowReaders,
) {
	// First bucket the shard time ranges by block size
	var blockSize time.Duration
	switch run {
	case bootstrapDataRunType:
		blockSize = ns.Options().RetentionOptions().BlockSize()
	case bootstrapIndexRunType:
		blockSize = ns.Options().IndexOptions().BlockSize()
	default:
		panic(fmt.Errorf("unrecognized run type: %d", run))
	}

	// Group them by block size
	groupFn := newShardTimeRangesTimeWindowGroups
	groupedByBlockSize := groupFn(shardTimeRanges, blockSize)

	// Now enqueue across all shards by block size
	for _, group := range groupedByBlockSize {
		readers := make(map[shardID]shardReaders, len(group.ranges))
		for shard, tr := range group.ranges {
			shardReaders := s.newShardReaders(ns, readerPool, shard, tr)
			readers[shardID(shard)] = shardReaders
		}
		readersCh <- newTimeWindowReaders(group.ranges, readers)
	}
}

func (s *fileSystemSource) newShardReaders(
	ns namespace.Metadata,
	readerPool *readerPool,
	shard uint32,
	tr xtime.Ranges,
) shardReaders {
	readInfoFilesResults := fs.ReadInfoFiles(s.fsopts.FilePathPrefix(),
		ns.ID(), shard, s.fsopts.InfoReaderBufferSize(), s.fsopts.DecodingOptions())
	if len(readInfoFilesResults) == 0 {
		return shardReaders{} // No readers
	}

	readers := make([]fs.DataFileSetReader, 0, len(readInfoFilesResults))
	for i := 0; i < len(readInfoFilesResults); i++ {
		result := readInfoFilesResults[i]
		if result.Err.Error() != nil {
			s.log.WithFields(
				xlog.NewField("shard", shard),
				xlog.NewField("namespace", ns.ID().String()),
				xlog.NewField("error", result.Err.Error()),
				xlog.NewField("timeRange", tr.String()),
				xlog.NewField("path", result.Err.Filepath()),
			).Error("fs bootstrapper unable to read info file")
			// Errors are marked unfulfilled by markRunResultErrorsAndUnfulfilled
			// and will be re-attempted by the next bootstrapper
			continue
		}

		info := result.Info
		blockStart := xtime.FromNanoseconds(info.BlockStart)
		if !tr.Overlaps(xtime.Range{
			Start: blockStart,
			End:   blockStart.Add(ns.Options().RetentionOptions().BlockSize()),
		}) {
			// Errors are marked unfulfilled by markRunResultErrorsAndUnfulfilled
			// and will be re-attempted by the next bootstrapper
			continue
		}

		r, err := readerPool.get()
		if err != nil {
			s.log.Errorf("unable to get reader from pool")
			// Errors are marked unfulfilled by markRunResultErrorsAndUnfulfilled
			// and will be re-attempted by the next bootstrapper
			continue
		}

		openOpts := fs.DataReaderOpenOptions{
			Identifier: fs.FileSetFileIdentifier{
				Namespace:  ns.ID(),
				Shard:      shard,
				BlockStart: blockStart,
			},
		}
		if err := r.Open(openOpts); err != nil {
			s.log.WithFields(
				xlog.NewField("shard", shard),
				xlog.NewField("blockStart", blockStart.String()),
				xlog.NewField("error", err.Error()),
			).Error("unable to open fileset files")
			readerPool.put(r)
			// Errors are marked unfulfilled by markRunResultErrorsAndUnfulfilled
			// and will be re-attempted by the next bootstrapper
			continue
		}

		readers = append(readers, r)
	}

	return shardReaders{readers: readers}
}

func (s *fileSystemSource) bootstrapFromReaders(
	ns namespace.Metadata,
	run runType,
	runOpts bootstrap.RunOptions,
	readerPool *readerPool,
	retriever block.DatabaseBlockRetriever,
	readersCh <-chan timeWindowReaders,
) *runResult {
	var (
		runResult         = newRunResult()
		resultOpts        = s.opts.ResultOptions()
		shardRetrieverMgr block.DatabaseShardBlockRetrieverManager
		wg                sync.WaitGroup
		processors        xsync.WorkerPool
	)
	if retriever != nil {
		shardRetrieverMgr = block.NewDatabaseShardBlockRetrieverManager(retriever)
	}

	switch run {
	case bootstrapDataRunType:
		processors = s.dataProcessors
	case bootstrapIndexRunType:
		processors = s.indexProcessors
	default:
		panic(fmt.Errorf("unrecognized run type: %d", run))
	}

	for timeWindowReaders := range readersCh {
		timeWindowReaders := timeWindowReaders
		wg.Add(1)
		processors.Go(func() {
			s.loadShardReadersDataIntoShardResult(ns, run, runOpts, runResult,
				resultOpts, shardRetrieverMgr, timeWindowReaders, readerPool)
			wg.Done()
		})
	}
	wg.Wait()

	shardResults := runResult.data.ShardResults()
	for shard, results := range shardResults {
		if results.NumSeries() == 0 {
			delete(shardResults, shard)
		}
	}

	return runResult
}

// markRunResultErrorsAndUnfulfilled checks the list of times that had errors and makes
// sure that we don't return any blocks or bloom filters for them. In addition,
// it looks at any remaining (unfulfilled) ranges and makes sure they're marked
// as unfulfilled
func (s *fileSystemSource) markRunResultErrorsAndUnfulfilled(
	runResult *runResult,
	requestedRanges result.ShardTimeRanges,
	remainingRanges result.ShardTimeRanges,
	timesWithErrors []time.Time,
) {
	// NB(xichen): this is the exceptional case where we encountered errors due to files
	// being corrupted, which should be fairly rare so we can live with the overhead. We
	// experimented with adding the series to a temporary map and only adding the temporary map
	// to the final result but adding series to large map with string keys is expensive, and
	// the current implementation saves the extra overhead of merging temporary map with the
	// final result.
	if len(timesWithErrors) > 0 {
		timesWithErrorsString := make([]string, len(timesWithErrors))
		for i := range timesWithErrors {
			timesWithErrorsString[i] = timesWithErrors[i].String()
		}
		s.log.WithFields(
			xlog.NewField("requestedRanges", requestedRanges.SummaryString()),
			xlog.NewField("timesWithErrors", timesWithErrorsString),
		).Info("deleting entries from results for times with errors")

		runResult.Lock()
		for shard := range requestedRanges {
			// Delete all affected times from the data results.
			shardResult, ok := runResult.data.ShardResults()[shard]
			if ok {
				for _, entry := range shardResult.AllSeries().Iter() {
					series := entry.Value()
					for _, t := range timesWithErrors {
						shardResult.RemoveBlockAt(series.ID, t)
					}
				}
			}
		}
		// NB(r): We explicitly do not remove entries from the index results
		// as they are additive and get merged together with results from other
		// bootstrappers by just appending the result (unlike data bootstrap
		// results that when merged replace the block with the current block).
		// It would also be difficult to remove only series that were added to the
		// index block as results from data files can be subsets of the index block
		// and there's no way to definitively delete the entry we added as a result
		// of just this data file failing.
		runResult.Unlock()
	}

	if !remainingRanges.IsEmpty() {
		runResult.Lock()
		for _, unfulfilled := range []result.ShardTimeRanges{
			runResult.data.Unfulfilled(),
			runResult.index.Unfulfilled(),
		} {
			unfulfilled.AddRanges(remainingRanges)
		}
		runResult.Unlock()
	}
}

func (s *fileSystemSource) loadShardReadersDataIntoShardResult(
	ns namespace.Metadata,
	run runType,
	runOpts bootstrap.RunOptions,
	runResult *runResult,
	ropts result.Options,
	shardRetrieverMgr block.DatabaseShardBlockRetrieverManager,
	timeWindowReaders timeWindowReaders,
	readerPool *readerPool,
) {
	var (
		blockPool         = ropts.DatabaseBlockOptions().DatabaseBlockPool()
		seriesCachePolicy = ropts.SeriesCachePolicy()
		indexBlockSegment segment.MutableSegment
		timesWithErrors   []time.Time
		shardResult       result.ShardResult
		shardRetriever    block.DatabaseShardBlockRetriever
	)

	requestedRanges := timeWindowReaders.ranges
	remainingRanges := requestedRanges.Copy()
	shardReaders := timeWindowReaders.readers
	for shard, shardReaders := range shardReaders {
		shard := uint32(shard)
		readers := shardReaders.readers

		if run == bootstrapDataRunType {
			// For the bootstrap data case we need the shard retriever
			if shardRetrieverMgr != nil {
				shardRetriever = shardRetrieverMgr.ShardRetriever(shard)
			}
		}

		for _, r := range readers {
			var (
				timeRange = r.Range()
				start     = timeRange.Start
				blockSize = ns.Options().RetentionOptions().BlockSize()
				err       error
			)
			switch run {
			case bootstrapDataRunType:
				capacity := r.Entries()
				shardResult = runResult.getOrAddDataShardResult(shard, capacity, ropts)
			case bootstrapIndexRunType:
				indexBlockSegment, err = runResult.getOrAddIndexSegment(start, ns, ropts)
			default:
				// Unreachable unless an internal method calls with a run type casted from int
				panic(fmt.Errorf("invalid run type: %d", run))
			}

			numEntries := r.Entries()
			for i := 0; err == nil && i < numEntries; i++ {
				switch run {
				case bootstrapDataRunType:
					err = s.readNextEntryAndRecordBlock(r, runResult, start, blockSize, shardResult,
						shardRetriever, blockPool, seriesCachePolicy)
				case bootstrapIndexRunType:
					// We can just read the entry and index if performing an index run
					err = s.readNextEntryAndIndex(r, runResult, indexBlockSegment)
				default:
					// Unreachable unless an internal method calls with a run type casted from int
					panic(fmt.Errorf("invalid run type: %d", run))
				}
			}

			if err == nil {
				// Validate the read results
				var validateErr error
				switch run {
				case bootstrapDataRunType:
					switch seriesCachePolicy {
					case series.CacheAll:
						validateErr = r.Validate()
					default:
						err = fmt.Errorf("invalid series cache policy: %s", seriesCachePolicy.String())
					}
				case bootstrapIndexRunType:
					validateErr = r.ValidateMetadata()
				default:
					// Unreachable unless an internal method calls with a run type casted from int
					panic(fmt.Errorf("invalid run type: %d", run))
				}
				if validateErr != nil {
					err = fmt.Errorf("data validation failed: %v", validateErr)
				}
			}

			if err == nil && run == bootstrapIndexRunType {
				// Mark index block as fulfilled
				fulfilled := result.ShardTimeRanges{
					shard: xtime.Ranges{}.AddRange(timeRange),
				}
				err = runResult.index.IndexResults().MarkFulfilled(start, fulfilled,
					ns.Options().IndexOptions())
			}

			if err == nil {
				remainingRanges.Subtract(result.ShardTimeRanges{
					shard: xtime.Ranges{}.AddRange(timeRange),
				})
			} else {
				s.log.Errorf("%v", err)
				timesWithErrors = append(timesWithErrors, timeRange.Start)
			}
		}
	}

	var (
		shouldPersist = s.shouldPersist(runOpts)
		noneRemaining = remainingRanges.IsEmpty()
	)
	if run == bootstrapIndexRunType && shouldPersist && noneRemaining {
		err := s.persistBootstrapIndexSegment(ns, requestedRanges, runResult)
		if err != nil {
			iopts := s.opts.ResultOptions().InstrumentOptions()
			instrument.EmitAndLogInvariantViolation(iopts, func(l xlog.Logger) {
				l.WithFields(
					xlog.NewField("namespace", ns.ID().String()),
					xlog.NewField("requestedRanges", requestedRanges.String()),
					xlog.NewField("error", err.Error()),
				).Error("persist fs index bootstrap failed")
			})
		}
	}

	// Return readers to pool
	for _, shardReaders := range shardReaders {
		for _, r := range shardReaders.readers {
			if err := r.Close(); err == nil {
				readerPool.put(r)
			}
		}
	}

	s.markRunResultErrorsAndUnfulfilled(runResult, requestedRanges,
		remainingRanges, timesWithErrors)
}

func (s *fileSystemSource) readNextEntryAndRecordBlock(
	r fs.DataFileSetReader,
	runResult *runResult,
	blockStart time.Time,
	blockSize time.Duration,
	shardResult result.ShardResult,
	shardRetriever block.DatabaseShardBlockRetriever,
	blockPool block.DatabaseBlockPool,
	seriesCachePolicy series.CachePolicy,
) error {
	var (
		seriesBlock = blockPool.Get()
		id          ident.ID
		tagsIter    ident.TagIterator
		data        checked.Bytes
		err         error
	)
	switch seriesCachePolicy {
	case series.CacheAll:
		id, tagsIter, data, _, err = r.Read()
	default:
		err = fmt.Errorf("invalid series cache policy: %s", seriesCachePolicy.String())
	}
	if err != nil {
		return fmt.Errorf("error reading data file: %v", err)
	}

	var (
		entry  result.DatabaseSeriesBlocks
		tags   ident.Tags
		exists bool
	)
	runResult.Lock()
	defer runResult.Unlock()

	entry, exists = shardResult.AllSeries().Get(id)
	if exists {
		// NB(r): In the case the series is already inserted
		// we can avoid holding onto this ID and use the already
		// allocated ID.
		id.Finalize()
		id = entry.ID
		tags = entry.Tags
	} else {
		tags, err = convert.TagsFromTagsIter(id, tagsIter, s.idPool)
		if err != nil {
			return fmt.Errorf("unable to decode tags: %v", err)
		}
	}
	tagsIter.Close()

	switch seriesCachePolicy {
	case series.CacheAll:
		seg := ts.NewSegment(data, nil, ts.FinalizeHead)
		seriesBlock.Reset(blockStart, blockSize, seg)
	default:
		return fmt.Errorf("invalid series cache policy: %s", seriesCachePolicy.String())
	}

	if exists {
		entry.Blocks.AddBlock(seriesBlock)
	} else {
		shardResult.AddBlock(id, tags, seriesBlock)
	}
	return nil
}

func (s *fileSystemSource) readNextEntryAndIndex(
	r fs.DataFileSetReader,
	runResult *runResult,
	segment segment.MutableSegment,
) error {
	// If performing index run, then simply read the metadata and add to segment
	id, tagsIter, _, _, err := r.ReadMetadata()
	if err != nil {
		return err
	}

	// NB(r): Avoiding defer in the hot path here
	release := func() {
		// Finalize the ID and tags
		id.Finalize()
		tagsIter.Close()
	}

	idBytes := id.Bytes()

	runResult.RLock()
	exists, err := segment.ContainsID(idBytes)
	runResult.RUnlock()
	if err != nil {
		release()
		return err
	}
	if exists {
		release()
		return nil
	}

	d, err := convert.FromMetricIter(id, tagsIter)
	release()
	if err != nil {
		return err
	}

	runResult.Lock()
	exists, err = segment.ContainsID(d.ID)
	// ID and tags no longer required below
	if err != nil {
		runResult.Unlock()
		return err
	}
	if exists {
		runResult.Unlock()
		return nil
	}
	_, err = segment.Insert(d)
	runResult.Unlock()

	return err
}

func (s *fileSystemSource) persistBootstrapIndexSegment(
	ns namespace.Metadata,
	requestedRanges result.ShardTimeRanges,
	runResult *runResult,
) error {
	// If we're performing an index run with persistence enabled
	// determine if we covered a full block exactly (which should
	// occur since we always group readers by block size)
	min, max := requestedRanges.MinMax()
	blockSize := ns.Options().IndexOptions().BlockSize()
	blockStart := min.Truncate(blockSize)

	shards := make(map[uint32]struct{})
	expectedRanges := make(result.ShardTimeRanges, len(requestedRanges))
	for shard := range requestedRanges {
		shards[shard] = struct{}{}
		expectedRanges[shard] = xtime.Ranges{}.AddRange(xtime.Range{
			Start: blockStart,
			End:   blockStart.Add(blockSize),
		})
	}

	indexResults := runResult.index.IndexResults()
	indexBlock, ok := indexResults[xtime.ToUnixNano(blockStart)]
	if !ok {
		return fmt.Errorf("did not find index block for blocksStart: %d", blockStart.Unix())
	}

	var (
		mutableSegment     segment.MutableSegment
		numMutableSegments = 0
	)

	for _, seg := range indexBlock.Segments() {
		if mSeg, ok := seg.(segment.MutableSegment); ok {
			mutableSegment = mSeg
			numMutableSegments++
		}
	}

	if numMutableSegments != 1 {
		return fmt.Errorf("error asserting index block has single mutable segment for blocksStart: %d, found: %d",
			blockStart.Unix(), numMutableSegments)
	}

	var (
		fulfilled           = indexBlock.Fulfilled()
		success             = false
		replacementSegments []segment.Segment
	)
	defer func() {
		if !success {
			return
		}
		// if we're successful, we need to update the segments in the block.
		segments := replacementSegments

		// get references to existing immutable segments from the block
		for _, seg := range indexBlock.Segments() {
			mSeg, ok := seg.(segment.MutableSegment)
			if !ok {
				segments = append(segments, seg)
				continue
			}
			if err := mSeg.Close(); err != nil {
				// safe to only log warning as we have persisted equivalent for the mutable block
				// at this point.
				s.log.Warnf("encountered error while closing persisted mutable segment: %v", err)
			}
		}

		// Now replace the active segment with the persisted segment
		newFulfilled := fulfilled.Copy()
		newFulfilled.AddRanges(expectedRanges)
		replacedBlock := result.NewIndexBlock(blockStart, segments, newFulfilled)
		indexResults[xtime.ToUnixNano(blockStart)] = replacedBlock
	}()

	// Check that completely fulfilled all shards for the block
	// and we didn't bootstrap any more/less
	requireFulfilled := expectedRanges.Copy()
	requireFulfilled.Subtract(fulfilled)
	exactStartEnd := min.Equal(blockStart) && max.Equal(blockStart.Add(blockSize))
	if !exactStartEnd || !requireFulfilled.IsEmpty() {
		return fmt.Errorf("persistent fs index bootstrap invalid ranges to persist: expected=%v, actual=%v, fulfilled=%v",
			expectedRanges.String(), requestedRanges.String(), fulfilled.String())
	}

	// NB(r): Need to get an exclusive lock to actually write the segment out
	// due to needing to incrementing the index file set volume index and also
	// using non-thread safe resources on the persist manager
	s.persistManager.Lock()
	defer s.persistManager.Unlock()

	flush, err := s.persistManager.mgr.StartIndexPersist()
	if err != nil {
		return err
	}

	var calledDone bool
	defer func() {
		if !calledDone {
			flush.DoneIndex()
		}
	}()

	preparedPersist, err := flush.PrepareIndex(persist.IndexPrepareOptions{
		NamespaceMetadata: ns,
		BlockStart:        indexBlock.BlockStart(),
		FileSetType:       persist.FileSetFlushType,
		Shards:            shards,
	})
	if err != nil {
		return err
	}

	var calledClose bool
	defer func() {
		if !calledClose {
			preparedPersist.Close()
		}
	}()

	if !mutableSegment.IsSealed() {
		if _, err := mutableSegment.Seal(); err != nil {
			return err
		}
	}

	if err := preparedPersist.Persist(mutableSegment); err != nil {
		return err
	}

	calledClose = true
	replacementSegments, err = preparedPersist.Close()
	if err != nil {
		return err
	}

	calledDone = true
	if err := flush.DoneIndex(); err != nil {
		return err
	}

	// Track success
	s.metrics.persistedIndexBlocksWrite.Inc(1)

	// indicate the defer above should replace the mutable segments in the index block.
	success = true
	return nil
}

func (s *fileSystemSource) read(
	md namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	run runType,
	runOpts bootstrap.RunOptions,
) (*runResult, error) {
	var (
		seriesCachePolicy = s.opts.ResultOptions().SeriesCachePolicy()
		blockRetriever    block.DatabaseBlockRetriever
		res               *runResult
	)
	if shardsTimeRanges.IsEmpty() {
		return newRunResult(), nil
	}

	setOrMergeResult := func(newResult *runResult) {
		if newResult == nil {
			return
		}
		if res == nil {
			res = newResult
		} else {
			res = res.mergedResult(newResult)
		}
	}

	if run == bootstrapDataRunType {
		// NB(r): We only need to cache shard indices and marks blocks as
		// fulfilled when bootstrapping data, because the data can be retrieved
		// lazily from disk during reads.
		// On the other hand, if we're bootstrapping the index then currently we
		// need to rebuild it from scratch by reading all the IDs/tags until
		// we can natively bootstrap persisted segments from disk and compact them
		// with series metadata from other shards if topology has changed.
		if mgr := s.opts.DatabaseBlockRetrieverManager(); mgr != nil {
			shards := make([]uint32, 0, len(shardsTimeRanges))
			for shard := range shardsTimeRanges {
				shards = append(shards, shard)
			}
			var err error
			blockRetriever, err = s.resolveBlockRetrieverAndCacheDataShardIndices(md,
				mgr, shards)
			if err != nil {
				return nil, err
			}
		}

		switch seriesCachePolicy {
		case series.CacheAll:
			// No checks necessary
		default:
			// Unless we're caching all series (or all series metadata) in memory, we
			// return just the availability of the files we have
			return s.bootstrapDataRunResultFromAvailability(md,
				shardsTimeRanges), nil
		}
	}

	if run == bootstrapIndexRunType {
		// NB(r): First read all the FSTs and add to runResult index results,
		// subtract the shard + time ranges from what we intend to bootstrap
		// for those we found
		r, err := s.bootstrapFromIndexPersistedBlocks(md,
			shardsTimeRanges)
		if err != nil {
			s.log.Warnf("filesystem bootstrapped failed to read persisted index blocks")
		} else {
			// We may have less we need to read
			shardsTimeRanges = shardsTimeRanges.Copy()
			shardsTimeRanges.Subtract(r.fulfilled)
			// Set or merge result
			setOrMergeResult(r.result)
		}
	}

	// Create a reader pool once per bootstrap as we don't really want to
	// allocate and keep around readers outside of the bootstrapping process,
	// hence why its created on demand each time.
	readerPool := newReaderPool(s.newReaderPoolOpts)
	readersCh := make(chan timeWindowReaders)
	go s.enqueueReaders(md, run, runOpts, shardsTimeRanges,
		readerPool, readersCh)
	bootstrapFromDataReadersResult := s.bootstrapFromReaders(md, run, runOpts,
		readerPool, blockRetriever, readersCh)

	// Merge any existing results if necessary
	setOrMergeResult(bootstrapFromDataReadersResult)

	return res, nil
}

func (s *fileSystemSource) newReader() (fs.DataFileSetReader, error) {
	bytesPool := s.opts.ResultOptions().DatabaseBlockOptions().BytesPool()
	return s.newReaderFn(bytesPool, s.fsopts)
}

func (s *fileSystemSource) resolveBlockRetrieverAndCacheDataShardIndices(
	md namespace.Metadata,
	blockRetrieverMgr block.DatabaseBlockRetrieverManager,
	shards []uint32,
) (
	block.DatabaseBlockRetriever,
	error,
) {
	var blockRetriever block.DatabaseBlockRetriever

	s.log.WithFields(
		xlog.NewField("namespace", md.ID().String()),
	).Infof("filesystem bootstrapper resolving block retriever")

	var err error
	blockRetriever, err = blockRetrieverMgr.Retriever(md)
	if err != nil {
		return nil, err
	}

	s.log.WithFields(
		xlog.NewField("namespace", md.ID().String()),
		xlog.NewField("shards", len(shards)),
	).Infof("filesystem bootstrapper caching block retriever shard indices")

	err = blockRetriever.CacheShardIndices(shards)
	if err != nil {
		return nil, err
	}

	return blockRetriever, nil
}

func (s *fileSystemSource) bootstrapDataRunResultFromAvailability(
	md namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
) *runResult {
	runResult := newRunResult()
	unfulfilled := runResult.data.Unfulfilled()
	for shard, ranges := range shardsTimeRanges {
		if ranges.IsEmpty() {
			continue
		}
		availability := s.shardAvailability(md.ID(), shard, ranges)
		remaining := ranges.RemoveRanges(availability)
		runResult.data.Add(shard, nil, remaining)
	}
	runResult.data.SetUnfulfilled(unfulfilled)
	return runResult
}

type bootstrapFromIndexPersistedBlocksResult struct {
	fulfilled result.ShardTimeRanges
	result    *runResult
}

func (s *fileSystemSource) bootstrapFromIndexPersistedBlocks(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
) (bootstrapFromIndexPersistedBlocksResult, error) {
	res := bootstrapFromIndexPersistedBlocksResult{
		fulfilled: result.ShardTimeRanges{},
	}

	indexBlockSize := ns.Options().IndexOptions().BlockSize()
	infoFiles := fs.ReadIndexInfoFiles(s.fsopts.FilePathPrefix(), ns.ID(),
		s.fsopts.InfoReaderBufferSize())

	for _, infoFile := range infoFiles {
		if infoFile.Err.Error() != nil {
			s.log.WithFields(
				xlog.NewField("namespace", ns.ID().String()),
				xlog.NewField("error", infoFile.Err.Error()),
				xlog.NewField("shardsTimeRanges", shardsTimeRanges.String()),
				xlog.NewField("filepath", infoFile.Err.Filepath()),
			).Error("unable to read index info file")
			continue
		}

		info := infoFile.Info
		indexBlockStart := xtime.UnixNano(info.BlockStart).ToTime()
		indexBlockRange := xtime.Range{
			Start: indexBlockStart,
			End:   indexBlockStart.Add(indexBlockSize),
		}
		willFulfill := result.ShardTimeRanges{}
		for _, shard := range info.Shards {
			tr, ok := shardsTimeRanges[shard]
			if !ok {
				// No ranges match for this shard
				continue
			}

			iter := tr.Iter()
			for iter.Next() {
				curr := iter.Value()
				intersection, intersects := curr.Intersect(indexBlockRange)
				if !intersects {
					continue
				}
				willFulfill[shard] = willFulfill[shard].AddRange(intersection)
			}
		}

		if willFulfill.IsEmpty() {
			// No matching shard/time ranges with this block
			continue
		}

		segments, err := fs.ReadIndexSegments(fs.ReadIndexSegmentsOptions{
			ReaderOptions: fs.IndexReaderOpenOptions{
				Identifier:  infoFile.ID,
				FileSetType: persist.FileSetFlushType,
			},
			FilesystemOptions: s.fsopts,
		})
		if err != nil {
			s.log.WithFields(
				xlog.NewField("namespace", ns.ID().String()),
				xlog.NewField("error", err.Error()),
				xlog.NewField("blockStart", indexBlockStart.String()),
				xlog.NewField("volumeIndex", infoFile.ID.VolumeIndex),
			).Error("unable to read segments from index fileset")
			continue
		}

		// Track success
		s.metrics.persistedIndexBlocksRead.Inc(1)

		// Record result
		if res.result == nil {
			res.result = newRunResult()
		}
		segmentsFulfilled := willFulfill
		indexBlock := result.NewIndexBlock(indexBlockStart, segments,
			segmentsFulfilled)
		// NB(r): Don't need to call MarkFulfilled on the IndexResults here
		// as we've already passed the ranges fulfilled to the block that
		// we place in the IndexResuts with the call to Add(...)
		res.result.index.Add(indexBlock, nil)
		res.fulfilled.AddRanges(segmentsFulfilled)
	}

	return res, nil
}

func (s *fileSystemSource) shouldPersist(runOpts bootstrap.RunOptions) bool {
	persistConfig := runOpts.PersistConfig()
	return persistConfig.Enabled && persistConfig.FileSetType == persist.FileSetFlushType
}

type timeWindowReaders struct {
	ranges  result.ShardTimeRanges
	readers map[shardID]shardReaders
}

type shardID uint32

type shardReaders struct {
	readers []fs.DataFileSetReader
}

func newTimeWindowReaders(
	ranges result.ShardTimeRanges,
	readers map[shardID]shardReaders,
) timeWindowReaders {
	return timeWindowReaders{
		ranges:  ranges,
		readers: readers,
	}
}

// readerPool is a lean pool that does not allocate
// instances up front and is used per bootstrap call
type readerPool struct {
	sync.Mutex
	alloc        readerPoolAllocFn
	values       []fs.DataFileSetReader
	disableReuse bool
}

type readerPoolAllocFn func() (fs.DataFileSetReader, error)

type newReaderPoolOptions struct {
	alloc        readerPoolAllocFn
	disableReuse bool
}

func newReaderPool(
	opts newReaderPoolOptions,
) *readerPool {
	return &readerPool{alloc: opts.alloc, disableReuse: opts.disableReuse}
}

func (p *readerPool) get() (fs.DataFileSetReader, error) {
	p.Lock()
	if len(p.values) == 0 {
		p.Unlock()
		return p.alloc()
	}
	length := len(p.values)
	value := p.values[length-1]
	p.values[length-1] = nil
	p.values = p.values[:length-1]
	p.Unlock()
	return value, nil
}

func (p *readerPool) put(r fs.DataFileSetReader) {
	if p.disableReuse {
		return // Useful for tests
	}
	p.Lock()
	p.values = append(p.values, r)
	p.Unlock()
}

type runResult struct {
	sync.RWMutex
	data  result.DataBootstrapResult
	index result.IndexBootstrapResult
}

func newRunResult() *runResult {
	return &runResult{
		data:  result.NewDataBootstrapResult(),
		index: result.NewIndexBootstrapResult(),
	}
}

func (r *runResult) getOrAddDataShardResult(
	shard uint32,
	capacity int,
	ropts result.Options,
) result.ShardResult {
	// Only called once per shard so ok to acquire write lock immediately
	r.Lock()
	defer r.Unlock()

	dataResults := r.data.ShardResults()
	shardResult, exists := dataResults[shard]
	if exists {
		return shardResult
	}

	// NB(r): Wait until we have a reader to initialize the shard result
	// to be able to somewhat estimate the size of it.
	shardResult = result.NewShardResult(capacity, ropts)
	dataResults[shard] = shardResult

	return shardResult
}

func (r *runResult) getOrAddIndexSegment(
	start time.Time,
	ns namespace.Metadata,
	ropts result.Options,
) (segment.MutableSegment, error) {
	// Only called once per shard so ok to acquire write lock immediately
	r.Lock()
	defer r.Unlock()

	indexResults := r.index.IndexResults()
	indexBlockSegment, err := indexResults.GetOrAddSegment(start,
		ns.Options().IndexOptions(), ropts)
	return indexBlockSegment, err
}

func (r *runResult) mergedResult(other *runResult) *runResult {
	return &runResult{
		data:  result.MergedDataBootstrapResult(r.data, other.data),
		index: result.MergedIndexBootstrapResult(r.index, other.index),
	}
}

type shardTimeRangesTimeWindowGroup struct {
	ranges result.ShardTimeRanges
	window xtime.Range
}

func newShardTimeRangesTimeWindowGroups(
	shardTimeRanges result.ShardTimeRanges,
	windowSize time.Duration,
) []shardTimeRangesTimeWindowGroup {
	min, max := shardTimeRanges.MinMax()
	estimate := int(math.Ceil(float64(max.Sub(min)) / float64(windowSize)))
	grouped := make([]shardTimeRangesTimeWindowGroup, 0, estimate)
	for t := min.Truncate(windowSize); t.Before(max); t = t.Add(windowSize) {
		currRange := xtime.Range{
			Start: t,
			End:   minTime(t.Add(windowSize), max),
		}

		group := make(result.ShardTimeRanges)
		for shard, tr := range shardTimeRanges {
			iter := tr.Iter()
			for iter.Next() {
				evaluateRange := iter.Value()
				intersection, intersects := evaluateRange.Intersect(currRange)
				if !intersects {
					continue
				}
				// Add to this range
				group[shard] = group[shard].AddRange(intersection)
			}
		}

		entry := shardTimeRangesTimeWindowGroup{
			ranges: group,
			window: currRange,
		}

		grouped = append(grouped, entry)
	}
	return grouped
}

func minTime(x, y time.Time) time.Time {
	if x.Before(y) {
		return x
	}
	return y
}
