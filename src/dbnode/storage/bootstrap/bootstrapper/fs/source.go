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
	"sync"
	"time"

	"github.com/m3db/m3db/src/dbnode/persist/fs"
	"github.com/m3db/m3db/src/dbnode/storage/block"
	"github.com/m3db/m3db/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3db/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3db/src/dbnode/storage/index/convert"
	"github.com/m3db/m3db/src/dbnode/storage/namespace"
	"github.com/m3db/m3db/src/dbnode/storage/series"
	"github.com/m3db/m3db/src/dbnode/ts"
	"github.com/m3db/m3ninx/index/segment"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/ident"
	xlog "github.com/m3db/m3x/log"
	"github.com/m3db/m3x/pool"
	xsync "github.com/m3db/m3x/sync"
	xtime "github.com/m3db/m3x/time"
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
	opts        Options
	fsopts      fs.Options
	log         xlog.Logger
	idPool      ident.Pool
	newReaderFn newDataFileSetReaderFn
	processors  xsync.WorkerPool
}

func newFileSystemSource(opts Options) bootstrap.Source {
	processors := xsync.NewWorkerPool(opts.NumProcessors())
	processors.Init()
	return &fileSystemSource{
		opts:        opts,
		fsopts:      opts.FilesystemOptions(),
		log:         opts.ResultOptions().InstrumentOptions().Logger(),
		idPool:      opts.IdentifierPool(),
		newReaderFn: fs.NewReader,
		processors:  processors,
	}
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
) result.ShardTimeRanges {
	return s.availability(md, shardsTimeRanges)
}

func (s *fileSystemSource) ReadData(
	md namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	_ bootstrap.RunOptions,
) (result.DataBootstrapResult, error) {
	r, err := s.read(md, shardsTimeRanges, bootstrapDataRunType)
	if err != nil {
		return nil, err
	}
	return r.data, nil
}

func (s *fileSystemSource) AvailableIndex(
	md namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
) result.ShardTimeRanges {
	return s.availability(md, shardsTimeRanges)
}

func (s *fileSystemSource) ReadIndex(
	md namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	opts bootstrap.RunOptions,
) (result.IndexBootstrapResult, error) {
	r, err := s.read(md, shardsTimeRanges, bootstrapIndexRunType)
	if err != nil {
		return nil, err
	}
	return r.index, nil
}

func (s *fileSystemSource) availability(
	md namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
) result.ShardTimeRanges {
	result := make(map[uint32]xtime.Ranges)
	for shard, ranges := range shardsTimeRanges {
		result[shard] = s.shardAvailability(md.ID(), shard, ranges)
	}
	return result
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

	if len(readInfoFilesResults) == 0 {
		return xtime.Ranges{}
	}

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
	namespace ident.ID,
	shardsTimeRanges result.ShardTimeRanges,
	readerPool *readerPool,
	readersCh chan<- shardReaders,
) {
	for shard, tr := range shardsTimeRanges {
		readInfoFilesResults := fs.ReadInfoFiles(s.fsopts.FilePathPrefix(),
			namespace, shard, s.fsopts.InfoReaderBufferSize(), s.fsopts.DecodingOptions())

		if len(readInfoFilesResults) == 0 {
			// Use default readers value to indicate no readers for this shard
			readersCh <- newShardReaders(shard, tr, nil)
			continue
		}

		readers := make([]fs.DataFileSetReader, 0, len(readInfoFilesResults))
		for i := 0; i < len(readInfoFilesResults); i++ {
			result := readInfoFilesResults[i]
			if result.Err.Error() != nil {
				s.log.WithFields(
					xlog.NewField("shard", shard),
					xlog.NewField("namespace", namespace.String()),
					xlog.NewField("error", result.Err.Error()),
					xlog.NewField("timeRange", tr),
					xlog.NewField("filepath", result.Err.Filepath()),
				).Error("unable to read info files in enqueueReaders")
				continue
			}
			info := result.Info

			r, err := readerPool.get()
			if err != nil {
				s.log.Errorf("unable to get reader from pool")
				readersCh <- newShardReadersErr(shard, tr, err)
				continue
			}
			t := xtime.FromNanoseconds(info.BlockStart)
			openOpts := fs.DataReaderOpenOptions{
				Identifier: fs.FileSetFileIdentifier{
					Namespace:  namespace,
					Shard:      shard,
					BlockStart: t,
				},
			}
			if err := r.Open(openOpts); err != nil {
				s.log.WithFields(
					xlog.NewField("shard", shard),
					xlog.NewField("time", t.String()),
					xlog.NewField("error", err.Error()),
				).Error("unable to open fileset files")
				readerPool.put(r)
				readersCh <- newShardReadersErr(shard, tr, err)
				continue
			}
			timeRange := r.Range()
			if !tr.Overlaps(timeRange) {
				r.Close()
				readerPool.put(r)
				continue
			}
			readers = append(readers, r)
		}
		readersCh <- newShardReaders(shard, tr, readers)
	}

	close(readersCh)
}

func (s *fileSystemSource) bootstrapFromReaders(
	ns namespace.Metadata,
	run runType,
	readerPool *readerPool,
	retriever block.DatabaseBlockRetriever,
	readersCh <-chan shardReaders,
) *runResult {
	var (
		runResult         = newRunResult()
		resultOpts        = s.opts.ResultOptions()
		shardRetrieverMgr block.DatabaseShardBlockRetrieverManager
		wg                sync.WaitGroup
	)
	if retriever != nil {
		shardRetrieverMgr = block.NewDatabaseShardBlockRetrieverManager(retriever)
	}

	for shardReaders := range readersCh {
		shardReaders := shardReaders
		wg.Add(1)
		s.processors.Go(func() {
			s.loadShardReadersDataIntoShardResult(ns, run, runResult,
				resultOpts, shardRetrieverMgr, shardReaders, readerPool)
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
	shard uint32,
	remainingRanges xtime.Ranges,
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
			xlog.NewField("shard", shard),
			xlog.NewField("timesWithErrors", timesWithErrorsString),
		).Info("deleting entries from results for times with errors")

		runResult.Lock()
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
			shardUnfulfilled, ok := unfulfilled[shard]
			if !ok {
				shardUnfulfilled = xtime.Ranges{}.AddRanges(remainingRanges)
			} else {
				shardUnfulfilled = shardUnfulfilled.AddRanges(remainingRanges)
			}
			unfulfilled[shard] = shardUnfulfilled
		}
		runResult.Unlock()
	}
}

func (s *fileSystemSource) tagsFromTagsIter(
	iter ident.TagIterator,
) (ident.Tags, error) {
	tags := s.idPool.Tags()
	for iter.Next() {
		curr := iter.Current()
		tags.Append(s.idPool.CloneTag(curr))
	}
	return tags, iter.Err()
}

func (s *fileSystemSource) loadShardReadersDataIntoShardResult(
	ns namespace.Metadata,
	run runType,
	runResult *runResult,
	ropts result.Options,
	shardRetrieverMgr block.DatabaseShardBlockRetrieverManager,
	shardReaders shardReaders,
	readerPool *readerPool,
) {
	var (
		timesWithErrors   []time.Time
		shardResult       result.ShardResult
		shardRetriever    block.DatabaseShardBlockRetriever
		blockPool         = ropts.DatabaseBlockOptions().DatabaseBlockPool()
		seriesCachePolicy = ropts.SeriesCachePolicy()
		indexBlockSegment segment.MutableSegment
	)

	sr := shardReaders
	shard, tr, readers, err := sr.shard, sr.tr, sr.readers, sr.err
	if err != nil {
		s.markRunResultErrorsAndUnfulfilled(runResult, shard, tr, timesWithErrors)
		return
	}

	if shardRetrieverMgr != nil {
		shardRetriever = shardRetrieverMgr.ShardRetriever(shard)
	}
	if run == bootstrapDataRunType && seriesCachePolicy == series.CacheAllMetadata && shardRetriever == nil {
		s.log.WithFields(
			xlog.NewField("has-shard-retriever-mgr", shardRetrieverMgr != nil),
			xlog.NewField("has-shard-retriever", shardRetriever != nil),
		).Errorf("shard retriever missing for shard: %d", shard)
		s.markRunResultErrorsAndUnfulfilled(runResult, shard, tr, timesWithErrors)
		return
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
			var validateErr error
			switch run {
			case bootstrapDataRunType:
				switch seriesCachePolicy {
				case series.CacheAll:
					validateErr = r.Validate()
				case series.CacheAllMetadata:
					validateErr = r.ValidateMetadata()
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

		if err == nil {
			tr = tr.RemoveRange(timeRange)
		} else {
			s.log.Errorf("%v", err)
			timesWithErrors = append(timesWithErrors, timeRange.Start)
		}
	}

	for _, r := range readers {
		if err := r.Close(); err == nil {
			readerPool.put(r)
		}
	}

	s.markRunResultErrorsAndUnfulfilled(runResult, shard, tr, timesWithErrors)
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
		length      int
		checksum    uint32
		err         error
	)
	switch seriesCachePolicy {
	case series.CacheAll:
		id, tagsIter, data, checksum, err = r.Read()
	case series.CacheAllMetadata:
		id, tagsIter, length, checksum, err = r.ReadMetadata()
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
	runResult.RLock()
	entry, exists = shardResult.AllSeries().Get(id)
	runResult.RUnlock()

	if exists {
		// NB(r): In the case the series is already inserted
		// we can avoid holding onto this ID and use the already
		// allocated ID.
		id.Finalize()
		id = entry.ID
		tags = entry.Tags
	} else {
		tags, err = s.tagsFromTagsIter(tagsIter)
		if err != nil {
			return fmt.Errorf("unable to decode tags: %v", err)
		}
	}
	tagsIter.Close()

	switch seriesCachePolicy {
	case series.CacheAll:
		seg := ts.NewSegment(data, nil, ts.FinalizeHead)
		seriesBlock.Reset(blockStart, blockSize, seg)
	case series.CacheAllMetadata:
		metadata := block.RetrievableBlockMetadata{
			ID:       id,
			Length:   length,
			Checksum: checksum,
		}
		seriesBlock.ResetRetrievable(blockStart, blockSize, shardRetriever, metadata)
	default:
		return fmt.Errorf("invalid series cache policy: %s", seriesCachePolicy.String())
	}

	runResult.Lock()
	if exists {
		entry.Blocks.AddBlock(seriesBlock)
	} else {
		shardResult.AddBlock(id, tags, seriesBlock)
	}
	runResult.Unlock()
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

func (s *fileSystemSource) read(
	md namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	run runType,
) (*runResult, error) {
	var (
		nsID              = md.ID()
		seriesCachePolicy = s.opts.ResultOptions().SeriesCachePolicy()
		blockRetriever    block.DatabaseBlockRetriever
	)
	if shardsTimeRanges.IsEmpty() {
		return newRunResult(), nil
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
		case series.CacheAllMetadata:
			// Need to check block retriever available
			if blockRetriever == nil {
				return nil, fmt.Errorf(
					"missing block retriever when using series cache metadata for namespace: %s",
					nsID.String())
			}
		default:
			// Unless we're caching all series (or all series metadata) in memory, we
			// return just the availability of the files we have
			result := s.bootstrapDataRunResultFromAvailability(md, shardsTimeRanges)
			return result, nil
		}
	}

	s.log.WithFields(
		xlog.NewField("shards", len(shardsTimeRanges)),
		xlog.NewField("concurrency", s.opts.NumProcessors()),
		xlog.NewField("metadata-only", blockRetriever != nil),
	).Infof("filesystem bootstrapper bootstrapping shards for ranges")
	bytesPool := s.opts.ResultOptions().DatabaseBlockOptions().BytesPool()

	// Create a reader pool once per bootstrap as we don't really want to
	// allocate and keep around readers outside of the bootstrapping process,
	// hence why its created on demand each time.
	readerPool := newReaderPool(func() (fs.DataFileSetReader, error) {
		return s.newReaderFn(bytesPool, s.fsopts)
	})
	readersCh := make(chan shardReaders)
	go s.enqueueReaders(nsID, shardsTimeRanges, readerPool, readersCh)
	return s.bootstrapFromReaders(md, run, readerPool, blockRetriever, readersCh), nil
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

type bootstrapFromIndexBlocksResult struct {
	fulfilled result.ShardTimeRanges
}

func (s *fileSystemSource) bootstrapIndexRunResultFromIndexBlocks(
	md namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
) (bootstrapFromIndexBlocksResult, error) {
	return bootstrapFromIndexBlocksResult{}, nil
}

type shardReaders struct {
	shard   uint32
	tr      xtime.Ranges
	readers []fs.DataFileSetReader
	err     error
}

func newShardReaders(shard uint32, tr xtime.Ranges, readers []fs.DataFileSetReader) shardReaders {
	return shardReaders{
		shard:   shard,
		tr:      tr,
		readers: readers,
	}
}

func newShardReadersErr(shard uint32, tr xtime.Ranges, err error) shardReaders {
	return shardReaders{
		shard: shard,
		tr:    tr,
		err:   err,
	}
}

// readerPool is a lean pool that does not allocate
// instances up front and is used per bootstrap call
type readerPool struct {
	sync.Mutex
	alloc  readerPoolAllocFn
	values []fs.DataFileSetReader
}

type readerPoolAllocFn func() (fs.DataFileSetReader, error)

func newReaderPool(alloc readerPoolAllocFn) *readerPool {
	return &readerPool{alloc: alloc}
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
