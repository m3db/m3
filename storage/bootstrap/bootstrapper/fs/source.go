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

	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/storage/series"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3ninx/doc"
	"github.com/m3db/m3ninx/index/segment"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/ident"
	xlog "github.com/m3db/m3x/log"
	"github.com/m3db/m3x/pool"
	xsync "github.com/m3db/m3x/sync"
	xtime "github.com/m3db/m3x/time"
)

type newDataFileSetReaderFn func(
	bytesPool pool.CheckedBytesPool,
	opts fs.Options,
) (fs.DataFileSetReader, error)

type runType int

const (
	bootstrapDataRunType runType = iota
	bootstrapIndexRunType
)

type runResult struct {
	data  result.DataBootstrapResult
	index result.IndexBootstrapResult
}

func newRunResult() *runResult {
	return &runResult{
		data:  result.NewDataBootstrapResult(),
		index: result.NewIndexBootstrapResult(),
	}
}

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
	r, err := s.read(md, shardsTimeRanges, bootstrapDataRunType)
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
		wg                sync.WaitGroup
		resultLock        = &sync.RWMutex{}
		shardRetrieverMgr block.DatabaseShardBlockRetrieverManager
		runResult         = newRunResult()
		bopts             = s.opts.ResultOptions()
	)

	if retriever != nil {
		shardRetrieverMgr = block.NewDatabaseShardBlockRetrieverManager(retriever)
	}

	for shardReaders := range readersCh {
		shardReaders := shardReaders
		wg.Add(1)
		s.processors.Go(func() {
			defer wg.Done()
			s.loadShardReadersDataIntoShardResult(ns, run, runResult, resultLock,
				bopts, shardRetrieverMgr, shardReaders, readerPool)
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

// handleErrorsAndUnfulfilled checks the list of times that had errors and makes
// sure that we don't return any blocks or bloom filters for them. In addition,
// it looks at any remaining (unfulfilled) ranges and makes sure they're marked
// as unfulfilled
func (s *fileSystemSource) handleErrorsAndUnfulfilled(
	resultLock *sync.RWMutex,
	runResult *runResult,
	shard uint32,
	remainingRanges xtime.Ranges,
	shardResult result.ShardResult,
	timesWithErrors []time.Time,
) {
	// NB(xichen): this is the exceptional case where we encountered errors due to files
	// being corrupted, which should be fairly rare so we can live with the overhead. We
	// experimented with adding the series to a temporary map and only adding the temporary map
	// to the final result but adding series to large map with string keys is expensive, and
	// the current implementation saves the extra overhead of merging temporary map with the
	// final result.
	if len(timesWithErrors) > 0 {
		s.log.WithFields(
			xlog.NewField("shard", shard),
			xlog.NewField("timesWithErrors", timesWithErrors),
		).Info("deleting entries from results for times with errors")

		resultLock.Lock()
		shardResult, ok := runResult.data.ShardResults()[shard]
		if ok {
			for _, entry := range shardResult.AllSeries().Iter() {
				series := entry.Value()
				for _, t := range timesWithErrors {
					shardResult.RemoveBlockAt(series.ID, t)
				}
			}
		}
		resultLock.Unlock()
	}

	if !remainingRanges.IsEmpty() {
		resultLock.Lock()
		unfulfilled := runResult.data.Unfulfilled()
		shardUnfulfilled, ok := unfulfilled[shard]
		if !ok {
			shardUnfulfilled = xtime.Ranges{}.AddRanges(remainingRanges)
		} else {
			shardUnfulfilled = shardUnfulfilled.AddRanges(remainingRanges)
		}

		unfulfilled[shard] = shardUnfulfilled
		resultLock.Unlock()
	}
}

func (s *fileSystemSource) tagsFromTagsIter(
	iter ident.TagIterator,
) (ident.Tags, error) {
	tags := make(ident.Tags, 0, iter.Remaining())
	for iter.Next() {
		curr := iter.Current()
		tags = append(tags, s.idPool.CloneTag(curr))
	}
	return tags, iter.Err()
}

func (s *fileSystemSource) loadShardReadersDataIntoShardResult(
	ns namespace.Metadata,
	run runType,
	runResult *runResult,
	resultLock *sync.RWMutex,
	bopts result.Options,
	shardRetrieverMgr block.DatabaseShardBlockRetrieverManager,
	shardReaders shardReaders,
	readerPool *readerPool,
) {
	var (
		timesWithErrors   []time.Time
		shardResult       result.ShardResult
		shardSeries       *result.Map
		shardRetriever    block.DatabaseShardBlockRetriever
		blockPool         = bopts.DatabaseBlockOptions().DatabaseBlockPool()
		seriesCachePolicy = bopts.SeriesCachePolicy()
		indexBlockSegment segment.MutableSegment
	)

	shard, tr, readers, err := shardReaders.shard, shardReaders.tr, shardReaders.readers, shardReaders.err
	if err != nil {
		s.handleErrorsAndUnfulfilled(resultLock, runResult, shard, tr,
			shardResult, timesWithErrors)
		return
	}

	if shardRetrieverMgr != nil {
		shardRetriever = shardRetrieverMgr.ShardRetriever(shard)
	}
	if seriesCachePolicy == series.CacheAllMetadata && shardRetriever == nil {
		s.log.Errorf("shard retriever missing for shard: %d", shard)
		s.handleErrorsAndUnfulfilled(resultLock, runResult, shard, tr,
			shardResult, timesWithErrors)
		return
	}

	for _, r := range readers {
		var (
			timeRange = r.Range()
			start     = timeRange.Start
			hasError  bool
		)
		switch run {
		case bootstrapDataRunType:
			if shardResult == nil {
				resultLock.RLock()
				results := runResult.data.ShardResults()
				var exists bool
				shardResult, exists = results[shard]
				resultLock.RUnlock()

				if !exists {
					resultLock.Lock()
					shardResult, exists = results[shard]
					if !exists {
						// NB(r): Wait until we have a reader to initialize the shard result
						// to be able to somewhat estimate the size of it.
						shardResult = result.NewShardResult(r.Entries(), bopts)
						results[shard] = shardResult
					}
					resultLock.Unlock()
				}

				shardSeries = shardResult.AllSeries()
			}
		case bootstrapIndexRunType:
			resultLock.Lock()
			results := runResult.index.IndexResults()
			indexBlockSegment, err = results.GetOrAddSegment(start,
				ns.Options().IndexOptions(), bopts)
			resultLock.Unlock()
			if err != nil {
				s.log.Errorf("unable to create index segment: %v", err)
				hasError = true
			}
		default:
			// Unreachable unless an internal method calls with a run type casted from int
			panic(fmt.Errorf("invalid run type: %d", run))
		}

		var numEntries int
		if !hasError {
			numEntries = r.Entries()
		}
		for i := 0; i < numEntries; i++ {
			if run == bootstrapIndexRunType {
				// If performing index run, then simply read the metadata and add to segment
				id, tagsIter, _, _, err := r.ReadMetadata()
				if err != nil {
					s.log.Errorf("unable read metadata: %v", err)
					hasError = true
					continue
				}

				idBytes := id.Bytes()

				resultLock.RLock()
				exists, err := indexBlockSegment.ContainsID(idBytes)
				resultLock.RUnlock()

				if err == nil && !exists {
					resultLock.Lock()
					exists, err = indexBlockSegment.ContainsID(idBytes)
					if err == nil && !exists {
						d := doc.Document{
							ID:     append([]byte(nil), idBytes...),
							Fields: make(doc.Fields, 0, tagsIter.Remaining()),
						}
						for tagsIter.Next() {
							curr := tagsIter.Current()
							d.Fields = append(d.Fields, doc.Field{
								Name:  append([]byte(nil), curr.Name.Bytes()...),
								Value: append([]byte(nil), curr.Value.Bytes()...),
							})
						}
						err = tagsIter.Err()
						if err == nil {
							_, err = indexBlockSegment.Insert(d)
						}
					}
					resultLock.Unlock()
				}

				// Finalize the ID and tags
				id.Finalize()
				tagsIter.Close()

				if err != nil {
					s.log.Errorf("unable to add doc to segment: %v", err)
					hasError = true
				}
				continue
			}

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
				s.log.WithFields(
					xlog.NewField("shard", shard),
					xlog.NewField("seriesCachePolicy", seriesCachePolicy.String()),
				).Error("invalid series cache policy: expected CacheAll or CacheAllMetadata")
				hasError = true
			}
			if hasError {
				break
			}

			if err != nil {
				s.log.WithFields(
					xlog.NewField("shard", shard),
					xlog.NewField("error", err.Error()),
				).Error("reading data file failed")
				hasError = true
				break
			}

			var (
				entry  result.DatabaseSeriesBlocks
				tags   ident.Tags
				exists bool
			)
			resultLock.RLock()
			entry, exists = shardSeries.Get(id)
			resultLock.RUnlock()

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
					s.log.Errorf("unable to decode tags: %v", err)
					hasError = true
				}
			}
			tagsIter.Close()
			if hasError {
				break
			}

			switch seriesCachePolicy {
			case series.CacheAll:
				seg := ts.NewSegment(data, nil, ts.FinalizeHead)
				seriesBlock.Reset(start, seg)
			case series.CacheAllMetadata:
				metadata := block.RetrievableBlockMetadata{
					ID:       id,
					Length:   length,
					Checksum: checksum,
				}
				seriesBlock.ResetRetrievable(start, shardRetriever, metadata)
			default:
				s.log.WithFields(
					xlog.NewField("shard", shard),
					xlog.NewField("seriesCachePolicy", seriesCachePolicy.String()),
				).Error("invalid series cache policy: expected CacheAll or CacheAllMetadata")
				hasError = true
			}
			if hasError {
				break
			}

			resultLock.Lock()
			if exists {
				entry.Blocks.AddBlock(seriesBlock)
			} else {
				shardResult.AddBlock(id, tags, seriesBlock)
			}
			resultLock.Unlock()
		}

		if !hasError {
			var validateErr error
			switch seriesCachePolicy {
			case series.CacheAll:
				validateErr = r.Validate()
			case series.CacheAllMetadata:
				validateErr = r.ValidateMetadata()
			default:
				s.log.WithFields(
					xlog.NewField("shard", shard),
					xlog.NewField("seriesCachePolicy", seriesCachePolicy.String()),
				).Error("invalid series cache policy: expected CacheAll or CacheAllMetadata")
				hasError = true
			}
			if validateErr != nil {
				s.log.WithFields(
					xlog.NewField("shard", shard),
					xlog.NewField("error", validateErr.Error()),
				).Error("data validation failed")
				hasError = true
			}
		}

		if !hasError {
			tr = tr.RemoveRange(timeRange)
		} else {
			timesWithErrors = append(timesWithErrors, timeRange.Start)
		}
	}

	for _, r := range readers {
		if err := r.Close(); err == nil {
			readerPool.put(r)
		}
	}

	s.handleErrorsAndUnfulfilled(resultLock, runResult, shard,
		tr, shardResult, timesWithErrors)
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
		xlog.NewField("metadataOnly", blockRetriever != nil),
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
