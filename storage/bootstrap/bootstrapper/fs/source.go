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
	"sync"
	"time"

	"github.com/m3db/m3db/persist/encoding/msgpack"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/sync"
	"github.com/m3db/m3x/time"
)

type newFileSetReaderFn func(
	filePathPrefix string,
	readerBufferSize int,
	bytesPool pool.CheckedBytesPool,
	decodingOpts msgpack.DecodingOptions,
) fs.FileSetReader

type fileSystemSource struct {
	opts             Options
	log              xlog.Logger
	filePathPrefix   string
	readerBufferSize int
	decodingOpts     msgpack.DecodingOptions
	newReaderFn      newFileSetReaderFn
	processors       xsync.WorkerPool
}

func newFileSystemSource(prefix string, opts Options) bootstrap.Source {
	processors := xsync.NewWorkerPool(opts.NumProcessors())
	processors.Init()

	fileSystemOpts := opts.FilesystemOptions()
	return &fileSystemSource{
		opts:             opts,
		log:              opts.ResultOptions().InstrumentOptions().Logger(),
		filePathPrefix:   prefix,
		readerBufferSize: fileSystemOpts.ReaderBufferSize(),
		decodingOpts:     fileSystemOpts.DecodingOptions(),
		newReaderFn:      fs.NewReader,
		processors:       processors,
	}
}

func (s *fileSystemSource) Can(strategy bootstrap.Strategy) bool {
	switch strategy {
	case bootstrap.BootstrapSequential:
		return true
	}
	return false
}

func (s *fileSystemSource) Available(
	namespace ts.ID,
	shardsTimeRanges result.ShardTimeRanges,
) result.ShardTimeRanges {
	result := make(map[uint32]xtime.Ranges)
	for shard, ranges := range shardsTimeRanges {
		result[shard] = s.shardAvailability(namespace, shard, ranges)
	}
	return result
}

func (s *fileSystemSource) shardAvailability(
	namespace ts.ID,
	shard uint32,
	targetRangesForShard xtime.Ranges,
) xtime.Ranges {
	if targetRangesForShard == nil {
		return nil
	}

	entries := fs.ReadInfoFiles(s.filePathPrefix, namespace, shard, s.readerBufferSize, s.decodingOpts)
	if len(entries) == 0 {
		return nil
	}

	tr := xtime.NewRanges()
	for i := 0; i < len(entries); i++ {
		info := entries[i]
		t := xtime.FromNanoseconds(info.Start)
		w := time.Duration(info.BlockSize)
		currRange := xtime.Range{Start: t, End: t.Add(w)}
		if targetRangesForShard.Overlaps(currRange) {
			tr = tr.AddRange(currRange)
		}
	}
	return tr
}

func (s *fileSystemSource) enqueueReaders(
	namespace ts.ID,
	shardsTimeRanges result.ShardTimeRanges,
	readerPool *readerPool,
	readersCh chan<- shardReaders,
) {
	for shard, tr := range shardsTimeRanges {
		files := fs.ReadInfoFiles(s.filePathPrefix, namespace, shard, s.readerBufferSize, s.decodingOpts)
		if len(files) == 0 {
			if tr == nil {
				tr = xtime.NewRanges()
			}
			// Use default readers value to indicate no readers for this shard
			readersCh <- shardReaders{shard: shard, tr: tr}
			continue
		}

		readers := make([]fs.FileSetReader, 0, len(files))
		for i := 0; i < len(files); i++ {
			r := readerPool.get()
			t := xtime.FromNanoseconds(files[i].Start)
			if err := r.Open(namespace, shard, t); err != nil {
				s.log.WithFields(
					xlog.NewLogField("shard", shard),
					xlog.NewLogField("time", t.String()),
					xlog.NewLogField("error", err.Error()),
				).Error("unable to open fileset files")
				readerPool.put(r)
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
		readersCh <- shardReaders{shard: shard, tr: tr, readers: readers}
	}

	close(readersCh)
}

func (s *fileSystemSource) bootstrapFromReaders(
	readerPool *readerPool,
	retriever block.DatabaseBlockRetriever,
	readersCh <-chan shardReaders,
) result.BootstrapResult {
	var (
		wg                sync.WaitGroup
		resultLock        sync.RWMutex
		shardRetrieverMgr block.DatabaseShardBlockRetrieverManager
		bootstrapResult   = result.NewBootstrapResult()
		bopts             = s.opts.ResultOptions()
		blockPool         = bopts.DatabaseBlockOptions().DatabaseBlockPool()
	)

	if retriever != nil {
		shardRetrieverMgr = block.NewDatabaseShardBlockRetrieverManager(retriever)
	}

	for shardReaders := range readersCh {
		shardReaders := shardReaders
		wg.Add(1)
		s.processors.Go(func() {
			defer wg.Done()

			var (
				timesWithErrors []time.Time
				shardResult     result.ShardResult
				shardRetriever  block.DatabaseShardBlockRetriever
			)

			shard, tr, readers := shardReaders.shard, shardReaders.tr, shardReaders.readers
			if shardRetrieverMgr != nil {
				shardRetriever = shardRetrieverMgr.ShardRetriever(shard)
			}

			for _, r := range readers {
				if shardResult == nil {
					resultLock.RLock()
					results := bootstrapResult.ShardResults()
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
				}

				var (
					timeRange  = r.Range()
					start      = timeRange.Start
					hasError   = false
					numEntries = r.Entries()
				)
				for i := 0; i < numEntries; i++ {
					var (
						seriesBlock = blockPool.Get()
						entryErr    error
					)
					id, data, checksum, err := r.Read()
					if err != nil {
						entryErr = err
						s.log.WithFields(
							xlog.NewLogField("shard", shard),
							xlog.NewLogField("error", entryErr),
						).Error("reading data file failed")
						hasError = true
						break
					}

					idHash := id.Hash()

					resultLock.RLock()
					series, seriesExists := shardResult.AllSeries()[idHash]
					resultLock.RUnlock()

					if seriesExists {
						// NB(r): In the case the series is already inserted
						// we can avoid holding onto this ID and use the already
						// allocated ID.
						id.Finalize()
						id = series.ID
					}

					if retriever == nil {
						seg := ts.NewSegment(data, nil, ts.FinalizeHead)
						seriesBlock.Reset(start, seg)
					} else {
						data.IncRef()
						length := data.Len()
						data.DecRef()
						data.Finalize()
						data = nil

						metadata := block.RetrievableBlockMetadata{
							ID:       id,
							Length:   length,
							Checksum: checksum,
						}
						seriesBlock.ResetRetrievable(start, shardRetriever, metadata)
					}

					resultLock.Lock()
					if seriesExists {
						series.Blocks.AddBlock(seriesBlock)
					} else {
						shardResult.AddBlock(id, seriesBlock)
					}
					resultLock.Unlock()
				}

				if !hasError {
					if err := r.Validate(); err != nil {
						s.log.WithFields(
							xlog.NewLogField("shard", shard),
							xlog.NewLogField("error", err),
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
				r.Close()
				readerPool.put(r)
			}

			// NB(xichen): this is the exceptional case where we encountered errors due to files
			// being corrupted, which should be fairly rare so we can live with the overhead. We
			// experimented with adding the series to a temporary map and only adding the temporary map
			// to the final result but adding series to large map with string keys is expensive, and
			// the current implementation saves the extra overhead of merging temporary map with the
			// final result.
			if len(timesWithErrors) > 0 {
				s.log.WithFields(
					xlog.NewLogField("shard", shard),
					xlog.NewLogField("timesWithErrors", timesWithErrors),
				).Info("deleting entries from results for times with errors")

				resultLock.Lock()
				shardResult, ok := bootstrapResult.ShardResults()[shard]
				if ok {
					for _, series := range shardResult.AllSeries() {
						for _, t := range timesWithErrors {
							shardResult.RemoveBlockAt(series.ID, t)
						}
					}
				}
				resultLock.Unlock()
			}

			if !tr.IsEmpty() {
				resultLock.Lock()
				unfulfilled := bootstrapResult.Unfulfilled()
				shardUnfulfilled, ok := unfulfilled[shard]
				if !ok {
					shardUnfulfilled = xtime.NewRanges().AddRanges(tr)
				} else {
					shardUnfulfilled = shardUnfulfilled.AddRanges(tr)
				}

				unfulfilled[shard] = shardUnfulfilled
				resultLock.Unlock()
			}
		})
	}

	wg.Wait()

	shardResults := bootstrapResult.ShardResults()
	for shard, results := range shardResults {
		if len(results.AllSeries()) == 0 {
			delete(shardResults, shard)
		}
	}

	return bootstrapResult
}

func (s *fileSystemSource) Read(
	namespace ts.ID,
	shardsTimeRanges result.ShardTimeRanges,
	_ bootstrap.RunOptions,
) (result.BootstrapResult, error) {
	if shardsTimeRanges.IsEmpty() {
		return nil, nil
	}

	var blockRetriever block.DatabaseBlockRetriever
	blockRetrieverMgr := s.opts.DatabaseBlockRetrieverManager()
	if blockRetrieverMgr != nil {
		s.log.WithFields(
			xlog.NewLogField("namespace", namespace.String()),
		).Infof("filesystem bootstrapper resolving block retriever")

		var err error
		blockRetriever, err = blockRetrieverMgr.Retriever(namespace)
		if err != nil {
			return nil, err
		}

		s.log.WithFields(
			xlog.NewLogField("namespace", namespace.String()),
			xlog.NewLogField("shards", len(shardsTimeRanges)),
		).Infof("filesystem bootstrapper caching block retriever shard indices")

		shards := make([]uint32, 0, len(shardsTimeRanges))
		for shard := range shardsTimeRanges {
			shards = append(shards, shard)
		}

		err = blockRetriever.CacheShardIndices(shards)
		if err != nil {
			return nil, err
		}
	}

	s.log.WithFields(
		xlog.NewLogField("shards", len(shardsTimeRanges)),
		xlog.NewLogField("concurrency", s.opts.NumProcessors()),
		xlog.NewLogField("metadataOnly", blockRetriever != nil),
	).Infof("filesystem bootstrapper bootstrapping shards for ranges")
	readerPool := newReaderPool(func() fs.FileSetReader {
		return s.newReaderFn(
			s.filePathPrefix,
			s.readerBufferSize,
			s.opts.ResultOptions().DatabaseBlockOptions().BytesPool(),
			s.decodingOpts,
		)
	})
	readersCh := make(chan shardReaders)
	go s.enqueueReaders(namespace, shardsTimeRanges, readerPool, readersCh)
	return s.bootstrapFromReaders(readerPool, blockRetriever, readersCh), nil
}

type shardReaders struct {
	shard   uint32
	tr      xtime.Ranges
	readers []fs.FileSetReader
}

// readerPool is a lean pool that does not allocate
// instances up front and is used per bootstrap call
type readerPool struct {
	sync.RWMutex
	alloc  readerPoolAllocFn
	values []fs.FileSetReader
}

type readerPoolAllocFn func() fs.FileSetReader

func newReaderPool(alloc readerPoolAllocFn) *readerPool {
	return &readerPool{alloc: alloc}
}

func (p *readerPool) get() fs.FileSetReader {
	p.Lock()
	if len(p.values) == 0 {
		p.Unlock()
		return p.alloc()
	}
	length := len(p.values)
	value := p.values[length-1]
	p.values = p.values[:length-1]
	p.Unlock()
	return value
}

func (p *readerPool) put(r fs.FileSetReader) {
	p.Lock()
	p.values = append(p.values, r)
	p.Unlock()
}
