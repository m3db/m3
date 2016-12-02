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

	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/sync"
	"github.com/m3db/m3x/time"
)

type newFileSetReaderFn func(filePathPrefix string, readerBufferSize int, bytesPool pool.BytesPool) fs.FileSetReader

type fileSystemSource struct {
	opts             Options
	log              xlog.Logger
	filePathPrefix   string
	readerBufferSize int
	newReaderFn      newFileSetReaderFn
	ioWorkers        xsync.WorkerPool
	processors       xsync.WorkerPool
}

func newFileSystemSource(prefix string, opts Options) bootstrap.Source {
	ioWorkers := xsync.NewWorkerPool(opts.NumIOWorkers())
	ioWorkers.Init()

	processors := xsync.NewWorkerPool(opts.NumProcessors())
	processors.Init()

	return &fileSystemSource{
		opts:             opts,
		log:              opts.BootstrapOptions().InstrumentOptions().Logger(),
		filePathPrefix:   prefix,
		readerBufferSize: opts.FilesystemOptions().ReaderBufferSize(),
		newReaderFn:      fs.NewReader,
		ioWorkers:        ioWorkers,
		processors:       processors,
	}
}

func (s *fileSystemSource) Can(strategy bootstrap.Strategy) bool {
	switch strategy {
	case bootstrap.BootstrapParallel, bootstrap.BootstrapSequential:
		return true
	}
	return false
}

func (s *fileSystemSource) Available(
	namespace ts.ID,
	shardsTimeRanges bootstrap.ShardTimeRanges,
) bootstrap.ShardTimeRanges {
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

	entries := fs.ReadInfoFiles(s.filePathPrefix, namespace, shard, s.readerBufferSize)
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
	shardsTimeRanges bootstrap.ShardTimeRanges,
	readerPool *readerPool,
	readersCh chan<- shardReaders,
) {
	var wg sync.WaitGroup

	for shard, tr := range shardsTimeRanges {
		shard, tr := shard, tr
		wg.Add(1)
		s.ioWorkers.Go(func() {
			defer wg.Done()

			var files []string
			fs.ForEachInfoFile(s.filePathPrefix, namespace, shard, s.readerBufferSize, func(fname string, _ []byte) {
				files = append(files, fname)
			})

			if len(files) == 0 {
				// Use default readers value to indicate no readers for this shard
				readersCh <- shardReaders{shard: shard, tr: tr}
				return
			}

			readers := make([]fs.FileSetReader, 0, len(files))
			for i := 0; i < len(files); i++ {
				t, err := fs.TimeFromFileName(files[i])
				if err != nil {
					s.log.WithFields(
						xlog.NewLogField("shard", shard),
						xlog.NewLogField("file", files[i]),
						xlog.NewLogField("error", err.Error()),
					).Error("unable to get time from info file")
					continue
				}
				r := readerPool.get()
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
		})
	}

	wg.Wait()
	close(readersCh)
}

func (s *fileSystemSource) bootstrapFromReaders(
	readerPool *readerPool,
	readersCh <-chan shardReaders,
) bootstrap.Result {
	var (
		wg         sync.WaitGroup
		resultLock sync.Mutex
		result     = bootstrap.NewResult()
		bopts      = s.opts.BootstrapOptions()
	)

	for shardReaders := range readersCh {
		shardReaders := shardReaders
		wg.Add(1)
		s.processors.Go(func() {
			defer wg.Done()

			var (
				seriesMap       bootstrap.ShardResult
				timesWithErrors []time.Time
			)

			shard, tr, readers := shardReaders.shard, shardReaders.tr, shardReaders.readers
			for _, r := range readers {
				if seriesMap == nil {
					// Delay initializing seriesMap until we have a good idea of its capacity
					seriesMap = bootstrap.NewShardResult(r.Entries(), bopts)
				}

				var (
					timeRange  = r.Range()
					hasError   = false
					numEntries = r.Entries()
				)

				for i := 0; i < numEntries; i++ {
					id, data, err := r.Read()
					if err != nil {
						s.log.WithFields(
							xlog.NewLogField("shard", shard),
							xlog.NewLogField("error", err),
						).Error("reading data file failed")
						hasError = true
						break
					}

					encoder := bopts.DatabaseBlockOptions().EncoderPool().Get()
					encoder.ResetSetData(timeRange.Start, data, false)
					block := bopts.DatabaseBlockOptions().DatabaseBlockPool().Get()
					block.Reset(timeRange.Start, encoder)
					seriesMap.AddBlock(id, block)
					block.Seal()
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
			if seriesMap != nil && len(timesWithErrors) > 0 {
				s.log.WithFields(
					xlog.NewLogField("shard", shard),
					xlog.NewLogField("timesWithErrors", timesWithErrors),
				).Info("deleting entries from results for times with errors")

				allSeries := seriesMap.AllSeries()
				for id, series := range allSeries {
					for _, t := range timesWithErrors {
						series.Blocks.RemoveBlockAt(t)
					}
					if series.Blocks.Len() == 0 {
						delete(allSeries, id)
					}
				}
			}

			resultLock.Lock()
			result.Add(shard, seriesMap, tr)
			resultLock.Unlock()
		})
	}

	wg.Wait()
	return result
}

func (s *fileSystemSource) Read(
	namespace ts.ID,
	shardsTimeRanges bootstrap.ShardTimeRanges,
) (bootstrap.Result, error) {
	if shardsTimeRanges.IsEmpty() {
		return nil, nil
	}

	readerPool := newReaderPool(func() fs.FileSetReader {
		return s.newReaderFn(s.filePathPrefix, s.readerBufferSize, s.opts.BootstrapOptions().
			DatabaseBlockOptions().BytesPool())
	})
	readersCh := make(chan shardReaders)
	go s.enqueueReaders(namespace, shardsTimeRanges, readerPool, readersCh)
	return s.bootstrapFromReaders(readerPool, readersCh), nil
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
