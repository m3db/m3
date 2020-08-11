// Copyright (c) 2020 Uber Technologies, Inc.
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

package bootstrapper

import (
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	xtime "github.com/m3db/m3/src/x/time"

	"go.uber.org/zap"
)

// TimeWindowReaders are grouped by data block.
type TimeWindowReaders struct {
	Ranges  result.ShardTimeRanges
	Readers map[ShardID]ShardReaders
}

// ShardID is the shard #.
type ShardID uint32

// ShardReaders are the fileset readers for a shard.
type ShardReaders struct {
	Readers []fs.DataFileSetReader
}

func newTimeWindowReaders(
	ranges result.ShardTimeRanges,
	readers map[ShardID]ShardReaders,
) TimeWindowReaders {
	return TimeWindowReaders{
		Ranges:  ranges,
		Readers: readers,
	}
}

// EnqueueReaders into a readers channel grouped by data block.
func EnqueueReaders(
	ns namespace.Metadata,
	runOpts bootstrap.RunOptions,
	runtimeOpts runtime.Options,
	fsOpts fs.Options,
	shardsTimeRanges result.ShardTimeRanges,
	readerPool *ReaderPool,
	readersCh chan<- TimeWindowReaders,
	blockSize time.Duration,
	logger *zap.Logger,
) {
	// Close the readers ch if and only if all readers are enqueued.
	defer close(readersCh)

	// Normal run, open readers
	enqueueReadersGroupedByBlockSize(ns, runOpts, fsOpts,
		shardsTimeRanges, readerPool, readersCh, blockSize, logger)
}

func enqueueReadersGroupedByBlockSize(
	ns namespace.Metadata,
	runOpts bootstrap.RunOptions,
	fsOpts fs.Options,
	shardTimeRanges result.ShardTimeRanges,
	readerPool *ReaderPool,
	readersCh chan<- TimeWindowReaders,
	blockSize time.Duration,
	logger *zap.Logger,
) {
	// Group them by block size.
	groupFn := NewShardTimeRangesTimeWindowGroups
	groupedByBlockSize := groupFn(shardTimeRanges, blockSize)

	// Now enqueue across all shards by block size.
	for _, group := range groupedByBlockSize {
		readers := make(map[ShardID]ShardReaders, group.Ranges.Len())
		for shard, tr := range group.Ranges.Iter() {
			shardReaders := newShardReaders(ns, fsOpts, readerPool, shard, tr, logger)
			readers[ShardID(shard)] = shardReaders
		}
		readersCh <- newTimeWindowReaders(group.Ranges, readers)
	}
}

func newShardReaders(
	ns namespace.Metadata,
	fsOpts fs.Options,
	readerPool *ReaderPool,
	shard uint32,
	tr xtime.Ranges,
	logger *zap.Logger,
) ShardReaders {
	readInfoFilesResults := fs.ReadInfoFiles(fsOpts.FilePathPrefix(),
		ns.ID(), shard, fsOpts.InfoReaderBufferSize(), fsOpts.DecodingOptions(), persist.FileSetFlushType)
	if len(readInfoFilesResults) == 0 {
		// No readers.
		return ShardReaders{}
	}

	readers := make([]fs.DataFileSetReader, 0, len(readInfoFilesResults))
	for i := 0; i < len(readInfoFilesResults); i++ {
		result := readInfoFilesResults[i]
		if err := result.Err.Error(); err != nil {
			logger.Error("fs bootstrapper unable to read info file",
				zap.Uint32("shard", shard),
				zap.Stringer("namespace", ns.ID()),
				zap.Error(err),
				zap.String("timeRange", tr.String()),
				zap.String("path", result.Err.Filepath()),
			)
			// Errors are marked unfulfilled by markRunResultErrorsAndUnfulfilled
			// and will be re-attempted by the next bootstrapper.
			continue
		}

		info := result.Info
		blockStart := xtime.FromNanoseconds(info.BlockStart)
		if !tr.Overlaps(xtime.Range{
			Start: blockStart,
			End:   blockStart.Add(ns.Options().RetentionOptions().BlockSize()),
		}) {
			// Errors are marked unfulfilled by markRunResultErrorsAndUnfulfilled
			// and will be re-attempted by the next bootstrapper.
			continue
		}

		r, err := readerPool.Get()
		if err != nil {
			logger.Error("unable to get reader from pool")
			// Errors are marked unfulfilled by markRunResultErrorsAndUnfulfilled
			// and will be re-attempted by the next bootstrapper.
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
			logger.Error("unable to open fileset files",
				zap.Uint32("shard", shard),
				zap.Time("blockStart", blockStart),
				zap.Error(err),
			)
			readerPool.Put(r)
			// Errors are marked unfulfilled by markRunResultErrorsAndUnfulfilled
			// and will be re-attempted by the next bootstrapper.
			continue
		}

		readers = append(readers, r)
	}

	return ShardReaders{Readers: readers}
}

// ReaderPool is a lean pool that does not allocate
// instances up front and is used per bootstrap call.
type ReaderPool struct {
	sync.Mutex
	alloc        ReaderPoolAllocFn
	values       []fs.DataFileSetReader
	disableReuse bool
}

// ReaderPoolAllocFn allocates a new fileset reader.
type ReaderPoolAllocFn func() (fs.DataFileSetReader, error)

// NewReaderPoolOptions contains reader pool options.
type NewReaderPoolOptions struct {
	Alloc        ReaderPoolAllocFn
	DisableReuse bool
}

// NewReaderPool creates a new share-able fileset reader pool
func NewReaderPool(
	opts NewReaderPoolOptions,
) *ReaderPool {
	return &ReaderPool{alloc: opts.Alloc, disableReuse: opts.DisableReuse}
}

// Get gets a fileset reader from the pool in synchronized fashion.
func (p *ReaderPool) Get() (fs.DataFileSetReader, error) {
	p.Lock()
	defer p.Unlock()
	if len(p.values) == 0 {
		return p.alloc()
	}
	length := len(p.values)
	value := p.values[length-1]
	p.values[length-1] = nil
	p.values = p.values[:length-1]
	return value, nil
}

// Put returns a fileset reader back the the pool in synchronized fashion.
func (p *ReaderPool) Put(r fs.DataFileSetReader) {
	if p.disableReuse {
		// Useful for tests.
		return
	}
	p.Lock()
	defer p.Unlock()
	p.values = append(p.values, r)
}
