package bootstrapper

import (
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
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
	shouldPersistIndexBootstrap bool,
	blockSize time.Duration,
	logger *zap.Logger,
) {
	// Close the readers ch if and only if all readers are enqueued.
	defer close(readersCh)

	if !shouldPersistIndexBootstrap {
		// Normal run, open readers
		enqueueReadersGroupedByBlockSize(ns, runOpts, fsOpts,
			shardsTimeRanges, readerPool, readersCh, blockSize, logger)
		return
	}

	// If the run is an index bootstrap with the persist configuration enabled
	// then we need to write out the metadata into FSTs that we store on disk,
	// to avoid creating any one single huge FST at once we bucket the
	// shards into number of buckets.
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
			// greater than the number of shards.
			continue
		}
		enqueueReadersGroupedByBlockSize(ns, runOpts, fsOpts,
			bucket, readerPool, readersCh, blockSize, logger)
	}
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
		readers := make(map[ShardID]ShardReaders, len(group.Ranges))
		for shard, tr := range group.Ranges {
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
		ns.ID(), shard, fsOpts.InfoReaderBufferSize(), fsOpts.DecodingOptions())
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

// Put returns a fileset reader back the the pool in synchronized fashion.
func (p *ReaderPool) Put(r fs.DataFileSetReader) {
	if p.disableReuse {
		// Useful for tests.
		return
	}
	p.Lock()
	p.values = append(p.values, r)
	p.Unlock()
}
