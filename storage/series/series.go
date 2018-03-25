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
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3db/digest"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3db/x/xio"
	"github.com/m3db/m3x/context"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/ident"
	xlog "github.com/m3db/m3x/log"
	xtime "github.com/m3db/m3x/time"
)

type bootstrapState int

const (
	bootstrapNotStarted bootstrapState = iota
	bootstrapping
	bootstrapped
)

var (
	// ErrSeriesAllDatapointsExpired is returned on tick when all datapoints are expired
	ErrSeriesAllDatapointsExpired = errors.New("series datapoints are all expired")

	errSeriesIsBootstrapping = errors.New("series is bootstrapping")
	errSeriesNotBootstrapped = errors.New("series is not yet bootstrapped")
)

type dbSeries struct {
	sync.RWMutex
	opts Options

	// NB(r): One should audit all places that access the
	// series ID before changing ownership semantics (e.g.
	// pooling the ID rather than releasing it to the GC on
	// calling series.Reset()).
	id   ident.ID
	tags ident.Tags

	buffer                      databaseBuffer
	blocks                      block.DatabaseSeriesBlocks
	bs                          bootstrapState
	blockRetriever              QueryableBlockRetriever
	onRetrieveBlock             block.OnRetrieveBlock
	blockOnEvictedFromWiredList block.OnEvictedFromWiredList
	pool                        DatabaseSeriesPool
}

// NewDatabaseSeries creates a new database series
func NewDatabaseSeries(id ident.ID, tags ident.Tags, opts Options) DatabaseSeries {
	s := newDatabaseSeries()
	s.Reset(id, tags, nil, nil, nil, opts)
	return s
}

// newPooledDatabaseSeries creates a new pooled database series
func newPooledDatabaseSeries(pool DatabaseSeriesPool) DatabaseSeries {
	series := newDatabaseSeries()
	series.pool = pool
	return series
}

// NB(prateek): dbSeries.Reset(...) must be called upon the returned
// object prior to use.
func newDatabaseSeries() *dbSeries {
	series := &dbSeries{
		blocks: block.NewDatabaseSeriesBlocks(0),
		bs:     bootstrapNotStarted,
	}
	series.buffer = newDatabaseBuffer(series.bufferDrained)
	return series
}

func (s *dbSeries) now() time.Time {
	nowFn := s.opts.ClockOptions().NowFn()
	return nowFn()
}

func (s *dbSeries) ID() ident.ID {
	s.RLock()
	id := s.id
	s.RUnlock()
	return id
}

func (s *dbSeries) Tags() ident.Tags {
	s.RLock()
	tags := s.tags
	s.RUnlock()
	return tags
}

func (s *dbSeries) Tick() (TickResult, error) {
	var r TickResult

	s.Lock()

	bufferResult := s.buffer.Tick()
	r.MergedOutOfOrderBlocks = bufferResult.mergedOutOfOrderBlocks

	update := s.updateBlocksWithLock()
	r.TickStatus = update.TickStatus
	r.MadeExpiredBlocks, r.MadeUnwiredBlocks =
		update.madeExpiredBlocks, update.madeUnwiredBlocks

	s.Unlock()

	if update.ActiveBlocks == 0 {
		return r, ErrSeriesAllDatapointsExpired
	}
	return r, nil
}

type updateBlocksResult struct {
	TickStatus
	madeExpiredBlocks int
	madeUnwiredBlocks int
}

func (s *dbSeries) updateBlocksWithLock() updateBlocksResult {
	var (
		result       updateBlocksResult
		now          = s.now()
		ropts        = s.opts.RetentionOptions()
		retriever    = s.blockRetriever
		cachePolicy  = s.opts.CachePolicy()
		expireCutoff = now.Add(-ropts.RetentionPeriod()).Truncate(ropts.BlockSize())
		wiredTimeout = ropts.BlockDataExpiryAfterNotAccessedPeriod()
	)
	for startNano, currBlock := range s.blocks.AllBlocks() {
		start := startNano.ToTime()
		if start.Before(expireCutoff) {
			s.blocks.RemoveBlockAt(start)
			// If we're using the LRU policy and the block was retrieved from disk,
			// then don't close the block because that is the WiredList's
			// responsibility. The block will hang around the WiredList until
			// it is evicted to make room for something else at which point it
			// will be closed.
			//
			// Note that while we don't close the block, we do remove it from the list
			// of blocks. This is so that the series itself can still be expired if this
			// was the last block. The WiredList will still notify the shard/series via
			// the OnEvictedFromWiredList method when it closes the block, but those
			// methods are noops for series/blocks that have already been removed.
			//
			// Also note that while technically the DatabaseBlock protects against double
			// closes, they can be problematic due to pooling. I.E if the following sequence
			// of actions happens:
			// 		1) Tick closes expired block
			// 		2) Block is re-inserted into pool
			// 		3) Block is pulled out of pool and used for critical data
			// 		4) WiredList tries to close the block, not knowing that it has
			// 		   already been closed, and re-opened / re-used leading to
			// 		   unexpected behavior or data loss.
			if cachePolicy == CacheLRU && currBlock.WasRetrievedFromDisk() {
				// Do nothing
			} else {
				currBlock.Close()
			}
			result.madeExpiredBlocks++
			continue
		}

		result.ActiveBlocks++

		if cachePolicy == CacheAll || retriever == nil {
			// Never unwire
			result.WiredBlocks++
			continue
		}

		if cachePolicy == CacheAllMetadata && !currBlock.IsRetrieved() {
			// Already unwired
			result.UnwiredBlocks++
			continue
		}

		// Potentially unwire
		var unwired, shouldUnwire bool
		// IsBlockRetrievable makes sure that the block has been flushed. This
		// prevents us from unwiring blocks that haven't been flushed yet which
		// would cause data loss.
		if retriever.IsBlockRetrievable(start) {
			switch cachePolicy {
			case CacheNone:
				shouldUnwire = true
			case CacheAllMetadata:
				// Apply RecentlyRead logic (CacheAllMetadata is being removed soon)
				fallthrough
			case CacheRecentlyRead:
				sinceLastRead := now.Sub(currBlock.LastReadTime())
				shouldUnwire = sinceLastRead >= wiredTimeout
			case CacheLRU:
				// The tick is responsible for managing the lifecycle of blocks that were not
				// read from disk (not retrieved), and the WiredList will manage those that were
				// retrieved from disk.
				shouldUnwire = !currBlock.WasRetrievedFromDisk()
			default:
				s.opts.InstrumentOptions().Logger().Fatalf(
					"unhandled cache policy in series tick: %s", cachePolicy)
			}
		}

		if shouldUnwire {
			switch cachePolicy {
			case CacheAllMetadata:
				// Keep the metadata but remove contents

				// NB(r): Each block needs shared ref to the series ID
				// or else each block needs to have a copy of the ID
				id := s.id
				metadata := block.RetrievableBlockMetadata{
					ID:       id,
					Length:   currBlock.Len(),
					Checksum: currBlock.Checksum(),
				}
				currBlock.ResetRetrievable(start, retriever, metadata)
			default:
				// Remove the block and it will be looked up later
				s.blocks.RemoveBlockAt(start)
				currBlock.Close()
			}

			unwired = true
			result.madeUnwiredBlocks++
		}

		if unwired {
			result.UnwiredBlocks++
		} else {
			result.WiredBlocks++
		}
	}

	bufferStats := s.buffer.Stats()
	result.ActiveBlocks += bufferStats.wiredBlocks
	result.WiredBlocks += bufferStats.wiredBlocks
	result.OpenBlocks += bufferStats.openBlocks

	return result
}

func (s *dbSeries) IsEmpty() bool {
	s.RLock()
	blocksLen := s.blocks.Len()
	bufferEmpty := s.buffer.IsEmpty()
	s.RUnlock()
	if blocksLen == 0 && bufferEmpty {
		return true
	}
	return false
}

func (s *dbSeries) NumActiveBlocks() int {
	s.RLock()
	value := s.blocks.Len() + s.buffer.Stats().wiredBlocks
	s.RUnlock()
	return value
}

func (s *dbSeries) IsBootstrapped() bool {
	s.RLock()
	state := s.bs
	s.RUnlock()
	return state == bootstrapped
}

func (s *dbSeries) Write(
	ctx context.Context,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	s.Lock()
	err := s.buffer.Write(ctx, timestamp, value, unit, annotation)
	s.Unlock()
	return err
}

func (s *dbSeries) ReadEncoded(
	ctx context.Context,
	start, end time.Time,
) ([][]xio.SegmentReader, error) {
	s.RLock()
	reader := NewReaderUsingRetriever(s.id, s.blockRetriever, s.onRetrieveBlock, s, s.opts)
	r, err := reader.readersWithBlocksMapAndBuffer(ctx, start, end, s.blocks, s.buffer)
	s.RUnlock()
	return r, err
}

func (s *dbSeries) FetchBlocks(
	ctx context.Context,
	starts []time.Time,
) ([]block.FetchBlockResult, error) {
	s.RLock()
	r, err := Reader{
		opts:       s.opts,
		id:         s.id,
		retriever:  s.blockRetriever,
		onRetrieve: s.onRetrieveBlock,
	}.fetchBlocksWithBlocksMapAndBuffer(ctx, starts, s.blocks, s.buffer)
	s.RUnlock()
	return r, err
}

func (s *dbSeries) FetchBlocksMetadata(
	ctx context.Context,
	start, end time.Time,
	opts FetchBlocksMetadataOptions,
) block.FetchBlocksMetadataResult {
	blockSize := s.opts.RetentionOptions().BlockSize()
	res := s.opts.FetchBlockMetadataResultsPool().Get()

	s.RLock()
	defer s.RUnlock()

	blocks := s.blocks.AllBlocks()

	for tNano, b := range blocks {
		t := tNano.ToTime()
		if !start.Before(t.Add(blockSize)) || !t.Before(end) {
			continue
		}
		if !opts.IncludeCachedBlocks && b.IsCachedBlock() {
			// Do not include cached blocks if not specified to, this is
			// to avoid high amounts of duplication if a significant number of
			// blocks are cached in memory when returning blocks metadata
			// from both in-memory and disk structures.
			continue
		}
		var (
			size     int64
			checksum *uint32
			lastRead time.Time
		)
		if opts.IncludeSizes {
			size = int64(b.Len())
		}
		if opts.IncludeChecksums {
			v := b.Checksum()
			checksum = &v
		}
		if opts.IncludeLastRead {
			lastRead = b.LastReadTime()
		}
		res.Add(block.FetchBlockMetadataResult{
			Start:    t,
			Size:     size,
			Checksum: checksum,
			LastRead: lastRead,
		})
	}

	// Iterate over the encoders in the database buffer
	if !s.buffer.IsEmpty() {
		bufferResults := s.buffer.FetchBlocksMetadata(ctx, start, end, opts)
		for _, result := range bufferResults.Results() {
			res.Add(result)
		}
		bufferResults.Close()
	}

	id := s.opts.IdentifierPool().Clone(s.id)

	res.Sort()

	return block.NewFetchBlocksMetadataResult(id, res)
}

func (s *dbSeries) bufferDrained(newBlock block.DatabaseBlock) {
	// NB(r): by the very nature of this method executing we have the
	// lock already. Executing the drain method occurs during a write if the
	// buffer needs to drain or if tick is called and series explicitly asks
	// the buffer to drain ready buckets.
	s.mergeBlockWithLock(s.blocks, newBlock)
}

func (s *dbSeries) mergeBlockWithLock(
	blocks block.DatabaseSeriesBlocks,
	newBlock block.DatabaseBlock,
) {
	blockStart := newBlock.StartTime()

	// If we don't have an existing block just insert the new block.
	existingBlock, ok := blocks.BlockAt(blockStart)
	if !ok {
		s.addBlockWithLock(newBlock)
		return
	}

	// We are performing this in a lock, cannot wait for the existing
	// block potentially to be retrieved from disk, lazily merge the stream.
	newBlock.Merge(existingBlock)
	s.addBlockWithLock(newBlock)
}

func (s *dbSeries) addBlockWithLock(b block.DatabaseBlock) {
	b.SetOnEvictedFromWiredList(s.blockOnEvictedFromWiredList)
	s.blocks.AddBlock(b)
}

// NB(xichen): we are holding a big lock here to drain the in-memory buffer.
// This could potentially be expensive in that we might accumulate a lot of
// data in memory during bootstrapping. If that becomes a problem, we could
// bootstrap in batches, e.g., drain and reset the buffer, drain the streams,
// then repeat, until len(s.pendingBootstrap) is below a given threshold.
func (s *dbSeries) Bootstrap(blocks block.DatabaseSeriesBlocks) error {
	s.Lock()
	defer s.Unlock()

	if s.bs == bootstrapped {
		return nil
	}
	if s.bs == bootstrapping {
		return errSeriesIsBootstrapping
	}

	s.bs = bootstrapping
	existingBlocks := s.blocks

	multiErr := xerrors.NewMultiError()
	if blocks == nil {
		// If no data to bootstrap from then fallback to the empty blocks map
		blocks = existingBlocks
	} else {
		// Request the in-memory buffer to drain and reset so that the start times
		// of the blocks in the buckets are set to the latest valid times
		s.buffer.DrainAndReset()

		// If any received data falls within the buffer then we emplace it there
		min, _ := s.buffer.MinMax()
		for tNano, block := range blocks.AllBlocks() {
			t := tNano.ToTime()
			if !t.Before(min) {
				if err := s.buffer.Bootstrap(block); err != nil {
					multiErr = multiErr.Add(s.newBootstrapBlockError(block, err))
				}
				blocks.RemoveBlockAt(t)
			}
		}

		// If we're overwriting the blocks then merge any existing blocks
		// already drained
		for _, existingBlock := range existingBlocks.AllBlocks() {
			s.mergeBlockWithLock(blocks, existingBlock)
		}
	}

	s.blocks = blocks
	s.bs = bootstrapped

	return multiErr.FinalError()
}

func (s *dbSeries) OnRetrieveBlock(
	id ident.ID,
	startTime time.Time,
	segment ts.Segment,
) {
	s.Lock()
	defer s.Unlock()

	if !id.Equal(s.id) {
		return
	}

	b := s.opts.DatabaseBlockOptions().DatabaseBlockPool().Get()
	metadata := block.RetrievableBlockMetadata{
		ID:       s.id,
		Length:   segment.Len(),
		Checksum: digest.SegmentChecksum(segment),
	}
	b.ResetRetrievable(startTime, s.blockRetriever, metadata)
	// Use s.id instead of id here, because id is finalized by the context whereas
	// we rely on the G.C to reclaim s.id. This is important because the block will
	// hold onto the id ref, and (if the LRU caching policy is enabled) the shard
	// will need it later when the WiredList calls its OnEvictedFromWiredList method.
	// Also note that ResetRetrievable will mark the block as not retrieved from disk,
	// but OnRetrieveBlock will then properly mark it as retrieved from disk so subsequent
	// calls to WasRetrievedFromDisk will return true.
	b.OnRetrieveBlock(s.id, startTime, segment)

	// NB(r): Blocks retrieved have been triggered by a read, so set the last
	// read time as now so caching policies are followed.
	b.SetLastReadTime(s.now())

	// If we retrieved this from disk then we directly emplace it
	s.addBlockWithLock(b)

	if list := s.opts.DatabaseBlockOptions().WiredList(); list != nil {
		// Need to update the WiredList so blocks that were read from disk
		// can enter the list (OnReadBlock is only called for blocks that
		// were read from memory, regardless of whether the data originated
		// from disk or a buffer rotation.)
		list.Update(b)
	}
}

// OnReadBlock is only called for blocks that were read from memory, regardless of
// whether the data originated from disk or buffer rotation.
func (s *dbSeries) OnReadBlock(b block.DatabaseBlock) {
	if list := s.opts.DatabaseBlockOptions().WiredList(); list != nil {
		// The WiredList is only responsible for managing the lifecycle of blocks
		// retrieved from disk.
		if b.WasRetrievedFromDisk() {
			// Need to update the WiredList so it knows which blocks have been
			// most recently read.
			list.Update(b)
		}
	}
}

func (s *dbSeries) OnEvictedFromWiredList(id ident.ID, blockStart time.Time) {
	s.Lock()
	defer s.Unlock()

	// Should never happen
	if !id.Equal(s.id) {
		return
	}

	block, ok := s.blocks.BlockAt(blockStart)
	if ok {
		if !block.WasRetrievedFromDisk() {
			// Should never happen - invalid application state could cause data loss
			s.opts.InstrumentOptions().Logger().WithFields(
				xlog.NewField("id", id.String()),
				xlog.NewField("blockStart", blockStart),
			).Errorf("tried to evict block that was not retrieved from disk")
			return
		}

		s.blocks.RemoveBlockAt(blockStart)
	}
}

func (s *dbSeries) newBootstrapBlockError(
	b block.DatabaseBlock,
	err error,
) error {
	msgFmt := "bootstrap series error occurred for %s block at %s: %v"
	renamed := fmt.Errorf(msgFmt, s.id.String(), b.StartTime().String(), err)
	return xerrors.NewRenamedError(err, renamed)
}

func (s *dbSeries) Flush(
	ctx context.Context,
	blockStart time.Time,
	persistFn persist.Fn,
) error {
	// NB(r): Do not use defer here as we need to make sure the
	// call to sr.Segment() which may fetch data from disk is not
	// blocking the series lock.
	s.RLock()

	if s.bs != bootstrapped {
		s.RUnlock()
		return errSeriesNotBootstrapped
	}
	b, exists := s.blocks.BlockAt(blockStart)
	if !exists {
		s.RUnlock()
		return nil
	}

	sr, err := b.Stream(ctx)
	s.RUnlock()

	if err != nil {
		return err
	}
	if sr == nil {
		return nil
	}
	segment, err := sr.Segment()
	if err != nil {
		return err
	}

	return persistFn(s.id, segment, b.Checksum())
}

func (s *dbSeries) Close() {
	s.Lock()
	defer s.Unlock()

	// NB(r): We explicitly do not place this ID back into an
	// existing pool as high frequency users of series IDs such
	// as the commit log need to use the reference without the
	// overhead of ownership tracking. In addition, the blocks
	// themselves have a reference to the ID which is required
	// for the LRU/WiredList caching strategy eviction process.
	// Since the wired list can still have a reference to a
	// DatabaseBlock for which the corresponding DatabaseSeries
	// has been closed, its important that the ID itself is still
	// available because the process of kicking a DatabaseBlock
	// out of the WiredList requires the ID.
	//
	// Since series are purged so infrequently the overhead
	// of not releasing back an ID to a pool is amortized over
	// a long period of time.
	s.id = nil

	switch s.opts.CachePolicy() {
	case CacheLRU:
		// In the CacheLRU case, blocks that were retrieved from disk are owned
		// by the WiredList and should not be closed here. They will eventually
		// be evicted and closed by  the WiredList when it needs to make room
		// for new blocks.
		for _, block := range s.blocks.AllBlocks() {
			if !block.WasRetrievedFromDisk() {
				block.Close()
			}
		}
	default:
		s.blocks.RemoveAll()
	}

	// Reset (not close) underlying resources because the series will go
	// back into the pool and be re-used.
	s.buffer.Reset(s.opts)
	s.blocks.Reset()

	if s.pool != nil {
		s.pool.Put(s)
	}
}

func (s *dbSeries) Reset(
	id ident.ID,
	tags ident.Tags,
	blockRetriever QueryableBlockRetriever,
	onRetrieveBlock block.OnRetrieveBlock,
	onEvictedFromWiredList block.OnEvictedFromWiredList,
	opts Options,
) {
	s.Lock()
	defer s.Unlock()

	s.id = id
	s.tags = tags
	s.blocks.Reset()
	s.buffer.Reset(opts)
	s.opts = opts
	s.bs = bootstrapNotStarted
	s.blockRetriever = blockRetriever
	s.onRetrieveBlock = onRetrieveBlock
	s.blockOnEvictedFromWiredList = onEvictedFromWiredList
}
