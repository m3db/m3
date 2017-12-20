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

	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/ts"
	xio "github.com/m3db/m3db/x/io"
	xerrors "github.com/m3db/m3x/errors"
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

	errSeriesReadInvalidRange = errors.New("series invalid time range read argument specified")
	errSeriesIsBootstrapping  = errors.New("series is bootstrapping")
	errSeriesNotBootstrapped  = errors.New("series is not yet bootstrapped")
)

type dbSeries struct {
	sync.RWMutex
	opts Options

	// NB(r): One should audit all places that access the
	// series ID before changing ownership semantics (e.g.
	// pooling the ID rather than releasing it to the GC on
	// calling series.Reset()).
	id ts.ID

	buffer         databaseBuffer
	blocks         block.DatabaseSeriesBlocks
	bs             bootstrapState
	blockRetriever QueryableBlockRetriever
	pool           DatabaseSeriesPool
}

// NewDatabaseSeries creates a new database series
func NewDatabaseSeries(id ts.ID, opts Options) DatabaseSeries {
	var (
		s              = newDatabaseSeries()
		blockRetriever QueryableBlockRetriever
	)
	s.Reset(id, blockRetriever, opts)
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

func (s *dbSeries) ID() ts.ID {
	s.RLock()
	id := s.id
	s.RUnlock()
	return id
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
		checkUnwire  = retriever != nil && ropts.BlockDataExpiry()
		expireCutoff = now.Add(-ropts.RetentionPeriod()).Truncate(ropts.BlockSize())
		wiredTimeout = ropts.BlockDataExpiryAfterNotAccessedPeriod()
	)
	for startNano, currBlock := range s.blocks.AllBlocks() {
		start := startNano.ToTime()
		if start.Before(expireCutoff) {
			s.blocks.RemoveBlockAt(start)
			currBlock.Close()
			result.madeExpiredBlocks++
			continue
		}

		result.ActiveBlocks++

		if !currBlock.IsRetrieved() {
			result.UnwiredBlocks++
			continue
		}

		var unwired bool
		if checkUnwire {
			sinceLastRead := now.Sub(currBlock.LastReadTime())
			if sinceLastRead >= wiredTimeout &&
				retriever.IsBlockRetrievable(start) {
				// NB(r): Each block needs shared ref to the series ID
				// or else each block needs to have a copy of the ID
				id := s.id
				metadata := block.RetrievableBlockMetadata{
					ID:       id,
					Length:   currBlock.Len(),
					Checksum: currBlock.Checksum(),
				}
				currBlock.ResetRetrievable(start, retriever, metadata)

				unwired = true
				result.madeUnwiredBlocks++
			}
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
	if end.Before(start) {
		return nil, xerrors.NewInvalidParamsError(errSeriesReadInvalidRange)
	}

	// TODO(r): pool these results arrays
	var results [][]xio.SegmentReader

	now := s.now()
	cachePolicy := s.opts.CachePolicy()
	ropts := s.opts.RetentionOptions()
	blockSize := ropts.BlockSize()
	alignedStart := start.Truncate(blockSize)
	alignedEnd := end.Truncate(blockSize)
	if alignedEnd.Equal(end) {
		// Move back to make range [start, end)
		alignedEnd = alignedEnd.Add(-1 * blockSize)
	}

	s.RLock()
	defer s.RUnlock()

	numBlocks := s.blocks.Len()
	if numBlocks < 1 {
		if earliest := retention.FlushTimeStart(ropts, now); alignedStart.Before(earliest) {
			alignedStart = earliest
		}
		if latest := retention.FlushTimeEnd(ropts, now); alignedEnd.After(latest) {
			alignedEnd = latest
		}
	} else {
		if min := s.blocks.MinTime(); min.After(alignedStart) {
			alignedStart = min
		}
		if max := s.blocks.MaxTime(); max.Before(alignedEnd) {
			alignedEnd = max
		}
	}

	var clonedID ts.ID

	// Squeeze the lookup window by what's available to make range queries like [0, infinity) possible
	for blockAt := alignedStart; !blockAt.After(alignedEnd); blockAt = blockAt.Add(blockSize) {
		if numBlocks > 0 {
			if block, ok := s.blocks.BlockAt(blockAt); ok {
				// Block served from in memory
				stream, err := block.Stream(ctx)
				if err != nil {
					return nil, err
				}
				if stream != nil {
					results = append(results, []xio.SegmentReader{stream})
					// NB(r): Mark this block as read now
					block.SetLastReadTime(now)
				}
				continue
			}
		}

		switch cachePolicy {
		case CacheAll:
		case CacheAllMetadata:
		default:
			// Try to stream from disk
			if s.blockRetriever.IsBlockRetrievable(blockAt) {
				if clonedID == nil {
					// Clone ID as the block retriever uses the ID async from the lock on this series
					clonedID = s.opts.IdentifierPool().Clone(s.id)
					ctx.RegisterFinalizer(clonedID)
				}
				stream, err := s.blockRetriever.Stream(clonedID, blockAt, nil)
				if err != nil {
					return nil, err
				}
				if stream != nil {
					results = append(results, []xio.SegmentReader{stream})
				}
			}
		}
	}

	bufferResults := s.buffer.ReadEncoded(ctx, start, end)
	if len(bufferResults) > 0 {
		results = append(results, bufferResults...)
	}

	return results, nil
}

func (s *dbSeries) FetchBlocks(ctx context.Context, starts []time.Time) []block.FetchBlockResult {
	res := make([]block.FetchBlockResult, 0, len(starts))

	var clonedID ts.ID
	cachePolicy := s.opts.CachePolicy()

	s.RLock()
	defer s.RUnlock()

	for _, start := range starts {
		if b, exists := s.blocks.BlockAt(start); exists {
			stream, err := b.Stream(ctx)
			if err != nil {
				r := block.NewFetchBlockResult(start, nil,
					fmt.Errorf("unable to retrieve block stream for series %s time %v: %v",
						s.id.String(), start, err), nil)
				res = append(res, r)
			} else if stream != nil {
				checksum := b.Checksum()
				r := block.NewFetchBlockResult(start, []xio.SegmentReader{stream}, nil, &checksum)
				res = append(res, r)
			}
			continue
		}

		switch cachePolicy {
		case CacheAll:
		case CacheAllMetadata:
		default:
			// Try to stream from disk
			if s.blockRetriever.IsBlockRetrievable(start) {
				if clonedID == nil {
					// Clone ID as the block retriever uses the ID async from the lock on this series
					clonedID = s.opts.IdentifierPool().Clone(s.id)
					ctx.RegisterFinalizer(clonedID)
				}
				stream, err := s.blockRetriever.Stream(clonedID, start, nil)
				if err != nil {
					r := block.NewFetchBlockResult(start, nil,
						fmt.Errorf("unable to retrieve block stream for series %s time %v: %v",
							s.id.String(), start, err), nil)
					res = append(res, r)
				} else if stream != nil {
					// TODO: work out how to pass checksum here or defer till later somehow
					r := block.NewFetchBlockResult(start, []xio.SegmentReader{stream}, nil, nil)
					res = append(res, r)
				}
			}
		}
	}

	if !s.buffer.IsEmpty() {
		bufferResults := s.buffer.FetchBlocks(ctx, starts)
		res = append(res, bufferResults...)
	}

	block.SortFetchBlockResultByTimeAscending(res)

	return res
}

func (s *dbSeries) FetchBlocksMetadata(
	ctx context.Context,
	start, end time.Time,
	opts block.FetchBlocksMetadataOptions,
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

	// NB(r): it's possible a ref to this series was taken before the series was
	// closed then the method is called, which means no data and a nil ID.
	//
	// Hence we should return an empty result set here.
	//
	// In the future we need a way to make sure the call we're performing is for the
	// series we're originally took a ref to. This will avoid pooling of series messing
	// with calls to a series that was ref'd before it was closed/reused.
	if len(res.Results()) == 0 {
		return block.NewFetchBlocksMetadataResult(nil, res)
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
	s.mergeBlock(s.blocks, newBlock)
}

func (s *dbSeries) mergeBlock(
	blocks block.DatabaseSeriesBlocks,
	newBlock block.DatabaseBlock,
) {
	blockStart := newBlock.StartTime()

	// If we don't have an existing block just insert the new block.
	existingBlock, ok := blocks.BlockAt(blockStart)
	if !ok {
		blocks.AddBlock(newBlock)
		return
	}

	// We are performing this in a lock, cannot wait for the existing
	// block potentially to be retrieved from disk, lazily merge the stream.
	newBlock.Merge(existingBlock)
	blocks.AddBlock(newBlock)
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
			s.mergeBlock(blocks, existingBlock)
		}
	}

	s.blocks = blocks
	s.bs = bootstrapped

	return multiErr.FinalError()
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
	// overhead of ownership tracking.
	// Since series are purged so infrequently the overhead
	// of not releasing back an ID to a pool is amortized over
	// a long period of time.
	s.id = nil
	s.buffer.Reset(s.opts)
	s.blocks.Close()

	if s.pool != nil {
		s.pool.Put(s)
	}
}

func (s *dbSeries) Reset(
	id ts.ID,
	blockRetriever QueryableBlockRetriever,
	opts Options,
) {
	s.Lock()
	defer s.Unlock()

	s.id = id
	s.blocks.RemoveAll()
	s.buffer.Reset(opts)
	s.opts = opts
	s.bs = bootstrapNotStarted
	s.blockRetriever = blockRetriever
}
