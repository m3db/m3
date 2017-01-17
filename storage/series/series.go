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
	"io"
	"sync"
	"time"

	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/ts"
	xio "github.com/m3db/m3db/x/io"
	"github.com/m3db/m3x/checked"
	xerrors "github.com/m3db/m3x/errors"
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

	errSeriesReadInvalidRange = errors.New("series invalid time range read argument specified")
	errSeriesDrainEmptyStream = errors.New("series attempted to drain an empty stream")
	errSeriesIsBootstrapping  = errors.New("series is bootstrapping")
	errSeriesNotBootstrapped  = errors.New("series is not yet bootstrapped")
)

var nilID = ts.BinaryID(checked.NewBytes(nil, nil))

type dbSeries struct {
	sync.RWMutex
	opts           Options
	id             ts.ID
	buffer         databaseBuffer
	blocks         block.DatabaseSeriesBlocks
	bs             bootstrapState
	blockRetriever ShardBlockRetriever
	pool           DatabaseSeriesPool
}

type updateBlocksResult struct {
	expired          int
	resetRetrievable int
}

// NewDatabaseSeries creates a new database series
func NewDatabaseSeries(id ts.ID, opts Options) DatabaseSeries {
	return newDatabaseSeries(id, opts)
}

// newPooledDatabaseSeries creates a new pooled database series
func newPooledDatabaseSeries(pool DatabaseSeriesPool, opts Options) DatabaseSeries {
	series := newDatabaseSeries(nil, opts)
	series.pool = pool
	return series
}

func newDatabaseSeries(id ts.ID, opts Options) *dbSeries {
	series := &dbSeries{
		id:     id,
		opts:   opts,
		blocks: block.NewDatabaseSeriesBlocks(0, opts.DatabaseBlockOptions()),
		bs:     bootstrapNotStarted,
	}
	series.buffer = newDatabaseBuffer(series.bufferDrained, opts)
	return series
}

func (s *dbSeries) log() xlog.Logger {
	// NB(r): only in exceptional cases do we log, so defer
	// creating the logger with fields until we need it
	log := s.opts.InstrumentOptions().Logger()
	log = log.WithFields(xlog.NewLogField("id", s.id.String()))
	return log
}

func (s *dbSeries) ID() ts.ID {
	return s.id
}

func (s *dbSeries) Tick() (TickResult, error) {
	var r TickResult
	if s.IsEmpty() {
		return r, ErrSeriesAllDatapointsExpired
	}

	// In best case when explicitly asked to drain may have no
	// stale buckets, cheaply check this case first with a Rlock
	s.RLock()

	needsDrain := s.buffer.NeedsDrain()
	needsBlockUpdate := s.needsBlockUpdateWithLock()
	r.ActiveBlocks = s.blocks.Len()

	s.RUnlock()

	if !needsDrain && !needsBlockUpdate {
		return r, nil
	}

	s.Lock()

	// NB(r): don't bother to check if needs drain or needs
	// block update still holds as running both these checks
	// are relatively expensive and running these methods
	// will be a no-op in case conditions no longer hold.
	s.buffer.DrainAndReset()

	updateResult := s.updateBlocksWithLock()
	r.ExpiredBlocks = updateResult.expired
	r.ResetRetrievableBlocks = updateResult.resetRetrievable
	r.ActiveBlocks = s.blocks.Len()

	s.Unlock()

	return r, nil
}

func (s *dbSeries) needsBlockUpdateWithLock() bool {
	if s.blocks.Len() == 0 {
		return false
	}

	now := s.opts.ClockOptions().NowFn()()
	if s.shouldExpireAnyBlockData(now) {
		return true
	}

	// If the earliest block is not within the retention period,
	// we should expire the blocks.
	minBlockStart := s.blocks.MinTime()
	return s.shouldExpireBlockAt(now, minBlockStart)
}

func (s *dbSeries) shouldExpireAnyBlockData(now time.Time) bool {
	retriever := s.blockRetriever
	if retriever == nil {
		return false
	}

	ropts := s.opts.RetentionOptions()
	if !ropts.BlockDataExpiry() {
		return false
	}

	cutoff := ropts.BlockDataExpiryAfterNotAccessedPeriod()
	for start, block := range s.blocks.AllBlocks() {
		if !block.IsRetrieved() {
			continue
		}
		sinceLastAccessed := now.Sub(block.LastAccessTime())
		if sinceLastAccessed >= cutoff &&
			retriever.IsBlockRetrievable(start) {
			return true
		}
	}

	return false
}

func (s *dbSeries) shouldExpireBlockAt(now, blockStart time.Time) bool {
	var cutoff time.Time
	rops := s.opts.RetentionOptions()
	if rops.ShortExpiry() {
		// Some blocks of series is stored in disk only
		cutoff = now.Add(-rops.ShortExpiryPeriod() - rops.BlockSize()).Truncate(rops.BlockSize())
	} else {
		// Everything for the retention period is kept in mem
		cutoff = now.Add(-rops.RetentionPeriod()).Truncate(rops.BlockSize())
	}

	return blockStart.Before(cutoff)
}

func (s *dbSeries) updateBlocksWithLock() updateBlocksResult {
	var (
		r         updateBlocksResult
		now       = s.opts.ClockOptions().NowFn()()
		allBlocks = s.blocks.AllBlocks()

		retriever  = s.blockRetriever
		ropts      = s.opts.RetentionOptions()
		dataExpiry = ropts.BlockDataExpiry()
		dataCutoff = ropts.BlockDataExpiryAfterNotAccessedPeriod()
	)
	for start, currBlock := range allBlocks {
		if s.shouldExpireBlockAt(now, start) {
			s.blocks.RemoveBlockAt(start)
			currBlock.Close()
			r.expired++
		}
		if retriever != nil && dataExpiry && currBlock.IsRetrieved() {
			sinceLastAccessed := now.Sub(currBlock.LastAccessTime())
			if sinceLastAccessed > dataCutoff {
				// NB(r): Each block needs shared ref to the series ID
				// or else each block needs to have a copy of the ID
				id := s.id
				metadata := block.RetrievableBlockMetadata{
					Shard:    retriever.ShardID(),
					ID:       id,
					Length:   currBlock.Len(),
					Checksum: currBlock.Checksum(),
				}
				currBlock.ResetRetrievable(start, retriever, metadata)
				r.resetRetrievable++
			}
		}
	}
	return r
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
	// TODO(r): as discussed we need to revisit locking to provide a finer grained lock to
	// avoiding block writes while reads from blocks (not the buffer) go through.  There is
	// a few different ways we can accomplish this.  Will revisit soon once we have benchmarks
	// for mixed write/read workload.
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

	blockSize := s.opts.RetentionOptions().BlockSize()
	alignedStart := start.Truncate(blockSize)
	alignedEnd := end.Truncate(blockSize)
	if alignedEnd.Equal(end) {
		// Move back to make range [start, end)
		alignedEnd = alignedEnd.Add(-1 * blockSize)
	}

	s.RLock()

	if s.blocks.Len() > 0 {
		// Squeeze the lookup window by what's available to make range queries like [0, infinity) possible
		if s.blocks.MinTime().After(alignedStart) {
			alignedStart = s.blocks.MinTime()
		}
		if s.blocks.MaxTime().Before(alignedEnd) {
			alignedEnd = s.blocks.MaxTime()
		}
		for blockAt := alignedStart; !blockAt.After(alignedEnd); blockAt = blockAt.Add(blockSize) {
			if block, ok := s.blocks.BlockAt(blockAt); ok {
				stream, err := block.Stream(ctx)
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

	s.RUnlock()

	return results, nil
}

func (s *dbSeries) FetchBlocks(ctx context.Context, starts []time.Time) []block.FetchBlockResult {
	res := make([]block.FetchBlockResult, 0, len(starts))

	s.RLock()

	for _, start := range starts {
		if b, exists := s.blocks.BlockAt(start); exists {
			stream, err := b.Stream(ctx)
			if err != nil {
				r := block.NewFetchBlockResult(start, nil,
					fmt.Errorf("unable to retrieve block stream for series %s time %v: %v",
						s.id.String(), start, err))
				res = append(res, r)
			} else if stream != nil {
				r := block.NewFetchBlockResult(start, []xio.SegmentReader{stream}, nil)
				res = append(res, r)
			}
		}
	}

	if !s.buffer.IsEmpty() {
		bufferResults := s.buffer.FetchBlocks(ctx, starts)
		res = append(res, bufferResults...)
	}

	s.RUnlock()

	block.SortFetchBlockResultByTimeAscending(res)

	return res
}

func (s *dbSeries) FetchBlocksMetadata(
	ctx context.Context,
	start, end time.Time,
	includeSizes bool,
	includeChecksums bool,
) block.FetchBlocksMetadataResult {
	blockSize := s.opts.RetentionOptions().BlockSize()
	s.RLock()
	blocks := s.blocks.AllBlocks()
	res := s.opts.FetchBlockMetadataResultsPool().Get()
	// Iterate over the data blocks
	for t, b := range blocks {
		if !start.Before(t.Add(blockSize)) || !t.Before(end) {
			continue
		}
		var (
			pSize     *int64
			pChecksum *uint32
		)
		if includeSizes {
			size := int64(b.Len())
			pSize = &size
		}
		if includeChecksums {
			checksum := b.Checksum()
			pChecksum = &checksum
		}
		res.Add(block.NewFetchBlockMetadataResult(t, pSize, pChecksum, nil))
	}

	// Iterate over the encoders in the database buffer
	if !s.buffer.IsEmpty() {
		bufferResults := s.buffer.FetchBlocksMetadata(ctx, start, end, includeSizes, includeChecksums)
		for _, result := range bufferResults.Results() {
			res.Add(result)
		}
		bufferResults.Close()
	}

	// NB(r): clone the ID before releasing lock incase this series is being
	// closed or removed immediately after unlocking.
	id := s.opts.IdentifierPool().Clone(s.id)

	s.RUnlock()

	res.Sort()

	return block.NewFetchBlocksMetadataResult(id, res)
}

func (s *dbSeries) bufferDrained(newBlock block.DatabaseBlock) {
	// NB(r): by the very nature of this method executing we have the
	// lock already. Executing the drain method occurs during a write if the
	// buffer needs to drain or if tick is called and series explicitly asks
	// the buffer to drain ready buckets.
	if _, ok := s.blocks.BlockAt(newBlock.StartTime()); !ok {
		s.addBlock(newBlock)
		return
	}
	mergedBlock, err := s.mergeBlock(s.blocks, newBlock)
	if err != nil {
		s.log().Errorf("series error merging blocks: %v", err)
		return
	}
	if mergedBlock != nil {
		s.addBlock(mergedBlock)
	}
}

func (s *dbSeries) addBlock(b block.DatabaseBlock) {
	// Ensure that if this block lives on disk, when it is retrieved
	// we cache the data for this block in memory
	b.SetOnRetrieveBlock(s)

	s.blocks.AddBlock(b)
}

func (s *dbSeries) mergeBlock(
	blocks block.DatabaseSeriesBlocks,
	newBlock block.DatabaseBlock,
) (block.DatabaseBlock, error) {
	blockStart := newBlock.StartTime()

	// If we don't have an existing block there is no need to merge
	existingBlock, ok := blocks.BlockAt(blockStart)
	if !ok {
		return newBlock, nil
	}

	ctx := s.opts.ContextPool().Get()
	defer ctx.Close()

	// If enc is empty, no block to create
	newBlockReader, err := newBlock.Stream(ctx)
	if err != nil {
		return nil, err
	}
	if newBlockReader == nil {
		return nil, nil
	}

	existingBlockReader, err := existingBlock.Stream(ctx)
	if err != nil {
		return nil, err
	}
	if existingBlockReader == nil {
		// Existing block has no data, nothing to merge
		return newBlock, nil
	}

	var readers [2]io.Reader
	readers[0] = newBlockReader
	readers[1] = existingBlockReader

	multiIter := s.opts.MultiReaderIteratorPool().Get()
	multiIter.Reset(readers[:])
	defer multiIter.Close()

	blopts := s.opts.DatabaseBlockOptions()
	encoder := s.opts.EncoderPool().Get()
	encoder.Reset(blockStart, blopts.DatabaseBlockAllocSize())

	for multiIter.Next() {
		dp, unit, annotation := multiIter.Current()
		err := encoder.Encode(dp, unit, annotation)
		if err != nil {
			encoder.Close()
			return nil, err
		}
	}
	if err := multiIter.Err(); err != nil {
		encoder.Close()
		return nil, err
	}

	block := blopts.DatabaseBlockPool().Get()
	block.Reset(blockStart, encoder.Discard())

	// Close the existing and new blocks
	existingBlock.Close()
	newBlock.Close()

	return block, nil
}

// NB(xichen): we are holding a big lock here to drain the in-memory buffer.
// This could potentially be expensive in that we might accumulate a lot of
// data in memory during bootstrapping. If that becomes a problem, we could
// bootstrap in batches, e.g., drain and reset the buffer, drain the streams,
// then repeat, until len(s.pendingBootstrap) is below a given threshold.
func (s *dbSeries) Bootstrap(blocks block.DatabaseSeriesBlocks) error {
	s.Lock()
	if s.bs == bootstrapped {
		s.Unlock()
		return nil
	}
	if s.bs == bootstrapping {
		s.Unlock()
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
		for t, block := range blocks.AllBlocks() {
			if !t.Before(min) {
				if err := s.buffer.Bootstrap(block); err != nil {
					multiErr = multiErr.Add(s.newBootstrapBlockError(block, err))
				} else {
					// Call back the series when retrieved
					block.SetOnRetrieveBlock(s)
				}
				blocks.RemoveBlockAt(t)
			}
		}

		// If we're overwriting the blocks then merge any existing blocks
		// already drained
		for _, existingBlock := range existingBlocks.AllBlocks() {
			if _, ok := blocks.BlockAt(existingBlock.StartTime()); ok {
				mergedBlock, err := s.mergeBlock(blocks, existingBlock)
				if err != nil {
					multiErr = multiErr.Add(s.newBootstrapBlockError(existingBlock, err))
				}
				if mergedBlock != nil {
					blocks.AddBlock(mergedBlock)
				}
			}
		}

		// Ensure all blocks we keep will call back the series when retrieved
		for _, block := range blocks.AllBlocks() {
			block.SetOnRetrieveBlock(s)
		}
	}

	s.blocks = blocks
	s.bs = bootstrapped
	s.Unlock()

	return multiErr.FinalError()
}

// OnRetrieveCallback is called by each individual block when
// they are retrieved from disk
func (s *dbSeries) OnRetrieveBlock(
	id ts.ID,
	blockStart time.Time,
	segment ts.Segment,
) {
	var err error

	s.Lock()
	if !s.id.Equal(id) {
		err = fmt.Errorf("retrieved ID %s does not match series ID",
			id.String())
	} else {
		min, _ := s.buffer.MinMax()
		if !blockStart.Before(min) {
			err = s.buffer.CacheRetrievedBlock(blockStart, segment)
		} else {
			block, ok := s.blocks.BlockAt(blockStart)
			if !ok {
				err = fmt.Errorf("retrieved for %s not an existing block",
					blockStart.String())
			} else {
				block.Reset(blockStart, segment)
			}
		}
	}
	s.Unlock()

	if err != nil {
		s.log().Errorf("series cache retrieved block error: %v", err)
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
	s.id = nil
	s.buffer.Reset()
	s.blocks.Close()
	s.Unlock()

	if s.pool != nil {
		s.pool.Put(s)
	}
}

func (s *dbSeries) Reset(id ts.ID, blockRetriever ShardBlockRetriever) {
	s.Lock()
	s.id = id
	s.buffer.Reset()
	s.blocks.RemoveAll()
	s.bs = bootstrapNotStarted
	s.blockRetriever = blockRetriever
	s.Unlock()
}
