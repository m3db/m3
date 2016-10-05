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
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/storage/block"
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
	errSeriesDrainEmptyStream = errors.New("series attempted to drain an empty stream")
	errSeriesIsBootstrapping  = errors.New("series is bootstrapping")
	errSeriesNotBootstrapped  = errors.New("series is not yet bootstrapped")
)

type dbSeries struct {
	sync.RWMutex
	opts             Options
	seriesID         string
	buffer           databaseBuffer
	blocks           block.DatabaseSeriesBlocks
	pendingBootstrap []pendingBootstrapDrain
	bs               bootstrapState
	pool             DatabaseSeriesPool
}

type pendingBootstrapDrain struct {
	start   time.Time
	encoder encoding.Encoder
}

// NewDatabaseSeries creates a new database series
func NewDatabaseSeries(id string, opts Options) DatabaseSeries {
	return newDatabaseSeries(id, opts)
}

// NewPooledDatabaseSeries creates a new pooled database series
func NewPooledDatabaseSeries(id string, pool DatabaseSeriesPool, opts Options) DatabaseSeries {
	series := newDatabaseSeries(id, opts)
	series.pool = pool
	return series
}

func newDatabaseSeries(id string, opts Options) *dbSeries {
	ropts := opts.RetentionOptions()
	blocksLen := int(ropts.RetentionPeriod() / ropts.BlockSize())
	series := &dbSeries{
		opts:     opts,
		seriesID: id,
		blocks:   block.NewDatabaseSeriesBlocks(blocksLen, opts.DatabaseBlockOptions()),
		bs:       bootstrapNotStarted,
	}
	series.buffer = newDatabaseBuffer(series.bufferDrained, opts)
	return series
}

func (s *dbSeries) ID() string {
	return s.seriesID
}

func (s *dbSeries) Tick() error {
	if s.IsEmpty() {
		return ErrSeriesAllDatapointsExpired
	}

	// In best case when explicitly asked to drain may have no
	// stale buckets, cheaply check this case first with a Rlock
	s.RLock()
	needsDrain := s.buffer.NeedsDrain()
	needsBlockUpdate := s.needsBlockUpdateWithRLock()
	s.RUnlock()

	if !needsDrain && !needsBlockUpdate {
		return nil
	}

	s.Lock()
	if needsDrain {
		s.buffer.DrainAndReset(false)
	}

	if needsBlockUpdate {
		s.updateBlocksWithLock()
	}
	s.Unlock()

	return nil
}

func (s *dbSeries) needsBlockUpdateWithRLock() bool {
	if s.blocks.Len() == 0 {
		return false
	}

	// If the earliest block is not within the retention period,
	// we should expire the blocks.
	now := s.opts.ClockOptions().NowFn()()
	minBlockStart := s.blocks.MinTime()
	if s.shouldExpire(now, minBlockStart) {
		return true
	}

	// If one or more blocks need to be sealed, we should update
	// the blocks.
	allBlocks := s.blocks.AllBlocks()
	for blockStart, block := range allBlocks {
		if s.shouldSeal(now, blockStart, block) {
			return true
		}
	}

	return false
}

func (s *dbSeries) shouldExpire(now, blockStart time.Time) bool {
	rops := s.opts.RetentionOptions()
	cutoff := now.Add(-rops.RetentionPeriod()).Truncate(rops.BlockSize())
	return blockStart.Before(cutoff)
}

func (s *dbSeries) updateBlocksWithLock() {
	now := s.opts.ClockOptions().NowFn()()
	allBlocks := s.blocks.AllBlocks()
	for blockStart, block := range allBlocks {
		if s.shouldExpire(now, blockStart) {
			s.blocks.RemoveBlockAt(blockStart)
			block.Close()
		} else if s.shouldSeal(now, blockStart, block) {
			block.Seal()
		}
	}
}

func (s *dbSeries) shouldSeal(now, blockStart time.Time, block block.DatabaseBlock) bool {
	if block.IsSealed() {
		return false
	}
	rops := s.opts.RetentionOptions()
	blockSize := rops.BlockSize()
	cutoff := now.Add(-rops.BufferPast()).Add(-blockSize).Truncate(blockSize)
	return blockStart.Before(cutoff)
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
	res := make([]block.FetchBlockResult, 0, len(starts)+1)

	s.RLock()

	for _, start := range starts {
		if b, exists := s.blocks.BlockAt(start); exists {
			stream, err := b.Stream(ctx)
			if err != nil {
				detailedErr := fmt.Errorf("unable to retrieve block stream for series %s time %v: %v", s.seriesID, start, err)
				res = append(res, block.NewFetchBlockResult(start, nil, detailedErr))
			} else if stream != nil {
				res = append(res, block.NewFetchBlockResult(start, []xio.SegmentReader{stream}, nil))
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
	includeSizes bool,
	includeChecksums bool,
) block.FetchBlocksMetadataResult {
	s.RLock()
	blocks := s.blocks.AllBlocks()
	// TODO(xichen): pool these if this method is called frequently (e.g., for background repairs)
	res := make([]block.FetchBlockMetadataResult, 0, len(blocks))
	// Iterate over the data blocks
	for t, b := range blocks {
		reader, err := b.Stream(ctx)
		// If we failed to read some blocks, skip this block and continue to get
		// the metadata for the rest of the blocks.
		if err != nil {
			detailedErr := fmt.Errorf("unable to retrieve block stream for series %s time %v: %v", s.seriesID, t, err)
			res = append(res, block.NewFetchBlockMetadataResult(t, nil, nil, detailedErr))
			continue
		}
		// If there are no datapoints in the block, continue and don't append it to the result.
		if reader == nil {
			continue
		}
		var (
			pSize     *int64
			pChecksum *uint32
		)
		if includeSizes {
			segment := reader.Segment()
			size := int64(len(segment.Head) + len(segment.Tail))
			pSize = &size
		}
		if includeChecksums {
			pChecksum = b.Checksum()
		}
		res = append(res, block.NewFetchBlockMetadataResult(t, pSize, pChecksum, nil))
		reader.Close()
	}

	// Iterate over the encoders in the database buffer
	if !s.buffer.IsEmpty() {
		bufferResult := s.buffer.FetchBlocksMetadata(ctx, includeSizes, includeChecksums)
		res = append(res, bufferResult...)
	}

	s.RUnlock()

	block.SortFetchBlockMetadataResultByTimeAscending(res)

	return block.NewFetchBlocksMetadataResult(s.seriesID, res)
}

func (s *dbSeries) bufferDrained(start time.Time, encoder encoding.Encoder) {
	// NB(r): by the very nature of this method executing we have the
	// lock already. Executing the drain method occurs during a write if the
	// buffer needs to drain or if tick is called and series explicitly asks
	// the buffer to drain ready buckets.
	if s.bs != bootstrapped {
		s.pendingBootstrap = append(s.pendingBootstrap, pendingBootstrapDrain{
			start:   start,
			encoder: encoder,
		})
		return
	}

	if _, ok := s.blocks.BlockAt(start); !ok {
		// New completed block
		newBlock := s.opts.DatabaseBlockOptions().DatabaseBlockPool().Get()
		newBlock.Reset(start, encoder)
		s.blocks.AddBlock(newBlock)
		return
	}

	// NB(r): this will occur if after bootstrap we have a partial
	// block and now the buffer is passing the rest of that block
	s.drainBufferedEncoderWithLock(s.blocks, start, encoder)
}

func (s *dbSeries) drainBufferedEncoderWithLock(
	blocks block.DatabaseSeriesBlocks,
	blockStart time.Time,
	enc encoding.Encoder,
) error {
	// If enc is empty, do nothing
	stream := enc.Stream()
	if stream == nil {
		return nil
	}
	defer stream.Close()

	// If we don't have an existing block, grab a block from the pool
	// and reset its encoder
	blopts := s.opts.DatabaseBlockOptions()
	existing, ok := blocks.BlockAt(blockStart)
	if !ok {
		block := blopts.DatabaseBlockPool().Get()
		block.Reset(blockStart, enc)
		blocks.AddBlock(block)
		return nil
	}

	var readers [2]io.Reader
	readers[0] = stream

	ctx := s.opts.ContextPool().Get()
	defer ctx.Close()
	reader, err := existing.Stream(ctx)
	if err != nil {
		return err
	}
	readers[1] = reader

	multiIter := s.opts.MultiReaderIteratorPool().Get()
	multiIter.Reset(readers[:])
	defer multiIter.Close()

	encoder := s.opts.EncoderPool().Get()
	encoder.Reset(blockStart, blopts.DatabaseBlockAllocSize())

	abort := func() {
		// Return encoder to the pool if we abort draining the stream
		encoder.Close()
	}

	for multiIter.Next() {
		dp, unit, annotation := multiIter.Current()

		err := encoder.Encode(dp, unit, annotation)
		if err != nil {
			abort()
			return err
		}
	}
	if err := multiIter.Err(); err != nil {
		abort()
		return err
	}

	block := blopts.DatabaseBlockPool().Get()
	block.Reset(blockStart, encoder)
	blocks.AddBlock(block)

	return nil
}

// NB(xichen): we are holding a big lock here to drain the in-memory buffer.
// This could potentially be expensive in that we might accumulate a lot of
// data in memory during bootstrapping. If that becomes a problem, we could
// bootstrap in batches, e.g., drain and reset the buffer, drain the streams,
// then repeat, until len(s.pendingBootstrap) is below a given threshold.
func (s *dbSeries) Bootstrap(rs block.DatabaseSeriesBlocks) error {
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

	if rs == nil {
		// If no data to bootstrap from then fallback to the empty blocks map.
		rs = s.blocks
	}

	// Force the in-memory buffer to drain and reset so we can merge the
	// in-memory data accumulated during bootstrapping.
	s.buffer.DrainAndReset(true)

	// NB(xichen): if an error occurred during series bootstrap, we close
	// the database series blocks and mark the series bootstrapped regardless
	// in the hope that the other replicas will provide data for this series.
	multiErr := xerrors.NewMultiError()
	for i := range s.pendingBootstrap {
		if err := s.drainBufferedEncoderWithLock(rs, s.pendingBootstrap[i].start, s.pendingBootstrap[i].encoder); err != nil {
			rs.Close()

			ropts := s.opts.RetentionOptions()
			blocksLen := int(ropts.RetentionPeriod() / ropts.BlockSize())
			rs = block.NewDatabaseSeriesBlocks(blocksLen, s.opts.DatabaseBlockOptions())
			err = xerrors.NewRenamedError(err, fmt.Errorf("error occurred bootstrapping series %s: %v", s.seriesID, err))
			multiErr = multiErr.Add(err)
		}
	}

	s.blocks = rs
	s.pendingBootstrap = nil
	s.bs = bootstrapped
	s.Unlock()

	return multiErr.FinalError()
}

func (s *dbSeries) Flush(ctx context.Context, blockStart time.Time, persistFn persist.Fn) error {
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
	segment := sr.Segment()
	return persistFn(s.seriesID, segment)
}

func (s *dbSeries) Close() {
	s.Lock()
	s.buffer.Reset()
	s.blocks.Close()
	s.pendingBootstrap = nil
	s.Unlock()
	if s.pool != nil {
		s.pool.Put(s)
	}
}

func (s *dbSeries) Reset(id string) {
	s.Lock()
	s.seriesID = id
	s.buffer.Reset()
	s.blocks.RemoveAll()
	s.pendingBootstrap = nil
	s.bs = bootstrapNotStarted
	s.Unlock()
}
