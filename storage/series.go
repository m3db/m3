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

package storage

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

var (
	errInvalidRange               = errors.New("invalid time range specified")
	errSeriesAllDatapointsExpired = errors.New("all series datapoints expired")
)

type dbSeries struct {
	sync.RWMutex
	opts             Options
	seriesID         string
	buffer           databaseBuffer
	blocks           block.DatabaseSeriesBlocks
	pendingBootstrap []pendingBootstrapDrain
	bs               bootstrapState
}

type pendingBootstrapDrain struct {
	encoder encoding.Encoder
}

func newDatabaseSeries(id string, bs bootstrapState, opts Options) databaseSeries {
	series := &dbSeries{
		opts:     opts,
		seriesID: id,
		blocks:   block.NewDatabaseSeriesBlocks(opts.GetDatabaseBlockOptions()),
		bs:       bs,
	}
	series.buffer = newDatabaseBuffer(series.bufferDrained, opts)
	return series
}

func (s *dbSeries) ID() string {
	return s.seriesID
}

func (s *dbSeries) Tick() error {
	if s.Empty() {
		return errSeriesAllDatapointsExpired
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
	now := s.opts.GetClockOptions().GetNowFn()()
	minBlockStart := s.blocks.GetMinTime()
	if s.shouldExpire(now, minBlockStart) {
		return true
	}

	// If one or more blocks need to be sealed, we should update
	// the blocks.
	allBlocks := s.blocks.GetAllBlocks()
	for blockStart, block := range allBlocks {
		if s.shouldSeal(now, blockStart, block) {
			return true
		}
	}

	return false
}

func (s *dbSeries) shouldExpire(now, blockStart time.Time) bool {
	rops := s.opts.GetRetentionOptions()
	cutoff := now.Add(-rops.GetRetentionPeriod()).Truncate(rops.GetBlockSize())
	return blockStart.Before(cutoff)
}

func (s *dbSeries) updateBlocksWithLock() {
	now := s.opts.GetClockOptions().GetNowFn()()
	allBlocks := s.blocks.GetAllBlocks()
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
	rops := s.opts.GetRetentionOptions()
	blockSize := rops.GetBlockSize()
	cutoff := now.Add(-rops.GetBufferPast()).Add(-blockSize).Truncate(blockSize)
	return blockStart.Before(cutoff)
}

func (s *dbSeries) Empty() bool {
	s.RLock()
	blocksLen := s.blocks.Len()
	bufferEmpty := s.buffer.Empty()
	s.RUnlock()
	if blocksLen == 0 && bufferEmpty {
		return true
	}
	return false
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
		return nil, xerrors.NewInvalidParamsError(errInvalidRange)
	}

	// TODO(r): pool these results arrays
	var results [][]xio.SegmentReader

	blockSize := s.opts.GetRetentionOptions().GetBlockSize()
	alignedStart := start.Truncate(blockSize)
	alignedEnd := end.Truncate(blockSize)
	if alignedEnd.Equal(end) {
		// Move back to make range [start, end)
		alignedEnd = alignedEnd.Add(-1 * blockSize)
	}

	s.RLock()

	if s.blocks.Len() > 0 {
		// Squeeze the lookup window by what's available to make range queries like [0, infinity) possible
		if s.blocks.GetMinTime().After(alignedStart) {
			alignedStart = s.blocks.GetMinTime()
		}
		if s.blocks.GetMaxTime().Before(alignedEnd) {
			alignedEnd = s.blocks.GetMaxTime()
		}
		for blockAt := alignedStart; !blockAt.After(alignedEnd); blockAt = blockAt.Add(blockSize) {
			if block, ok := s.blocks.GetBlockAt(blockAt); ok {
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

func (s *dbSeries) FetchBlocks(ctx context.Context, starts []time.Time) []FetchBlockResult {
	res := make([]FetchBlockResult, 0, len(starts)+1)

	s.RLock()

	for _, start := range starts {
		if b, exists := s.blocks.GetBlockAt(start); exists {
			stream, err := b.Stream(ctx)
			if err != nil {
				detailedErr := fmt.Errorf("unable to retrieve block stream for series %s time %v: %v", s.seriesID, start, err)
				res = append(res, newFetchBlockResult(start, nil, detailedErr))
			} else if stream != nil {
				res = append(res, newFetchBlockResult(start, []xio.SegmentReader{stream}, nil))
			}
		}
	}

	if !s.buffer.Empty() {
		bufferResults := s.buffer.FetchBlocks(ctx, starts)
		res = append(res, bufferResults...)
	}

	s.RUnlock()

	sortFetchBlockResultByTimeAscending(res)

	return res
}

func (s *dbSeries) FetchBlocksMetadata(ctx context.Context, includeSizes bool) FetchBlocksMetadataResult {
	// TODO(xichen): pool these if this method is called frequently (e.g., for background repairs)
	var res []FetchBlockMetadataResult

	s.RLock()

	// Iterate over the data blocks
	blocks := s.blocks.GetAllBlocks()
	for t, b := range blocks {
		reader, err := b.Stream(ctx)
		// If we failed to read some blocks, skip this block and continue to get
		// the metadata for the rest of the blocks.
		if err != nil {
			detailedErr := fmt.Errorf("unable to retrieve block stream for series %s time %v: %v", s.seriesID, t, err)
			res = append(res, newFetchBlockMetadataResult(t, nil, detailedErr))
			continue
		}
		// If there are no datapoints in the block, continue and don't append it to the result.
		if reader == nil {
			continue
		}
		var pSize *int64
		if includeSizes {
			segment := reader.Segment()
			size := int64(len(segment.Head) + len(segment.Tail))
			pSize = &size
		}
		res = append(res, newFetchBlockMetadataResult(t, pSize, nil))
	}

	// Iterate over the encoders in the database buffer
	if !s.buffer.Empty() {
		bufferResult := s.buffer.FetchBlocksMetadata(ctx, includeSizes)
		res = append(res, bufferResult...)
	}

	s.RUnlock()

	sortFetchBlockMetadataResultByTimeAscending(res)

	return newFetchBlocksMetadataResult(s.seriesID, res)
}

func (s *dbSeries) bufferDrained(start time.Time, encoder encoding.Encoder) {
	// NB(r): by the very nature of this method executing we have the
	// lock already. Executing the drain method occurs during a write if the
	// buffer needs to drain or if tick is called and series explicitly asks
	// the buffer to drain ready buckets.
	if s.bs != bootstrapped {
		s.pendingBootstrap = append(s.pendingBootstrap, pendingBootstrapDrain{encoder})
		return
	}

	if _, ok := s.blocks.GetBlockAt(start); !ok {
		// New completed block
		newBlock := s.opts.GetDatabaseBlockOptions().GetDatabaseBlockPool().Get()
		newBlock.Reset(start, encoder)
		s.blocks.AddBlock(newBlock)
		return
	}

	// NB(r): this will occur if after bootstrap we have a partial
	// block and now the buffer is passing the rest of that block
	stream := encoder.Stream()
	s.drainStream(s.blocks, stream, timeZero)
	stream.Close()
}

func (s *dbSeries) drainStream(blocks block.DatabaseSeriesBlocks, stream io.Reader, cutover time.Time) error {
	iter := s.opts.GetReaderIteratorPool().Get()
	iter.Reset(stream)

	// Close the iterator and return to pool when done
	defer iter.Close()

	for iter.Next() {
		dp, unit, annotation := iter.Current()
		// If the datapoint timestamp is before the cutover, skip it.
		if dp.Timestamp.Before(cutover) {
			continue
		}
		blockStart := dp.Timestamp.Truncate(s.opts.GetRetentionOptions().GetBlockSize())
		block := blocks.GetBlockOrAdd(blockStart)

		if err := block.Write(dp.Timestamp, dp.Value, unit, annotation); err != nil {
			return err
		}
	}
	if err := iter.Err(); err != nil {
		return err
	}
	return nil
}

// NB(xichen): we are holding a big lock here to drain the in-memory buffer.
// This could potentially be expensive in that we might accumulate a lot of
// data in memory during bootstrapping. If that becomes a problem, we could
// bootstrap in batches, e.g., drain and reset the buffer, drain the streams,
// then repeat, until len(s.pendingBootstrap) is below a given threshold.
func (s *dbSeries) Bootstrap(rs block.DatabaseSeriesBlocks, cutover time.Time) error {
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
		rs = block.NewDatabaseSeriesBlocks(s.opts.GetDatabaseBlockOptions())
	}

	// Force the in-memory buffer to drain and reset so we can merge the in-memory
	// data accumulated during bootstrapping.
	s.buffer.DrainAndReset(true)

	// NB(xichen): if an error occurred during series bootstrap, we close
	// the database series blocks and mark the series bootstrapped regardless
	// in the hope that the other replicas will provide data for this series.
	multiErr := xerrors.NewMultiError()
	for i := range s.pendingBootstrap {
		stream := s.pendingBootstrap[i].encoder.Stream()
		err := s.drainStream(rs, stream, cutover)
		stream.Close()
		if err != nil {
			rs.Close()
			rs = block.NewDatabaseSeriesBlocks(s.opts.GetDatabaseBlockOptions())
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
	b, exists := s.blocks.GetBlockAt(blockStart)
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
