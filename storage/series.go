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
	"io"
	"sync"
	"time"

	"github.com/m3db/m3db/interfaces/m3db"
	xerrors "github.com/m3db/m3db/x/errors"
	xio "github.com/m3db/m3db/x/io"
	xtime "github.com/m3db/m3db/x/time"
)

var (
	errInvalidRange               = errors.New("invalid time range specified")
	errSeriesAllDatapointsExpired = errors.New("all series datapoints expired")
)

type databaseSeries interface {
	ID() string

	// Tick performs any updates to ensure buffer drains, blocks are written to disk, etc
	Tick() error

	Write(
		ctx m3db.Context,
		timestamp time.Time,
		value float64,
		unit xtime.Unit,
		annotation []byte,
	) error

	ReadEncoded(
		ctx m3db.Context,
		start, end time.Time,
	) (m3db.ReaderSliceReader, error)

	Empty() bool

	// Bootstrap merges the raw series bootstrapped along with the buffered data
	Bootstrap(rs m3db.DatabaseSeriesBlocks, cutover time.Time) error

	// FlushToDisk flushes the blocks to disk for a given start time.
	FlushToDisk(writer m3db.FileSetWriter, blockStart time.Time, segmentHolder [][]byte) error
}

type dbSeries struct {
	sync.RWMutex
	opts             m3db.DatabaseOptions
	seriesID         string
	buffer           databaseBuffer
	blocks           m3db.DatabaseSeriesBlocks
	blockSize        time.Duration
	pendingBootstrap []pendingBootstrapDrain
	bs               bootstrapState
}

type pendingBootstrapDrain struct {
	encoder m3db.Encoder
}

func newDatabaseSeries(id string, bs bootstrapState, opts m3db.DatabaseOptions) databaseSeries {
	series := &dbSeries{
		opts:      opts,
		seriesID:  id,
		blocks:    NewDatabaseSeriesBlocks(opts),
		blockSize: opts.GetBlockSize(),
		bs:        bs,
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
	needsBlockExpiry := s.needsBlockExpiry()
	s.RUnlock()

	if needsDrain || needsBlockExpiry {
		s.Lock()

		if needsDrain {
			s.buffer.DrainAndReset()
		}

		if needsBlockExpiry {
			s.expireBlocks()
		}

		s.Unlock()
	}
	return nil
}

// NB(xichen): this is called inside a RLock.
func (s *dbSeries) needsBlockExpiry() bool {
	if s.blocks.Len() == 0 {
		return false
	}

	// If the earliest block is not within the retention period,
	// we should expire the blocks.
	now := s.opts.GetNowFn()()
	minBlockStart := s.blocks.GetMinTime()
	return s.shouldExpire(now, minBlockStart)
}

// NB(xichen): this is called inside a Lock.
func (s *dbSeries) expireBlocks() {
	now := s.opts.GetNowFn()()
	allBlocks := s.blocks.GetAllBlocks()
	for timestamp, block := range allBlocks {
		if s.shouldExpire(now, timestamp) {
			s.blocks.RemoveBlockAt(timestamp)
			block.Close()
		}
	}
}

func (s *dbSeries) shouldExpire(now, blockStart time.Time) bool {
	cutoff := now.Add(-s.opts.GetRetentionPeriod()).Truncate(s.opts.GetBlockSize())
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
	ctx m3db.Context,
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
	ctx m3db.Context,
	start, end time.Time,
) (m3db.ReaderSliceReader, error) {
	if end.Before(start) {
		return nil, xerrors.NewInvalidParamsError(errInvalidRange)
	}

	// TODO(r): pool these results arrays
	var results []io.Reader

	alignedStart := start.Truncate(s.blockSize)
	alignedEnd := end.Truncate(s.blockSize)
	if alignedEnd.Equal(end) {
		// Move back to make range [start, end)
		alignedEnd = alignedEnd.Add(-1 * s.blockSize)
	}

	s.RLock()

	if len(s.blocks.GetAllBlocks()) > 0 {
		// Squeeze the lookup window by what's available to make range queries like [0, infinity) possible
		if s.blocks.GetMinTime().After(alignedStart) {
			alignedStart = s.blocks.GetMinTime()
		}
		if s.blocks.GetMaxTime().Before(alignedEnd) {
			alignedEnd = s.blocks.GetMaxTime()
		}
		for blockAt := alignedStart; !blockAt.After(alignedEnd); blockAt = blockAt.Add(s.blockSize) {
			if block, ok := s.blocks.GetBlockAt(blockAt); ok {
				if s := block.Stream(); s != nil {
					results = append(results, s)
				}
			}
		}
	}

	bufferResults := s.buffer.ReadEncoded(ctx, start, end)
	if len(bufferResults) > 0 {
		results = append(results, bufferResults...)
	}

	s.RUnlock()

	return xio.NewReaderSliceReader(results), nil
}

func (s *dbSeries) bufferDrained(start time.Time, encoder m3db.Encoder) {
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
		s.blocks.AddBlock(NewDatabaseBlock(start, encoder, s.opts))
		return
	}

	// NB(r): this will occur if after bootstrap we have a partial
	// block and now the buffer is passing the rest of that block
	stream := encoder.Stream()
	s.drainStream(s.blocks, stream, timeZero)
	stream.Close()
}

func (s *dbSeries) drainStream(blocks m3db.DatabaseSeriesBlocks, stream io.Reader, cutover time.Time) error {
	iter := s.opts.GetSingleReaderIteratorPool().Get()
	iter.Reset(stream)

	// Close the iterator and return to pool when done
	defer iter.Close()

	for iter.Next() {
		dp, unit, annotation := iter.Current()
		// If the datapoint timestamp is before the cutover, skip it.
		if dp.Timestamp.Before(cutover) {
			continue
		}
		blockStart := dp.Timestamp.Truncate(s.blockSize)
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

func (s *dbSeries) Bootstrap(rs m3db.DatabaseSeriesBlocks, cutover time.Time) error {
	if success, err := tryBootstrap(&s.RWMutex, &s.bs, "series"); !success {
		return err
	}

	s.Lock()
	s.bs = bootstrapping
	s.Unlock()

	if rs == nil {
		rs = NewDatabaseSeriesBlocks(s.opts)
	}

	for {
		s.Lock()
		if len(s.pendingBootstrap) == 0 {
			s.blocks = rs
			s.bs = bootstrapped
			s.Unlock()
			break
		}
		drain := s.pendingBootstrap[0]
		s.pendingBootstrap = s.pendingBootstrap[1:]
		s.Unlock()

		stream := drain.encoder.Stream()
		if err := s.drainStream(rs, stream, cutover); err != nil {
			stream.Close()
			return err
		}
		stream.Close()
	}

	return nil
}

// NB(xichen): segmentHolder is a two-item slice that's reused to
// hold pointers to the head and the tail of each segment so we
// don't need to allocate memory and gc it shortly after.
func (s *dbSeries) FlushToDisk(writer m3db.FileSetWriter, blockStart time.Time, segmentHolder [][]byte) error {
	s.RLock()
	b, exists := s.blocks.GetBlockAt(blockStart)
	if !exists {
		s.RUnlock()
		return nil
	}
	sr := b.Stream()
	if sr == nil {
		s.RUnlock()
		return nil
	}
	s.RUnlock()

	// TODO(xichen): register this with contexts when it's in place.
	segment := sr.Segment()
	if len(segmentHolder) != 2 {
		segmentHolder = make([][]byte, 2)
	}
	segmentHolder[0] = segment.Head
	segmentHolder[1] = segment.Tail
	return writer.WriteAll(s.seriesID, segmentHolder)
}
