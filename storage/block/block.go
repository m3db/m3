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

package block

import (
	"errors"
	"time"

	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/digest"
	"github.com/m3db/m3db/ts"
	xio "github.com/m3db/m3db/x/io"
)

var (
	errReadFromClosedBlock = errors.New("attempt to read from a closed block")
	errWriteToClosedBlock  = errors.New("attempt to write to a closed block")

	timeZero = time.Time{}
)

// NB(xichen): locking of dbBlock instances is currently done outside the dbBlock struct at the series level.
// Specifically, read lock is acquired for accessing operations like Stream(), and write lock is acquired
// for mutating operations like Write(), Reset(), and Close(). Adding a explicit lock to the dbBlock struct might
// make it more clear w.r.t. how/when we acquire locks, though.
type dbBlock struct {
	opts     Options
	ctx      context.Context
	start    time.Time
	segment  ts.Segment
	checksum uint32
	closed   bool
}

// NewDatabaseBlock creates a new DatabaseBlock instance.
func NewDatabaseBlock(start time.Time, segment ts.Segment, opts Options) DatabaseBlock {
	b := &dbBlock{
		opts:   opts,
		ctx:    opts.ContextPool().Get(),
		start:  start,
		closed: false,
	}
	b.resetSegment(segment)
	return b
}

func (b *dbBlock) StartTime() time.Time {
	return b.start
}

func (b *dbBlock) Checksum() *uint32 {
	cksum := b.checksum
	return &cksum
}

func (b *dbBlock) Stream(blocker context.Context) (xio.SegmentReader, error) {
	if b.closed {
		return nil, errReadFromClosedBlock
	}
	b.ctx.DependsOn(blocker)
	// If the block is not writable, and the segment is empty, it means
	// there are no data encoded in this block, so we return a nil reader.
	if b.segment.Head == nil && b.segment.Tail == nil {
		return nil, nil
	}
	s := b.opts.SegmentReaderPool().Get()
	// NB(r): We explicitly pass a new segment without a HeadPool to avoid
	// the segment reader returning these byte slices to the pool again as these
	// are immutable slices that are shared amongst all the readers.
	s.Reset(ts.Segment{
		Head: b.segment.Head,
		Tail: b.segment.Tail,
	})
	blocker.RegisterFinalizer(context.FinalizerFn(s.Close))
	return s, nil
}

// Reset resets the block start time and the encoder.
func (b *dbBlock) Reset(start time.Time, segment ts.Segment) {
	if !b.closed {
		b.ctx.Close()
	}
	b.ctx = b.opts.ContextPool().Get()
	b.start = start
	b.closed = false
	b.resetSegment(segment)
}

func (b *dbBlock) resetSegment(seg ts.Segment) {
	b.segment = seg
	b.checksum = digest.SegmentChecksum(seg)

	b.ctx.RegisterFinalizer(&seg)
}

func (b *dbBlock) Close() {
	if b.closed {
		return
	}

	b.closed = true
	b.ctx.Close()

	if pool := b.opts.DatabaseBlockPool(); pool != nil {
		pool.Put(b)
	}
}

type databaseSeriesBlocks struct {
	opts  Options
	elems map[time.Time]DatabaseBlock
	min   time.Time
	max   time.Time
}

// NewDatabaseSeriesBlocks creates a databaseSeriesBlocks instance.
func NewDatabaseSeriesBlocks(capacity int, opts Options) DatabaseSeriesBlocks {
	return &databaseSeriesBlocks{
		opts:  opts,
		elems: make(map[time.Time]DatabaseBlock, capacity),
	}
}

func (dbb *databaseSeriesBlocks) Options() Options {
	return dbb.opts
}

func (dbb *databaseSeriesBlocks) Len() int {
	return len(dbb.elems)
}

func (dbb *databaseSeriesBlocks) AddBlock(block DatabaseBlock) {
	start := block.StartTime()
	if dbb.min.Equal(timeZero) || start.Before(dbb.min) {
		dbb.min = start
	}
	if dbb.max.Equal(timeZero) || start.After(dbb.max) {
		dbb.max = start
	}
	dbb.elems[start] = block
}

func (dbb *databaseSeriesBlocks) AddSeries(other DatabaseSeriesBlocks) {
	if other == nil {
		return
	}
	blocks := other.AllBlocks()
	for _, b := range blocks {
		dbb.AddBlock(b)
	}
}

// MinTime returns the min time of the blocks contained.
func (dbb *databaseSeriesBlocks) MinTime() time.Time {
	return dbb.min
}

// MaxTime returns the max time of the blocks contained.
func (dbb *databaseSeriesBlocks) MaxTime() time.Time {
	return dbb.max
}

func (dbb *databaseSeriesBlocks) BlockAt(t time.Time) (DatabaseBlock, bool) {
	b, ok := dbb.elems[t]
	return b, ok
}

func (dbb *databaseSeriesBlocks) AllBlocks() map[time.Time]DatabaseBlock {
	return dbb.elems
}

func (dbb *databaseSeriesBlocks) RemoveBlockAt(t time.Time) {
	if _, exists := dbb.elems[t]; !exists {
		return
	}
	delete(dbb.elems, t)
	if !dbb.min.Equal(t) && !dbb.max.Equal(t) {
		return
	}
	dbb.min, dbb.max = timeZero, timeZero
	if len(dbb.elems) == 0 {
		return
	}
	for k := range dbb.elems {
		if dbb.min == timeZero || dbb.min.After(k) {
			dbb.min = k
		}
		if dbb.max == timeZero || dbb.max.Before(k) {
			dbb.max = k
		}
	}
}

func (dbb *databaseSeriesBlocks) RemoveAll() {
	for t, block := range dbb.elems {
		block.Close()
		delete(dbb.elems, t)
	}
}

func (dbb *databaseSeriesBlocks) Close() {
	dbb.RemoveAll()
}
