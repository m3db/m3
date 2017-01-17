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
	"github.com/m3db/m3x/clock"
)

var (
	errReadFromClosedBlock         = errors.New("attempt to read from a closed block")
	errRetrievableBlockNoRetriever = errors.New("attempt to read from a retrievable block with no retriever")

	timeZero = time.Time{}
)

type dbBlock struct {
	opts     Options
	nowFn    clock.NowFn
	ctx      context.Context
	start    time.Time
	segment  ts.Segment
	length   int
	checksum uint32

	lastAccess time.Time

	retriever     DatabaseBlockRetriever
	retrieveShard uint32
	retrieveID    ts.ID
	onRetrieve    OnRetrieveBlock

	closed bool
}

// NewDatabaseBlock creates a new DatabaseBlock instance.
func NewDatabaseBlock(start time.Time, segment ts.Segment, opts Options) DatabaseBlock {
	b := &dbBlock{
		opts:   opts,
		nowFn:  opts.ClockOptions().NowFn(),
		ctx:    opts.ContextPool().Get(),
		start:  start,
		closed: false,
	}
	if segment.Len() > 0 {
		b.resetSegment(segment)
	}
	return b
}

// NewRetrievableDatabaseBlock creates a new retrievable DatabaseBlock instance.
func NewRetrievableDatabaseBlock(
	start time.Time,
	retriever DatabaseBlockRetriever,
	metadata RetrievableBlockMetadata,
	opts Options,
) DatabaseBlock {
	b := &dbBlock{
		opts:   opts,
		ctx:    opts.ContextPool().Get(),
		start:  start,
		closed: false,
	}
	b.resetRetrievable(retriever, metadata)
	return b
}

func (b *dbBlock) StartTime() time.Time {
	return b.start
}

func (b *dbBlock) LastAccessTime() time.Time {
	return b.lastAccess
}

func (b *dbBlock) Len() int {
	return b.length
}

func (b *dbBlock) Checksum() uint32 {
	return b.checksum
}

func (b *dbBlock) Stream(blocker context.Context) (xio.SegmentReader, error) {
	if b.closed {
		return nil, errReadFromClosedBlock
	}

	b.ctx.DependsOn(blocker)
	b.lastAccess = b.nowFn()

	// If the block retrieve ID is set then it must be retrieved
	if b.retriever != nil {
		shard := b.retrieveShard
		id := b.retrieveID
		return b.retriever.Stream(shard, id, b.start, b.onRetrieve)
	}
	// If the block is not writable, and the segment is empty, it means
	// there are no data encoded in this block, so we return a nil reader.
	if b.segment.Head == nil && b.segment.Tail == nil {
		return nil, nil
	}
	s := b.opts.SegmentReaderPool().Get()
	// NB(r): We explicitly create a new segment to ensure references
	// are taken to the bytes refs and to not finalize the bytes.
	s.Reset(ts.NewSegment(b.segment.Head, b.segment.Tail, ts.FinalizeNone))
	blocker.RegisterFinalizer(context.FinalizerFn(s.Close))
	return s, nil
}

func (b *dbBlock) SetOnRetrieveBlock(onRetrieve OnRetrieveBlock) {
	b.onRetrieve = onRetrieve
}

func (b *dbBlock) IsRetrieved() bool {
	return b.retriever == nil
}

func (b *dbBlock) Reset(start time.Time, segment ts.Segment) {
	if !b.closed {
		b.ctx.Close()
	}
	b.ctx = b.opts.ContextPool().Get()
	b.start = start
	b.closed = false
	b.lastAccess = b.nowFn()
	b.resetSegment(segment)
}

func (b *dbBlock) ResetRetrievable(
	start time.Time,
	retriever DatabaseBlockRetriever,
	metadata RetrievableBlockMetadata,
) {
	if !b.closed {
		b.ctx.Close()
	}
	b.ctx = b.opts.ContextPool().Get()
	b.start = start
	b.closed = false
	b.lastAccess = b.nowFn()
	b.resetRetrievable(retriever, metadata)
}

func (b *dbBlock) resetSegment(seg ts.Segment) {
	b.segment = seg
	b.length = seg.Len()
	b.checksum = digest.SegmentChecksum(seg)

	b.retriever = nil
	b.retrieveShard = 0
	b.retrieveID = nil

	b.ctx.RegisterFinalizer(&seg)
}

func (b *dbBlock) resetRetrievable(
	retriever DatabaseBlockRetriever,
	metadata RetrievableBlockMetadata,
) {
	b.segment = ts.Segment{}
	b.length = metadata.Length
	b.checksum = metadata.Checksum

	b.retriever = retriever
	b.retrieveShard = metadata.Shard
	b.retrieveID = metadata.ID
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
