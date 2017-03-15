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
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/digest"
	"github.com/m3db/m3db/ts"
	xio "github.com/m3db/m3db/x/io"
)

var (
	errReadFromClosedBlock         = errors.New("attempt to read from a closed block")
	errRetrievableBlockNoRetriever = errors.New("attempt to read from a retrievable block with no retriever")

	timeZero = time.Time{}
)

type dbBlock struct {
	sync.RWMutex

	opts          Options
	ctx           context.Context
	startUnixNano int64
	segment       ts.Segment
	length        int
	checksum      uint32

	accessedUnixNano int64

	mergeTarget DatabaseBlock

	retriever  DatabaseShardBlockRetriever
	retrieveID ts.ID

	closed bool
}

// NewDatabaseBlock creates a new DatabaseBlock instance.
func NewDatabaseBlock(start time.Time, segment ts.Segment, opts Options) DatabaseBlock {
	b := &dbBlock{
		opts:          opts,
		ctx:           opts.ContextPool().Get(),
		startUnixNano: start.UnixNano(),
		closed:        false,
	}
	if segment.Len() > 0 {
		b.resetSegmentWithLock(segment)
	}
	return b
}

// NewRetrievableDatabaseBlock creates a new retrievable DatabaseBlock instance.
func NewRetrievableDatabaseBlock(
	start time.Time,
	retriever DatabaseShardBlockRetriever,
	metadata RetrievableBlockMetadata,
	opts Options,
) DatabaseBlock {
	b := &dbBlock{
		opts:          opts,
		ctx:           opts.ContextPool().Get(),
		startUnixNano: start.UnixNano(),
		closed:        false,
	}
	b.resetRetrievableWithLock(retriever, metadata)
	return b
}

func (b *dbBlock) now() time.Time {
	nowFn := b.opts.ClockOptions().NowFn()
	return nowFn()
}

func (b *dbBlock) StartTime() time.Time {
	b.RLock()
	start := b.startWithLock()
	b.RUnlock()
	return start
}

func (b *dbBlock) startWithLock() time.Time {
	return time.Unix(0, b.startUnixNano)
}

func (b *dbBlock) LastAccessTime() time.Time {
	return time.Unix(0, atomic.LoadInt64(&b.accessedUnixNano))
}

func (b *dbBlock) Len() int {
	b.RLock()
	length := b.length
	b.RUnlock()
	return length
}

func (b *dbBlock) Checksum() uint32 {
	b.RLock()
	checksum := b.checksum
	b.RUnlock()
	return checksum
}

func (b *dbBlock) OnRetrieveBlock(
	id ts.ID,
	startTime time.Time,
	segment ts.Segment,
) {
	b.Lock()
	defer b.Unlock()

	if b.closed ||
		!id.Equal(b.retrieveID) ||
		!startTime.Equal(b.startWithLock()) {
		return
	}

	b.resetSegmentWithLock(segment)
}

func (b *dbBlock) Stream(blocker context.Context) (xio.SegmentReader, error) {
	b.RLock()
	defer b.RUnlock()

	if b.closed {
		return nil, errReadFromClosedBlock
	}

	b.ctx.DependsOn(blocker)

	// Use an int64 to avoid needing a write lock for
	// the high frequency stream method
	atomic.StoreInt64(&b.accessedUnixNano, b.now().UnixNano())

	// If the block retrieve ID is set then it must be retrieved
	var (
		stream xio.SegmentReader
		err    error
	)
	if b.retriever != nil {
		stream, err = b.retriever.Stream(b.retrieveID, b.startWithLock(), b)
		if err != nil {
			return nil, err
		}
	} else {
		stream = b.opts.SegmentReaderPool().Get()
		// NB(r): We explicitly create a new segment to ensure references
		// are taken to the bytes refs and to not finalize the bytes.
		stream.Reset(ts.NewSegment(b.segment.Head, b.segment.Tail, ts.FinalizeNone))
	}

	if b.mergeTarget != nil {
		var mergeStream xio.SegmentReader
		mergeStream, err = b.mergeTarget.Stream(blocker)
		if err != nil {
			stream.Finalize()
			return nil, err
		}
		// Return a lazily merged stream
		// TODO(r): once merged reset this block with the contents of it
		stream = newDatabaseMergedBlockReader(b.startWithLock(), stream, mergeStream, b.opts)
	}

	// Register the finalizer for the stream
	blocker.RegisterFinalizer(stream)

	return stream, nil
}

func (b *dbBlock) IsRetrieved() bool {
	b.RLock()
	retrieved := b.retriever == nil
	b.RUnlock()
	return retrieved
}

func (b *dbBlock) Merge(other DatabaseBlock) {
	b.Lock()
	b.resetMergeTargetWithLock()
	b.mergeTarget = other
	b.Unlock()
}

func (b *dbBlock) Reset(start time.Time, segment ts.Segment) {
	b.Lock()
	defer b.Unlock()
	b.resetNewBlockStartWithLock(start)
	b.resetSegmentWithLock(segment)
}

func (b *dbBlock) ResetRetrievable(
	start time.Time,
	retriever DatabaseShardBlockRetriever,
	metadata RetrievableBlockMetadata,
) {
	b.Lock()
	defer b.Unlock()
	b.resetNewBlockStartWithLock(start)
	b.resetRetrievableWithLock(retriever, metadata)
}

func (b *dbBlock) resetNewBlockStartWithLock(start time.Time) {
	if !b.closed {
		b.ctx.Close()
	}
	b.ctx = b.opts.ContextPool().Get()
	b.startUnixNano = start.UnixNano()
	b.closed = false
	b.resetMergeTargetWithLock()
	atomic.StoreInt64(&b.accessedUnixNano, b.now().UnixNano())
}

func (b *dbBlock) resetSegmentWithLock(seg ts.Segment) {
	b.segment = seg
	b.length = seg.Len()
	b.checksum = digest.SegmentChecksum(seg)

	b.retriever = nil
	b.retrieveID = nil

	b.ctx.RegisterFinalizer(&seg)
}

func (b *dbBlock) resetRetrievableWithLock(
	retriever DatabaseShardBlockRetriever,
	metadata RetrievableBlockMetadata,
) {
	b.segment = ts.Segment{}
	b.length = metadata.Length
	b.checksum = metadata.Checksum

	b.retriever = retriever
	b.retrieveID = metadata.ID
}

func (b *dbBlock) Close() {
	b.Lock()
	defer b.Unlock()

	if b.closed {
		return
	}

	b.closed = true

	// NB(xichen): we use the worker pool to close the context instead doing
	// an asychronous context close to explicitly control the context closing
	// concurrency. This is particularly important during a node removal because
	// all the shards are removed at once, causing a goroutine explosion without
	// limiting the concurrency. We also cannot do a blocking close here because
	// the block may be closed before the underlying context is closed, which
	// causes a deadlock if the block and the underlying context are closed
	// from within the same goroutine.
	b.opts.CloseContextWorkers().Go(b.ctx.BlockingClose)

	b.resetMergeTargetWithLock()
	if pool := b.opts.DatabaseBlockPool(); pool != nil {
		pool.Put(b)
	}
}

func (b *dbBlock) resetMergeTargetWithLock() {
	if b.mergeTarget != nil {
		b.mergeTarget.Close()
	}
	b.mergeTarget = nil
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
