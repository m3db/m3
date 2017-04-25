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

	// NB(r): we cache this instance's stream context finalizer
	// function to avoid allocating a reference to the fn
	// every time a read occurs
	onStreamDoneFinalizer context.Finalizer

	opts Options
	ctx  context.Context

	startUnixNano int64
	segment       ts.Segment
	length        int
	checksum      uint32

	accessedUnixNano int64

	retriever  DatabaseShardBlockRetriever
	retrieveID ts.ID

	// For use in a list of blocks, notably for the wired list. The
	// doubly linked list pointers are on the block themselves to avoid
	// creating a heap allocated struct per block.
	// NB(r): Beware that the list controls the synchronization of these
	// fields to avoid unnecessary lock contention.
	// They should never be read or mutated by the block itself.
	next *dbBlock
	prev *dbBlock

	closed bool
}

// NewDatabaseBlock creates a new DatabaseBlock instance.
func NewDatabaseBlock(
	start time.Time,
	segment ts.Segment,
	opts Options,
) DatabaseBlock {
	b := &dbBlock{
		opts:          opts,
		ctx:           opts.ContextPool().Get(),
		startUnixNano: start.UnixNano(),
		closed:        false,
	}
	b.onStreamDoneFinalizer = context.FinalizerFn(b.updateWiredList)
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
	b.onStreamDoneFinalizer = context.FinalizerFn(b.updateWiredList)
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

func (b *dbBlock) updateWiredList() {
	list := b.opts.WiredList()
	list.update(b)
}

func (b *dbBlock) wiredWithLock() bool {
	return !b.segment.Empty()
}

type wiredListEntry struct {
	start      time.Time
	length     int
	checksum   uint32
	wired      bool
	retriever  DatabaseShardBlockRetriever
	retrieveID ts.ID
}

func (b *dbBlock) wiredListEntry() wiredListEntry {
	b.RLock()
	result := wiredListEntry{
		start:      b.startWithLock(),
		length:     b.length,
		checksum:   b.checksum,
		wired:      b.wiredWithLock(),
		retriever:  b.retriever,
		retrieveID: b.retrieveID,
	}
	b.RUnlock()
	return result
}

func (b *dbBlock) Stream(blocker context.Context) (xio.SegmentReader, error) {
	b.RLock()
	defer b.RUnlock()

	if b.closed {
		return nil, errReadFromClosedBlock
	}

	b.ctx.DependsOn(blocker)

	reader, err := b.readerWithLock()
	if err != nil {
		return nil, err
	}

	// Use an int64 to avoid needing a write lock for
	// the high frequency stream method
	atomic.StoreInt64(&b.accessedUnixNano, b.now().UnixNano())

	// Register the finalizer for the stream
	blocker.RegisterFinalizer(reader)

	// Register a callback to update the wired list
	// if this block is wired at the completion of the
	// read.
	//
	// NB(r): Performing this after the read is complete
	// keeps the wired list being a point of contention
	// only after a read and all bytes are sent to the client.
	//
	// This helps keep the read latency tails low.
	blocker.RegisterFinalizer(b.onStreamDoneFinalizer)

	return reader, nil
}

func (b *dbBlock) readerWithLock() (xio.SegmentReader, error) {
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

	return stream, nil
}

func (b *dbBlock) IsRetrieved() bool {
	b.RLock()
	result := b.wiredWithLock()
	b.RUnlock()
	return result
}

func (b *dbBlock) Merge(other DatabaseBlock) error {
	b.Lock()
	defer b.Unlock()

	reader, err := b.readerWithLock()
	if err != nil {
		return err
	}

	ctx := b.opts.ContextPool().Get()
	defer ctx.Close()

	targetReader, err := other.Stream(ctx)
	if err != nil {
		return err
	}

	mergedReader := newDatabaseMergedBlockReader(b.startWithLock(),
		reader, targetReader, b.opts)

	segment, err := mergedReader.Segment()
	if err != nil {
		return err
	}

	// NB(r): If there was a current retriever and retrieve ID
	// they are invalid now as they reference a different
	// set of data.
	b.retriever = nil
	b.retrieveID = nil

	b.resetSegmentWithLock(segment)

	return nil
}

func (b *dbBlock) Reset(start time.Time, segment ts.Segment) {
	b.Lock()
	b.resetNewBlockStartWithLock(start)
	b.resetSegmentWithLock(segment)
	b.Unlock()
}

func (b *dbBlock) ResetRetrievable(
	start time.Time,
	retriever DatabaseShardBlockRetriever,
	metadata RetrievableBlockMetadata,
) {
	b.Lock()
	b.resetNewBlockStartWithLock(start)
	b.resetRetrievableWithLock(retriever, metadata)
	b.Unlock()
}

func (b *dbBlock) unwire() bool {
	b.Lock()
	defer b.Unlock()

	if !b.wiredWithLock() || b.retriever != nil || b.retrieveID != nil {
		// Cannot unwire if not wired or without a block retriever and retrieve ID
		return false
	}

	b.resetNewBlockStartWithLock(b.startWithLock())
	b.resetRetrievableWithLock(b.retriever, RetrievableBlockMetadata{
		ID:       b.retrieveID,
		Length:   b.length,
		Checksum: b.checksum,
	})

	return true
}

func (b *dbBlock) resetNewBlockStartWithLock(start time.Time) {
	if !b.closed {
		b.ctx.Close()
	}
	b.ctx = b.opts.ContextPool().Get()
	b.startUnixNano = start.UnixNano()
	b.closed = false
	atomic.StoreInt64(&b.accessedUnixNano, b.now().UnixNano())
}

func (b *dbBlock) resetSegmentWithLock(seg ts.Segment) {
	b.segment = seg
	b.length = seg.Len()
	b.checksum = digest.SegmentChecksum(seg)

	b.ctx.RegisterFinalizer(&seg)

	// Trigger update of the wired list
	b.updateWiredList()
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

	// Trigger update of the wired list
	b.updateWiredList()
}

func (b *dbBlock) Close() {
	b.Lock()
	defer b.Unlock()

	if b.closed {
		return
	}

	b.closed = true

	// Trigger update of the wired list
	b.retriever = nil
	b.retrieveID = nil
	b.updateWiredList()

	// NB(xichen): we use the worker pool to close the context instead doing
	// an asychronous context close to explicitly control the context closing
	// concurrency. This is particularly important during a node removal because
	// all the shards are removed at once, causing a goroutine explosion without
	// limiting the concurrency. We also cannot do a blocking close here because
	// the block may be closed before the underlying context is closed, which
	// causes a deadlock if the block and the underlying context are closed
	// from within the same goroutine.
	b.opts.CloseContextWorkers().Go(b.ctx.BlockingClose)

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

// NewDatabaseSeriesBlocks returns a new map container of blocks.
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
