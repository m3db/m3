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

	"github.com/m3db/m3db/digest"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3db/x/xio"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"
)

var (
	errReadFromClosedBlock = errors.New("attempt to read from a closed block")

	timeZero = time.Time{}
)

type dbBlock struct {
	sync.RWMutex

	opts           Options
	ctx            context.Context
	startUnixNanos int64
	segment        ts.Segment
	length         int

	lastReadUnixNanos int64

	mergeTarget DatabaseBlock

	retriever  DatabaseShardBlockRetriever
	retrieveID ident.ID

	owner Owner

	// listState contains state that the Wired List requires in order to track a block's
	// position in the wired list. All the state in this struct is "owned" by the wired
	// list and should only be accessed by the Wired List itself. Does not require any
	// synchronization because the WiredList is not concurrent.
	listState listState

	checksum uint32

	wasRetrieved bool
	closed       bool
}

type listState struct {
	next                      DatabaseBlock
	prev                      DatabaseBlock
	nextPrevUpdatedAtUnixNano int64
}

// NewDatabaseBlock creates a new DatabaseBlock instance.
func NewDatabaseBlock(
	start time.Time,
	segment ts.Segment,
	opts Options,
) DatabaseBlock {
	b := &dbBlock{
		opts:           opts,
		ctx:            opts.ContextPool().Get(),
		startUnixNanos: start.UnixNano(),
		closed:         false,
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
		opts:           opts,
		ctx:            opts.ContextPool().Get(),
		startUnixNanos: start.UnixNano(),
		closed:         false,
	}
	b.resetRetrievableWithLock(retriever, metadata)
	return b
}

func (b *dbBlock) StartTime() time.Time {
	b.RLock()
	start := b.startWithLock()
	b.RUnlock()
	return start
}

func (b *dbBlock) startWithLock() time.Time {
	return time.Unix(0, b.startUnixNanos)
}

func (b *dbBlock) SetLastReadTime(value time.Time) {
	// Use an int64 to avoid needing a write lock for
	// this high frequency called method (i.e. each individual
	// read needing a write lock would be excessive)
	atomic.StoreInt64(&b.lastReadUnixNanos, value.UnixNano())
}

func (b *dbBlock) LastReadTime() time.Time {
	return time.Unix(0, atomic.LoadInt64(&b.lastReadUnixNanos))
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
	id ident.ID,
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
	b.retrieveID = id
	b.wasRetrieved = true
}

func (b *dbBlock) Stream(blocker context.Context) (xio.SegmentReader, error) {
	b.RLock()
	defer b.RUnlock()

	if b.closed {
		return nil, errReadFromClosedBlock
	}

	b.ctx.DependsOn(blocker)

	// If the block retrieve ID is set then it must be retrieved
	var (
		stream             xio.SegmentReader
		err                error
		fromBlockRetriever bool
	)
	if b.retriever != nil {
		fromBlockRetriever = true
		start := b.startWithLock()
		stream, err = b.retriever.Stream(blocker, b.retrieveID, start, b)
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

	if !fromBlockRetriever {
		// Register the finalizer for the stream, the block retriever already
		// registers the stream as a finalizer for the context so we only perform
		// this if we return a stream directly from this block
		blocker.RegisterFinalizer(stream)
	}
	return stream, nil
}

func (b *dbBlock) IsRetrieved() bool {
	b.RLock()
	retrieved := b.retriever == nil
	b.RUnlock()
	return retrieved
}

func (b *dbBlock) WasRetrieved() bool {
	b.RLock()
	wasRetrieved := b.wasRetrieved
	b.RUnlock()
	return wasRetrieved
}

func (b *dbBlock) IsCachedBlock() bool {
	b.RLock()
	retrieved := b.retriever == nil
	wasRetrieved := b.wasRetrieved
	b.RUnlock()
	return !retrieved || wasRetrieved
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
	b.startUnixNanos = start.UnixNano()
	atomic.StoreInt64(&b.lastReadUnixNanos, 0)
	b.closed = false
	b.resetMergeTargetWithLock()
}

func (b *dbBlock) resetSegmentWithLock(seg ts.Segment) {
	b.segment = seg
	b.length = seg.Len()
	b.checksum = digest.SegmentChecksum(seg)

	b.retriever = nil
	b.retrieveID = nil
	b.wasRetrieved = false

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
	b.wasRetrieved = false
}

func (b *dbBlock) Close() {
	b.Lock()
	defer b.Unlock()

	if b.closed {
		return
	}

	b.closed = true

	// NB(xichen): we use the worker pool to close the context instead doing
	// an asynchronous context close to explicitly control the context closing
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

// Should only be used by the WiredList.
func (b *dbBlock) next() DatabaseBlock {
	return b.listState.next
}

// Should only be used by the WiredList.
func (b *dbBlock) setNext(value DatabaseBlock) {
	b.listState.next = value
}

// Should only be used by the WiredList.
func (b *dbBlock) prev() DatabaseBlock {
	return b.listState.prev
}

// Should only be used by the WiredList.
func (b *dbBlock) setPrev(value DatabaseBlock) {
	b.listState.prev = value
}

// Should only be used by the WiredList.
func (b *dbBlock) nextPrevUpdatedAtUnixNano() int64 {
	return b.listState.nextPrevUpdatedAtUnixNano
}

// Should only be used by the WiredList.
func (b *dbBlock) setNextPrevUpdatedAtUnixNano(value int64) {
	b.listState.nextPrevUpdatedAtUnixNano = value
}

// wiredListEntry is a snapshot of a subset of the block's state that the WiredList
// uses to determine if a block is eligible for inclusion in the WiredList.
type wiredListEntry struct {
	closed       bool
	retrieveID   ident.ID
	wasRetrieved bool
	startTime    time.Time
}

// wiredListEntry generates a wiredListEntry for the block, and should only
// be used by the WiredList.
func (b *dbBlock) wiredListEntry() wiredListEntry {
	b.RLock()
	result := wiredListEntry{
		closed:       b.closed,
		retrieveID:   b.retrieveID,
		wasRetrieved: b.wasRetrieved,
		startTime:    b.startWithLock(),
	}
	b.RUnlock()
	return result
}

// SetOwner sets the owner of the block.
func (b *dbBlock) SetOwner(owner Owner) {
	b.Lock()
	b.owner = owner
	b.Unlock()
}

func (b *dbBlock) Owner() Owner {
	b.RLock()
	owner := b.owner
	b.RUnlock()
	return owner
}

type databaseSeriesBlocks struct {
	elems map[xtime.UnixNano]DatabaseBlock
	min   time.Time
	max   time.Time
}

// NewDatabaseSeriesBlocks creates a databaseSeriesBlocks instance.
func NewDatabaseSeriesBlocks(capacity int) DatabaseSeriesBlocks {
	return &databaseSeriesBlocks{
		elems: make(map[xtime.UnixNano]DatabaseBlock, capacity),
	}
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
	dbb.elems[xtime.ToUnixNano(start)] = block
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
	b, ok := dbb.elems[xtime.ToUnixNano(t)]
	return b, ok
}

func (dbb *databaseSeriesBlocks) AllBlocks() map[xtime.UnixNano]DatabaseBlock {
	return dbb.elems
}

func (dbb *databaseSeriesBlocks) RemoveBlockAt(t time.Time) {
	tNano := xtime.ToUnixNano(t)
	if _, exists := dbb.elems[tNano]; !exists {
		return
	}
	delete(dbb.elems, tNano)
	if !dbb.min.Equal(t) && !dbb.max.Equal(t) {
		return
	}
	dbb.min, dbb.max = timeZero, timeZero
	if len(dbb.elems) == 0 {
		return
	}
	for key := range dbb.elems {
		keyTime := key.ToTime()
		if dbb.min == timeZero || dbb.min.After(keyTime) {
			dbb.min = keyTime
		}
		if dbb.max == timeZero || dbb.max.Before(keyTime) {
			dbb.max = keyTime
		}
	}
}

func (dbb *databaseSeriesBlocks) RemoveAll() {
	for t, block := range dbb.elems {
		block.Close()
		delete(dbb.elems, t)
	}
}

func (dbb *databaseSeriesBlocks) Reset() {
	dbb.RemoveAll()
	// Ensure the old, possibly large map is GC'd
	dbb.elems = nil
	dbb.elems = make(map[xtime.UnixNano]DatabaseBlock)
	dbb.min = time.Time{}
	dbb.max = time.Time{}
}

func (dbb *databaseSeriesBlocks) Close() {
	dbb.RemoveAll()
	// Mark the map as nil to prevent maps that have grown large from wasting
	// space in the pool (Deleting elements from a large map will not cause
	// the underlying resources to shrink)
	dbb.elems = nil
}
