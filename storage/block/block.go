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
	errReadFromClosedBlock       = errors.New("attempt to read from a closed block")
	errTriedToMergeBlockFromDisk = errors.New("[invariant violated] tried to merge a block that was retrieved from disk")

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

	onEvicted OnEvictedFromWiredList

	// listState contains state that the Wired List requires in order to track a block's
	// position in the wired list. All the state in this struct is "owned" by the wired
	// list and should only be accessed by the Wired List itself. Does not require any
	// synchronization because the WiredList is not concurrent.
	listState listState

	checksum uint32

	wasRetrievedFromDisk bool
	closed               bool
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

func (b *dbBlock) Checksum() (uint32, error) {
	b.RLock()
	checksum := b.checksum
	hasMergeTarget := b.mergeTarget != nil
	b.RUnlock()

	if !hasMergeTarget {
		return checksum, nil
	}

	b.Lock()
	defer b.Unlock()
	// Since we released the lock temporarily we need to check again.
	hasMergeTarget = b.mergeTarget != nil
	if !hasMergeTarget {
		return b.checksum, nil
	}

	tempCtx := b.opts.ContextPool().Get()

	stream, err := b.stream(tempCtx)
	if err != nil {
		return 0, err
	}

	// This will merge the existing stream with the merge target's stream,
	// as well as recalculate and store the new checksum.
	_, err = b.forceMergeWithLock(tempCtx, stream)
	if err != nil {
		return 0, err
	}

	return b.checksum, nil
}

func (b *dbBlock) OnRetrieveBlock(
	id ident.ID,
	_ ident.TagIterator,
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
	b.wasRetrievedFromDisk = true
}

func (b *dbBlock) Stream(blocker context.Context) (xio.SegmentReader, error) {
	b.RLock()
	defer b.RUnlock()

	if b.closed {
		return nil, errReadFromClosedBlock
	}

	b.ctx.DependsOn(blocker)

	stream, err := b.stream(blocker)
	if err != nil {
		return nil, err
	}

	if b.mergeTarget == nil {
		return stream, nil
	}

	// This will merge the existing stream with the merge target's stream,
	// as well as recalculate and store the new checksum.
	return b.forceMergeWithLock(blocker, stream)
}

func (b *dbBlock) IsRetrieved() bool {
	b.RLock()
	retrieved := b.retriever == nil
	b.RUnlock()
	return retrieved
}

func (b *dbBlock) WasRetrievedFromDisk() bool {
	b.RLock()
	wasRetrieved := b.wasRetrievedFromDisk
	b.RUnlock()
	return wasRetrieved
}

func (b *dbBlock) IsCachedBlock() bool {
	b.RLock()
	retrieved := b.retriever == nil
	wasRetrieved := b.wasRetrievedFromDisk
	b.RUnlock()
	return !retrieved || wasRetrieved
}

func (b *dbBlock) Merge(other DatabaseBlock) error {
	b.Lock()
	if b.wasRetrievedFromDisk || other.WasRetrievedFromDisk() {
		// We use Merge to lazily merge blocks that eventually need to be flushed to disk
		// If we try to perform a merge on blocks that were retrieved from disk then we've
		// violated an invariant and probably have a bug that is causing data loss.
		b.Unlock()
		return errTriedToMergeBlockFromDisk
	}

	if b.mergeTarget == nil {
		b.mergeTarget = other
	} else {
		b.mergeTarget.Merge(other)
	}

	b.Unlock()
	return nil
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

func (b *dbBlock) stream(ctx context.Context) (xio.SegmentReader, error) {
	b.ctx.DependsOn(ctx)

	// If the block retrieve ID is set then it must be retrieved
	var (
		stream xio.SegmentReader
		err    error
	)
	if b.retriever != nil {
		start := b.startWithLock()
		stream, err = b.retriever.Stream(ctx, b.retrieveID, start, b)
		if err != nil {
			return nil, err
		}
	} else {
		stream = b.opts.SegmentReaderPool().Get()
		stream.Reset(ts.NewSegment(b.segment.Head, b.segment.Tail, ts.FinalizeNone))
		ctx.RegisterFinalizer(stream)
	}

	return stream, nil
}

// TODO(rartoul): The existing ctx is still holding a reference to the old segment so that will hang around
// and waste memory until the block is closed. We could improve this by swapping out the underlying ctx with
// a new one, allowing us to close the old one and release the old segment, freeing memory.
func (b *dbBlock) forceMergeWithLock(ctx context.Context, stream xio.SegmentReader) (xio.SegmentReader, error) {
	targetStream, err := b.mergeTarget.Stream(ctx)
	if err != nil {
		return nil, err
	}

	mergedStream := newDatabaseMergedBlockReader(b.startWithLock(),
		mergeableStream{stream: stream, finalize: false},       // Should have been marked for finalization by the caller
		mergeableStream{stream: targetStream, finalize: false}, // Already marked for finalization by the Stream() call above
		b.opts)
	mergedSegment, err := mergedStream.Segment()
	if err != nil {
		return nil, err
	}

	b.resetMergeTargetWithLock()
	b.resetSegmentWithLock(mergedSegment)
	return mergedStream, nil
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
	b.wasRetrievedFromDisk = false

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
	b.wasRetrievedFromDisk = false
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
	retrieveID           ident.ID
	startTime            time.Time
	closed               bool
	wasRetrievedFromDisk bool
}

// wiredListEntry generates a wiredListEntry for the block, and should only
// be used by the WiredList.
func (b *dbBlock) wiredListEntry() wiredListEntry {
	b.RLock()
	result := wiredListEntry{
		closed:               b.closed,
		retrieveID:           b.retrieveID,
		wasRetrievedFromDisk: b.wasRetrievedFromDisk,
		startTime:            b.startWithLock(),
	}
	b.RUnlock()
	return result
}

func (b *dbBlock) SetOnEvictedFromWiredList(onEvicted OnEvictedFromWiredList) {
	b.Lock()
	b.onEvicted = onEvicted
	b.Unlock()
}

func (b *dbBlock) OnEvictedFromWiredList() OnEvictedFromWiredList {
	b.RLock()
	onEvicted := b.onEvicted
	b.RUnlock()
	return onEvicted
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
