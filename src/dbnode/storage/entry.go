// Copyright (c) 2018 Uber Technologies, Inc.
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
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber-go/tally"
	xatomic "go.uber.org/atomic"

	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/ts/writes"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/resource"
	xtime "github.com/m3db/m3/src/x/time"
)

// IndexWriter accepts index inserts.
type IndexWriter interface {
	// WritePending indexes the provided pending entries.
	WritePending(
		pending []writes.PendingIndexInsert,
	) error

	// BlockStartForWriteTime returns the index block start
	// time for the given writeTime.
	BlockStartForWriteTime(
		writeTime xtime.UnixNano,
	) xtime.UnixNano
}

// EntryMetrics are metrics for an entry.
type EntryMetrics struct {
	gcNoReconcile    tally.Counter
	gcNeedsReconcile tally.Counter

	duplicateNoReconcile    tally.Counter
	duplicateNeedsReconcile tally.Counter
}

// NewEntryMetrics builds an entry metrics.
func NewEntryMetrics(scope tally.Scope) *EntryMetrics {
	return &EntryMetrics{
		gcNoReconcile: scope.Tagged(map[string]string{
			"reconcile": "no_reconcile",
			"path":      "gc",
		}).Counter("count"),
		gcNeedsReconcile: scope.Tagged(map[string]string{
			"reconcile": "needs_reconcile",
			"path":      "gc",
		}).Counter("count"),

		duplicateNoReconcile: scope.Tagged(map[string]string{
			"reconcile": "no_reconcile",
			"path":      "duplicate",
		}).Counter("count"),

		duplicateNeedsReconcile: scope.Tagged(map[string]string{
			"reconcile": "needs_reconcile",
			"path":      "duplicate",
		}).Counter("count"),
	}
}

// Entry is the entry in the shard ident.ID -> series map. It has additional
// members to track lifecycle and minimize indexing overhead.
// NB: users are expected to use `NewEntry` to construct these objects.
type Entry struct {
	ID                       ident.ID
	Shard                    Shard
	Series                   series.DatabaseSeries
	Index                    uint64
	IndexGarbageCollected    *xatomic.Bool
	insertTime               *xatomic.Int64
	indexWriter              IndexWriter
	curReadWriters           int32
	reverseIndex             entryIndexState
	nowFn                    clock.NowFn
	metrics                  *EntryMetrics
	pendingIndexBatchSizeOne []writes.PendingIndexInsert
}

// ensure Entry satisfies the `doc.OnIndexSeries` interface.
var _ doc.OnIndexSeries = &Entry{}

// ensure Entry satisfies the `bootstrap.SeriesRef` interface.
var _ bootstrap.SeriesRef = &Entry{}

// ensure Entry satisfies the `bootstrap.SeriesRefResolver` interface.
var _ bootstrap.SeriesRefResolver = &Entry{}

// NewEntryOptions supplies options for a new entry.
type NewEntryOptions struct {
	Shard        Shard
	Series       series.DatabaseSeries
	Index        uint64
	IndexWriter  IndexWriter
	NowFn        clock.NowFn
	EntryMetrics *EntryMetrics
}

// NewEntry returns a new Entry.
func NewEntry(opts NewEntryOptions) *Entry {
	nowFn := time.Now
	if opts.NowFn != nil {
		nowFn = opts.NowFn
	}
	entry := &Entry{
		ID:                       opts.Series.ID(),
		Shard:                    opts.Shard,
		Series:                   opts.Series,
		Index:                    opts.Index,
		IndexGarbageCollected:    xatomic.NewBool(false),
		insertTime:               xatomic.NewInt64(0),
		indexWriter:              opts.IndexWriter,
		nowFn:                    nowFn,
		pendingIndexBatchSizeOne: make([]writes.PendingIndexInsert, 1),
		reverseIndex:             newEntryIndexState(),
		metrics:                  opts.EntryMetrics,
	}
	return entry
}

// StringID returns the index series ID, as a string.
func (entry *Entry) StringID() string {
	return entry.ID.String()
}

// ReaderWriterCount returns the current ref count on the Entry.
func (entry *Entry) ReaderWriterCount() int32 {
	return atomic.LoadInt32(&entry.curReadWriters)
}

// IncrementReaderWriterCount increments the ref count on the Entry.
func (entry *Entry) IncrementReaderWriterCount() {
	atomic.AddInt32(&entry.curReadWriters, 1)
}

// DecrementReaderWriterCount decrements the ref count on the Entry.
func (entry *Entry) DecrementReaderWriterCount() {
	atomic.AddInt32(&entry.curReadWriters, -1)
}

// IndexedBlockCount returns the count of indexed block states.
func (entry *Entry) IndexedBlockCount() int {
	entry.reverseIndex.RLock()
	count := len(entry.reverseIndex.states)
	entry.reverseIndex.RUnlock()
	return count
}

// IndexedForBlockStart returns a bool to indicate if the Entry has been successfully
// indexed for the given index blockStart.
func (entry *Entry) IndexedForBlockStart(indexBlockStart xtime.UnixNano) bool {
	entry.reverseIndex.RLock()
	isIndexed := entry.reverseIndex.indexedWithRLock(indexBlockStart)
	entry.reverseIndex.RUnlock()
	return isIndexed
}

// IndexedRange returns minimum and maximum blockStart values covered by index entry.
// The range is inclusive. Note that there may be uncovered gaps within the range.
// Returns (0, 0) for an empty range.
func (entry *Entry) IndexedRange() (xtime.UnixNano, xtime.UnixNano) {
	entry.reverseIndex.RLock()
	min, max := entry.reverseIndex.indexedRangeWithRLock()
	entry.reverseIndex.RUnlock()
	return min, max
}

// ReconciledOnIndexSeries attempts to retrieve the most recent index entry from the
// shard if the entry this method was called on was never inserted there. If there
// is an error during retrieval, simply returns the current entry. Additionally,
// returns a cleanup function to run once finished using the reconciled entry and
// a boolean value indicating whether the result came from reconciliation or not.
func (entry *Entry) ReconciledOnIndexSeries() (doc.OnIndexSeries, resource.SimpleCloser, bool) {
	if entry.insertTime.Load() > 0 {
		return entry, resource.SimpleCloserFn(func() {}), false
	}

	e, _, err := entry.Shard.TryRetrieveSeriesAndIncrementReaderWriterCount(entry.ID)
	if err != nil || e == nil {
		return entry, resource.SimpleCloserFn(func() {}), false
	}

	// NB: attempt to merge the index series here, to ensure the returned
	// reconciled series will have each index block marked from both this and the
	// reconciliated series.
	entry.mergeInto(e)

	return e, resource.SimpleCloserFn(func() {
		e.DecrementReaderWriterCount()
	}), true
}

// MergeEntryIndexBlockStates merges the given states into the current
// indexed entry.
func (entry *Entry) MergeEntryIndexBlockStates(states doc.EntryIndexBlockStates) {
	entry.reverseIndex.Lock()
	for t, state := range states {
		set := false
		if state.Success {
			set = true
			entry.reverseIndex.setSuccessWithWLock(t)
		} else {
			// NB: setSuccessWithWLock(t) will perform the logic to determine if
			// minIndexedT/maxIndexedT need to be updated; if this is not being called
			// these should be updated.
			if entry.reverseIndex.maxIndexedT < t {
				entry.reverseIndex.maxIndexedT = t
			}
			if entry.reverseIndex.minIndexedT > t {
				entry.reverseIndex.minIndexedT = t
			}
		}

		if state.Attempt {
			set = true
			entry.reverseIndex.setAttemptWithWLock(t, false)
		}

		if !set {
			// NB: if not set through the above methods, need to create an index block
			// state at the given timestamp.
			entry.reverseIndex.states[t] = doc.EntryIndexBlockState{}
		}
	}

	entry.reverseIndex.Unlock()
}

// NeedsIndexUpdate returns a bool to indicate if the Entry needs to be indexed
// for the provided blockStart. It only allows a single index attempt at a time
// for a single entry.
// NB(prateek): NeedsIndexUpdate is a CAS, i.e. when this method returns true, it
// also sets state on the entry to indicate that a write for the given blockStart
// is going to be sent to the index, and other go routines should not attempt the
// same write. Callers are expected to ensure they follow this guideline.
// Further, every call to NeedsIndexUpdate which returns true needs to have a corresponding
// OnIndexFinalize() call. This is required for correct lifecycle maintenance.
func (entry *Entry) NeedsIndexUpdate(indexBlockStartForWrite xtime.UnixNano) bool {
	// first we try the low-cost path: acquire a RLock and see if the given block start
	// has been marked successful or that we've attempted it.
	entry.reverseIndex.RLock()
	alreadyIndexedOrAttempted := entry.reverseIndex.indexedOrAttemptedWithRLock(indexBlockStartForWrite)
	entry.reverseIndex.RUnlock()
	if alreadyIndexedOrAttempted {
		// if so, the entry does not need to be indexed.
		return false
	}

	// now acquire a write lock and set that we're going to attempt to do this so we don't try
	// multiple times.
	entry.reverseIndex.Lock()
	// NB(prateek): not defer-ing here, need to avoid the the extra ~150ns to minimize contention.

	// but first, we have to ensure no one has done so since we released the read lock
	alreadyIndexedOrAttempted = entry.reverseIndex.indexedOrAttemptedWithRLock(indexBlockStartForWrite)
	if alreadyIndexedOrAttempted {
		entry.reverseIndex.Unlock()
		return false
	}

	entry.reverseIndex.setAttemptWithWLock(indexBlockStartForWrite, true)
	entry.reverseIndex.Unlock()
	return true
}

// OnIndexPrepare prepares the Entry to be handed off to the indexing sub-system.
// NB(prateek): we retain the ref count on the entry while the indexing is pending,
// the callback executed on the entry once the indexing is completed releases this
// reference.
func (entry *Entry) OnIndexPrepare(blockStartNanos xtime.UnixNano) {
	entry.reverseIndex.Lock()
	entry.reverseIndex.setAttemptWithWLock(blockStartNanos, true)
	entry.reverseIndex.Unlock()
	entry.IncrementReaderWriterCount()
}

// OnIndexSuccess marks the given block start as successfully indexed.
func (entry *Entry) OnIndexSuccess(blockStartNanos xtime.UnixNano) {
	entry.reverseIndex.Lock()
	entry.reverseIndex.setSuccessWithWLock(blockStartNanos)
	entry.reverseIndex.Unlock()
}

// OnIndexFinalize marks any attempt for the given block start as finished
// and decrements the entry ref count.
func (entry *Entry) OnIndexFinalize(blockStartNanos xtime.UnixNano) {
	entry.reverseIndex.Lock()
	entry.reverseIndex.setAttemptWithWLock(blockStartNanos, false)
	entry.reverseIndex.Unlock()
	// indicate the index has released held reference for provided write
	entry.DecrementReaderWriterCount()
}

// IfAlreadyIndexedMarkIndexSuccessAndFinalize marks the entry as successfully
// indexed if already indexed and returns true. Otherwise returns false.
func (entry *Entry) IfAlreadyIndexedMarkIndexSuccessAndFinalize(
	blockStart xtime.UnixNano,
) bool {
	successAlready := false
	entry.reverseIndex.Lock()
	for _, state := range entry.reverseIndex.states {
		if state.Success {
			successAlready = true
			break
		}
	}
	if successAlready {
		entry.reverseIndex.setSuccessWithWLock(blockStart)
		entry.reverseIndex.setAttemptWithWLock(blockStart, false)
	}
	entry.reverseIndex.Unlock()
	if successAlready {
		// indicate the index has released held reference for provided write
		entry.DecrementReaderWriterCount()
	}
	return successAlready
}

// TryMarkIndexGarbageCollected checks if the entry is eligible to be garbage collected
// from the index. If so, it marks the entry as GCed and returns true. Otherwise returns false.
func (entry *Entry) TryMarkIndexGarbageCollected() bool {
	// Since series insertions + index insertions are done separately async, it is possible for
	// a series to be in the index but not have data written yet, and so any series not in the
	// lookup yet we cannot yet consider empty.
	e, _, err := entry.Shard.TryRetrieveSeriesAndIncrementReaderWriterCount(entry.ID)
	if err != nil || e == nil {
		return false
	}

	defer e.DecrementReaderWriterCount()

	// Was reconciled if the entry retrieved from the shard differs from the current.
	if e != entry {
		// If this entry needs further reconciliation, merge this entry's index
		// states into the
		entry.reverseIndex.RLock()
		e.MergeEntryIndexBlockStates(entry.reverseIndex.states)
		entry.reverseIndex.RUnlock()
	}

	// Consider non-empty if the entry is still being held since this could indicate
	// another thread holding a new series prior to writing to it.
	if e.ReaderWriterCount() > 1 {
		return false
	}

	// Series must be empty to be GCed. This happens when the data and index are flushed to disk and
	// so the series no longer has in-mem data.
	if !e.Series.IsEmpty() {
		return false
	}

	// Mark as GCed from index so the entry can be safely cleaned up in the shard.
	// The reference to this entry from the index is removed by the code path that
	// marks this GCed bool.
	e.IndexGarbageCollected.Store(true)

	if e != entry {
		entry.metrics.gcNeedsReconcile.Inc(1)
	} else {
		entry.metrics.gcNoReconcile.Inc(1)
	}

	return true
}

// mergeInto merges this entry index blocks into the provided index series.
func (entry *Entry) mergeInto(indexSeries doc.OnIndexSeries) {
	if entry == indexSeries {
		// NB: short circuit if attempting to merge an entry into itself.
		return
	}

	entry.reverseIndex.RLock()
	indexSeries.MergeEntryIndexBlockStates(entry.reverseIndex.states)
	entry.reverseIndex.RUnlock()
}

// TryReconcileDuplicates attempts to reconcile the index states of this entry.
func (entry *Entry) TryReconcileDuplicates() {
	// Since series insertions + index insertions are done separately async, it is possible for
	// a series to be in the index but not have data written yet, and so any series not in the
	// lookup yet we cannot yet consider empty.
	e, _, err := entry.Shard.TryRetrieveSeriesAndIncrementReaderWriterCount(entry.ID)
	if err != nil || e == nil {
		return
	}

	if e != entry {
		entry.mergeInto(e)
		entry.metrics.duplicateNeedsReconcile.Inc(1)
	} else {
		entry.metrics.duplicateNoReconcile.Inc(1)
	}

	e.DecrementReaderWriterCount()
}

// NeedsIndexGarbageCollected checks if the entry is eligible to be garbage collected
// from the index. Otherwise returns false.
func (entry *Entry) NeedsIndexGarbageCollected() bool {
	// This is a cheaper check that loading the entry from the shard again
	// which makes it cheaper to run frequently.
	// It may not be as accurate, but it's fine for an approximation since
	// only a single series in a segment needs to return true to trigger an
	// index segment to be garbage collected.
	if entry.insertTime.Load() == 0 {
		return false // Not inserted, does not need garbage collection.
	}
	// Check that a write is not potentially pending and the series is empty.
	return entry.ReaderWriterCount() == 0 && entry.Series.IsEmpty()
}

// SetInsertTime marks the entry as having been inserted into the shard at a given timestamp.
func (entry *Entry) SetInsertTime(t time.Time) {
	entry.insertTime.Store(t.UnixNano())
}

// Write writes a new value.
func (entry *Entry) Write(
	ctx context.Context,
	timestamp xtime.UnixNano,
	value float64,
	unit xtime.Unit,
	annotation []byte,
	wOpts series.WriteOptions,
) (bool, series.WriteType, error) {
	if err := entry.maybeIndex(timestamp); err != nil {
		return false, 0, err
	}
	return entry.Series.Write(
		ctx,
		timestamp,
		value,
		unit,
		annotation,
		wOpts,
	)
}

// LoadBlock loads a single block into the series.
func (entry *Entry) LoadBlock(
	block block.DatabaseBlock,
	writeType series.WriteType,
) error {
	// TODO(bodu): We can remove this once we have index snapshotting as index snapshots will
	// contained snapshotted index segments that cover snapshotted data.
	if err := entry.maybeIndex(block.StartTime()); err != nil {
		return err
	}
	return entry.Series.LoadBlock(block, writeType)
}

// UniqueIndex is the unique index for the series.
func (entry *Entry) UniqueIndex() uint64 {
	return entry.Series.UniqueIndex()
}

func (entry *Entry) maybeIndex(timestamp xtime.UnixNano) error {
	idx := entry.indexWriter
	if idx == nil {
		return nil
	}
	if !entry.NeedsIndexUpdate(idx.BlockStartForWriteTime(timestamp)) {
		return nil
	}
	entry.pendingIndexBatchSizeOne[0] = writes.PendingIndexInsert{
		Entry: index.WriteBatchEntry{
			Timestamp:     timestamp,
			OnIndexSeries: entry,
			EnqueuedAt:    entry.nowFn(),
		},
		Document: entry.Series.Metadata(),
	}
	entry.OnIndexPrepare(idx.BlockStartForWriteTime(timestamp))
	return idx.WritePending(entry.pendingIndexBatchSizeOne)
}

// SeriesRef returns the series read write ref.
func (entry *Entry) SeriesRef() (bootstrap.SeriesRef, error) {
	return entry, nil
}

// ReleaseRef must be called after using the series ref
// to release the reference count to the series so it can
// be expired by the owning shard eventually.
func (entry *Entry) ReleaseRef() {
	entry.DecrementReaderWriterCount()
}

// entryIndexState is used to capture the state of indexing for a single shard
// entry. It's used to prevent redundant indexing operations.
// NB(prateek): We need this amount of state because in the worst case, as we can have 3 active blocks being
// written to. Albeit that's an edge case due to bad configuration. Even outside of that, 2 blocks can
// be written to due to delayed, out of order writes. Consider an index block size of 2h, and buffer
// past of 10m. Say a write comes in at 2.05p (wallclock) for 2.05p (timestamp in the write), we'd index
// the entry, and update the entry to have a success for 4p. Now imagine another write
// comes in at 2.06p (wallclock) for 1.57p (timestamp in the write). We need to differentiate that we don't
// have a write for the 12-2p block from the 2-4p block, or we'd drop the late write.
type entryIndexState struct {
	sync.RWMutex
	states                   doc.EntryIndexBlockStates
	minIndexedT, maxIndexedT xtime.UnixNano
}

func newEntryIndexState() entryIndexState {
	return entryIndexState{
		states: make(doc.EntryIndexBlockStates, 4),
	}
}

func (s *entryIndexState) indexedRangeWithRLock() (xtime.UnixNano, xtime.UnixNano) {
	return s.minIndexedT, s.maxIndexedT
}

func (s *entryIndexState) indexedWithRLock(t xtime.UnixNano) bool {
	v, ok := s.states[t]
	if ok {
		return v.Success
	}
	return false
}

func (s *entryIndexState) indexedOrAttemptedWithRLock(t xtime.UnixNano) bool {
	v, ok := s.states[t]
	if ok {
		return v.Success || v.Attempt
	}
	return false
}

func (s *entryIndexState) setSuccessWithWLock(t xtime.UnixNano) {
	if s.indexedWithRLock(t) {
		return
	}

	// NB(r): If not inserted state yet that means we need to make an insertion,
	// this will happen if synchronously indexing and we haven't called
	// NeedIndexUpdate before we indexed the series.
	s.states[t] = doc.EntryIndexBlockState{
		Success: true,
	}

	if t > s.maxIndexedT {
		s.maxIndexedT = t
	}
	if t < s.minIndexedT || s.minIndexedT == 0 {
		s.minIndexedT = t
	}
}

func (s *entryIndexState) setAttemptWithWLock(t xtime.UnixNano, attempt bool) {
	v, ok := s.states[t]
	if ok {
		if v.Success {
			return // Attempt is not relevant if success.
		}
		v.Attempt = attempt
		s.states[t] = v
		return
	}

	s.states[t] = doc.EntryIndexBlockState{
		Attempt: attempt,
	}
}
