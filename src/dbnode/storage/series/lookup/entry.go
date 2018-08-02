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

package lookup

import (
	"sync"
	"sync/atomic"

	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/series"
	xtime "github.com/m3db/m3x/time"
)

const (
	maxUint64 = ^uint64(0)
	maxInt64  = int64(maxUint64 >> 1)
)

// Entry is the entry in the shard ident.ID -> series map. It has additional
// members to track lifecycle and minimize indexing overhead.
// NB: users are expected to use `NewEntry` to construct these objects.
type Entry struct {
	Series         series.DatabaseSeries
	Index          uint64
	curReadWriters int32
	reverseIndex   entryIndexState
}

// ensure Entry satisfies the `index.OnIndexSeries` interface.
var _ index.OnIndexSeries = &Entry{}

// NewEntry returns a new Entry.
func NewEntry(series series.DatabaseSeries, index uint64) *Entry {
	entry := &Entry{
		Series: series,
		Index:  index,
	}
	entry.reverseIndex.states = entry.reverseIndex._staticAloc[:0]
	return entry
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

// IndexedForBlockStart returns a bool to indicate if the Entry has been successfully
// indexed for the given index blockstart.
func (entry *Entry) IndexedForBlockStart(indexBlockStart xtime.UnixNano) bool {
	entry.reverseIndex.RLock()
	isIndexed := entry.reverseIndex.indexedWithRLock(indexBlockStart)
	entry.reverseIndex.RUnlock()
	return isIndexed
}

// NeedsIndexUpdate returns a bool to indicate if the Entry needs to be indexed
// for the provided blockStart. It only allows a single index attempt at a time
// for a single entry.
// NB(prateek): NeedsIndexUpdate is a CAS, i.e. when this method returns true, it
// also sets state on the entry to indicate that a write for the given blockStart
// is going to be sent to the index, and other go routines should not attempt the
// same write. Callers are expected to ensure they follow this guideline.
// Further, every call to NeedsIndexUpdate which returns true needs to have a corresponding
// OnIndexFinalze() call. This is required for correct lifecycle maintenance.
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
func (entry *Entry) OnIndexPrepare() {
	entry.IncrementReaderWriterCount()
}

// OnIndexSuccess marks the given block start as successfully indexed.
func (entry *Entry) OnIndexSuccess(blockStartNanos xtime.UnixNano) {
	entry.reverseIndex.Lock()
	entry.reverseIndex.setSuccessWithWLock(blockStartNanos)
	entry.reverseIndex.Unlock()
}

// OnIndexFinalize marks any attempt for the given block start is finished.
func (entry *Entry) OnIndexFinalize(blockStartNanos xtime.UnixNano) {
	entry.reverseIndex.Lock()
	entry.reverseIndex.setAttemptWithWLock(blockStartNanos, false)
	entry.reverseIndex.Unlock()
	// indicate the index has released held reference for provided write
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
	states []entryIndexBlockState

	// NB(prateek): we alloc an array (not slice) of size 3, as that is
	// the most we will need (only 3 blocks should ever be written to
	// simultaneously in the worst case). We allocate it like we're doing
	// to ensure it's along side the rest of the struct in memory. But
	// we only access it through `states`, to ensure that it can be
	// grown/shrunk as needed. Do not acccess it directly.
	_staticAloc [3]entryIndexBlockState
}

// entryIndexBlockState is used to capture the state of indexing for a single shard
// entry for a given index block start. It's used to prevent attempts at double indexing
// for the same block start.
type entryIndexBlockState struct {
	blockStart xtime.UnixNano
	attempt    bool
	success    bool
}

func (s *entryIndexState) indexedWithRLock(t xtime.UnixNano) bool {
	for i := range s.states {
		if s.states[i].blockStart.Equal(t) {
			return s.states[i].success
		}
	}
	return false
}

func (s *entryIndexState) indexedOrAttemptedWithRLock(t xtime.UnixNano) bool {
	for i := range s.states {
		if s.states[i].blockStart.Equal(t) {
			return s.states[i].success || s.states[i].attempt
		}
	}
	return false
}

func (s *entryIndexState) setSuccessWithWLock(t xtime.UnixNano) {
	for i := range s.states {
		if s.states[i].blockStart.Equal(t) {
			s.states[i].success = true
			return
		}
	}

	// NB(r): If not inserted state yet that means we need to make an insertion,
	// this will happen if synchronously indexing and we haven't called
	// NeedIndexUpdate before we indexed the series.
	s.insertBlockState(entryIndexBlockState{
		blockStart: t,
		success:    true,
	})
}

func (s *entryIndexState) setAttemptWithWLock(t xtime.UnixNano, attempt bool) {
	// first check if we have the block start in the slice already
	for i := range s.states {
		if s.states[i].blockStart.Equal(t) {
			s.states[i].attempt = attempt
			return
		}
	}

	s.insertBlockState(entryIndexBlockState{
		blockStart: t,
		attempt:    attempt,
	})
}

func (s *entryIndexState) insertBlockState(newState entryIndexBlockState) {
	// i.e. we don't have the block start in the slice
	// if we have less than 3 elements, we can just insert an element to the slice.
	if len(s.states) < 3 {
		s.states = append(s.states, newState)
		return
	}

	// i.e. len(s.states) == 3, in this case, we update the entry with the lowest block start
	// as we know only 3 writes can be active at any point. Think of this as a lazy compaction.
	var (
		minIdx        = -1
		minBlockStart = xtime.UnixNano(maxInt64)
	)
	for idx, blockState := range s.states {
		if blockState.blockStart < minBlockStart {
			minIdx = idx
			minBlockStart = blockState.blockStart
		}
	}

	s.states[minIdx] = newState
}
