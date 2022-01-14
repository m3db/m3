// Copyright (c) 2017 Uber Technologies, Inc.
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

package doc

import (
	"github.com/m3db/m3/src/x/resource"
	xtime "github.com/m3db/m3/src/x/time"
)

// MetadataIterator provides an iterator over a collection of document metadata. It is NOT
// safe for multiple goroutines to invoke methods on an MetadataIterator simultaneously.
type MetadataIterator interface {
	// Next returns a bool indicating if the iterator has any more metadata
	// to return.
	Next() bool

	// Current returns the current metadata. It is only safe to call Current immediately
	// after a call to Next confirms there are more elements remaining. The Metadata
	// returned from Current is only valid until the following call to Next(). Callers
	// should copy the Metadata if they need it live longer.
	Current() Metadata

	// Err returns any errors encountered during iteration.
	Err() error

	// Close releases any internal resources used by the iterator.
	Close() error
}

// Iterator provides an iterator over a collection of documents. It is NOT
// safe for multiple goroutines to invoke methods on an Iterator simultaneously.
type Iterator interface {

	// Next returns a bool indicating if the iterator has any more documents
	// to return.
	Next() bool

	// Current returns the current document. It is only safe to call Current immediately
	// after a call to Next confirms there are more elements remaining. The Document
	// returned from Current is only valid until the following call to Next(). Callers
	// should copy the Document if they need it live longer.
	Current() Document

	// Err returns any errors encountered during iteration.
	Err() error

	// Close releases any internal resources used by the iterator.
	Close() error
}

// QueryDocIterator is an Iterator for all documents returned for a query. See Iterator for more details.
type QueryDocIterator interface {
	Iterator

	// Done returns true if iterator is done and Next will return false on the next call. On the first call this will
	// always return false and Next may still return false for an empty iterator. Callers still need to check for an
	// Err after Done returns true.
	// This is used by the index query path to check if there are more docs to process before waiting for an index
	// worker.
	Done() bool
}

// OnIndexSeries provides a set of callback hooks to allow the reverse index
// to do lifecycle management of any resources retained during indexing.
type OnIndexSeries interface {
	// StringID returns the index series ID, as a string.
	StringID() string

	// OnIndexSuccess is executed when an entry is successfully indexed. The
	// provided value for `blockStart` is the blockStart for which the write
	// was indexed.
	OnIndexSuccess(blockStart xtime.UnixNano)

	// OnIndexFinalize is executed when the index no longer holds any references
	// to the provided resources. It can be used to cleanup any resources held
	// during the course of indexing. `blockStart` is the startTime of the index
	// block for which the write was attempted.
	OnIndexFinalize(blockStart xtime.UnixNano)

	// OnIndexPrepare prepares the Entry to be handed off to the indexing sub-system.
	// NB(prateek): we retain the ref count on the entry while the indexing is pending,
	// the callback executed on the entry once the indexing is completed releases this
	// reference.
	OnIndexPrepare(blockStart xtime.UnixNano)

	// NeedsIndexUpdate returns a bool to indicate if the Entry needs to be indexed
	// for the provided blockStart. It only allows a single index attempt at a time
	// for a single entry.
	// NB(prateek): NeedsIndexUpdate is a CAS, i.e. when this method returns true, it
	// also sets state on the entry to indicate that a write for the given blockStart
	// is going to be sent to the index, and other go routines should not attempt the
	// same write. Callers are expected to ensure they follow this guideline.
	// Further, every call to NeedsIndexUpdate which returns true needs to have a corresponding
	// OnIndexFinalze() call. This is required for correct lifecycle maintenance.
	NeedsIndexUpdate(indexBlockStartForWrite xtime.UnixNano) bool

	// IfAlreadyIndexedMarkIndexSuccessAndFinalize checks if the blockStart has been indexed.
	// If indexed, it will be marked as such and finalized, and then return true. Otherwise false.
	IfAlreadyIndexedMarkIndexSuccessAndFinalize(
		blockStart xtime.UnixNano,
	) bool

	// TryMarkIndexGarbageCollected checks if the entry is eligible to be garbage collected
	// from the index. If so, it marks the entry as GCed and returns true. Otherwise returns false.
	TryMarkIndexGarbageCollected() bool

	// NeedsIndexGarbageCollected returns if the entry is eligible to be garbage collected
	// from the index.
	NeedsIndexGarbageCollected() bool

	// IndexedForBlockStart returns true if the blockStart has been indexed.
	IndexedForBlockStart(blockStart xtime.UnixNano) bool

	// IndexedRange returns minimum and maximum blockStart values covered by index entry.
	// The range is inclusive. Note that there may be uncovered gaps within the range.
	// Returns (0, 0) for an empty range.
	IndexedRange() (xtime.UnixNano, xtime.UnixNano)

	// ReconciledOnIndexSeries attempts to retrieve the most recent index entry from the
	// shard if the entry this method was called on was never inserted there. If there
	// is an error during retrieval, simply returns the current entry. Additionally,
	// returns a cleanup function to run once finished using the reconciled entry and
	// a boolean value indicating whether the result came from reconciliation or not.
	// Cleanup function must be called once done with the reconciled entry so that
	// reader and writer counts are accurately updated.
	ReconciledOnIndexSeries() (OnIndexSeries, resource.SimpleCloser, bool)

	// MergeEntryIndexBlockStates merges the given states into the current
	// indexed entry.
	MergeEntryIndexBlockStates(states EntryIndexBlockStates)

	// TryReconcileDuplicates attempts to reconcile the index states of this entry.
	TryReconcileDuplicates()
}

// EntryIndexBlockStates captures the indexing for a single shard entry, across
// all block starts.
type EntryIndexBlockStates map[xtime.UnixNano]EntryIndexBlockState

// EntryIndexBlockState is used to capture the state of indexing for a single shard
// entry for a given index block start. It's used to prevent attempts at double indexing
// for the same block start.
type EntryIndexBlockState struct {
	// Attempt indicates that indexing has been attempted.
	Attempt bool
	// Success indicates that indexing has succeeded.
	Success bool
}
