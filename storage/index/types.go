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

package index

import (
	"fmt"
	"sort"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3ninx/doc"
	"github.com/m3db/m3ninx/idx"
	"github.com/m3db/m3ninx/index/segment"
	"github.com/m3db/m3ninx/index/segment/mem"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"
)

var (
	// ReservedFieldNameID is the field name used to index the ID in the
	// m3ninx subsytem.
	ReservedFieldNameID = doc.IDReservedFieldName
)

// InsertMode specifies whether inserts are synchronous or asynchronous.
type InsertMode byte

// nolint
const (
	InsertSync InsertMode = iota
	InsertAsync
)

// Query is a rich end user query to describe a set of constraints on required IDs.
type Query struct {
	idx.Query
}

// QueryOptions enables users to specify constraints on query execution.
type QueryOptions struct {
	StartInclusive time.Time
	EndExclusive   time.Time
	Limit          int
}

// QueryResults is the collection of results for a query.
type QueryResults struct {
	Results    Results
	Exhaustive bool
}

// Results is a collection of results for a query.
type Results interface {
	// Namespace returns the namespace associated with the result.
	Namespace() ident.ID

	// Map returns a map from seriesID -> seriesTags, comprising index results.
	Map() *ResultsMap

	// Reset resets the Results object to initial state.
	Reset(nsID ident.ID)

	// Finalize releases any resources held by the Results object,
	// including returning it to a backing pool.
	Finalize()

	// Size returns the number of IDs tracked.
	Size() int

	// Add converts the provided document to a metric and adds it to the results.
	// This method makes a copy of the bytes backing the document, so the original
	// may be modified after this function returns without affecting the results map.
	// NB: it returns a bool to indicate if the doc was added (it won't be added
	// if it already existed in the ResultsMap).
	Add(d doc.Document) (added bool, size int, err error)
}

// ResultsAllocator allocates Results types.
type ResultsAllocator func() Results

// ResultsPool allows users to pool `Results` types.
type ResultsPool interface {
	// Init initialized the results pool.
	Init(alloc ResultsAllocator)

	// Get retrieves a Results object for use.
	Get() Results

	// Put returns the provide value to the pool.
	Put(value Results)
}

// MutableSegmentAllocator allocates a new MutableSegment type.
type MutableSegmentAllocator func() (segment.MutableSegment, error)

// OnIndexSeries provides a set of callback hooks to allow the reverse index
// to do lifecycle management of any resources retained during indexing.
type OnIndexSeries interface {
	// OnIndexSuccess is executed when an entry is successfully indexed. The
	// provided value for `blockStart` is the blockStart for which the write
	// was indexed.
	OnIndexSuccess(blockStart xtime.UnixNano)

	// OnIndexFinalize is executed when the index no longer holds any references
	// to the provided resources. It can be used to cleanup any resources held
	// during the course of indexing. `blockStart` is the startTime of the index
	// block for which the write was attempted.
	OnIndexFinalize(blockStart xtime.UnixNano)
}

// Block represents a collection of segments. Each `Block` is a complete reverse
// index for a period of time defined by [StartTime, EndTime).
type Block interface {
	// StartTime returns the start time of the period this Block indexes.
	StartTime() time.Time

	// EndTime returns the end time of the period this Block indexes.
	EndTime() time.Time

	// WriteBatch writes a batch of provided entries.
	WriteBatch(inserts *WriteBatch) (WriteBatchResult, error)

	// Query resolves the given query into known IDs.
	Query(
		query Query,
		opts QueryOptions,
		results Results,
	) (exhaustive bool, err error)

	// Bootstrap bootstraps the index the provided segments.
	Bootstrap(
		segments []segment.Segment,
	) error

	// Tick does internal house keeping operations.
	Tick(c context.Cancellable) (BlockTickResult, error)

	// Seal prevents the block from taking any more writes, but, it still permits
	// addition of segments via Bootstrap().
	Seal() error

	// IsSealed returns whether this block was sealed.
	IsSealed() bool

	// Close will release any held resources and close the Block.
	Close() error
}

// WriteBatchResult returns statistics about the WriteBatch execution.
type WriteBatchResult struct {
	NumSuccess int64
	NumError   int64
}

// BlockTickResult returns statistics about tick.
type BlockTickResult struct {
	NumSegments int64
	NumDocs     int64
}

// WriteBatch is a batch type that allows for building of a slice of documents
// with metadata in a separate slice, this allows the documents slice to be
// passed to the segment to batch insert without having to copy into a buffer
// again.
type WriteBatch struct {
	opts   WriteBatchOptions
	sortBy writeBatchSortBy

	entries []WriteBatchEntry
	docs    []doc.Document
}

type writeBatchSortBy uint

const (
	writeBatchSortByUnmarkedAndBlockStart writeBatchSortBy = iota
	writeBatchSortByEnqueued
)

// WriteBatchOptions is a set of options required for a write batch.
type WriteBatchOptions struct {
	InitialCapacity int
	IndexBlockSize  time.Duration
}

// NewWriteBatch creates a new write batch.
func NewWriteBatch(opts WriteBatchOptions) *WriteBatch {
	return &WriteBatch{
		opts:    opts,
		entries: make([]WriteBatchEntry, 0, opts.InitialCapacity),
		docs:    make([]doc.Document, 0, opts.InitialCapacity),
	}
}

// Append appends an entry with accompanying document.
func (b *WriteBatch) Append(
	entry WriteBatchEntry,
	doc doc.Document,
) {
	// Set private WriteBatchEntry fields
	entry.enqueuedIdx = len(b.entries)
	entry.result = WriteBatchEntryResult{}

	// Append
	b.entries = append(b.entries, entry)
	b.docs = append(b.docs, doc)
}

// ForEachWriteBatchEntryFn allows a caller to perform an operation for each
// batch entry.
type ForEachWriteBatchEntryFn func(
	idx int,
	entry WriteBatchEntry,
	doc doc.Document,
	result WriteBatchEntryResult,
)

// ForEach allows a caller to perform an operation for each batch entry.
func (b *WriteBatch) ForEach(fn ForEachWriteBatchEntryFn) {
	for idx, entry := range b.entries {
		fn(idx, entry, b.docs[idx], entry.Result())
	}
}

// ForEachWriteBatchByBlockStartFn allows a caller to perform an operation with
// reference to a restricted set of the write batch for each unique block
// start.
type ForEachWriteBatchByBlockStartFn func(
	blockStart time.Time,
	batch *WriteBatch,
)

// ForEachUnmarkedBatchByBlockStart allows a caller to perform an operation
// with reference to a restricted set of the write batch for each unique block
// start for entries that have not been marked completed yet.
func (b *WriteBatch) ForEachUnmarkedBatchByBlockStart(
	fn ForEachWriteBatchByBlockStartFn,
) {
	// Ensure sorted correctly first
	b.SortByUnmarkedAndIndexBlockStart()

	// What we do is a little funky but least alloc intensive, essentially we mutate
	// this batch and then restore the pointers to the original docs after.
	allEntries := b.entries
	allDocs := b.docs
	defer func() {
		b.entries = allEntries
		b.docs = allDocs
	}()

	var (
		blockSize      = b.opts.IndexBlockSize
		startIdx       = 0
		lastBlockStart xtime.UnixNano
	)
	for i := range allEntries {
		if allEntries[i].OnIndexSeries == nil {
			// Hit a marked done entry
			b.entries = allEntries[startIdx:i]
			b.docs = allDocs[startIdx:i]
			if len(b.entries) != 0 {
				fn(lastBlockStart.ToTime(), b)
			}
			return
		}

		blockStart := allEntries[i].indexBlockStart(blockSize)
		if !blockStart.Equal(lastBlockStart) {
			prevLastBlockStart := lastBlockStart.ToTime()
			lastBlockStart = blockStart
			// We only want to call the the ForEachUnmarkedBatchByBlockStart once we have calculated the entire group,
			// i.e. once we have gone past the last element for a given blockStart, but the first element
			// in the slice is a special case because we are always starting a new group at that point.
			if i == 0 {
				continue
			}
			b.entries = allEntries[startIdx:i]
			b.docs = allDocs[startIdx:i]
			fn(prevLastBlockStart, b)
			startIdx = i
		}
	}

	// spill over
	if startIdx < len(allEntries) {
		b.entries = allEntries[startIdx:]
		b.docs = allDocs[startIdx:]
		fn(lastBlockStart.ToTime(), b)
	}
}

// PendingDocs returns all the docs in this batch that are unmarked.
func (b *WriteBatch) PendingDocs() []doc.Document {
	// Ensure sorted by unmarked first
	b.SortByUnmarkedAndIndexBlockStart()

	numUnmarked := 0
	for i := range b.entries {
		if b.entries[i].OnIndexSeries == nil {
			break
		}
		numUnmarked++
	}
	return b.docs[:numUnmarked]
}

// NumErrs returns the number of errors encountered by the batch.
func (b *WriteBatch) NumErrs() int {
	errs := 0
	for _, entry := range b.entries {
		if entry.result.Err != nil {
			errs++
		}
	}
	return errs
}

// Reset resets the batch for use.
func (b *WriteBatch) Reset() {
	// Memset optimizations
	var entryZeroed WriteBatchEntry
	for i := range b.entries {
		b.entries[i] = entryZeroed
	}
	b.entries = b.entries[:0]
	var docZeroed doc.Document
	for i := range b.docs {
		b.docs[i] = docZeroed
	}
	b.docs = b.docs[:0]
}

// SortByUnmarkedAndIndexBlockStart sorts the batch by unmarked first and then
// by index block start time.
func (b *WriteBatch) SortByUnmarkedAndIndexBlockStart() {
	b.sortBy = writeBatchSortByUnmarkedAndBlockStart
	sort.Sort(b)
}

// SortByEnqueued sorts the entries and documents back to the sort order they
// were enqueued as.
func (b *WriteBatch) SortByEnqueued() {
	b.sortBy = writeBatchSortByEnqueued
	sort.Sort(b)
}

// MarkUnmarkedEntriesSuccess marks all unmarked entries as success.
func (b *WriteBatch) MarkUnmarkedEntriesSuccess() {
	for idx := range b.entries {
		if b.entries[idx].OnIndexSeries != nil {
			blockStart := b.entries[idx].indexBlockStart(b.opts.IndexBlockSize)
			b.entries[idx].OnIndexSeries.OnIndexSuccess(blockStart)
			b.entries[idx].OnIndexSeries.OnIndexFinalize(blockStart)
			b.entries[idx].OnIndexSeries = nil
			b.entries[idx].result = WriteBatchEntryResult{Err: nil}
		}
	}
}

// MarkUnmarkedEntriesError marks all unmarked entries as error.
func (b *WriteBatch) MarkUnmarkedEntriesError(err error) {
	for idx := range b.entries {
		if b.entries[idx].OnIndexSeries != nil {
			blockStart := b.entries[idx].indexBlockStart(b.opts.IndexBlockSize)
			b.entries[idx].OnIndexSeries.OnIndexFinalize(blockStart)
			b.entries[idx].OnIndexSeries = nil
			b.entries[idx].result = WriteBatchEntryResult{Err: err}
		}
	}
}

// MarkUnmarkedEntryError marks an unmarked entry at index as error.
func (b *WriteBatch) MarkUnmarkedEntryError(
	err error,
	idx int,
) {
	if b.entries[idx].OnIndexSeries != nil {
		blockStart := b.entries[idx].indexBlockStart(b.opts.IndexBlockSize)
		b.entries[idx].OnIndexSeries.OnIndexFinalize(blockStart)
		b.entries[idx].OnIndexSeries = nil
		b.entries[idx].result = WriteBatchEntryResult{Err: err}
	}
}

// Ensure that WriteBatch meets the sort interface
var _ sort.Interface = (*WriteBatch)(nil)

// Len returns the length of the batch.
func (b *WriteBatch) Len() int {
	return len(b.entries)
}

// Swap will swap two entries and the corresponding docs.
func (b *WriteBatch) Swap(i, j int) {
	b.entries[i], b.entries[j] = b.entries[j], b.entries[i]
	b.docs[i], b.docs[j] = b.docs[j], b.docs[i]
}

// Less returns whether an entry appears before another depending
// on the type of sort.
func (b *WriteBatch) Less(i, j int) bool {
	if b.sortBy == writeBatchSortByEnqueued {
		return b.entries[i].enqueuedIdx < b.entries[j].enqueuedIdx
	}
	if b.sortBy != writeBatchSortByUnmarkedAndBlockStart {
		panic(fmt.Errorf("unexpected sort by: %d", b.sortBy))
	}

	if b.entries[i].OnIndexSeries != nil && b.entries[j].OnIndexSeries == nil {
		// This other entry has already been marked and this hasn't
		return true
	}
	if b.entries[i].OnIndexSeries == nil && b.entries[j].OnIndexSeries != nil {
		// This entry has already been marked and other hasn't
		return false
	}

	// They're either both unmarked or marked
	blockStartI := b.entries[i].indexBlockStart(b.opts.IndexBlockSize)
	blockStartJ := b.entries[j].indexBlockStart(b.opts.IndexBlockSize)
	return blockStartI.Before(blockStartJ)
}

// WriteBatchEntry represents the metadata accompanying the document that is
// being inserted.
type WriteBatchEntry struct {
	// Timestamp is the timestamp that this entry should be indexed for
	Timestamp time.Time
	// OnIndexSeries is a listener/callback for when this entry is marked done
	// it is set to nil when the entry is marked done
	OnIndexSeries OnIndexSeries
	// enqueuedIdx is the idx of the entry when originally enqueued by the call
	// to append on the write batch
	enqueuedIdx int
	// result is the result for this entry which is updated when marked done
	result WriteBatchEntryResult
}

// WriteBatchEntryResult represents a result.
type WriteBatchEntryResult struct {
	Err error
}

func (e WriteBatchEntry) indexBlockStart(
	indexBlockSize time.Duration,
) xtime.UnixNano {
	return xtime.ToUnixNano(e.Timestamp.Truncate(indexBlockSize))
}

// Result returns the result for this entry.
func (e WriteBatchEntry) Result() WriteBatchEntryResult {
	return e.result
}

// Options control the Indexing knobs.
type Options interface {
	// Validate validates assumptions baked into the code.
	Validate() error

	// SetIndexInsertMode sets the index insert mode (sync/async).
	SetInsertMode(value InsertMode) Options

	// IndexInsertMode returns the index's insert mode (sync/async).
	InsertMode() InsertMode

	// SetClockOptions sets the clock options.
	SetClockOptions(value clock.Options) Options

	// ClockOptions returns the clock options.
	ClockOptions() clock.Options

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetMemSegmentOptions sets the mem segment options.
	SetMemSegmentOptions(value mem.Options) Options

	// MemSegmentOptions returns the mem segment options.
	MemSegmentOptions() mem.Options

	// SetIdentifierPool sets the identifier pool.
	SetIdentifierPool(value ident.Pool) Options

	// IdentifierPool returns the identifier pool.
	IdentifierPool() ident.Pool

	// SetCheckedBytesPool sets the checked bytes pool.
	SetCheckedBytesPool(value pool.CheckedBytesPool) Options

	// CheckedBytesPool returns the checked bytes pool.
	CheckedBytesPool() pool.CheckedBytesPool

	// SetResultsPool updates the results pool.
	SetResultsPool(values ResultsPool) Options

	// ResultsPool returns the results pool.
	ResultsPool() ResultsPool
}
