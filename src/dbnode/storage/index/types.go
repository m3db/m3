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

	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/index/compaction"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	"github.com/m3db/m3/src/m3ninx/index/segment/mem"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	xsync "github.com/m3db/m3x/sync"
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

	// AddResults adds bootstrap results to the block, if c.
	AddResults(results result.IndexBlock) error

	// Tick does internal house keeping operations.
	Tick(c context.Cancellable, tickStart time.Time) (BlockTickResult, error)

	// Stats returns statistics about the block.
	Stats() BlockStats

	// Seal prevents the block from taking any more writes, but, it still permits
	// addition of segments via Bootstrap().
	Seal() error

	// IsSealed returns whether this block was sealed.
	IsSealed() bool

	// NeedsFlush returns whether this block has any segments backed by memory which
	// need to be evicted.
	NeedsFlush() bool

	// EvictActiveSegments closes any mem backed segments (active/bootstrapped mem segments).
	// This is only valid to be called once the block is sealed.
	// It is expected that results have been added to the block that covers any
	// data the segments should have held at this time.
	EvictActiveSegments() (EvictActiveSegmentResults, error)

	// Close will release any held resources and close the Block.
	Close() error
}

// EvictActiveSegmentResults returns statistics about the EvictActiveSegments execution.
type EvictActiveSegmentResults struct {
	NumSegments int64
	NumDocs     int64
}

// Add adds the provided results to the receiver.
func (e *EvictActiveSegmentResults) Add(o EvictActiveSegmentResults) {
	e.NumDocs += o.NumDocs
	e.NumSegments += o.NumSegments
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

// BlockStats returns statistics about index block.
type BlockStats struct {
	Active ActiveSegmentsStats
	Shard  ShardRangeSegmentsStats
}

// Add adds the provided results to the receiver.
func (s *BlockStats) Add(o BlockStats) {
	s.Active.NumDocs += o.Active.NumDocs
	s.Active.NumSegments += o.Active.NumSegments
	s.Active.NumMutableSegments += o.Active.NumMutableSegments
	s.Active.NumFSTSegments += o.Active.NumFSTSegments
	s.Active.NumActiveCompactions += o.Active.NumActiveCompactions
	s.Active.NumSegmentsCompacting += o.Active.NumSegmentsCompacting
	s.Shard.NumDocs += o.Shard.NumDocs
	s.Shard.NumSegments += o.Shard.NumSegments
}

// ActiveSegmentsStats returns stats about active segments.
type ActiveSegmentsStats struct {
	NumDocs               int64
	NumSegments           int64
	NumMutableSegments    int64
	NumFSTSegments        int64
	NumActiveCompactions  int64
	NumSegmentsCompacting int64
}

// ShardRangeSegmentsStats returns stats about shard range segments.
type ShardRangeSegmentsStats struct {
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
// The underlying batch returned is simply the current batch but with updated
// subslices to the relevant entries and documents that are restored at the
// end of `fn` being applied.
// NOTE: This means `fn` cannot perform any asynchronous work that uses the
// arguments passed to it as the args will be invalid at the synchronous
// execution of `fn`.
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

	// We can unconditionally spill over here since we haven't hit any marked
	// done entries yet and thanks to sort order there weren't any, therefore
	// we can execute all the remaining entries we had.
	if startIdx < len(allEntries) {
		b.entries = allEntries[startIdx:]
		b.docs = allDocs[startIdx:]
		fn(lastBlockStart.ToTime(), b)
	}
}

func (b *WriteBatch) numPending() int {
	numUnmarked := 0
	for i := range b.entries {
		if b.entries[i].OnIndexSeries == nil {
			break
		}
		numUnmarked++
	}
	return numUnmarked
}

// PendingDocs returns all the docs in this batch that are unmarked.
func (b *WriteBatch) PendingDocs() []doc.Document {
	b.SortByUnmarkedAndIndexBlockStart() // Ensure sorted by unmarked first
	return b.docs[:b.numPending()]
}

// PendingEntries returns all the entries in this batch that are unmarked.
func (b *WriteBatch) PendingEntries() WriteBatchEntries {
	b.SortByUnmarkedAndIndexBlockStart() // Ensure sorted by unmarked first
	return b.entries[:b.numPending()]
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
	sort.Stable(b)
}

// SortByEnqueued sorts the entries and documents back to the sort order they
// were enqueued as.
func (b *WriteBatch) SortByEnqueued() {
	b.sortBy = writeBatchSortByEnqueued
	sort.Stable(b)
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
		b.MarkUnmarkedEntryError(err, idx)
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
	// EnqueuedAt is the timestamp that this entry was enqueued for indexing
	// so that we can calculate the latency it takes to index the entry
	EnqueuedAt time.Time
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

// WriteBatchEntries is a slice of WriteBatchEntry elements.
type WriteBatchEntries []WriteBatchEntry

// EarliestEnqueuedAt returns the minimum value for EnqueuedAt in
// the elements provided.
func (w WriteBatchEntries) EarliestEnqueuedAt() (time.Time, bool) {
	if len(w) == 0 {
		return time.Time{}, false
	}
	min := w[0].EnqueuedAt
	for i := 1; i < len(w); i++ {
		if w[i].EnqueuedAt.Before(min) {
			min = w[i].EnqueuedAt
		}
	}
	return min, true
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

	// SetFSTSegmentOptions sets the fst segment options.
	SetFSTSegmentOptions(value fst.Options) Options

	// FSTSegmentOptions returns the fst segment options.
	FSTSegmentOptions() fst.Options

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

	// SetCompactionOptions sets the compaction options.
	SetCompactionOptions(value CompactionOptions) Options

	// CompactionOptions returns the compaction options.
	CompactionOptions() CompactionOptions
}

// CompactionOptions are a set of knobs to control compaction behaviour.
type CompactionOptions interface {
	// SetPlannerOptions sets the compaction.PlannerOptions.
	SetPlannerOptions(value compaction.PlannerOptions) CompactionOptions

	// PlannerOptions returns the compaction.PlannerOptions.
	PlannerOptions() compaction.PlannerOptions

	// SetWorkerPool sets the compaction worker pool.
	SetWorkerPool(value xsync.WorkerPool) CompactionOptions

	// WorkerPool returns the compaction worker pool.
	WorkerPool() xsync.WorkerPool
}
