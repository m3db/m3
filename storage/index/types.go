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
	// provided value for `indexEntryExpiry` describes the TTL for the indexed
	// entry.
	OnIndexSuccess(indexEntryExpiry time.Time)

	// OnIndexFinalize is executed when the index no longer holds any references
	// to the provided resources. It can be used to cleanup any resources held
	// during the course of indexing.
	OnIndexFinalize()
}

// Block represents a collection of segments. Each `Block` is a complete reverse
// index for a period of time defined by [StartTime, EndTime).
type Block interface {
	// StartTime returns the start time of the period this Block indexes.
	StartTime() time.Time

	// EndTime returns the end time of the period this Block indexes.
	EndTime() time.Time

	// WriteBatch writes a batch of provided entries.
	WriteBatch([]WriteBatchEntry) (WriteBatchResult, error)

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

// WriteBatchEntry captures a document to index, and the lifecycle hooks to call thereafter.
type WriteBatchEntry struct {
	BlockStart    xtime.UnixNano
	Document      doc.Document
	OnIndexSeries OnIndexSeries
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

// WriteBatchEntryByBlockStart implements sort.Interface for WriteBatchEntry slices
// based on the BlockStart field.
type WriteBatchEntryByBlockStart []WriteBatchEntry

func (w WriteBatchEntryByBlockStart) Len() int           { return len(w) }
func (w WriteBatchEntryByBlockStart) Swap(i, j int)      { w[i], w[j] = w[j], w[i] }
func (w WriteBatchEntryByBlockStart) Less(i, j int) bool { return w[i].BlockStart < w[j].BlockStart }

// ForEachBlockStartFn is lambda to iterate over WriteBatchEntry(s) a single blockStart at a time.
type ForEachBlockStartFn func(blockStart xtime.UnixNano, writes []WriteBatchEntry)

// ForEachBlockStart iterates over the provided WriteBatchEntryByBlockStart, and calls `fn` on each
// group of elements with the same blockStart.
func (w WriteBatchEntryByBlockStart) ForEachBlockStart(fn ForEachBlockStartFn) {
	var (
		startIdx  = 0
		lastNanos xtime.UnixNano
	)
	for i := 0; i < len(w); i++ {
		elem := w[i]
		if elem.BlockStart != lastNanos {
			lastNanos = elem.BlockStart
			// We only want to call the the ForEachBlockStartFn once we have calculated the entire group,
			// i.e. once we have gone past the last element for a given blockStart, but the first element
			// in the slice is a special case because we are always starting a new group at that point.
			if i == 0 {
				continue
			}
			fn(w[startIdx].BlockStart, w[startIdx:i])
			startIdx = i
		}
	}
	// spill over
	if startIdx < len(w) {
		fn(w[startIdx].BlockStart, w[startIdx:])
	}
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
