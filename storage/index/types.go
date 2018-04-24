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
	"github.com/m3db/m3db/x/xpool"
	"github.com/m3db/m3ninx/doc"
	"github.com/m3db/m3ninx/idx"
	"github.com/m3db/m3ninx/index/segment/mem"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
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
	Iterator   Iterator
	Exhaustive bool
}

// Iterator iterates over a collection of IDs with associated
// tags and namespace.
type Iterator interface {
	// Next returns whether there are more items in the collection
	Next() bool

	// Current returns the ID, Tags and Namespace for a single timeseries.
	// These remain valid until Next() is called again.
	Current() (namespaceID ident.ID, seriesID ident.ID, tags ident.TagIterator)

	// Err returns any error encountered
	Err() error
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

	// SetCheckedBytesWrapperPool sets the checked bytes wrapper pool.
	SetCheckedBytesWrapperPool(value xpool.CheckedBytesWrapperPool) Options

	// CheckedBytesWrapperPool returns the checked bytes wrapper pool.
	CheckedBytesWrapperPool() xpool.CheckedBytesWrapperPool
}
