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

package result

import (
	"time"

	"github.com/m3db/m3db/src/dbnode/clock"
	"github.com/m3db/m3db/src/dbnode/storage/block"
	"github.com/m3db/m3db/src/dbnode/storage/series"
	"github.com/m3db/m3db/src/m3ninx/index/segment"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	xtime "github.com/m3db/m3x/time"
)

// DataBootstrapResult is the result of a bootstrap of series data.
type DataBootstrapResult interface {
	// ShardResults is the results of all shards for the bootstrap.
	ShardResults() ShardResults

	// Unfulfilled is the unfulfilled time ranges for the bootstrap.
	Unfulfilled() ShardTimeRanges

	// Add adds a shard result with any unfulfilled time ranges.
	Add(shard uint32, result ShardResult, unfulfilled xtime.Ranges)

	// SetUnfulfilled sets the current unfulfilled shard time ranges.
	SetUnfulfilled(unfulfilled ShardTimeRanges)
}

// IndexBootstrapResult is the result of a bootstrap of series index metadata.
type IndexBootstrapResult interface {
	// Blocks returns a map of all index block results.
	IndexResults() IndexResults

	// Unfulfilled is the unfulfilled time ranges for the bootstrap.
	Unfulfilled() ShardTimeRanges

	// SetUnfulfilled sets the current unfulfilled shard time ranges.
	SetUnfulfilled(unfulfilled ShardTimeRanges)

	// Add adds an index block result.
	Add(block IndexBlock, unfulfilled ShardTimeRanges)
}

// IndexResults is a set of index blocks indexed by block start.
type IndexResults map[xtime.UnixNano]IndexBlock

// IndexBlock contains the bootstrap data structures for an index block.
type IndexBlock struct {
	blockStart time.Time
	segments   []segment.Segment
	fulfilled  ShardTimeRanges
}

// MutableSegmentAllocator allocates a new MutableSegment type when
// creating a bootstrap result to return to the index.
type MutableSegmentAllocator func() (segment.MutableSegment, error)

// ShardResult returns the bootstrap result for a shard.
type ShardResult interface {
	// IsEmpty returns whether the result is empty.
	IsEmpty() bool

	// BlockAt returns the block at a given time for a given id,
	// or nil if there is no such block.
	BlockAt(id ident.ID, t time.Time) (block.DatabaseBlock, bool)

	// AllSeries returns a map of all series with their associated blocks.
	AllSeries() *Map

	// NumSeries returns the number of distinct series'.
	NumSeries() int64

	// AddBlock adds a data block.
	AddBlock(id ident.ID, tags ident.Tags, block block.DatabaseBlock)

	// AddSeries adds a single series of blocks.
	AddSeries(id ident.ID, tags ident.Tags, rawSeries block.DatabaseSeriesBlocks)

	// AddResult adds a shard result.
	AddResult(other ShardResult)

	// RemoveBlockAt removes a data block at a given timestamp
	RemoveBlockAt(id ident.ID, t time.Time)

	// RemoveSeries removes a single series of blocks.
	RemoveSeries(id ident.ID)

	// Close closes a shard result.
	Close()
}

// DatabaseSeriesBlocks represents a series of blocks and a associated series ID.
type DatabaseSeriesBlocks struct {
	ID     ident.ID
	Tags   ident.Tags
	Blocks block.DatabaseSeriesBlocks
}

// ShardResults is a map of shards to shard results.
type ShardResults map[uint32]ShardResult

// ShardTimeRanges is a map of shards to time ranges.
type ShardTimeRanges map[uint32]xtime.Ranges

// Options represents the options for bootstrap results.
type Options interface {
	// SetClockOptions sets the clock options.
	SetClockOptions(value clock.Options) Options

	// ClockOptions returns the clock options.
	ClockOptions() clock.Options

	// SetInstrumentOptions sets the instrumentation options.
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrumentation options.
	InstrumentOptions() instrument.Options

	// SetDatabaseBlockOptions sets the database block options.
	SetDatabaseBlockOptions(value block.Options) Options

	// DatabaseBlockOptions returns the database block options.
	DatabaseBlockOptions() block.Options

	// SetNewBlocksLen sets the size of a new blocks map size.
	SetNewBlocksLen(value int) Options

	// NewBlocksLen returns the size of a new blocks map size.
	NewBlocksLen() int

	// SetSeriesCachePolicy sets the series cache policy.
	SetSeriesCachePolicy(value series.CachePolicy) Options

	// SeriesCachePolicy returns the series cache policy.
	SeriesCachePolicy() series.CachePolicy

	// SetIndexMutableSegmentAllocator sets the index mutable segment allocator.
	SetIndexMutableSegmentAllocator(value MutableSegmentAllocator) Options

	// IndexMutableSegmentAllocator returns the index mutable segment allocator.
	IndexMutableSegmentAllocator() MutableSegmentAllocator
}
