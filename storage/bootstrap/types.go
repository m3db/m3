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

package bootstrap

import (
	"bytes"
	"fmt"
	"time"

	"github.com/m3db/m3db/instrument"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/block"
	xtime "github.com/m3db/m3x/time"
)

// NewBootstrapFn creates a new bootstrap processor.
type NewBootstrapFn func() Bootstrap

// Result is the result of a bootstrap.
type Result interface {
	// ShardResults is the results of all shards for the bootstrap.
	ShardResults() ShardResults

	// Unfulfilled is the unfulfilled time ranges for the bootstrap.
	Unfulfilled() ShardTimeRanges

	// AddShardResult adds a shard result with any unfulfilled time ranges.
	AddShardResult(shard uint32, result ShardResult, unfulfilled xtime.Ranges)

	// SetUnfulfilled sets the current unfulfilled shard time ranges.
	SetUnfulfilled(unfulfilled ShardTimeRanges)

	// AddResult adds a result.
	AddResult(other Result)
}

// ShardResult returns the bootstrap result for a shard.
type ShardResult interface {
	// IsEmpty returns whether the result is empty.
	IsEmpty() bool

	// AllSeries returns all series of blocks.
	AllSeries() map[string]block.DatabaseSeriesBlocks

	// AddBlock adds a data block.
	AddBlock(id string, block block.DatabaseBlock)

	// AddSeries adds a single series of blocks.
	AddSeries(id string, rawSeries block.DatabaseSeriesBlocks)

	// AddResult adds a shard result.
	AddResult(other ShardResult)

	// RemoveSeries removes a single series of blocks.
	RemoveSeries(id string)

	// Close closes a shard result.
	Close()
}

// ShardResults is a map of shards to shard results.
type ShardResults map[uint32]ShardResult

// AddResults adds other shard results to the current shard results.
func (r ShardResults) AddResults(other ShardResults) {
	for shard, result := range other {
		if existing, ok := r[shard]; ok {
			existing.AddResult(result)
		} else {
			r[shard] = result
		}
	}
}

// ShardTimeRanges is a map of shards to time ranges.
type ShardTimeRanges map[uint32]xtime.Ranges

// IsEmpty returns whether the shard time ranges is empty or not.
func (r ShardTimeRanges) IsEmpty() bool {
	for _, ranges := range r {
		if !xtime.IsEmpty(ranges) {
			return false
		}
	}
	return true
}

// Equal returns whether two shard time ranges are equal.
func (r ShardTimeRanges) Equal(other ShardTimeRanges) bool {
	if len(r) != len(other) {
		return false
	}
	for shard, ranges := range r {
		otherRanges, ok := other[shard]
		if !ok {
			return false
		}
		matched := 0
		it := ranges.Iter()
		otherIt := otherRanges.Iter()
		if it.Next() && otherIt.Next() {
			value := it.Value()
			otherValue := otherIt.Value()
			if !value.Start.Equal(otherValue.Start) ||
				!value.End.Equal(otherValue.End) {
				return false
			}
			matched++
		}
		if matched != ranges.Len() {
			return false
		}
	}
	return true
}

// Copy will return a copy of the current shard time ranges.
func (r ShardTimeRanges) Copy() ShardTimeRanges {
	result := make(map[uint32]xtime.Ranges, len(r))
	for shard, ranges := range r {
		result[shard] = xtime.NewRanges().AddRanges(ranges)
	}
	return result
}

// AddRanges adds other shard time ranges to the current shard time ranges.
func (r ShardTimeRanges) AddRanges(other ShardTimeRanges) {
	for shard, ranges := range other {
		if xtime.IsEmpty(ranges) {
			continue
		}
		if existing, ok := r[shard]; ok {
			r[shard] = existing.AddRanges(ranges)
		} else {
			r[shard] = ranges
		}
	}
}

// ToUnfulfilledResult will return a result that is comprised of wholly
// unfufilled time ranges from the set of shard time ranges.
func (r ShardTimeRanges) ToUnfulfilledResult() Result {
	result := NewResult()
	for shard, ranges := range r {
		result.AddShardResult(shard, nil, ranges)
	}
	return result
}

// Subtract will subtract another range from the current range.
func (r ShardTimeRanges) Subtract(other ShardTimeRanges) {
	for shard, ranges := range r {
		otherRanges, ok := other[shard]
		if !ok {
			continue
		}

		subtractedRanges := ranges.RemoveRanges(otherRanges)
		if subtractedRanges.IsEmpty() {
			delete(r, shard)
		} else {
			r[shard] = subtractedRanges
		}
	}
}

// String returns a description of the time ranges
func (r ShardTimeRanges) String() string {
	var buf bytes.Buffer
	buf.WriteString("{")
	hasPrev := false
	for shard, ranges := range r {
		buf.WriteString(fmt.Sprintf("%d: %s", shard, ranges.String()))
		if hasPrev {
			buf.WriteString(", ")
		}
		hasPrev = true
		buf.WriteString(fmt.Sprintf("%d: %s", shard, ranges.String()))
	}
	buf.WriteString("}")
	return buf.String()
}

// SummaryString returns a summary description of the time ranges
func (r ShardTimeRanges) SummaryString() string {
	var buf bytes.Buffer
	buf.WriteString("{")
	hasPrev := false
	for shard, ranges := range r {
		if hasPrev {
			buf.WriteString(", ")
		}
		hasPrev = true

		var (
			duration time.Duration
			it       = ranges.Iter()
		)
		for it.Next() {
			curr := it.Value()
			duration += curr.End.Sub(curr.Start)
		}
		buf.WriteString(fmt.Sprintf("%d: %s", shard, duration.String()))
	}
	buf.WriteString("}")
	return buf.String()
}

// Bootstrap represents the bootstrap process.
type Bootstrap interface {
	// Run runs the bootstrap process, returning the bootstrap result and any error encountered.
	Run(writeStart time.Time, shards []uint32) (Result, error)
}

// Strategy describes a bootstrap strategy.
type Strategy int

const (
	// BootstrapSequential describes whether a bootstrap can use the sequential bootstrap strategy.
	BootstrapSequential Strategy = iota
	// BootstrapParallel describes whether a bootstrap can use the parallel bootstrap strategy.
	BootstrapParallel
)

// Bootstrapper is the interface for different bootstrapping mechanisms.
type Bootstrapper interface {
	// Can returns whether a specific bootstrapper strategy can be applied.
	Can(strategy Strategy) bool

	// Bootstrap performs bootstrapping for the given time ranges, returning the bootstrapped
	// series data and the time ranges it's unable to fulfill in parallel.
	Bootstrap(shardsTimeRanges ShardTimeRanges) (Result, error)
}

// Source represents a bootstrap source.
type Source interface {
	// Can returns whether a specific bootstrapper strategy can be applied.
	Can(strategy Strategy) bool

	// Available returns what time ranges are available for a given set of shards.
	Available(shardsTimeRanges ShardTimeRanges) ShardTimeRanges

	// Read returns raw series for a given set of shards & specified time ranges and
	// the time ranges it's unable to fulfill.
	Read(shardsTimeRanges ShardTimeRanges) (Result, error)
}

// Options represents the options for bootstrapping
type Options interface {
	// InstrumentOptions sets the instrumentation options
	InstrumentOptions(value instrument.Options) Options

	// GetInstrumentOptions returns the instrumentation options
	GetInstrumentOptions() instrument.Options

	// RetentionOptions sets the retention options
	RetentionOptions(value retention.Options) Options

	// GetRetentionOptions returns the retention options
	GetRetentionOptions() retention.Options

	// DatabaseBlockOptions sets the database block options
	DatabaseBlockOptions(value block.Options) Options

	// GetDatabaseBlockOptions returns the database block options
	GetDatabaseBlockOptions() block.Options
}
