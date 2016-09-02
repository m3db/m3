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
	"time"

	"github.com/m3db/m3db/clock"
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

	// Add adds a shard result with any unfulfilled time ranges.
	Add(shard uint32, result ShardResult, unfulfilled xtime.Ranges)

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

// ShardTimeRanges is a map of shards to time ranges.
type ShardTimeRanges map[uint32]xtime.Ranges

// Bootstrap represents the bootstrap process.
type Bootstrap interface {
	// Run runs the bootstrap process, returning the bootstrap result and any error encountered.
	Run(writeStart time.Time, namespace string, shards []uint32) (Result, error)
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
	// series data and the time ranges it's unable to fulfill in parallel. A bootstrapper
	// should only return an error should it want to entirely cancel the bootstrapping of the
	// node, i.e. non-recoverable situation like not being able to read from the filesystem.
	Bootstrap(namespace string, shardsTimeRanges ShardTimeRanges) (Result, error)
}

// Source represents a bootstrap source.
type Source interface {
	// Can returns whether a specific bootstrapper strategy can be applied.
	Can(strategy Strategy) bool

	// Available returns what time ranges are available for a given set of shards.
	Available(namespace string, shardsTimeRanges ShardTimeRanges) ShardTimeRanges

	// Read returns raw series for a given set of shards & specified time ranges and
	// the time ranges it's unable to fulfill. A bootstrapper source should only return
	// an error should it want to entirely cancel the bootstrapping of the node,
	// i.e. non-recoverable situation like not being able to read from the filesystem.
	Read(namespace string, shardsTimeRanges ShardTimeRanges) (Result, error)
}

// Options represents the options for bootstrapping
type Options interface {
	// ClockOptions sets the clock options
	ClockOptions(value clock.Options) Options

	// GetClockOptions returns the clock options
	GetClockOptions() clock.Options

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
