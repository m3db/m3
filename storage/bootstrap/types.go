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

	"github.com/m3db/m3db/instrument"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/block"
	xtime "github.com/m3db/m3x/time"
)

// NewBootstrapFn creates a new bootstrap processor.
type NewBootstrapFn func() Bootstrap

// ShardResult returns the bootstrap result for a shard.
type ShardResult interface {

	// IsEmpty returns whether the result is empty.
	IsEmpty() bool

	// AddBlock adds a data block.
	AddBlock(id string, block block.DatabaseBlock)

	// AddSeries adds a single series of blocks.
	AddSeries(id string, rawSeries block.DatabaseSeriesBlocks)

	// AddResult adds a shard result.
	AddResult(other ShardResult)

	// RemoveSeries removes a single series of blocks.
	RemoveSeries(id string)

	// GetAllSeries returns all series of blocks.
	GetAllSeries() map[string]block.DatabaseSeriesBlocks

	// Close closes a shard result.
	Close()
}

// Bootstrap represents the bootstrap process.
type Bootstrap interface {
	// Run runs the bootstrap process, returning the bootstrap result and any error encountered.
	Run(writeStart time.Time, shard uint32) (ShardResult, error)
}

// Bootstrapper is the interface for different bootstrapping mechanisms.
type Bootstrapper interface {
	// Bootstrap performs bootstrapping for the given time ranges, returning the bootstrapped
	// series data, the time ranges it's unable to fulfill, and any critical errors during bootstrapping.
	Bootstrap(shard uint32, timeRanges xtime.Ranges) (ShardResult, xtime.Ranges)
}

// Source is the data source for bootstrapping a node.
type Source interface {
	// GetAvailability returns what time ranges are available for a given shard.
	GetAvailability(shard uint32, targetRanges xtime.Ranges) xtime.Ranges

	// ReadData returns raw series for a given shard within certain time ranges,
	// the time ranges it's unable to fulfill, and any critical errors during bootstrapping.
	ReadData(shard uint32, tr xtime.Ranges) (ShardResult, xtime.Ranges)
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

	// ThrottlePeriod sets how long we wait till the next iteration of bootstrap starts by default
	ThrottlePeriod(value time.Duration) Options

	// GetThrottlePeriod returns how long we wait till the next iteration of bootstrap starts by default
	GetThrottlePeriod() time.Duration

	// MaxRetries is the maximum number of bootstrap retries
	MaxRetries(value int) Options

	// GetMaxRetries returns the maximum number of bootstrap retries
	GetMaxRetries() int
}
