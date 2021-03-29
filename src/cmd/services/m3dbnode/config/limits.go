// Copyright (c) 2019 Uber Technologies, Inc.
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

package config

import "time"

// LimitsConfiguration contains configuration for configurable limits that can be applied to M3DB.
type LimitsConfiguration struct {
	// MaxRecentlyQueriedSeriesDiskBytesRead sets the upper limit on time series bytes
	// read from disk within a given lookback period. Queries which are issued while this
	// max is surpassed encounter an error.
	MaxRecentlyQueriedSeriesDiskBytesRead *MaxRecentQueryResourceLimitConfiguration `yaml:"maxRecentlyQueriedSeriesDiskBytesRead"`

	// MaxRecentlyQueriedSeriesDiskRead sets the upper limit on time series read from disk within a given lookback
	// period. Queries which are issued while this max is surpassed encounter an error.
	// This is the number of time series, which is different from the number of bytes controlled by
	// MaxRecentlyQueriedSeriesDiskBytesRead.
	MaxRecentlyQueriedSeriesDiskRead *MaxRecentQueryResourceLimitConfiguration `yaml:"maxRecentlyQueriedSeriesDiskRead"`

	// MaxRecentlyQueriedSeriesBlocks sets the upper limit on time series blocks
	// count within a given lookback period. Queries which are issued while this
	// max is surpassed encounter an error.
	MaxRecentlyQueriedSeriesBlocks *MaxRecentQueryResourceLimitConfiguration `yaml:"maxRecentlyQueriedSeriesBlocks"`

	// MaxOutstandingWriteRequests controls the maximum number of outstanding write requests
	// that the server will allow before it begins rejecting requests. Note that this value
	// is independent of the number of values that are being written (due to variable batch
	// size from the client) but is still very useful for enforcing backpressure due to the fact
	// that all writes within a single RPC are single-threaded.
	MaxOutstandingWriteRequests int `yaml:"maxOutstandingWriteRequests" validate:"min=0"`

	// MaxOutstandingReadRequests controls the maximum number of outstanding read requests that
	// the server will allow before it begins rejecting requests. Just like MaxOutstandingWriteRequests
	// this value is independent of the number of time series being read.
	MaxOutstandingReadRequests int `yaml:"maxOutstandingReadRequests" validate:"min=0"`

	// MaxOutstandingRepairedBytes controls the maximum number of bytes that can be loaded into memory
	// as part of the repair process. For example if the value was set to 2^31 then up to 2GiB of
	// repaired data could be "outstanding" in memory at one time. Once that limit was hit, the repair
	// process would pause until some of the repaired bytes had been persisted to disk (and subsequently
	// evicted from memory) at which point it would resume.
	MaxOutstandingRepairedBytes int64 `yaml:"maxOutstandingRepairedBytes" validate:"min=0"`

	// MaxEncodersPerBlock is the maximum number of encoders permitted in a block.
	// When there are too many encoders, merging them (during a tick) puts a high
	// load on the CPU, which can prevent other DB operations.
	// A setting of 0 means there is no maximum.
	MaxEncodersPerBlock int `yaml:"maxEncodersPerBlock" validate:"min=0"`

	// Write new series limit per second to limit overwhelming during new ID bursts.
	WriteNewSeriesPerSecond int `yaml:"writeNewSeriesPerSecond" validate:"min=0"`
}

// MaxRecentQueryResourceLimitConfiguration sets an upper limit on resources consumed by all queries
// globally within a dbnode per some lookback period of time. Once exceeded, queries within that period
// of time will be abandoned.
type MaxRecentQueryResourceLimitConfiguration struct {
	// Value sets the max value for the resource limit.
	Value int64 `yaml:"value" validate:"min=0"`
	// Lookback is the period in which a given resource limit is enforced.
	Lookback time.Duration `yaml:"lookback" validate:"min=0"`
}
