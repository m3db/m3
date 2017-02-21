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

package persist

import (
	"time"

	"github.com/m3db/m3db/ratelimit"
	"github.com/m3db/m3db/ts"
)

// Fn is a function that persists a m3db segment for a given ID.
type Fn func(id ts.ID, segment ts.Segment, checksum uint32) error

// Closer is a function that performs cleanup after persisting the data
// blocks for a (shard, blockStart) combination.
type Closer func() error

// PreparedPersist is an object that wraps holds a persist function and a closer.
type PreparedPersist struct {
	Persist Fn
	Close   Closer
}

// Manager manages the internals of persisting data onto storage layer.
type Manager interface {
	// StartFlush begins a flush for a set of shards.
	StartFlush() (Flush, error)

	// SetRateLimitOptions sets the rate limit options.
	SetRateLimitOptions(value ratelimit.Options)

	// RateLimitOptions returns the rate limit options.
	RateLimitOptions() ratelimit.Options
}

// Flush is a persist flush cycle, each shard and block start permutation needs
// to explicility be prepared.
type Flush interface {
	// Prepare prepares writing data for a given (shard, blockStart) combination,
	// returning a PreparedPersist object and any error encountered during
	// preparation if any.
	Prepare(namespace ts.ID, shard uint32, blockStart time.Time) (PreparedPersist, error)

	// Done marks the flush as complete.
	Done() error
}
