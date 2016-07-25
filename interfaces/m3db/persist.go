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

package m3db

import "time"

// PersistenceFunc is a function that persists a m3db segment for a given id.
type PersistenceFunc func(id string, segment Segment) error

// PersistenceCloser is a function that performs cleanup after persisting the data
// blocks for a (shard, blockStart) combination.
type PersistenceCloser func()

// PreparedPersistence is an object that wraps inside a persistence function and
// a closer.
type PreparedPersistence struct {
	Persist PersistenceFunc
	Close   PersistenceCloser
}

// PersistenceManager manages the internals of persisting data onto storage layer.
type PersistenceManager interface {

	// Prepare prepares writing data for a given (shard, blockStart) combination,
	// returning a PreparedPersistence object and any error encountered during
	// preparation if any.
	Prepare(shard uint32, blockStart time.Time) (PreparedPersistence, error)
}

// NewPersistenceManagerFn creates a new persistence manager.
type NewPersistenceManagerFn func(opts DatabaseOptions) PersistenceManager
