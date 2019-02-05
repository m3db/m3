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

	xtime "github.com/m3db/m3x/time"
)

// QueryCacheEntry represents an entry in the query cache.
type QueryCacheEntry struct {
	Namespace  string
	BlockStart xtime.UnixNano
	Regexp     string
}

// QueryCacheValue represents a value stored in the query cache.
type QueryCacheValue struct {
	Results Results
}

// QueryCache implements an LRU for caching queries and their results.
type QueryCache struct {
	c map[QueryCacheEntry]QueryCacheValue
}

// Get returns the cached results for the provided query, if any.
func (q *QueryCache) Get(
	namespace string,
	blockStart time.Time,
	Regexp string,
) Results {
	return nil
}

// Put updates the LRU with the result of the query.
func (q *QueryCache) Put(
	namespace string,
	blockStart time.Time,
	Regexp string,
	r Results,
) {
	// Ensure that the underlying IDs and tags in the results we're
	// about to cache won't get finalized when the request that
	// generated them completes.
	r.NoFinalize()

}
