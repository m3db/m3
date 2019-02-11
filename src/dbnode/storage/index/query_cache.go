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

	"github.com/m3db/m3/src/m3ninx/postings"
	xtime "github.com/m3db/m3x/time"
)

// PatternType is an enum for the various pattern types. It allows us
// separate them logically within the cache.
type PatternType int

var (
	// PatternTypeRegexp indicates that the pattern is of type regexp.
	PatternTypeRegexp PatternType = 0
	// PatternTypeTerm indicates that the pattern is of type term.
	PatternTypeTerm PatternType = 1
)

// QueryCacheEntry represents an entry in the query cache.
type QueryCacheEntry struct {
	Namespace   string
	BlockStart  xtime.UnixNano
	VolumeIndex int
	Pattern     string
	PatternType PatternType
}

// QueryCacheValue represents a value stored in the query cache.
type QueryCacheValue struct {
	PostingsList postings.List
}

// QueryCache implements an LRU for caching queries and their results.
type QueryCache struct {
	c map[QueryCacheEntry]QueryCacheValue
}

// NewQueryCache creates a new query cache.
func NewQueryCache(size int) *QueryCache {
	return &QueryCache{
		c: make(map[QueryCacheEntry]QueryCacheValue),
	}
}

// GetRegexp returns the cached results for the provided regexp query, if any.
func (q *QueryCache) GetRegexp(
	namespace string,
	blockStart time.Time,
	volumeIndex int,
	pattern string,
) (postings.List, bool) {
	p, ok := q.c[QueryCacheEntry{
		Namespace:   namespace,
		BlockStart:  xtime.ToUnixNano(blockStart),
		VolumeIndex: volumeIndex,
		Pattern:     pattern,
		PatternType: PatternTypeRegexp,
	}]
	return p.PostingsList, ok
}

// GetTerm returns the cached results for the provided term query, if any.
func (q *QueryCache) GetTerm(
	namespace string,
	blockStart time.Time,
	volumeIndex int,
	pattern string,
) (postings.List, bool) {
	p, ok := q.c[QueryCacheEntry{
		Namespace:   namespace,
		BlockStart:  xtime.ToUnixNano(blockStart),
		VolumeIndex: volumeIndex,
		Pattern:     pattern,
		PatternType: PatternTypeTerm,
	}]
	return p.PostingsList, ok
}

// PutRegexp updates the LRU with the result of the regexp query.
func (q *QueryCache) PutRegexp(
	namespace string,
	blockStart time.Time,
	volumeIndex int,
	pattern string,
	pl postings.List,
) {
	q.c[QueryCacheEntry{
		Namespace:   namespace,
		BlockStart:  xtime.ToUnixNano(blockStart),
		VolumeIndex: volumeIndex,
		Pattern:     pattern,
		PatternType: PatternTypeRegexp,
	}] = QueryCacheValue{PostingsList: pl}
}

// PutTerm updates the LRU with the result of the term query.
func (q *QueryCache) PutTerm(
	namespace string,
	blockStart time.Time,
	volumeIndex int,
	pattern string,
	pl postings.List,
) {
	q.c[QueryCacheEntry{
		Namespace:   namespace,
		BlockStart:  xtime.ToUnixNano(blockStart),
		VolumeIndex: volumeIndex,
		Pattern:     pattern,
		PatternType: PatternTypeTerm,
	}] = QueryCacheValue{PostingsList: pl}
}
