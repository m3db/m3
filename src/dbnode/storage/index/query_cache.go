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
	"sync"
	"time"

	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/m3db/m3x/instrument"
	xtime "github.com/m3db/m3x/time"
	"github.com/pilosa/pilosa/lru"
	"github.com/uber-go/tally"
)

// PatternType is an enum for the various pattern types. It allows us
// separate them logically within the cache.
type PatternType int

const (
	// PatternTypeRegexp indicates that the pattern is of type regexp.
	PatternTypeRegexp PatternType = 0
	// PatternTypeTerm indicates that the pattern is of type term.
	PatternTypeTerm PatternType = 1

	reportLoopInterval = time.Second
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

// QueryCacheOptions is the options struct for the query cache.
type QueryCacheOptions struct {
	InstrumentOptions instrument.Options
}

// QueryCache implements an LRU for caching queries and their results.
type QueryCache struct {
	sync.RWMutex

	lru *lru.Cache

	size    int
	opts    QueryCacheOptions
	metrics *queryCacheMetrics

	stopReporting chan struct{}
}

// NewQueryCache creates a new query cache.
func NewQueryCache(size int, opts QueryCacheOptions) *QueryCache {
	return &QueryCache{
		lru:     lru.New(size),
		size:    size,
		opts:    opts,
		metrics: newQueryCacheMetrics(opts.InstrumentOptions.MetricsScope()),
	}
}

// GetRegexp returns the cached results for the provided regexp query, if any.
func (q *QueryCache) GetRegexp(
	namespace string,
	blockStart time.Time,
	volumeIndex int,
	pattern string,
) (postings.List, bool) {
	return q.get(
		namespace,
		blockStart,
		volumeIndex,
		pattern,
		PatternTypeRegexp)
}

// GetTerm returns the cached results for the provided term query, if any.
func (q *QueryCache) GetTerm(
	namespace string,
	blockStart time.Time,
	volumeIndex int,
	pattern string,
) (postings.List, bool) {
	return q.get(
		namespace,
		blockStart,
		volumeIndex,
		pattern,
		PatternTypeTerm)
}

func (q *QueryCache) get(
	namespace string,
	blockStart time.Time,
	volumeIndex int,
	pattern string,
	patternType PatternType,
) (postings.List, bool) {
	q.RLock()
	p, ok := q.lru.Get(QueryCacheEntry{
		Namespace:   namespace,
		BlockStart:  xtime.ToUnixNano(blockStart),
		VolumeIndex: volumeIndex,
		Pattern:     pattern,
		PatternType: PatternTypeRegexp,
	})
	q.RUnlock()
	if ok {
		q.metrics.regexp.hits.Inc(1)
	} else {
		q.metrics.regexp.misses.Inc(1)
	}
	return p.(QueryCacheValue).PostingsList, ok
}

// PutRegexp updates the LRU with the result of the regexp query.
func (q *QueryCache) PutRegexp(
	namespace string,
	blockStart time.Time,
	volumeIndex int,
	pattern string,
	pl postings.List,
) {
	q.put(
		namespace,
		blockStart,
		volumeIndex,
		pattern,
		PatternTypeRegexp,
		pl)
}

// PutTerm updates the LRU with the result of the term query.
func (q *QueryCache) PutTerm(
	namespace string,
	blockStart time.Time,
	volumeIndex int,
	pattern string,
	pl postings.List,
) {
	q.put(
		namespace,
		blockStart,
		volumeIndex,
		pattern,
		PatternTypeTerm,
		pl)
}

func (q *QueryCache) put(
	namespace string,
	blockStart time.Time,
	volumeIndex int,
	pattern string,
	patternType PatternType,
	pl postings.List,
) {
	q.Lock()
	q.lru.Add(QueryCacheEntry{
		Namespace:   namespace,
		BlockStart:  xtime.ToUnixNano(blockStart),
		VolumeIndex: volumeIndex,
		Pattern:     pattern,
		PatternType: PatternTypeRegexp,
	}, QueryCacheValue{PostingsList: pl})
	q.Unlock()
	q.metrics.regexp.puts.Inc(1)
}

// StartReportLoop starts a background process that will call Report()
// on a regular basis and returns a function that will end the background
// process.
func (q *QueryCache) StartReportLoop() func() {
	doneCh := make(chan struct{})

	go func() {
		for {
			select {
			case <-doneCh:
				return
			default:
			}

			q.Report()
			time.Sleep(reportLoopInterval)
		}
	}()

	return func() { close(doneCh) }
}

// Report will emit metrics about the status of the cache.
func (q *QueryCache) Report() {
	var size float64
	var capacity float64

	q.RLock()
	size = float64(q.lru.Len())
	capacity = float64(q.size)
	q.RUnlock()

	q.metrics.size.Update(size)
	q.metrics.capacity.Update(capacity)
}

type queryCacheMetrics struct {
	regexp *queryCacheMethodMetrics
	term   *queryCacheMethodMetrics

	size     tally.Gauge
	capacity tally.Gauge
}

func newQueryCacheMetrics(scope tally.Scope) *queryCacheMetrics {
	return &queryCacheMetrics{
		regexp: newQueryCacheMethodMetrics(scope.SubScope("regexp")),
		term:   newQueryCacheMethodMetrics(scope.SubScope("term")),

		size:     scope.Gauge("size"),
		capacity: scope.Gauge("capacity"),
	}
}

type queryCacheMethodMetrics struct {
	hits   tally.Counter
	misses tally.Counter
	puts   tally.Counter
}

func newQueryCacheMethodMetrics(scope tally.Scope) *queryCacheMethodMetrics {
	return &queryCacheMethodMetrics{
		hits:   scope.Counter("hits"),
		misses: scope.Counter("misses"),
		puts:   scope.Counter("puts"),
	}
}
