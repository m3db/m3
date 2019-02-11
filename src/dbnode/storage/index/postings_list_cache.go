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
	"github.com/pborman/uuid"
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

// PostingsListCacheEntry represents an entry in the query cache.
type PostingsListCacheEntry struct {
	Namespace   string
	BlockStart  xtime.UnixNano
	VolumeIndex int
	Pattern     string
	PatternType PatternType
}

// PostingsListCacheValue represents a value stored in the query cache.
type PostingsListCacheValue struct {
	PostingsList postings.List
}

// PostingsListCacheOptions is the options struct for the query cache.
type PostingsListCacheOptions struct {
	InstrumentOptions instrument.Options
}

// PostingsListCache implements an LRU for caching queries and their results.
type PostingsListCache struct {
	sync.Mutex

	lru *postingsListLRU

	size    int
	opts    PostingsListCacheOptions
	metrics *postingsListCacheMetrics

	stopReporting chan struct{}
}

// NewPostingsListCache creates a new query cache.
func NewPostingsListCache(size int, opts PostingsListCacheOptions) (*PostingsListCache, error) {
	lru, err := newPostingsListLRU(size)
	if err != nil {
		return nil, err
	}

	return &PostingsListCache{
		lru:     lru,
		size:    size,
		opts:    opts,
		metrics: newPostingsListCacheMetrics(opts.InstrumentOptions.MetricsScope()),
	}, nil
}

// GetRegexp returns the cached results for the provided regexp query, if any.
func (q *PostingsListCache) GetRegexp(
	segmentUUID uuid.UUID,
	pattern string,
) (postings.List, bool) {
	return q.get(
		segmentUUID,
		pattern,
		PatternTypeRegexp)
}

// GetTerm returns the cached results for the provided term query, if any.
func (q *PostingsListCache) GetTerm(
	segmentUUID uuid.UUID,
	pattern string,
) (postings.List, bool) {
	return q.get(
		segmentUUID,
		pattern,
		PatternTypeTerm)
}

func (q *PostingsListCache) get(
	segmentUUID uuid.UUID,
	pattern string,
	patternType PatternType,
) (postings.List, bool) {
	// No RLock because a Get() operation mutates the LRU.
	q.Lock()
	p, ok := q.lru.Get(segmentUUID, pattern, patternType)
	q.Unlock()

	if ok {
		q.metrics.regexp.hits.Inc(1)
	} else {
		q.metrics.regexp.misses.Inc(1)
	}

	if !ok {
		return nil, false
	}

	return p, ok
}

// PutRegexp updates the LRU with the result of the regexp query.
func (q *PostingsListCache) PutRegexp(
	segmentUUID uuid.UUID,
	pattern string,
	pl postings.List,
) {
	q.put(segmentUUID, pattern, PatternTypeRegexp, pl)
}

// PutTerm updates the LRU with the result of the term query.
func (q *PostingsListCache) PutTerm(
	segmentUUID uuid.UUID,
	pattern string,
	pl postings.List,
) {
	q.put(segmentUUID, pattern, PatternTypeTerm, pl)
}

func (q *PostingsListCache) put(
	segmentUUID uuid.UUID,
	pattern string,
	patternType PatternType,
	pl postings.List,
) {
	q.Lock()
	q.lru.Add(
		segmentUUID,
		pattern,
		patternType,
		pl,
	)
	q.Unlock()
	q.metrics.regexp.puts.Inc(1)
}

// PurgeSegment removes all postings lists associated with the specified
// segment from the cache.
func (q *PostingsListCache) PurgeSegment(segmentUUID uuid.UUID) {
	q.Lock()
	q.lru.PurgeSegment(segmentUUID)
	q.Unlock()
}

// StartReportLoop starts a background process that will call Report()
// on a regular basis and returns a function that will end the background
// process.
func (q *PostingsListCache) StartReportLoop() func() {
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
func (q *PostingsListCache) Report() {
	var size float64
	var capacity float64

	q.Lock()
	size = float64(q.lru.Len())
	capacity = float64(q.size)
	q.Unlock()

	q.metrics.size.Update(size)
	q.metrics.capacity.Update(capacity)
}

type postingsListCacheMetrics struct {
	regexp *postingsListCacheMethodMetrics
	term   *postingsListCacheMethodMetrics

	size     tally.Gauge
	capacity tally.Gauge
}

func newPostingsListCacheMetrics(scope tally.Scope) *postingsListCacheMetrics {
	return &postingsListCacheMetrics{
		regexp: newPostingsListCacheMethodMetrics(scope.SubScope("regexp")),
		term:   newPostingsListCacheMethodMetrics(scope.SubScope("term")),

		size:     scope.Gauge("size"),
		capacity: scope.Gauge("capacity"),
	}
}

type postingsListCacheMethodMetrics struct {
	hits   tally.Counter
	misses tally.Counter
	puts   tally.Counter
}

func newPostingsListCacheMethodMetrics(scope tally.Scope) *postingsListCacheMethodMetrics {
	return &postingsListCacheMethodMetrics{
		hits:   scope.Counter("hits"),
		misses: scope.Counter("misses"),
		puts:   scope.Counter("puts"),
	}
}
