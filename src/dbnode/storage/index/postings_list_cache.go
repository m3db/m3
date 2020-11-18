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

package index

import (
	"bytes"
	"errors"
	"time"

	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/m3db/m3/src/m3ninx/postings/roaring"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/cespare/xxhash/v2"
	"github.com/dgraph-io/ristretto"
	"github.com/pborman/uuid"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

var (
	errNoPostingsListPool = errors.New("no postings list pool set")
	errInstrumentOptions  = errors.New("no instrument options set")
)

// PatternType is an enum for the various pattern types. It allows us
// separate them logically within the cache.
type PatternType string

// Closer represents a function that will close managed resources.
type Closer func()

const (
	// PatternTypeRegexp indicates that the pattern is of type regexp.
	PatternTypeRegexp PatternType = "regexp"
	// PatternTypeTerm indicates that the pattern is of type term.
	PatternTypeTerm PatternType = "term"
	// PatternTypeField indicates that the pattern is of type field.
	PatternTypeField PatternType = "field"
	// PatternTypeSearch indicates that the pattern is of type search.
	PatternTypeSearch PatternType = "search"

	reportLoopInterval = 10 * time.Second
	emptyPattern       = ""
)

// PostingsListCacheOptions is the options struct for the query cache.
type PostingsListCacheOptions struct {
	PostingsListPool  postings.Pool
	InstrumentOptions instrument.Options
}

// Validate will return an error if the options are not valid.
func (o PostingsListCacheOptions) Validate() error {
	if o.PostingsListPool == nil {
		return errNoPostingsListPool
	}
	if o.InstrumentOptions == nil {
		return errInstrumentOptions
	}
	return nil
}

// PostingsListCache implements an LRU for caching queries and their results.
type PostingsListCache struct {
	lru *ristretto.Cache

	size    int
	opts    PostingsListCacheOptions
	metrics *postingsListCacheMetrics
	logger  *zap.Logger
}

// NewPostingsListCache creates a new query cache.
func NewPostingsListCache(
	size int,
	opts PostingsListCacheOptions,
) (*PostingsListCache, Closer, error) {
	err := opts.Validate()
	if err != nil {
		return nil, nil, err
	}

	lru, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: int64(10 * size), // number of keys to track frequency of.
		MaxCost:     int64(size),      // maximum cost of cache.
		BufferItems: 64,               // number of keys per Get buffer.
		KeyToHash: func(k interface{}) (uint64, uint64) {
			return k.(uint64), 0
		},
	})
	if err != nil {
		return nil, nil, err
	}

	plc := &PostingsListCache{
		size:    size,
		lru:     lru,
		opts:    opts,
		metrics: newPostingsListCacheMetrics(opts.InstrumentOptions.MetricsScope()),
		logger:  opts.InstrumentOptions.Logger(),
	}

	closer := plc.startReportLoop()
	return plc, closer, nil
}

// GetRegexp returns the cached results for the provided regexp query, if any.
func (q *PostingsListCache) GetRegexp(
	segmentUUID uuid.UUID,
	field string,
	pattern string,
) (postings.List, bool) {
	return q.get(segmentUUID, field, pattern, PatternTypeRegexp)
}

// GetTerm returns the cached results for the provided term query, if any.
func (q *PostingsListCache) GetTerm(
	segmentUUID uuid.UUID,
	field string,
	pattern string,
) (postings.List, bool) {
	return q.get(segmentUUID, field, pattern, PatternTypeTerm)
}

// GetField returns the cached results for the provided field query, if any.
func (q *PostingsListCache) GetField(
	segmentUUID uuid.UUID,
	field string,
) (postings.List, bool) {
	return q.get(segmentUUID, field, emptyPattern, PatternTypeField)
}

// GetSearch returns the cached results for the provided search query, if any.
func (q *PostingsListCache) GetSearch(
	segmentUUID uuid.UUID,
	query string,
) (postings.List, bool) {
	return q.get(segmentUUID, query, emptyPattern, PatternTypeSearch)
}

func (q *PostingsListCache) get(
	segmentUUID uuid.UUID,
	field string,
	pattern string,
	patternType PatternType,
) (postings.List, bool) {
	var pl *cachedPostings
	entry, ok := q.lru.Get(keyHash(segmentUUID, field, pattern, patternType))
	if ok {
		pl = entry.(*cachedPostings)
		ok = bytes.Equal(segmentUUID, pl.segmentUUID) &&
			field == pl.field &&
			pattern == pl.pattern &&
			patternType == pl.patternType
	}

	q.emitCacheGetMetrics(patternType, ok)

	if !ok {
		return nil, false
	}

	return pl.postings, ok
}

type cachedPostings struct {
	// key
	segmentUUID uuid.UUID
	field       string
	pattern     string
	patternType PatternType

	// value
	postings postings.List
}

func keyHash(
	segmentUUID uuid.UUID,
	field string,
	pattern string,
	patternType PatternType,
) uint64 {
	var h xxhash.Digest
	h.Reset()
	_, _ = h.Write(segmentUUID)
	_, _ = h.WriteString(field)
	_, _ = h.WriteString(pattern)
	_, _ = h.WriteString(string(patternType))
	return h.Sum64()
}

// PutRegexp updates the LRU with the result of the regexp query.
func (q *PostingsListCache) PutRegexp(
	segmentUUID uuid.UUID,
	field string,
	pattern string,
	pl postings.List,
) {
	q.put(segmentUUID, field, pattern, PatternTypeRegexp, pl)
}

// PutTerm updates the LRU with the result of the term query.
func (q *PostingsListCache) PutTerm(
	segmentUUID uuid.UUID,
	field string,
	pattern string,
	pl postings.List,
) {
	q.put(segmentUUID, field, pattern, PatternTypeTerm, pl)
}

// PutField updates the LRU with the result of the field query.
func (q *PostingsListCache) PutField(
	segmentUUID uuid.UUID,
	field string,
	pl postings.List,
) {
	q.put(segmentUUID, field, emptyPattern, PatternTypeField, pl)
}

// PutSearch updates the LRU with the result of a search query.
func (q *PostingsListCache) PutSearch(
	segmentUUID uuid.UUID,
	query string,
	pl postings.List,
) {
	if roaring.IsReadOnlyPostingsList(pl) {
		// Copy into mutable postings list since it's expensive to read from
		// a read only postings list over and over again (it's lazily
		// evaluated from for allocation purposes).
		mutable := q.opts.PostingsListPool.Get()
		if err := mutable.AddIterator(pl.Iterator()); err != nil {
			q.metrics.pooledGetErrAddIter.Inc(1)
			q.logger.Error("unable to add postings iter", zap.Error(err))
			return
		}
		pl = mutable
	}

	q.put(segmentUUID, query, emptyPattern, PatternTypeSearch, pl)
}

func (q *PostingsListCache) put(
	segmentUUID uuid.UUID,
	field string,
	pattern string,
	patternType PatternType,
	pl postings.List,
) {
	key := keyHash(segmentUUID, field, pattern, patternType)
	value := &cachedPostings{
		segmentUUID: segmentUUID,
		field:       field,
		pattern:     pattern,
		patternType: patternType,
		postings:    pl,
	}
	q.lru.Set(key, value, 1)
	q.emitCachePutMetrics(patternType)
}

// startReportLoop starts a background process that will call Report()
// on a regular basis and returns a function that will end the background
// process.
func (q *PostingsListCache) startReportLoop() Closer {
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
	q.metrics.capacity.Update(float64(q.size))
}

func (q *PostingsListCache) emitCacheGetMetrics(patternType PatternType, hit bool) {
	var method *postingsListCacheMethodMetrics
	switch patternType {
	case PatternTypeRegexp:
		method = q.metrics.regexp
	case PatternTypeTerm:
		method = q.metrics.term
	case PatternTypeField:
		method = q.metrics.field
	case PatternTypeSearch:
		method = q.metrics.search
	default:
		method = q.metrics.unknown // should never happen
	}
	if hit {
		method.hits.Inc(1)
	} else {
		method.misses.Inc(1)
	}
}

func (q *PostingsListCache) emitCachePutMetrics(patternType PatternType) {
	switch patternType {
	case PatternTypeRegexp:
		q.metrics.regexp.puts.Inc(1)
	case PatternTypeTerm:
		q.metrics.term.puts.Inc(1)
	case PatternTypeField:
		q.metrics.field.puts.Inc(1)
	case PatternTypeSearch:
		q.metrics.search.puts.Inc(1)
	default:
		q.metrics.unknown.puts.Inc(1) // should never happen
	}
}

type postingsListCacheMetrics struct {
	regexp  *postingsListCacheMethodMetrics
	term    *postingsListCacheMethodMetrics
	field   *postingsListCacheMethodMetrics
	search  *postingsListCacheMethodMetrics
	unknown *postingsListCacheMethodMetrics

	size     tally.Gauge
	capacity tally.Gauge

	pooledGet              tally.Counter
	pooledGetErrAddIter    tally.Counter
	pooledPut              tally.Counter
	pooledPutErrNotMutable tally.Counter
}

func newPostingsListCacheMetrics(scope tally.Scope) *postingsListCacheMetrics {
	return &postingsListCacheMetrics{
		regexp: newPostingsListCacheMethodMetrics(scope.Tagged(map[string]string{
			"query_type": "regexp",
		})),
		term: newPostingsListCacheMethodMetrics(scope.Tagged(map[string]string{
			"query_type": "term",
		})),
		field: newPostingsListCacheMethodMetrics(scope.Tagged(map[string]string{
			"query_type": "field",
		})),
		search: newPostingsListCacheMethodMetrics(scope.Tagged(map[string]string{
			"query_type": "search",
		})),
		unknown: newPostingsListCacheMethodMetrics(scope.Tagged(map[string]string{
			"query_type": "unknown",
		})),
		size:      scope.Gauge("size"),
		capacity:  scope.Gauge("capacity"),
		pooledGet: scope.Counter("pooled_get"),
		pooledGetErrAddIter: scope.Tagged(map[string]string{
			"error_type": "add_iter",
		}).Counter("pooled_get_error"),
		pooledPut: scope.Counter("pooled_put"),
		pooledPutErrNotMutable: scope.Tagged(map[string]string{
			"error_type": "not_mutable",
		}).Counter("pooled_put_error"),
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
