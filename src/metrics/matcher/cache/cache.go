// Copyright (c) 2017 Uber Technologies, Inc.
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

package cache

import (
	"sync"
	"time"

	"github.com/m3db/m3/src/metrics/rules"
	"github.com/m3db/m3/src/x/clock"

	"github.com/uber-go/tally"
)

// Source is a datasource providing match results.
type Source interface {
	// ForwardMatch returns the match result for a given id within time range
	// [fromNanos, toNanos).
	ForwardMatch(id []byte, fromNanos, toNanos int64) rules.MatchResult
}

// Cache caches the rule matching result associated with metrics.
type Cache interface {
	// ForwardMatch returns the rule matching result associated with a metric id
	// between [fromNanos, toNanos).
	ForwardMatch(namespace, id []byte, fromNanos, toNanos int64) rules.MatchResult

	// Register sets the source for a given namespace.
	Register(namespace []byte, source Source)

	// Refresh clears the cached results for the given source for a given namespace.
	Refresh(namespace []byte, source Source)

	// Unregister deletes the cached results for a given namespace.
	Unregister(namespace []byte)
}

type sleepFn func(time.Duration)

type elementPtr *element

type results struct {
	elems  *elemMap
	source Source
}

func newResults(source Source) results {
	return results{
		elems:  newElemMap(elemMapOptions{}),
		source: source,
	}
}

type cacheMetrics struct {
	hits                tally.Counter
	misses              tally.Counter
	expires             tally.Counter
	registers           tally.Counter
	registerExists      tally.Counter
	updates             tally.Counter
	updateNotExists     tally.Counter
	updateStaleSource   tally.Counter
	unregisters         tally.Counter
	unregisterNotExists tally.Counter
	promotions          tally.Counter
	evictions           tally.Counter
	deletions           tally.Counter
}

func newCacheMetrics(scope tally.Scope) cacheMetrics {
	return cacheMetrics{
		hits:                scope.Counter("hits"),
		misses:              scope.Counter("misses"),
		expires:             scope.Counter("expires"),
		registers:           scope.Counter("registers"),
		registerExists:      scope.Counter("register-exists"),
		updates:             scope.Counter("updates"),
		updateNotExists:     scope.Counter("update-not-exists"),
		updateStaleSource:   scope.Counter("update-stale-source"),
		unregisters:         scope.Counter("unregisters"),
		unregisterNotExists: scope.Counter("unregister-not-exists"),
		promotions:          scope.Counter("promotions"),
		evictions:           scope.Counter("evictions"),
		deletions:           scope.Counter("deletions"),
	}
}

// cache is an LRU-based read-through cache.
type cache struct {
	sync.RWMutex

	capacity          int
	nowFn             clock.NowFn
	freshDuration     time.Duration
	stutterDuration   time.Duration
	evictionBatchSize int
	deletionBatchSize int
	invalidationMode  InvalidationMode
	sleepFn           sleepFn

	namespaces *namespaceResultsMap
	list       lockedList
	evictCh    chan struct{}
	deleteCh   chan struct{}
	toDelete   []*elemMap
	metrics    cacheMetrics
}

// NewCache creates a new cache.
func NewCache(opts Options) Cache {
	clockOpts := opts.ClockOptions()
	instrumentOpts := opts.InstrumentOptions()
	c := &cache{
		capacity:          opts.Capacity(),
		nowFn:             clockOpts.NowFn(),
		freshDuration:     opts.FreshDuration(),
		stutterDuration:   opts.StutterDuration(),
		evictionBatchSize: opts.EvictionBatchSize(),
		deletionBatchSize: opts.DeletionBatchSize(),
		invalidationMode:  opts.InvalidationMode(),
		sleepFn:           time.Sleep,
		namespaces:        newNamespaceResultsMap(namespaceResultsMapOptions{}),
		evictCh:           make(chan struct{}, 1),
		deleteCh:          make(chan struct{}, 1),
		metrics:           newCacheMetrics(instrumentOpts.MetricsScope()),
	}

	return c
}

func (c *cache) ForwardMatch(namespace, id []byte, fromNanos, toNanos int64) rules.MatchResult {
	c.RLock()
	results, exists := c.namespaces.Get(namespace)
	c.RUnlock()
	if !exists {
		return rules.EmptyMatchResult
	}
	return results.source.ForwardMatch(id, fromNanos, toNanos)
}

func (c *cache) Register(namespace []byte, source Source) {
	c.Lock()
	defer c.Unlock()

	if results, exist := c.namespaces.Get(namespace); !exist {
		c.namespaces.Set(namespace, newResults(source))
		c.metrics.registers.Inc(1)
	} else {
		c.refreshWithLock(namespace, source, results)
		c.metrics.registerExists.Inc(1)
	}
}

func (c *cache) Refresh(namespace []byte, source Source) {
	c.Lock()
	defer c.Unlock()

	results, exist := c.namespaces.Get(namespace)
	// NB: The namespace does not exist yet. This could happen if the source update came
	// before its namespace is registered. It is safe to ignore this premature update
	// because the namespace will eventually register itself and refreshes the cache.
	if !exist {
		c.metrics.updateNotExists.Inc(1)
		return
	}
	// NB: The source to update is different from what's stored in the cache. This could
	// happen if the namespace is changed, removed, and then revived before the rule change
	// could be processed. It is safe to ignore this stale update because the last rule
	// change update will eventually be processed and the cache will be refreshed.
	if results.source != source {
		c.metrics.updateStaleSource.Inc(1)
		return
	}
	c.refreshWithLock(namespace, source, results)
	c.metrics.updates.Inc(1)
}

func (c *cache) Unregister(namespace []byte) {
	c.Lock()
	defer c.Unlock()

	results, exists := c.namespaces.Get(namespace)
	if !exists {
		c.metrics.unregisterNotExists.Inc(1)
		return
	}
	c.namespaces.Delete(namespace)
	c.toDelete = append(c.toDelete, results.elems)
	c.metrics.unregisters.Inc(1)
}

// refreshWithLock clears the existing cached results for namespace nsHash
// and associates the namespace results with a new source.
func (c *cache) refreshWithLock(namespace []byte, source Source, results results) {
	c.toDelete = append(c.toDelete, results.elems)
	results.source = source
	results.elems = newElemMap(elemMapOptions{})
	c.namespaces.Set(namespace, results)
}
