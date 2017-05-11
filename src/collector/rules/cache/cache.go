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
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/m3db/m3collector/rules"
	mrules "github.com/m3db/m3metrics/rules"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/id"
)

const (
	numOngoingTasks          = 2
	deletionThrottleInterval = 100 * time.Millisecond
)

var (
	errCacheClosed = errors.New("cache is already closed")
)

type setType int

const (
	dontSetIfNotFound setType = iota
	setIfNotFound
)

type sleepFn func(time.Duration)

type elemMap map[xid.Hash]*element

type results struct {
	elems  elemMap
	source rules.Source
}

func newResults(source rules.Source) results {
	return results{elems: make(elemMap), source: source}
}

// cache is an LRU-based read-through cache.
type cache struct {
	sync.RWMutex

	capacity          int
	nowFn             clock.NowFn
	maxPositiveSkew   time.Duration
	maxNegativeSkew   time.Duration
	freshDuration     time.Duration
	stutterDuration   time.Duration
	evictionBatchSize int
	deletionBatchSize int
	invalidationMode  InvalidationMode
	sleepFn           sleepFn

	namespaces map[xid.Hash]results
	list       lockedList
	evictCh    chan struct{}
	deleteCh   chan struct{}
	toDelete   []elemMap
	wgWorker   sync.WaitGroup
	closed     bool
	closedCh   chan struct{}
}

// NewCache creates a new cache.
func NewCache(opts Options) rules.Cache {
	clockOpts := opts.ClockOptions()
	c := &cache{
		capacity:          opts.Capacity(),
		nowFn:             clockOpts.NowFn(),
		maxPositiveSkew:   clockOpts.MaxPositiveSkew(),
		maxNegativeSkew:   clockOpts.MaxNegativeSkew(),
		freshDuration:     opts.FreshDuration(),
		stutterDuration:   opts.StutterDuration(),
		evictionBatchSize: opts.EvictionBatchSize(),
		deletionBatchSize: opts.DeletionBatchSize(),
		invalidationMode:  opts.InvalidationMode(),
		sleepFn:           time.Sleep,
		namespaces:        make(map[xid.Hash]results),
		evictCh:           make(chan struct{}, 1),
		deleteCh:          make(chan struct{}, 1),
		closedCh:          make(chan struct{}),
	}

	c.wgWorker.Add(numOngoingTasks)
	go c.evict()
	go c.delete()

	return c
}

func (c *cache) Match(namespace []byte, id []byte) mrules.MatchResult {
	c.RLock()
	res, found := c.tryGetWithLock(namespace, id, dontSetIfNotFound)
	c.RUnlock()
	if found {
		return res
	}

	c.Lock()
	res, _ = c.tryGetWithLock(namespace, id, setIfNotFound)
	c.Unlock()

	return res
}

func (c *cache) Register(namespace []byte, source rules.Source) {
	c.Lock()
	nsHash := xid.HashFn(namespace)
	results, exist := c.namespaces[nsHash]
	if !exist {
		results = newResults(source)
	} else {
		// Invalidate existing cached results.
		c.toDelete = append(c.toDelete, results.elems)
		c.notifyDeletion()
		results.source = source
		results.elems = make(elemMap)
	}
	c.namespaces[nsHash] = results
	c.Unlock()
}

func (c *cache) Unregister(namespace []byte) {
	c.Lock()
	nsHash := xid.HashFn(namespace)
	results, exists := c.namespaces[nsHash]
	if !exists {
		c.Unlock()
		return
	}
	delete(c.namespaces, nsHash)
	c.toDelete = append(c.toDelete, results.elems)
	c.Unlock()

	c.notifyDeletion()
}

func (c *cache) Close() error {
	c.Lock()
	if c.closed {
		c.Unlock()
		return errCacheClosed
	}
	c.closed = true
	c.Unlock()

	close(c.closedCh)
	c.wgWorker.Wait()
	return nil
}

// tryGetWithLock attempts to get the match result, returning true if a match
// result is successfully determined and no further processing is required,
// and false otherwise.
func (c *cache) tryGetWithLock(
	namespace []byte,
	id []byte,
	setType setType,
) (mrules.MatchResult, bool) {
	res := mrules.EmptyMatchResult
	nsHash := xid.HashFn(namespace)
	results, exists := c.namespaces[nsHash]
	if !exists {
		return res, true
	}
	idHash := xid.HashFn(id)
	elem, exists := results.elems[idHash]
	if exists {
		now := c.nowFn()
		res = elem.result
		latest := now.Add(c.maxPositiveSkew)
		// NB(xichen): the cached match result expires when a new rule takes effect.
		// Because we handle clock skews by matching all policies within
		// [now - max_negative_skew, now + max_positive_skew), the cached result expires
		// as soon as now + max_positive_skew reaches the cutover time of the next rule.
		if !res.HasExpired(latest) {
			// NB(xichen): in order to avoid the overhead acquiring an exclusive
			// lock to perform a promotion to move the element to the front of the
			// list, we set an expiry time for each promotion and do not perform
			// another promotion if the previous one is still fresh. This should be
			// good enough because if the cache is sufficiently large, the frequently
			// accessed items should be still near the front of the list. Additionally,
			// we can still achieve the exact LRU semantics by setting fresh duration
			// and stutter duration to 0.
			if elem.ShouldPromote(now) {
				c.promote(now, elem)
			}
			return res, true
		}
	}
	if setType == dontSetIfNotFound {
		return res, false
	}
	// NB(xichen): the result is either not cached, or cached but invalid, in both
	// cases we should use the source to compute the result and set it in the cache.
	return c.setWithLock(nsHash, id, idHash, results, exists), true
}

func (c *cache) setWithLock(
	nsHash xid.Hash,
	id []byte,
	idHash xid.Hash,
	results results,
	invalidate bool,
) mrules.MatchResult {
	// NB(xichen): if a cached result is invalid, it's very likely that we've reached
	// a new cutover time and the old cached results are now invalid, therefore it's
	// preferrable to invalidate everything to save the overhead of multiple invalidations.
	if invalidate {
		results = c.invalidateWithLock(nsHash, idHash, results)
	}
	now := c.nowFn()
	earliest := now.Add(-c.maxNegativeSkew)
	latest := now.Add(c.maxPositiveSkew)
	res := results.source.Match(id, earliest, latest)
	newElem := &element{nsHash: nsHash, idHash: idHash, result: res}
	newElem.SetPromotionExpiry(c.newPromotionExpiry(now))
	results.elems[idHash] = newElem
	// NB(xichen): we don't evict until the number of cached items goes
	// above the capacity by at least the eviction batch size to amortize
	// the eviction overhead.
	if newSize := c.add(newElem); newSize > c.capacity+c.evictionBatchSize {
		c.notifyEviction()
	}
	return res
}

func (c *cache) add(elem *element) int {
	c.list.Lock()
	c.list.PushFront(elem)
	size := c.list.Len()
	c.list.Unlock()
	return size
}

func (c *cache) promote(now time.Time, elem *element) {
	c.list.Lock()
	// Bail if someone else got ahead of us and promoted this element.
	if !elem.ShouldPromote(now) {
		c.list.Unlock()
		return
	}
	// Otherwise proceed with promotion.
	elem.SetPromotionExpiry(c.newPromotionExpiry(now))
	c.list.MoveToFront(elem)
	c.list.Unlock()
}

func (c *cache) invalidateWithLock(nsHash xid.Hash, idHash xid.Hash, results results) results {
	if c.invalidationMode == InvalidateAll {
		c.toDelete = append(c.toDelete, results.elems)
		c.notifyDeletion()
		results.elems = make(elemMap)
		c.namespaces[nsHash] = results
	} else {
		elem := results.elems[idHash]
		delete(results.elems, idHash)
		c.list.Lock()
		c.list.Remove(elem)
		c.list.Unlock()
	}
	return results
}

func (c *cache) evict() {
	defer c.wgWorker.Done()

	for {
		select {
		case <-c.evictCh:
			c.doEvict()
		case <-c.closedCh:
			return
		}
	}
}

func (c *cache) doEvict() {
	c.Lock()
	c.list.Lock()
	for c.list.Len() > c.capacity {
		elem := c.list.Back()
		c.list.Remove(elem)
		// NB(xichen): the namespace owning this element may have been deleted,
		// in which case we simply continue. This is okay because the deleted element
		// will be marked as deleted so when the deletion goroutine sees and tries to
		// delete it again, it will be a no op, at which point it will be removed from
		// the owning map as well.
		results, exists := c.namespaces[elem.nsHash]
		if !exists {
			continue
		}
		delete(results.elems, elem.idHash)
	}
	c.list.Unlock()
	c.Unlock()
}

func (c *cache) delete() {
	defer c.wgWorker.Done()

	for {
		select {
		case <-c.deleteCh:
			c.doDelete()
		case <-c.closedCh:
			return
		}
	}
}

func (c *cache) doDelete() {
	c.Lock()
	if len(c.toDelete) == 0 {
		c.Unlock()
		return
	}

	// NB(xichen): add pooling if deletion happens frequent enough.
	toDelete := c.toDelete
	c.toDelete = nil
	c.Unlock()

	deleted := 0
	c.list.Lock()
	for _, elems := range toDelete {
		for _, elem := range elems {
			c.list.Remove(elem)
			deleted++
			// If we have deleted enough elements, release the lock
			// and give other goroutines a chance to acquire the lock
			// since deletion does not need to be fast.
			if deleted >= c.deletionBatchSize {
				c.list.Unlock()
				c.sleepFn(deletionThrottleInterval)
				deleted = 0
				c.list.Lock()
			}
		}
	}
	c.list.Unlock()
}

func (c *cache) notifyEviction() {
	select {
	case c.evictCh <- struct{}{}:
	default:
	}
}

func (c *cache) notifyDeletion() {
	select {
	case c.deleteCh <- struct{}{}:
	default:
	}
}

func (c *cache) newPromotionExpiry(now time.Time) time.Time {
	expiry := now.Add(c.freshDuration)
	if c.stutterDuration > 0 {
		expiry = expiry.Add(time.Duration(rand.Int63n(int64(c.stutterDuration))))
	}
	return expiry
}
