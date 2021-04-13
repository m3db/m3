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
	"container/list"
	"errors"
	"math"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/pborman/uuid"
)

// PostingsListLRU implements a non-thread safe fixed size LRU cache of postings lists
// that were resolved by running a given query against a particular segment for a given
// field and pattern type (term vs regexp). Normally a key in the LRU would look like:
//
// type key struct {
//    segmentUUID uuid.UUID
//    field       string
//    pattern     string
//    patternType PatternType
// }
//
// However, some of the postings lists that we will store in the LRU have a fixed lifecycle
// because they reference mmap'd byte slices which will eventually be unmap'd. To prevent
// these postings lists that point to unmap'd regions from remaining in the LRU, we want to
// support the ability to efficiently purge the LRU of any postings list that belong to a
// given segment. This isn't technically required for correctness as once a segment has been
// closed, its old postings list in the LRU will never be accessed again (since they are only
// addressable by that segments UUID), but we purge them from the LRU before closing the segment
// anyways as an additional safety precaution.
//
// Instead of adding additional tracking on-top of an existing generic LRU, we've created a
// specialized LRU that instead of having a single top-level map pointing into the linked-list,
// has a two-level map where the top level map is keyed by segment UUID and the second level map
// is keyed by the field/pattern/patternType.
//
// As a result, when a segment is ready to be closed, they can call into the cache with their
// UUID and we can efficiently remove all the entries corresponding to that segment from the
// LRU. The specialization has the additional nice property that we don't need to allocate everytime
// we add an item to the LRU due to the interface{} conversion.
type postingsListLRU struct {
	shards    []*postingsListLRUShard
	numShards uint64
}

type postingsListLRUShard struct {
	sync.RWMutex
	size      int
	evictList *list.List
	items     map[uuid.Array]map[key]*list.Element
}

// entry is used to hold a value in the evictList.
type entry struct {
	uuid           uuid.UUID
	key            key
	cachedPostings *cachedPostings
}

type key struct {
	field       string
	pattern     string
	patternType PatternType
}

type postingsListLRUOptions struct {
	size   int
	shards int
}

// newPostingsListLRU constructs an LRU of the given size.
func newPostingsListLRU(opts postingsListLRUOptions) (*postingsListLRU, error) {
	size, shards := opts.size, opts.shards
	if size <= 0 {
		return nil, errors.New("must provide a positive size")
	}
	if shards <= 0 {
		return nil, errors.New("must provide a positive shards")
	}

	lruShards := make([]*postingsListLRUShard, 0, shards)
	for i := 0; i < shards; i++ {
		lruShard := newPostingsListLRUShard(int(math.Ceil(float64(size) / float64(shards))))
		lruShards = append(lruShards, lruShard)
	}

	return &postingsListLRU{
		shards:    lruShards,
		numShards: uint64(len(lruShards)),
	}, nil
}

// newPostingsListLRU constructs an LRU of the given size.
func newPostingsListLRUShard(size int) *postingsListLRUShard {
	return &postingsListLRUShard{
		size:      size,
		evictList: list.New(),
		items:     make(map[uuid.Array]map[key]*list.Element),
	}
}

func (c *postingsListLRU) shard(
	segmentUUID uuid.UUID,
	field, pattern string,
	patternType PatternType,
) *postingsListLRUShard {
	idx := hashKey(segmentUUID, field, pattern, patternType) % c.numShards
	return c.shards[idx]
}

func (c *postingsListLRU) Add(
	segmentUUID uuid.UUID,
	field string,
	pattern string,
	patternType PatternType,
	cachedPostings *cachedPostings,
) bool {
	shard := c.shard(segmentUUID, field, pattern, patternType)
	return shard.Add(segmentUUID, field, pattern, patternType, cachedPostings)
}

func (c *postingsListLRU) Get(
	segmentUUID uuid.UUID,
	field string,
	pattern string,
	patternType PatternType,
) (*cachedPostings, bool) {
	shard := c.shard(segmentUUID, field, pattern, patternType)
	return shard.Get(segmentUUID, field, pattern, patternType)
}

func (c *postingsListLRU) Remove(
	segmentUUID uuid.UUID,
	field string,
	pattern string,
	patternType PatternType,
) bool {
	shard := c.shard(segmentUUID, field, pattern, patternType)
	return shard.Remove(segmentUUID, field, pattern, patternType)
}

func (c *postingsListLRU) PurgeSegment(segmentUUID uuid.UUID) {
	for _, shard := range c.shards {
		shard.PurgeSegment(segmentUUID)
	}
}

func (c *postingsListLRU) Len() int {
	n := 0
	for _, shard := range c.shards {
		n += shard.Len()
	}
	return n
}

// Add adds a value to the cache. Returns true if an eviction occurred.
func (c *postingsListLRUShard) Add(
	segmentUUID uuid.UUID,
	field string,
	pattern string,
	patternType PatternType,
	cachedPostings *cachedPostings,
) (evicted bool) {
	c.Lock()
	defer c.Unlock()

	newKey := newKey(field, pattern, patternType)
	// Check for existing item.
	uuidArray := segmentUUID.Array()
	if uuidEntries, ok := c.items[uuidArray]; ok {
		if ent, ok := uuidEntries[newKey]; ok {
			// If it already exists, just move it to the front. This avoids storing
			// the same item in the LRU twice which is important because the maps
			// can only point to one entry at a time and we use them for purges. Also,
			// it saves space by avoiding storing duplicate values.
			c.evictList.MoveToFront(ent)
			ent.Value.(*entry).cachedPostings = cachedPostings
			return false
		}
	}

	// Add new item.
	var (
		ent = &entry{
			uuid:           segmentUUID,
			key:            newKey,
			cachedPostings: cachedPostings,
		}
		entry = c.evictList.PushFront(ent)
	)
	if queries, ok := c.items[uuidArray]; ok {
		queries[newKey] = entry
	} else {
		c.items[uuidArray] = map[key]*list.Element{
			newKey: entry,
		}
	}

	evict := c.evictList.Len() > c.size
	// Verify size not exceeded.
	if evict {
		c.removeOldest()
	}
	return evict
}

// Get looks up a key's value from the cache.
func (c *postingsListLRUShard) Get(
	segmentUUID uuid.UUID,
	field string,
	pattern string,
	patternType PatternType,
) (*cachedPostings, bool) {
	c.Lock()
	defer c.Unlock()

	newKey := newKey(field, pattern, patternType)
	uuidArray := segmentUUID.Array()

	uuidEntries, ok := c.items[uuidArray]
	if !ok {
		return nil, false
	}

	ent, ok := uuidEntries[newKey]
	if !ok {
		return nil, false
	}

	c.evictList.MoveToFront(ent)
	return ent.Value.(*entry).cachedPostings, true
}

// Remove removes the provided key from the cache, returning if the
// key was contained.
func (c *postingsListLRUShard) Remove(
	segmentUUID uuid.UUID,
	field string,
	pattern string,
	patternType PatternType,
) bool {
	c.Lock()
	defer c.Unlock()

	newKey := newKey(field, pattern, patternType)
	uuidArray := segmentUUID.Array()
	if uuidEntries, ok := c.items[uuidArray]; ok {
		if ent, ok := uuidEntries[newKey]; ok {
			c.removeElement(ent)
			return true
		}
	}

	return false
}

func (c *postingsListLRUShard) PurgeSegment(segmentUUID uuid.UUID) {
	c.Lock()
	defer c.Unlock()

	if uuidEntries, ok := c.items[segmentUUID.Array()]; ok {
		for _, ent := range uuidEntries {
			c.removeElement(ent)
		}
	}
}

// Len returns the number of items in the cache.
func (c *postingsListLRUShard) Len() int {
	c.RLock()
	defer c.RUnlock()
	return c.evictList.Len()
}

// removeOldest removes the oldest item from the cache.
func (c *postingsListLRUShard) removeOldest() {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
	}
}

// removeElement is used to remove a given list element from the cache
func (c *postingsListLRUShard) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	entry := e.Value.(*entry)

	if patterns, ok := c.items[entry.uuid.Array()]; ok {
		delete(patterns, entry.key)
		if len(patterns) == 0 {
			delete(c.items, entry.uuid.Array())
		}
	}
}

func newKey(field, pattern string, patternType PatternType) key {
	return key{field: field, pattern: pattern, patternType: patternType}
}

func hashKey(
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
