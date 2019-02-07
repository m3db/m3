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
	"container/list"
	"errors"

	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/pborman/uuid"
)

// EvictCallback is used to get a callback when a cache entry is evicted
type EvictCallback func(uuid uuid.UUID, pattern string, postingsList postings.List)

// LRU implements a non-thread safe fixed size LRU cache
type LRU struct {
	size      int
	evictList *list.List
	// items     map[interface{}]*list.Element
	items   map[[16]byte]map[string]*list.Element
	onEvict EvictCallback
}

type cachedQuery struct {
	postingsList postings.List
}

// entry is used to hold a value in the evictList
type entry struct {
	uuid         uuid.UUID
	pattern      string
	postingsList postings.List
}

type key struct {
	uuid    uuid.UUID
	pattern string
}

// NewLRU constructs an LRU of the given size
func NewLRU(size int, onEvict EvictCallback) (*LRU, error) {
	if size <= 0 {
		return nil, errors.New("Must provide a positive size")
	}
	c := &LRU{
		size:      size,
		evictList: list.New(),
		// items:     make(map[interface{}]*list.Element),
		items:   make(map[[16]byte]map[string]*list.Element),
		onEvict: onEvict,
	}
	return c, nil
}

// Add adds a value to the cache.  Returns true if an eviction occurred.
func (c *LRU) Add(uuid uuid.UUID, pattern string, pl postings.List) (evicted bool) {
	// Check for existing item.
	if patterns, ok := c.items[uuid.Array()]; ok {
		if ent, ok := patterns[pattern]; ok {
			c.evictList.MoveToFront(ent)
			ent.Value.(*entry).postingsList = pl
			return false
		}
	}

	// Add new item.
	ent := &entry{uuid: uuid, pattern: pattern, postingsList: pl}
	entry := c.evictList.PushFront(ent)

	if patterns, ok := c.items[uuid.Array()]; ok {
		patterns[pattern] = entry
	} else {
		c.items[uuid.Array()] = map[string]*list.Element{
			pattern: entry,
		}
	}

	evict := c.evictList.Len() > c.size
	// Verify size not exceeded
	if evict {
		c.removeOldest()
	}
	return evict
}

// Get looks up a key's value from the cache.
func (c *LRU) Get(uuid uuid.UUID, pattern string) (postings.List, bool) {
	if patterns, ok := c.items[uuid.Array()]; ok {
		if ent, ok := patterns[pattern]; ok {
			c.evictList.MoveToFront(ent)
			return ent.Value.(*entry).postingsList, true
		}
	}

	return nil, false
}

// Remove removes the provided key from the cache, returning if the
// key was contained.
func (c *LRU) Remove(uuid uuid.UUID, pattern string) bool {
	if patterns, ok := c.items[uuid.Array()]; ok {
		if ent, ok := patterns[pattern]; ok {
			c.removeElement(ent)
			return true
		}
	}
	return false
}

// Len returns the number of items in the cache.
func (c *LRU) Len() int {
	return c.evictList.Len()
}

// Keys returns a slice of the keys in the cache, from oldest to newest.
func (c *LRU) keys() []key {
	keys := make([]key, len(c.items))
	i := 0
	for ent := c.evictList.Back(); ent != nil; ent = ent.Prev() {
		entry := ent.Value.(*entry)
		keys[i] = key{uuid: entry.uuid, pattern: entry.pattern}
		i++
	}
	return keys
}

// removeOldest removes the oldest item from the cache.
func (c *LRU) removeOldest() {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
	}
}

// removeElement is used to remove a given list element from the cache
func (c *LRU) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	entry := e.Value.(*entry)

	if patterns, ok := c.items[entry.uuid.Array()]; ok {
		delete(patterns, entry.pattern)
		if len(patterns) == 0 {
			delete(c.items, entry.uuid.Array())
		}
		if c.onEvict != nil {
			c.onEvict(entry.uuid, entry.pattern, entry.postingsList)
		}
	}
}
