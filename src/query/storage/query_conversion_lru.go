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

package storage

import (
	"container/list"
	"fmt"
	"sync"

	"github.com/m3db/m3/src/m3ninx/idx"
)

// QueryConversionLRU implements a fixed size LRU cache
type QueryConversionLRU struct {
	sync.Mutex

	size      int
	evictList *list.List
	items     map[interface{}]*list.Element
}

// entry is used to hold a value in the evictList
type entry struct {
	key   string
	value idx.Query
}

// NewQueryConversionLRU constructs an LRU of the given size
func NewQueryConversionLRU(size int) (*QueryConversionLRU, error) {
	if size <= 0 {
		return nil, fmt.Errorf("must provide a positive size, instead got: %d", size)
	}

	c := &QueryConversionLRU{
		size:      size,
		evictList: list.New(),
		items:     make(map[interface{}]*list.Element),
	}

	return c, nil
}

// AddWithLock adds a value to the cache. Returns true if an eviction occurred.
func (c *QueryConversionLRU) AddWithLock(key string, value idx.Query) (evicted bool) {
	// Check for existing item
	c.Lock()
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		ent.Value.(*entry).value = value
		c.Unlock()
		return false
	}

	// Add new item
	ent := &entry{key, value}
	entry := c.evictList.PushFront(ent)
	c.items[key] = entry

	evict := c.evictList.Len() > c.size
	// Verify size not exceeded
	if evict {
		c.removeOldest()
	}

	c.Unlock()
	return evict
}

// GetWithLock looks up a key's value from the cache.
func (c *QueryConversionLRU) GetWithLock(key string) (value idx.Query, ok bool) {
	c.Lock()
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		c.Unlock()
		return ent.Value.(*entry).value, true
	}

	c.Unlock()
	return
}

// removeOldest removes the oldest item from the cache.
func (c *QueryConversionLRU) removeOldest() {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
	}
}

// removeElement is used to remove a given list element from the cache
func (c *QueryConversionLRU) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	kv := e.Value.(*entry)
	delete(c.items, kv.key)
}
