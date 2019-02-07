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

	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/pborman/uuid"
)

// PostingsListLRU implements a non-thread safe fixed size LRU cache of postings lists
// that were resolved by running a given query against a particular segment. Normally
// an key in the LRU would look like:
//
// type key struct {
//    segmentUUID uuid.UUID
//    pattern     string
//    patternType PatternType
// }
//
// However, some of the postings lists that we will store in the LRU have a fixed lifecycle
// because they reference mmap'd byte slices which will eventually be unmap'd. To prevent
// these postings lists that point to unmap'd regions from remaining in the LRU, we want to
// support the ability to efficiently purge the LRU of any postings list that belong to a
// given segment.
//
// Instead of adding additional tracking on-top of an existing generic LRU, we've created a
// specialized LRU that instead of having a single top-level map pointing into the linked-list,
// has a two-level map where the top level map is keyed by segment UUID and the second level map
// is keyed by pattern and pattern type.
//
// As a result, when a segment is ready to be closed, they can call into the cache with their
// UUID and we can efficiently remove all the entries corresponding to that segment from the
// LRU. The specialization has the additional nice property that we don't need to allocate everytime
// we add an item to the LRU due to the interface{} conversion.
type postingsListLRU struct {
	size      int
	evictList *list.List
	items     map[[16]byte]map[patternAndPatternType]*list.Element
}

type cachedQuery struct {
	postingsList postings.List
}

// entry is used to hold a value in the evictList
type entry struct {
	uuid         uuid.UUID
	pattern      string
	patternType  PatternType
	postingsList postings.List
}

type key struct {
	uuid        uuid.UUID
	pattern     string
	patternType PatternType
}

type patternAndPatternType struct {
	pattern     string
	patternType PatternType
}

// newPostingsListLRU constructs an LRU of the given size.
func newPostingsListLRU(size int) (*postingsListLRU, error) {
	if size <= 0 {
		return nil, errors.New("Must provide a positive size")
	}

	return &postingsListLRU{
		size:      size,
		evictList: list.New(),
		items:     make(map[[16]byte]map[patternAndPatternType]*list.Element),
	}, nil
}

// Add adds a value to the cache.  Returns true if an eviction occurred.
func (c *postingsListLRU) Add(uuid uuid.UUID, pattern string, patternType PatternType, pl postings.List) (evicted bool) {
	// Check for existing item.
	if uuidEntries, ok := c.items[uuid.Array()]; ok {
		key := newPatternAndPatternType(pattern, patternType)
		if ent, ok := uuidEntries[key]; ok {
			c.evictList.MoveToFront(ent)
			ent.Value.(*entry).postingsList = pl
			return false
		}
	}

	// Add new item.
	var (
		ent   = &entry{uuid: uuid, pattern: pattern, patternType: patternType, postingsList: pl}
		entry = c.evictList.PushFront(ent)
		key   = newPatternAndPatternType(pattern, patternType)
	)
	if patterns, ok := c.items[uuid.Array()]; ok {
		patterns[key] = entry
	} else {
		c.items[uuid.Array()] = map[patternAndPatternType]*list.Element{
			key: entry,
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
func (c *postingsListLRU) Get(uuid uuid.UUID, pattern string, patternType PatternType) (postings.List, bool) {
	if uuidEntries, ok := c.items[uuid.Array()]; ok {
		key := newPatternAndPatternType(pattern, patternType)
		if ent, ok := uuidEntries[key]; ok {
			c.evictList.MoveToFront(ent)
			return ent.Value.(*entry).postingsList, true
		}
	}

	return nil, false
}

// Remove removes the provided key from the cache, returning if the
// key was contained.
func (c *postingsListLRU) Remove(segmentUUID uuid.UUID, pattern string, patternType PatternType) bool {
	if uuidEntries, ok := c.items[segmentUUID.Array()]; ok {
		key := newPatternAndPatternType(pattern, patternType)
		if ent, ok := uuidEntries[key]; ok {
			c.removeElement(ent)
			return true
		}
	}
	return false
}

func (c *postingsListLRU) PurgeSegment(segmentUUID uuid.UUID) {
	if uuidEntries, ok := c.items[segmentUUID.Array()]; ok {
		for _, ent := range uuidEntries {
			c.removeElement(ent)
		}
	}
}

// Len returns the number of items in the cache.
func (c *postingsListLRU) Len() int {
	return c.evictList.Len()
}

// Keys returns a slice of the keys in the cache, from oldest to newest. Mostly
// used for testing.
func (c *postingsListLRU) keys() []key {
	keys := make([]key, 0, len(c.items))
	for ent := c.evictList.Back(); ent != nil; ent = ent.Prev() {
		entry := ent.Value.(*entry)
		keys = append(keys, key{
			uuid:        entry.uuid,
			pattern:     entry.pattern,
			patternType: entry.patternType,
		})
	}
	return keys
}

// removeOldest removes the oldest item from the cache.
func (c *postingsListLRU) removeOldest() {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
	}
}

// removeElement is used to remove a given list element from the cache
func (c *postingsListLRU) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	entry := e.Value.(*entry)

	if patterns, ok := c.items[entry.uuid.Array()]; ok {
		key := newPatternAndPatternType(entry.pattern, entry.patternType)
		delete(patterns, key)
		if len(patterns) == 0 {
			delete(c.items, entry.uuid.Array())
		}
	}
}

func newPatternAndPatternType(pattern string, patternType PatternType) patternAndPatternType {
	return patternAndPatternType{pattern: pattern, patternType: patternType}
}
