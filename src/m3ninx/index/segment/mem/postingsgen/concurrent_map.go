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

package postingsgen

import (
	"bytes"
	"regexp"
	"sync"

	"github.com/m3db/m3ninx/postings"

	"github.com/cespare/xxhash"
)

// ConcurrentMap is a thread-safe map from []byte -> postings.List.
type ConcurrentMap struct {
	sync.RWMutex
	internalMap *Map

	opts ConcurrentMapOpts
}

// ConcurrentMapOpts are the options by which to create a new
// ConcurrentMap.
type ConcurrentMapOpts struct {
	InitialSize      int
	PostingsListPool postings.Pool
}

// NewConcurrentMap returns a new thread-safe map from []byte -> postings.List.
func NewConcurrentMap(opts ConcurrentMapOpts) *ConcurrentMap {
	return &ConcurrentMap{
		internalMap: newInternalMap(opts.InitialSize),
		opts:        opts,
	}
}

// Add adds the provided `id` to the postings.List backing `key`.
func (m *ConcurrentMap) Add(key []byte, id postings.ID) {
	// Try read lock to see if we already have a postings list for the given value.
	m.RLock()
	p, ok := m.internalMap.Get(key)
	m.RUnlock()

	// We have a postings list, insert the ID and move on.
	if ok {
		p.Insert(id)
		return
	}

	// A corresponding postings list doesn't exist, time to acquire write lock.
	m.Lock()
	p, ok = m.internalMap.Get(key)

	// Check if the corresponding postings list has been created since we released lock.
	if ok {
		m.Unlock()
		p.Insert(id)
		return
	}

	// Create a new posting list for the term, and insert into fieldValues.
	p = m.opts.PostingsListPool.Get()
	m.internalMap.SetUnsafe(key, p, SetUnsafeOptions{
		NoCopyKey:     true,
		NoFinalizeKey: true,
	})
	m.Unlock()
	p.Insert(id)
}

// Get returns the postings.List backing `key`.
func (m *ConcurrentMap) Get(key []byte) (postings.List, bool) {
	m.RLock()
	p, ok := m.internalMap.Get(key)
	m.RUnlock()
	if ok {
		return p, true
	}
	return nil, false
}

// GetRegex returns the union of the postings lists whose keys match the
// provided regexp.
func (m *ConcurrentMap) GetRegex(re *regexp.Regexp) (postings.List, bool) {
	var pl postings.MutableList

	m.RLock()
	for _, mapEntry := range m.internalMap.Iter() {
		// TODO: Evaluate lock contention caused by holding on to the read lock while
		// evaluating this predicate.
		// TODO: Evaluate if performing a prefix match would speed up the common case.
		if re.Match(mapEntry.Key()) {
			if pl == nil {
				pl = mapEntry.Value().Clone()
			} else {
				pl.Union(mapEntry.Value())
			}
		}
	}
	m.RUnlock()

	if pl == nil {
		return nil, false
	}
	return pl, true
}

// newInternalMap returns a new []bytes->postings.MutableList map.
func newInternalMap(initialSize int) *Map {
	return newMap(mapOptions{
		hash: func(k []byte) MapHash {
			return MapHash(xxhash.Sum64(k))
		},
		equals:      bytes.Equal,
		copy:        undefinedCopyFn,
		finalize:    undefinedFinalizeFn,
		initialSize: initialSize,
	})
}

var undefinedCopyFn CopyFn = func([]byte) []byte {
	// NB: intentionally not defined to force users of the map to not
	// allocate extra copies.
	panic("not implemented")
}

var undefinedFinalizeFn FinalizeFn = func([]byte) {
	// NB: intentionally not defined to force users of the map to not
	// allocate extra copies.
	panic("not implemented")
}
