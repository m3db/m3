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

package mem

import (
	"regexp"
	"sync"

	"github.com/m3db/fast-skiplist"
	"github.com/m3db/m3/src/m3ninx/postings"
)

// concurrentPostingsMap is a thread-safe map from []byte -> postings.List.
type concurrentPostingsMap struct {
	sync.RWMutex
	*postingsMap
	sorted *skiplist.SkipList

	opts Options
}

// newConcurrentPostingsMap returns a new thread-safe map from []byte -> postings.List.
func newConcurrentPostingsMap(opts Options) *concurrentPostingsMap {
	return &concurrentPostingsMap{
		postingsMap: newPostingsMap(opts.InitialCapacity()),
		sorted:      skiplist.New(),
		opts:        opts,
	}
}

// Add adds the provided `id` to the postings.List backing `key`.
func (m *concurrentPostingsMap) Add(key []byte, id postings.ID) {
	// Try read lock to see if we already have a postings list for the given value.
	m.RLock()
	p, ok := m.postingsMap.Get(key)
	m.RUnlock()

	// We have a postings list, insert the ID and move on.
	if ok {
		p.Insert(id)
		return
	}

	// A corresponding postings list doesn't exist, time to acquire write lock.
	m.Lock()
	p, ok = m.postingsMap.Get(key)

	// Check if the corresponding postings list has been created since we released lock.
	if ok {
		m.Unlock()
		p.Insert(id)
		return
	}

	// Create a new posting list for the term, and insert into fieldValues.
	p = m.opts.PostingsListPool().Get()
	m.postingsMap.SetUnsafe(key, p, postingsMapSetUnsafeOptions{
		NoCopyKey:     true,
		NoFinalizeKey: true,
	})
	m.sorted.Set(key, p)
	m.Unlock()
	p.Insert(id)
}

// Keys returns the keys known to the map.
func (m *concurrentPostingsMap) Keys() *skipListIter {
	// NB(r): Skip list is thread safe, no need to grab any locks
	return newSkipListIter(m.sorted)
}

// Get returns the postings.List backing `key`.
func (m *concurrentPostingsMap) Get(key []byte) (postings.List, bool) {
	m.RLock()
	p, ok := m.postingsMap.Get(key)
	m.RUnlock()
	if ok {
		return p, true
	}
	return nil, false
}

// GetRegex returns the union of the postings lists whose keys match the
// provided regexp.
func (m *concurrentPostingsMap) GetRegex(re *regexp.Regexp) (postings.List, bool) {
	var pl postings.MutableList

	// NB(r): Skip list is thread safe, no need to grab any locks
	front := m.sorted.Front()
	if front == nil {
		return nil, false
	}

	for elem := front; elem != nil; elem = elem.Next() {
		// TODO: Evaluate if performing a prefix match would speed up the common case.
		if re.Match(elem.Key()) {
			value := elem.Value().(postings.List)
			if pl == nil {
				pl = value.Clone()
			} else {
				pl.Union(value)
			}
		}
	}

	if pl == nil {
		return nil, false
	}
	return pl, true
}

func (m *concurrentPostingsMap) Reset() {
	m.Lock()
	m.postingsMap.Reset()
	m.Unlock()
}
