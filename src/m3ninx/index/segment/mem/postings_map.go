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
	"math"
	"regexp"
	"sync"

	"github.com/m3db/m3ninx/postings"
)

const (
	regexpMatchFactor = 0.01
	regexMatchMaxLen  = 1024.0
)

// postingsMap maps a byte slice to a postings list of all documents associated with
// that byte slice.
type postingsMap struct {
	sync.RWMutex

	opts Options

	// TODO: as noted in https://github.com/m3db/m3ninx/issues/11, evalute impact of using
	// a custom hash map where we can avoid using string keys, both to save allocs and
	// help perm.
	postings map[string]postings.MutableList
}

func newPostingsMap(opts Options) *postingsMap {
	return &postingsMap{
		opts:     opts,
		postings: make(map[string]postings.MutableList),
	}
}

func (m *postingsMap) addID(key []byte, id postings.ID) error {
	keyStr := string(key)

	// Try read lock to see if we already have a postings list for the given value.
	m.RLock()
	p, ok := m.postings[keyStr]
	m.RUnlock()

	// We have a postings list, insert the ID and move on.
	if ok {
		return p.Insert(id)
	}

	// A corresponding postings list doesn't exist, time to acquire write lock.
	m.Lock()
	p, ok = m.postings[keyStr]

	// Check if the corresponding postings list has been created since we released lock.
	if ok {
		m.Unlock()
		return p.Insert(id)
	}

	// Create a new posting list for the term, and insert into fieldValues.
	p = m.opts.PostingsListPool().Get()
	m.postings[keyStr] = p
	m.Unlock()
	return p.Insert(id)
}

func (m *postingsMap) get(key []byte) postings.List {
	m.RLock()
	p := m.postings[string(key)]
	m.RUnlock()
	if p == nil {
		p = m.opts.PostingsListPool().Get()
	}
	return p
}

func (m *postingsMap) getRegex(re *regexp.Regexp) []postings.List {
	m.RLock()

	initLen := math.Min(regexpMatchFactor*float64(len(m.postings)), regexMatchMaxLen)
	ps := make([]postings.List, 0, int(initLen))

	for key, p := range m.postings {
		// TODO: Evaluate lock contention caused by holding on to the read lock while
		// evaluating this predicate.
		// TODO: Evaluate if performing a prefix match would speed up the common case.
		if re.MatchString(key) {
			ps = append(ps, p)
		}
	}

	m.RUnlock()
	return ps
}
