// Copyright (c) 2020 Uber Technologies, Inc.
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

package builder

import (
	"bytes"

	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/twotwotwo/sorts"
)

type terms struct {
	opts                Options
	pool                postings.Pool
	poolLocal           []postings.MutableList
	postings            *PostingsMap
	postingsListUnion   postings.MutableList
	uniqueTerms         []termElem
	uniqueTermsIsSorted bool
}

type termElem struct {
	term     []byte
	postings postings.List
}

func newTerms(opts Options) *terms {
	pool := opts.PostingsListPool()
	return &terms{
		opts:              opts,
		pool:              pool,
		postingsListUnion: pool.Get(),
		postings:          NewPostingsMap(PostingsMapOptions{}),
	}
}

func (t *terms) size() int {
	return len(t.uniqueTerms)
}

func (t *terms) poolGet() postings.MutableList {
	if len(t.poolLocal) == 0 {
		return t.pool.Get()
	}

	last := len(t.poolLocal) - 1
	elem := t.poolLocal[last]
	t.poolLocal = t.poolLocal[:last]

	return elem
}

func (t *terms) poolPut(v postings.MutableList) {
	v.Reset()
	t.poolLocal = append(t.poolLocal, v)
}

func (t *terms) post(term []byte, id postings.ID, opts indexJobEntryOptions) error {
	postingsList, exists := t.postings.Get(term)
	if !exists {
		postingsList = t.poolGet()
		postingsList.Reset()
		t.postings.SetUnsafe(term, postingsList, PostingsMapSetUnsafeOptions{
			NoCopyKey:     true,
			NoFinalizeKey: true,
		})
	}

	if !opts.graphitePathNode && !opts.graphitePathLeaf {
		// NB(rob): For graphite path indexing we don't actually associate
		// the timeseries that are associated to the actual graphite path
		// since this adds a lot of indexing pressure.
		// The graphite paths that are indexed are purely only used for find lookups
		// so we don't need to actually need the correct postings list for each term.
		if err := postingsList.Insert(id); err != nil {
			return err
		}
		if err := t.postingsListUnion.Insert(id); err != nil {
			return err
		}
	}

	// If new postings list, track insertion of this key into the terms
	// collection for correct response when retrieving all terms.
	newTerm := !exists
	if newTerm {
		t.uniqueTerms = append(t.uniqueTerms, termElem{
			term:     term,
			postings: postingsList,
		})
		t.uniqueTermsIsSorted = false
	}
	return nil
}

// nolint: unused
func (t *terms) get(term []byte) (postings.List, bool) {
	value, ok := t.postings.Get(term)
	return value, ok
}

func (t *terms) sortIfRequired() {
	if t.uniqueTermsIsSorted {
		return
	}

	// NB(r): See SetSortConcurrency why this RLock is required.
	sortConcurrencyLock.RLock()
	sorts.ByBytes(t)
	sortConcurrencyLock.RUnlock()

	t.uniqueTermsIsSorted = true
}

func (t *terms) reset() {
	// Keep postings map lookup, return postings lists to pool
	for _, entry := range t.postings.Iter() {
		t.poolPut(entry.Value())
	}
	t.postings.Reset()
	t.postingsListUnion.Reset()

	// Reset the unique terms slice
	var emptyTerm termElem
	for i := range t.uniqueTerms {
		t.uniqueTerms[i] = emptyTerm
	}
	t.uniqueTerms = t.uniqueTerms[:0]
	t.uniqueTermsIsSorted = false
}

func (t *terms) Len() int {
	return len(t.uniqueTerms)
}

func (t *terms) Less(i, j int) bool {
	return bytes.Compare(t.uniqueTerms[i].term, t.uniqueTerms[j].term) < 0
}

func (t *terms) Swap(i, j int) {
	t.uniqueTerms[i], t.uniqueTerms[j] = t.uniqueTerms[j], t.uniqueTerms[i]
}

func (t *terms) Key(i int) []byte {
	return t.uniqueTerms[i].term
}
