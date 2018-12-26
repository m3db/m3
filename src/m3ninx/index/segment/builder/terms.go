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

package builder

import (
	"github.com/m3db/m3/src/m3ninx/postings"
)

type terms struct {
	opts        Options
	pool        postings.Pool
	postings    *PostingsMap
	uniqueTerms []termElem
}

type termElem struct {
	term     []byte
	postings postings.List
}

func newTerms(opts Options) *terms {
	return &terms{
		opts:     opts,
		pool:     opts.PostingsListPool(),
		postings: NewPostingsMap(PostingsMapOptions{}),
	}
}

func (t *terms) size() int {
	return len(t.uniqueTerms)
}

func (t *terms) post(term []byte, id postings.ID) error {
	postingsList, ok := t.postings.Get(term)
	if !ok {
		postingsList = t.pool.Get()
		t.postings.SetUnsafe(term, postingsList, PostingsMapSetUnsafeOptions{
			NoCopyKey:     true,
			NoFinalizeKey: true,
		})
	}

	// If empty posting list, track insertion of this key into the terms
	// collection for correct response when retrieving all terms
	newTerm := postingsList.Len() == 0
	if err := postingsList.Insert(id); err != nil {
		return err
	}
	if newTerm {
		t.uniqueTerms = append(t.uniqueTerms, termElem{
			term:     term,
			postings: postingsList,
		})
	}
	return nil
}

func (t *terms) get(term []byte) (postings.List, bool) {
	value, ok := t.postings.Get(term)
	return value, ok
}

func (t *terms) reset() {
	// Keep postings around, just reset the list for each term
	for _, entry := range t.postings.Iter() {
		entry.Value().Reset()
	}

	// Reset the unique terms slice
	var emptyTerm termElem
	for i := range t.uniqueTerms {
		t.uniqueTerms[i] = emptyTerm
	}
	t.uniqueTerms = t.uniqueTerms[:0]
}
