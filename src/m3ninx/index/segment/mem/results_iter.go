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

package mem

import (
	"fmt"

	"github.com/m3db/m3ninx/doc"
	"github.com/m3db/m3ninx/index/segment"
)

type resultsIter struct {
	current document
	next    document
	hasNext bool
	done    bool
	err     error

	predicateFn  matchPredicate
	postingsList segment.ImmutablePostingsList
	idsIter      segment.PostingsIter
	queryable    queryable
}

func newResultsIter(
	postingsList segment.ImmutablePostingsList,
	predicateFn matchPredicate,
	q queryable,
) segment.ResultsIter {
	// return empty iterator if we were given an empty postings list
	if postingsList == nil {
		return &resultsIter{
			done: true,
		}
	}

	it := &resultsIter{
		predicateFn:  predicateFn,
		postingsList: postingsList,
		idsIter:      postingsList.Iter(),
		queryable:    q,
	}
	it.setupNextIteration()
	return it
}

func (r *resultsIter) setupNextIteration() {
	d, ok, err := r.getNextDocument()
	if err != nil || !ok {
		r.release()
		r.hasNext = false
		r.err = err
		r.done = true
		return
	}

	// setup values so that next time Next() is called, we can rotate
	// next -> current
	r.next = d
	r.hasNext = true
}

func (r *resultsIter) release() {
	// need this cast because we're provided an immutable postings lists
	// and we pool mutable postings lists
	pl, ok := r.postingsList.(segment.PostingsList)
	if ok {
		r.queryable.Options().PostingsListPool().Put(pl)
		// unset internal references
		r.postingsList = nil
		r.idsIter = nil
	}
}

func (r *resultsIter) getNextDocument() (document, bool, error) {
	// need to loop as we may filter out values due to the predicate function
	for {
		hasNext := r.idsIter.Next()
		if !hasNext {
			return document{}, false, nil
		}

		id := r.idsIter.Current()
		doc, err := r.queryable.FetchDocument(id)
		if err != nil {
			return document{}, false, fmt.Errorf("unable to retrieve document with DocID: %d, err: %v", id, err)
		}

		// ensure document matches predicateFn
		if r.predicateFn(doc.Document) {
			return doc, true, nil
		}
	}
}

func (r *resultsIter) Next() bool {
	// ensure internal state is valid, we have another value queued
	valid := !r.done && r.err == nil
	remaining := r.hasNext
	if !valid || !remaining {
		return false
	}

	// i.e. we have another value, indicate that, and queue next
	r.current = r.next
	r.setupNextIteration()
	return true
}

func (r *resultsIter) Current() (doc.Document, bool) {
	return r.current.Document, r.current.tombstoned
}

func (r *resultsIter) Err() error {
	return r.err
}
