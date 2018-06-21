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

package executor

import (
	"errors"

	"github.com/m3db/m3db/src/m3ninx/doc"
	"github.com/m3db/m3db/src/m3ninx/index"
	"github.com/m3db/m3db/src/m3ninx/search"
)

var (
	errNotEnoughReaders = errors.New("searcher returned more postings lists than the number of readers")
	errTooManyReaders   = errors.New("searcher returned less postings lists than the number of readers")
)

type iterator struct {
	searcher search.Searcher
	readers  index.Readers

	idx      int
	currDoc  doc.Document
	currIter doc.Iterator

	err    error
	closed bool
}

func newIterator(s search.Searcher, rs index.Readers) (doc.Iterator, error) {
	it := &iterator{
		searcher: s,
		readers:  rs,
	}

	currIter, err := it.nextIter()
	if err != nil {
		return nil, err
	}
	it.currIter = currIter
	return it, nil
}

func (it *iterator) Next() bool {
	if it.closed || it.err != nil || it.idx == len(it.readers) {
		return false
	}

	for !it.currIter.Next() {
		// Check if the current iterator encountered an error.
		if err := it.currIter.Err(); err != nil {
			it.err = err
			return false
		}

		// Close current iterator now that we are finished with it.
		err := it.currIter.Close()
		it.currIter = nil
		if err != nil {
			it.err = err
			return false
		}

		it.idx++
		iter, err := it.nextIter()
		if err != nil {
			it.err = err
			return false
		}

		if iter == nil {
			return false
		}
		it.currIter = iter
	}

	it.currDoc = it.currIter.Current()
	return true
}

func (it *iterator) Current() doc.Document {
	return it.currDoc
}

func (it *iterator) Err() error {
	return it.err
}

func (it *iterator) Close() error {
	var err error
	if it.currIter != nil {
		err = it.currIter.Close()
	}
	return err
}

// nextIter gets the next document iterator by getting the next postings list from
// the it's searcher and then getting the documents for that postings list from the
// corresponding reader associated with that postings list. It also validates that
// the number of postings lists returned by the searcher is equal to the number of
// readers that the iterator is searching over.
func (it *iterator) nextIter() (doc.Iterator, error) {
	if !it.searcher.Next() {
		if err := it.searcher.Err(); err != nil {
			return nil, err
		}

		// Check that the Searcher hasn't returned too few postings lists.
		if it.idx != len(it.readers) {
			return nil, errTooManyReaders
		}

		return nil, nil
	}

	// Check that the Searcher hasn't returned too many postings lists.
	if it.idx == len(it.readers) {
		return nil, errNotEnoughReaders
	}

	pl := it.searcher.Current()
	iter, err := it.readers[it.idx].Docs(pl)
	if err != nil {
		return nil, err
	}
	return iter, nil
}
