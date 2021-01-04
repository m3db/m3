// Copyright (c) 2021  Uber Technologies, Inc.
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
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/postings"
)

type documentIterator struct {
	retriever    DocRetriever
	postingsIter postings.Iterator

	currDoc doc.Document
	currID  postings.ID
	closed  bool
	err     error
}

// NewIterator returns a new Iterator
func NewIterator(r DocRetriever, pi postings.Iterator) doc.Iterator {
	return &documentIterator{
		retriever:    r,
		postingsIter: pi,
	}
}

func (e *documentIterator) Next() bool {
	if e.closed || e.err != nil || !e.postingsIter.Next() {
		return false
	}
	id := e.postingsIter.Current()
	e.currID = id

	d, err := e.retriever.Doc(id)
	if err != nil {
		e.err = err
		return false
	}
	e.currDoc = d
	return true
}

func (e *documentIterator) Current() doc.Document {
	return e.currDoc
}

func (e *documentIterator) Err() error {
	return e.err
}

func (e *documentIterator) Close() error {
	if e.closed {
		return errIteratorClosed
	}
	e.closed = true
	e.currDoc = doc.Document{}
	e.currID = postings.ID(0)
	err := e.postingsIter.Close()
	return err
}
