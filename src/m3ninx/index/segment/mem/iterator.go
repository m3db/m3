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
	"errors"

	"github.com/m3db/m3ninx/doc"
	"github.com/m3db/m3ninx/postings"
)

var (
	errIteratorClosed = errors.New("iterator has been closed")
)

// NB: the ordering below is to minimize size of the underlying struct.
type iterator struct {
	// immutable fields
	segment      ReadableSegment
	postingsIter postings.Iterator

	// mutable fields
	current doc.Document
	err     error
	closed  bool

	// immutable fields
	maxID postings.ID
}

func newIterator(s ReadableSegment, pi postings.Iterator, maxID postings.ID) doc.Iterator {
	return &iterator{
		segment:      s,
		postingsIter: pi,
		maxID:        maxID,
	}
}

func (it *iterator) Next() bool {
	if it.closed || it.err != nil || !it.postingsIter.Next() {
		return false
	}
	id := it.postingsIter.Current()
	if id > it.maxID {
		return false
	}

	d, err := it.segment.getDoc(id)
	if err != nil {
		it.err = err
		return false
	}
	it.current = d
	return true
}

func (it *iterator) Current() doc.Document {
	return it.current
}

func (it *iterator) Err() error {
	return it.err
}

func (it *iterator) Close() error {
	if it.closed {
		return errIteratorClosed
	}
	it.closed = true
	it.current = doc.Document{}
	err := it.postingsIter.Close()
	return err
}
