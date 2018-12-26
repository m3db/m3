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

	"github.com/m3db/fast-skiplist"
	sgmt "github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/postings"
)

var (
	errSkipListValueNotPostingsList = errors.New("skip list element value is not a postings list")
)

type skipListTermsIter struct {
	sorted   *skiplist.SkipList
	curr     *skiplist.Element
	postings postings.List
	done     bool
	err      error
}

var _ sgmt.TermsIterator = &skipListTermsIter{}

func newSkipListTermsIter(sorted *skiplist.SkipList) *skipListTermsIter {
	return &skipListTermsIter{
		sorted: sorted,
	}
}

func (b *skipListTermsIter) Next() bool {
	if b.done || b.err != nil {
		return false
	}

	var next *skiplist.Element
	if b.curr == nil {
		next = b.sorted.Front()
	} else {
		next = b.curr.Next()
	}

	if next == nil {
		b.done = true
		return false
	}

	b.curr = next

	var ok bool
	b.postings, ok = b.curr.Value().(postings.List)
	if !ok {
		b.err = errSkipListValueNotPostingsList
		return false
	}

	return true
}

func (b *skipListTermsIter) Current() ([]byte, postings.List) {
	return b.curr.Key(), b.postings
}

func (b *skipListTermsIter) Err() error {
	return b.err
}

func (b *skipListTermsIter) Close() error {
	b.done = true
	b.sorted = nil
	return nil
}
