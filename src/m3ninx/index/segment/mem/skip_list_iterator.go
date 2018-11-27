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
	"github.com/m3db/fast-skiplist"
	sgmt "github.com/m3db/m3/src/m3ninx/index/segment"
)

type skipListIter struct {
	sorted *skiplist.SkipList
	curr   *skiplist.Element
	done   bool
}

var _ sgmt.FieldsIterator = &skipListIter{}
var _ sgmt.TermsIterator = &skipListIter{}

func newSkipListIter(sorted *skiplist.SkipList) *skipListIter {
	return &skipListIter{
		sorted: sorted,
	}
}

func (b *skipListIter) Next() bool {
	if b.done {
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
	return true
}

func (b *skipListIter) Current() []byte {
	return b.curr.Key()
}

func (b *skipListIter) Err() error {
	return nil
}

func (b *skipListIter) Close() error {
	b.done = true
	b.sorted = nil
	return nil
}
