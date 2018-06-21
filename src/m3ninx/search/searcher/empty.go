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

package searcher

import (
	"github.com/m3db/m3db/src/m3ninx/postings"
	"github.com/m3db/m3db/src/m3ninx/postings/roaring"
	"github.com/m3db/m3db/src/m3ninx/search"
)

type emptySearcher struct {
	numReaders int
	postings   postings.List

	idx int
	err error
}

// NewEmptySearcher returns a new searcher which always returns an empty postings list.
// It is not safe for concurrent access.
func NewEmptySearcher(numReaders int) search.Searcher {
	return &emptySearcher{
		numReaders: numReaders,
		postings:   roaring.NewPostingsList(),
		idx:        -1,
	}
}

func (s *emptySearcher) Next() bool {
	if s.idx == s.numReaders-1 {
		return false
	}
	s.idx++
	return true
}

func (s *emptySearcher) Current() postings.List {
	return s.postings
}

func (s *emptySearcher) Err() error {
	return s.err
}

func (s *emptySearcher) NumReaders() int {
	return s.numReaders
}
