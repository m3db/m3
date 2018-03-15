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
	"github.com/m3db/m3ninx/index"
	"github.com/m3db/m3ninx/postings"
	"github.com/m3db/m3ninx/search"
)

type termSearcher struct {
	field, term []byte
	readers     index.Readers

	idx  int
	curr postings.List
	err  error
}

// NewTermSearcher returns a new searcher for finding documents which match the given term.
// It is not safe for concurrent access.
func NewTermSearcher(rs index.Readers, field, term []byte) search.Searcher {
	return &termSearcher{
		field:   field,
		term:    term,
		readers: rs,
		idx:     -1,
	}
}

func (s *termSearcher) Next() bool {
	if s.err != nil || s.idx == len(s.readers)-1 {
		return false
	}

	s.idx++
	r := s.readers[s.idx]
	pl, err := r.MatchTerm(s.field, s.term)
	if err != nil {
		s.err = err
		return false
	}
	s.curr = pl

	return true
}

func (s *termSearcher) Current() postings.List {
	return s.curr
}

func (s *termSearcher) Err() error {
	return s.err
}

func (s *termSearcher) NumReaders() int {
	return len(s.readers)
}
