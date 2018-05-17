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
	"github.com/m3db/m3ninx/postings"
	"github.com/m3db/m3ninx/search"
)

type conjunctionSearcher struct {
	searchers  search.Searchers
	negations  search.Searchers
	numReaders int

	idx  int
	curr postings.List
	err  error
}

// NewConjunctionSearcher returns a new Searcher which matches documents which match each
// of the given searchers and none of the negations. It is not safe for concurrent access.
func NewConjunctionSearcher(numReaders int, searchers, negations search.Searchers) (search.Searcher, error) {
	if len(searchers) == 0 {
		return nil, errEmptySearchers
	}

	if err := validateSearchers(numReaders, searchers); err != nil {
		return nil, err
	}
	if err := validateSearchers(numReaders, negations); err != nil {
		return nil, err
	}

	return &conjunctionSearcher{
		searchers:  searchers,
		negations:  negations,
		numReaders: numReaders,
		idx:        -1,
	}, nil
}

func (s *conjunctionSearcher) Next() bool {
	if s.err != nil || s.idx == s.numReaders-1 {
		return false
	}

	var pl postings.MutableList
	s.idx++
	for _, sr := range s.searchers {
		if !sr.Next() {
			err := sr.Err()
			if err == nil {
				err = errSearcherTooShort
			}
			s.err = err
			return false
		}
		curr := sr.Current()

		// TODO: Sort the iterators so that we take the intersection in order of increasing size.
		if pl == nil {
			pl = curr.Clone()
		} else {
			pl.Intersect(curr)
		}

		// We can break early if the interescted postings list is ever empty.
		if pl.IsEmpty() {
			break
		}
	}

	for _, sr := range s.negations {
		if !sr.Next() {
			err := sr.Err()
			if err == nil {
				err = errSearcherTooShort
			}
			s.err = err
			return false
		}
		curr := sr.Current()

		// TODO: Sort the iterators so that we take the set differences in order of decreasing size.
		pl.Difference(curr)

		// We can break early if the interescted postings list is ever empty.
		if pl.IsEmpty() {
			break
		}
	}

	s.curr = pl

	return true
}

func (s *conjunctionSearcher) Current() postings.List {
	return s.curr
}

func (s *conjunctionSearcher) Err() error {
	return s.err
}

func (s *conjunctionSearcher) NumReaders() int {
	return s.numReaders
}
