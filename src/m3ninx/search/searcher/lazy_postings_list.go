// Copyright (c) 2020 Uber Technologies, Inc.
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
	"errors"
	"sort"

	"github.com/m3db/m3/src/m3ninx/postings"
)

var (
	errNoPostingsLists = errors.New("no postings lists")
)

var _ postings.Iterator = (*intersectAndNegatePostingsListIter)(nil)

type intersectAndNegatePostingsListIter struct {
	smallestIntersectIter    postings.Iterator
	nonSmallestIntersectsAsc []postings.List
	negationsDesc            []postings.List
	current                  postings.ID
}

func newIntersectAndNegatePostingsListIter(
	intersects []postings.List,
	negations []postings.List,
) (postings.Iterator, error) {
	if len(intersects) == 0 {
		return nil, errNoPostingsLists
	}

	// Always intersect using the smallest at top so it can
	// directly compare if intersected with other results from
	// other lists.
	sort.Slice(intersects, func(i, j int) bool {
		return intersects[i].Len() < intersects[j].Len()
	})
	sort.Slice(negations, func(i, j int) bool {
		return negations[i].Len() > negations[j].Len()
	})
	return &intersectAndNegatePostingsListIter{
		smallestIntersectIter:    intersects[0].Iterator(),
		nonSmallestIntersectsAsc: intersects[1:],
		negationsDesc:            negations,
		current:                  postings.MaxID,
	}, nil
}

func (it *intersectAndNegatePostingsListIter) Current() postings.ID {
	return it.current
}

func (it *intersectAndNegatePostingsListIter) Next() bool {
NextValue:
	for {
		if !it.smallestIntersectIter.Next() {
			return false
		}
		curr := it.smallestIntersectIter.Current()
		for _, list := range it.nonSmallestIntersectsAsc {
			if !list.Contains(curr) {
				continue NextValue
			}
		}
		for _, list := range it.negationsDesc {
			if list.Contains(curr) {
				continue NextValue
			}
		}
		it.current = curr
		return true
	}
}

func (it *intersectAndNegatePostingsListIter) Err() error {
	return nil
}

func (it *intersectAndNegatePostingsListIter) Close() error {
	return nil
}
