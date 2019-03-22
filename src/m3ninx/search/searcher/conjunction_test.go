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
	"testing"

	"github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/m3db/m3/src/m3ninx/postings/roaring"
	"github.com/m3db/m3/src/m3ninx/search"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestConjunctionSearcher(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	firstReader := index.NewMockReader(mockCtrl)
	secondReader := index.NewMockReader(mockCtrl)

	// First searcher.
	firstPL1 := roaring.NewPostingsList()
	require.NoError(t, firstPL1.Insert(postings.ID(42)))
	require.NoError(t, firstPL1.Insert(postings.ID(50)))
	firstPL2 := roaring.NewPostingsList()
	require.NoError(t, firstPL2.Insert(postings.ID(64)))
	firstSearcher := search.NewMockSearcher(mockCtrl)

	// Second searcher.
	secondPL1 := roaring.NewPostingsList()
	require.NoError(t, secondPL1.Insert(postings.ID(53)))
	require.NoError(t, secondPL1.Insert(postings.ID(50)))
	secondPL2 := roaring.NewPostingsList()
	require.NoError(t, secondPL2.Insert(postings.ID(64)))
	require.NoError(t, secondPL2.Insert(postings.ID(72)))
	secondSearcher := search.NewMockSearcher(mockCtrl)

	// Third searcher.
	thirdPL1 := roaring.NewPostingsList()
	require.NoError(t, thirdPL1.Insert(postings.ID(42)))
	require.NoError(t, thirdPL1.Insert(postings.ID(53)))
	thirdPL2 := roaring.NewPostingsList()
	require.NoError(t, thirdPL2.Insert(postings.ID(64)))
	require.NoError(t, thirdPL2.Insert(postings.ID(89)))
	thirdSearcher := search.NewMockSearcher(mockCtrl)

	gomock.InOrder(
		// Get the postings lists for the first Reader.
		firstSearcher.EXPECT().Search(firstReader).Return(firstPL1, nil),
		secondSearcher.EXPECT().Search(firstReader).Return(secondPL1, nil),
		thirdSearcher.EXPECT().Search(firstReader).Return(thirdPL1, nil),

		// Get the postings lists for the second Reader.
		firstSearcher.EXPECT().Search(secondReader).Return(firstPL2, nil),
		secondSearcher.EXPECT().Search(secondReader).Return(secondPL2, nil),
		thirdSearcher.EXPECT().Search(secondReader).Return(thirdPL2, nil),
	)

	var (
		searchers = []search.Searcher{firstSearcher, secondSearcher}
		negations = []search.Searcher{thirdSearcher}
	)

	s, err := NewConjunctionSearcher(searchers, negations)
	require.NoError(t, err)

	// Test the postings list from the first Reader.
	expected := firstPL1.Clone()
	expected.Intersect(secondPL1)
	expected.Difference(thirdPL1)
	pl, err := s.Search(firstReader)
	require.NoError(t, err)
	require.True(t, pl.Equal(expected))

	// Test the postings list from the second Reader.
	expected = firstPL2.Clone()
	expected.Intersect(secondPL2)
	expected.Difference(thirdPL2)
	pl, err = s.Search(secondReader)
	require.NoError(t, err)
	require.True(t, pl.Equal(expected))
}

func TestConjunctionSearcherError(t *testing.T) {
	tests := []struct {
		name      string
		searchers search.Searchers
		negations search.Searchers
	}{
		{
			name: "empty list of searchers",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewConjunctionSearcher(test.searchers, test.negations)
			require.Error(t, err)
		})
	}
}
