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

	"github.com/m3db/m3ninx/postings"
	"github.com/m3db/m3ninx/postings/roaring"
	"github.com/m3db/m3ninx/search"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestDisjunctionSearcher(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	// First searcher.
	firstPL1 := roaring.NewPostingsList()
	firstPL1.Insert(postings.ID(42))
	firstPL1.Insert(postings.ID(50))
	firstPL2 := roaring.NewPostingsList()
	firstPL2.Insert(postings.ID(64))
	firstSearcher := search.NewMockSearcher(mockCtrl)

	// Second searcher.
	secondPL1 := roaring.NewPostingsList()
	secondPL1.Insert(postings.ID(53))
	secondPL2 := roaring.NewPostingsList()
	secondPL2.Insert(postings.ID(64))
	secondPL2.Insert(postings.ID(72))
	secondSearcher := search.NewMockSearcher(mockCtrl)

	// Third searcher.
	thirdPL1 := roaring.NewPostingsList()
	thirdPL1.Insert(postings.ID(53))
	thirdPL2 := roaring.NewPostingsList()
	thirdPL2.Insert(postings.ID(72))
	thirdPL2.Insert(postings.ID(89))
	thirdSearcher := search.NewMockSearcher(mockCtrl)

	numReaders := 2
	gomock.InOrder(
		firstSearcher.EXPECT().NumReaders().Return(numReaders),
		secondSearcher.EXPECT().NumReaders().Return(numReaders),
		thirdSearcher.EXPECT().NumReaders().Return(numReaders),

		// Get the postings lists for the first Reader.
		firstSearcher.EXPECT().Next().Return(true),
		firstSearcher.EXPECT().Current().Return(firstPL1),
		secondSearcher.EXPECT().Next().Return(true),
		secondSearcher.EXPECT().Current().Return(secondPL1),
		thirdSearcher.EXPECT().Next().Return(true),
		thirdSearcher.EXPECT().Current().Return(thirdPL1),

		// Get the postings lists for the second Reader.
		firstSearcher.EXPECT().Next().Return(true),
		firstSearcher.EXPECT().Current().Return(firstPL2),
		secondSearcher.EXPECT().Next().Return(true),
		secondSearcher.EXPECT().Current().Return(secondPL2),
		thirdSearcher.EXPECT().Next().Return(true),
		thirdSearcher.EXPECT().Current().Return(thirdPL2),
	)

	searchers := []search.Searcher{firstSearcher, secondSearcher, thirdSearcher}

	s, err := NewDisjunctionSearcher(numReaders, searchers)
	require.NoError(t, err)

	// Ensure the searcher is searching over two readers.
	require.Equal(t, 2, s.NumReaders())

	// Test the postings list from the first Reader.
	require.True(t, s.Next())

	expected := firstPL1.Clone()
	expected.Union(secondPL1)
	expected.Union(thirdPL1)
	require.True(t, s.Current().Equal(expected))

	// Test the postings list from the second Reader.
	require.True(t, s.Next())

	expected = firstPL2.Clone()
	expected.Union(secondPL2)
	expected.Union(thirdPL2)
	require.True(t, s.Current().Equal(expected))

	require.False(t, s.Next())
	require.NoError(t, s.Err())
}
