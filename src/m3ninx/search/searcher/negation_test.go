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

func TestNegationSearcher(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	// First reader.
	readerFirstPL := roaring.NewPostingsList()
	require.NoError(t, readerFirstPL.AddRange(postings.ID(42), postings.ID(48)))
	firstReader := index.NewMockReader(mockCtrl)

	// Second reader.
	readerSecondPL := roaring.NewPostingsList()
	require.NoError(t, readerSecondPL.AddRange(postings.ID(48), postings.ID(52)))
	secondReader := index.NewMockReader(mockCtrl)

	// Mock searcher.
	searcherFirstPL := roaring.NewPostingsList()
	require.NoError(t, searcherFirstPL.Insert(postings.ID(42)))
	require.NoError(t, searcherFirstPL.Insert(postings.ID(44)))
	require.NoError(t, searcherFirstPL.Insert(postings.ID(45)))
	searcherSecondPL := roaring.NewPostingsList()
	require.NoError(t, searcherSecondPL.Insert(postings.ID(51)))
	require.NoError(t, searcherSecondPL.Insert(postings.ID(52)))
	searcher := search.NewMockSearcher(mockCtrl)

	gomock.InOrder(
		firstReader.EXPECT().MatchAll().Return(readerFirstPL, nil),
		searcher.EXPECT().Search(firstReader).Return(searcherFirstPL, nil),
		secondReader.EXPECT().MatchAll().Return(readerSecondPL, nil),
		searcher.EXPECT().Search(secondReader).Return(searcherSecondPL, nil),
	)

	s, err := NewNegationSearcher(searcher)
	require.NoError(t, err)

	// Test the postings list from the first Reader.
	expected := readerFirstPL.Clone()
	expected.Difference(searcherFirstPL)
	pl, err := s.Search(firstReader)
	require.NoError(t, err)
	require.True(t, pl.Equal(expected))

	// Test the postings list from the second Reader.
	require.NoError(t, err)
	expected = readerSecondPL.Clone()
	expected.Difference(searcherSecondPL)
	pl, err = s.Search(secondReader)
	require.NoError(t, err)
	require.True(t, pl.Equal(expected))
}
