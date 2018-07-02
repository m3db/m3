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

	"github.com/m3db/m3db/src/m3ninx/index"
	"github.com/m3db/m3db/src/m3ninx/postings"
	"github.com/m3db/m3db/src/m3ninx/postings/roaring"
	"github.com/m3db/m3db/src/m3ninx/search"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestNegationSearcher(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	// First reader.
	readerFirstPL := roaring.NewPostingsList()
	readerFirstPL.AddRange(postings.ID(42), postings.ID(48))
	firstReader := index.NewMockReader(mockCtrl)

	// Second reader.
	readerSecondPL := roaring.NewPostingsList()
	readerSecondPL.AddRange(postings.ID(48), postings.ID(52))
	secondReader := index.NewMockReader(mockCtrl)

	// Mock searcher.
	searcherFirstPL := roaring.NewPostingsList()
	searcherFirstPL.Insert(postings.ID(42))
	searcherFirstPL.Insert(postings.ID(44))
	searcherFirstPL.Insert(postings.ID(45))
	searcherSecondPL := roaring.NewPostingsList()
	searcherSecondPL.Insert(postings.ID(51))
	searcherSecondPL.Insert(postings.ID(52))
	searcher := search.NewMockSearcher(mockCtrl)

	gomock.InOrder(
		searcher.EXPECT().NumReaders().Return(2),

		searcher.EXPECT().Next().Return(true),
		firstReader.EXPECT().MatchAll().Return(readerFirstPL, nil),
		searcher.EXPECT().Current().Return(searcherFirstPL),

		searcher.EXPECT().Next().Return(true),
		secondReader.EXPECT().MatchAll().Return(readerSecondPL, nil),
		searcher.EXPECT().Current().Return(searcherSecondPL),
	)

	readers := []index.Reader{firstReader, secondReader}

	s, err := NewNegationSearcher(readers, searcher)
	require.NoError(t, err)

	// Ensure the searcher is searching over two readers.
	require.Equal(t, 2, s.NumReaders())

	// Test the postings list from the first Reader.
	require.True(t, s.Next())

	expected := readerFirstPL.Clone()
	expected.Difference(searcherFirstPL)
	require.True(t, s.Current().Equal(expected))

	// Test the postings list from the second Reader.
	require.True(t, s.Next())

	expected = readerSecondPL.Clone()
	expected.Difference(searcherSecondPL)
	require.True(t, s.Current().Equal(expected))

	require.False(t, s.Next())
	require.NoError(t, s.Err())
}
