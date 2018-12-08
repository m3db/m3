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

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestTermSearcher(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	field, term := []byte("fruit"), []byte("apple")

	// First reader.
	firstPL := roaring.NewPostingsList()
	require.NoError(t, firstPL.Insert(postings.ID(42)))
	require.NoError(t, firstPL.Insert(postings.ID(50)))
	firstReader := index.NewMockReader(mockCtrl)

	// Second reader.
	secondPL := roaring.NewPostingsList()
	require.NoError(t, secondPL.Insert(postings.ID(57)))
	secondReader := index.NewMockReader(mockCtrl)

	gomock.InOrder(
		// Query the first reader.
		firstReader.EXPECT().MatchTerm(field, term).Return(firstPL, nil),

		// Query the second reader.
		secondReader.EXPECT().MatchTerm(field, term).Return(secondPL, nil),
	)

	s := NewTermSearcher(field, term)

	// Test the postings list from the first Reader.
	pl, err := s.Search(firstReader)
	require.NoError(t, err)
	require.True(t, pl.Equal(firstPL))

	// Test the postings list from the second Reader.
	pl, err = s.Search(secondReader)
	require.NoError(t, err)
	require.True(t, pl.Equal(secondPL))
}
