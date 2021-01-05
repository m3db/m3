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

package executor

import (
	"testing"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/postings/roaring"
	"github.com/m3db/m3/src/m3ninx/search"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestIterator(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	// Set up Searcher.
	firstPL := roaring.NewPostingsList()
	require.NoError(t, firstPL.Insert(42))
	require.NoError(t, firstPL.Insert(47))
	secondPL := roaring.NewPostingsList()
	require.NoError(t, secondPL.Insert(67))

	// Set up Readers.
	docs := []doc.Document{
		doc.NewDocumentFromEncoded(doc.Encoded{Bytes: []byte("encodedbytes1")}),
		doc.NewDocumentFromEncoded(doc.Encoded{Bytes: []byte("encodedbytes2")}),
		doc.NewDocumentFromEncoded(doc.Encoded{Bytes: []byte("encodedbytes3")}),
	}

	firstDocIter := doc.NewMockIterator(mockCtrl)
	secondDocIter := doc.NewMockIterator(mockCtrl)
	gomock.InOrder(
		firstDocIter.EXPECT().Next().Return(true),
		firstDocIter.EXPECT().Current().Return(docs[0]),
		firstDocIter.EXPECT().Next().Return(true),
		firstDocIter.EXPECT().Current().Return(docs[1]),
		firstDocIter.EXPECT().Next().Return(false),
		firstDocIter.EXPECT().Err().Return(nil),
		firstDocIter.EXPECT().Close().Return(nil),

		secondDocIter.EXPECT().Next().Return(true),
		secondDocIter.EXPECT().Current().Return(docs[2]),
		secondDocIter.EXPECT().Next().Return(false),
		secondDocIter.EXPECT().Err().Return(nil),
		secondDocIter.EXPECT().Close().Return(nil),
	)

	firstReader := index.NewMockReader(mockCtrl)
	secondReader := index.NewMockReader(mockCtrl)
	gomock.InOrder(
		firstReader.EXPECT().Docs(firstPL).Return(firstDocIter, nil),
		secondReader.EXPECT().Docs(secondPL).Return(secondDocIter, nil),
	)

	searcher := search.NewMockSearcher(mockCtrl)
	gomock.InOrder(
		searcher.EXPECT().Search(firstReader).Return(firstPL, nil),
		searcher.EXPECT().Search(secondReader).Return(secondPL, nil),
	)

	readers := index.Readers{firstReader, secondReader}

	// Construct iterator and run tests.
	iter, err := newIterator(searcher, readers)
	require.NoError(t, err)

	require.True(t, iter.Next())
	require.Equal(t, docs[0], iter.Current())
	require.True(t, iter.Next())
	require.Equal(t, docs[1], iter.Current())
	require.True(t, iter.Next())
	require.Equal(t, docs[2], iter.Current())

	require.False(t, iter.Next())
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
}
