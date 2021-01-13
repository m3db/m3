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

package index

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/postings"
)

func TestIterator(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	docWithIds := []struct {
		id  postings.ID
		doc doc.Metadata
	}{
		{
			id: 42,
			doc: doc.Metadata{
				Fields: []doc.Field{
					{
						Name:  []byte("apple"),
						Value: []byte("red"),
					},
				},
			},
		},
		{
			id: 53,
			doc: doc.Metadata{
				Fields: []doc.Field{
					{
						Name:  []byte("banana"),
						Value: []byte("yellow"),
					},
				},
			},
		},
		{
			id: 81,
			doc: doc.Metadata{
				Fields: []doc.Field{
					{
						Name:  []byte("carrot"),
						Value: []byte("orange"),
					},
				},
			},
		},
	}

	retriever := NewMockMetadataRetriever(mockCtrl)
	gomock.InOrder(
		retriever.EXPECT().Metadata(docWithIds[0].id).Return(docWithIds[0].doc, nil),
		retriever.EXPECT().Metadata(docWithIds[1].id).Return(docWithIds[1].doc, nil),
		retriever.EXPECT().Metadata(docWithIds[2].id).Return(docWithIds[2].doc, nil),
	)

	postingsIter := postings.NewMockIterator(mockCtrl)
	gomock.InOrder(
		postingsIter.EXPECT().Next().Return(true),
		postingsIter.EXPECT().Current().Return(docWithIds[0].id),
		postingsIter.EXPECT().Next().Return(true),
		postingsIter.EXPECT().Current().Return(docWithIds[1].id),
		postingsIter.EXPECT().Next().Return(true),
		postingsIter.EXPECT().Current().Return(docWithIds[2].id),
		postingsIter.EXPECT().Next().Return(false),
		postingsIter.EXPECT().Close().Return(nil),
	)

	it := NewIDDocIterator(retriever, postingsIter)

	require.True(t, it.Next())
	require.Equal(t, docWithIds[0].doc, it.Current())
	require.Equal(t, docWithIds[0].id, it.PostingsID())
	require.True(t, it.Next())
	require.Equal(t, docWithIds[1].doc, it.Current())
	require.Equal(t, docWithIds[1].id, it.PostingsID())
	require.True(t, it.Next())
	require.Equal(t, docWithIds[2].doc, it.Current())
	require.Equal(t, docWithIds[2].id, it.PostingsID())

	require.False(t, it.Next())
	require.NoError(t, it.Err())

	require.NoError(t, it.Close())
}
