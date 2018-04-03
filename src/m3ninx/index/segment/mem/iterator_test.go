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

package mem

import (
	"testing"

	"github.com/m3db/m3ninx/doc"
	"github.com/m3db/m3ninx/postings"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestIterator(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	maxID := postings.ID(41)
	docs := []doc.Document{
		doc.Document{
			Fields: []doc.Field{
				doc.Field{
					Name:  []byte("apple"),
					Value: []byte("red"),
				},
			},
		},
		doc.Document{
			Fields: []doc.Field{
				doc.Field{
					Name:  []byte("banana"),
					Value: []byte("yellow"),
				},
			},
		},
		doc.Document{
			Fields: []doc.Field{
				doc.Field{
					Name:  []byte("carrot"),
					Value: []byte("orange"),
				},
			},
		},
		doc.Document{
			Fields: []doc.Field{
				doc.Field{
					Name:  []byte("date"),
					Value: []byte("brown"),
				},
			},
		},
	}

	segment := NewMockReadableSegment(mockCtrl)
	gomock.InOrder(
		segment.EXPECT().getDoc(maxID-2).Return(docs[0], nil),
		segment.EXPECT().getDoc(maxID-1).Return(docs[1], nil),
		segment.EXPECT().getDoc(maxID).Return(docs[2], nil),
	)

	postingsIter := postings.NewMockIterator(mockCtrl)
	gomock.InOrder(
		postingsIter.EXPECT().Next().Return(true),
		postingsIter.EXPECT().Current().Return(maxID-2),
		postingsIter.EXPECT().Next().Return(true),
		postingsIter.EXPECT().Current().Return(maxID-1),
		postingsIter.EXPECT().Next().Return(true),
		postingsIter.EXPECT().Current().Return(maxID),
		postingsIter.EXPECT().Next().Return(true),
		postingsIter.EXPECT().Current().Return(maxID+1),
		postingsIter.EXPECT().Close().Return(nil),
	)

	it := newIterator(segment, postingsIter, maxID)

	require.True(t, it.Next())
	require.Equal(t, docs[0], it.Current())
	require.True(t, it.Next())
	require.Equal(t, docs[1], it.Current())
	require.True(t, it.Next())
	require.Equal(t, docs[2], it.Current())

	require.False(t, it.Next())
	require.NoError(t, it.Err())

	require.NoError(t, it.Close())
	require.Error(t, it.Close())
}
