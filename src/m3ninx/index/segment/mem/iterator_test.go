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

	"github.com/m3db/m3db/src/m3ninx/postings"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestIterator(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	ids := []postings.ID{
		13, 27, 42, 65,
	}
	limit := ids[len(ids)-2]

	postingsIter := postings.NewMockIterator(mockCtrl)
	gomock.InOrder(
		postingsIter.EXPECT().Next().Return(true),
		postingsIter.EXPECT().Current().Return(ids[0]),
		postingsIter.EXPECT().Next().Return(true),
		postingsIter.EXPECT().Current().Return(ids[1]),
		postingsIter.EXPECT().Next().Return(true),
		postingsIter.EXPECT().Current().Return(ids[2]),
		postingsIter.EXPECT().Next().Return(true),
		postingsIter.EXPECT().Current().Return(ids[3]),
		postingsIter.EXPECT().Next().Return(false),
		postingsIter.EXPECT().Err().Return(nil),
		postingsIter.EXPECT().Close().Return(nil),
	)

	bounds := readerDocRange{startInclusive: 0, endExclusive: limit}
	it := newBoundedPostingsIterator(postingsIter, bounds)

	require.True(t, it.Next())
	require.Equal(t, ids[0], it.Current())
	require.True(t, it.Next())
	require.Equal(t, ids[1], it.Current())

	require.False(t, it.Next())
	require.NoError(t, it.Err())

	require.NoError(t, it.Close())
}
