// Copyright (c) 2017 Uber Technologies, Inc.
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

package roaring

import (
	"testing"

	"github.com/m3db/m3/src/m3ninx/postings"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestRoaringPostingsListEmpty(t *testing.T) {
	d := NewPostingsList()
	require.True(t, d.IsEmpty())
	require.Equal(t, 0, d.Len())
}

func TestRoaringPostingsListMax(t *testing.T) {
	d := NewPostingsList()
	require.NoError(t, d.Insert(42))
	require.NoError(t, d.Insert(78))
	require.NoError(t, d.Insert(103))

	max, err := d.Max()
	require.NoError(t, err)
	require.Equal(t, postings.ID(103), max)

	d = NewPostingsList()
	_, err = d.Max()
	require.Error(t, err)
}

func TestRoaringPostingsListInsert(t *testing.T) {
	d := NewPostingsList()
	require.NoError(t, d.Insert(1))
	require.True(t, d.Contains(1))
	require.Equal(t, 1, d.Len())
	// Idempotency of inserts.
	require.NoError(t, d.Insert(1))
	require.Equal(t, 1, d.Len())
	require.True(t, d.Contains(1))
}

func TestRoaringPostingsListClone(t *testing.T) {
	d := NewPostingsList()
	require.NoError(t, d.Insert(1))
	require.True(t, d.Contains(1))
	require.Equal(t, 1, d.Len())

	c := d.Clone()
	require.True(t, c.Contains(1))
	require.Equal(t, 1, c.Len())

	// Ensure only clone is uniquely backed.
	require.NoError(t, c.Insert(2))
	require.True(t, c.Contains(2))
	require.Equal(t, 2, c.Len())
	require.True(t, d.Contains(1))
	require.Equal(t, 1, d.Len())
}

func TestRoaringPostingsListIntersect(t *testing.T) {
	d := NewPostingsList()
	require.NoError(t, d.Insert(1))
	require.True(t, d.Contains(1))
	require.Equal(t, 1, d.Len())

	c := d.Clone()
	require.True(t, c.Contains(1))

	require.NoError(t, d.Insert(2))
	require.NoError(t, c.Insert(3))

	require.NoError(t, d.Intersect(c))
	require.True(t, d.Contains(1))
	require.Equal(t, 1, d.Len())
	require.True(t, c.Contains(1))
	require.True(t, c.Contains(3))
	require.Equal(t, 2, c.Len())
}

func TestRoaringPostingsListDifference(t *testing.T) {
	d := NewPostingsList()
	require.NoError(t, d.Insert(1))
	require.True(t, d.Contains(1))
	require.Equal(t, 1, d.Len())

	c := d.Clone()
	require.True(t, c.Contains(1))

	require.NoError(t, d.Insert(2))
	require.NoError(t, d.Insert(3))
	require.NoError(t, d.Difference(c))

	require.False(t, d.Contains(1))
	require.True(t, c.Contains(1))
	require.Equal(t, 2, d.Len())
	require.Equal(t, 1, c.Len())
	require.True(t, d.Contains(3))
	require.True(t, d.Contains(2))
}

func TestRoaringPostingsListUnion(t *testing.T) {
	d := NewPostingsList()
	require.NoError(t, d.Insert(1))
	require.True(t, d.Contains(1))
	require.Equal(t, 1, d.Len())

	c := d.Clone()
	require.True(t, c.Contains(1))
	require.NoError(t, d.Insert(2))
	require.NoError(t, c.Insert(3))

	require.NoError(t, d.Union(c))
	require.True(t, d.Contains(1))
	require.True(t, d.Contains(2))
	require.True(t, d.Contains(3))
	require.Equal(t, 3, d.Len())
	require.True(t, c.Contains(1))
	require.True(t, c.Contains(3))
	require.Equal(t, 2, c.Len())
}

func TestRoaringPostingsListAddRange(t *testing.T) {
	d := NewPostingsList()
	require.NoError(t, d.Insert(1))
	require.NoError(t, d.Insert(9))
	require.NoError(t, d.AddRange(3, 5))

	require.Equal(t, 4, d.Len())
	require.True(t, d.Contains(1))
	require.False(t, d.Contains(2))
	require.True(t, d.Contains(3))
	require.True(t, d.Contains(4))
	require.False(t, d.Contains(5))
	require.True(t, d.Contains(9))
}

func TestRoaringPostingsListRemoveRange(t *testing.T) {
	d := NewPostingsList()
	require.NoError(t, d.Insert(1))
	require.NoError(t, d.Insert(2))
	require.NoError(t, d.Insert(7))
	require.NoError(t, d.Insert(8))
	require.NoError(t, d.Insert(9))

	require.NoError(t, d.RemoveRange(2, 8))
	require.Equal(t, 3, d.Len())
	require.True(t, d.Contains(1))
	require.False(t, d.Contains(2))
	require.False(t, d.Contains(7))
	require.True(t, d.Contains(8))
	require.True(t, d.Contains(9))
}

func TestRoaringPostingsListReset(t *testing.T) {
	d := NewPostingsList()
	require.NoError(t, d.Insert(1))
	require.True(t, d.Contains(1))
	require.Equal(t, 1, d.Len())
	d.Reset()
	require.True(t, d.IsEmpty())
	require.Equal(t, 0, d.Len())
}

func TestRoaringPostingsListIter(t *testing.T) {
	d := NewPostingsList()
	require.NoError(t, d.Insert(1))
	require.NoError(t, d.Insert(2))
	require.Equal(t, 2, d.Len())

	it := d.Iterator()
	defer it.Close()
	found := map[postings.ID]bool{
		1: false,
		2: false,
	}
	for it.Next() {
		found[it.Current()] = true
	}

	for id, ok := range found {
		require.True(t, ok, id)
	}
}

func TestRoaringPostingsListEqualWithOtherRoaring(t *testing.T) {
	first := NewPostingsList()
	require.NoError(t, first.Insert(42))
	require.NoError(t, first.Insert(44))
	require.NoError(t, first.Insert(51))

	second := NewPostingsList()
	require.NoError(t, second.Insert(42))
	require.NoError(t, second.Insert(44))
	require.NoError(t, second.Insert(51))

	require.True(t, first.Equal(second))
}

func TestRoaringPostingsListNotEqualWithOtherRoaring(t *testing.T) {
	first := NewPostingsList()
	require.NoError(t, first.Insert(42))
	require.NoError(t, first.Insert(44))
	require.NoError(t, first.Insert(51))

	second := NewPostingsList()
	require.NoError(t, second.Insert(42))
	require.NoError(t, second.Insert(44))
	require.NoError(t, second.Insert(51))
	require.NoError(t, second.Insert(53))

	require.False(t, first.Equal(second))
}

func TestRoaringPostingsListEqualWithOtherNonRoaring(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	first := NewPostingsList()
	require.NoError(t, first.Insert(42))
	require.NoError(t, first.Insert(44))
	require.NoError(t, first.Insert(51))

	postingsIter := postings.NewMockIterator(mockCtrl)
	gomock.InOrder(
		postingsIter.EXPECT().Next().Return(true),
		postingsIter.EXPECT().Current().Return(postings.ID(42)),
		postingsIter.EXPECT().Next().Return(true),
		postingsIter.EXPECT().Current().Return(postings.ID(44)),
		postingsIter.EXPECT().Next().Return(true),
		postingsIter.EXPECT().Current().Return(postings.ID(51)),
	)

	second := postings.NewMockList(mockCtrl)
	gomock.InOrder(
		second.EXPECT().Len().Return(3),
		second.EXPECT().Iterator().Return(postingsIter),
	)

	require.True(t, first.Equal(second))
}

func TestRoaringPostingsListNotEqualWithOtherNonRoaring(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	first := NewPostingsList()
	require.NoError(t, first.Insert(42))
	require.NoError(t, first.Insert(44))
	require.NoError(t, first.Insert(51))

	postingsIter := postings.NewMockIterator(mockCtrl)
	gomock.InOrder(
		postingsIter.EXPECT().Next().Return(true),
		postingsIter.EXPECT().Current().Return(postings.ID(42)),
		postingsIter.EXPECT().Next().Return(true),
		postingsIter.EXPECT().Current().Return(postings.ID(44)),
		postingsIter.EXPECT().Next().Return(true),
		postingsIter.EXPECT().Current().Return(postings.ID(53)),
	)

	second := postings.NewMockList(mockCtrl)
	gomock.InOrder(
		second.EXPECT().Len().Return(3),
		second.EXPECT().Iterator().Return(postingsIter),
	)

	require.False(t, first.Equal(second))
}

func TestRoaringPostingsAddIterator(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	first := NewPostingsList()

	postingsIter := postings.NewMockIterator(mockCtrl)
	gomock.InOrder(
		postingsIter.EXPECT().Next().Return(true),
		postingsIter.EXPECT().Current().Return(postings.ID(42)),
		postingsIter.EXPECT().Next().Return(true),
		postingsIter.EXPECT().Current().Return(postings.ID(44)),
		postingsIter.EXPECT().Next().Return(true),
		postingsIter.EXPECT().Current().Return(postings.ID(51)),
		postingsIter.EXPECT().Next().Return(false),
		postingsIter.EXPECT().Err().Return(nil),
		postingsIter.EXPECT().Close().Return(nil),
	)

	require.NoError(t, first.AddIterator(postingsIter))
	require.Equal(t, 3, first.Len())
	require.True(t, first.Contains(postings.ID(42)))
	require.True(t, first.Contains(postings.ID(44)))
	require.True(t, first.Contains(postings.ID(51)))
}
