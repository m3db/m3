// Copyright (c) 2019 Uber Technologies, Inc.
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

	"github.com/m3db/m3/src/m3ninx/index/segment"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestNewFilterFieldsIteratorError(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	s := segment.NewMockSegment(ctrl)
	_, err := newFilterFieldsIterator(s, nil)
	require.Error(t, err)
}

func TestNewFilterFieldsIteratorNoMatchesInSegment(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	filters := AggregateFieldFilter{[]byte("a"), []byte("b")}
	s := segment.NewMockSegment(ctrl)
	iter, err := newFilterFieldsIterator(s, filters)
	require.NoError(t, err)

	s.EXPECT().ContainsField(gomock.Any()).Return(false, nil).AnyTimes()
	require.False(t, iter.Next())
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
}

func TestNewFilterFieldsIteratorFirstMatch(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	filters := AggregateFieldFilter{[]byte("a"), []byte("b"), []byte("c")}
	s := segment.NewMockSegment(ctrl)
	iter, err := newFilterFieldsIterator(s, filters)
	require.NoError(t, err)

	gomock.InOrder(
		s.EXPECT().ContainsField([]byte("a")).Return(true, nil),
		s.EXPECT().ContainsField([]byte("b")).Return(false, nil),
		s.EXPECT().ContainsField([]byte("c")).Return(false, nil),
	)
	require.True(t, iter.Next())
	require.Equal(t, "a", string(iter.Current()))
	require.False(t, iter.Next())
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
}

func TestNewFilterFieldsIteratorMiddleMatch(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	filters := AggregateFieldFilter{[]byte("a"), []byte("b"), []byte("c")}
	s := segment.NewMockSegment(ctrl)
	iter, err := newFilterFieldsIterator(s, filters)
	require.NoError(t, err)

	gomock.InOrder(
		s.EXPECT().ContainsField([]byte("a")).Return(false, nil),
		s.EXPECT().ContainsField([]byte("b")).Return(true, nil),
		s.EXPECT().ContainsField([]byte("c")).Return(false, nil),
	)
	require.True(t, iter.Next())
	require.Equal(t, "b", string(iter.Current()))
	require.False(t, iter.Next())
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
}

func TestNewFilterFieldsIteratorEndMatch(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	filters := AggregateFieldFilter{[]byte("a"), []byte("b"), []byte("c")}
	s := segment.NewMockSegment(ctrl)
	iter, err := newFilterFieldsIterator(s, filters)
	require.NoError(t, err)

	gomock.InOrder(
		s.EXPECT().ContainsField([]byte("a")).Return(false, nil),
		s.EXPECT().ContainsField([]byte("b")).Return(false, nil),
		s.EXPECT().ContainsField([]byte("c")).Return(true, nil),
	)
	require.True(t, iter.Next())
	require.Equal(t, "c", string(iter.Current()))
	require.False(t, iter.Next())
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
}

func TestNewFilterFieldsIteratorAllMatch(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	filters := AggregateFieldFilter{[]byte("a"), []byte("b"), []byte("c")}
	s := segment.NewMockSegment(ctrl)
	iter, err := newFilterFieldsIterator(s, filters)
	require.NoError(t, err)

	gomock.InOrder(
		s.EXPECT().ContainsField([]byte("a")).Return(true, nil),
		s.EXPECT().ContainsField([]byte("b")).Return(true, nil),
		s.EXPECT().ContainsField([]byte("c")).Return(true, nil),
	)
	require.True(t, iter.Next())
	require.Equal(t, "a", string(iter.Current()))
	require.True(t, iter.Next())
	require.Equal(t, "b", string(iter.Current()))
	require.True(t, iter.Next())
	require.Equal(t, "c", string(iter.Current()))
	require.False(t, iter.Next())
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
}

func TestNewFilterFieldsIteratorRandomMatch(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	filters := AggregateFieldFilter{[]byte("a"), []byte("b"), []byte("c")}
	s := segment.NewMockSegment(ctrl)
	iter, err := newFilterFieldsIterator(s, filters)
	require.NoError(t, err)

	gomock.InOrder(
		s.EXPECT().ContainsField([]byte("a")).Return(true, nil),
		s.EXPECT().ContainsField([]byte("b")).Return(false, nil),
		s.EXPECT().ContainsField([]byte("c")).Return(true, nil),
	)
	require.True(t, iter.Next())
	require.Equal(t, "a", string(iter.Current()))
	require.True(t, iter.Next())
	require.Equal(t, "c", string(iter.Current()))
	require.False(t, iter.Next())
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
}
