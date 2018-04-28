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

package index_test

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3db/storage/index"
	"github.com/m3db/m3x/ident"
	"github.com/stretchr/testify/require"
)

func TestIteratorMatcherMatches(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mIter := index.NewMockIterator(ctrl)
	gomock.InOrder(
		mIter.EXPECT().Next().Return(true),
		mIter.EXPECT().Current().Return(
			ident.StringID("ns"),
			ident.StringID("id0"),
			ident.NewTagIterator(ident.StringTag("fgh", "ijk")),
		),
		mIter.EXPECT().Next().Return(true),
		mIter.EXPECT().Current().Return(
			ident.StringID("ns"),
			ident.StringID("id1"),
			ident.NewTagIterator(ident.StringTag("fgh", "ijk")),
		),
		mIter.EXPECT().Next().Return(false),
		mIter.EXPECT().Next().Return(false),
		mIter.EXPECT().Err().Return(nil),
	)

	m, err := index.NewIteratorMatcher(index.IteratorMatcherOption{
		Namespace: "ns",
		ID:        "id0",
		Tags:      []string{"fgh", "ijk"},
	}, index.IteratorMatcherOption{
		Namespace: "ns",
		ID:        "id1",
		Tags:      []string{"fgh", "ijk"},
	})
	require.NoError(t, err)
	require.True(t, m.Matches(mIter))
}

func TestIteratorMatcherDoesNotMatchTooFew(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mIter := index.NewMockIterator(ctrl)
	gomock.InOrder(
		mIter.EXPECT().Next().Return(true),
		mIter.EXPECT().Current().Return(
			ident.StringID("ns"),
			ident.StringID("id0"),
			ident.NewTagIterator(ident.StringTag("fgh", "ijk")),
		),
		mIter.EXPECT().Next().Return(false),
		mIter.EXPECT().Next().Return(false),
		mIter.EXPECT().Err().Return(nil),
	)

	m, err := index.NewIteratorMatcher(index.IteratorMatcherOption{
		Namespace: "ns",
		ID:        "id0",
		Tags:      []string{"fgh", "ijk"},
	}, index.IteratorMatcherOption{
		Namespace: "ns",
		ID:        "id1",
		Tags:      []string{"fgh", "ijk"},
	})
	require.NoError(t, err)
	require.False(t, m.Matches(mIter))
}

func TestIteratorMatcherDoesNotMatchTooMany(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mIter := index.NewMockIterator(ctrl)
	gomock.InOrder(
		mIter.EXPECT().Next().Return(true),
		mIter.EXPECT().Current().Return(
			ident.StringID("ns"),
			ident.StringID("id0"),
			ident.NewTagIterator(ident.StringTag("fgh", "ijk")),
		),
		mIter.EXPECT().Next().Return(true),
		mIter.EXPECT().Current().Return(
			ident.StringID("ns"),
			ident.StringID("id1"),
			ident.NewTagIterator(ident.StringTag("fgh", "ijk")),
		),
		mIter.EXPECT().Next().Return(true),
		mIter.EXPECT().Current().Return(
			ident.StringID("ns"),
			ident.StringID("id2"),
			ident.NewTagIterator(ident.StringTag("fgh", "ijk")),
		),
	)

	m, err := index.NewIteratorMatcher(index.IteratorMatcherOption{
		Namespace: "ns",
		ID:        "id0",
		Tags:      []string{"fgh", "ijk"},
	}, index.IteratorMatcherOption{
		Namespace: "ns",
		ID:        "id1",
		Tags:      []string{"fgh", "ijk"},
	})
	require.NoError(t, err)
	require.False(t, m.Matches(mIter))
}

func TestIteratorMatcherDuplicateIDs(t *testing.T) {
	_, err := index.NewIteratorMatcher(index.IteratorMatcherOption{
		ID:        "abc",
		Namespace: "cde",
		Tags:      []string{"fgh", "ijk"},
	}, index.IteratorMatcherOption{
		ID:        "abc",
		Namespace: "cde",
		Tags:      []string{"fgh", "ijk"},
	})
	require.Error(t, err)
}
