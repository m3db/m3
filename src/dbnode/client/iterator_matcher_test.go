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

package client_test

import (
	"testing"

	"github.com/m3db/m3db/src/dbnode/client"
	"github.com/m3db/m3x/ident"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestTaggedIDsIteratorMatcherMatches(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mIter := client.NewMockTaggedIDsIterator(ctrl)
	gomock.InOrder(
		mIter.EXPECT().Next().Return(true),
		mIter.EXPECT().Current().Return(
			ident.StringID("ns"),
			ident.StringID("id0"),
			ident.NewTagsIterator(ident.NewTags(ident.StringTag("fgh", "ijk"))),
		),
		mIter.EXPECT().Next().Return(true),
		mIter.EXPECT().Current().Return(
			ident.StringID("ns"),
			ident.StringID("id1"),
			ident.NewTagsIterator(ident.NewTags(ident.StringTag("fgh", "ijk"))),
		),
		mIter.EXPECT().Next().Return(false),
		mIter.EXPECT().Next().Return(false),
		mIter.EXPECT().Err().Return(nil),
	)

	m, err := client.NewTaggedIDsIteratorMatcher(client.TaggedIDsIteratorMatcherOption{
		Namespace: "ns",
		ID:        "id0",
		Tags:      []string{"fgh", "ijk"},
	}, client.TaggedIDsIteratorMatcherOption{
		Namespace: "ns",
		ID:        "id1",
		Tags:      []string{"fgh", "ijk"},
	})
	require.NoError(t, err)
	require.True(t, m.Matches(mIter))
}

func TestTaggedIDsIteratorMatcherDoesNotMatchTooFew(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mIter := client.NewMockTaggedIDsIterator(ctrl)
	gomock.InOrder(
		mIter.EXPECT().Next().Return(true),
		mIter.EXPECT().Current().Return(
			ident.StringID("ns"),
			ident.StringID("id0"),
			ident.NewTagsIterator(ident.NewTags(ident.StringTag("fgh", "ijk"))),
		),
		mIter.EXPECT().Next().Return(false),
		mIter.EXPECT().Next().Return(false),
		mIter.EXPECT().Err().Return(nil),
	)

	m, err := client.NewTaggedIDsIteratorMatcher(client.TaggedIDsIteratorMatcherOption{
		Namespace: "ns",
		ID:        "id0",
		Tags:      []string{"fgh", "ijk"},
	}, client.TaggedIDsIteratorMatcherOption{
		Namespace: "ns",
		ID:        "id1",
		Tags:      []string{"fgh", "ijk"},
	})
	require.NoError(t, err)
	require.False(t, m.Matches(mIter))
}

func TestTaggedIDsIteratorMatcherDoesNotMatchTooMany(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mIter := client.NewMockTaggedIDsIterator(ctrl)
	gomock.InOrder(
		mIter.EXPECT().Next().Return(true),
		mIter.EXPECT().Current().Return(
			ident.StringID("ns"),
			ident.StringID("id0"),
			ident.NewTagsIterator(ident.NewTags(ident.StringTag("fgh", "ijk"))),
		),
		mIter.EXPECT().Next().Return(true),
		mIter.EXPECT().Current().Return(
			ident.StringID("ns"),
			ident.StringID("id1"),
			ident.NewTagsIterator(ident.NewTags(ident.StringTag("fgh", "ijk"))),
		),
		mIter.EXPECT().Next().Return(true),
		mIter.EXPECT().Current().Return(
			ident.StringID("ns"),
			ident.StringID("id2"),
			ident.NewTagsIterator(ident.NewTags(ident.StringTag("fgh", "ijk"))),
		),
	)

	m, err := client.NewTaggedIDsIteratorMatcher(client.TaggedIDsIteratorMatcherOption{
		Namespace: "ns",
		ID:        "id0",
		Tags:      []string{"fgh", "ijk"},
	}, client.TaggedIDsIteratorMatcherOption{
		Namespace: "ns",
		ID:        "id1",
		Tags:      []string{"fgh", "ijk"},
	})
	require.NoError(t, err)
	require.False(t, m.Matches(mIter))
}

func TestTaggedIDsIteratorMatcherDuplicateIDs(t *testing.T) {
	_, err := client.NewTaggedIDsIteratorMatcher(client.TaggedIDsIteratorMatcherOption{
		ID:        "abc",
		Namespace: "cde",
		Tags:      []string{"fgh", "ijk"},
	}, client.TaggedIDsIteratorMatcherOption{
		ID:        "abc",
		Namespace: "cde",
		Tags:      []string{"fgh", "ijk"},
	})
	require.Error(t, err)
}
