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

package testutil

import (
	"errors"
	"testing"

	"github.com/m3db/m3x/ident"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTagsFromTagIterator(t *testing.T) {
	in := ident.Tags{
		ident.StringTag("foo", "bar"),
		ident.StringTag("qux", "qaz"),
	}
	tags, err := NewTagsFromTagIterator(ident.NewTagSliceIterator(in))
	require.NoError(t, err)
	require.Equal(t, len(in), len(tags))
	for i, expected := range in {
		actual := tags[i]
		assert.True(t, expected.Equal(actual))
		assert.True(t, expected != actual) // Make sure made a copy
	}
}

func TestNewTagsFromTagIteratorEmptyTagIteratorDoesNotAlloc(t *testing.T) {
	tags, err := NewTagsFromTagIterator(ident.EmptyTagIterator)
	require.NoError(t, err)
	require.Nil(t, tags)
}

func TestNewTagsFromTagIteratorError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	expectedErr := errors.New("expected error")

	mockIter := ident.NewMockTagIterator(ctrl)
	gomock.InOrder(
		mockIter.EXPECT().Remaining().Return(1),
		mockIter.EXPECT().Next().Return(true),
		mockIter.EXPECT().Current().Return(ident.StringTag("foo", "bar")),
		mockIter.EXPECT().Next().Return(false),
		mockIter.EXPECT().Err().Return(expectedErr),
		mockIter.EXPECT().Close(),
	)

	_, err := NewTagsFromTagIterator(mockIter)
	require.Error(t, err)
}
