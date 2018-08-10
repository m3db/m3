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

package doc

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/golang/mock/gomock"
)

var (
	testDocuments = []Document{
		Document{
			Fields: []Field{
				Field{
					Name:  []byte("apple"),
					Value: []byte("red"),
				},
			},
		},
		Document{
			Fields: []Field{
				Field{
					Name:  []byte("banana"),
					Value: []byte("yellow"),
				},
			},
		},
		Document{
			Fields: []Field{
				Field{
					Name:  []byte("carrot"),
					Value: []byte("orange"),
				},
			},
		},
	}
)

func TestIteratorMatcherSimple(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	firstDocIter := NewMockIterator(mockCtrl)
	setup := func() {
		gomock.InOrder(
			firstDocIter.EXPECT().Next().Return(true),
			firstDocIter.EXPECT().Current().Return(testDocuments[0]),
			firstDocIter.EXPECT().Next().Return(true),
			firstDocIter.EXPECT().Current().Return(testDocuments[1]),
			firstDocIter.EXPECT().Next().Return(true),
			firstDocIter.EXPECT().Current().Return(testDocuments[2]),
			firstDocIter.EXPECT().Next().Return(false),
			firstDocIter.EXPECT().Err().Return(nil),
			firstDocIter.EXPECT().Close().Return(nil),
		)
	}

	setup()
	matcher := NewIteratorMatcher(true, testDocuments...)
	require.True(t, matcher.Matches(firstDocIter))

	setup()
	matcher = NewIteratorMatcher(false, testDocuments...)
	require.True(t, matcher.Matches(firstDocIter))
}

func TestIteratorMatcherAnyOrdering(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	firstDocIter := NewMockIterator(mockCtrl)
	setup := func() {
		gomock.InOrder(
			firstDocIter.EXPECT().Next().Return(true),
			firstDocIter.EXPECT().Current().Return(testDocuments[0]),
			firstDocIter.EXPECT().Next().Return(true),
			firstDocIter.EXPECT().Current().Return(testDocuments[2]),
			firstDocIter.EXPECT().Next().Return(true),
			firstDocIter.EXPECT().Current().Return(testDocuments[1]),
			firstDocIter.EXPECT().Next().Return(false),
			firstDocIter.EXPECT().Err().Return(nil),
			firstDocIter.EXPECT().Close().Return(nil),
		)
	}
	setup()
	matcher := NewIteratorMatcher(true, testDocuments...)
	require.False(t, matcher.Matches(firstDocIter))

	setup()
	matcher = NewIteratorMatcher(false, testDocuments...)
	require.True(t, matcher.Matches(firstDocIter))
}
