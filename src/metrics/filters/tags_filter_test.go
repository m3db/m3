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

package filters

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmptyTagsFilterMatches(t *testing.T) {
	f, err := NewTagsFilter(nil, NewMockSortedTagIterator, Conjunction)
	require.NoError(t, err)
	require.True(t, f.Matches([]byte("foo")))
}

func TestTagsFilterMatches(t *testing.T) {
	filters := map[string]string{
		"tagName1": "tagValue1",
		"tagName2": "tagValue2",
	}
	f, err := NewTagsFilter(filters, NewMockSortedTagIterator, Conjunction)
	inputs := []mockFilterData{
		{val: "tagName1=tagValue1,tagName2=tagValue2", match: true},
		{val: "tagName0=tagValue0,tagName1=tagValue1,tagName2=tagValue2,tagName3=tagValue3", match: true},
		{val: "tagName1=tagValue1", match: false},
		{val: "tagName2=tagValue2", match: false},
		{val: "tagName1=tagValue2,tagName2=tagValue1", match: false},
	}
	require.NoError(t, err)
	for _, input := range inputs {
		require.Equal(t, input.match, f.Matches([]byte(input.val)))
	}

	f, err = NewTagsFilter(filters, NewMockSortedTagIterator, Disjunction)
	inputs = []mockFilterData{
		{val: "tagName1=tagValue1,tagName2=tagValue2", match: true},
		{val: "tagName0=tagValue0,tagName1=tagValue1,tagName2=tagValue2,tagName3=tagValue3", match: true},
		{val: "tagName0=tagValue0,tagName1=tagValue2,tagName2=tagValue2,tagName3=tagValue3", match: true},
		{val: "tagName1=tagValue1", match: true},
		{val: "tagName2=tagValue2", match: true},
		{val: "tagName1=tagValue2,tagName2=tagValue1", match: false},
		{val: "tagName3=tagValue3", match: false},
		{val: "tagName2=tagValue1", match: false},
		{val: "tagName15=tagValue2,tagName3=tagValue2", match: false},
	}
	require.NoError(t, err)
	for _, input := range inputs {
		require.Equal(t, input.match, f.Matches([]byte(input.val)), "val:", input.val)
	}
}

func TestTagsFilterString(t *testing.T) {
	filters := map[string]string{
		"tagName1": "tagValue1",
		"tagName2": "tagValue2",
	}
	f, err := NewTagsFilter(filters, NewMockSortedTagIterator, Conjunction)
	require.NoError(t, err)
	require.Equal(t, `tagName1:Equals("tagValue1") && tagName2:Equals("tagValue2")`, f.String())

	f, err = NewTagsFilter(filters, NewMockSortedTagIterator, Disjunction)
	require.NoError(t, err)
	require.Equal(t, `tagName1:Equals("tagValue1") || tagName2:Equals("tagValue2")`, f.String())
}
