// Copyright (c) 2016 Uber Technologies, Inc.
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

package policy

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testTagPairSeparator  = ","
	testTagValueSeparator = "="
)

type testFilterData struct {
	id    string
	match bool
}

type testTagPair struct {
	name  string
	value string
}

type testSortedTagIterator struct {
	idx   int
	err   error
	pairs []testTagPair
}

func idToTestTagPairs(id string) []testTagPair {
	tagPairs := strings.Split(id, testTagPairSeparator)
	var pairs []testTagPair
	for _, pair := range tagPairs {
		p := strings.Split(pair, testTagValueSeparator)
		pairs = append(pairs, testTagPair{name: p[0], value: p[1]})
	}
	return pairs
}

func newTestSortedTagIterator(id string) SortedTagIterator {
	pairs := idToTestTagPairs(id)
	return &testSortedTagIterator{idx: -1, pairs: pairs}
}

func (it *testSortedTagIterator) Next() bool {
	if it.err != nil || it.idx >= len(it.pairs) {
		return false
	}
	it.idx++
	return it.err == nil && it.idx < len(it.pairs)
}

func (it *testSortedTagIterator) Current() (string, string) {
	return it.pairs[it.idx].name, it.pairs[it.idx].value
}

func (it *testSortedTagIterator) Err() error {
	return it.err
}

func (it *testSortedTagIterator) Close() {}

func TestEqualityFilter(t *testing.T) {
	inputs := []testFilterData{
		{id: "foo", match: true},
		{id: "fo", match: false},
		{id: "foob", match: false},
	}
	f := newEqualityFilter("foo")
	for _, input := range inputs {
		require.Equal(t, input.match, f.Matches(input.id))
	}
}

func TestEmptyTagsFilterMatches(t *testing.T) {
	f := newTagsFilter(nil, newTestSortedTagIterator)
	require.True(t, f.Matches("foo"))
}

func TestTagsFilterMatches(t *testing.T) {
	filters := map[string]string{
		"tagName1": "tagValue1",
		"tagName2": "tagValue2",
	}
	f := newTagsFilter(filters, newTestSortedTagIterator)
	inputs := []testFilterData{
		{id: "tagName1=tagValue1,tagName2=tagValue2", match: true},
		{id: "tagName0=tagValue0,tagName1=tagValue1,tagName2=tagValue2,tagName3=tagValue3", match: true},
		{id: "tagName1=tagValue1", match: false},
		{id: "tagName1=tagValue1", match: false},
		{id: "tagName2=tagValue2", match: false},
		{id: "tagName1=tagValue2,tagName2=tagValue1", match: false},
	}
	for _, input := range inputs {
		require.Equal(t, input.match, f.Matches(input.id))
	}
}

func TestTagsFilterString(t *testing.T) {
	filters := map[string]string{
		"tagName1": "tagValue1",
		"tagName2": "tagValue2",
	}
	f := newTagsFilter(filters, newTestSortedTagIterator)
	require.Equal(t, `tagName1:Equals("tagValue1") && tagName2:Equals("tagValue2")`, f.String())
}
