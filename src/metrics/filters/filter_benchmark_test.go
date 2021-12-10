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
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/metrics/metric/id"
)

var (
	testFlatID = []byte("tagname1=tagvalue1,tagname3=tagvalue3," +
		"tagname4=tagvalue4,tagname2=tagvalue2,tagname6=tagvalue6," +
		"tagname5=tagvalue5,name=my.test.metric.name,tagname7=tagvalue7")

	testTagsFilterMapOne = map[string]FilterValue{
		"tagname1": FilterValue{Pattern: "tagvalue1"},
	}

	testTagsFilterMapThree = map[string]FilterValue{
		"tagname1":    FilterValue{Pattern: "tagvalue1"},
		"tagname2":    FilterValue{Pattern: "tagvalue2"},
		"tagname3":    FilterValue{Pattern: "tagvalue3"},
		"faketagname": FilterValue{Pattern: "faketagvalue"},
	}
)

func BenchmarkEqualityFilter(b *testing.B) {
	f1 := newEqualityFilter([]byte("test"))
	f2 := newEqualityFilter([]byte("test2"))

	val := []byte("test")
	for n := 0; n < b.N; n++ {
		_ = testUnionFilter(val, []Filter{f1, f2})
	}
}

func BenchmarkEqualityFilterByValue(b *testing.B) {
	f1 := newTestEqualityFilter([]byte("test"))
	f2 := newTestEqualityFilter([]byte("test2"))

	val := []byte("test")
	for n := 0; n < b.N; n++ {
		_ = testUnionFilter(val, []Filter{f1, f2})
	}
}

func BenchmarkTagsFilterOne(b *testing.B) {
	filter, _ := NewTagsFilter(testTagsFilterMapOne, Conjunction, testTagsFilterOptions())
	benchTagsFilter(b, testFlatID, filter)
}

func BenchmarkMapTagsFilterOne(b *testing.B) {
	benchTagsFilter(b, testFlatID, newTestMapTagsFilter(testTagsFilterMapOne, NewMockSortedTagIterator))
}

func BenchmarkTagsFilterThree(b *testing.B) {
	filter, _ := NewTagsFilter(testTagsFilterMapThree, Conjunction, testTagsFilterOptions())
	benchTagsFilter(b, testFlatID, filter)
}

func BenchmarkMapTagsFilterThree(b *testing.B) {
	benchTagsFilter(b, testFlatID, newTestMapTagsFilter(testTagsFilterMapThree, NewMockSortedTagIterator))
}

func BenchmarkRangeFilterStructsMatchRange(b *testing.B) {
	benchRangeFilterStructs(b, []byte("a-z"), []byte("p"), true)
}

func BenchmarkRangeFilterRangeMatchRange(b *testing.B) {
	benchRangeFilterRange(b, []byte("a-z"), []byte("p"), true)
}

func BenchmarkRangeFilterStructsNotMatchRange(b *testing.B) {
	benchRangeFilterStructs(b, []byte("a-z"), []byte("P"), false)
}

func BenchmarkRangeFilterRangeNotMatchRange(b *testing.B) {
	benchRangeFilterRange(b, []byte("a-z"), []byte("P"), false)
}

func BenchmarkRangeFilterStructsMatch(b *testing.B) {
	benchRangeFilterStructs(b, []byte("02468"), []byte("6"), true)
}

func BenchmarkRangeFilterRangeMatch(b *testing.B) {
	benchRangeFilterRange(b, []byte("02468"), []byte("6"), true)
}

func BenchmarkRangeFilterStructsNotMatch(b *testing.B) {
	benchRangeFilterStructs(b, []byte("13579"), []byte("6"), false)
}

func BenchmarkRangeFilterRangeNotMatch(b *testing.B) {
	benchRangeFilterRange(b, []byte("13579"), []byte("6"), false)
}

func BenchmarkRangeFilterStructsMatchNegation(b *testing.B) {
	benchRangeFilterStructs(b, []byte("!a-z"), []byte("p"), false)
}

func BenchmarkRangeFilterRangeMatchNegation(b *testing.B) {
	benchRangeFilterRange(b, []byte("!a-z"), []byte("p"), false)
}

func BenchmarkMultiRangeFilterTwo(b *testing.B) {
	benchMultiRangeFilter(b, []byte("test_1,test_2"), false, [][]byte{[]byte("test_1"), []byte("fake_1")})
}

func BenchmarkMultiRangeFilterSelectTwo(b *testing.B) {
	benchMultiRangeFilterSelect(b, []byte("test_1,test_2"), false, [][]byte{[]byte("test_1"), []byte("fake_1")})
}

func BenchmarkMultiRangeFilterTrieTwo(b *testing.B) {
	benchMultiRangeFilterTrie(b, []byte("test_1,test_2"), false, [][]byte{[]byte("test_1"), []byte("fake_1")})
}

func BenchmarkMultiRangeFilterSix(b *testing.B) {
	benchMultiRangeFilter(b, []byte("test_1,test_2,staging_1,staging_2,prod_1,prod_2"), false, [][]byte{[]byte("prod_1"), []byte("staging_3")})
}

func BenchmarkMultiRangeFilterSelectSix(b *testing.B) {
	benchMultiRangeFilterSelect(b, []byte("test_1,test_2,staging_1,staging_2,prod_1,prod_2"), false, [][]byte{[]byte("prod_1"), []byte("staging_3")})
}

func BenchmarkMultiRangeFilterTrieSix(b *testing.B) {
	benchMultiRangeFilterTrie(b, []byte("test_1,test_2,staging_1,staging_2,prod_1,prod_2"), false, [][]byte{[]byte("prod_1"), []byte("staging_3")})
}

// nolint: unparam
func benchMultiRangeFilter(b *testing.B, patterns []byte, backwards bool, vals [][]byte) {
	f, _ := newMultiCharSequenceFilter(patterns, backwards)
	for n := 0; n < b.N; n++ {
		for _, val := range vals {
			f.matches(val)
		}
	}
}

// nolint: unparam
func benchMultiRangeFilterSelect(b *testing.B, patterns []byte, backwards bool, vals [][]byte) {
	f, err := newTestMultiCharRangeSelectFilter(patterns, backwards)
	if err != nil {
		b.Errorf("encountered error creating filter: %v", err)
	}

	for n := 0; n < b.N; n++ {
		for _, val := range vals {
			f.matches(val)
		}
	}
}

// nolint: unparam
func benchMultiRangeFilterTrie(b *testing.B, patterns []byte, backwards bool, vals [][]byte) {
	f, err := newTestMultiCharRangeTrieFilter(patterns, backwards)
	if err != nil {
		b.Errorf("encountered error creating filter: %v", err)
	}

	for n := 0; n < b.N; n++ {
		for _, val := range vals {
			f.matches(val)
		}
	}
}

func benchRangeFilterStructs(b *testing.B, pattern, val []byte, expectedMatch bool) {
	f, _ := newSingleRangeFilter(pattern, false)
	for n := 0; n < b.N; n++ {
		_, match := f.matches(val)
		if match != expectedMatch {
			b.FailNow()
		}
	}
}

func benchRangeFilterRange(b *testing.B, pattern, val []byte, expectedMatch bool) {
	for n := 0; n < b.N; n++ {
		match, err := validateRangeByScan(pattern, val)
		if err != nil {
			b.Errorf("unexpected error: %v", err)
		}
		if match != expectedMatch {
			b.FailNow()
		}
	}
}

func benchTagsFilter(b *testing.B, id []byte, tagsFilter TagsFilter) {
	for n := 0; n < b.N; n++ {
		_, err := tagsFilter.Matches(id, testTagsMatchOptions())
		require.NoError(b, err)
	}
}

// nolint: unparam
func testUnionFilter(val []byte, filters []Filter) bool {
	for _, filter := range filters {
		if !filter.Matches(val) {
			return false
		}
	}

	return true
}

type testEqualityFilter struct {
	pattern []byte
}

func newTestEqualityFilter(pattern []byte) Filter {
	return newImmutableFilter(testEqualityFilter{pattern: pattern})
}

func (f testEqualityFilter) String() string {
	return fmt.Sprintf("Equals(%q)", f.pattern)
}

func (f testEqualityFilter) Matches(id []byte) bool {
	return bytes.Equal(f.pattern, id)
}

type testMapTagsFilter struct {
	filters map[string]Filter
	iterFn  id.SortedTagIteratorFn
}

func newTestMapTagsFilter(
	tagFilters TagFilterValueMap,
	iterFn id.SortedTagIteratorFn,
) TagsFilter {
	filters := make(map[string]Filter, len(tagFilters))
	for name, value := range tagFilters {
		filter, _ := NewFilterFromFilterValue(value)
		filters[name] = filter
	}

	return &testMapTagsFilter{
		filters: filters,
		iterFn:  iterFn,
	}
}

func (f *testMapTagsFilter) String() string {
	return ""
}

func (f *testMapTagsFilter) Matches(id []byte, _ TagMatchOptions) (bool, error) {
	if len(f.filters) == 0 {
		return true, nil
	}

	matches := 0
	iter := f.iterFn(id)
	defer iter.Close()

	for iter.Next() {
		name, value := iter.Current()

		for n, filter := range f.filters {
			if !bytes.Equal([]byte(n), name) {
				continue
			}

			if filter.Matches(value) {
				matches++
				if matches == len(f.filters) {
					return true, nil
				}
				break
			}

			return false, nil
		}
	}
	if iter.Err() != nil {
		return false, iter.Err()
	}

	return matches == len(f.filters), nil
}

func newTestMultiCharRangeSelectFilter(pattern []byte, backwards bool) (chainFilter, error) {
	if len(pattern) == 0 {
		return nil, errInvalidFilterPattern
	}

	patterns := bytes.Split(pattern, multiRangeSplit)
	filters := make([]chainFilter, len(patterns))
	for i, p := range patterns {
		f, _ := newMultiCharSequenceFilter(p, backwards)
		filters[i] = f
	}

	return &testSelectChainFilter{
		filters: filters,
	}, nil
}

type testMultiCharRangeTrieFilter struct {
	b         *byteTrie
	backwards bool
}

func newTestMultiCharRangeTrieFilter(patterns []byte, backwards bool) (chainFilter, error) {
	if len(patterns) == 0 {
		return nil, errInvalidFilterPattern
	}

	b := &byteTrie{}
	for _, p := range bytes.Split(patterns, multiRangeSplit) {
		b.insert(p, backwards)
	}

	return &testMultiCharRangeTrieFilter{
		b:         b,
		backwards: backwards,
	}, nil
}

func (f *testMultiCharRangeTrieFilter) String() string {
	results := &traverseResults{}
	f.b.listTraverse(nil, results, f.backwards)
	return "Range(\"" + string(bytes.Join(results.vals, multiRangeSplit)) + "\")"
}

func (f *testMultiCharRangeTrieFilter) matches(val []byte) ([]byte, bool) {
	return f.b.lookup(val, f.backwards)
}

func validateRangeByScan(pattern, val []byte) (bool, error) {
	if len(pattern) == 0 {
		return false, errInvalidFilterPattern
	}

	// TODO(r): utf8 decode to ensure slicing is valid.

	negate := false
	if pattern[0] == negationChar {
		pattern = pattern[1:]
		if len(pattern) == 0 {
			return false, errInvalidFilterPattern
		}
		negate = true
	}

	if len(pattern) == 3 && pattern[1] == rangeChar {
		if pattern[0] >= pattern[2] {
			return false, errInvalidFilterPattern
		}

		match := val[0] >= pattern[0] && val[0] <= pattern[2]
		if negate {
			match = !match
		}

		return match, nil
	}

	match := false
	for i := 0; i < len(pattern); i++ {
		if val[0] == pattern[i] {
			match = true
			break
		}
	}

	if negate {
		match = !match
	}

	return match, nil
}

// testSelectChainFilter selects one of multiple filters with ||.
type testSelectChainFilter struct {
	filters []chainFilter
}

func (f *testSelectChainFilter) String() string {
	return ""
}

func (f *testSelectChainFilter) matches(val []byte) ([]byte, bool) {
	if len(f.filters) == 0 {
		return val, true
	}

	for _, filter := range f.filters {
		remainder, match := filter.matches(val)
		if match {
			return remainder, match
		}
	}

	return nil, false
}

// byteTrie is a trie for bytes that provides multi-direction inserts and lookups with
// early exit lookups for variable length input.
// It is not designed to take reads and writes concurrently.
type byteTrie struct {
	b        byte
	leaf     bool
	children []*byteTrie
}

func (t *byteTrie) insert(val []byte, backwards bool) {
	if len(val) == 0 {
		return
	}

	idx := 0
	remainder := val[1:]
	if backwards {
		idx = len(val) - 1
		remainder = val[:idx]
	}

	var child *byteTrie
	for _, c := range t.children {
		if c.b == val[idx] {
			child = c
			break
		}
	}

	if child == nil {
		child = &byteTrie{b: val[idx]}
		t.children = append(t.children, child)
	}

	if len(remainder) == 0 {
		child.leaf = true
		return
	}

	child.insert(remainder, backwards)
}

func (t *byteTrie) lookup(val []byte, backwards bool) ([]byte, bool) {
	if len(val) == 0 {
		return nil, false
	}

	idx := 0
	remainder := val[1:]
	if backwards {
		idx = len(val) - 1
		remainder = val[:idx]
	}

	for _, c := range t.children {
		if c.b == val[idx] {
			if c.leaf {
				return remainder, true
			}

			return c.lookup(remainder, backwards)
		}
	}

	return nil, false
}

type traverseResults struct {
	vals [][]byte
}

func (t *byteTrie) listTraverse(val []byte, results *traverseResults, backwards bool) {
	for _, c := range t.children {
		newVal := make([]byte, len(val)+1)
		if backwards {
			newVal[0] = c.b
			copy(newVal[1:], val)
		} else {
			copy(newVal, val)
			newVal[len(val)] = c.b
		}

		if c.leaf {
			results.vals = append(results.vals, newVal)
		}

		c.listTraverse(newVal, results, backwards)
	}
}
