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
	"fmt"
	"testing"
)

var (
	testFlatID           = "tagname1=tagvalue1,tagname3=tagvalue3,tagname4=tagvalue4,tagname2=tagvalue2,tagname6=tagvalue6,tagname5=tagvalue5,name=my.test.metric.name,tagname7=tagvalue7"
	testTagsFilterMapOne = map[string]string{
		"tagname1": "tagvalue1",
	}

	testTagsFilterMapThree = map[string]string{
		"tagname1":    "tagvalue1",
		"tagname2":    "tagvalue2",
		"tagname3":    "tagvalue3",
		"faketagname": "faketagvalue",
	}
)

func BenchmarkEquityFilter(b *testing.B) {
	f1 := newEqualityFilter("test")
	f2 := newEqualityFilter("test2")

	for n := 0; n < b.N; n++ {
		testUnionFilter("test", []Filter{f1, f2})
	}
}

func BenchmarkEquityFilterByValue(b *testing.B) {
	f1 := newTestEqualityFilter("test")
	f2 := newTestEqualityFilter("test2")

	for n := 0; n < b.N; n++ {
		testUnionFilter("test", []Filter{f1, f2})
	}
}

func BenchmarkTagsFilterOne(b *testing.B) {
	filter, _ := NewTagsFilter(testTagsFilterMapOne, NewMockSortedTagIterator, Conjunction)
	benchTagsFilter(b, testFlatID, filter)
}

func BenchmarkMapTagsFilterOne(b *testing.B) {
	benchTagsFilter(b, testFlatID, newTestMapTagsFilter(testTagsFilterMapOne, NewMockSortedTagIterator))
}

func BenchmarkTagsFilterThree(b *testing.B) {
	filter, _ := NewTagsFilter(testTagsFilterMapThree, NewMockSortedTagIterator, Conjunction)
	benchTagsFilter(b, testFlatID, filter)
}

func BenchmarkMapTagsFilterThree(b *testing.B) {
	benchTagsFilter(b, testFlatID, newTestMapTagsFilter(testTagsFilterMapThree, NewMockSortedTagIterator))
}

func BenchmarkRangeFilterStructsMatchRange(b *testing.B) {
	benchRangeFilterStructs(b, "a-z", "p", true)
}

func BenchmarkRangeFilterRangeMatchRange(b *testing.B) {
	benchRangeFilterRange(b, "a-z", "p", true)
}

func BenchmarkRangeFilterStructsNotMatchRange(b *testing.B) {
	benchRangeFilterStructs(b, "a-z", "P", false)
}

func BenchmarkRangeFilterRangeNotMatchRange(b *testing.B) {
	benchRangeFilterRange(b, "a-z", "P", false)
}

func BenchmarkRangeFilterStructsMatch(b *testing.B) {
	benchRangeFilterStructs(b, "02468", "6", true)
}

func BenchmarkRangeFilterRangeMatch(b *testing.B) {
	benchRangeFilterRange(b, "02468", "6", true)
}

func BenchmarkRangeFilterStructsNotMatch(b *testing.B) {
	benchRangeFilterStructs(b, "13579", "6", false)
}

func BenchmarkRangeFilterRangeNotMatch(b *testing.B) {
	benchRangeFilterRange(b, "13579", "6", false)
}

func BenchmarkRangeFilterStructsMatchNegation(b *testing.B) {
	benchRangeFilterStructs(b, "!a-z", "p", false)
}

func BenchmarkRangeFilterRangeMatchNegation(b *testing.B) {
	benchRangeFilterRange(b, "!a-z", "p", false)
}

func benchRangeFilterStructs(b *testing.B, pattern string, val string, expectedMatch bool) {
	f, _ := newSingleRangeFilter(pattern, false)
	for n := 0; n < b.N; n++ {
		match, _ := f.matches(val)
		if match != expectedMatch {
			b.FailNow()
		}
	}
}

func benchRangeFilterRange(b *testing.B, pattern string, val string, expectedMatch bool) {
	for n := 0; n < b.N; n++ {
		match, _ := validateRangeByScan(pattern, val)
		if match != expectedMatch {
			b.FailNow()
		}
	}
}

func benchTagsFilter(b *testing.B, id string, tagsFilter Filter) {
	for n := 0; n < b.N; n++ {
		tagsFilter.Matches(id)
	}
}

func testUnionFilter(val string, filters []Filter) bool {
	for _, filter := range filters {
		if !filter.Matches(val) {
			return false
		}
	}

	return true
}

type testEqualityFilter struct {
	pattern string
}

func newTestEqualityFilter(pattern string) Filter {
	return testEqualityFilter{pattern: pattern}
}

func (f testEqualityFilter) String() string {
	return fmt.Sprintf("Equals(%q)", f.pattern)
}

func (f testEqualityFilter) Matches(id string) bool {
	return f.pattern == id
}

type testMapTagsFilter struct {
	filters map[string]Filter
	iterFn  NewSortedTagIteratorFn
}

func newTestMapTagsFilter(tagFilters map[string]string, iterFn NewSortedTagIteratorFn) Filter {
	filters := make(map[string]Filter, len(tagFilters))
	for name, value := range tagFilters {
		filter, _ := NewFilter(value)
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

func (f *testMapTagsFilter) Matches(id string) bool {
	if len(f.filters) == 0 {
		return true
	}

	matches := 0
	iter := f.iterFn(id)
	defer iter.Close()

	for iter.Next() {
		name, value := iter.Current()

		if filter, exists := f.filters[name]; exists {
			if filter.Matches(value) {
				matches++
				if matches == len(f.filters) {
					return true
				}
				continue
			}

			return false
		}
	}

	return iter.Err() == nil && matches == len(f.filters)
}

func validateRangeByScan(pattern string, val string) (bool, error) {
	if len(pattern) == 0 {
		return false, errInvalidFilterPattern
	}

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
