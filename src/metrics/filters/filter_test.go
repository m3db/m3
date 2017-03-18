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

	"github.com/stretchr/testify/require"
)

func TestEqualityFilter(t *testing.T) {
	inputs := []mockFilterData{
		{val: "foo", match: true},
		{val: "fo", match: false},
		{val: "foob", match: false},
	}
	f := newEqualityFilter("foo")
	for _, input := range inputs {
		require.Equal(t, input.match, f.Matches(input.val))
	}
}

func TestEmptyFilter(t *testing.T) {
	f, err := NewFilter("")
	require.NoError(t, err)
	require.True(t, f.Matches(""))
	require.False(t, f.Matches(" "))
	require.False(t, f.Matches("foo"))
}

func TestWildcardFilters(t *testing.T) {
	filters := genAndValidateFilters(t, []testPattern{
		testPattern{pattern: "foo", expectedStr: "Equals(\"foo\")"},
		testPattern{pattern: "*bar", expectedStr: "EndsWith(\"bar\")"},
		testPattern{pattern: "baz*", expectedStr: "StartsWith(\"baz\")"},
		testPattern{pattern: "*cat*", expectedStr: "Contains(\"cat\")"},
		testPattern{pattern: "foo*bar", expectedStr: "StartsWith(\"foo\") && EndsWith(\"bar\")"},
		testPattern{pattern: "*", expectedStr: "All"},
	})

	inputs := []testInput{
		newTestInput("foo", true, false, false, false, false, true),
		newTestInput("foobar", false, true, false, false, true, true),
		newTestInput("foozapbar", false, true, false, false, true, true),
		newTestInput("bazbar", false, true, true, false, false, true),
		newTestInput("bazzzbar", false, true, true, false, false, true),
		newTestInput("cat", false, false, false, true, false, true),
		newTestInput("catbar", false, true, false, true, false, true),
		newTestInput("baztestcat", false, false, true, true, false, true),
		newTestInput("foocatbar", false, true, false, true, true, true),
		newTestInput("footestcatbar", false, true, false, true, true, true),
	}

	for _, input := range inputs {
		for i, expectedMatch := range input.matches {
			require.Equal(t, expectedMatch, filters[i].Matches(input.val),
				fmt.Sprintf("input: %s, pattern: %s", input.val, filters[i].String()))
		}
	}
}

func TestMultiFilter(t *testing.T) {
	filters := []Filter{
		newMultiFilter([]Filter{}, Conjunction),
		newMultiFilter([]Filter{}, Disjunction),
		newMultiFilter([]Filter{newEqualityFilter("foo")}, Conjunction),
		newMultiFilter([]Filter{newEqualityFilter("foo")}, Disjunction),
		newMultiFilter([]Filter{newEqualityFilter("foo"), newEndsWithFilter("bar")}, Conjunction),
		newMultiFilter([]Filter{newEqualityFilter("foo"), newEndsWithFilter("bar")}, Disjunction),
	}

	inputs := []testInput{
		newTestInput("cat", true, true, false, false, false, false),
		newTestInput("foo", true, true, true, true, false, true),
		newTestInput("foobar", true, true, false, false, false, true),
		newTestInput("bar", true, true, false, false, false, true),
	}

	for _, input := range inputs {
		for i, expectedMatch := range input.matches {
			require.Equal(t, expectedMatch, filters[i].Matches(input.val),
				fmt.Sprintf("input: %s, pattern: %s", input.val, filters[i].String()))
		}
	}
}

func TestNegationFilter(t *testing.T) {
	filters := genAndValidateFilters(t, []testPattern{
		testPattern{pattern: "!foo", expectedStr: "Not(Equals(\"foo\"))"},
		testPattern{pattern: "!*bar", expectedStr: "Not(EndsWith(\"bar\"))"},
		testPattern{pattern: "!baz*", expectedStr: "Not(StartsWith(\"baz\"))"},
		testPattern{pattern: "!*cat*", expectedStr: "Not(Contains(\"cat\"))"},
		testPattern{pattern: "!foo*bar", expectedStr: "Not(StartsWith(\"foo\") && EndsWith(\"bar\"))"},
		testPattern{pattern: "foo!", expectedStr: "Equals(\"foo!\")"},
	})

	inputs := []testInput{
		newTestInput("foo", false, true, true, true, true, false),
		newTestInput("foo!", true, true, true, true, true, true),
		newTestInput("foobar", true, false, true, true, false, false),
		newTestInput("bazbar", true, false, false, true, true, false),
		newTestInput("cat", true, true, true, false, true, false),
		newTestInput("catbar", true, false, true, false, true, false),
		newTestInput("baztestcat", true, true, false, false, true, false),
		newTestInput("foocatbar", true, false, true, false, false, false),
		newTestInput("footestcatbar", true, false, true, false, false, false),
	}

	for _, input := range inputs {
		for i, expectedMatch := range input.matches {
			require.Equal(t, expectedMatch, filters[i].Matches(input.val),
				fmt.Sprintf("input: %s, pattern: %s", input.val, filters[i].String()))
		}
	}
}

func TestBadPatterns(t *testing.T) {
	patterns := []string{
		"**",
		"***",
		"*too*many*",
		"*too**many",
		"to*o*many",
		"to*o*ma*ny",
	}

	for _, pattern := range patterns {
		_, err := NewFilter(pattern)
		require.Error(t, err)
	}
}

type testPattern struct {
	pattern     string
	expectedStr string
}

type testInput struct {
	val     string
	matches []bool
}

func newTestInput(val string, matches ...bool) testInput {
	return testInput{val: val, matches: matches}
}

func genAndValidateFilters(t *testing.T, patterns []testPattern) []Filter {
	var err error
	filters := make([]Filter, len(patterns))
	for i, pattern := range patterns {
		filters[i], err = NewFilter(pattern.pattern)
		require.NoError(t, err, fmt.Sprintf("No error expected, but got: %v for pattern: %s", err, pattern.pattern))
		require.Equal(t, pattern.expectedStr, filters[i].String())
	}

	return filters
}
