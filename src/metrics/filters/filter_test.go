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

func TestNewFilterFromFilterValueInvalidPattern(t *testing.T) {
	inputs := []string{"ab]c[sdf", "abc[z-a]", "*con[tT]ains*"}
	for _, input := range inputs {
		_, err := NewFilterFromFilterValue(FilterValue{Pattern: input})
		require.Error(t, err)
	}
}

func TestNewFilterFromFilterValueWithNegation(t *testing.T) {
	inputs := []struct {
		pattern string
		negate  bool
		data    []mockFilterData
	}{
		{
			pattern: "foo",
			negate:  false,
			data: []mockFilterData{
				{val: "foo", match: true},
				{val: "fo", match: false},
			},
		},
		{
			pattern: "foo*bar",
			negate:  false,
			data: []mockFilterData{
				{val: "foobar", match: true},
				{val: "foozapbar", match: true},
				{val: "bazbar", match: false},
			},
		},
		{
			pattern: "ba?[0-9][!a-z]9",
			negate:  false,
			data: []mockFilterData{
				{val: "bar959", match: true},
				{val: "bat449", match: true},
				{val: "bar9", match: false},
			},
		},
		{
			pattern: "{ba,fo,car}*",
			negate:  false,
			data: []mockFilterData{
				{val: "ba", match: true},
				{val: "foo", match: true},
				{val: "car", match: true},
				{val: "ca", match: false},
			},
		},
		{
			pattern: "foo",
			negate:  true,
			data: []mockFilterData{
				{val: "foo", match: false},
				{val: "fo", match: true},
			},
		},
		{
			pattern: "foo*bar",
			negate:  true,
			data: []mockFilterData{
				{val: "foobar", match: false},
				{val: "foozapbar", match: false},
				{val: "bazbar", match: true},
			},
		},
		{
			pattern: "ba?[0-9][!a-z]9",
			negate:  true,
			data: []mockFilterData{
				{val: "bar959", match: false},
				{val: "bat449", match: false},
				{val: "bar9", match: true},
			},
		},
		{
			pattern: "{ba,fo,car}*",
			negate:  true,
			data: []mockFilterData{
				{val: "ba", match: false},
				{val: "foo", match: false},
				{val: "car", match: false},
				{val: "ca", match: true},
			},
		},
	}

	for _, input := range inputs {
		f, err := NewFilterFromFilterValue(FilterValue{Pattern: input.pattern, Negate: input.negate})
		require.NoError(t, err)
		for _, testcase := range input.data {
			require.Equal(t, testcase.match, f.Matches([]byte(testcase.val)))
		}
	}
}

func TestFilters(t *testing.T) {
	filters := genAndValidateFilters(t, []testPattern{
		testPattern{pattern: "f[A-z]?*", expectedStr: "StartsWith(Equals(\"f\") then Range(\"A-z\") then AnyChar)"},
		testPattern{pattern: "*ba[a-z]", expectedStr: "EndsWith(Equals(\"ba\") then Range(\"a-z\"))"},
		testPattern{pattern: "fo*?ba[!0-9][0-9]{8,9}", expectedStr: "StartsWith(Equals(\"fo\")) && EndsWith(AnyChar then Equals(\"ba\") then Not(Range(\"0-9\")) then Range(\"0-9\") then Range(\"8,9\"))"},
	})

	inputs := []testInput{
		newTestInput("foo", true, false, false),
		newTestInput("test", false, false, false),
		newTestInput("bar", false, true, false),
		newTestInput("foobar", true, true, false),
		newTestInput("foobar08", true, false, true),
		newTestInput("footybar09", true, false, true),
	}

	for _, input := range inputs {
		for i, expectedMatch := range input.matches {
			require.Equal(t, expectedMatch, filters[i].Matches(input.val),
				fmt.Sprintf("input: %s, pattern: %s", input.val, filters[i].String()))
		}
	}
}

func TestEqualityFilter(t *testing.T) {
	inputs := []mockFilterData{
		{val: "foo", match: true},
		{val: "fo", match: false},
		{val: "foob", match: false},
	}
	f := newEqualityFilter([]byte("foo"))
	for _, input := range inputs {
		require.Equal(t, input.match, f.Matches([]byte(input.val)))
	}
}

func TestEmptyFilter(t *testing.T) {
	f, err := NewFilter(nil)
	require.NoError(t, err)
	require.True(t, f.Matches([]byte("")))
	require.False(t, f.Matches([]byte(" ")))
	require.False(t, f.Matches([]byte("foo")))
}

func TestWildcardFilters(t *testing.T) {
	filters := genAndValidateFilters(t, []testPattern{
		testPattern{pattern: "foo", expectedStr: "Equals(\"foo\")"},
		testPattern{pattern: "*bar", expectedStr: "EndsWith(Equals(\"bar\"))"},
		testPattern{pattern: "baz*", expectedStr: "StartsWith(Equals(\"baz\"))"},
		testPattern{pattern: "*cat*", expectedStr: "Contains(\"cat\")"},
		testPattern{pattern: "foo*bar", expectedStr: "StartsWith(Equals(\"foo\")) && EndsWith(Equals(\"bar\"))"},
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

func TestRangeFilters(t *testing.T) {
	filters := genAndValidateFilters(t, []testPattern{
		testPattern{pattern: "fo[a-zA-Z0-9]", expectedStr: "Equals(\"fo\") then Range(\"a-z || A-Z || 0-9\")"},
		testPattern{pattern: "f[o-o]?", expectedStr: "Equals(\"f\") then Range(\"o-o\") then AnyChar"},
		testPattern{pattern: "???", expectedStr: "AnyChar then AnyChar then AnyChar"},
		testPattern{pattern: "ba?", expectedStr: "Equals(\"ba\") then AnyChar"},
		testPattern{pattern: "[!cC]ar", expectedStr: "Not(Range(\"cC\")) then Equals(\"ar\")"},
		testPattern{pattern: "ba?[0-9][!a-z]9", expectedStr: "Equals(\"ba\") then AnyChar then Range(\"0-9\") then Not(Range(\"a-z\")) then Equals(\"9\")"},
		testPattern{pattern: "{ba,fo,car}*", expectedStr: "StartsWith(Range(\"ba,fo,car\"))"},
		testPattern{pattern: "ba{r,t}*[!a-zA-Z]", expectedStr: "StartsWith(Equals(\"ba\") then Range(\"r,t\")) && EndsWith(Not(Range(\"a-z || A-Z\")))"},
		testPattern{pattern: "*{9}", expectedStr: "EndsWith(Range(\"9\"))"},
	})

	inputs := []testInput{
		newTestInput("foo", true, true, true, false, false, false, true, false, false),
		newTestInput("fo!", false, true, true, false, false, false, true, false, false),
		newTestInput("boo", false, false, true, false, false, false, false, false, false),
		newTestInput("bar", false, false, true, true, true, false, true, false, false),
		newTestInput("Bar", false, false, true, false, true, false, false, false, false),
		newTestInput("car", false, false, true, false, false, false, true, false, false),
		newTestInput("bar9", false, false, false, false, false, false, true, true, true),
		newTestInput("bar990", false, false, false, false, false, false, true, true, false),
		newTestInput("bar959", false, false, false, false, false, true, true, true, true),
		newTestInput("bar009", false, false, false, false, false, true, true, true, true),
		newTestInput("bat449", false, false, false, false, false, true, true, true, true),
	}

	for _, input := range inputs {
		for i, expectedMatch := range input.matches {
			require.Equal(t, expectedMatch, filters[i].Matches(input.val),
				fmt.Sprintf("input: %s, pattern: %s", input.val, filters[i].String()))
		}
	}
}

func TestMultiFilter(t *testing.T) {
	cf, _ := newContainsFilter([]byte("bar"))
	filters := []Filter{
		NewMultiFilter([]Filter{}, Conjunction),
		NewMultiFilter([]Filter{}, Disjunction),
		NewMultiFilter([]Filter{newEqualityFilter([]byte("foo"))}, Conjunction),
		NewMultiFilter([]Filter{newEqualityFilter([]byte("foo"))}, Disjunction),
		NewMultiFilter([]Filter{newEqualityFilter([]byte("foo")), cf}, Conjunction),
		NewMultiFilter([]Filter{newEqualityFilter([]byte("foo")), cf}, Disjunction),
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
		testPattern{pattern: "!*bar", expectedStr: "Not(EndsWith(Equals(\"bar\")))"},
		testPattern{pattern: "!baz*", expectedStr: "Not(StartsWith(Equals(\"baz\")))"},
		testPattern{pattern: "!*cat*", expectedStr: "Not(Contains(\"cat\"))"},
		testPattern{pattern: "!foo*bar", expectedStr: "Not(StartsWith(Equals(\"foo\")) && EndsWith(Equals(\"bar\")))"},
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
		"!", // negation of nothing is everything, so user should use *.
		"**",
		"***",
		"*too*many*",
		"*too**many",
		"to*o*many",
		"to*o*ma*ny",
		"abc[sdf",
		"ab]c[sdf",
		"abc[z-a]",
		"*con[tT]ains*",
		"*con{tT}ains*",
		"*con?ains*",
		"abc[a-zA-Z0-]",
		"abc[a-zA-Z0]",
		"abc[a-zZ-A]",
		"ab}c{sdf",
		"ab{}sdf",
		"ab[]sdf",
	}

	for _, pattern := range patterns {
		_, err := NewFilter([]byte(pattern))
		require.Error(t, err, fmt.Sprintf("pattern: %s", pattern))
	}
}

func TestMultiCharSequenceFilter(t *testing.T) {
	_, err := newMultiCharSequenceFilter([]byte(""), false)
	require.Error(t, err)

	f, err := newMultiCharSequenceFilter([]byte("test2,test,tent,book"), false)
	require.NoError(t, err)
	validateLookup(t, f, "", false, "")
	validateLookup(t, f, "t", false, "")
	validateLookup(t, f, "tes", false, "")
	validateLookup(t, f, "tset", false, "")

	validateLookup(t, f, "test", true, "")
	validateLookup(t, f, "tent", true, "")
	validateLookup(t, f, "test3", true, "3")
	validateLookup(t, f, "test2", true, "")
	validateLookup(t, f, "book123", true, "123")

	f, err = newMultiCharSequenceFilter([]byte("test2,test,tent,book"), true)
	require.NoError(t, err)
	validateLookup(t, f, "", false, "")
	validateLookup(t, f, "t", false, "")
	validateLookup(t, f, "tes", false, "")
	validateLookup(t, f, "test3", false, "")

	validateLookup(t, f, "test", true, "")
	validateLookup(t, f, "tent", true, "")
	validateLookup(t, f, "test2", true, "")
	validateLookup(t, f, "12test", true, "12")
	validateLookup(t, f, "12test2", true, "12")
	validateLookup(t, f, "123book", true, "123")
}

func validateLookup(t *testing.T, f chainFilter, val string, expectedMatch bool, expectedRemainder string) {
	remainder, match := f.matches([]byte(val))
	require.Equal(t, expectedMatch, match)
	require.Equal(t, expectedRemainder, string(remainder))
}

type mockFilterData struct {
	val   string
	match bool
	err   error
}

type testPattern struct {
	pattern     string
	expectedStr string
}

type testInput struct {
	val     []byte
	matches []bool
}

func newTestInput(val string, matches ...bool) testInput {
	return testInput{val: []byte(val), matches: matches}
}

func genAndValidateFilters(t *testing.T, patterns []testPattern) []Filter {
	var err error
	filters := make([]Filter, len(patterns))
	for i, pattern := range patterns {
		filters[i], err = NewFilter([]byte(pattern.pattern))
		require.NoError(t, err, fmt.Sprintf("No error expected, but got: %v for pattern: %s", err, pattern.pattern))
		require.Equal(t, pattern.expectedStr, filters[i].String())
	}

	return filters
}
