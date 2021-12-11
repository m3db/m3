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
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseTagFilterValueMap(t *testing.T) {
	inputs := []struct {
		str      string
		expected TagFilterValueMap
	}{
		{
			str: "tagName1:tagValue1",
			expected: TagFilterValueMap{
				"tagName1": FilterValue{Pattern: "tagValue1", Negate: false},
			},
		},
		{
			str: "tagName1:tagValue1 tagName2:tagValue2",
			expected: TagFilterValueMap{
				"tagName1": FilterValue{Pattern: "tagValue1", Negate: false},
				"tagName2": FilterValue{Pattern: "tagValue2", Negate: false},
			},
		},
		{
			str: "  tagName1:tagValue1    tagName2:tagValue2   tagName3:tagValue3  tagName4:tagValue4",
			expected: TagFilterValueMap{
				"tagName1": FilterValue{Pattern: "tagValue1", Negate: false},
				"tagName2": FilterValue{Pattern: "tagValue2", Negate: false},
				"tagName3": FilterValue{Pattern: "tagValue3", Negate: false},
				"tagName4": FilterValue{Pattern: "tagValue4", Negate: false},
			},
		},
	}

	for _, input := range inputs {
		res, err := ParseTagFilterValueMap(input.str)
		require.NoError(t, err)
		require.Equal(t, input.expected, res)
	}
}

func TestParseTagFilterValueMapErrors(t *testing.T) {
	inputs := []string{
		"tagName1=tagValue1",
		"tagName1:tagValue1 tagName2~=tagValue2",
		"tagName1:tagValue1  tagName2:tagValue2 tagName1:tagValue3",
		"tagName:",
		":tagValue",
	}

	for _, input := range inputs {
		_, err := ParseTagFilterValueMap(input)
		require.Error(t, err)
	}
}

func TestEmptyTagsFilterMatches(t *testing.T) {
	f, err := NewTagsFilter(nil, Conjunction, testTagsFilterOptions())
	require.NoError(t, err)
	matches, err := f.Matches([]byte("foo"), testTagsMatchOptions())
	require.NoError(t, err)
	require.True(t, matches)
}

func TestTagsFilterMatchesNoNameTag(t *testing.T) {
	filters := map[string]FilterValue{
		"tagName1": FilterValue{Pattern: "tagValue1"},
		"tagName2": FilterValue{Pattern: "tagValue2"},
	}
	f, err := NewTagsFilter(filters, Conjunction, testTagsFilterOptions())
	inputs := []mockFilterData{
		{val: "tagName1=tagValue1,tagName2=tagValue2", match: true},
		{val: "tagName0=tagValue0,tagName1=tagValue1,tagName2=tagValue2,tagName3=tagValue3", match: true},
		{val: "tagName1=tagValue1", match: false},
		{val: "tagName2=tagValue2", match: false},
		{val: "tagName1=tagValue2,tagName2=tagValue1", match: false},
	}
	require.NoError(t, err)
	for _, input := range inputs {
		matches, err := f.Matches([]byte(input.val), testTagsMatchOptions())
		require.NoError(t, err)
		require.Equal(t, input.match, matches)
	}

	f, err = NewTagsFilter(filters, Disjunction, testTagsFilterOptions())
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
		matches, err := f.Matches([]byte(input.val), testTagsMatchOptions())
		require.NoError(t, err)
		require.Equal(t, input.match, matches, "val:", input.val)
	}
}

func TestTagsFilterMatchesWithNameTag(t *testing.T) {
	filters := map[string]FilterValue{
		"name":     FilterValue{Pattern: "foo"},
		"tagName1": FilterValue{Pattern: "tagValue1"},
		"tagName2": FilterValue{Pattern: "tagValue2"},
	}

	f, err := NewTagsFilter(filters, Conjunction, testTagsFilterOptions())
	require.NoError(t, err)
	inputs := []mockFilterData{
		{val: "foo+tagName0=tagValue0,tagName1=tagValue1,tagName2=tagValue2", match: true},
		{val: "tagName1=tagValue1,tagName2=tagValue2", match: false, err: errInvalidMetric},
		{val: "foo+tagName1=tagValue1", match: false},
		{val: "foo+tagName1=tagValue2,tagName2=tagValue1", match: false},
	}
	for _, input := range inputs {
		matches, err := f.Matches([]byte(input.val), testTagsMatchOptionsWithNameTag())
		if input.err != nil {
			require.True(t, errors.Is(err, input.err))
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, input.match, matches)
	}

	f, err = NewTagsFilter(filters, Disjunction, testTagsFilterOptions())
	require.NoError(t, err)
	inputs = []mockFilterData{
		{val: "foo+tagName1=tagValue1,tagName2=tagValue2", match: true},
		{val: "foo+tagName1=tagValue2,tagName2=tagValue2", match: true},
		{val: "bar+tagName1=tagValue1", match: true},
		{val: "foo+tagName1=tagValue2", match: true},
		{val: "foo+tagName2=tagValue1", match: true},
		{val: "foo+tagName15=tagValue2,tagName3=tagValue2", match: true},
		{val: "tagName1=tagValue1,tagName2=tagValue2", match: false, err: errInvalidMetric},
		{val: "bar+tagName1=tagValue2,tagName2=tagValue1", match: false},
		{val: "bar+tagName3=tagValue3", match: false},
	}
	for _, input := range inputs {
		matches, err := f.Matches([]byte(input.val), testTagsMatchOptionsWithNameTag())
		if input.err != nil {
			require.True(t, errors.Is(err, input.err))
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, input.match, matches)
	}
}

func TestTagsFilterStringNoNameTag(t *testing.T) {
	filters := map[string]FilterValue{
		"tagName1": FilterValue{Pattern: "tagValue1"},
		"tagName2": FilterValue{Pattern: "tagValue2"},
	}
	f, err := NewTagsFilter(filters, Conjunction, testTagsFilterOptions())
	require.NoError(t, err)
	tf := f.(*tagsFilter)
	require.Equal(t, `tagName1:Equals("tagValue1") && tagName2:Equals("tagValue2")`, tf.String())

	f, err = NewTagsFilter(filters, Disjunction, testTagsFilterOptions())
	require.NoError(t, err)
	tf = f.(*tagsFilter)
	require.Equal(t, `tagName1:Equals("tagValue1") || tagName2:Equals("tagValue2")`, tf.String())
}

func TestTagsFilterStringWithNameTag(t *testing.T) {
	filters := map[string]FilterValue{
		"name":     FilterValue{Pattern: "foo"},
		"tagName1": FilterValue{Pattern: "tagValue1"},
		"tagName2": FilterValue{Pattern: "tagValue2"},
	}
	f, err := NewTagsFilter(filters, Conjunction, testTagsFilterOptions())
	require.NoError(t, err)
	tf := f.(*tagsFilter)
	require.Equal(t, `name:Equals("foo") && tagName1:Equals("tagValue1") && tagName2:Equals("tagValue2")`, tf.String())

	f, err = NewTagsFilter(filters, Disjunction, testTagsFilterOptions())
	require.NoError(t, err)
	tf = f.(*tagsFilter)
	require.Equal(t, `name:Equals("foo") || tagName1:Equals("tagValue1") || tagName2:Equals("tagValue2")`, tf.String())
}

func TestValidateTagsFilter(t *testing.T) {
	inputs := []struct {
		str      string
		expected TagFilterValueMap
	}{
		{
			str: "tagName1:tagValue1",
			expected: TagFilterValueMap{
				"tagName1": FilterValue{Pattern: "tagValue1", Negate: false},
			},
		},
		{
			str: "tagName1:tagValue1 tagName2:tagValue2*tagValue3",
			expected: TagFilterValueMap{
				"tagName1": FilterValue{Pattern: "tagValue1", Negate: false},
				"tagName2": FilterValue{Pattern: "tagValue2*tagValue3", Negate: false},
			},
		},
		{
			str: "  tagName1:tagValue1?[0-9][!a-z]9    tagName2:{tagValue21,tagValue22}*   tagName3:tagValue3  tagName4:tagValue4",
			expected: TagFilterValueMap{
				"tagName1": FilterValue{Pattern: "tagValue1?[0-9][!a-z]9", Negate: false},
				"tagName2": FilterValue{Pattern: "{tagValue21,tagValue22}*", Negate: false},
				"tagName3": FilterValue{Pattern: "tagValue3", Negate: false},
				"tagName4": FilterValue{Pattern: "tagValue4", Negate: false},
			},
		},
	}

	for _, input := range inputs {
		res, err := ValidateTagsFilter(input.str)
		require.NoError(t, err)
		require.Equal(t, input.expected, res)
	}
}

func TestValidateTagsFilterError(t *testing.T) {
	inputs := []struct {
		str string
		err string
	}{
		{
			str: "tagName1=tagValue1",
			err: "tags filter tagName1=tagValue1 is malformed",
		},
		{
			str: "tagName1:tagValue1 tagName2~=tagValue2",
			err: "tags filter tagName1:tagValue1 tagName2~=tagValue2 is malformed",
		},
		{
			str: "tagName1:tagValue1  tagName2:tagValue2 tagName1:tagValue3",
			err: "tags filter tagName1:tagValue1  tagName2:tagValue2 tagName1:tagValue3 is malformed",
		},
		{
			str: "tagName1:*too*many*",
			err: "tags filter tagName1:*too*many* contains invalid filter pattern *too*many* for tag tagName1",
		},
		{
			str: "tagName1:abcsdf tagName2:*con[tT]ains*",
			err: "tags filter tagName1:abcsdf tagName2:*con[tT]ains* contains invalid filter pattern *con[tT]ains* for tag tagName2",
		},
	}

	for _, input := range inputs {
		_, err := ValidateTagsFilter(input.str)
		require.Error(t, err)
		require.True(t, strings.Contains(err.Error(), input.err))
	}
}

func testTagsMatchOptions() TagMatchOptions {
	return TagMatchOptions{
		NameAndTagsFn:       func(b []byte) ([]byte, []byte, error) { return nil, b, nil },
		SortedTagIteratorFn: NewMockSortedTagIterator,
	}
}

func testTagsFilterOptions() TagsFilterOptions {
	return TagsFilterOptions{
		NameTagKey: []byte("name"),
	}
}

var errInvalidMetric = errors.New("invalid metric")

func testTagsMatchOptionsWithNameTag() TagMatchOptions {
	return TagMatchOptions{
		NameAndTagsFn: func(b []byte) ([]byte, []byte, error) {
			idx := bytes.IndexByte(b, '+')
			if idx == -1 {
				return nil, nil, errInvalidMetric
			}
			return b[:idx], b[idx+1:], nil
		},
		SortedTagIteratorFn: NewMockSortedTagIterator,
	}
}
