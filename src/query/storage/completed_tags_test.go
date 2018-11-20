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

package storage

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func strsToBytes(str []string) [][]byte {
	b := make([][]byte, len(str))
	for i, s := range str {
		b[i] = []byte(s)
	}

	return b
}

func mapToCompletedTag(nameOnly bool, m map[string][]string) CompleteTagsResult {
	tags := make([]CompletedTag, 0, len(m))
	for k, v := range m {
		tags = append(tags, CompletedTag{
			Name:   []byte(k),
			Values: strsToBytes(v),
		})
	}

	sort.Sort(completedTagsByName(tags))
	return CompleteTagsResult{
		CompleteNameOnly: nameOnly,
		CompletedTags:    tags,
	}
}

func TestFinalizeCompletedTag(t *testing.T) {
	initialVals := []string{"a", "z", "b", "z"}
	expected := []string{"a", "b", "z"}

	builder := completedTagBuilder{}
	builder.add(strsToBytes(initialVals))
	actual := builder.build()

	assert.Equal(t, strsToBytes(expected), actual)
}

func TestMergeCompletedTag(t *testing.T) {
	initialVals := []string{"a", "z", "b"}
	secondVals := []string{"a", "ab"}
	thirdVals := []string{"ab", "c", "d"}
	expected := []string{"a", "ab", "b", "c", "d", "z"}

	builder := completedTagBuilder{}
	builder.add(strsToBytes(initialVals))
	builder.add(strsToBytes(secondVals))
	builder.add(strsToBytes(thirdVals))
	actual := builder.build()

	assert.Equal(t, strsToBytes(expected), actual)
}

func TestMergeCompletedTagResultDifferentNameTypes(t *testing.T) {
	nameOnlyVals := []bool{true, false}
	for _, nameOnly := range nameOnlyVals {
		builder := NewCompleteTagsResultBuilder(nameOnly)
		err := builder.Add(&CompleteTagsResult{
			CompleteNameOnly: !nameOnly,
		})

		assert.Error(t, err)
	}
}

func TestMergeEmptyCompletedTagResult(t *testing.T) {
	nameOnlyVals := []bool{true, false}
	for _, nameOnly := range nameOnlyVals {
		builder := NewCompleteTagsResultBuilder(nameOnly)
		actual := builder.Build()
		expected := CompleteTagsResult{
			CompleteNameOnly: nameOnly,
			CompletedTags:    []CompletedTag{},
		}

		assert.Equal(t, expected, actual)
	}
}

var testMergeCompletedTags = []struct {
	name             string
	incoming         []map[string][]string
	expected         map[string][]string
	expectedNameOnly map[string][]string
}{
	{
		"no tag",
		[]map[string][]string{},
		map[string][]string{},
		map[string][]string{},
	},
	{
		"single tag",
		[]map[string][]string{
			{"a": []string{"a", "b", "c"}},
		},
		map[string][]string{
			"a": []string{"a", "b", "c"},
		},
		map[string][]string{
			"a": []string{},
		},
	},
	{
		"multiple distinct tags",
		[]map[string][]string{
			{"b": []string{"d", "e", "f"}},
			{"a": []string{"a", "b", "c"}},
		},
		map[string][]string{
			"a": []string{"a", "b", "c"},
			"b": []string{"d", "e", "f"},
		},
		map[string][]string{
			"a": []string{},
			"b": []string{},
		},
	},
	{
		"multiple tags with distinct values",
		[]map[string][]string{
			{"a": []string{"a", "b", "c"}},
			{"a": []string{"d", "e", "f"}},
		},
		map[string][]string{
			"a": []string{"a", "b", "c", "d", "e", "f"},
		},
		map[string][]string{
			"a": []string{},
		},
	},
	{
		"multiple tags with same values",
		[]map[string][]string{
			{"a": []string{"a", "b", "c"}},
			{"a": []string{"c", "b", "a"}},
			{"a": []string{"a", "b", "c"}},
			{"b": []string{"d", "e", "f"}},
			{"b": []string{"g", "z", "a"}},
		},
		map[string][]string{
			"a": []string{"a", "b", "c"},
			"b": []string{"a", "d", "e", "f", "g", "z"},
		},
		map[string][]string{
			"a": []string{},
			"b": []string{},
		},
	},
}

func TestMergeCompletedTagResult(t *testing.T) {
	nameOnlyVals := []bool{true, false}
	for _, nameOnly := range nameOnlyVals {
		for _, tt := range testMergeCompletedTags {
			t.Run(fmt.Sprintf("%s_%t", tt.name, nameOnly), func(t *testing.T) {
				builder := NewCompleteTagsResultBuilder(nameOnly)
				for _, incoming := range tt.incoming {
					result := mapToCompletedTag(nameOnly, incoming)
					err := builder.Add(&result)
					assert.NoError(t, err)
				}

				actual := builder.Build()
				exResult := tt.expected
				if nameOnly {
					exResult = tt.expectedNameOnly
				}

				expected := mapToCompletedTag(nameOnly, exResult)
				for _, tags := range expected.CompletedTags {
					for _, val := range tags.Values {
						fmt.Println(string(tags.Name), string(val))
					}
				}

				assert.Equal(t, expected, actual)
			})
		}
	}
}
