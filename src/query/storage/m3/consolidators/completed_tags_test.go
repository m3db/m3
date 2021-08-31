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

package consolidators

import (
	"fmt"
	"sort"
	"testing"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		Metadata:         block.NewResultMetadata(),
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
		builder := NewCompleteTagsResultBuilder(nameOnly, models.NewTagOptions())
		err := builder.Add(&CompleteTagsResult{
			CompleteNameOnly: !nameOnly,
		})

		assert.Error(t, err)
	}
}

func TestMergeEmptyCompletedTagResult(t *testing.T) {
	nameOnlyVals := []bool{true, false}
	for _, nameOnly := range nameOnlyVals {
		builder := NewCompleteTagsResultBuilder(nameOnly, models.NewTagOptions())
		actual := builder.Build()
		expected := CompleteTagsResult{
			CompleteNameOnly: nameOnly,
			CompletedTags:    []CompletedTag{},
			Metadata:         block.NewResultMetadata(),
		}

		assert.Equal(t, expected, actual)
	}
}

var testMergeCompletedTags = []struct {
	name                         string
	incoming                     []map[string][]string
	expected                     map[string][]string
	expectedNameOnly             map[string][]string
	expectedMetdataCount         int
	expectedNameOnlyMetdataCount int
}{
	{
		"no tag",
		[]map[string][]string{},
		map[string][]string{},
		map[string][]string{},
		0, 0,
	},
	{
		"single tag",
		[]map[string][]string{
			{"a": {"a", "b", "c"}},
		},
		map[string][]string{
			"a": {"a", "b", "c"},
		},
		map[string][]string{
			"a": {},
		},
		3, 1,
	},
	{
		"multiple distinct tags",
		[]map[string][]string{
			{"b": {"d", "e", "f"}},
			{"a": {"a", "b", "c"}},
		},
		map[string][]string{
			"a": {"a", "b", "c"},
			"b": {"d", "e", "f"},
		},
		map[string][]string{
			"a": {},
			"b": {},
		},
		6, 2,
	},
	{
		"multiple tags with distinct values",
		[]map[string][]string{
			{"a": {"a", "b", "c"}},
			{"a": {"d", "e", "f"}},
		},
		map[string][]string{
			"a": {"a", "b", "c", "d", "e", "f"},
		},
		map[string][]string{
			"a": {},
		},
		6, 1,
	},
	{
		"multiple tags with same values",
		[]map[string][]string{
			{"a": {"a", "b", "c"}},
			{"a": {"c", "b", "a"}},
			{"a": {"a", "b", "c"}},
			{"b": {"d", "e", "f"}},
			{"b": {"g", "z", "a"}},
		},
		map[string][]string{
			"a": {"a", "b", "c"},
			"b": {"a", "d", "e", "f", "g", "z"},
		},
		map[string][]string{
			"a": {},
			"b": {},
		},
		9, 2,
	},
}

func TestMergeCompletedTagResult(t *testing.T) {
	nameOnlyVals := []bool{true, false}
	for _, nameOnly := range nameOnlyVals {
		nameOnly := nameOnly
		for _, tt := range testMergeCompletedTags {
			t.Run(fmt.Sprintf("%s_%t", tt.name, nameOnly), func(t *testing.T) {
				builder := NewCompleteTagsResultBuilder(nameOnly, models.NewTagOptions())
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
				if nameOnly {
					expected.Metadata.FetchedMetadataCount = tt.expectedNameOnlyMetdataCount
				} else {
					expected.Metadata.FetchedMetadataCount = tt.expectedMetdataCount
				}

				assert.Equal(t, expected, actual)
			})
		}
	}
}

func TestMetaMerge(t *testing.T) {
	for _, nameOnly := range []bool{true, false} {
		for _, tt := range exhaustTests {
			builder := NewCompleteTagsResultBuilder(nameOnly, models.NewTagOptions())
			t.Run(fmt.Sprintf("%s_%v", tt.name, nameOnly), func(t *testing.T) {
				for _, ex := range tt.exhaustives {
					meta := block.NewResultMetadata()
					meta.Exhaustive = ex
					builder.Add(&CompleteTagsResult{
						CompleteNameOnly: nameOnly,
						Metadata:         meta,
					})
				}

				ctr := builder.Build()
				assert.Equal(t, tt.expected, ctr.Metadata.Exhaustive)
			})
		}
	}
}

func TestCompleteTagNameFilter(t *testing.T) {
	filters := models.Filters{
		models.Filter{Name: b("foo")},
		models.Filter{Name: b("bar"), Values: [][]byte{b("baz"), b("qux")}},
	}

	opts := models.NewTagOptions().SetFilters(filters)
	builder := NewCompleteTagsResultBuilder(true, opts)
	assert.NoError(t, builder.Add(&CompleteTagsResult{
		CompleteNameOnly: true,
		Metadata:         block.NewResultMetadata(),
		CompletedTags: []CompletedTag{
			{Name: b("foo")}, {Name: b("qux")}, {Name: b("bar")},
		},
	}))

	assert.NoError(t, builder.Add(&CompleteTagsResult{
		CompleteNameOnly: true,
		Metadata:         block.NewResultMetadata(),
		CompletedTags: []CompletedTag{
			{Name: b("foo")},
		},
	}))

	res := builder.Build()
	require.True(t, res.CompleteNameOnly)
	require.Equal(t, 2, len(res.CompletedTags))
	sort.Sort(completedTagsByName(res.CompletedTags))

	assert.Equal(t, b("bar"), res.CompletedTags[0].Name)
	assert.Equal(t, b("qux"), res.CompletedTags[1].Name)
}

func TestCompleteTagFilter(t *testing.T) {
	filters := models.Filters{
		models.Filter{Name: b("foo"), Values: [][]byte{b("bar")}},
		models.Filter{Name: b("bar"), Values: [][]byte{b("baz"), b("qux")}},
		models.Filter{Name: b("qux")},
	}

	opts := models.NewTagOptions().SetFilters(filters)
	builder := NewCompleteTagsResultBuilder(false, opts)
	assert.NoError(t, builder.Add(&CompleteTagsResult{
		CompleteNameOnly: false,
		Metadata:         block.NewResultMetadata(),
		CompletedTags: []CompletedTag{
			{Name: b("foo"), Values: [][]byte{b("bar"), b("foobar")}},
			{Name: b("bar"), Values: [][]byte{b("qux"), b("baz")}},
			{Name: b("qux"), Values: [][]byte{b("abc"), b("def")}},
		},
	}))

	assert.NoError(t, builder.Add(&CompleteTagsResult{
		CompleteNameOnly: false,
		Metadata:         block.NewResultMetadata(),
		CompletedTags: []CompletedTag{
			{Name: b("foo"), Values: [][]byte{b("bar"), b("foofoo")}},
			{Name: b("qux"), Values: [][]byte{b("xyz")}},
			{Name: b("quince"), Values: [][]byte{b("quail"), b("quart")}},
		},
	}))

	res := builder.Build()
	require.False(t, res.CompleteNameOnly)
	require.Equal(t, 2, len(res.CompletedTags))
	sort.Sort(completedTagsByName(res.CompletedTags))

	ex := []CompletedTag{
		{
			Name:   b("foo"),
			Values: [][]byte{b("foobar"), b("foofoo")},
		},
		{
			Name:   b("quince"),
			Values: [][]byte{b("quail"), b("quart")},
		},
	}

	assert.Equal(t, ex, res.CompletedTags)
}
