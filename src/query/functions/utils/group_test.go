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

package utils

import (
	"testing"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/test"
)

var collectTest = []struct {
	name            string
	matching        []string
	tagLists        []models.Tags
	expectedIndices [][]int
	expectedTags    []models.Tags
	withoutIndices  [][]int
	withoutTags     []models.Tags
}{
	{
		"noMatching",
		[]string{},
		[]models.Tags{
			{"a": "1"},
			{"a": "1", "b": "2", "c": "4"},
			{"b": "2"},
			{"a": "1", "b": "2", "c": "3"},
			{"a": "1", "b": "2", "d": "3"},
			{"c": "d"},
		},
		[][]int{{0, 1, 2, 3, 4, 5}},
		[]models.Tags{{}},
		[][]int{{0}, {1}, {2}, {3}, {4}, {5}},
		[]models.Tags{
			{"a": "1"},
			{"a": "1", "b": "2", "c": "4"},
			{"b": "2"},
			{"a": "1", "b": "2", "c": "3"},
			{"a": "1", "b": "2", "d": "3"},
			{"c": "d"},
		},
	},
	{
		"no equal Matching",
		[]string{"f", "g", "h"},
		[]models.Tags{
			{"a": "1"},
			{"a": "1", "b": "2", "c": "4"},
			{"b": "2"},
			{"a": "1", "b": "2", "c": "3"},
			{"a": "1", "b": "2", "d": "3"},
			{"c": "d"},
		},
		[][]int{{0, 1, 2, 3, 4, 5}},
		[]models.Tags{{}},
		[][]int{{0}, {1}, {2}, {3}, {4}, {5}},
		[]models.Tags{
			{"a": "1"},
			{"a": "1", "b": "2", "c": "4"},
			{"b": "2"},
			{"a": "1", "b": "2", "c": "3"},
			{"a": "1", "b": "2", "d": "3"},
			{"c": "d"},
		},
	},
	{
		"oneMatching",
		[]string{"a"},
		[]models.Tags{
			{"a": "1"},
			{"a": "1", "b": "2", "c": "4"},
			{"b": "2"},
			{"a": "1", "b": "2", "c": "3"},
			{"a": "1", "b": "2", "d": "3"},
			{"c": "d"},
		},
		[][]int{{0, 1, 3, 4}, {2, 5}},
		[]models.Tags{
			{"a": "1"},
			{},
		},
		[][]int{{0}, {1}, {2}, {3}, {4}, {5}},
		[]models.Tags{
			{},
			{"b": "2", "c": "4"},
			{"b": "2"},
			{"b": "2", "c": "3"},
			{"b": "2", "d": "3"},
			{"c": "d"},
		},
	},
	{
		"same tag matching",
		[]string{"a", "a"},
		[]models.Tags{
			{"a": "1"},
			{"a": "1", "b": "2", "c": "4"},
			{"b": "2"},
			{"a": "1", "b": "2", "c": "3"},
			{"a": "1", "b": "2", "d": "3"},
			{"c": "d"},
		},
		[][]int{{0, 1, 3, 4}, {2, 5}},
		[]models.Tags{
			{"a": "1"},
			{},
		},
		[][]int{{0}, {1}, {2}, {3}, {4}, {5}},
		[]models.Tags{
			{},
			{"b": "2", "c": "4"},
			{"b": "2"},
			{"b": "2", "c": "3"},
			{"b": "2", "d": "3"},
			{"c": "d"},
		},
	},
	{
		"diffMatching",
		[]string{"a"},
		[]models.Tags{
			{"a": "1"},
			{"a": "2", "b": "2", "c": "4"},
			{"a": "2"},
			{"a": "1", "b": "2", "c": "3"},
			{"a": "1", "b": "2", "d": "3"},
			{"a": "d"},
		},
		[][]int{{0, 3, 4}, {1, 2}, {5}},
		[]models.Tags{
			{"a": "1"},
			{"a": "2"},
			{"a": "d"},
		},
		[][]int{{0, 2, 5}, {1}, {3}, {4}},
		[]models.Tags{
			{},
			{"b": "2", "c": "4"},
			{"b": "2", "c": "3"},
			{"b": "2", "d": "3"},
		},
	},
	{
		"someMatching",
		[]string{"a", "b"},
		[]models.Tags{
			{"a": "1"},
			{"a": "1", "b": "2", "c": "4"},
			{"b": "2"},
			{"a": "1", "b": "2", "c": "3"},
			{"a": "1", "b": "2", "d": "3"},
			{"c": "3"},
		},
		[][]int{{0}, {1, 3, 4}, {2}, {5}},
		[]models.Tags{
			{"a": "1"},
			{"a": "1", "b": "2"},
			{"b": "2"},
			{},
		},
		[][]int{{0, 2}, {1}, {3, 5}, {4}},
		[]models.Tags{
			{},
			{"c": "4"},
			{"c": "3"},
			{"d": "3"},
		},
	},
	{
		"functionMatching",
		[]string{"a"},
		[]models.Tags{
			{"a": "1"},
			{"a": "1"},
			{"a": "1", "b": "2"},
			{"a": "2", "b": "2"},
			{"b": "2"},
			{"c": "3"},
		},
		[][]int{{0, 1, 2}, {3}, {4, 5}},
		[]models.Tags{
			{"a": "1"},
			{"a": "2"},
			{},
		},
		[][]int{{0, 1}, {2, 3, 4}, {5}},
		[]models.Tags{
			{},
			{"b": "2"},
			{"c": "3"},
		},
	},
	{
		"different matching",
		[]string{"a", "b"},
		[]models.Tags{
			{"a": "1", "c": "3", "d": "4"},
			{"b": "1", "c": "3", "d": "5"},
			{"b": "1", "c": "3", "d": "6"},
		},
		[][]int{{0}, {1, 2}},
		[]models.Tags{
			{"a": "1"},
			{"b": "1"},
		},
		[][]int{{0}, {1}, {2}},
		[]models.Tags{
			{"c": "3", "d": "4"},
			{"c": "3", "d": "5"},
			{"c": "3", "d": "6"},
		},
	},
}

func testCollect(t *testing.T, without bool) {
	for _, tt := range collectTest {
		name := tt.name + " with tags"
		if without {
			name = tt.name + " without tags"
		}

		t.Run(name, func(t *testing.T) {
			metas := make([]block.SeriesMeta, len(tt.tagLists))
			for i, tagList := range tt.tagLists {
				metas[i] = block.SeriesMeta{Tags: tagList}
			}

			indices, collected := GroupSeries(tt.matching, without, name, metas)

			expectedTags := tt.expectedTags
			expectedIndicies := tt.expectedIndices
			if without {
				expectedTags = tt.withoutTags
				expectedIndicies = tt.withoutIndices
			}

			expectedMetas := make([]block.SeriesMeta, len(expectedTags))
			for i, tags := range expectedTags {
				expectedMetas[i] = block.SeriesMeta{
					Tags: tags,
					Name: name,
				}
			}

			test.CompareLists(t, collected, expectedMetas, indices, expectedIndicies)
		})
	}
}

func TestCollectWithTags(t *testing.T) {
	testCollect(t, false)
}

func TestCollectWithoutTags(t *testing.T) {
	testCollect(t, true)
}
