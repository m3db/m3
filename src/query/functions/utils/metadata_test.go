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

	"github.com/stretchr/testify/assert"
)

func TestFlattenMetadata(t *testing.T) {
	meta := block.Metadata{Tags: test.TagSliceToTags([]models.Tag{
		{Name: []byte("a"), Value: []byte("b")},
		{Name: []byte("c"), Value: []byte("d")},
	})}

	seriesMetas := []block.SeriesMeta{
		{Name: "foo", Tags: test.TagSliceToTags([]models.Tag{{Name: []byte("e"), Value: []byte("f")}})},
		{Name: "bar", Tags: test.TagSliceToTags([]models.Tag{{Name: []byte("g"), Value: []byte("h")}})},
	}
	flattened := FlattenMetadata(meta, seriesMetas)

	expected := []block.SeriesMeta{
		{Name: "foo", Tags: test.TagSliceToTags([]models.Tag{
			{Name: []byte("a"), Value: []byte("b")},
			{Name: []byte("c"), Value: []byte("d")},
			{Name: []byte("e"), Value: []byte("f")},
		})},
		{Name: "bar", Tags: test.TagSliceToTags([]models.Tag{
			{Name: []byte("a"), Value: []byte("b")},
			{Name: []byte("c"), Value: []byte("d")},
			{Name: []byte("g"), Value: []byte("h")},
		})},
	}

	assert.Equal(t, expected, flattened)
}

var dedupeMetadataTests = []struct {
	name               string
	metaTags           []test.StringTags
	expectedCommon     test.StringTags
	expectedSeriesTags []test.StringTags
}{
	{
		"empty metas",
		[]test.StringTags{},
		test.StringTags{},
		[]test.StringTags{},
	},
	{
		"single metas",
		[]test.StringTags{{{"a", "b"}, {"c", "d"}}},
		test.StringTags{{"a", "b"}, {"c", "d"}},
		[]test.StringTags{{}},
	},
	{
		"one common tag, longer first",
		[]test.StringTags{{{"a", "b"}, {"c", "d"}}, {{"a", "b"}}},
		test.StringTags{{"a", "b"}},
		[]test.StringTags{{{"c", "d"}}, {}},
	},
	{
		"one common tag, longer second",
		[]test.StringTags{{{"a", "b"}}, {{"a", "b"}, {"c", "d"}}},
		test.StringTags{{"a", "b"}},
		[]test.StringTags{{}, {{"c", "d"}}},
	},
	{
		"two common tags",
		[]test.StringTags{{{"a", "b"}, {"c", "d"}}, {{"a", "b"},
			{"c", "d"}}, {{"a", "b"}, {"c", "d"}}},
		test.StringTags{{"a", "b"}, {"c", "d"}},
		[]test.StringTags{{}, {}, {}},
	},
	{
		"no common tags in one series",
		[]test.StringTags{{{"a", "b"}, {"c", "d"}}, {{"a", "b"}, {"c", "d"}},
			{{"a", "b*"}, {"c*", "d"}}},
		test.StringTags{},
		[]test.StringTags{{{"a", "b"}, {"c", "d"}}, {{"a", "b"},
			{"c", "d"}}, {{"a", "b*"}, {"c*", "d"}}},
	},
}

func TestDedupeMetadata(t *testing.T) {
	for _, tt := range dedupeMetadataTests {
		t.Run(tt.name, func(t *testing.T) {
			metaTags := tt.metaTags
			numSeries := len(metaTags)
			seriesMetas := make([]block.SeriesMeta, numSeries)
			for i, stringTags := range metaTags {
				tags := test.StringTagsToTags(stringTags)
				seriesMetas[i] = block.SeriesMeta{Tags: tags}
			}

			actual, actualSeriesMetas := DedupeMetadata(seriesMetas)
			exCommon := test.StringTagsToTags(tt.expectedCommon)
			assert.Equal(t, exCommon, actual)

			actualTags := make([]models.Tags, numSeries)
			for i, metas := range actualSeriesMetas {
				actualTags[i] = metas.Tags
			}

			exSeriesTags := test.StringTagsSliceToTagSlice(tt.expectedSeriesTags)
			assert.Equal(t, exSeriesTags, actualTags)
		})
	}
}
