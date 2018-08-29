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

	"github.com/stretchr/testify/assert"
)

func TestFlattenMetadata(t *testing.T) {
	meta := block.Metadata{Tags: models.Tags{{"a", "b"}, {"c", "d"}}}
	seriesMetas := []block.SeriesMeta{
		{Name: "foo", Tags: models.Tags{{"e", "f"}}},
		{Name: "bar", Tags: models.Tags{{"g", "h"}}},
	}
	flattened := FlattenMetadata(meta, seriesMetas)

	expected := []block.SeriesMeta{
		{Name: "foo", Tags: models.Tags{{"a", "b"}, {"c", "d"}, {"e", "f"}}},
		{Name: "bar", Tags: models.Tags{{"a", "b"}, {"c", "d"}, {"g", "h"}}},
	}

	assert.Equal(t, expected, flattened)
}

var dedupeMetadataTests = []struct {
	name               string
	metaTags           []models.Tags
	expectedCommon     models.Tags
	expectedSeriesTags []models.Tags
}{
	{
		"empty metas",
		[]models.Tags{},
		models.Tags{},
		[]models.Tags{},
	},
	{
		"single metas",
		[]models.Tags{{{"a", "b"}, {"c", "d"}}},
		models.Tags{{"a", "b"}, {"c", "d"}},
		[]models.Tags{{}},
	},
	{
		"one common tag, longer first",
		[]models.Tags{{{"a", "b"}, {"c", "d"}}, {{"a", "b"}}},
		models.Tags{{"a", "b"}},
		[]models.Tags{{{"c", "d"}}, {}},
	},
	{
		"one common tag, longer second",
		[]models.Tags{{{"a", "b"}}, {{"a", "b"}, {"c", "d"}}},
		models.Tags{{"a", "b"}},
		[]models.Tags{{}, {{"c", "d"}}},
	},
	{
		"two common tags",
		[]models.Tags{{{"a", "b"}, {"c", "d"}}, {{"a", "b"},
			{"c", "d"}}, {{"a", "b"}, {"c", "d"}}},
		models.Tags{{"a", "b"}, {"c", "d"}},
		[]models.Tags{{}, {}, {}},
	},
	{
		"no common tags in one series",
		[]models.Tags{{{"a", "b"}, {"c", "d"}}, {{"a", "b"}, {"c", "d"}},
			{{"a", "b*"}, {"c*", "d"}}},
		models.Tags{},
		[]models.Tags{{{"a", "b"}, {"c", "d"}}, {{"a", "b"},
			{"c", "d"}}, {{"a", "b*"}, {"c*", "d"}}},
	},
}

func TestDedupeMetadata(t *testing.T) {
	for _, tt := range dedupeMetadataTests {
		t.Run(tt.name, func(t *testing.T) {
			metaTags := tt.metaTags
			numSeries := len(metaTags)
			seriesMetas := make([]block.SeriesMeta, numSeries)
			for i, tags := range metaTags {
				seriesMetas[i] = block.SeriesMeta{Tags: tags}
			}

			actual, actualSeriesMetas := DedupeMetadata(seriesMetas)
			assert.Equal(t, tt.expectedCommon, actual)

			actualTags := make([]models.Tags, numSeries)
			for i, metas := range actualSeriesMetas {
				actualTags[i] = metas.Tags
			}
			assert.Equal(t, tt.expectedSeriesTags, actualTags)
		})
	}
}
