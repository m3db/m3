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

package binary

import (
	"math"
	"testing"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var nan = math.NaN()

func builderMockWithExpectedValues(
	ctrl *gomock.Controller,
	indices []int,
	values [][]float64,
) block.Builder {
	builder := block.NewMockBuilder(ctrl)
	for i, val := range values {
		for _, idx := range indices {
			builder.EXPECT().AppendValue(i, val[idx])
		}
	}

	return builder
}

func stepIterWithExpectedValues(
	ctrl *gomock.Controller,
	_ []int,
	values [][]float64,
) block.StepIter {
	stepIter := block.NewMockStepIter(ctrl)
	stepIter.EXPECT().Next().Return(true).Times(len(values))
	for _, val := range values {
		bl := block.NewMockStep(ctrl)
		bl.EXPECT().Values().Return(val)
		stepIter.EXPECT().Current().Return(bl)
	}

	stepIter.EXPECT().Next().Return(false)
	stepIter.EXPECT().Err().Return(nil)

	return stepIter
}

var combineMetaAndSeriesMetaTests = []struct {
	name                                        string
	tags, otherTags, expectedTags               test.StringTags
	seriesTags, otherSeriesTags                 test.StringTags
	expectedSeriesTags, expectedOtherSeriesTags test.StringTags
}{
	{
		"no right tags",
		test.StringTags{{N: "a", V: "b"}},
		test.StringTags{},
		test.StringTags{},

		test.StringTags{{N: "c", V: "d"}},
		test.StringTags{{N: "1", V: "2"}},
		test.StringTags{{N: "a", V: "b"}, {N: "c", V: "d"}},
		test.StringTags{{N: "1", V: "2"}},
	},
	{
		"no left tags",
		test.StringTags{},
		test.StringTags{{N: "a", V: "b"}},
		test.StringTags{},

		test.StringTags{},
		test.StringTags{},
		test.StringTags{},
		test.StringTags{{N: "a", V: "b"}},
	},
	{
		"same tags",
		test.StringTags{{N: "a", V: "b"}},
		test.StringTags{{N: "a", V: "b"}},
		test.StringTags{{N: "a", V: "b"}},

		test.StringTags{{N: "a", V: "b"}, {N: "c", V: "d"}},
		test.StringTags{},
		test.StringTags{{N: "a", V: "b"}, {N: "c", V: "d"}},
		test.StringTags{},
	},
	{
		"different tags",
		test.StringTags{{N: "a", V: "b"}},
		test.StringTags{{N: "c", V: "d"}},
		test.StringTags{},

		test.StringTags{{N: "1", V: "2"}},
		test.StringTags{{N: "3", V: "4"}},
		test.StringTags{{N: "1", V: "2"}, {N: "a", V: "b"}},
		test.StringTags{{N: "3", V: "4"}, {N: "c", V: "d"}},
	},
	{
		"conflicting tags",
		test.StringTags{{N: "a", V: "b"}},
		test.StringTags{{N: "a", V: "*b"}},
		test.StringTags{},

		test.StringTags{{N: "1", V: "2"}},
		test.StringTags{{N: "3", V: "4"}},
		test.StringTags{{N: "1", V: "2"}, {N: "a", V: "b"}},
		test.StringTags{{N: "3", V: "4"}, {N: "a", V: "*b"}},
	},
	{
		"mixed tags",
		test.StringTags{{N: "a", V: "b"}, {N: "c", V: "d"}, {N: "e", V: "f"}},
		test.StringTags{{N: "a", V: "b"}, {N: "c", V: "*d"}, {N: "g", V: "h"}},
		test.StringTags{{N: "a", V: "b"}},

		test.StringTags{{N: "1", V: "2"}},
		test.StringTags{{N: "3", V: "4"}},
		test.StringTags{{N: "1", V: "2"}, {N: "c", V: "d"}, {N: "e", V: "f"}},
		test.StringTags{{N: "3", V: "4"}, {N: "c", V: "*d"}, {N: "g", V: "h"}},
	},
}

func TestCombineMetaAndSeriesMeta(t *testing.T) {
	for _, tt := range combineMetaAndSeriesMetaTests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				tags               = test.StringTagsToTags(tt.tags)
				otherTags          = test.StringTagsToTags(tt.otherTags)
				seriesTags         = test.StringTagsToTags(tt.seriesTags)
				expectedTags       = test.StringTagsToTags(tt.expectedTags)
				otherSeriesTags    = test.StringTagsToTags(tt.otherSeriesTags)
				expectedSeriesTags = test.StringTagsToTags(tt.expectedSeriesTags)
				expectedOtherTags  = test.StringTagsToTags(tt.expectedOtherSeriesTags)

				meta      = block.Metadata{Tags: tags}
				otherMeta = block.Metadata{Tags: otherTags}

				metas      = []block.SeriesMeta{{Tags: seriesTags}, {Tags: seriesTags}}
				otherMetas = []block.SeriesMeta{{Tags: otherSeriesTags}}
			)

			meta, seriesMeta, otherSeriesMeta, err := combineMetaAndSeriesMeta(meta,
				otherMeta, metas, otherMetas)
			require.NoError(t, err)
			assert.Equal(t, expectedTags, meta.Tags)

			require.Equal(t, 2, len(seriesMeta))
			for _, meta := range seriesMeta {
				assert.True(t, expectedSeriesTags.Equals(meta.Tags))
			}

			require.Equal(t, 1, len(otherSeriesMeta))
			for _, otherMeta := range otherSeriesMeta {
				assert.True(t, expectedOtherTags.Equals(otherMeta.Tags))
			}
		})
	}
}

func TestCombineMetaAndSeriesMetaError(t *testing.T) {
	now := xtime.Now()
	meta, otherMeta :=
		block.Metadata{Bounds: models.Bounds{Start: now}},
		block.Metadata{Bounds: models.Bounds{Start: now.Add(2)}}

	metas, otherMetas := []block.SeriesMeta{}, []block.SeriesMeta{}
	_, _, _, err := combineMetaAndSeriesMeta(meta, otherMeta, metas, otherMetas)
	assert.Error(t, err, errMismatchedBounds.Error())
}

func toTag(n, v string) models.Tag {
	return models.Tag{Name: []byte(n), Value: []byte(v)}
}

func TestRemoveNameTags(t *testing.T) {
	mTags := models.NewTags(2, models.NewTagOptions()).AddTags([]models.Tag{
		toTag("foo", "bar"),
	}).SetName([]byte("baz"))

	meta := block.Metadata{
		Tags: mTags,
	}

	firstTags := models.NewTags(2, models.NewTagOptions()).AddTags([]models.Tag{
		toTag("qux", "qaz"),
	}).SetName([]byte("quail"))

	secondTags := models.NewTags(1, models.NewTagOptions()).AddTags([]models.Tag{
		toTag("foobar", "baz"),
	})

	metas := []block.SeriesMeta{
		{Tags: firstTags},
		{Tags: secondTags},
	}

	// NB: ensure the name tags exist initially
	ex := []models.Tag{toTag("__name__", "baz"), toTag("foo", "bar")}
	assert.Equal(t, ex, meta.Tags.Tags)

	ex = []models.Tag{toTag("__name__", "quail"), toTag("qux", "qaz")}
	assert.Equal(t, ex, metas[0].Tags.Tags)

	ex = []models.Tag{toTag("foobar", "baz")}
	assert.Equal(t, ex, metas[1].Tags.Tags)

	meta, metas = removeNameTags(meta, metas)

	// NB: ensure name tags are removed
	ex = []models.Tag{toTag("foo", "bar")}
	assert.Equal(t, ex, meta.Tags.Tags)

	ex = []models.Tag{toTag("qux", "qaz")}
	assert.Equal(t, ex, metas[0].Tags.Tags)

	ex = []models.Tag{toTag("foobar", "baz")}
	assert.Equal(t, ex, metas[1].Tags.Tags)
}
