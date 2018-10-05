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
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func builderMockWithExpectedValues(ctrl *gomock.Controller, indices []int, values [][]float64) block.Builder {
	builder := block.NewMockBuilder(ctrl)
	for i, val := range values {
		for _, idx := range indices {
			builder.EXPECT().AppendValue(i, val[idx])
		}
	}

	return builder
}

func stepIterWithExpectedValues(ctrl *gomock.Controller, _ []int, values [][]float64) block.StepIter {
	stepIter := block.NewMockStepIter(ctrl)
	for _, val := range values {
		stepIter.EXPECT().Next().Return(true)
		bl := block.NewMockStep(ctrl)
		bl.EXPECT().Values().Return(val)
		stepIter.EXPECT().Current().Return(bl, nil)
	}
	stepIter.EXPECT().Next().Return(false)

	return stepIter
}

var appendAtIndicesTests = []struct {
	name                          string
	indices, expectedIndices      []int
	builderValues, expectedValues [][]float64
}{
	{
		"no indecis",
		[]int{},
		[]int{},
		[][]float64{[]float64{1, 2}, []float64{3, 4}},
		[][]float64{[]float64{1, 2}, []float64{3, 4}},
	},
	{
		"take first",
		[]int{0},
		[]int{0},
		[][]float64{[]float64{1, 2}, []float64{3, 4}},
		[][]float64{[]float64{1, 2}, []float64{3, 4}},
	},
	{
		"take second",
		[]int{1},
		[]int{1},
		[][]float64{[]float64{1, 2}, []float64{3, 4}},
		[][]float64{[]float64{1, 2}, []float64{3, 4}},
	},
	{
		"take both",
		[]int{0, 1},
		[]int{0, 1},
		[][]float64{[]float64{1, 2}, []float64{3, 4}},
		[][]float64{[]float64{1, 2}, []float64{3, 4}},
	},
}

func TestAppendAtIndices(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	for _, tt := range appendAtIndicesTests {
		t.Run(tt.name, func(t *testing.T) {
			builder := builderMockWithExpectedValues(ctrl, tt.expectedIndices, tt.expectedValues)
			stepIter := stepIterWithExpectedValues(ctrl, tt.expectedIndices, tt.expectedValues)

			err := appendValuesAtIndices(tt.indices, stepIter, builder)
			assert.NoError(t, err)
		})
	}
}

func TestAddAtIndicesErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	builder := block.NewMockBuilder(ctrl)
	stepIter := block.NewMockStepIter(ctrl)

	msg := "err"
	stepIter.EXPECT().Next().Return(true)
	stepIter.EXPECT().Current().Return(nil, fmt.Errorf(msg))
	err := appendValuesAtIndices([]int{1}, stepIter, builder)
	assert.EqualError(t, err, msg)
}

var combineMetaAndSeriesMetaTests = []struct {
	name                                        string
	tags, otherTags, expectedTags               test.StringTags
	seriesTags, otherSeriesTags                 test.StringTags
	expectedSeriesTags, expectedOtherSeriesTags test.StringTags
}{
	{
		"no right tags",
		test.StringTags{{"a", "b"}},
		test.StringTags{},
		test.StringTags{},

		test.StringTags{{"c", "d"}},
		test.StringTags{{"1", "2"}},
		test.StringTags{{"a", "b"}, {"c", "d"}},
		test.StringTags{{"1", "2"}},
	},
	{
		"no left tags",
		test.StringTags{},
		test.StringTags{{"a", "b"}},
		test.StringTags{},

		test.StringTags{},
		test.StringTags{},
		test.StringTags{},
		test.StringTags{{"a", "b"}},
	},
	{
		"same tags",
		test.StringTags{{"a", "b"}},
		test.StringTags{{"a", "b"}},
		test.StringTags{{"a", "b"}},

		test.StringTags{{"a", "b"}, {"c", "d"}},
		test.StringTags{},
		test.StringTags{{"a", "b"}, {"c", "d"}},
		test.StringTags{},
	},
	{
		"different tags",
		test.StringTags{{"a", "b"}},
		test.StringTags{{"c", "d"}},
		test.StringTags{},

		test.StringTags{{"1", "2"}},
		test.StringTags{{"3", "4"}},
		test.StringTags{{"1", "2"}, {"a", "b"}},
		test.StringTags{{"3", "4"}, {"c", "d"}},
	},
	{
		"conflicting tags",
		test.StringTags{{"a", "b"}},
		test.StringTags{{"a", "*b"}},
		test.StringTags{},

		test.StringTags{{"1", "2"}},
		test.StringTags{{"3", "4"}},
		test.StringTags{{"1", "2"}, {"a", "b"}},
		test.StringTags{{"3", "4"}, {"a", "*b"}},
	},
	{
		"mixed tags",
		test.StringTags{{"a", "b"}, {"c", "d"}, {"e", "f"}},
		test.StringTags{{"a", "b"}, {"c", "*d"}, {"g", "h"}},
		test.StringTags{{"a", "b"}},

		test.StringTags{{"1", "2"}},
		test.StringTags{{"3", "4"}},
		test.StringTags{{"1", "2"}, {"c", "d"}, {"e", "f"}},
		test.StringTags{{"3", "4"}, {"c", "*d"}, {"g", "h"}},
	},
}

func TestCombineMetaAndSeriesMeta(t *testing.T) {
	for _, tt := range combineMetaAndSeriesMetaTests {
		t.Run(tt.name, func(t *testing.T) {
			tags := test.StringTagsToTags(tt.tags)
			otherTags := test.StringTagsToTags(tt.otherTags)
			seriesTags := test.StringTagsToTags(tt.seriesTags)
			expectedTags := test.StringTagsToTags(tt.expectedTags)
			otherSeriesTags := test.StringTagsToTags(tt.otherSeriesTags)
			expectedSeriesTags := test.StringTagsToTags(tt.expectedSeriesTags)
			expectedOtherSeriesTags := test.StringTagsToTags(tt.expectedOtherSeriesTags)

			meta, otherMeta := block.Metadata{Tags: tags}, block.Metadata{Tags: otherTags}

			metas := []block.SeriesMeta{{Tags: seriesTags}, {Tags: seriesTags}}
			otherMetas := []block.SeriesMeta{{Tags: otherSeriesTags}}

			meta, seriesMeta, otherSeriesMeta, err := combineMetaAndSeriesMeta(meta, otherMeta, metas, otherMetas)
			require.NoError(t, err)
			assert.Equal(t, expectedTags, meta.Tags)

			require.Equal(t, 2, len(seriesMeta))
			for _, meta := range seriesMeta {
				assert.Equal(t, expectedSeriesTags, meta.Tags)
			}

			require.Equal(t, 1, len(otherSeriesMeta))
			for _, otherMeta := range otherSeriesMeta {
				assert.Equal(t, expectedOtherSeriesTags, otherMeta.Tags)
			}
		})
	}
}

func TestCombineMetaAndSeriesMetaError(t *testing.T) {
	now := time.Now()
	meta, otherMeta :=
		block.Metadata{Bounds: models.Bounds{Start: now}},
		block.Metadata{Bounds: models.Bounds{Start: now.Add(2)}}

	metas, otherMetas := []block.SeriesMeta{}, []block.SeriesMeta{}
	_, _, _, err := combineMetaAndSeriesMeta(meta, otherMeta, metas, otherMetas)
	assert.Error(t, err, errMismatchedBounds.Error())
}
