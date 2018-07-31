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

package logical

import (
	"fmt"
	"testing"

	"github.com/m3db/m3db/src/coordinator/block"
	"github.com/m3db/m3db/src/coordinator/models"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func builderMockWithExpectedValues(ctrl *gomock.Controller, indeces []int, values [][]float64) block.Builder {
	builder := block.NewMockBuilder(ctrl)
	for i, val := range values {
		for _, idx := range indeces {
			builder.EXPECT().AppendValue(i, val[idx])
		}
	}

	return builder
}

func stepIterWithExpectedValues(ctrl *gomock.Controller, indeces []int, values [][]float64) block.StepIter {
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
	indeces, expectedIndeces      []int
	builderValues, expectedValues [][]float64
}{
	{
		"no indeces",
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
			builder := builderMockWithExpectedValues(ctrl, tt.expectedIndeces, tt.expectedValues)
			stepIter := stepIterWithExpectedValues(ctrl, tt.expectedIndeces, tt.expectedValues)

			err := appendValuesAtIndeces(tt.indeces, stepIter, builder)
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
	err := appendValuesAtIndeces([]int{1}, stepIter, builder)
	assert.EqualError(t, err, msg)
}

var combineMetadataTests = []struct {
	name                       string
	lTags, rTags, expectedTags models.Tags
	expectedErr                error
}{
	{
		"no right tags",
		models.Tags{"a": "b"},
		models.Tags{},
		models.Tags{"a": "b"},
		nil,
	},
	{
		"no left tags",
		models.Tags{},
		models.Tags{"a": "b"},
		models.Tags{"a": "b"},
		nil,
	},
	{
		"same tags",
		models.Tags{"a": "b"},
		models.Tags{"a": "b"},
		models.Tags{"a": "b"},
		nil,
	},
	{
		"different tags",
		models.Tags{"a": "b"},
		models.Tags{"c": "d"},
		models.Tags{"a": "b", "c": "d"},
		nil,
	},
}

func TestCombineMetadata(t *testing.T) {
	for _, tt := range combineMetadataTests {
		t.Run(tt.name, func(t *testing.T) {
			l, r := block.Metadata{Tags: tt.lTags}, block.Metadata{Tags: tt.rTags}
			meta, err := combineMetadata(l, r)
			if tt.expectedErr != nil {
				require.EqualError(t, err, tt.expectedErr.Error())
				require.Equal(t, block.Metadata{}, meta)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedTags, meta.Tags)
			}
		})
	}
}

var combineMetaAndSeriesMetaTests = []struct {
	name                                        string
	tags, otherTags, expectedTags               models.Tags
	seriesTags, otherSeriesTags                 models.Tags
	expectedSeriesTags, expectedOtherSeriesTags models.Tags
}{
	{
		"no right tags",
		models.Tags{"a": "b"},
		models.Tags{},
		models.Tags{},

		models.Tags{"c": "d"},
		models.Tags{"1": "2"},
		models.Tags{"a": "b", "c": "d"},
		models.Tags{"1": "2"},
	},
	{
		"no left tags",
		models.Tags{},
		models.Tags{"a": "b"},
		models.Tags{},

		models.Tags{},
		models.Tags{},
		models.Tags{},
		models.Tags{"a": "b"},
	},
	{
		"same tags",
		models.Tags{"a": "b"},
		models.Tags{"a": "b"},
		models.Tags{"a": "b"},

		models.Tags{"a": "b", "c": "d"},
		models.Tags{},
		models.Tags{"a": "b", "c": "d"},
		models.Tags{},
	},
	{
		"different tags",
		models.Tags{"a": "b"},
		models.Tags{"c": "d"},
		models.Tags{},

		models.Tags{"1": "2"},
		models.Tags{"3": "4"},
		models.Tags{"a": "b", "1": "2"},
		models.Tags{"c": "d", "3": "4"},
	},
}

func TestCombineMetaAndSeriesMeta(t *testing.T) {
	for _, tt := range combineMetaAndSeriesMetaTests {
		t.Run(tt.name, func(t *testing.T) {
			meta, otherMeta := block.Metadata{Tags: tt.tags}, block.Metadata{Tags: tt.otherTags}

			metas := []block.SeriesMeta{{Tags: tt.seriesTags}, {Tags: tt.seriesTags}}
			otherMetas := []block.SeriesMeta{{Tags: tt.expectedOtherSeriesTags}}

			meta, seriesMeta, otherSeriesMeta, err := combineMetaAndSeriesMeta(meta, otherMeta, metas, otherMetas)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedTags, meta.Tags)

			require.Equal(t, 2, len(seriesMeta))
			for _, meta := range seriesMeta {
				assert.Equal(t, tt.expectedSeriesTags, meta.Tags)
			}

			require.Equal(t, 1, len(otherSeriesMeta))
			for _, otherMeta := range otherSeriesMeta {
				assert.Equal(t, tt.expectedOtherSeriesTags, otherMeta.Tags)
			}
		})
	}
}
