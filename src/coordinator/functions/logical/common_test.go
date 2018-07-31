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

var addAtIndicesTests = []struct {
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

func TestAddAtIndices(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	for _, tt := range addAtIndicesTests {
		t.Run(tt.name, func(t *testing.T) {
			builder := builderMockWithExpectedValues(ctrl, tt.expectedIndeces, tt.expectedValues)
			stepIter := stepIterWithExpectedValues(ctrl, tt.expectedIndeces, tt.expectedValues)

			err := addValuesAtIndeces(tt.indeces, stepIter, builder)
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
	err := addValuesAtIndeces([]int{1}, stepIter, builder)
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
	{
		"conflicting tags",
		models.Tags{"a": "b"},
		models.Tags{"a": "c"},
		models.Tags{},
		errConflictingTags,
	},
}

func TestCombineMetadataFail(t *testing.T) {
	for _, tt := range combineMetadataTests {
		t.Run(tt.name, func(t *testing.T) {
			// lTags, rTags := models.Tags{"a": "b"}, models.Tags{"c": "d"}
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
