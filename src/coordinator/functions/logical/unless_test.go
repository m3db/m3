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
	"math"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/m3db/m3db/src/coordinator/block"
	"github.com/m3db/m3db/src/coordinator/parser"
	"github.com/m3db/m3db/src/coordinator/test"
	"github.com/m3db/m3db/src/coordinator/test/executor"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnlessWithExactValues(t *testing.T) {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)
	block1 := test.NewBlockFromValues(bounds, values)
	block2 := test.NewBlockFromValues(bounds, values)

	op := NewUnlessOp(parser.NodeID(0), parser.NodeID(1), &VectorMatching{})
	c, sink := executor.NewControllerWithSink(parser.NodeID(2))
	node := op.Node(c)

	err := node.Process(parser.NodeID(1), block2)
	require.NoError(t, err)
	err = node.Process(parser.NodeID(0), block1)
	require.NoError(t, err)
	assert.Equal(t, [][]float64{}, sink.Values)
}

func TestUnlessWithSomeValues(t *testing.T) {
	values1, bounds1 := test.GenerateValuesAndBounds(nil, nil)
	block1 := test.NewBlockFromValues(bounds1, values1)

	v := [][]float64{
		{0, math.NaN(), 2, 3, 4},
		{math.NaN(), 6, 7, 8, 4, 9},
	}

	values2, bounds2 := test.GenerateValuesAndBounds(v, nil)
	block2 := test.NewBlockFromValues(bounds2, values2)

	op := NewOrOp(parser.NodeID(0), parser.NodeID(1), &VectorMatching{})
	c, sink := executor.NewControllerWithSink(parser.NodeID(2))
	node := op.Node(c)

	err := node.Process(parser.NodeID(1), block2)
	require.NoError(t, err)
	err = node.Process(parser.NodeID(0), block1)
	require.NoError(t, err)
	// NAN values should be filled

	fmt.Println(sink.Values)
	test.EqualsWithNans(t, [][]float64{}, sink.Values)
}

var unlessTests = []struct {
	name                 string
	lhs, rhs             []block.SeriesMeta
	expectedL, expectedR []int
}{
	{
		"equal tags",
		generateMetaDataWithTagsInRange(0, 5), generateMetaDataWithTagsInRange(0, 5),
		[]int{}, []int{},
	},
	{
		"empty rhs",
		generateMetaDataWithTagsInRange(0, 5), []block.SeriesMeta{},
		[]int{0, 1, 2, 3, 4}, []int{},
	},
	{
		"empty lhs",
		[]block.SeriesMeta{}, generateMetaDataWithTagsInRange(0, 5),
		[]int{}, []int{0, 1, 2, 3, 4},
	},
	{
		"longer lhs",
		generateMetaDataWithTagsInRange(-1, 6), generateMetaDataWithTagsInRange(0, 5),
		[]int{0, 6}, []int{},
	},
	{
		"longer rhs",
		generateMetaDataWithTagsInRange(0, 5), generateMetaDataWithTagsInRange(-1, 6),
		[]int{}, []int{0, 6},
	},
	{
		"shorter lhs",
		generateMetaDataWithTagsInRange(1, 4), generateMetaDataWithTagsInRange(0, 5),
		[]int{}, []int{0, 4},
	},
	{
		"shorter rhs",
		generateMetaDataWithTagsInRange(0, 5), generateMetaDataWithTagsInRange(1, 4),
		[]int{0, 4}, []int{},
	},
	{
		"partial overlap",
		generateMetaDataWithTagsInRange(0, 5), generateMetaDataWithTagsInRange(1, 6),
		[]int{0}, []int{4},
	},
	{
		"no overlap",
		generateMetaDataWithTagsInRange(0, 5), generateMetaDataWithTagsInRange(6, 9),
		[]int{0, 1, 2, 3, 4}, []int{0, 1, 2},
	},
}

func TestIntersect(t *testing.T) {
	unlessNode := UnlessNode{
		op: BaseOp{
			Matching: &VectorMatching{},
		},
	}

	for _, tt := range unlessTests {
		t.Run(tt.name, func(t *testing.T) {
			xorLeft, xorRight := unlessNode.exclusion(tt.lhs, tt.rhs)
			assert.Equal(t, tt.expectedL, xorLeft)
			assert.Equal(t, tt.expectedR, xorRight)
		})
	}
}

func builderMockWithExpectedValues(ctrl *gomock.Controller, indeces []int, values [][]float64) block.Builder {
	builder := block.NewMockBuilder(ctrl)
	for i, idx := range indeces {
		for _, val := range values[idx] {
			builder.EXPECT().AppendValue(i, val)
		}
	}

	return builder
}

func stepIterWithExpectedValues(ctrl *gomock.Controller, indeces []int, values [][]float64) block.StepIter {
	stepIter := block.NewMockStepIter(ctrl)
	for _, idx := range indeces {
		if idx > 0 {
			stepIter.EXPECT().Next().Return(true)
		}
		bl := block.NewMockStep(ctrl)
		bl.EXPECT().Values().Return(values[idx])
		stepIter.EXPECT().Current().Return(bl, nil)
	}

	return stepIter
}

var addAtIndicesTests = []struct {
	name          string
	indeces       []int
	builderValues [][]float64
}{
	{"no indeces", []int{}, [][]float64{[]float64{1, 2}, []float64{3, 4}}},
	{"take first", []int{0}, [][]float64{[]float64{1, 2}, []float64{3, 4}}},
	{"take second", []int{1}, [][]float64{[]float64{1, 2}, []float64{3, 4}}},
	{"take both", []int{0, 1}, [][]float64{[]float64{1, 2}, []float64{3, 4}}},
}

func TestAddAtIndices(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	for _, tt := range addAtIndicesTests {
		t.Run(tt.name, func(t *testing.T) {
			builder := builderMockWithExpectedValues(ctrl, tt.indeces, tt.builderValues)
			stepIter := stepIterWithExpectedValues(ctrl, tt.indeces, tt.builderValues)

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
	stepIter.EXPECT().Next().Return(false)

	err := addValuesAtIndeces([]int{1}, stepIter, builder)
	assert.EqualError(t, err, errNoIndexInIterator.Error())

	msg := "err"
	stepIter.EXPECT().Next().Return(true)
	stepIter.EXPECT().Current().Return(nil, fmt.Errorf(msg))
	err = addValuesAtIndeces([]int{1}, stepIter, builder)
	assert.EqualError(t, err, msg)
}
