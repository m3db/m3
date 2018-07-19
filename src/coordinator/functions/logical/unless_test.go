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
	"github.com/m3db/m3db/src/coordinator/parser"
	"github.com/m3db/m3db/src/coordinator/test"
	"github.com/m3db/m3db/src/coordinator/test/executor"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var exclusionTests = []struct {
	name      string
	lhs, rhs  []block.SeriesMeta
	expectedL []int
}{
	{
		"equal tags",
		generateMetaDataWithTagsInRange(0, 5), generateMetaDataWithTagsInRange(0, 5),
		[]int{},
	},
	{
		"empty rhs",
		generateMetaDataWithTagsInRange(0, 5), []block.SeriesMeta{},
		[]int{0, 1, 2, 3, 4},
	},
	{
		"empty lhs",
		[]block.SeriesMeta{}, generateMetaDataWithTagsInRange(0, 5),
		[]int{},
	},
	{
		"longer lhs",
		generateMetaDataWithTagsInRange(-1, 6), generateMetaDataWithTagsInRange(0, 5),
		[]int{0, 6},
	},
	{
		"longer rhs",
		generateMetaDataWithTagsInRange(0, 5), generateMetaDataWithTagsInRange(-1, 6),
		[]int{},
	},
	{
		"shorter lhs",
		generateMetaDataWithTagsInRange(1, 4), generateMetaDataWithTagsInRange(0, 5),
		[]int{},
	},
	{
		"shorter rhs",
		generateMetaDataWithTagsInRange(0, 5), generateMetaDataWithTagsInRange(1, 4),
		[]int{0, 4},
	},
	{
		"partial overlap",
		generateMetaDataWithTagsInRange(0, 5), generateMetaDataWithTagsInRange(1, 6),
		[]int{0},
	},
	{
		"no overlap",
		generateMetaDataWithTagsInRange(0, 5), generateMetaDataWithTagsInRange(6, 9),
		[]int{0, 1, 2, 3, 4},
	},
}

func TestIntersect(t *testing.T) {
	unlessNode := UnlessNode{
		op: BaseOp{
			Matching: &VectorMatching{},
		},
	}

	for _, tt := range exclusionTests {
		t.Run(tt.name, func(t *testing.T) {
			excluded := unlessNode.exclusion(tt.lhs, tt.rhs)
			assert.Equal(t, tt.expectedL, excluded)
		})
	}
}

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

	msg := "err"
	stepIter.EXPECT().Next().Return(true)
	stepIter.EXPECT().Current().Return(nil, fmt.Errorf(msg))
	err := addValuesAtIndeces([]int{1}, stepIter, builder)
	assert.EqualError(t, err, msg)
}

var unlessTests = []struct {
	name          string
	lhsMeta       []block.SeriesMeta
	lhs           [][]float64
	rhsMeta       []block.SeriesMeta
	rhs           [][]float64
	expectedMetas []block.SeriesMeta
	expected      [][]float64
	err           error
}{
	{
		"valid, equal tags",
		test.NewSeriesMeta("a", 2),
		[][]float64{{1, 2}, {10, 20}},
		test.NewSeriesMeta("a", 2),
		[][]float64{{3, 4}, {30, 40}},
		[]block.SeriesMeta{},
		[][]float64{},
		nil,
	},
	{
		"valid, some overlap right",
		test.NewSeriesMeta("a", 2),
		[][]float64{{1, 2}, {10, 20}},
		test.NewSeriesMeta("a", 3),
		[][]float64{{3, 4}, {30, 40}, {50, 60}},
		[]block.SeriesMeta{},
		[][]float64{},
		nil,
	},
	{
		"valid, some overlap left",
		test.NewSeriesMeta("a", 3),
		[][]float64{{1, 2}, {10, 20}, {100, 200}},
		test.NewSeriesMeta("a", 3)[1:],
		[][]float64{{3, 4}, {30, 40}},
		test.NewSeriesMeta("a", 1),
		[][]float64{{1, 2}},
		nil,
	},
	{
		"valid, some overlap both",
		test.NewSeriesMeta("a", 3),
		[][]float64{{1, 2}, {10, 20}, {100, 200}},
		test.NewSeriesMeta("a", 4)[1:],
		[][]float64{{3, 4}, {30, 40}, {300, 400}},
		test.NewSeriesMeta("a", 1),
		[][]float64{{1, 2}},
		nil,
	},
	{
		"valid, equal size",
		test.NewSeriesMeta("a", 2),
		[][]float64{{1, 2}, {10, 20}},
		test.NewSeriesMeta("b", 2),
		[][]float64{{3, 4}, {30, 40}},
		test.NewSeriesMeta("a", 2),
		[][]float64{{1, 2}, {10, 20}},
		nil,
	},
	{
		"valid, longer rhs",
		test.NewSeriesMeta("a", 2),
		[][]float64{{1, 2}, {10, 20}},
		test.NewSeriesMeta("b", 3),
		[][]float64{{3, 4}, {30, 40}, {300, 400}},
		test.NewSeriesMeta("a", 2),
		[][]float64{{1, 2}, {10, 20}},
		nil,
	},
	{
		"valid, longer lhs",
		test.NewSeriesMeta("a", 3),
		[][]float64{{1, 2}, {10, 20}, {100, 200}},
		test.NewSeriesMeta("b", 2),
		[][]float64{{3, 4}, {30, 40}},
		test.NewSeriesMeta("a", 3),
		[][]float64{{1, 2}, {10, 20}, {100, 200}},
		nil,
	},
	{
		"mismatched step counts",
		test.NewSeriesMeta("a", 2),
		[][]float64{{1, 2, 3}, {10, 20, 30}},
		test.NewSeriesMeta("b", 2),
		[][]float64{{3, 4}, {30, 40}},
		[]block.SeriesMeta{},
		[][]float64{},
		errMismatchedStepCounts,
	},
}

func TestUnless(t *testing.T) {
	for _, tt := range unlessTests {
		t.Run(tt.name, func(t *testing.T) {
			_, bounds := test.GenerateValuesAndBounds(nil, nil)

			op := NewUnlessOp(parser.NodeID(0), parser.NodeID(1), &VectorMatching{})
			c, sink := executor.NewControllerWithSink(parser.NodeID(2))
			node := op.Node(c)

			lhs := test.NewBlockFromValuesWithMeta(bounds, tt.lhsMeta, tt.lhs)
			err := node.Process(parser.NodeID(0), lhs)
			require.NoError(t, err)

			rhs := test.NewBlockFromValuesWithMeta(bounds, tt.rhsMeta, tt.rhs)
			err = node.Process(parser.NodeID(1), rhs)
			if tt.err != nil {
				require.EqualError(t, err, tt.err.Error())
				return
			}

			require.NoError(t, err)
			test.EqualsWithNans(t, tt.expected, sink.Values)
			assert.Equal(t, tt.expectedMetas, sink.Metas)
		})
	}
}
