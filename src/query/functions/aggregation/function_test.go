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

package aggregation

import (
	"math"
	"testing"

	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/test/executor"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCountWithAllValues(t *testing.T) {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)
	block := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	op, err := NewAggregationOp(CountType, NodeParams{})
	require.NoError(t, err)
	countNode := op.(baseOp).Node(c, transform.Options{})
	err = countNode.Process(parser.NodeID(0), block)
	require.NoError(t, err)
	expected := make([]float64, len(values[0]))
	for i := range expected {
		expected[i] = 2
	}
	assert.Len(t, sink.Values, 1)
	assert.Equal(t, expected, sink.Values[0])
}

func TestCountWithSomeValues(t *testing.T) {
	v := [][]float64{
		{0, math.NaN(), 2, 3, 4},
		{math.NaN(), 6, 7, 8, 9},
	}

	values, bounds := test.GenerateValuesAndBounds(v, nil)
	block := test.NewBlockFromValues(bounds, values)
	c, sink := executor.NewControllerWithSink(parser.NodeID(1))
	op, err := NewAggregationOp(CountType, NodeParams{})
	require.NoError(t, err)
	countNode := op.(baseOp).Node(c, transform.Options{})
	err = countNode.Process(parser.NodeID(0), block)
	require.NoError(t, err)
	expected := []float64{1, 1, 2, 2, 2}
	assert.Len(t, sink.Values, 1)
	assert.Equal(t, expected, sink.Values[0])
}

type funcTest struct {
	name     string
	fn       aggregationFn
	expected []float64
}

var fnTest = []struct {
	name      string
	values    []float64
	buckets   [][]int
	functions []funcTest
}{
	{
		"empty", []float64{}, [][]int{}, []funcTest{
			{SumType, sumFn, []float64{}},
			{MinType, minFn, []float64{}},
			{MaxType, maxFn, []float64{}},
			{AverageType, averageFn, []float64{}},
			{StandardDeviationType, stddevFn, []float64{}},
			{StandardVarianceType, varianceFn, []float64{}},
			{CountType, countFn, []float64{}},
		},
	},
	{
		"one value", []float64{1.5}, [][]int{{0}}, []funcTest{
			{SumType, sumFn, []float64{1.5}},
			{MinType, minFn, []float64{1.5}},
			{MaxType, maxFn, []float64{1.5}},
			{AverageType, averageFn, []float64{1.5}},
			{StandardDeviationType, stddevFn, []float64{math.NaN()}},
			{StandardVarianceType, varianceFn, []float64{math.NaN()}},
			{CountType, countFn, []float64{1}},
		},
	},
	{
		"two values, one index", []float64{1.5, 2.6}, [][]int{{0, 1}}, []funcTest{
			{SumType, sumFn, []float64{4.1}},
			{MinType, minFn, []float64{1.5}},
			{MaxType, maxFn, []float64{2.6}},
			{AverageType, averageFn, []float64{2.05}},
			{StandardDeviationType, stddevFn, []float64{0.777817}},
			{StandardVarianceType, varianceFn, []float64{0.605}},
			{CountType, countFn, []float64{2}},
		},
	},
	{
		"two values, two index", []float64{1.5, 2.6}, [][]int{{0}, {1}}, []funcTest{
			{SumType, sumFn, []float64{1.5, 2.6}},
			{MinType, minFn, []float64{1.5, 2.6}},
			{MaxType, maxFn, []float64{1.5, 2.6}},
			{AverageType, averageFn, []float64{1.5, 2.6}},
			{StandardDeviationType, stddevFn, []float64{math.NaN(), math.NaN()}},
			{StandardVarianceType, varianceFn, []float64{math.NaN(), math.NaN()}},
			{CountType, countFn, []float64{1, 1}},
		},
	},
	{
		"many values, one index", []float64{10, 8, 10, 8, 8, 4}, [][]int{{0, 1, 2, 3, 4, 5}}, []funcTest{
			{SumType, sumFn, []float64{48}},
			{MinType, minFn, []float64{4}},
			{MaxType, maxFn, []float64{10}},
			{AverageType, averageFn, []float64{8}},
			{StandardDeviationType, stddevFn, []float64{2.19089}},
			{StandardVarianceType, varianceFn, []float64{4.8}},
			{CountType, countFn, []float64{6}},
		},
	},
	{
		"many values, many indices",
		[]float64{10, 17, 8, 1.5, 10, -3, 8, 100, 8, 0, 4, -0.5},
		[][]int{{0, 2, 4, 6, 8, 10}, {1, 3, 5, 7, 9, 11}},
		[]funcTest{
			{SumType, sumFn, []float64{48, 115}},
			{MinType, minFn, []float64{4, -3}},
			{MaxType, maxFn, []float64{10, 100}},
			{AverageType, averageFn, []float64{8, 19.16666}},
			{StandardDeviationType, stddevFn, []float64{2.19089, 40.24011}},
			{StandardVarianceType, varianceFn, []float64{4.8, 1619.26667}},
			{CountType, countFn, []float64{6, 6}},
		},
	},
	{
		"many values, one index, with nans",
		[]float64{10, math.NaN(), 10, math.NaN(), 8, 4},
		[][]int{{0, 1, 2, 3, 4, 5}}, []funcTest{
			{SumType, sumFn, []float64{32}},
			{MinType, minFn, []float64{4}},
			{MaxType, maxFn, []float64{10}},
			{AverageType, averageFn, []float64{8}},
			{StandardDeviationType, stddevFn, []float64{2.82843}},
			{StandardVarianceType, varianceFn, []float64{8}},
			{CountType, countFn, []float64{4}},
		},
	},
	{
		"only nans",
		[]float64{math.NaN(), math.NaN(), math.NaN(), math.NaN()},
		[][]int{{0, 1, 2, 3}}, []funcTest{
			{SumType, sumFn, []float64{0}},
			{MinType, minFn, []float64{math.NaN()}},
			{MaxType, maxFn, []float64{math.NaN()}},
			{AverageType, averageFn, []float64{math.NaN()}},
			{StandardDeviationType, stddevFn, []float64{math.NaN()}},
			{StandardVarianceType, varianceFn, []float64{math.NaN()}},
			{CountType, countFn, []float64{0}},
		},
	},
}

func TestAggFns(t *testing.T) {
	for _, tt := range fnTest {
		for _, function := range tt.functions {
			t.Run(tt.name+" "+function.name, func(t *testing.T) {
				for i, bucket := range tt.buckets {
					actual := function.fn(tt.values, bucket)
					expected := function.expected[i]
					test.EqualsWithNansWithDelta(t, expected, actual, 0.00001)
				}
			})
		}
	}
}
