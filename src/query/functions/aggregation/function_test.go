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

	"github.com/stretchr/testify/assert"

	"github.com/m3db/m3/src/query/test/compare"
)

type funcTest struct {
	name     string
	fn       aggregationFn
	expected []float64
}

var nan = math.NaN()

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
			{StandardDeviationType, stddevFn, []float64{0}},
			{StandardVarianceType, varianceFn, []float64{0}},
			{CountType, countFn, []float64{1}},
		},
	},
	{
		"two values, one index", []float64{1.5, 2.6}, [][]int{{0, 1}}, []funcTest{
			{SumType, sumFn, []float64{4.1}},
			{MinType, minFn, []float64{1.5}},
			{MaxType, maxFn, []float64{2.6}},
			{AverageType, averageFn, []float64{2.05}},
			{StandardDeviationType, stddevFn, []float64{0.55}},
			{StandardVarianceType, varianceFn, []float64{0.3025}},
			{CountType, countFn, []float64{2}},
		},
	},
	{
		"two values, two index", []float64{1.5, 2.6}, [][]int{{0}, {1}}, []funcTest{
			{SumType, sumFn, []float64{1.5, 2.6}},
			{MinType, minFn, []float64{1.5, 2.6}},
			{MaxType, maxFn, []float64{1.5, 2.6}},
			{AverageType, averageFn, []float64{1.5, 2.6}},
			{StandardDeviationType, stddevFn, []float64{0, 0}},
			{StandardVarianceType, varianceFn, []float64{0, 0}},
			{CountType, countFn, []float64{1, 1}},
		},
	},
	{
		"many values, one index", []float64{10, 8, 10, 8, 8, 4}, [][]int{{0, 1, 2, 3, 4, 5}}, []funcTest{
			{SumType, sumFn, []float64{48}},
			{MinType, minFn, []float64{4}},
			{MaxType, maxFn, []float64{10}},
			{AverageType, averageFn, []float64{8}},
			{StandardDeviationType, stddevFn, []float64{2}},
			{StandardVarianceType, varianceFn, []float64{4}},
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
			{StandardDeviationType, stddevFn, []float64{2, 36.73403}},
			{StandardVarianceType, varianceFn, []float64{4, 1349.38889}},
			{CountType, countFn, []float64{6, 6}},
		},
	},
	{
		"many values, one index, with nans",
		[]float64{10, nan, 10, nan, 8, 4},
		[][]int{{0, 1, 2, 3, 4, 5}}, []funcTest{
			{SumType, sumFn, []float64{32}},
			{MinType, minFn, []float64{4}},
			{MaxType, maxFn, []float64{10}},
			{AverageType, averageFn, []float64{8}},
			{StandardDeviationType, stddevFn, []float64{2.44949}},
			{StandardVarianceType, varianceFn, []float64{6}},
			{CountType, countFn, []float64{4}},
			{AbsentType, absentFn, []float64{nan}},
		},
	},
	{
		"only nans",
		[]float64{nan, nan, nan, nan},
		[][]int{{0, 1, 2, 3}}, []funcTest{
			{SumType, sumFn, []float64{nan}},
			{MinType, minFn, []float64{nan}},
			{MaxType, maxFn, []float64{nan}},
			{AverageType, averageFn, []float64{nan}},
			{StandardDeviationType, stddevFn, []float64{nan}},
			{StandardVarianceType, varianceFn, []float64{nan}},
			{CountType, countFn, []float64{0}},
			{AbsentType, absentFn, []float64{1}},
		},
	},
	{
		"verified population deviations",
		[]float64{9, 2, 5, 4, 12, 7, 8, 11, 9, 3, 7, 4, 12, 5, 4, 10, 9, 6, 9, 4},
		[][]int{{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}}, []funcTest{
			{StandardDeviationType, stddevFn, []float64{2.98329}},
			{StandardVarianceType, varianceFn, []float64{8.9}},
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
					compare.EqualsWithNansWithDelta(t, expected, actual, math.Pow10(-5))
				}
			})
		}
	}
}

var equalValuePrecisionTest = []struct {
	name   string
	values []float64
}{
	{
		"five 1.33e-5",
		[]float64{1.33e-5, 1.33e-5, 1.33e-5, 1.33e-5, 1.33e-5},
	},
	{
		"three 13.3",
		[]float64{13.3, 13.3, 13.3},
	},
}

func TestVarianceFnEqualValuePrecision(t *testing.T) {
	for _, tt := range equalValuePrecisionTest {
		t.Run(tt.name, func(t *testing.T) {
			bucket := make([]int, len(tt.values))
			for i := range bucket {
				bucket[i] = i
			}

			assert.Equal(t, 0.0, varianceFn(tt.values, bucket))
		})
	}
}

func TestStddevFnEqualValuePrecision(t *testing.T) {
	for _, tt := range equalValuePrecisionTest {
		t.Run(tt.name, func(t *testing.T) {
			bucket := make([]int, len(tt.values))
			for i := range bucket {
				bucket[i] = i
			}

			assert.Equal(t, 0.0, stddevFn(tt.values, bucket))
		})
	}
}
