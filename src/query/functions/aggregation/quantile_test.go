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

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuantileFn(t *testing.T) {
	values := []float64{3.1, 100, 200, 300, 2.1, 800, 1.1, 4.1, 5.1}
	// NB Taken values by bucket: [3.1, 2.1, 1.1, 4.1]
	buckets := []int{0, 4, 7, 6}
	ns := make([]float64, 13)
	// set ns to -0.1, 0, ..., 1, 1.1
	for i := range ns {
		ns[i] = -0.1 + 0.1*float64(i)
	}

	// 10 steps over length of 3 with uniform step sizes,
	// expected to go up from values[0] by 0.3 each step.
	expected := make([]float64, len(ns))
	for i, v := range ns {
		expected[i] = 1.1 + v*3
	}
	// Set expected at q < 0 || q > 1
	expected[0] = math.Inf(-1)
	expected[len(ns)-1] = math.Inf(1)

	actual := make([]float64, len(ns))
	for i, n := range ns {
		actual[i] = quantileFn(n, values, buckets)
	}

	test.EqualsWithNansWithDelta(t, expected, actual, math.Pow10(-5))
}

func TestQuantileFnMostlyNan(t *testing.T) {
	values := []float64{math.NaN(), math.NaN(), 1, math.NaN(), 0.5}
	buckets := []int{0, 1, 2, 3, 4}
	ns := make([]float64, 13)
	// set ns to -0.1, 0, ..., 1, 1.1
	for i := range ns {
		ns[i] = -0.1 + 0.1*float64(i)
	}

	// 10 steps over length of 0.5 with uniform step sizes,
	// expected to go up from values[0] by 0.05 each step.
	expected := make([]float64, len(ns))
	for i, v := range ns {
		expected[i] = 0.5 + v*0.5
	}
	// Set expected at q < 0 || q > 1
	expected[0] = math.Inf(-1)
	expected[len(ns)-1] = math.Inf(1)

	actual := make([]float64, len(ns))
	for i, n := range ns {
		actual[i] = quantileFn(n, values, buckets)
	}

	test.EqualsWithNansWithDelta(t, expected, actual, math.Pow10(-5))
}

func TestQuantileFnSingleNonNan(t *testing.T) {
	values := []float64{math.NaN(), math.NaN(), 1, math.NaN(), math.NaN()}
	buckets := []int{0, 1, 2, 3, 4}
	ns := make([]float64, 13)
	// set ns to -0.1, 0, ..., 1, 1.1
	for i := range ns {
		ns[i] = -0.1 + 0.1*float64(i)
	}

	// Only non Nan value is 1, all values should be 1
	expected := make([]float64, len(ns))
	for i := range expected {
		expected[i] = 1
	}
	// Set expected at q < 0 || q > 1
	expected[0] = math.Inf(-1)
	expected[len(ns)-1] = math.Inf(1)

	actual := make([]float64, len(ns))
	for i, n := range ns {
		actual[i] = quantileFn(n, values, buckets)
	}

	test.EqualsWithNansWithDelta(t, expected, actual, math.Pow10(-5))
}

func TestQuantileCreationFn(t *testing.T) {
	n := 0.145
	op, success := makeQuantileFn("badOp", n)
	assert.False(t, success)
	assert.Nil(t, op)

	op, success = makeQuantileFn(QuantileType, n)
	assert.True(t, success)

	values := []float64{11, math.NaN(), 13.1, 0.1, -5.1}
	buckets := []int{0, 1, 2, 3, 4}

	quantile := op(values, buckets)
	// NB: expected calculated independently
	expected := -2.838
	test.EqualsWithNansWithDelta(t, expected, quantile, math.Pow10(-5))
}

func TestQuantileFunctionFilteringWithoutA(t *testing.T) {
	op, err := NewAggregationOp(QuantileType, NodeParams{
		MatchingTags: []string{"a"}, Without: true, Parameter: 0.6,
	})
	require.NoError(t, err)
	sink := processAggregationOp(t, op)
	expected := [][]float64{
		// 0.6 quantile of first two series
		{0, 6, 5, 6, 7},
		// 0.6 quantile of third, fourth, and fifth series
		{60, 88, 116, 144, 172},
		// stddev of sixth series
		{600, 700, 800, 900, 1000},
	}

	expectedMetas := []block.SeriesMeta{
		{Name: QuantileType, Tags: models.Tags{}},
		{Name: QuantileType, Tags: models.Tags{"b": "2"}},
		{Name: QuantileType, Tags: models.Tags{"c": "3"}},
	}
	expectedMetaTags := models.Tags{"d": "4"}

	test.CompareValues(t, sink.Metas, expectedMetas, sink.Values, expected)
	assert.Equal(t, bounds, sink.Meta.Bounds)
	assert.Equal(t, expectedMetaTags, sink.Meta.Tags)
}

func TestNans(t *testing.T) {
	actual := quantileFn(0.5, []float64{}, []int{})
	assert.True(t, math.IsNaN(actual))

	actual = quantileFn(0.5, []float64{1}, []int{})
	assert.True(t, math.IsNaN(actual))

	actual = quantileFn(0.5, []float64{}, []int{1})
	assert.True(t, math.IsNaN(actual))

	// all NaNs in bucket
	values := []float64{math.NaN(), math.NaN(), 1, math.NaN(), math.NaN()}
	buckets := []int{0, 1, 3, 4}
	actual = quantileFn(0.5, values, buckets)
	assert.True(t, math.IsNaN(actual))
}
