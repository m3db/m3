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
	"sort"
)

const (
	// QuantileType takes the n-th non nan quantile element in a list of series
	// Special cases are:
	// 	 n < 0 = -Inf
	// 	 n > 1 = +Inf
	QuantileType = "quantile"
)

// Creates a quantile aggregation function for a given q-quantile measurement
func makeQuantileFn(opType string, q float64) (aggregationFn, bool) {
	if opType != QuantileType {
		return nil, false
	}
	return func(values []float64, bucket []int) float64 {
		return bucketedQuantileFn(q, values, bucket)
	}, true
}

func bucketedQuantileFn(q float64, values []float64, bucket []int) float64 {
	if math.IsNaN(q) || len(bucket) == 0 || len(values) == 0 {
		return math.NaN()
	}

	if q < 0 || q > 1 {
		// Use math.Inf(0) == +Inf by truncating q and subtracting 1 to give
		// the correctly signed infinity
		return math.Inf(int(q) - 1)
	}

	bucketVals := make([]float64, 0, len(bucket))
	for _, idx := range bucket {
		val := values[idx]
		if !math.IsNaN(val) {
			bucketVals = append(bucketVals, values[idx])
		}
	}

	return quantileFn(q, bucketVals)
}

func quantileFn(q float64, values []float64) float64 {
	l := float64(len(values))
	if l == 0 {
		// No non-NaN values
		return math.NaN()
	}

	sort.Float64s(values)
	// When the quantile lies between two samples,
	// use a weighted average of the two samples.
	rank := q * (l - 1)

	leftIndex := math.Max(0, math.Floor(rank))
	rightIndex := math.Min(l-1, leftIndex+1)

	weight := rank - math.Floor(rank)
	weightedLeft := values[int(leftIndex)] * (1 - weight)
	weightedRight := values[int(rightIndex)] * weight
	return weightedLeft + weightedRight
}
