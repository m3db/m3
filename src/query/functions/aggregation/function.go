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
)

const (
	// SumType adds all non nan elements in a list of series
	SumType = "sum"
	// MinType takes the minimum all non nan elements in a list of series
	MinType = "min"
	// MaxType takes the maximum all non nan elements in a list of series
	MaxType = "max"
	// AverageType averages all non nan elements in a list of series
	AverageType = "avg"
	// StandardDeviationType takes the population standard deviation of all non
	// nan elements in a list of series
	StandardDeviationType = "stddev"
	// StandardVarianceType takes the population standard variance of all non
	// nan elements in a list of series
	StandardVarianceType = "var"
	// CountType counts all non nan elements in a list of series
	CountType = "count"
)

func sumAndCount(values []float64, bucket []int) (float64, float64) {
	sum := 0.0
	count := 0.0
	for _, idx := range bucket {
		v := values[idx]
		if !math.IsNaN(v) {
			sum += v
			count++
		}
	}

	// If all elements are NaN, sum should be NaN
	if count == 0 {
		sum = math.NaN()
	}

	return sum, count
}

func sumFn(values []float64, bucket []int) float64 {
	sum, _ := sumAndCount(values, bucket)
	return sum
}

func minFn(values []float64, bucket []int) float64 {
	min := math.NaN()
	for _, idx := range bucket {
		v := values[idx]
		if !math.IsNaN(v) {
			if math.IsNaN(min) || min > v {
				min = v
			}
		}
	}

	return min
}

func maxFn(values []float64, bucket []int) float64 {
	max := math.NaN()
	for _, idx := range bucket {
		v := values[idx]
		if !math.IsNaN(v) {
			if math.IsNaN(max) || max < v {
				max = v
			}
		}
	}

	return max
}

func averageFn(values []float64, bucket []int) float64 {
	sum, count := sumAndCount(values, bucket)

	// Cannot take average of no values
	if count == 0 {
		return math.NaN()
	}

	return sum / count
}

func stddevFn(values []float64, bucket []int) float64 {
	return math.Sqrt(varianceFn(values, bucket))
}

func varianceFn(values []float64, bucket []int) float64 {
	sum, count := sumAndCount(values, bucket)

	// Cannot take population standard deviation of less than 1 value
	if count < 1 {
		return math.NaN()
	}

	average := sum / count
	sumOfSquares := 0.0
	for _, idx := range bucket {
		v := values[idx]
		if !math.IsNaN(v) {
			diff := v - average
			sumOfSquares += diff * diff
		}
	}

	return sumOfSquares / count
}

func countFn(values []float64, bucket []int) float64 {
	_, count := sumAndCount(values, bucket)
	return count
}
