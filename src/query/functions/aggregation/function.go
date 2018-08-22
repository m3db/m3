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
	// StandardDeviationType takes the standard deviation of all non nan elements
	// in a list of series
	StandardDeviationType = "stddev"
	// StandardVarianceType takes the standard variance of all non nan elements
	// in a list of series
	StandardVarianceType = "var"
	// CountType counts all non nan elements in a list of series
	CountType = "count"
)

func sumFn(values []float64, indices [][]int) []float64 {
	sumByIndices := make([]float64, len(indices))
	for i, indexList := range indices {
		sum := 0.0
		for _, idx := range indexList {
			v := values[idx]
			if !math.IsNaN(v) {
				sum += v
			}
		}

		sumByIndices[i] = sum
	}

	return sumByIndices
}

func minFn(values []float64, indices [][]int) []float64 {
	minByIndices := make([]float64, len(indices))
	for i, indexList := range indices {
		min := math.NaN()
		for _, idx := range indexList {
			v := values[idx]
			if !math.IsNaN(v) {
				if math.IsNaN(min) {
					min = v
				} else {
					if min > v {
						min = v
					}
				}
			}
		}

		minByIndices[i] = min
	}

	return minByIndices
}

func maxFn(values []float64, indices [][]int) []float64 {
	maxByIndices := make([]float64, len(indices))
	for i, indexList := range indices {
		max := math.NaN()
		for _, idx := range indexList {
			v := values[idx]
			if !math.IsNaN(v) {
				if math.IsNaN(max) {
					max = v
				} else {
					if max < v {
						max = v
					}
				}
			}
		}

		maxByIndices[i] = max
	}

	return maxByIndices
}

func averageFn(values []float64, indices [][]int) []float64 {
	averageByIndices := make([]float64, len(indices))
	for i, indexList := range indices {
		sum := 0.0
		count := 0.0
		for _, idx := range indexList {
			v := values[idx]
			if !math.IsNaN(v) {
				sum += v
				count++
			}
		}

		// Cannot take average of no values
		if count == 0 {
			averageByIndices[i] = math.NaN()
			continue
		}

		averageByIndices[i] = sum / count
	}

	return averageByIndices
}

func stddevFn(values []float64, indices [][]int) []float64 {
	stddevByIndices := make([]float64, len(indices))
	for i, indexList := range indices {
		sum := 0.0
		count := 0.0
		for _, idx := range indexList {
			v := values[idx]
			if !math.IsNaN(v) {
				sum += v
				count++
			}
		}

		// Cannot take standard deviation of one or fewer values
		if count < 2 {
			stddevByIndices[i] = math.NaN()
			continue
		}

		average := sum / count
		sumOfSquares := 0.0
		for _, idx := range indexList {
			v := values[idx]
			if !math.IsNaN(v) {
				diff := v - average
				sumOfSquares += diff * diff
			}
		}

		variance := sumOfSquares / (count - 1)
		stddevByIndices[i] = math.Sqrt(variance)
	}

	return stddevByIndices
}

func varianceFn(values []float64, indices [][]int) []float64 {
	varianceByIndices := make([]float64, len(indices))
	for i, indexList := range indices {
		sum := 0.0
		count := 0.0
		for _, idx := range indexList {
			v := values[idx]
			if !math.IsNaN(v) {
				sum += v
				count++
			}
		}

		// Cannot take standard variance of one or fewer values
		if count < 2 {
			varianceByIndices[i] = math.NaN()
			continue
		}

		average := sum / count
		sumOfSquares := 0.0
		for _, idx := range indexList {
			v := values[idx]
			if !math.IsNaN(v) {
				diff := v - average
				sumOfSquares += diff * diff
			}
		}

		varianceByIndices[i] = sumOfSquares / (count - 1)
	}

	return varianceByIndices
}

func countFn(values []float64, indices [][]int) []float64 {
	countByIndices := make([]float64, len(indices))
	for i, indexList := range indices {
		count := 0.0
		for _, idx := range indexList {
			v := values[idx]
			if !math.IsNaN(v) {
				count++
			}
		}

		countByIndices[i] = count
	}

	return countByIndices
}
