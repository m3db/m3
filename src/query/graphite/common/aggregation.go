// Copyright (c) 2019 Uber Technologies, Inc.
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

package common

import (
	"math"
	"sort"

	"github.com/m3db/m3/src/query/graphite/ts"
)

// Range distills down a set of inputs into the range of the series.
func Range(ctx *Context, series ts.SeriesList, renamer SeriesListRenamer) (*ts.Series, error) {
	numSeries := series.Len()
	if numSeries == 0 {
		return nil, ErrEmptySeriesList
	}
	normalized, start, end, millisPerStep, err := Normalize(ctx, series)
	if err != nil {
		return nil, err
	}
	numSteps := ts.NumSteps(start, end, millisPerStep)
	vals := ts.NewValues(ctx, millisPerStep, numSteps)
	nan := math.NaN()

	for i := 0; i < numSteps; i++ {
		minVal, maxVal := nan, nan
		for j := 0; j < numSeries; j++ {
			v := normalized.Values[j].ValueAt(i)
			if math.IsNaN(v) {
				continue
			}
			if math.IsNaN(minVal) || minVal > v {
				minVal = v
			}
			if math.IsNaN(maxVal) || maxVal < v {
				maxVal = v
			}
		}
		if !math.IsNaN(minVal) && !math.IsNaN(maxVal) {
			vals.SetValueAt(i, maxVal-minVal)
		}
	}
	name := renamer(normalized)
	return ts.NewSeries(ctx, name, start, vals), nil
}

type SafeAggregationFn func(input []float64) (float64, int)

// SafeSort sorts the input slice and returns the number of NaNs in the input.
func SafeSort(input []float64) int {
	nans := 0
	for i := 0; i < len(input); i++ {
		if math.IsNaN(input[i]) {
			nans++
		}
	}
	sort.Float64s(input)
	return nans
}

// SafeSum returns the sum of the input slice and  the number of NaNs in the input.
func SafeSum(input []float64) (float64, int) {
	nans := 0
	sum := 0.0
	for _, v := range input {
		if !math.IsNaN(v) {
			sum += v
		} else {
			nans++
		}
	}
	return sum, nans
}

// SafeAverage returns the average of the input slice and the number of NaNs in the input.
func SafeAverage(input []float64) (float64, int) {
	sum, nans := SafeSum(input)
	count := len(input) - nans
	return sum / float64(count), nans
}

// SafeMax returns the maximum value of the input slice and the number of NaNs in the input.
func SafeMax(input []float64) (float64, int) {
	nans := 0
	max := -math.MaxFloat64
	for _, v := range input {
		if math.IsNaN(v) {
			nans++
			continue
		}
		if v > max {
			max = v
		}
	}
	return max, nans
}

// SafeMin returns the minimum value of the input slice and the number of NaNs in the input.
func SafeMin(input []float64) (float64, int) {
	nans := 0
	min := math.MaxFloat64
	for _, v := range input {
		if math.IsNaN(v) {
			nans++
			continue
		}
		if v < min {
			min = v
		}
	}
	return min, nans
}

// SafeMedian returns the median value of the input slice and the number of NaNs in the input.
func SafeMedian(input []float64) (float64, int) {
	safeValues, nans := safeValuesFn(input)

	return ts.Median(safeValues, len(safeValues)), nans
}

// SafeDiff returns the subtracted value of all the subsequent numbers from the 1st one and the number of NaNs in the input.
func SafeDiff(input []float64) (float64, int) {
	safeValues, nans := safeValuesFn(input)

	diff := safeValues[0]
	for i := 1; i < len(safeValues); i++ {
		diff = diff - safeValues[i]
	}

	return diff, nans
}

// SafeStddev returns the standard deviation value of the input slice and the number of NaNs in the input.
func SafeStddev(input []float64) (float64, int) {
	safeAvg, nans := SafeAverage(input)

	sum := 0.0
	safeValues, _ := safeValuesFn(input)
	for _, v := range safeValues {
		sum = sum + (v-safeAvg)*(v-safeAvg)
	}

	return math.Sqrt(sum / float64(len(safeValues))), nans
}

// SafeRange returns the range value of the input slice and the number of NaNs in the input.
func SafeRange(input []float64) (float64, int) {
	safeMax, nans := SafeMax(input)
	safeMin, _ := SafeMin(input)

	return safeMax - safeMin, nans
}

// SafeMul returns the product value of the input slice and the number of NaNs in the input
func SafeMul(input []float64) (float64, int) {
	safeValues, nans := safeValuesFn(input)

	product := 1.0
	for _, v := range safeValues {
		product = product * v
	}

	return product, nans
}

// SafeLast returns the last value of the input slice and the number of NaNs in the input
func SafeLast(input []float64) (float64, int) {
	safeValues, nans := safeValuesFn(input)
	if len(safeValues) >= 1 {
		return safeValues[len(safeValues)-1], nans
	} else {
		return math.NaN(), nans
	}
}

func safeValuesFn(input []float64) ([]float64, int) {
	if len(input) == 0 {
		return []float64{math.NaN()}, 0
	}

	nans := 0
	var safeValues []float64
	for _, v := range input {
		if !math.IsNaN(v) {
			safeValues = append(safeValues, v)
		} else {
			nans++
		}
	}
	return safeValues, nans
}

var SafeAggregationFuncs = map[string]SafeAggregationFn{
	"sum":      SafeSum,
	"avg":      SafeAverage,
	"average":  SafeAverage,
	"max":      SafeMax,
	"min":      SafeMin,
	"median":   SafeMedian,
	"diff":     SafeDiff,
	"stddev":   SafeStddev,
	"range":    SafeRange,
	"multiply": SafeMul,
	"last":     SafeLast,
}
