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

// SafeAggregationFn is a safe aggregation function.
type SafeAggregationFn func(input []float64) (float64, int, bool)

// SafeAggregationFns is the collection of safe aggregation functions.
var SafeAggregationFns = map[string]SafeAggregationFn{
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
func SafeSum(input []float64) (float64, int, bool) {
	nans := 0
	sum := 0.0
	for _, v := range input {
		if !math.IsNaN(v) {
			sum += v
		} else {
			nans++
		}
	}
	if len(input) == nans {
		return 0, 0, false // Either no elements or all nans.
	}
	return sum, nans, true
}

// SafeAverage returns the average of the input slice and the number of NaNs in the input.
func SafeAverage(input []float64) (float64, int, bool) {
	sum, nans, ok := SafeSum(input)
	if !ok {
		return 0, 0, false
	}
	if len(input) == nans {
		return 0, 0, false // Either no elements or all nans.
	}
	count := len(input) - nans
	return sum / float64(count), nans, true
}

// SafeMax returns the maximum value of the input slice and the number of NaNs in the input.
func SafeMax(input []float64) (float64, int, bool) {
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
	if len(input) == nans {
		return 0, 0, false // Either no elements or all nans.
	}
	return max, nans, true
}

// SafeMin returns the minimum value of the input slice and the number of NaNs in the input.
func SafeMin(input []float64) (float64, int, bool) {
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
	if len(input) == nans {
		return 0, 0, false // Either no elements or all nans.
	}
	return min, nans, true
}

// SafeMedian returns the median value of the input slice and the number of NaNs in the input.
func SafeMedian(input []float64) (float64, int, bool) {
	safeValues, nans, ok := safeValues(input)
	if !ok {
		return 0, 0, false
	}
	return ts.Median(safeValues, len(safeValues)), nans, true
}

// SafeDiff returns the subtracted value of all the subsequent numbers from the 1st one and
// the number of NaNs in the input.
func SafeDiff(input []float64) (float64, int, bool) {
	safeValues, nans, ok := safeValues(input)
	if !ok {
		return 0, 0, false
	}

	diff := safeValues[0]
	for i := 1; i < len(safeValues); i++ {
		diff -= safeValues[i]
	}

	return diff, nans, true
}

// SafeStddev returns the standard deviation value of the input slice and the number of NaNs in the input.
func SafeStddev(input []float64) (float64, int, bool) {
	safeAvg, nans, ok := SafeAverage(input)
	if !ok {
		return 0, 0, false
	}

	safeValues, _, ok := safeValues(input)
	if !ok {
		return 0, 0, false
	}

	sum := 0.0
	for _, v := range safeValues {
		sum += (v - safeAvg) * (v - safeAvg)
	}

	return math.Sqrt(sum / float64(len(safeValues))), nans, true
}

// SafeRange returns the range value of the input slice and the number of NaNs in the input.
func SafeRange(input []float64) (float64, int, bool) {
	safeMax, nans, ok := SafeMax(input)
	if !ok {
		return 0, 0, false
	}

	safeMin, _, ok := SafeMin(input)
	if !ok {
		return 0, 0, false
	}

	return safeMax - safeMin, nans, true
}

// SafeMul returns the product value of the input slice and the number of NaNs in the input.
func SafeMul(input []float64) (float64, int, bool) {
	safeValues, nans, ok := safeValues(input)
	if !ok {
		return 0, 0, false
	}

	product := 1.0
	for _, v := range safeValues {
		product *= v
	}

	return product, nans, true
}

// SafeLast returns the last value of the input slice and the number of NaNs in the input.
func SafeLast(input []float64) (float64, int, bool) {
	safeValues, nans, ok := safeValues(input)
	if !ok {
		return 0, 0, false
	}

	return safeValues[len(safeValues)-1], nans, true
}

func safeValues(input []float64) ([]float64, int, bool) {
	nans := 0
	safeValues := make([]float64, 0, len(input))
	for _, v := range input {
		if !math.IsNaN(v) {
			safeValues = append(safeValues, v)
		} else {
			nans++
		}
	}
	if len(input) == nans {
		return nil, 0, false // Either no elements or all nans.
	}
	return safeValues, nans, true
}
