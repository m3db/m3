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
	"fmt"
	"math"
	"sort"

	"github.com/m3db/m3/src/query/graphite/ts"
	"github.com/m3db/m3/src/x/errors"
)

const (
	// FloatingPointFormat is the floating point format for naming
	FloatingPointFormat = "%.3f"
)

// ErrInvalidPercentile is used when the percentile specified is incorrect
func ErrInvalidPercentile(percentile float64) error {
	return errors.NewInvalidParamsError(fmt.Errorf("invalid percentile, percentile="+FloatingPointFormat, percentile))
}

// PercentileNamer formats a string with a percentile
type PercentileNamer func(name string, percentile float64) string

// ThresholdComparator compares two floats for other comparison
// functions such as Percentile checks.
type ThresholdComparator func(v, threshold float64) bool

// GreaterThan is a ThresholdComparator function for when
// a value is greater than a threshold
func GreaterThan(v, threshold float64) bool {
	return v > threshold
}

// LessThan is a ThresholdComparator function for when
// a value is less than a threshold
func LessThan(v, threshold float64) bool {
	return v < threshold
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

// SafeSum returns the sum of the input slice the number of NaNs in the input.
func SafeSum(input []float64) (float64, int) {
	nans := 0
	sum := 0.0
	for _, v := range input {
		if !math.IsNaN(v) {
			sum += v
		} else {
			nans += 1
		}
	}
	return sum, nans
}

// SafeMax returns the maximum value of the input slice the number of NaNs in the input.
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

// SafeMin returns the minimum value of the input slice the number of NaNs in the input.
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

// GetPercentile computes the percentile cut off for an array of floats
func GetPercentile(input []float64, percentile float64, interpolate bool) float64 {
	nans := SafeSort(input)
	series := input[nans:]
	if len(series) == 0 {
		return math.NaN()
	}

	fractionalRank := (percentile / 100.0) * (float64(len(series) + 1))
	rank := int(fractionalRank)
	rankFraction := fractionalRank - float64(rank)

	if interpolate == false {
		rank = rank + int(math.Ceil(rankFraction))
	}

	var percentileResult float64
	if rank == 0 {
		percentileResult = series[0]
	} else if rank-1 == len(series) {
		percentileResult = series[len(series)-1]
	} else {
		percentileResult = series[rank-1]
	}

	if interpolate && rank != len(series) {
		nextValue := series[rank]
		percentileResult = percentileResult + (rankFraction * (nextValue - percentileResult))
	}

	return percentileResult
}

// NPercentile returns percentile-percent of each series in the seriesList.
func NPercentile(ctx *Context, in ts.SeriesList, percentile float64, pn PercentileNamer) (ts.SeriesList, error) {
	if percentile < 0.0 || percentile > 100.0 {
		return ts.NewSeriesList(), ErrInvalidPercentile(percentile)
	}
	results := make([]*ts.Series, 0, in.Len())
	for _, s := range in.Values {
		safeValues := s.SafeValues()
		if len(safeValues) == 0 {
			continue
		}
		percentileVal := GetPercentile(safeValues, percentile, false)
		if !math.IsNaN(percentileVal) {
			vals := ts.NewConstantValues(ctx, percentileVal, s.Len(), s.MillisPerStep())
			percentileSeries := ts.NewSeries(ctx, pn(s.Name(), percentile), s.StartTime(), vals)
			results = append(results, percentileSeries)
		}
	}
	in.Values = results
	return in, nil
}

// RemoveByPercentile removes all series above or below the given percentile, as
// determined by the PercentileComparator
func RemoveByPercentile(
	ctx *Context,
	in ts.SeriesList,
	percentile float64,
	pn PercentileNamer,
	tc ThresholdComparator,
) (ts.SeriesList, error) {
	results := make([]*ts.Series, 0, in.Len())
	for _, series := range in.Values {
		single := ts.SeriesList{
			Values:   []*ts.Series{series},
			Metadata: in.Metadata,
		}
		percentileSeries, err := NPercentile(ctx, single, percentile, pn)
		if err != nil {
			return ts.NewSeriesList(), err
		}

		numSteps := series.Len()
		vals := ts.NewValues(ctx, series.MillisPerStep(), numSteps)
		if percentileSeries.Len() == 1 {
			percentile := percentileSeries.Values[0].ValueAt(0)
			for i := 0; i < numSteps; i++ {
				v := series.ValueAt(i)
				if !tc(v, percentile) {
					vals.SetValueAt(i, v)
				}
			}
		}
		name := pn(series.Name(), percentile)
		newSeries := ts.NewSeries(ctx, name, series.StartTime(), vals)
		results = append(results, newSeries)
	}
	in.Values = results
	return in, nil
}
