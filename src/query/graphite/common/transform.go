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

	"github.com/m3db/m3/src/query/graphite/ts"
	"github.com/m3db/m3/src/x/errors"
)

// TransformFunc is used by Transform to apply a function
// to all values in a series.
type TransformFunc func(float64) float64

// TransformFuncFactory creates transformation functions
type TransformFuncFactory func() TransformFunc

// Transformer transforms a value
type Transformer interface {
	// Apply applies the transformation
	Apply(value float64) float64

	// Reset resets the state
	Reset()
}

type statelessTransformer struct {
	fn TransformFunc
}

// NewStatelessTransformer creates a new stateless transformer
func NewStatelessTransformer(fn TransformFunc) Transformer {
	return statelessTransformer{fn: fn}
}

func (t statelessTransformer) Apply(value float64) float64 {
	return t.fn(value)
}

func (t statelessTransformer) Reset() {}

// MaintainNaNTransformer only applies a given ValueTransformer to
// non-NaN values.
func MaintainNaNTransformer(f TransformFunc) TransformFunc {
	return func(v float64) float64 {
		if math.IsNaN(v) {
			return v
		}
		return f(v)
	}
}

// Scale multiplies each element of a series list by a given value.
func Scale(scale float64) TransformFunc {
	return MaintainNaNTransformer(func(v float64) float64 {
		return v * scale
	})
}

// Offset adds a value to each element of a series list.
func Offset(factor float64) TransformFunc {
	return MaintainNaNTransformer(func(v float64) float64 {
		return v + factor
	})
}

// TransformNull transforms all nulls in a series to a value.
func TransformNull(value float64) TransformFunc {
	return func(v float64) float64 {
		if math.IsNaN(v) {
			return value
		}

		return v
	}
}

// IsNonNull replaces datapoints that are non-null with 1, and null values with 0.
// This is useful for understanding which series have data at a given point in time (i.e. to count
// which servers are alive).
func IsNonNull() TransformFunc {
	return func(v float64) float64 {
		if math.IsNaN(v) {
			return 0
		}

		return 1
	}
}

// PredicateFn is a predicate function.
type PredicateFn func(v float64) bool

// Filter removes data that does not satisfy a given predicate.
func Filter(fn PredicateFn) TransformFunc {
	return MaintainNaNTransformer(func(v float64) float64 {
		if !fn(v) {
			return math.NaN()
		}

		return v
	})
}

// Logarithm takes one series or a series list, and draws the y-axis in logarithmic format. Only support
// base 10 logarithms.
func Logarithm() TransformFunc {
	return func(v float64) float64 {
		if !math.IsNaN(v) && v > 0 {
			return math.Log10(v)
		}

		return math.NaN()
	}
}

// Integral returns a function that accumulates values it has seen
func Integral() TransformFunc {
	currentSum := 0.0

	return func(v float64) float64 {
		if !math.IsNaN(v) {
			currentSum += v
		} else {
			return v
		}
		return currentSum
	}
}

// Derivative returns a function that computes the derivative among the values
// it has seen
func Derivative() TransformFunc {
	previousValue := math.NaN()

	return func(v float64) float64 {
		var r float64
		if math.IsNaN(v) || math.IsNaN(previousValue) {
			previousValue, r = v, math.NaN()
		} else {
			previousValue, r = v, v-previousValue
		}
		return r
	}
}

// NonNegativeDerivative returns a function that computes the derivative among the
// values it has seen but ignores datapoints that trend down
func NonNegativeDerivative(maxValue float64) TransformFunc {
	previousValue := math.NaN()

	return func(v float64) float64 {
		var r float64

		if math.IsNaN(v) || math.IsNaN(previousValue) {
			previousValue, r = v, math.NaN()
		} else if difference := v - previousValue; difference >= 0 {
			previousValue, r = v, difference
		} else if !math.IsNaN(maxValue) && maxValue >= v {
			previousValue, r = v, (maxValue-previousValue)+v+1.0
		} else {
			previousValue, r = v, math.NaN()
		}
		return r
	}
}

// Transform applies a specified ValueTransform to all values in each series, renaming
// each series with the given SeriesRenamer.
func Transform(ctx *Context, in ts.SeriesList, t Transformer, renamer SeriesRenamer) (ts.SeriesList, error) {
	results := make([]*ts.Series, in.Len())

	for i, series := range in.Values {
		t.Reset()
		values := ts.NewValues(ctx, series.MillisPerStep(), series.Len())
		for step := 0; step < series.Len(); step++ {
			value := series.ValueAt(step)
			values.SetValueAt(step, t.Apply(value))
		}

		results[i] = ts.NewSeries(ctx, renamer(series), series.StartTime(), values)
	}

	in.Values = results
	return in, nil
}

// Stdev takes one metric or a wildcard seriesList followed by an integer N. Draw the standard deviation
// of all metrics passed for the past N datapoints. If the ratio of null points in the window is greater than
// windowTolerance, skip the calculation.
func Stdev(ctx *Context, in ts.SeriesList, points int, windowTolerance float64, renamer RenamerWithNumPoints) (ts.SeriesList, error) {
	if points <= 0 {
		return ts.NewSeriesList(), errors.NewInvalidParamsError(fmt.Errorf("invalid window size, points=%d", points))
	}
	results := make([]*ts.Series, 0, in.Len())
	for _, series := range in.Values {
		stdevName := renamer(series, points)
		stdevVals := ts.NewValues(ctx, series.MillisPerStep(), series.Len())
		validPoints := 0
		currentSum := 0.0
		currentSumOfSquares := 0.0
		for index := 0; index < series.Len(); index++ {
			newValue := series.ValueAt(index)
			var bootstrapping bool
			var droppedValue float64

			// Mark whether we've reached our window size, don't drop points out otherwise
			if index < points {
				bootstrapping = true
				droppedValue = math.NaN()
			} else {
				bootstrapping = false
				droppedValue = series.ValueAt(index - points)
			}

			// Remove the value that just dropped out of the window
			if !bootstrapping && !math.IsNaN(droppedValue) {
				validPoints--
				currentSum -= droppedValue
				currentSumOfSquares -= droppedValue * droppedValue
			}

			// Add in the value that just popped in the window
			if !math.IsNaN(newValue) {
				validPoints++
				currentSum += newValue
				currentSumOfSquares += newValue * newValue
			}

			if validPoints > 0 && float64(validPoints)/float64(points) >= windowTolerance {
				deviation := math.Sqrt(float64(validPoints)*currentSumOfSquares-currentSum*currentSum) / float64(validPoints)
				stdevVals.SetValueAt(index, deviation)
			}
		}
		stdevSeries := ts.NewSeries(ctx, stdevName, series.StartTime(), stdevVals)
		results = append(results, stdevSeries)
	}
	in.Values = results
	return in, nil
}

// RenamerWithNumPoints is a signature for renaming a single series that is passed to Stdev
type RenamerWithNumPoints func(series *ts.Series, points int) string

// PerSecond computes the derivative between consecutive values in the a time series, taking into
// account the time interval between the values. It skips missing values, and calculates the
// derivative between consecutive non-missing values.
func PerSecond(ctx *Context, in ts.SeriesList, renamer SeriesRenamer) (ts.SeriesList, error) {
	results := make([]*ts.Series, 0, in.Len())

	for _, series := range in.Values {
		var (
			vals             = ts.NewValues(ctx, series.MillisPerStep(), series.Len())
			prev             = math.NaN()
			secsPerStep      = float64(series.MillisPerStep()) / 1000
			secsSinceLastVal = secsPerStep
		)

		for step := 0; step < series.Len(); step++ {
			cur := series.ValueAt(step)

			if math.IsNaN(prev) {
				vals.SetValueAt(step, math.NaN())
				prev = cur
				continue
			}

			if math.IsNaN(cur) {
				vals.SetValueAt(step, math.NaN())
				secsSinceLastVal += secsPerStep
				continue
			}

			diff := cur - prev

			if diff >= 0 {
				vals.SetValueAt(step, diff/secsSinceLastVal)
			} else {
				vals.SetValueAt(step, math.NaN())
			}

			prev = cur
			secsSinceLastVal = secsPerStep
		}

		s := ts.NewSeries(ctx, renamer(series), series.StartTime(), vals)
		results = append(results, s)
	}

	in.Values = results
	return in, nil
}
