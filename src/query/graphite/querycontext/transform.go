package querycontext

import (
	"fmt"
	"math"

	"github.com/m3db/m3/src/query/graphite/ts"
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

type statefulTransformer struct {
	factory TransformFuncFactory
	fn      TransformFunc
}

// NewStatefulTransformer creates a new stateful transformer
func NewStatefulTransformer(factory TransformFuncFactory) Transformer {
	return &statefulTransformer{factory: factory}
}

func (t *statefulTransformer) Apply(value float64) float64 {
	if t.fn == nil {
		t.fn = t.factory()
	}
	return t.fn(value)
}

func (t *statefulTransformer) Reset() {
	t.fn = nil
}

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

// IsNonNull takes a series or series list and counts up how many non-null values are specified.
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
		results[i].Tags = series.Tags
	}

	in.Values = results
	return in, nil
}

// Stdev takes one metric or a wildcard seriesList followed by an integer N. Draw the standard deviation
// of all metrics passed for the past N datapoints. If the ratio of null points in the window is greater than
// windowTolerance, skip the calculation.
func Stdev(ctx *Context, in ts.SeriesList, points int, windowTolerance float64, renamer RenamerWithNumPoints) (ts.SeriesList, error) {
	if points <= 0 {
		return ts.SeriesList{}, fmt.Errorf("invalid window size, points=%d", points)
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
		// Preserve tags
		stdevSeries.Tags = series.Tags
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
		s.Tags = series.Tags
		results = append(results, s)
	}

	in.Values = results
	return in, nil
}

// DynamicComparator compares values of input series to values of an operand. Values of the input
// timeseries will get returned if the result of the callback function is true, else a NaN is
// written to the new time series.
func DynamicComparator(
	ctx *Context,
	in, operand ts.SeriesList,
	comparator string,
	fn func(v float64, comp float64) bool,
) (ts.SeriesList, error) {
	if operand.Len() != 1 {
		return ts.SeriesList{},
			fmt.Errorf(
				"operand must contain exactly 1 time series, instead it contains: %d time series",
				operand.Len())
	}

	var (
		operandSeries = operand.Values[0]
		results       = make([]*ts.Series, 0, in.Len())
		valueSet      = make([]ts.MutableValues, 0, in.Len())
	)

	normalized, _, _, _, err := Normalize(ctx, ts.SeriesList{
		Values: append(in.Values, operandSeries),
	})
	if err != nil {
		return ts.SeriesList{}, fmt.Errorf("issue normalizing input and operand timeseries: %v", err)
	}

	in.Values = normalized.Values[:len(normalized.Values)-1]
	operandSeries = normalized.Values[len(normalized.Values)-1]

	for _, series := range in.Values {
		values := ts.NewValues(ctx, series.MillisPerStep(), series.Len())
		valueSet = append(valueSet, values)
	}

	for i := 0; i < operandSeries.Len(); i++ {
		opVal := operandSeries.ValueAt(i)
		for j, inSeries := range in.Values {
			inVal := inSeries.ValueAt(i)

			if math.IsNaN(inVal) || math.IsNaN(opVal) {
				continue
			}

			if fn(inVal, opVal) {
				valueSet[j].SetValueAt(i, inVal)
			}
		}
	}

	for i, s := range in.Values {
		results = append(results, ts.NewSeries(ctx,
			fmt.Sprintf("%s | %s (%s)", s.Name(), comparator, operandSeries.Name()),
			s.StartTime(),
			valueSet[i]))
	}

	in.Values = results
	return in, nil
}
