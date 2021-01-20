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

package native

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/m3db/m3/src/query/graphite/common"
	"github.com/m3db/m3/src/query/graphite/errors"
	"github.com/m3db/m3/src/query/graphite/graphite"
	"github.com/m3db/m3/src/query/graphite/ts"
	"github.com/m3db/m3/src/query/util"
	xerrors "github.com/m3db/m3/src/x/errors"
)

const (
	millisPerSecond  = 1000
	secondsPerDay    = 24 * 3600
	daysPerWeek      = 7
	secondsPerWeek   = secondsPerDay * daysPerWeek
	cactiStyleFormat = "%.2f"
	wrappingFmt      = "%s(%s)"
	alpha            = 0.1
	gamma            = 0.1
	beta             = 0.0035
)

func joinPathExpr(series ts.SeriesList) string {
	seen := make(map[string]struct{})

	joined := make([]string, 0, series.Len())
	for _, s := range series.Values {
		if len(s.Specification) == 0 {
			continue
		}

		if _, exists := seen[s.Specification]; exists {
			continue
		}

		seen[s.Specification] = struct{}{}
		joined = append(joined, s.Specification)
	}

	return strings.Join(joined, ",")
}

// sortByName sorts timeseries results by their names
func sortByName(_ *common.Context, series singlePathSpec) (ts.SeriesList, error) {
	sorted := make([]*ts.Series, len(series.Values))
	for i := range series.Values {
		sorted[i] = series.Values[i]
	}

	sort.Sort(ts.SeriesByName(sorted))

	r := ts.SeriesList(series)
	r.Values = sorted
	r.SortApplied = true
	return r, nil
}

// sortByTotal sorts timeseries results by the sum of values.
func sortByTotal(ctx *common.Context, series singlePathSpec) (ts.SeriesList, error) {
	return highestSum(ctx, series, len(series.Values))
}

// sortByMaxima sorts timeseries by the maximum value across the time period specified.
func sortByMaxima(ctx *common.Context, series singlePathSpec) (ts.SeriesList, error) {
	return highestMax(ctx, series, len(series.Values))
}

// useSeriesAbove compares the maximum of each series against the given `value`. If the series
// maximum is greater than `value`, the regular expression search and replace is
// applied against the series name to plot a related metric.
//
// e.g. given useSeriesAbove(ganglia.metric1.reqs,10,'reqs','time'),
// the response time metric will be plotted only when the maximum value of the
// corresponding request/s metric is > 10
// Example: useSeriesAbove(ganglia.metric1.reqs,10,"reqs","time")
func useSeriesAbove(ctx *common.Context, seriesList singlePathSpec, maxAllowedValue float64, search, replace string) (ts.SeriesList, error) {
	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		multiErr xerrors.MultiError
		newNames []string

		output         = make([]*ts.Series, 0, len(seriesList.Values))
		maxConcurrency = runtime.NumCPU() / 2
	)

	for _, series := range seriesList.Values {
		if series.SafeMax() > maxAllowedValue {
			seriesName := strings.Replace(series.Name(), search, replace, -1)
			newNames = append(newNames, seriesName)
		}
	}

	for _, newNameChunk := range chunkArrayHelper(newNames, maxConcurrency) {
		if multiErr.LastError() != nil {
			return ts.NewSeriesList(), multiErr.LastError()
		}

		for _, newTarget := range newNameChunk {
			wg.Add(1)
			go func() {
				defer wg.Done()
				resultSeriesList, err := evaluateTarget(ctx, newTarget)

				if err != nil {
					mu.Lock()
					multiErr = multiErr.Add(err)
					mu.Unlock()
					return
				}

				mu.Lock()
				for _, resultSeries := range resultSeriesList.Values {
					resultSeries.Specification = newTarget
					output = append(output, resultSeries)
				}
				mu.Unlock()
			}()
		}
		wg.Wait()
	}

	r := ts.NewSeriesList()
	r.Values = output
	return r, nil
}

// sortByMinima sorts timeseries by the minimum value across the time period specified.
func sortByMinima(ctx *common.Context, series singlePathSpec) (ts.SeriesList, error) {
	return lowest(ctx, series, len(series.Values), "min")
}

type valueComparator func(v, threshold float64) bool

func compareByFunction(
	_ *common.Context,
	series singlePathSpec,
	sr ts.SeriesReducer,
	vc valueComparator,
	threshold float64,
) (ts.SeriesList, error) {
	res := make([]*ts.Series, 0, len(series.Values))
	for _, s := range series.Values {
		stats := sr(s)
		if vc(stats, threshold) {
			res = append(res, s)
		}
	}

	r := ts.SeriesList(series)
	r.Values = res
	return r, nil
}

func aboveByFunction(
	ctx *common.Context,
	series singlePathSpec,
	sr ts.SeriesReducer,
	threshold float64,
) (ts.SeriesList, error) {
	return compareByFunction(ctx, series, sr, func(stats, threshold float64) bool {
		return stats > threshold
	}, threshold)
}

func belowByFunction(
	ctx *common.Context,
	series singlePathSpec,
	sr ts.SeriesReducer,
	threshold float64,
) (ts.SeriesList, error) {
	return compareByFunction(ctx, series, sr, func(stats, threshold float64) bool {
		return stats < threshold
	}, threshold)
}

// maximumAbove takes one metric or a wildcard seriesList followed by an floating point number n,
// returns only the metrics with a maximum value above n.
func maximumAbove(ctx *common.Context, series singlePathSpec, n float64) (ts.SeriesList, error) {
	sr := ts.SeriesReducerMax.Reducer()
	return aboveByFunction(ctx, series, sr, n)
}

// minimumAbove takes one metric or a wildcard seriesList followed by an floating point number n,
// returns only the metrics with a minimum value above n.
func minimumAbove(ctx *common.Context, series singlePathSpec, n float64) (ts.SeriesList, error) {
	sr := ts.SeriesReducerMin.Reducer()
	return aboveByFunction(ctx, series, sr, n)
}

// averageAbove takes one metric or a wildcard seriesList followed by an floating point number n,
// returns only the metrics with an average value above n.
func averageAbove(ctx *common.Context, series singlePathSpec, n float64) (ts.SeriesList, error) {
	sr := ts.SeriesReducerAvg.Reducer()
	return aboveByFunction(ctx, series, sr, n)
}

// currentAbove takes one metric or a wildcard seriesList followed by an floating point number n,
// returns only the metrics with the last value above n.
func currentAbove(ctx *common.Context, series singlePathSpec, n float64) (ts.SeriesList, error) {
	sr := ts.SeriesReducerLast.Reducer()
	return aboveByFunction(ctx, series, sr, n)
}

// currentBelow takes one metric or a wildcard seriesList followed by an floating point number n,
// returns only the metrics with the last value below n.
func currentBelow(ctx *common.Context, series singlePathSpec, n float64) (ts.SeriesList, error) {
	sr := ts.SeriesReducerLast.Reducer()
	return belowByFunction(ctx, series, sr, n)
}

// constantLine takes value and creates a constant line at value.
func constantLine(ctx *common.Context, value float64) (ts.SeriesList, error) {
	newSeries, err := common.ConstantLine(ctx, value)
	if err != nil {
		return ts.NewSeriesList(), err
	}
	return ts.NewSeriesListWithSeries(newSeries), nil
}

// identity returns datapoints where the value equals the timestamp of the datapoint.
func identity(ctx *common.Context, name string) (ts.SeriesList, error) {
	return common.Identity(ctx, name)
}

// limit takes one metric or a wildcard seriesList followed by an integer N, and draws
// the first N metrics.
func limit(_ *common.Context, series singlePathSpec, n int) (ts.SeriesList, error) {
	if n < 0 {
		return ts.NewSeriesList(), errors.NewInvalidParamsError(fmt.Errorf("invalid limit parameter n: %d", n))
	}
	upperBound := int(math.Min(float64(len(series.Values)), float64(n)))

	r := ts.SeriesList(series)
	r.Values = series.Values[0:upperBound]
	return r, nil
}

// grep takes a metric or a wildcard seriesList, followed by a regular
// expression in double quotes. Excludes metrics that don’t match the regular expression.
func grep(_ *common.Context, seriesList singlePathSpec, regex string) (ts.SeriesList, error) {
	re, err := regexp.Compile(regex)
	if err != nil {
		return ts.NewSeriesList(), err
	}

	filtered := seriesList.Values[:0]
	for _, series := range seriesList.Values {
		if re.MatchString(series.Name()) {
			filtered = append(filtered, series)
		}
	}

	r := ts.NewSeriesList()
	r.Values = filtered
	return r, nil
}

// timeShift draws the selected metrics shifted in time. If no sign is given, a minus sign ( - ) is
// implied which will shift the metric back in time. If a plus sign ( + ) is given, the metric will
// be shifted forward in time
func timeShift(
	_ *common.Context,
	_ singlePathSpec,
	timeShiftS string,
	_ bool,
	_ bool,
) (*unaryContextShifter, error) {

	// TODO: implement resetEnd
	if !(strings.HasPrefix(timeShiftS, "+") || strings.HasPrefix(timeShiftS, "-")) {
		timeShiftS = "-" + timeShiftS
	}

	shift, err := common.ParseInterval(timeShiftS)
	if err != nil {
		return nil, errors.NewInvalidParamsError(fmt.Errorf("invalid timeShift parameter %s: %v", timeShiftS, err))
	}

	contextShiftingFn := func(c *common.Context) *common.Context {
		opts := common.NewChildContextOptions()
		opts.AdjustTimeRange(shift, shift, 0, 0)
		childCtx := c.NewChildContext(opts)
		return childCtx
	}

	transformerFn := func(input ts.SeriesList) (ts.SeriesList, error) {
		output := make([]*ts.Series, input.Len())
		for i, in := range input.Values {
			// NB(jayp): opposite direction
			output[i] = in.Shift(-1 * shift).RenamedTo(fmt.Sprintf("timeShift(%s, %s)", in.Name(), timeShiftS))
		}
		input.Values = output
		return input, nil
	}

	return &unaryContextShifter{
		ContextShiftFunc: contextShiftingFn,
		UnaryTransformer: transformerFn,
	}, nil
}

// delay shifts all samples later by an integer number of steps. This can be used
// for custom derivative calculations, among other things. Note: this will pad
// the early end of the data with NaN for every step shifted. delay complements
// other time-displacement functions such as timeShift and timeSlice, in that
// delay is indifferent about the step intervals being shifted.
func delay(
	ctx *common.Context,
	singlePath singlePathSpec,
	steps int,
) (ts.SeriesList, error) {
	input := ts.SeriesList(singlePath)
	output := make([]*ts.Series, 0, input.Len())

	for _, series := range input.Values {
		delayedVals := delayValuesHelper(ctx, series, steps)
		delayedSeries := ts.NewSeries(ctx, series.Name(), series.StartTime(), delayedVals)
		renamedSeries := delayedSeries.RenamedTo(fmt.Sprintf("delay(%s,%d)", delayedSeries.Name(), steps))
		output = append(output, renamedSeries)
	}
	input.Values = output
	return input, nil
}

// delayValuesHelper takes a series and returns a copy of the values after
// delaying the values by `steps` number of steps
func delayValuesHelper(ctx *common.Context, series *ts.Series, steps int) ts.Values {
	output := ts.NewValues(ctx, series.MillisPerStep(), series.Len())
	for i := steps; i < series.Len(); i++ {
		output.SetValueAt(i, series.ValueAt(i-steps))
	}
	return output
}

// timeSlice takes one metric or a wildcard metric, followed by a quoted string with the time to start the line and
// another quoted string with the time to end the line. The start and end times are inclusive.
// Useful for filtering out a part of a series of data from a wider range of data.
func timeSlice(ctx *common.Context, inputPath singlePathSpec, start string, end string) (ts.SeriesList, error) {
	var (
		now                     = time.Now()
		tzOffsetForAbsoluteTime time.Duration
	)
	startTime, err := graphite.ParseTime(start, now, tzOffsetForAbsoluteTime)
	if err != nil {
		return ts.NewSeriesList(), err
	}
	endTime, err := graphite.ParseTime(end, now, tzOffsetForAbsoluteTime)
	if err != nil {
		return ts.NewSeriesList(), err
	}

	input := ts.SeriesList(inputPath)
	output := make([]*ts.Series, 0, input.Len())

	for _, series := range input.Values {
		stepDuration := time.Duration(series.MillisPerStep()) * time.Millisecond
		truncatedValues := ts.NewValues(ctx, series.MillisPerStep(), series.Len())

		currentTime := series.StartTime()
		for i := 0; i < series.Len(); i++ {
			equalOrAfterStart := currentTime.Equal(startTime) || currentTime.After(startTime)
			beforeOrEqualEnd := currentTime.Before(endTime) || currentTime.Equal(endTime)
			if equalOrAfterStart && beforeOrEqualEnd {
				truncatedValues.SetValueAt(i, series.ValueAtTime(currentTime))
			}
			currentTime = currentTime.Add(stepDuration)
		}

		slicedSeries := ts.NewSeries(ctx, series.Name(), series.StartTime(), truncatedValues)
		renamedSlicedSeries := slicedSeries.RenamedTo(fmt.Sprintf("timeSlice(%s, %s, %s)", slicedSeries.Name(), start, end))
		output = append(output, renamedSlicedSeries)
	}
	input.Values = output
	return input, nil
}

// absolute returns the absolute value of each element in the series.
func absolute(ctx *common.Context, input singlePathSpec) (ts.SeriesList, error) {
	return transform(ctx, input,
		func(fname string) string { return fmt.Sprintf(wrappingFmt, "absolute", fname) },
		math.Abs)
}

// scale multiplies each element of a collection of time series by a given value
func scale(ctx *common.Context, input singlePathSpec, scale float64) (ts.SeriesList, error) {
	return transform(
		ctx,
		input,
		func(fname string) string {
			newName := fmt.Sprintf("%s,"+common.FloatingPointFormat, fname, scale)
			return fmt.Sprintf(wrappingFmt, "scale", newName)
		},
		common.Scale(scale),
	)
}

// scaleToSeconds makes a wildcard seriesList and returns "value per seconds"
func scaleToSeconds(
	ctx *common.Context,
	seriesList singlePathSpec,
	seconds int,
) (ts.SeriesList, error) {
	output := make([]*ts.Series, len(seriesList.Values))
	for i, series := range seriesList.Values {
		var (
			outvals = ts.NewValues(ctx, series.MillisPerStep(), series.Len())
			name    = fmt.Sprintf("scaleToSeconds(%s,%d)", series.Name(), seconds)
			factor  = float64(seconds*1000) / float64(series.MillisPerStep()) // convert seconds to millis
		)
		for step := 0; step < series.Len(); step++ {
			value := series.ValueAt(step)
			outvals.SetValueAt(step, value*factor)
		}
		output[i] = ts.NewSeries(ctx, name, series.StartTime(), outvals)
	}

	r := ts.SeriesList(seriesList)
	r.Values = output
	return r, nil
}

// offset adds a value to each element of a collection of time series
func offset(ctx *common.Context, input singlePathSpec, factor float64) (ts.SeriesList, error) {
	return transform(
		ctx,
		input,
		func(fname string) string {
			newName := fmt.Sprintf("%s,"+common.FloatingPointFormat, fname, factor)
			return fmt.Sprintf(wrappingFmt, "offset", newName)
		},
		common.Offset(factor),
	)
}

// transform converts values in a timeseries according to the valueTransformer.
func transform(ctx *common.Context, input singlePathSpec,
	fname func(inputName string) string, fn common.TransformFunc) (ts.SeriesList, error) {

	t := common.NewStatelessTransformer(fn)
	return common.Transform(ctx, ts.SeriesList(input), t, func(in *ts.Series) string {
		return fname(in.Name())
	})
}

// perSecond computes a derivative adjusted for the series time interval,
// useful for taking a running total metric and showing how many occurrences
// per second were handled
func perSecond(ctx *common.Context, input singlePathSpec, _ float64) (ts.SeriesList, error) {
	// TODO:  we are ignoring maxValue; we may need to implement it
	return common.PerSecond(ctx, ts.SeriesList(input), func(series *ts.Series) string {
		return fmt.Sprintf("perSecond(%s)", series.Name())
	})
}

// transformNull transforms all nulls (NaNs) in a time series to a defaultValue.
func transformNull(ctx *common.Context, input singlePathSpec, defaultValue float64) (ts.SeriesList, error) {
	return transform(
		ctx,
		input,
		func(fname string) string {
			newName := fmt.Sprintf("%s,"+common.FloatingPointFormat, fname, defaultValue)
			return fmt.Sprintf(wrappingFmt, "transformNull", newName)
		},
		common.TransformNull(defaultValue),
	)
}

// isNonNull replaces datapoints that are non-null with 1, and null values with 0.
// This is useful for understanding which metrics have data at a given point in time
// (ie, to count which servers are alive).
func isNonNull(ctx *common.Context, input singlePathSpec) (ts.SeriesList, error) {
	return transform(ctx,
		input,
		func(fname string) string { return fmt.Sprintf(wrappingFmt, "isNonNull", fname) },
		common.IsNonNull())
}

// keepLastValue takes one metric or a wildcard seriesList, and optionally a limit to the number of
// NaN values to skip over. If not specified, limit has a default value of -1, meaning all NaNs will
// be replaced by the closest preceding value that's not an NaN.
func keepLastValue(ctx *common.Context, input singlePathSpec, limit int) (ts.SeriesList, error) {
	output := make([]*ts.Series, 0, len(input.Values))
	for _, series := range input.Values {
		consecutiveNaNs := 0
		numSteps := series.Len()
		vals := ts.NewValues(ctx, series.MillisPerStep(), numSteps)
		for i := 0; i < numSteps; i++ {
			value := series.ValueAt(i)
			vals.SetValueAt(i, value)
			if i == 0 {
				continue
			}
			if math.IsNaN(value) {
				consecutiveNaNs++
			} else {
				if limit == -1 || (consecutiveNaNs > 0 && consecutiveNaNs <= limit) {
					v := series.ValueAt(i - consecutiveNaNs - 1)
					if !math.IsNaN(v) {
						for index := i - consecutiveNaNs; index < i; index++ {
							vals.SetValueAt(index, v)
						}
					}
				}
				consecutiveNaNs = 0
			}
		}
		if limit == -1 || (consecutiveNaNs > 0 && consecutiveNaNs <= limit) {
			for index := numSteps - consecutiveNaNs; index < numSteps; index++ {
				vals.SetValueAt(index, series.ValueAt(numSteps-consecutiveNaNs-1))
			}
		}
		name := fmt.Sprintf("keepLastValue(%s)", series.Name())
		newSeries := ts.NewSeries(ctx, name, series.StartTime(), vals)
		output = append(output, newSeries)
	}

	r := ts.SeriesList(input)
	r.Values = output
	return r, nil
}

type comparator func(float64, float64) bool

// lessOrEqualFunc checks whether x is less than or equal to y
func lessOrEqualFunc(x float64, y float64) bool {
	return x <= y
}

// greaterOrEqualFunc checks whether x is greater or equal to y
func greaterOrEqualFunc(x float64, y float64) bool {
	return x >= y
}

func sustainedCompare(ctx *common.Context, input singlePathSpec, threshold float64, intervalString string,
	comparisonFunction comparator, zeroValue float64, funcName string) (ts.SeriesList, error) {
	output := make([]*ts.Series, 0, len(input.Values))
	interval, err := common.ParseInterval(intervalString)
	if err != nil {
		return ts.NewSeriesList(), err
	}

	intervalMillis := int(interval / time.Millisecond)
	for _, series := range input.Values {
		numSteps := series.Len()
		vals := ts.NewValues(ctx, series.MillisPerStep(), numSteps)

		minSteps := intervalMillis / series.MillisPerStep()
		currSteps := 0

		for i := 0; i < numSteps; i++ {
			value := series.ValueAt(i)
			if comparisonFunction(value, threshold) {
				currSteps++
			} else {
				currSteps = 0
			}
			if currSteps >= minSteps {
				vals.SetValueAt(i, value)
			} else {
				vals.SetValueAt(i, zeroValue)
			}
		}

		name := fmt.Sprintf("%s(%s, %f, '%s')",
			funcName, series.Name(), threshold, intervalString)
		newSeries := ts.NewSeries(ctx, name, series.StartTime(), vals)
		output = append(output, newSeries)
	}

	r := ts.SeriesList(input)
	r.Values = output
	return r, nil
}

func sustainedAbove(ctx *common.Context, input singlePathSpec, threshold float64, intervalString string) (ts.SeriesList, error) {
	return sustainedCompare(ctx, input, threshold, intervalString, greaterOrEqualFunc, threshold-math.Abs(threshold), "sustainedAbove")
}

func sustainedBelow(ctx *common.Context, input singlePathSpec, threshold float64, intervalString string) (ts.SeriesList, error) {
	return sustainedCompare(ctx, input, threshold, intervalString, lessOrEqualFunc, threshold+math.Abs(threshold), "sustainedBelow")
}

// removeBelowValue removes data below the given threshold from the series or list of series provided.
// Values below this threshold are assigned a value of None.
func removeBelowValue(ctx *common.Context, input singlePathSpec, n float64) (ts.SeriesList, error) {
	return transform(ctx, input,
		func(inputName string) string {
			return fmt.Sprintf("removeBelowValue(%s, "+common.FloatingPointFormat+")", inputName, n)
		},
		common.Filter(func(v float64) bool { return v >= n }))
}

// removeAboveValue removes data above the given threshold from the series or list of series provided.
// Values above this threshold are assigned a value of None.
func removeAboveValue(ctx *common.Context, input singlePathSpec, n float64) (ts.SeriesList, error) {
	return transform(ctx, input,
		func(inputName string) string {
			return fmt.Sprintf("removeAboveValue(%s, "+common.FloatingPointFormat+")", inputName, n)
		},
		common.Filter(func(v float64) bool { return v <= n }))
}

// removeEmptySeries returns only the time-series with non-empty data
func removeEmptySeries(ctx *common.Context, input singlePathSpec) (ts.SeriesList, error) {
	return common.RemoveEmpty(ctx, ts.SeriesList(input))
}

func takeByFunction(input singlePathSpec, n int, sr ts.SeriesReducer, sort ts.Direction) (ts.SeriesList, error) {
	series, err := ts.SortSeries(input.Values, sr, sort)
	if err != nil {
		return ts.NewSeriesList(), err
	}
	r := ts.SeriesList{
		Values:      series,
		SortApplied: true,
		Metadata:    input.Metadata,
	}
	return common.Head(r, n)
}

func getReducer(f string) (ts.SeriesReducer, error) {
	sa := ts.SeriesReducerApproach(f)
	r, ok := sa.SafeReducer()
	if !ok {
		return r, errors.NewInvalidParamsError(fmt.Errorf("invalid function %s", f))
	}
	return r, nil
}

// highest takes one metric or a wildcard seriesList followed by an integer N and an aggregation function.
// Out of all metrics passed, draws only the N metrics with the highest
// aggregated value over the time period specified.
func highest(_ *common.Context, input singlePathSpec, n int, f string) (ts.SeriesList, error) {
	reducer, err := getReducer(f)
	if err != nil {
		return ts.NewSeriesList(), err
	}
	return takeByFunction(input, n, reducer, ts.Descending)
}

// highestSum takes one metric or a wildcard seriesList followed by an integer
// n.  Out of all metrics passed, draws only the N metrics with the highest
// total value in the time period specified.
func highestSum(ctx *common.Context, input singlePathSpec, n int) (ts.SeriesList, error) {
	return highest(ctx, input, n, "sum")
}

// highestMax takes one metric or a wildcard seriesList followed by an integer
// n.  Out of all metrics passed, draws only the N metrics with the highest
// maximum value in the time period specified.
func highestMax(ctx *common.Context, input singlePathSpec, n int) (ts.SeriesList, error) {
	return highest(ctx, input, n, "max")
}

// highestCurrent takes one metric or a wildcard seriesList followed by an
// integer n.  Out of all metrics passed, draws only the N metrics with the
// highest value at the end of the time period specified.
func highestCurrent(ctx *common.Context, input singlePathSpec, n int) (ts.SeriesList, error) {
	return highest(ctx, input, n, "current")
}

// highestAverage takes one metric or a wildcard seriesList followed by an
// integer n.  Out of all metrics passed, draws only the top N metrics with the
// highest average value for the time period specified.
func highestAverage(ctx *common.Context, input singlePathSpec, n int) (ts.SeriesList, error) {
	return highest(ctx, input, n, "average")
}

// fallbackSeries takes one metric or a wildcard seriesList, and a second fallback metric.
// If the wildcard does not match any series, draws the fallback metric.
func fallbackSeries(_ *common.Context, input singlePathSpec, fallback singlePathSpec) (ts.SeriesList, error) {
	if len(input.Values) > 0 {
		return ts.SeriesList(input), nil
	}

	return ts.SeriesList(fallback), nil
}

// mostDeviant takes one metric or a wildcard seriesList followed by an integer
// N. Draws the N most deviant metrics.  To find the deviants, the standard
// deviation (sigma) of each series is taken and ranked.  The top N standard
// deviations are returned.
func mostDeviant(ctx *common.Context, input singlePathSpec, n int) (ts.SeriesList, error) {
	return highest(ctx, input, n, "stddev")
}

// lowest takes one metric or a wildcard seriesList followed by an integer N and an aggregation function.
// Out of all metrics passed, draws only the N metrics with the lowest
// aggregated value over the time period specified.
func lowest(_ *common.Context, input singlePathSpec, n int, f string) (ts.SeriesList, error) {
	reducer, err := getReducer(f)
	if err != nil {
		return ts.NewSeriesList(), err
	}
	return takeByFunction(input, n, reducer, ts.Ascending)
}

// lowestAverage takes one metric or a wildcard seriesList followed by an
// integer n.  Out of all metrics passed, draws only the top N metrics with the
// lowest average value for the time period specified.
func lowestAverage(ctx *common.Context, input singlePathSpec, n int) (ts.SeriesList, error) {
	return lowest(ctx, input, n, "average")
}

// lowestCurrent takes one metric or a wildcard seriesList followed by an
// integer n.  Out of all metrics passed, draws only the N metrics with the
// lowest value at the end of the time period specified.
func lowestCurrent(ctx *common.Context, input singlePathSpec, n int) (ts.SeriesList, error) {
	return lowest(ctx, input, n, "current")
}

// effectiveXFF return true if windowPoints has a % of non-nulls above the xFilesFactor, false if not
func effectiveXFF(windowPoints, nans int, xFilesFactor float64) bool {
	return float64(windowPoints-nans)/float64(windowPoints) >= xFilesFactor
}

// windowSizeFunc calculates window size for moving average calculation
type windowSizeFunc func(stepSize int) int

type windowSizeParsed struct {
	deltaValue     time.Duration
	stringValue    string
	windowSizeFunc windowSizeFunc
}

func parseWindowSize(windowSizeValue genericInterface, input singlePathSpec) (windowSizeParsed, error) {
	windowSize := windowSizeParsed{}

	switch windowSizeValue := windowSizeValue.(type) {
	case string:
		interval, err := common.ParseInterval(windowSizeValue)
		if err != nil {
			return windowSize, err
		}
		if interval <= 0 {
			err := errors.NewInvalidParamsError(fmt.Errorf(
				"windowSize must be positive but instead is %v",
				interval))
			return windowSize, err
		}
		windowSize.windowSizeFunc = func(stepSize int) int {
			return int(int64(windowSize.deltaValue/time.Millisecond) / int64(stepSize))
		}
		windowSize.stringValue = fmt.Sprintf("%q", windowSizeValue)
		windowSize.deltaValue = interval
	case float64:
		windowSizeInt := int(windowSizeValue)
		if windowSizeInt <= 0 {
			err := errors.NewInvalidParamsError(fmt.Errorf(
				"windowSize must be positive but instead is %d",
				windowSizeInt))
			return windowSize, err
		}
		windowSize.windowSizeFunc = func(_ int) int { return windowSizeInt }
		windowSize.stringValue = fmt.Sprintf("%d", windowSizeInt)
		maxStepSize := input.Values[0].MillisPerStep()
		for i := 1; i < len(input.Values); i++ {
			maxStepSize = int(math.Max(float64(maxStepSize), float64(input.Values[i].MillisPerStep())))
		}
		windowSize.deltaValue = time.Duration(maxStepSize*windowSizeInt) * time.Millisecond
	default:
		err := errors.NewInvalidParamsError(fmt.Errorf(
			"windowSize must be either a string or an int but instead is a %T",
			windowSizeValue))
		return windowSize, err
	}
	return windowSize, nil
}

// movingAverage calculates the moving average of a metric (or metrics) over a time interval.
func movingAverage(ctx *common.Context, input singlePathSpec, windowSizeValue genericInterface, xFilesFactor float64) (*binaryContextShifter, error) {
	if len(input.Values) == 0 {
		return nil, nil
	}

	widowSize, err := parseWindowSize(windowSizeValue, input)

	if err != nil {
		return nil, err
	}

	contextShiftingFn := func(c *common.Context) *common.Context {
		opts := common.NewChildContextOptions()
		opts.AdjustTimeRange(0, 0, widowSize.deltaValue, 0)
		childCtx := c.NewChildContext(opts)
		return childCtx
	}

	bootstrapStartTime, bootstrapEndTime := ctx.StartTime.Add(-widowSize.deltaValue), ctx.StartTime
	transformerFn := func(bootstrapped, original ts.SeriesList) (ts.SeriesList, error) {
		bootstrapList, err := combineBootstrapWithOriginal(ctx,
			bootstrapStartTime, bootstrapEndTime,
			bootstrapped, singlePathSpec(original))
		if err != nil {
			return ts.NewSeriesList(), err
		}

		results := make([]*ts.Series, 0, original.Len())
		for i, bootstrap := range bootstrapList.Values {
			series := original.Values[i]
			stepSize := series.MillisPerStep()
			windowPoints := widowSize.windowSizeFunc(stepSize)
			if windowPoints == 0 {
				err := errors.NewInvalidParamsError(fmt.Errorf(
					"windowSize should not be smaller than stepSize, windowSize=%v, stepSize=%d",
					windowSizeValue, stepSize))
				return ts.NewSeriesList(), err
			}

			numSteps := series.Len()
			offset := bootstrap.Len() - numSteps
			vals := ts.NewValues(ctx, series.MillisPerStep(), numSteps)
			sum := 0.0
			num := 0
			nans := 0
			firstPoint := false
			for i := 0; i < numSteps; i++ {
				// NB: skip if the number of points received is less than the number
				// of points in the lookback window.
				if !firstPoint {
					firstPoint = true
					for j := offset - windowPoints; j < offset; j++ {
						if j < 0 {
							continue
						}

						v := bootstrap.ValueAt(j)
						if !math.IsNaN(v) {
							sum += v
							num++
						} else {
							nans++
						}
					}
				} else {
					if i+offset-windowPoints > 0 {
						prev := bootstrap.ValueAt(i + offset - windowPoints - 1)
						if !math.IsNaN(prev) {
							sum -= prev
							num--
						} else {
							nans--
						}
					}
					next := bootstrap.ValueAt(i + offset - 1)
					if !math.IsNaN(next) {
						sum += next
						num++
					} else {
						nans++
					}
				}

				if nans < windowPoints && effectiveXFF(windowPoints, nans, xFilesFactor) {
					vals.SetValueAt(i, sum/float64(num))
				}
			}
			name := fmt.Sprintf("movingAverage(%s,%s)", series.Name(), widowSize.stringValue)
			newSeries := ts.NewSeries(ctx, name, series.StartTime(), vals)
			results = append(results, newSeries)
		}

		original.Values = results
		return original, nil
	}

	return &binaryContextShifter{
		ContextShiftFunc:  contextShiftingFn,
		BinaryTransformer: transformerFn,
	}, nil
}

// exponentialMovingAverage takes a series of values and a window size and produces
// an exponential moving average utilizing the following formula:
// 		ema(current) = constant * (Current Value) + (1 - constant) * ema(previous)
// The `constant` is calculated as:
// 		constant = 2 / (windowSize + 1)
// the first period EMA uses a simple moving average for its value.
func exponentialMovingAverage(ctx *common.Context, input singlePathSpec, windowSizeValue genericInterface) (*binaryContextShifter, error) {
	if len(input.Values) == 0 {
		return nil, nil
	}

	windowSize, err := parseWindowSize(windowSizeValue, input)
	if err != nil {
		return nil, err
	}

	contextShiftingFn := func(c *common.Context) *common.Context {
		opts := common.NewChildContextOptions()
		opts.AdjustTimeRange(0, 0, windowSize.deltaValue, 0)
		childCtx := c.NewChildContext(opts)
		return childCtx
	}

	bootstrapStartTime, bootstrapEndTime := ctx.StartTime.Add(-windowSize.deltaValue), ctx.StartTime
	transformerFn := func(bootstrapped, original ts.SeriesList) (ts.SeriesList, error) {
		bootstrapList, err := combineBootstrapWithOriginal(ctx,
			bootstrapStartTime, bootstrapEndTime,
			bootstrapped, singlePathSpec(original))
		if err != nil {
			return ts.NewSeriesList(), err
		}

		results := make([]*ts.Series, 0, original.Len())
		for i, bootstrap := range bootstrapList.Values {
			series := original.Values[i]
			stepSize := series.MillisPerStep()
			windowPoints := windowSize.windowSizeFunc(stepSize)
			if windowPoints == 0 {
				err := errors.NewInvalidParamsError(fmt.Errorf(
					"windowSize should not be smaller than stepSize, windowSize=%v, stepSize=%d",
					windowSizeValue, stepSize))
				return ts.NewSeriesList(), err
			}
			emaConstant := 2.0 / (float64(windowPoints) + 1.0)

			numSteps := series.Len()
			offset := bootstrap.Len() - numSteps
			vals := ts.NewValues(ctx, series.MillisPerStep(), numSteps)
			firstWindow, err := bootstrap.Slice(0, offset)
			if err != nil {
				return ts.NewSeriesList(), err
			}

			// the first value is just a regular moving average
			ema := firstWindow.SafeAvg()
			if math.IsNaN(ema) {
				ema = 0
			}
			vals.SetValueAt(0, ema)
			for i := 1; i < numSteps; i++ {
				curr := bootstrap.ValueAt(i + offset)
				if !math.IsNaN(curr) {
					// formula: ema(current) = constant * (Current Value) + (1 - constant) * ema(previous)
					ema = emaConstant*curr + (1-emaConstant)*ema
					vals.SetValueAt(i, ema)
				} else {
					vals.SetValueAt(i, math.NaN())
				}

			}

			name := fmt.Sprintf("exponentialMovingAverage(%s,%s)", series.Name(), windowSize.stringValue)
			newSeries := ts.NewSeries(ctx, name, series.StartTime(), vals)
			results = append(results, newSeries)
		}

		original.Values = results
		return original, nil
	}

	return &binaryContextShifter{
		ContextShiftFunc:  contextShiftingFn,
		BinaryTransformer: transformerFn,
	}, nil
}

// totalFunc takes an index and returns a total value for that index
type totalFunc func(int, *ts.Series) float64

func totalBySum(seriesList []*ts.Series, index int) float64 {
	s, hasValue := 0.0, false
	for _, series := range seriesList {
		v := series.ValueAt(index)
		if !math.IsNaN(v) {
			hasValue = true
			s += v
		}
	}
	if hasValue {
		return s
	}
	return math.NaN()
}

// asPercent calculates a percentage of the total of a wildcard series.
func asPercent(ctx *common.Context, input singlePathSpec, total genericInterface, nodes ...int) (ts.SeriesList, error) {
	if len(input.Values) == 0 {
		return ts.SeriesList(input), nil
	}

	var toNormalize, normalized []*ts.Series
	var tf totalFunc
	var totalText string

	switch totalArg := total.(type) {
	case ts.SeriesList, singlePathSpec:
		var total ts.SeriesList
		switch v := totalArg.(type) {
		case ts.SeriesList:
			total = v
		case singlePathSpec:
			total = ts.SeriesList(v)
		}
		if total.Len() == 0 {
			// normalize input and sum up input as the total series
			toNormalize = input.Values
			tf = func(idx int, _ *ts.Series) float64 { return totalBySum(normalized, idx) }
		} else {
			if len(nodes) > 0 {
				// group the series by specified nodes and then sum those groups
				groupedTotal, err := groupByNodes(ctx, input, "sum", nodes...)
				if err != nil {
					return ts.NewSeriesList(), err
				}
				toNormalize = append(input.Values, groupedTotal.Values[0])
				metaSeriesSumByKey := make(map[string]*ts.Series)

				// map the aggregation key to the aggregated series
				for _, series := range groupedTotal.Values {
					metaSeriesSumByKey[series.Name()] = series
				}

				tf = func(idx int, series *ts.Series) float64 {
					// find which aggregation key this series falls under
					// and return the sum for that aggregated group
					key := getAggregationKey(series, nodes)
					return metaSeriesSumByKey[key].ValueAt(idx)
				}
				totalText = groupedTotal.Values[0].Name()
			} else {
				toNormalize = append(input.Values, total.Values[0])
				tf = func(idx int, _ *ts.Series) float64 { return normalized[len(normalized)-1].ValueAt(idx) }
				totalText = total.Values[0].Name()
			}
		}
	case float64:
		toNormalize = input.Values
		tf = func(idx int, _ *ts.Series) float64 { return totalArg }
		totalText = fmt.Sprintf(common.FloatingPointFormat, totalArg)
	case nil:
		// if total is nil, the total is the sum of all the input series
		toNormalize = input.Values
		var err error
		summedSeries, err := sumSeries(ctx, multiplePathSpecs(input))
		if err != nil {
			return ts.NewSeriesList(), err
		}
		tf = func(idx int, _ *ts.Series) float64 { return summedSeries.Values[0].ValueAt(idx) }
		totalText = summedSeries.Values[0].Name()
	default:
		err := errors.NewInvalidParamsError(errors.New("total must be either an int, a series, or nil"))
		return ts.NewSeriesList(), err
	}

	result, _, _, _, err := common.Normalize(ctx, ts.SeriesList{
		Values:   toNormalize,
		Metadata: input.Metadata,
	})
	if err != nil {
		return ts.NewSeriesList(), err
	}

	normalized = result.Values
	numInputSeries := len(input.Values)
	values := make([]ts.MutableValues, 0, numInputSeries)
	for i := 0; i < numInputSeries; i++ {
		percents := ts.NewValues(ctx, normalized[i].MillisPerStep(), normalized[i].Len())
		values = append(values, percents)
	}
	for i := 0; i < normalized[0].Len(); i++ {
		for j := 0; j < numInputSeries; j++ {
			t := tf(i, normalized[j])
			v := normalized[j].ValueAt(i)
			if !math.IsNaN(v) && !math.IsNaN(t) && t != 0 {
				values[j].SetValueAt(i, 100.0*v/t)
			}
		}
	}

	results := make([]*ts.Series, 0, numInputSeries)
	for i := 0; i < numInputSeries; i++ {
		var totalName string
		if len(totalText) == 0 {
			totalName = normalized[i].Specification
		} else {
			totalName = totalText
		}
		newName := fmt.Sprintf("asPercent(%s, %s)", normalized[i].Name(), totalName)
		newSeries := ts.NewSeries(ctx, newName, normalized[i].StartTime(), values[i])
		results = append(results, newSeries)
	}

	r := ts.SeriesList(input)
	r.Values = results
	return r, nil
}

// exclude takes a metric or a wildcard seriesList, followed by a regular
// expression in double quotes.  Excludes metrics that match the regular
// expression.
func exclude(_ *common.Context, input singlePathSpec, pattern string) (ts.SeriesList, error) {
	rePattern, err := regexp.Compile(pattern)
	//NB(rooz): we decided to just fail if regex compilation fails to
	//differentiate it from an all-excluding regex
	if err != nil {
		return ts.NewSeriesList(), err
	}

	output := make([]*ts.Series, 0, len(input.Values))
	for _, in := range input.Values {
		if m := rePattern.FindStringSubmatch(strings.TrimSpace(in.Name())); len(m) == 0 {
			output = append(output, in)
		}
	}

	r := ts.SeriesList(input)
	r.Values = output
	return r, nil
}

// logarithm takes one metric or a wildcard seriesList, and draws the y-axis in
// logarithmic format.
func logarithm(ctx *common.Context, input singlePathSpec, base int) (ts.SeriesList, error) {
	if base <= 0 {
		err := errors.NewInvalidParamsError(fmt.Errorf("invalid log base %d", base))
		return ts.NewSeriesList(), err
	}

	results := make([]*ts.Series, 0, len(input.Values))
	for _, series := range input.Values {
		vals := ts.NewValues(ctx, series.MillisPerStep(), series.Len())
		newName := fmt.Sprintf("log(%s, %d)", series.Name(), base)
		if series.AllNaN() {
			results = append(results, ts.NewSeries(ctx, newName, series.StartTime(), vals))
			continue
		}

		for i := 0; i < series.Len(); i++ {
			n := series.ValueAt(i)
			if !math.IsNaN(n) && n > 0 {
				vals.SetValueAt(i, math.Log10(n)/math.Log10(float64(base)))
			}
		}

		results = append(results, ts.NewSeries(ctx, newName, series.StartTime(), vals))
	}

	r := ts.SeriesList(input)
	r.Values = results
	return r, nil
}

// interpolate takes one metric or a wildcard seriesList, and optionally a limit to the number of ‘None’ values
// to skip over. Continues the line with the last received value when gaps (‘None’ values)
// appear in your data, rather than breaking your line.
//
// interpolate will not interpolate at the beginning or end of a series, only in the middle
func interpolate(ctx *common.Context, input singlePathSpec, limit int) (ts.SeriesList, error) {
	output := make([]*ts.Series, 0, len(input.Values))
	for _, series := range input.Values {
		var (
			consecutiveNaNs = 0
			numSteps        = series.Len()
			vals            = ts.NewValues(ctx, series.MillisPerStep(), numSteps)
			firstNonNan     = false
		)

		for i := 0; i < numSteps; i++ {
			value := series.ValueAt(i)
			vals.SetValueAt(i, value)

			if math.IsNaN(value) {
				if !firstNonNan {
					continue
				}

				consecutiveNaNs++
				if limit >= 0 && consecutiveNaNs > limit {
					consecutiveNaNs = 0
				}

				continue
			} else {
				firstNonNan = true
			}

			if consecutiveNaNs == 0 {
				// have a value but no need to interpolate
				continue
			}

			interpolated := series.ValueAt(i - consecutiveNaNs - 1)
			interpolateStep := (value - interpolated) / float64(consecutiveNaNs+1)
			for index := i - consecutiveNaNs; index < i; index++ {
				interpolated = interpolated + interpolateStep
				vals.SetValueAt(index, interpolated)
			}

			consecutiveNaNs = 0
		}

		name := fmt.Sprintf("interpolate(%s)", series.Name())
		newSeries := ts.NewSeries(ctx, name, series.StartTime(), vals)
		output = append(output, newSeries)
	}
	r := ts.SeriesList(input)
	r.Values = output
	return r, nil
}

// group takes an arbitrary number of pathspecs and adds them to a single timeseries array.
// This function is used to pass multiple pathspecs to a function which only takes one
func group(_ *common.Context, input multiplePathSpecs) (ts.SeriesList, error) {
	return ts.SeriesList(input), nil
}

func derivativeTemplate(ctx *common.Context, input singlePathSpec, nameTemplate string,
	fn func(float64, float64) float64) (ts.SeriesList, error) {

	output := make([]*ts.Series, len(input.Values))
	for i, in := range input.Values {
		derivativeValues := ts.NewValues(ctx, in.MillisPerStep(), in.Len())
		previousValue := math.NaN()

		for step := 0; step < in.Len(); step++ {
			value := in.ValueAt(step)
			if math.IsNaN(value) || math.IsNaN(previousValue) {
				derivativeValues.SetValueAt(step, math.NaN())
			} else {
				derivativeValues.SetValueAt(step, fn(value, previousValue))
			}

			previousValue = value
		}

		name := fmt.Sprintf("%s(%s)", nameTemplate, in.Name())
		output[i] = ts.NewSeries(ctx, name, in.StartTime(), derivativeValues)
	}

	r := ts.SeriesList(input)
	r.Values = output
	return r, nil
}

// integral shows the sum over time, sort of like a continuous addition function.
//  Useful for finding totals or trends in metrics that are collected per minute.
func integral(ctx *common.Context, input singlePathSpec) (ts.SeriesList, error) {
	results := make([]*ts.Series, 0, len(input.Values))
	for _, series := range input.Values {
		if series.AllNaN() {
			results = append(results, series)
			continue
		}

		outvals := ts.NewValues(ctx, series.MillisPerStep(), series.Len())
		var current float64
		for i := 0; i < series.Len(); i++ {
			n := series.ValueAt(i)
			if !math.IsNaN(n) {
				current += n
				outvals.SetValueAt(i, current)
			}
		}

		newName := fmt.Sprintf("integral(%s)", series.Name())
		results = append(results, ts.NewSeries(ctx, newName, series.StartTime(), outvals))
	}

	r := ts.SeriesList(input)
	r.Values = results
	return r, nil
}

// integralByInterval will do the same as integral funcion, except it resets the total to 0
// at the given time in the parameter “from”. Useful for finding totals per hour/day/week.
func integralByInterval(ctx *common.Context, input singlePathSpec, intervalString string) (ts.SeriesList, error) {
	intervalUnit, err := common.ParseInterval(intervalString)
	if err != nil {
		return ts.NewSeriesList(), err
	}
	results := make([]*ts.Series, 0, len(input.Values))

	for _, series := range input.Values {
		var (
			stepsPerInterval = intervalUnit.Milliseconds() / int64(series.MillisPerStep())
			outVals          = ts.NewValues(ctx, series.MillisPerStep(), series.Len())
			stepCounter      int64
			currentSum       float64
		)

		for i := 0; i < series.Len(); i++ {
			if stepCounter == stepsPerInterval {
				// startNewInterval
				stepCounter = 0
				currentSum = 0.0
			}
			n := series.ValueAt(i)
			if !math.IsNaN(n) {
				currentSum += n
			}
			outVals.SetValueAt(i, currentSum)
			stepCounter += 1
		}

		newName := fmt.Sprintf("integralByInterval(%s, %s)", series.Name(), intervalString)
		results = append(results, ts.NewSeries(ctx, newName, series.StartTime(), outVals))
	}

	r := ts.SeriesList(input)
	r.Values = results
	return r, nil
}

// This is the opposite of the integral function.  This is useful for taking a
// running total metric and calculating the delta between subsequent data
// points.
func derivative(ctx *common.Context, input singlePathSpec) (ts.SeriesList, error) {
	return derivativeTemplate(ctx, input, "derivative", func(value, previousValue float64) float64 {
		return value - previousValue
	})
}

// Same as the derivative function above, but ignores datapoints that trend down.
func nonNegativeDerivative(ctx *common.Context, input singlePathSpec, maxValue float64) (ts.SeriesList, error) {
	return derivativeTemplate(ctx, input, "nonNegativeDerivative", func(value, previousValue float64) float64 {
		difference := value - previousValue
		if difference >= 0 {
			return difference
		} else if !math.IsNaN(maxValue) && maxValue >= value {
			return (maxValue - previousValue) + value + 1.0
		} else {
			return math.NaN()
		}
	})
}

// nPercentile returns percentile-percent of each series in the seriesList.
func nPercentile(ctx *common.Context, seriesList singlePathSpec, percentile float64) (ts.SeriesList, error) {
	return common.NPercentile(ctx, ts.SeriesList(seriesList), percentile, func(name string, percentile float64) string {
		return fmt.Sprintf("nPercentile(%s, "+common.FloatingPointFormat+")", name, percentile)
	})
}

func percentileOfSeries(ctx *common.Context, seriesList singlePathSpec, percentile float64, interpolateValue genericInterface) (ts.SeriesList, error) {
	if percentile <= 0 || percentile > 100 {
		err := errors.NewInvalidParamsError(fmt.Errorf(
			"the requested percentile value must be betwween 0 and 100"))
		return ts.NewSeriesList(), err
	}

	var interpolate bool
	switch interpolateValue := interpolateValue.(type) {
	case bool:
		interpolate = interpolateValue
	case string:
		if interpolateValue == "true" {
			interpolate = true
		} else if interpolateValue != "false" {
			err := errors.NewInvalidParamsError(fmt.Errorf(
				"interpolateValue must be either true or false but instead is %s",
				interpolateValue))
			return ts.NewSeriesList(), err
		}
	default:
		err := errors.NewInvalidParamsError(fmt.Errorf(
			"interpolateValue must be either a boolean or a string but instead is %T",
			interpolateValue))
		return ts.NewSeriesList(), err
	}

	if len(seriesList.Values) == 0 {
		err := errors.NewInvalidParamsError(fmt.Errorf("series list cannot be empty"))
		return ts.NewSeriesList(), err
	}

	normalize, _, _, _, err := common.Normalize(ctx, ts.SeriesList(seriesList))
	if err != nil {
		return ts.NewSeriesList(), err
	}

	step := seriesList.Values[0].MillisPerStep()
	for _, series := range seriesList.Values[1:] {
		if step != series.MillisPerStep() {
			err := errors.NewInvalidParamsError(fmt.Errorf(
				"different step sizes in input series not supported"))
			return ts.NewSeriesList(), err
		}
	}

	// TODO: This is wrong when MillisPerStep is different across
	// the timeseries.
	min := seriesList.Values[0].Len()
	for _, series := range seriesList.Values[1:] {
		numSteps := series.Len()
		if numSteps < min {
			min = numSteps
		}
	}

	percentiles := make([]float64, min)
	for i := 0; i < min; i++ {
		row := make([]float64, len(seriesList.Values))
		for j, series := range seriesList.Values {
			row[j] = series.ValueAt(i)
		}

		percentiles[i] = common.GetPercentile(row, percentile, interpolate)
	}

	percentilesSeries := ts.NewValues(ctx, normalize.Values[0].MillisPerStep(), min)
	for k := 0; k < min; k++ {
		percentilesSeries.SetValueAt(k, percentiles[k])
	}

	name := fmt.Sprintf("percentileOfSeries(%s,"+common.FloatingPointFormat+")",
		seriesList.Values[0].Specification, percentile)
	return ts.SeriesList{
		Values: []*ts.Series{
			ts.NewSeries(ctx, name, normalize.Values[0].StartTime(), percentilesSeries),
		},
		Metadata: seriesList.Metadata,
	}, nil
}

// divMod takes dividend n and divisor m, returns quotient and remainder.
// prerequisite: m is nonzero.
func divMod(n, m int) (int, int) {
	quotient := n / m
	remainder := n - quotient*m
	return quotient, remainder
}

// addToBucket add value to buckets[idx] and handles NaNs properly.
func addToBucket(buckets ts.MutableValues, idx int, value float64) {
	v := buckets.ValueAt(idx)
	if math.IsNaN(v) {
		buckets.SetValueAt(idx, value)
	} else {
		buckets.SetValueAt(idx, v+value)
	}
}

// durationToSeconds converts a duration to number of seconds.
func durationToSeconds(d time.Duration) int {
	return int(d / time.Second)
}

// hitcount estimates hit counts from a list of time series. This function assumes the values in each time
// series represent hits per second. It calculates hits per some larger interval such as per day or per hour.
// NB(xichen): it doesn't support the alignToInterval parameter because no one seems to be using that.
func hitcount(ctx *common.Context, seriesList singlePathSpec, intervalString string) (ts.SeriesList, error) {
	interval, err := common.ParseInterval(intervalString)
	if err != nil {
		return ts.NewSeriesList(), err
	}

	intervalInSeconds := durationToSeconds(interval)
	if intervalInSeconds <= 0 {
		return ts.NewSeriesList(), common.ErrInvalidIntervalFormat
	}

	resultSeries := make([]*ts.Series, len(seriesList.Values))
	for index, series := range seriesList.Values {
		step := time.Duration(series.MillisPerStep()) * time.Millisecond
		bucketCount := int(math.Ceil(float64(series.EndTime().Sub(series.StartTime())) / float64(interval)))
		buckets := ts.NewValues(ctx, int(interval/time.Millisecond), bucketCount)
		newStart := series.EndTime().Add(-time.Duration(bucketCount) * interval)

		for i := 0; i < series.Len(); i++ {
			value := series.ValueAt(i)
			if math.IsNaN(value) {
				continue
			}
			startTime := series.StartTime().Add(time.Duration(i) * step)
			startBucket, startMod := divMod(durationToSeconds(startTime.Sub(newStart)), intervalInSeconds)
			endTime := startTime.Add(step)
			endBucket, endMod := divMod(durationToSeconds(endTime.Sub(newStart)), intervalInSeconds)

			if endBucket >= bucketCount {
				endBucket = bucketCount - 1
				endMod = intervalInSeconds
			}

			if startBucket == endBucket {
				addToBucket(buckets, startBucket, value*float64(endMod-startMod))
			} else {
				addToBucket(buckets, startBucket, value*float64(intervalInSeconds-startMod))
				hitsPerBucket := value * float64(intervalInSeconds)
				for j := startBucket + 1; j < endBucket; j++ {
					addToBucket(buckets, j, hitsPerBucket)
				}
				if endMod > 0 {
					addToBucket(buckets, endBucket, value*float64(endMod))
				}
			}
		}
		newName := fmt.Sprintf("hitcount(%s, %q)", series.Name(), intervalString)
		newSeries := ts.NewSeries(ctx, newName, newStart, buckets)
		resultSeries[index] = newSeries
	}

	r := ts.SeriesList(seriesList)
	r.Values = resultSeries
	return r, nil
}

func safeIndex(len, index int) int {
	return int(math.Min(float64(index), float64(len)))
}

// substr takes one metric or a wildcard seriesList followed by 1 or 2 integers. Prints n - length elements
// of the array (if only one integer n is passed) or n - m elements of the array (if two integers n and m
// are passed).
func substr(_ *common.Context, seriesList singlePathSpec, start, stop int) (ts.SeriesList, error) {
	results := make([]*ts.Series, len(seriesList.Values))
	re := regexp.MustCompile(",.*$")
	for i, series := range seriesList.Values {
		name := series.Name()
		left := strings.LastIndex(name, "(") + 1
		right := strings.Index(name, ")")
		length := len(name)
		if right < 0 {
			right = length
		}
		right = safeIndex(length, right)
		nameParts := strings.Split(name[left:right], ".")
		numParts := len(nameParts)
		// If stop == 0, it's as if stop was unspecified
		if start < 0 || start >= numParts || (stop != 0 && stop < start) {
			err := errors.NewInvalidParamsError(fmt.Errorf(
				"invalid substr params, start=%d, stop=%d", start, stop))
			return ts.NewSeriesList(), err
		}
		var newName string
		if stop == 0 {
			newName = strings.Join(nameParts[start:], ".")
		} else {
			stop = safeIndex(numParts, stop)
			newName = strings.Join(nameParts[start:stop], ".")
		}
		newName = re.ReplaceAllString(newName, "")
		results[i] = series.RenamedTo(newName)
	}

	r := ts.SeriesList(seriesList)
	r.Values = results
	return r, nil
}

// combineBootstrapWithOriginal combines the bootstrapped the series with the original series.
func combineBootstrapWithOriginal(
	ctx *common.Context,
	startTime time.Time,
	endTime time.Time,
	bootstrapped ts.SeriesList,
	seriesList singlePathSpec,
) (ts.SeriesList, error) {
	nameToSeries := make(map[string]*ts.Series)
	for _, series := range bootstrapped.Values {
		nameToSeries[series.Name()] = series
	}

	bootstrapList := make([]*ts.Series, 0, len(seriesList.Values))
	for _, series := range seriesList.Values {
		bs, found := nameToSeries[series.Name()]
		if !found {
			numSteps := ts.NumSteps(startTime, endTime, series.MillisPerStep())
			vals := ts.NewValues(ctx, series.MillisPerStep(), numSteps)
			bs = ts.NewSeries(ctx, series.Name(), startTime, vals)
		}
		bootstrapList = append(bootstrapList, bs)
	}

	var err error
	newSeriesList := make([]*ts.Series, len(seriesList.Values))
	for i, bootstrap := range bootstrapList {
		original := seriesList.Values[i]
		if bootstrap.MillisPerStep() < original.MillisPerStep() {
			bootstrap, err = bootstrap.IntersectAndResize(bootstrap.StartTime(), bootstrap.EndTime(), original.MillisPerStep(), original.ConsolidationFunc())
			if err != nil {
				return ts.NewSeriesList(), err
			}
		}
		bootstrapEndStep := endTime.Truncate(original.Resolution())
		if bootstrapEndStep.Before(endTime) {
			bootstrapEndStep = bootstrapEndStep.Add(original.Resolution())
		}
		// NB(braskin): using bootstrap.Len() is incorrect as it will include all
		// of the steps in the original timeseries, not just the steps up to the new end time
		bootstrapLength := bootstrap.StepAtTime(bootstrapEndStep)
		ratio := bootstrap.MillisPerStep() / original.MillisPerStep()
		numBootstrapValues := bootstrapLength * ratio
		numCombinedValues := numBootstrapValues + original.Len()
		values := ts.NewValues(ctx, original.MillisPerStep(), numCombinedValues)
		for j := 0; j < min(bootstrap.Len(), bootstrapLength); j++ {
			for k := j * ratio; k < (j+1)*ratio; k++ {
				values.SetValueAt(k, bootstrap.ValueAt(j))
			}
		}
		for j := numBootstrapValues; j < numCombinedValues; j++ {
			values.SetValueAt(j, original.ValueAt(j-numBootstrapValues))
		}
		newSeries := ts.NewSeries(ctx, original.Name(), startTime, values)
		newSeries.Specification = original.Specification
		newSeriesList[i] = newSeries
	}

	r := ts.SeriesList(seriesList)
	r.Values = newSeriesList
	return r, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// trimBootstrap trims the bootstrap period off the front of this series so it matches the original.
func trimBootstrap(ctx *common.Context, bootstrap, original *ts.Series) *ts.Series {
	originalLen := original.Len()
	bootstrapLen := bootstrap.Len()
	lengthLimit := (originalLen * original.MillisPerStep()) / bootstrap.MillisPerStep()
	trimStart := bootstrap.EndTime().Add(-time.Duration(lengthLimit*bootstrap.MillisPerStep()) * time.Millisecond)
	vals := ts.NewValues(ctx, bootstrap.MillisPerStep(), lengthLimit)
	for i := 0; i < lengthLimit; i++ {
		vals.SetValueAt(i, bootstrap.ValueAt(i+bootstrapLen-lengthLimit))
	}
	return ts.NewSeries(ctx, bootstrap.Name(), trimStart, vals)
}

// holtWintersForecast performs a Holt-Winters forecast using the series as input data.
// Data from one week previous to the series is used to bootstrap the initial forecast.
func holtWintersForecast(ctx *common.Context, seriesList singlePathSpec) (ts.SeriesList, error) {
	return holtWintersForecastInternal(ctx, seriesList, secondsPerWeek*time.Second)
}

// nolint: unparam
func holtWintersForecastInternal(ctx *common.Context, seriesList singlePathSpec, d time.Duration) (ts.SeriesList, error) {
	results := make([]*ts.Series, len(seriesList.Values))
	bootstrapList, err := common.FetchWithBootstrap(ctx, ts.SeriesList(seriesList), d)
	if err != nil {
		return ts.NewSeriesList(), err
	}
	for i, bootstrap := range bootstrapList.Values {
		series := seriesList.Values[i]
		analysis := holtWintersAnalysis(ctx, bootstrap)
		results[i] = trimBootstrap(ctx, analysis.predictions, series)
	}
	r := ts.SeriesList(seriesList)
	r.Values = results
	return r, nil
}

// holtWintersConfidenceBands performs a Holt-Winters forecast using the series as input data and
// plots upper and lower bands with the predicted forecast deviations.
func holtWintersConfidenceBands(ctx *common.Context, seriesList singlePathSpec, delta float64) (ts.SeriesList, error) {
	return holtWintersConfidenceBandsInternal(ctx, seriesList, delta, secondsPerWeek*time.Second)
}

// nolint: unparam
func holtWintersConfidenceBandsInternal(ctx *common.Context, seriesList singlePathSpec, delta float64, d time.Duration) (ts.SeriesList, error) {
	results := make([]*ts.Series, 2*len(seriesList.Values))
	bootstrapList, err := common.FetchWithBootstrap(ctx, ts.SeriesList(seriesList), d)
	if err != nil {
		return ts.NewSeriesList(), err
	}

	for index, bootstrap := range bootstrapList.Values {
		series := seriesList.Values[index]
		analysis := holtWintersAnalysis(ctx, bootstrap)
		forecast := trimBootstrap(ctx, analysis.predictions, series)
		deviation := trimBootstrap(ctx, analysis.deviations, series)
		seriesLength := forecast.Len()
		upperBand := ts.NewValues(ctx, forecast.MillisPerStep(), seriesLength)
		lowerBand := ts.NewValues(ctx, forecast.MillisPerStep(), seriesLength)
		for i := 0; i < seriesLength; i++ {
			forecastItem := forecast.ValueAt(i)
			deviationItem := deviation.ValueAt(i)
			if !math.IsNaN(forecastItem) && !math.IsNaN(deviationItem) {
				scaledDeviation := delta * deviationItem
				upperBand.SetValueAt(i, forecastItem+scaledDeviation)
				lowerBand.SetValueAt(i, forecastItem-scaledDeviation)
			}
		}
		upperName := fmt.Sprintf("holtWintersConfidenceUpper(%s)", series.Name())
		lowerName := fmt.Sprintf("holtWintersConfidenceLower(%s)", series.Name())
		upperSeries := ts.NewSeries(ctx, upperName, forecast.StartTime(), upperBand)
		lowerSeries := ts.NewSeries(ctx, lowerName, forecast.StartTime(), lowerBand)
		newIndex := index * 2
		results[newIndex] = lowerSeries
		results[newIndex+1] = upperSeries
	}

	r := ts.SeriesList(seriesList)
	r.Values = results
	return r, nil
}

// holtWintersAberration performs a Holt-Winters forecast using the series as input data and
// plots the positive or negative deviation of the series data from the forecast.
func holtWintersAberration(ctx *common.Context, seriesList singlePathSpec, delta float64) (ts.SeriesList, error) {
	// log if we are actually using this function
	var b bytes.Buffer
	b.WriteString("holtWintersAberration is used")
	if len(seriesList.Values) > 0 {
		b.WriteString(fmt.Sprintf(", series[0] name=%s", seriesList.Values[0].Name()))
	}
	return holtWintersAberrationInternal(ctx, seriesList, delta, secondsPerWeek*time.Second)
}

// nolint: unparam
func holtWintersAberrationInternal(
	ctx *common.Context,
	seriesList singlePathSpec,
	delta float64,
	d time.Duration,
) (ts.SeriesList, error) {
	results := make([]*ts.Series, len(seriesList.Values))
	for index, series := range seriesList.Values {
		confidenceBands, err := holtWintersConfidenceBandsInternal(ctx, singlePathSpec{
			Values: []*ts.Series{series},
		}, delta, d)
		if err != nil {
			return ts.NewSeriesList(), err
		}

		lowerBand := confidenceBands.Values[0]
		upperBand := confidenceBands.Values[1]
		numPoints := series.Len()
		aberration := ts.NewValues(ctx, series.MillisPerStep(), numPoints)
		for i := 0; i < numPoints; i++ {
			actual := series.ValueAt(i)
			upperVal := upperBand.ValueAt(i)
			lowerVal := lowerBand.ValueAt(i)
			var newValue float64
			if math.IsNaN(actual) {
				newValue = 0
			} else if !math.IsNaN(upperVal) && actual > upperVal {
				newValue = actual - upperVal
			} else if !math.IsNaN(lowerVal) && actual < lowerVal {
				newValue = actual - lowerVal
			} else {
				newValue = 0
			}
			aberration.SetValueAt(i, newValue)
		}
		newName := fmt.Sprintf("holtWintersAberration(%s)", series.Name())
		newSeries := ts.NewSeries(ctx, newName, series.StartTime(), aberration)
		results[index] = newSeries
	}

	r := ts.SeriesList(seriesList)
	r.Values = results
	return r, nil
}

func holtWintersIntercept(actual, lastSeason, lastIntercept, lastSlope float64) float64 {
	return alpha*(actual-lastSeason) + (1-alpha)*(lastIntercept+lastSlope)
}

func holtWintersSlope(intercept, lastIntercept, lastSlope float64) float64 {
	return beta*(intercept-lastIntercept) + (1-beta)*lastSlope
}

func holtWintersSeasonal(actual, intercept, lastSeason float64) float64 {
	return gamma*(actual-intercept) + (1-gamma)*lastSeason
}

func holtWintersDeviation(actual, prediction, lastSeasonalDev float64) float64 {
	if math.IsNaN(prediction) {
		prediction = 0
	}
	return gamma*math.Abs(actual-prediction) + (1-gamma)*lastSeasonalDev
}

type holtWintersAnalysisResult struct {
	predictions *ts.Series
	deviations  *ts.Series
	intercepts  []float64
	slopes      []float64
	seasonals   []float64
}

// holtWintersAnalysis takes a series, and returns the analysis result.
func holtWintersAnalysis(ctx *common.Context, series *ts.Series) *holtWintersAnalysisResult {
	seasonLength := secondsPerDay * millisPerSecond / series.MillisPerStep()
	numPoints := series.Len()
	intercepts := make([]float64, numPoints)
	slopes := make([]float64, numPoints)
	seasonals := make([]float64, numPoints)
	predictions := ts.NewValues(ctx, series.MillisPerStep(), numPoints)
	deviations := ts.NewValues(ctx, series.MillisPerStep(), numPoints)

	getLastSeasonal := func(i int) float64 {
		j := i - seasonLength
		if j >= 0 {
			return seasonals[j]
		}
		return 0.0
	}

	getLastDeviation := func(i int) float64 {
		j := i - seasonLength
		if j >= 0 {
			return deviations.ValueAt(j)
		}
		return 0.0
	}

	nextPred := math.NaN()
	for i := 0; i < numPoints; i++ {
		actual := series.ValueAt(i)
		if math.IsNaN(actual) {
			intercepts[i] = math.NaN()
			predictions.SetValueAt(i, nextPred)
			deviations.SetValueAt(i, 0.0)
			nextPred = math.NaN()
			continue
		}

		var lastIntercept, lastSlope, prediction float64
		if i == 0 {
			lastIntercept = actual
			lastSlope = 0
			prediction = actual
		} else {
			lastIntercept = intercepts[i-1]
			lastSlope = slopes[i-1]
			if math.IsNaN(lastIntercept) {
				lastIntercept = actual
			}
			prediction = nextPred
		}

		lastSeasonal := getLastSeasonal(i)
		nextLastSeasonal := getLastSeasonal(i + 1)
		lastSeasonalDev := getLastDeviation(i)

		intercept := holtWintersIntercept(actual, lastSeasonal, lastIntercept, lastSlope)
		slope := holtWintersSlope(intercept, lastIntercept, lastSlope)
		seasonal := holtWintersSeasonal(actual, intercept, lastSeasonal)
		nextPred = intercept + slope + nextLastSeasonal
		deviation := holtWintersDeviation(actual, prediction, lastSeasonalDev)

		intercepts[i] = intercept
		slopes[i] = slope
		seasonals[i] = seasonal
		predictions.SetValueAt(i, prediction)
		deviations.SetValueAt(i, deviation)
	}

	// make the new forecast series
	forecastName := fmt.Sprintf("holtWintersForecast(%s)", series.Name())
	forecastSeries := ts.NewSeries(ctx, forecastName, series.StartTime(), predictions)

	// make the new deviation series
	deviationName := fmt.Sprintf("holtWintersDeviation(%s)", series.Name())
	deviationSeries := ts.NewSeries(ctx, deviationName, series.StartTime(), deviations)

	return &holtWintersAnalysisResult{
		predictions: forecastSeries,
		deviations:  deviationSeries,
		intercepts:  intercepts,
		slopes:      slopes,
		seasonals:   seasonals,
	}
}

// squareRoot takes one metric or a wildcard seriesList, and computes the square root of each datapoint.
func squareRoot(ctx *common.Context, seriesList singlePathSpec) (ts.SeriesList, error) {
	return transform(
		ctx,
		seriesList,
		func(fname string) string { return fmt.Sprintf(wrappingFmt, "squareRoot", fname) },
		math.Sqrt,
	)
}

// stdev takes one metric or a wildcard seriesList followed by an integer N. Draw the standard deviation
// of all metrics passed for the past N datapoints. If the ratio of null points in the window is greater than
// windowTolerance, skip the calculation.
func stdev(ctx *common.Context, seriesList singlePathSpec, points int, windowTolerance float64) (ts.SeriesList, error) {
	return common.Stdev(ctx, ts.SeriesList(seriesList), points, windowTolerance, func(series *ts.Series, points int) string {
		return fmt.Sprintf("stddev(%s,%d)", series.Name(), points)
	})
}

// rangeOfSeries distills down a set of inputs into the range of the series.
func rangeOfSeries(ctx *common.Context, seriesList singlePathSpec) (ts.SeriesList, error) {
	series, err := common.Range(ctx, ts.SeriesList(seriesList), func(series ts.SeriesList) string {
		return wrapPathExpr("rangeOfSeries", series)
	})
	if err != nil {
		return ts.NewSeriesList(), err
	}
	return ts.SeriesList{
		Values:   []*ts.Series{series},
		Metadata: seriesList.Metadata,
	}, nil
}

// removeAbovePercentile removes data above the specified percentile from the series
// or list of series provided. Values above this percentile are assigned a value
// of None.
func removeAbovePercentile(ctx *common.Context, seriesList singlePathSpec, percentile float64) (ts.SeriesList, error) {
	return common.RemoveByPercentile(ctx,
		ts.SeriesList(seriesList),
		percentile,
		func(name string, percentile float64) string {
			return fmt.Sprintf("removeAbovePercentile(%s, "+common.FloatingPointFormat+")", name, percentile)
		},
		common.GreaterThan)
}

// removeBelowPercentile removes data below the specified percentile from the series
// or list of series provided. Values below this percentile are assigned a value of None.
func removeBelowPercentile(ctx *common.Context, seriesList singlePathSpec, percentile float64) (ts.SeriesList, error) {
	return common.RemoveByPercentile(
		ctx,
		ts.SeriesList(seriesList),
		percentile,
		func(name string, percentile float64) string {
			return fmt.Sprintf("removeBelowPercentile(%s, "+common.FloatingPointFormat+")", name, percentile)
		},
		common.LessThan)
}

// randomWalkFunction returns a random walk starting at 0.
// Note: step has a unit of seconds.
func randomWalkFunction(ctx *common.Context, name string, step int) (ts.SeriesList, error) {
	if step <= 0 {
		return ts.NewSeriesList(), errors.NewInvalidParamsError(fmt.Errorf("non-positive step size %d", step))
	}
	if !ctx.StartTime.Before(ctx.EndTime) {
		return ts.NewSeriesList(), errors.NewInvalidParamsError(fmt.Errorf("startTime %v is no earlier than endTime %v", ctx.StartTime, ctx.EndTime))
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	millisPerStep := step * millisPerSecond
	numSteps := ts.NumSteps(ctx.StartTime, ctx.EndTime, millisPerStep)
	vals := ts.NewValues(ctx, millisPerStep, numSteps)
	for i := 0; i < numSteps; i++ {
		vals.SetValueAt(i, r.Float64()-0.5)
	}
	newSeries := ts.NewSeries(ctx, name, ctx.StartTime, vals)
	return ts.NewSeriesListWithSeries(newSeries), nil
}

// aggregateLine draws a horizontal line based the function applied to the series.
func aggregateLine(ctx *common.Context, seriesList singlePathSpec, f string) (ts.SeriesList, error) {
	if len(seriesList.Values) == 0 {
		return ts.NewSeriesList(), common.ErrEmptySeriesList
	}

	sa := ts.SeriesReducerApproach(f)
	r, ok := sa.SafeReducer()
	if !ok {
		return ts.NewSeriesList(), errors.NewInvalidParamsError(fmt.Errorf("invalid function %s", f))
	}

	value := r(seriesList.Values[0])
	name := fmt.Sprintf("aggregateLine(%s,"+common.FloatingPointFormat+")",
		seriesList.Values[0].Specification, value)
	series, err := constantLine(ctx, value)
	if err != nil {
		return ts.NewSeriesList(), err
	}

	renamed := series.Values[0].RenamedTo(name)
	return ts.SeriesList{
		Values:   []*ts.Series{renamed},
		Metadata: seriesList.Metadata,
	}, nil
}

// changed takes one metric or a wildcard seriesList.
// Output 1 when the value changed, 0 when null or the same.
func changed(ctx *common.Context, seriesList singlePathSpec) (ts.SeriesList, error) {
	return common.Changed(ctx, ts.SeriesList(seriesList), func(series *ts.Series) string {
		return fmt.Sprintf("changed(%s)", series.Name())
	})
}

// windowPointsLength calculates the number of window points in a interval
func windowPointsLength(series *ts.Series, interval time.Duration) int {
	return int(interval / (time.Duration(series.MillisPerStep()) * time.Millisecond))
}

type movingImplementationFn func(window []float64, values ts.MutableValues, windowPoints int, i int, xFilesFactor float64)

// movingMedianHelper given a slice of floats, calculates the median and assigns it into vals as index i
func movingMedianHelper(window []float64, vals ts.MutableValues, windowPoints int, i int, xFilesFactor float64) {
	nans := common.SafeSort(window)

	if nans < windowPoints && effectiveXFF(windowPoints, nans, xFilesFactor) {
		index := (windowPoints - nans) / 2
		median := window[nans+index]
		vals.SetValueAt(i, median)
	}
}

// movingSumHelper given a slice of floats, calculates the sum and assigns it into vals as index i
func movingSumHelper(window []float64, vals ts.MutableValues, windowPoints int, i int, xFilesFactor float64) {
	sum, nans := common.SafeSum(window)

	if nans < windowPoints && effectiveXFF(windowPoints, nans, xFilesFactor) {
		vals.SetValueAt(i, sum)
	}
}

// movingMaxHelper given a slice of floats, finds the max and assigns it into vals as index i
func movingMaxHelper(window []float64, vals ts.MutableValues, windowPoints int, i int, xFilesFactor float64) {
	max, nans := common.SafeMax(window)

	if nans < windowPoints && effectiveXFF(windowPoints, nans, xFilesFactor) {
		vals.SetValueAt(i, max)
	}
}

// movingMinHelper given a slice of floats, finds the min and assigns it into vals as index i
func movingMinHelper(window []float64, vals ts.MutableValues, windowPoints int, i int, xFilesFactor float64) {
	min, nans := common.SafeMin(window)

	if nans < windowPoints && effectiveXFF(windowPoints, nans, xFilesFactor) {
		vals.SetValueAt(i, min)
	}
}

func newMovingBinaryTransform(
	ctx *common.Context,
	input singlePathSpec,
	windowSizeValue genericInterface,
	movingFunctionName string,
	xFilesFactor float64,
	impl movingImplementationFn,
) (*binaryContextShifter, error) {
	if len(input.Values) == 0 {
		return nil, nil
	}

	windowSize, err := parseWindowSize(windowSizeValue, input)
	if err != nil {
		return nil, err
	}

	interval := windowSize.deltaValue
	if interval <= 0 {
		return nil, common.ErrInvalidIntervalFormat
	}

	contextShiftingFn := func(c *common.Context) *common.Context {
		opts := common.NewChildContextOptions()
		opts.AdjustTimeRange(0, 0, interval, 0)
		childCtx := c.NewChildContext(opts)
		return childCtx
	}

	bootstrapStartTime, bootstrapEndTime := ctx.StartTime.Add(-interval), ctx.StartTime
	return &binaryContextShifter{
		ContextShiftFunc: contextShiftingFn,
		BinaryTransformer: func(bootstrapped, original ts.SeriesList) (ts.SeriesList, error) {
			bootstrapList, err := combineBootstrapWithOriginal(ctx,
				bootstrapStartTime, bootstrapEndTime,
				bootstrapped, singlePathSpec(original))
			if err != nil {
				return ts.NewSeriesList(), err
			}

			results := make([]*ts.Series, 0, original.Len())
			maxWindowPoints := 0
			for i, _ := range bootstrapList.Values {
				series := original.Values[i]
				windowPoints := windowPointsLength(series, interval)
				if windowPoints <= 0 {
					err := errors.NewInvalidParamsError(fmt.Errorf(
						"non positive window points, windowSize=%s, stepSize=%d",
						windowSize.stringValue, series.MillisPerStep()))
					return ts.NewSeriesList(), err
				}
				if windowPoints > maxWindowPoints {
					maxWindowPoints = windowPoints
				}
			}

			windowPoints := make([]float64, maxWindowPoints)
			for i, bootstrap := range bootstrapList.Values {
				series := original.Values[i]
				currWindowPoints := windowPointsLength(series, interval)
				window := windowPoints[:currWindowPoints]
				util.Memset(window, math.NaN())
				numSteps := series.Len()
				offset := bootstrap.Len() - numSteps
				vals := ts.NewValues(ctx, series.MillisPerStep(), numSteps)
				for i := 0; i < numSteps; i++ {
					for j := i + offset - currWindowPoints; j < i+offset; j++ {
						if j < 0 || j >= bootstrap.Len() {
							continue
						}

						idx := j - i - offset + currWindowPoints
						if idx < 0 || idx > len(window)-1 {
							continue
						}

						window[idx] = bootstrap.ValueAt(j)
					}
					impl(window, vals, currWindowPoints, i, xFilesFactor)
				}
				name := fmt.Sprintf("%s(%s,%s)", movingFunctionName, series.Name(), windowSize.stringValue)
				newSeries := ts.NewSeries(ctx, name, series.StartTime(), vals)
				results = append(results, newSeries)
			}

			original.Values = results
			return original, nil
		},
	}, nil
}

// movingMedian calculates the moving median of a metric (or metrics) over a time interval.
func movingMedian(ctx *common.Context, input singlePathSpec, windowSize genericInterface, xFilesFactor float64) (*binaryContextShifter, error) {
	return newMovingBinaryTransform(ctx, input, windowSize, "movingMedian", xFilesFactor, movingMedianHelper)
}

// movingSum calculates the moving sum of a metric (or metrics) over a time interval.
func movingSum(ctx *common.Context, input singlePathSpec, windowSize genericInterface, xFilesFactor float64) (*binaryContextShifter, error) {
	return newMovingBinaryTransform(ctx, input, windowSize, "movingSum", xFilesFactor, movingSumHelper)
}

// movingMax calculates the moving maximum of a metric (or metrics) over a time interval.
func movingMax(ctx *common.Context, input singlePathSpec, windowSize genericInterface, xFilesFactor float64) (*binaryContextShifter, error) {
	return newMovingBinaryTransform(ctx, input, windowSize, "movingMax", xFilesFactor, movingMaxHelper)
}

// movingMin calculates the moving minimum of a metric (or metrics) over a time interval.
func movingMin(ctx *common.Context, input singlePathSpec, windowSize genericInterface, xFilesFactor float64) (*binaryContextShifter, error) {
	return newMovingBinaryTransform(ctx, input, windowSize, "movingMin", xFilesFactor, movingMinHelper)
}

// legendValue takes one metric or a wildcard seriesList and a string in quotes.
// Appends a value to the metric name in the legend.  Currently one or several of:
// "last", "avg", "total", "min", "max".
func legendValue(_ *common.Context, seriesList singlePathSpec, valueType string) (ts.SeriesList, error) {
	sa := ts.SeriesReducerApproach(valueType)
	reducer, ok := sa.SafeReducer()
	if !ok {
		return ts.NewSeriesList(), errors.NewInvalidParamsError(fmt.Errorf("invalid function %s", valueType))
	}

	results := make([]*ts.Series, 0, len(seriesList.Values))
	for _, series := range seriesList.Values {
		value := reducer(series)
		newName := fmt.Sprintf("%s (%s: "+common.FloatingPointFormat+")", series.Name(), valueType, value)
		renamed := series.RenamedTo(newName)
		results = append(results, renamed)
	}

	r := ts.SeriesList(seriesList)
	r.Values = results
	return r, nil
}

func getStatLen(v float64) int {
	if math.IsNaN(v) {
		return 4
	}
	return len(fmt.Sprintf("%d", int(v))) + 3
}

func toCactiStyle(v float64) string {
	if math.IsNaN(v) {
		return "nan"
	}
	return fmt.Sprintf(cactiStyleFormat, v)
}

func findAllLens(seriesList ts.SeriesList) (int, int, int, int) {
	var nameLen, lastLen, maxLen, minLen float64
	for _, series := range seriesList.Values {
		name, min, max, last := series.Name(), series.SafeMin(), series.SafeMax(), series.SafeLastValue()
		nameLen = math.Max(nameLen, float64(len(name)))
		lastLen = math.Max(lastLen, float64(getStatLen(last)))
		maxLen = math.Max(maxLen, float64(getStatLen(max)))
		minLen = math.Max(minLen, float64(getStatLen(min)))
	}
	return int(nameLen), int(lastLen) + 3, int(maxLen) + 3, int(minLen) + 3
}

// cactiStyle takes a series list and modifies the aliases to provide column aligned
// output with Current, Max, and Min values in the style of cacti.
func cactiStyle(_ *common.Context, seriesList singlePathSpec) (ts.SeriesList, error) {
	if len(seriesList.Values) == 0 {
		return ts.NewSeriesList(), common.ErrEmptySeriesList
	}

	nameLen, lastLen, maxLen, minLen := findAllLens(ts.SeriesList(seriesList))
	results := make([]*ts.Series, 0, len(seriesList.Values))
	for _, series := range seriesList.Values {
		name := series.Name()
		last := toCactiStyle(series.SafeLastValue())
		max := toCactiStyle(series.SafeMax())
		min := toCactiStyle(series.SafeMin())

		newName := fmt.Sprintf(
			"%*s Current:%*s Max:%*s Min:%*s ",
			-nameLen, name,
			-lastLen, last,
			-maxLen, max,
			-minLen, min,
		)
		renamed := series.RenamedTo(newName)
		results = append(results, renamed)
	}

	r := ts.SeriesList(seriesList)
	r.Values = results
	return r, nil
}

// consolidateBy takes one metric or a wildcard seriesList and a consolidation
// function name. Valid function names are "sum", "average", "min", and "max".
// When a graph is drawn where width of the graph size in pixels is smaller than
// the number of data points to be graphed, m3 consolidates the values to
// to prevent line overlap. The consolidateBy() function changes the consolidation
// function from the default of "average" to one of "sum", "max", or "min".
func consolidateBy(_ *common.Context, seriesList singlePathSpec, consolidationApproach string) (ts.SeriesList, error) {
	ca := ts.ConsolidationApproach(consolidationApproach)
	cf, ok := ca.SafeFunc()
	if !ok {
		err := errors.NewInvalidParamsError(fmt.Errorf("invalid consolidation approach %s", consolidationApproach))
		return ts.NewSeriesList(), err
	}

	results := make([]*ts.Series, 0, len(seriesList.Values))
	for _, series := range seriesList.Values {
		newName := fmt.Sprintf("consolidateBy(%s,%q)", series.Name(), consolidationApproach)
		renamed := series.RenamedTo(newName)
		renamed.SetConsolidationFunc(cf)
		results = append(results, renamed)
	}

	r := ts.SeriesList(seriesList)
	r.Values = results
	return r, nil
}

// cumulative is an alias for consolidateBy(series, 'sum')
func cumulative(ctx *common.Context, seriesList singlePathSpec) (ts.SeriesList, error) {
	return consolidateBy(ctx, seriesList, "sum")
}

// offsetToZero offsets a metric or wildcard seriesList by subtracting the minimum
// value in the series from each data point.
func offsetToZero(ctx *common.Context, seriesList singlePathSpec) (ts.SeriesList, error) {
	results := make([]*ts.Series, len(seriesList.Values))
	for idx, series := range seriesList.Values {
		minimum := series.SafeMin()
		numSteps := series.Len()
		vals := ts.NewValues(ctx, series.MillisPerStep(), numSteps)
		if !math.IsNaN(minimum) {
			for i := 0; i < numSteps; i++ {
				v := series.ValueAt(i)
				if !math.IsNaN(v) {
					vals.SetValueAt(i, v-minimum)
				}
			}
		}
		name := fmt.Sprintf("offsetToZero(%s)", series.Name())
		series := ts.NewSeries(ctx, name, series.StartTime(), vals)
		results[idx] = series
	}

	r := ts.SeriesList(seriesList)
	r.Values = results
	return r, nil
}

// timeFunction returns the timestamp for each X value.
// Note: step is measured in seconds.
func timeFunction(ctx *common.Context, name string, step int) (ts.SeriesList, error) {
	if step <= 0 {
		return ts.NewSeriesList(), errors.NewInvalidParamsError(fmt.Errorf("step must be a positive int but instead is %d", step))
	}

	stepSizeInMilli := step * millisPerSecond
	numSteps := ts.NumSteps(ctx.StartTime, ctx.EndTime, stepSizeInMilli)
	vals := ts.NewValues(ctx, stepSizeInMilli, numSteps)
	start := ctx.StartTime.Truncate(time.Second)
	for current, index := start.Unix(), 0; index < numSteps; index++ {
		vals.SetValueAt(index, float64(current))
		current += int64(step)
	}

	series := ts.NewSeries(ctx, name, start, vals)
	return ts.NewSeriesListWithSeries(series), nil
}

// dashed draws the selected metrics with a dotted line with segments of length f.
func dashed(_ *common.Context, seriesList singlePathSpec, dashLength float64) (ts.SeriesList, error) {
	if dashLength <= 0 {
		return ts.NewSeriesList(), errors.NewInvalidParamsError(fmt.Errorf("expected a positive dashLength but got %f", dashLength))
	}

	results := make([]*ts.Series, len(seriesList.Values))
	for idx, s := range seriesList.Values {
		name := fmt.Sprintf("dashed(%s, "+common.FloatingPointFormat+")", s.Name(), dashLength)
		renamed := s.RenamedTo(name)
		results[idx] = renamed
	}

	r := ts.SeriesList(seriesList)
	r.Values = results
	return r, nil
}

// threshold draws a horizontal line at value f across the graph.
func threshold(ctx *common.Context, value float64, label string, color string) (ts.SeriesList, error) {
	seriesList, err := constantLine(ctx, value)
	if err != nil {
		err := errors.NewInvalidParamsError(fmt.Errorf(
			"error applying threshold function, error=%v", err))
		return ts.NewSeriesList(), err
	}

	series := seriesList.Values[0]
	if label != "" {
		series = series.RenamedTo(label)
	}

	return ts.NewSeriesListWithSeries(series), nil
}

func init() {
	// functions - in alpha ordering
	MustRegisterFunction(absolute)
	MustRegisterFunction(aggregate)
	MustRegisterFunction(aggregateLine).WithDefaultParams(map[uint8]interface{}{
		2: "avg", // f
	})
	MustRegisterFunction(aggregateWithWildcards).WithDefaultParams(map[uint8]interface{}{
		3: -1, // positions
	})
	MustRegisterFunction(alias)
	MustRegisterFunction(aliasByMetric)
	MustRegisterFunction(aliasByNode)
	MustRegisterFunction(aliasSub)
	MustRegisterFunction(applyByNode).WithDefaultParams(map[uint8]interface{}{
		4: "", // newName
	})
	MustRegisterFunction(asPercent).WithDefaultParams(map[uint8]interface{}{
		2: []*ts.Series(nil), // total
	})
	MustRegisterFunction(averageAbove)
	MustRegisterFunction(averageSeries)
	MustRegisterFunction(averageSeriesWithWildcards).WithDefaultParams(map[uint8]interface{}{
		2: -1, // positions
	})
	MustRegisterFunction(cactiStyle)
	MustRegisterFunction(changed)
	MustRegisterFunction(consolidateBy)
	MustRegisterFunction(constantLine)
	MustRegisterFunction(countSeries)
	MustRegisterFunction(cumulative)
	MustRegisterFunction(currentAbove)
	MustRegisterFunction(currentBelow)
	MustRegisterFunction(dashed).WithDefaultParams(map[uint8]interface{}{
		2: 5.0, // dashLength
	})
	MustRegisterFunction(delay)
	MustRegisterFunction(derivative)
	MustRegisterFunction(diffSeries)
	MustRegisterFunction(divideSeries)
	MustRegisterFunction(divideSeriesLists)
	MustRegisterFunction(exclude)
	MustRegisterFunction(exponentialMovingAverage)
	MustRegisterFunction(fallbackSeries)
	MustRegisterFunction(grep)
	MustRegisterFunction(group)
	MustRegisterFunction(groupByNode).WithDefaultParams(map[uint8]interface{}{
		3: "average", // fname
	})
	MustRegisterFunction(groupByNodes)
	MustRegisterFunction(highest).WithDefaultParams(map[uint8]interface{}{
		2: 1,         // n,
		3: "average", // f
	})
	MustRegisterFunction(highestAverage)
	MustRegisterFunction(highestCurrent)
	MustRegisterFunction(highestMax)
	MustRegisterFunction(hitcount)
	MustRegisterFunction(holtWintersAberration)
	MustRegisterFunction(holtWintersConfidenceBands)
	MustRegisterFunction(holtWintersForecast)
	MustRegisterFunction(identity)
	MustRegisterFunction(integral)
	MustRegisterFunction(integralByInterval)
	MustRegisterFunction(interpolate).WithDefaultParams(map[uint8]interface{}{
		2: -1, // limit
	})
	MustRegisterFunction(isNonNull)
	MustRegisterFunction(keepLastValue).WithDefaultParams(map[uint8]interface{}{
		2: -1, // limit
	})
	MustRegisterFunction(legendValue)
	MustRegisterFunction(limit)
	MustRegisterFunction(logarithm).WithDefaultParams(map[uint8]interface{}{
		2: 10, // base
	})
	MustRegisterFunction(lowest).WithDefaultParams(map[uint8]interface{}{
		2: 1,         // n,
		3: "average", // f
	})
	MustRegisterFunction(lowestAverage)
	MustRegisterFunction(lowestCurrent)
	MustRegisterFunction(maxSeries)
	MustRegisterFunction(maximumAbove)
	MustRegisterFunction(minSeries)
	MustRegisterFunction(minimumAbove)
	MustRegisterFunction(mostDeviant)
	MustRegisterFunction(movingAverage).WithDefaultParams(map[uint8]interface{}{
		3: 0.0, // XFilesFactor
	})
	MustRegisterFunction(movingMedian).WithDefaultParams(map[uint8]interface{}{
		3: 0.0, // XFilesFactor
	})
	MustRegisterFunction(movingSum).WithDefaultParams(map[uint8]interface{}{
		3: 0.0, // XFilesFactor
	})
	MustRegisterFunction(movingMax).WithDefaultParams(map[uint8]interface{}{
		3: 0.0, // XFilesFactor
	})
	MustRegisterFunction(movingMin).WithDefaultParams(map[uint8]interface{}{
		3: 0.0, // XFilesFactor
	})
	MustRegisterFunction(multiplySeries)
	MustRegisterFunction(nonNegativeDerivative).WithDefaultParams(map[uint8]interface{}{
		2: math.NaN(), // maxValue
	})
	MustRegisterFunction(nPercentile)
	MustRegisterFunction(offset)
	MustRegisterFunction(offsetToZero)
	MustRegisterFunction(percentileOfSeries).WithDefaultParams(map[uint8]interface{}{
		3: false, // interpolate
	})
	MustRegisterFunction(perSecond).WithDefaultParams(map[uint8]interface{}{
		2: math.NaN(), // maxValue
	})
	MustRegisterFunction(powSeries)
	MustRegisterFunction(rangeOfSeries)
	MustRegisterFunction(randomWalkFunction).WithDefaultParams(map[uint8]interface{}{
		2: 60, // step
	})
	MustRegisterFunction(removeAbovePercentile)
	MustRegisterFunction(removeAboveValue)
	MustRegisterFunction(removeBelowPercentile)
	MustRegisterFunction(removeBelowValue)
	MustRegisterFunction(removeEmptySeries)
	MustRegisterFunction(scale)
	MustRegisterFunction(scaleToSeconds)
	MustRegisterFunction(sortByMaxima)
	MustRegisterFunction(sortByMinima)
	MustRegisterFunction(sortByName)
	MustRegisterFunction(sortByTotal)
	MustRegisterFunction(squareRoot)
	MustRegisterFunction(stdev).WithDefaultParams(map[uint8]interface{}{
		3: 0.1, // windowTolerance
	})
	MustRegisterFunction(stddevSeries)
	MustRegisterFunction(substr).WithDefaultParams(map[uint8]interface{}{
		2: 0, // start
		3: 0, // stop
	})
	MustRegisterFunction(summarize).WithDefaultParams(map[uint8]interface{}{
		3: "",    // fname
		4: false, // alignToFrom
	})
	MustRegisterFunction(smartSummarize).WithDefaultParams(map[uint8]interface{}{
		3: "", // fname
	})
	MustRegisterFunction(sumSeries)
	MustRegisterFunction(sumSeriesWithWildcards).WithDefaultParams(map[uint8]interface{}{
		2: -1, // positions
	})
	MustRegisterFunction(sustainedAbove)
	MustRegisterFunction(sustainedBelow)
	MustRegisterFunction(threshold).WithDefaultParams(map[uint8]interface{}{
		2: "", // label
		3: "", // color
	})
	MustRegisterFunction(timeFunction).WithDefaultParams(map[uint8]interface{}{
		2: 60, // step
	})
	MustRegisterFunction(timeShift).WithDefaultParams(map[uint8]interface{}{
		3: true,  // resetEnd
		4: false, // alignDst
	})
	MustRegisterFunction(timeSlice).WithDefaultParams(map[uint8]interface{}{
		3: "now", // endTime
	})
	MustRegisterFunction(transformNull).WithDefaultParams(map[uint8]interface{}{
		2: 0.0, // defaultValue
	})
	MustRegisterFunction(useSeriesAbove)
	MustRegisterFunction(weightedAverage)

	// alias functions - in alpha ordering
	MustRegisterAliasedFunction("abs", absolute)
	MustRegisterAliasedFunction("aliasByTags", aliasByNode)
	MustRegisterAliasedFunction("avg", averageSeries)
	MustRegisterAliasedFunction("log", logarithm)
	MustRegisterAliasedFunction("max", maxSeries)
	MustRegisterAliasedFunction("min", minSeries)
	MustRegisterAliasedFunction("randomWalk", randomWalkFunction)
	MustRegisterAliasedFunction("sum", sumSeries)
	MustRegisterAliasedFunction("time", timeFunction)
}
