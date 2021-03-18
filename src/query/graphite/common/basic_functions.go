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
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/m3db/m3/src/query/graphite/ts"
	xerrors "github.com/m3db/m3/src/x/errors"
)

var (
	// ErrNegativeCount occurs when the request count is < 0.
	ErrNegativeCount = xerrors.NewInvalidParamsError(errors.New("n must be positive"))
	// ErrEmptySeriesList occurs when a function requires a series as input
	ErrEmptySeriesList = xerrors.NewInvalidParamsError(errors.New("empty series list"))
	// ErrInvalidIntervalFormat occurs when invalid interval string encountered
	ErrInvalidIntervalFormat = xerrors.NewInvalidParamsError(errors.New("invalid format"))

	reInterval *regexp.Regexp

	intervals = map[string]time.Duration{
		"s":       time.Second,
		"sec":     time.Second,
		"seconds": time.Second,
		"m":       time.Minute,
		"min":     time.Minute,
		"mins":    time.Minute,
		"minute":  time.Minute,
		"minutes": time.Minute,
		"h":       time.Hour,
		"hr":      time.Hour,
		"hour":    time.Hour,
		"hours":   time.Hour,
		"d":       time.Hour * 24,
		"day":     time.Hour * 24,
		"days":    time.Hour * 24,
		"w":       time.Hour * 24 * 7,
		"week":    time.Hour * 24 * 7,
		"weeks":   time.Hour * 24 * 7,
		"mon":     time.Hour * 24 * 30,
		"month":   time.Hour * 24 * 30,
		"months":  time.Hour * 24 * 30,
		"y":       time.Hour * 24 * 365,
		"year":    time.Hour * 24 * 365,
		"years":   time.Hour * 24 * 365,
	}
)

const (
	// MillisPerSecond is for millis per second
	MillisPerSecond = 1000
	// SecondsPerMinute is for seconds per minute
	SecondsPerMinute = 60
	// MillisPerMinute is for milliseconds per minute
	MillisPerMinute = MillisPerSecond * SecondsPerMinute
)

// SeriesListRenamer is a signature for renaming multiple series
// into a single name
type SeriesListRenamer func(series ts.SeriesList) string

// SeriesRenamer is a signature for renaming a single series
type SeriesRenamer func(series *ts.Series) string

// Head returns the first n elements of a series list or the entire list
func Head(series ts.SeriesList, n int) (ts.SeriesList, error) {
	if n < 0 {
		return ts.NewSeriesList(), ErrNegativeCount
	}
	r := series.Values[:int(math.Min(float64(n), float64(series.Len())))]
	series.Values = r
	return series, nil
}

// Identity returns datapoints where the value equals the timestamp of the datapoint.
func Identity(ctx *Context, name string) (ts.SeriesList, error) {
	millisPerStep := int(MillisPerMinute)
	numSteps := int(ctx.EndTime.Sub(ctx.StartTime) / time.Minute)
	vals := ts.NewValues(ctx, millisPerStep, numSteps)
	curTimeInSeconds := ctx.StartTime.Unix()
	for i := 0; i < vals.Len(); i++ {
		vals.SetValueAt(i, float64(curTimeInSeconds))
		curTimeInSeconds += SecondsPerMinute
	}
	newSeries := ts.NewSeries(ctx, name, ctx.StartTime, vals)
	newSeries.Specification = fmt.Sprintf("identity(%q)", name)
	return ts.NewSeriesListWithSeries(newSeries), nil
}

// Normalize normalizes all input series to the same start time, step size, and end time.
func Normalize(ctx *Context, input ts.SeriesList) (ts.SeriesList, time.Time, time.Time, int, error) {
	numSeries := input.Len()
	if numSeries == 0 {
		return ts.NewSeriesList(), ctx.StartTime, ctx.EndTime, -1, xerrors.NewInvalidParamsError(ErrEmptySeriesList)
	}
	if numSeries == 1 {
		return input, input.Values[0].StartTime(), input.Values[0].EndTime(), input.Values[0].MillisPerStep(), nil
	}

	lcmMillisPerStep := input.Values[0].MillisPerStep()
	minBegin, maxEnd := input.Values[0].StartTime(), input.Values[0].EndTime()

	for _, in := range input.Values[1:] {
		lcmMillisPerStep = int(ts.Lcm(int64(lcmMillisPerStep), int64(in.MillisPerStep())))

		if minBegin.After(in.StartTime()) {
			minBegin = in.StartTime()
		}

		if maxEnd.Before(in.EndTime()) {
			maxEnd = in.EndTime()
		}
	}

	// Fix the right interval border to be divisible by interval step.
	maxEnd = maxEnd.Add(-time.Duration(
		int64(maxEnd.Sub(minBegin)/time.Millisecond)%int64(lcmMillisPerStep)) * time.Millisecond)

	numSteps := ts.NumSteps(minBegin, maxEnd, lcmMillisPerStep)

	results := make([]*ts.Series, input.Len())

	for i, in := range input.Values {
		if in.StartTime() == minBegin && in.MillisPerStep() == lcmMillisPerStep && in.Len() == numSteps {
			results[i] = in
			continue
		}

		c := ts.NewConsolidation(ctx, minBegin, maxEnd, lcmMillisPerStep, ts.Avg)
		c.AddSeries(in, ts.Avg)
		results[i] = c.BuildSeries(in.Name(), ts.Finalize)
	}

	input.Values = results
	return input, minBegin, maxEnd, lcmMillisPerStep, nil
}

// Count draws a horizontal line representing the number of nodes found in the seriesList.
func Count(ctx *Context, seriesList ts.SeriesList, renamer SeriesListRenamer) (ts.SeriesList, error) {
	if seriesList.Len() == 0 {
		numSteps := ctx.EndTime.Sub(ctx.StartTime).Minutes()
		vals := ts.NewZeroValues(ctx, MillisPerMinute, int(numSteps))
		return ts.NewSeriesListWithSeries(
			ts.NewSeries(ctx, renamer(seriesList), ctx.StartTime, vals),
		), nil
	}

	normalized, start, end, millisPerStep, err := Normalize(ctx, seriesList)
	if err != nil {
		return ts.NewSeriesList(), err
	}
	numSteps := int(end.Sub(start) / (time.Duration(millisPerStep) * time.Millisecond))
	vals := ts.NewConstantValues(ctx, float64(normalized.Len()), numSteps, millisPerStep)
	return ts.SeriesList{
		Values:   []*ts.Series{ts.NewSeries(ctx, renamer(normalized), start, vals)},
		Metadata: seriesList.Metadata,
	}, nil
}

// ParseInterval parses an interval string and returns the corresponding duration.
func ParseInterval(fullInterval string) (time.Duration, error) {
	allIntervals := reInterval.FindAllString(fullInterval, -1)
	output := time.Duration(0)
	if allIntervals == nil {
		return 0, xerrors.NewInvalidParamsError(
			fmt.Errorf("unrecognized interval string: %s", fullInterval))
	}

	for _, interval := range allIntervals {
		if m := reInterval.FindStringSubmatch(strings.TrimSpace(interval)); len(m) != 0 {
			amount, err := strconv.ParseInt(m[1], 10, 32)
			if err != nil {
				return 0, xerrors.NewInvalidParamsError(err)
			}

			interval := intervals[strings.ToLower(m[2])]
			output += (interval * time.Duration(amount))
		}
	}

	return output, nil
}

// ConstantLine draws a horizontal line at a specified value
func ConstantLine(ctx *Context, value float64) (*ts.Series, error) {
	millisPerStep := int(ctx.EndTime.Sub(ctx.StartTime) / (2 * time.Millisecond))
	if millisPerStep <= 0 {
		err := fmt.Errorf("invalid boundary params: startTime=%v, endTime=%v", ctx.StartTime, ctx.EndTime)
		return nil, err
	}
	name := fmt.Sprintf(FloatingPointFormat, value)
	newSeries := ts.NewSeries(ctx, name, ctx.StartTime, ts.NewConstantValues(ctx, value, 3, millisPerStep))
	return newSeries, nil
}

// ConstantSeries returns a new constant series with a granularity
// of one data point per second
func ConstantSeries(ctx *Context, value float64) (*ts.Series, error) {
	// NB(jeromefroe): We use a granularity of one second to ensure that when multiple series
	// are normalized the constant series will always have the smallest granularity and will
	// not cause another series to be normalized to a greater granularity.
	numSteps := int(ctx.EndTime.Sub(ctx.StartTime) / time.Second)
	if numSteps <= 0 {
		err := fmt.Errorf("invalid boundary params: startTime=%v, endTime=%v", ctx.StartTime, ctx.EndTime)
		return nil, err
	}
	name := fmt.Sprintf(FloatingPointFormat, value)
	newSeries := ts.NewSeries(ctx, name, ctx.StartTime, ts.NewConstantValues(ctx, value, numSteps, MillisPerSecond))
	return newSeries, nil
}

// RemoveEmpty removes all series that have NaN data
func RemoveEmpty(ctx *Context, input ts.SeriesList) (ts.SeriesList, error) {
	output := make([]*ts.Series, 0, input.Len())
	for _, series := range input.Values {
		if series.AllNaN() {
			continue
		}
		seriesHasData := false
		for i := 0; i < series.Len(); i++ {
			v := series.ValueAt(i)
			if !math.IsNaN(v) {
				seriesHasData = true
				break
			}
		}
		if seriesHasData {
			output = append(output, series)
		}
	}
	input.Values = output
	return input, nil
}

// Changed will output a 1 if the value changed or 0 if not
func Changed(ctx *Context, seriesList ts.SeriesList, renamer SeriesRenamer) (ts.SeriesList, error) {
	results := make([]*ts.Series, 0, seriesList.Len())
	nan := math.NaN()
	for _, series := range seriesList.Values {
		previous := nan
		numSteps := series.Len()
		vals := ts.NewValues(ctx, series.MillisPerStep(), numSteps)
		for i := 0; i < numSteps; i++ {
			v := series.ValueAt(i)
			if math.IsNaN(previous) {
				previous = v
				vals.SetValueAt(i, 0)
			} else if !math.IsNaN(v) && previous != v {
				previous = v
				vals.SetValueAt(i, 1)
			} else {
				vals.SetValueAt(i, 0)
			}
		}
		newSeries := ts.NewSeries(ctx, renamer(series), series.StartTime(), vals)
		results = append(results, newSeries)
	}
	seriesList.Values = results
	return seriesList, nil
}

func init() {
	intervalNames := make([]string, 0, len(intervals))

	for name := range intervals {
		intervalNames = append(intervalNames, name)
	}

	reInterval = regexp.MustCompile("(?i)([+-]?[0-9]+)(s|min|h|d|w|mon|y)([A-Z]*)")
}
