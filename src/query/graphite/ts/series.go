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

package ts

import (
	"errors"
	"math"
	"sort"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/graphite/context"
	"github.com/m3db/m3/src/query/graphite/stats"
)

var (
	// ErrRangeIsInvalid is returned when attempting to slice Series with invalid range
	// endpoints (begin is beyond end).
	ErrRangeIsInvalid = errors.New("requested range is invalid")
)

// An AggregationFunc combines two data values at a given point.
type AggregationFunc func(a, b float64) float64

// A Series is the public interface to a block of timeseries values.  Each block has a start time,
// a logical number of steps, and a step size indicating the number of milliseconds represented by each point.
type Series struct {
	name      string
	startTime time.Time
	vals      Values
	ctx       context.Context

	// The Specification is the path that was used to generate this timeseries,
	// typically either the query, or the function stack used to transform
	// specific results.
	Specification string

	// consolidationFunc specifies how the series will be consolidated when the
	// number of data points in the series is more than the maximum number allowed.
	consolidationFunc ConsolidationFunc
}

// SeriesByName implements sort.Interface for sorting collections of series by name
type SeriesByName []*Series

// Len returns the length of the series collection
func (a SeriesByName) Len() int { return len(a) }

// Swap swaps two series in the collection
func (a SeriesByName) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Less determines if a series is ordered before another series by name
func (a SeriesByName) Less(i, j int) bool { return a[i].name < a[j].name }

// NewSeries creates a new Series at a given start time, backed by the provided values
func NewSeries(ctx context.Context, name string, startTime time.Time, vals Values) *Series {
	return &Series{
		name:          name,
		startTime:     startTime,
		vals:          vals,
		ctx:           ctx,
		Specification: name,
	}
}

// DerivedSeries returns a series derived from the current series with different datapoints
func (b *Series) DerivedSeries(startTime time.Time, vals Values) *Series {
	series := NewSeries(b.ctx, b.name, startTime, vals)
	series.Specification = b.Specification
	series.consolidationFunc = b.consolidationFunc
	return series
}

// Name returns the name of the timeseries block
func (b *Series) Name() string { return b.name }

// RenamedTo returns a new timeseries with the same values but a different name
func (b *Series) RenamedTo(name string) *Series {
	return &Series{
		name:              name,
		startTime:         b.startTime,
		vals:              b.vals,
		ctx:               b.ctx,
		Specification:     b.Specification,
		consolidationFunc: b.consolidationFunc,
	}
}

// Shift returns a new timeseries with the same values but a different startTime
func (b *Series) Shift(shift time.Duration) *Series {
	return &Series{
		name:              b.name,
		startTime:         b.startTime.Add(shift),
		vals:              b.vals,
		ctx:               b.ctx,
		Specification:     b.Specification,
		consolidationFunc: b.consolidationFunc,
	}
}

// StartTime returns the time the block starts
func (b *Series) StartTime() time.Time { return b.startTime }

// EndTime returns the time the block ends
func (b *Series) EndTime() time.Time { return b.startTime.Add(b.Duration()) }

// Duration returns the Duration covered by the block
func (b *Series) Duration() time.Duration {
	return time.Millisecond * time.Duration(b.vals.Len()*b.vals.MillisPerStep())
}

// MillisPerStep returns the number of milliseconds per step
func (b *Series) MillisPerStep() int { return b.vals.MillisPerStep() }

// Resolution returns resolution per step
func (b *Series) Resolution() time.Duration {
	return time.Duration(b.MillisPerStep()) * time.Millisecond
}

// StepAtTime returns the step within the block containing the given time
func (b *Series) StepAtTime(t time.Time) int {
	step := int(t.UnixNano()/1000000-b.startTime.UnixNano()/1000000) / b.vals.MillisPerStep()
	if step < 0 {
		return 0
	}

	return step
}

// StartTimeForStep returns the time at which the given step starts
func (b *Series) StartTimeForStep(n int) time.Time {
	return b.StartTime().Add(time.Millisecond * time.Duration(n*b.vals.MillisPerStep()))
}

// EndTimeForStep returns the time at which the given step end
func (b *Series) EndTimeForStep(n int) time.Time {
	return b.StartTimeForStep(n).Add(time.Millisecond * time.Duration(b.vals.MillisPerStep()))
}

// Slice returns a new Series composed from a subset of values in the original Series
func (b *Series) Slice(begin, end int) (*Series, error) {
	if begin >= end {
		return nil, ErrRangeIsInvalid
	}

	result := NewSeries(b.ctx, b.name, b.StartTimeForStep(begin), b.vals.Slice(begin, end))
	result.consolidationFunc = b.consolidationFunc

	return result, nil
}

// ValueAtTime returns the value stored at the step representing the given time
func (b *Series) ValueAtTime(t time.Time) float64 {
	return b.ValueAt(b.StepAtTime(t))
}

// AllNaN returns true if the timeseries is all NaNs
func (b *Series) AllNaN() bool { return b.vals.AllNaN() }

// CalcStatistics calculates a standard aggregation across the block values
func (b *Series) CalcStatistics() stats.Statistics {
	if agg, ok := b.vals.(CustomStatistics); ok {
		return agg.CalcStatistics()
	}

	return stats.Calc(b)
}

// Contains checks whether the given series contains the provided time
func (b *Series) Contains(t time.Time) bool {
	step := b.StepAtTime(t)
	return step >= 0 && step < b.Len()
}

// Len returns the number of values in the time series.  Used for aggregation
func (b *Series) Len() int { return b.vals.Len() }

// ValueAt returns the value at a given step.  Used for aggregation
func (b *Series) ValueAt(i int) float64 { return b.vals.ValueAt(i) }

// SafeMax returns the maximum value of a series that's not an NaN.
func (b *Series) SafeMax() float64 { return b.CalcStatistics().Max }

// SafeMin returns the minimum value of a series that's not an NaN.
func (b *Series) SafeMin() float64 { return b.CalcStatistics().Min }

// SafeSum returns the sum of the values of a series, excluding NaNs.
func (b *Series) SafeSum() float64 { return b.CalcStatistics().Sum }

// SafeAvg returns the average of the values of a series, excluding NaNs.
func (b *Series) SafeAvg() float64 { return b.CalcStatistics().Mean }

// SafeStdDev returns the standard deviation of the values of a series, excluding NaNs.
func (b *Series) SafeStdDev() float64 { return b.CalcStatistics().StdDev }

// SafeLastValue returns the last datapoint of a series that's not an NaN.
func (b *Series) SafeLastValue() float64 {
	numPoints := b.Len()
	for i := numPoints - 1; i >= 0; i-- {
		v := b.ValueAt(i)
		if !math.IsNaN(v) {
			return v
		}
	}
	return math.NaN()
}

// SafeValues returns all non-NaN values in the series.
func (b *Series) SafeValues() []float64 {
	numPoints := b.Len()
	vals := make([]float64, 0, numPoints)
	for i := 0; i < numPoints; i++ {
		v := b.ValueAt(i)
		if !math.IsNaN(v) {
			vals = append(vals, v)
		}
	}
	return vals
}

// ConsolidationFunc returns the consolidation function for the series,
// or the averaging function is none specified.
func (b *Series) ConsolidationFunc() ConsolidationFunc {
	if b.consolidationFunc != nil {
		return b.consolidationFunc
	}
	return Avg
}

// IsConsolidationFuncSet if the consolidationFunc is set
func (b *Series) IsConsolidationFuncSet() bool {
	return b.consolidationFunc != nil
}

// SetConsolidationFunc sets the consolidation function for the series
func (b *Series) SetConsolidationFunc(cf ConsolidationFunc) {
	b.consolidationFunc = cf
}

// PostConsolidationFunc is a function that takes a tuple of time and value after consolidation.
type PostConsolidationFunc func(timestamp time.Time, value float64)

// intersection returns a 3-tuple; First return parameter indicates if the intersection spans at
// least one nanosecond; the next two return parameters are the start and end boundary timestamps
// of the resulting overlap.
func (b *Series) intersection(start, end time.Time) (bool, time.Time, time.Time) {
	if b.EndTime().Before(start) || b.StartTime().After(end) {
		return false, start, end
	}
	if start.Before(b.StartTime()) {
		start = b.StartTime()
	}
	if end.After(b.EndTime()) {
		end = b.EndTime()
	}
	if start.Equal(end) {
		return false, start, end
	}
	return true, start, end
}

// resize takes a time series and returns a new time series of a different step size with aggregated
// values; callers must provide callback method that collects the aggregated result
func (b *Series) resizeStep(start, end time.Time, millisPerStep int,
	stepAggregator ConsolidationFunc, callback PostConsolidationFunc) {
	// panic, panic, panic for all malformed callers
	if end.Before(start) || start.Before(b.StartTime()) || end.After(b.EndTime()) {
		panic("invalid boundary params")
	}
	if b.MillisPerStep() == millisPerStep {
		panic("requires different step size")
	}
	if b.MillisPerStep() < millisPerStep {
		// Series step size is smaller than consolidation - aggregate each series step then apply
		// the agggregated value to the consolidate.
		seriesValuesPerStep := millisPerStep / b.MillisPerStep()
		seriesStart, seriesEnd := b.StepAtTime(start), b.StepAtTime(end)
		for n := seriesStart; n < seriesEnd; n += seriesValuesPerStep {
			timestamp := b.StartTimeForStep(n)
			aggregatedValue := math.NaN()
			count := 0

			for i := 0; i < seriesValuesPerStep && n+i < seriesEnd; i++ {
				value := b.ValueAt(n + i)
				aggregatedValue, count = consolidateValues(aggregatedValue, value, count,
					stepAggregator)
			}
			callback(timestamp, aggregatedValue)
		}
		return
	}
}

// resized implements PostConsolidationFunc.
type resized struct {
	values []float64
}

// appender adds new values to resized.values.
func (v *resized) appender(timestamp time.Time, value float64) {
	v.values = append(v.values, value)
}

// IntersectAndResize returns a new time series with a different millisPerStep that spans the
// intersection of the underlying timeseries and the provided start and end time parameters
func (b *Series) IntersectAndResize(start, end time.Time, millisPerStep int,
	stepAggregator ConsolidationFunc) (*Series, error) {
	intersects, start, end := b.intersection(start, end)
	if !intersects {
		ts := NewSeries(b.ctx, b.name, start, &float64Values{
			millisPerStep: millisPerStep,
			values:        []float64{},
			numSteps:      0,
		})
		ts.Specification = b.Specification
		return ts, nil
	}
	if b.MillisPerStep() == millisPerStep {
		return b.Slice(b.StepAtTime(start), b.StepAtTime(end))
	}

	// TODO: This append based model completely screws pooling; need to rewrite to allow for pooling.
	v := &resized{}
	b.resizeStep(start, end, millisPerStep, stepAggregator, v.appender)
	ts := NewSeries(b.ctx, b.name, start, &float64Values{
		millisPerStep: millisPerStep,
		values:        v.values,
		numSteps:      len(v.values),
	})
	ts.Specification = b.Specification
	return ts, nil
}

// A MutableSeries is a Series that allows updates
type MutableSeries struct {
	Series
}

// NewMutableSeries returns a new mutable Series at the
// given start time and backed by the provided storage
func NewMutableSeries(
	ctx context.Context,
	name string,
	startTime time.Time,
	vals MutableValues) *MutableSeries {
	return &MutableSeries{
		Series{
			name:          name,
			startTime:     startTime,
			vals:          vals,
			ctx:           ctx,
			Specification: name,
		},
	}
}

// SetValueAt sets the value at the given step
func (b *MutableSeries) SetValueAt(i int, v float64) {
	b.vals.(MutableValues).SetValueAt(i, v)
}

// SetValueAtTime sets the value at the step containing the given time
func (b *MutableSeries) SetValueAtTime(t time.Time, v float64) {
	b.SetValueAt(b.StepAtTime(t), v)
}

// A Consolidation produces a Series whose values are the result of applying a consolidation
// function to all of the datapoints that fall within each step.  It can used to quantize raw
// datapoints into a given resolution, for example, or to aggregate multiple timeseries at the
// same or smaller resolutions.
type Consolidation interface {
	// AddDatapoint adds an individual datapoint to the consolidation.
	AddDatapoint(timestamp time.Time, value float64)

	// AddDatapoints adds a set of datapoints to the consolidation.
	AddDatapoints(datapoints []Datapoint)

	// AddSeries adds the datapoints for each series to the consolidation.  The
	// stepAggregationFunc is used to combine values from the series if the series
	// has a smaller step size than the consolidation.  For example, an application
	// might want to produce a consolidation which is a minimum of the input timeseries,
	// but where the values in smaller timeseries units are summed together to
	// produce the value to which the consolidation applies.
	// To put it in another way, stepAggregationFunc is used for the series to resize itself
	// rather than for the consolidation
	AddSeries(series *Series, stepAggregationFunc ConsolidationFunc)

	// BuildSeries returns the consolidated Series and optionally finalizes
	// the consolidation returning it to the pool
	BuildSeries(id string, finalize FinalizeOption) *Series

	// Finalize returns the consolidation to the pool
	Finalize()
}

// FinalizeOption specifies the option to finalize or avoid finalizing
type FinalizeOption int

const (
	// NoFinalize will avoid finalizing the subject
	NoFinalize FinalizeOption = iota
	// Finalize will finalize the subject
	Finalize
)

// A ConsolidationFunc consolidates values at a given point in time.  It takes the current consolidated
// value, the new value to add to the consolidation, and a count of the number of values that have
// already been consolidated.
type ConsolidationFunc func(existing, toAdd float64, count int) float64

// NewConsolidation creates a new consolidation window.
func NewConsolidation(
	ctx context.Context,
	start, end time.Time,
	millisPerStep int,
	cf ConsolidationFunc,
) Consolidation {
	var (
		numSteps = NumSteps(start, end, millisPerStep)
		values   = NewValues(ctx, millisPerStep, numSteps)
		c        *consolidation
		pooled   = false
	)

	if consolidationPools != nil {
		temp := consolidationPools.Get(numSteps)
		c = temp.(*consolidation)
		if cap(c.counts) >= numSteps {
			c.counts = c.counts[:numSteps]
			for i := range c.counts {
				c.counts[i] = 0
			}
			pooled = true
		}
	}

	if !pooled {
		c = newConsolidation(numSteps)
	}

	c.ctx = ctx
	c.start = start
	c.end = end
	c.millisPerStep = millisPerStep
	c.values = values
	c.f = cf

	return c
}

func newConsolidation(numSteps int) *consolidation {
	counts := make([]int, numSteps)
	return &consolidation{
		counts: counts,
	}
}

type consolidation struct {
	ctx           context.Context
	start         time.Time
	end           time.Time
	millisPerStep int
	values        MutableValues
	counts        []int
	f             ConsolidationFunc
}

func (c *consolidation) AddDatapoints(datapoints []Datapoint) {
	for _, datapoint := range datapoints {
		c.AddDatapoint(datapoint.Timestamp, datapoint.Value)
	}
}

func (c *consolidation) AddDatapoint(timestamp time.Time, value float64) {
	if timestamp.Before(c.start) || timestamp.After(c.end) {
		return
	}

	if math.IsNaN(value) {
		return
	}

	step := int(timestamp.UnixNano()/1000000-c.start.UnixNano()/1000000) / c.millisPerStep
	if step >= c.values.Len() {
		return
	}

	n, count := consolidateValues(c.values.ValueAt(step), value, c.counts[step], c.f)
	c.counts[step] = count
	c.values.SetValueAt(step, n)
}

func consolidateValues(current, value float64, count int, f ConsolidationFunc) (float64, int) {
	if math.IsNaN(value) {
		return current, count
	}

	if count == 0 {
		return value, 1
	}

	return f(current, value, count), count + 1
}

// AddSeries adds a time series to the consolidation; stepAggregator is used to resize the
// provided timeseries if it's step size is different from the consolidator's step size.
func (c *consolidation) AddSeries(series *Series, stepAggregator ConsolidationFunc) {
	if series.AllNaN() {
		return
	}

	intersects, start, end := series.intersection(c.start, c.end)
	if !intersects {
		// Nothing to do.
		return
	}

	if series.MillisPerStep() == c.millisPerStep {
		// Series step size is identical to the consolidation: simply apply each series value to
		// the consolidation.
		startIndex := series.StepAtTime(start)
		endIndex := int(math.Min(float64(series.StepAtTime(end)), float64(series.Len()-1)))
		for n := startIndex; n <= endIndex; n++ {
			c.AddDatapoint(series.StartTimeForStep(n), series.ValueAt(n))
		}
		return
	}
	series.resizeStep(start, end, c.millisPerStep, stepAggregator, c.AddDatapoint)
}

func (c *consolidation) BuildSeries(id string, f FinalizeOption) *Series {
	series := NewSeries(c.ctx, id, c.start, c.values)
	if f == Finalize {
		c.Finalize()
	}
	return series
}

func (c *consolidation) Finalize() {
	c.ctx = nil
	c.start = time.Time{}
	c.end = time.Time{}
	c.millisPerStep = 0
	c.values = nil
	c.f = nil
	if consolidationPools == nil {
		return
	}
	consolidationPools.Put(c, cap(c.counts))
}

// NumSteps calculates the number of steps of a given size between two times.
func NumSteps(start, end time.Time, millisPerStep int) int {
	// We should round up.
	numSteps := int(math.Ceil(float64(
		end.Sub(start)/time.Millisecond) / float64(millisPerStep)))

	if numSteps > 0 {
		return numSteps
	}

	// Even for intervals less than millisPerStep, there should be at least one step.
	return 1
}

// Sum sums two values.
func Sum(a, b float64, count int) float64 { return a + b }

// Mul multiplies two values.
func Mul(a, b float64, count int) float64 { return a * b }

// Avg produces a running average.
func Avg(a, b float64, count int) float64 { return (a*float64(count) + b) / float64(count+1) }

// Min finds the min of two values.
func Min(a, b float64, count int) float64 { return math.Min(a, b) }

// Max finds the max of two values.
func Max(a, b float64, count int) float64 { return math.Max(a, b) }

// Last finds the latter of two values.
func Last(a, b float64, count int) float64 { return b }

// Pow returns the first value to the power of the second value
func Pow(a, b float64, count int) float64 { return math.Pow(a, b) }

// Median finds the median of a slice of values.
func Median(vals []float64, count int) float64 {
	if count < 1 {
		return math.NaN()
	}
	if count == 1 {
		return vals[0]
	}
	sort.Float64s(vals)
	if count%2 != 0 {
		// if count is odd
		return vals[(count-1)/2]
	}
	// if count is even
	return (vals[count/2] + vals[(count/2)-1]) / 2.0
}

// Gcd finds the gcd of two values.
func Gcd(a, b int64) int64 {
	if a < 0 {
		a = -a
	}

	if b < 0 {
		b = -b
	}

	if b == 0 {
		return a
	}

	return Gcd(b, a%b)
}

// Lcm finds the lcm of two values.
func Lcm(a, b int64) int64 {
	if a < 0 {
		a = -a
	}

	if b < 0 {
		b = -b
	}

	if a == b {
		return a
	}

	if a < b {
		a, b = b, a
	}

	return a / Gcd(a, b) * b
}

// A SeriesList is a list of series.
type SeriesList struct {
	// Values is the list of series.
	Values []*Series
	// SortApplied specifies whether a specific sort order has been applied.
	SortApplied bool
	// Metadata contains any additional metadata indicating information about
	// series execution.
	Metadata block.ResultMetadata
}

// NewSeriesList creates a blank series list.
func NewSeriesList() SeriesList {
	return SeriesList{Metadata: block.NewResultMetadata()}
}

// NewSeriesListWithSeries creates a series list with the given series and
// default metadata.
func NewSeriesListWithSeries(values ...*Series) SeriesList {
	return SeriesList{
		Values:   values,
		Metadata: block.NewResultMetadata(),
	}
}

// Len returns the length of the list.
func (l SeriesList) Len() int {
	return len(l.Values)
}
