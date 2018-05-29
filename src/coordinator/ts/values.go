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

package ts

import (
	"time"
	"math"
	"fmt"
)

// Values holds the values for a timeseries.  It provides a minimal interface
// for storing and retrieving values in the series, with Series providing a
// more convenient interface for applications to build on top of.  Values
// objects are not specific to a given time, allowing them to be
// pre-allocated, pooled, and re-used across multiple Series.  There are
// multiple implementations of Values so that we can optimize storage based on
// the density of the series.
type Values interface {
	// Len returns the number of values present
	Len() int

	// ValueAt returns the value at the nth element
	ValueAt(n int) float64

	// DatapointAt returns the datapoint at the nth element
	DatapointAt(n int) Datapoint
}

// A Datapoint is a single data value reported at a given time
type Datapoint struct {
	Timestamp time.Time
	Value     float64
}

// Datapoints is a list of datapoints.
type Datapoints []Datapoint

// Len is the length of the array.
func (d Datapoints) Len() int { return len(d) }

// ValueAt returns the value at the nth element.
func (d Datapoints) ValueAt(n int) float64 { return d[n].Value }

// DatapointAt returns the value at the nth element.
func (d Datapoints) DatapointAt(n int) Datapoint { return d[n] }

// MutableValues is the interface for values that can be updated
type MutableValues interface {
	Values

	// Sets the value at the given entry
	SetValueAt(n int, v float64)
}

// FixedResolutionMutableValues are mutable values with fixed resolution between steps
type FixedResolutionMutableValues interface {
	MutableValues
	Resolution() time.Duration
	StepAtTime(t time.Time) int
	StartTimeForStep(n int) time.Time
	// Time when the series starts
	StartTime() time.Time
	MillisPerStep() time.Duration
}

type fixedResolutionValues struct {
	millisPerStep time.Duration
	numSteps      int
	values        []float64
	startTime     time.Time
}

func (b *fixedResolutionValues) MillisPerStep() time.Duration { return b.millisPerStep }
func (b *fixedResolutionValues) Len() int                     { return b.numSteps }
func (b *fixedResolutionValues) ValueAt(point int) float64    { return b.values[point] }
func (b *fixedResolutionValues) DatapointAt(point int) Datapoint {
	return Datapoint{
		Timestamp: b.StartTimeForStep(point),
		Value:     b.ValueAt(point),
	}
}

// StartTime returns the time the values start
func (b *fixedResolutionValues) StartTime() time.Time {
	return b.startTime
}

// Resolution returns resolution per step
func (b *fixedResolutionValues) Resolution() time.Duration {
	return b.MillisPerStep()
}

// StepAtTime returns the step within the block containing the given time
func (b *fixedResolutionValues) StepAtTime(t time.Time) int {
	return int(t.Sub(b.StartTime()) / b.MillisPerStep())
}

// StartTimeForStep returns the time at which the given step starts
func (b *fixedResolutionValues) StartTimeForStep(n int) time.Time {
	return b.startTime.Add(time.Duration(n) * b.MillisPerStep())
}

// SetValueAt sets the value at the given entry
func (b *fixedResolutionValues) SetValueAt(n int, v float64) {
	b.values[n] = v
}

// NewFixedStepValues returns mutable values with fixed resolution
func NewFixedStepValues(millisPerStep time.Duration, numSteps int, initialValue float64, startTime time.Time) FixedResolutionMutableValues {
	return newFixedStepValues(millisPerStep, numSteps, initialValue, startTime)
}

func newFixedStepValues(millisPerStep time.Duration, numSteps int, initialValue float64, startTime time.Time) *fixedResolutionValues {
	values := make([]float64, numSteps)
	// Faster way to initialize an array instead of a loop
	Memset(values, initialValue)
	return &fixedResolutionValues{
		millisPerStep: millisPerStep,
		numSteps:      numSteps,
		startTime:     startTime,
		values:        values,
	}
}

// At every resolution timestep, the last sample value is chosen
func RawPointsToFixedStep(datapoints Datapoints, start time.Time, end time.Time, interval time.Duration) (FixedResolutionMutableValues, error) {
	if end.Before(start) {
		return nil, fmt.Errorf("start cannot be after end, start: %v, end: %v", start, end)
	}
	numSteps := int(end.Sub(start) / interval)
	if end == start {
		numSteps = 1
	}
	fixStepValues := newFixedStepValues(interval, numSteps, math.NaN(), start)
	currIdx := 0
	idx := 0
	for t := start; !t.After(end) && currIdx < numSteps; t = t.Add(interval) {
		// Find first datapoint not before time t
		for ; idx < len(datapoints) && datapoints.DatapointAt(idx).Timestamp.Before(t); idx++ {
		}

		if idx >= len(datapoints) {
			fixStepValues.values[currIdx] = math.NaN()
			currIdx++
			continue
		}

		// If datapoint aligns to the time or its the first datapoint then take that
		if datapoints.DatapointAt(idx).Timestamp == t || idx == 0 {
			fixStepValues.values[currIdx] = datapoints.ValueAt(idx)
			currIdx++
		} else {
			fixStepValues.values[currIdx] = datapoints.ValueAt(idx - 1)
			currIdx++
		}
	}

	return fixStepValues, nil
}
