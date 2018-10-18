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

	"github.com/m3db/m3/src/query/models"
)

// Values holds the values for a timeseries.  It provides a minimal interface
// for storing and retrieving values in the series, with Series providing a
// more convenient interface for applications to build on top of.  Values
// objects are not specific to a given time, allowing them to be
// pre-allocated, pooled, and re-used across multiple series.  There are
// multiple implementations of Values so that we can optimize storage based on
// the density of the series.
type Values interface {
	// Len returns the number of values present
	Len() int

	// ValueAt returns the value at the nth element
	ValueAt(n int) float64

	// DatapointAt returns the datapoint at the nth element
	DatapointAt(n int) Datapoint

	// Datapoints returns all the datapoints
	Datapoints() []Datapoint

	// AlignToBounds returns values aligned to the start time and duration
	AlignToBounds(bounds models.Bounds) []Datapoints
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

// Datapoints returns all the datapoints.
func (d Datapoints) Datapoints() []Datapoint { return d }

// Values returns the values representation.
func (d Datapoints) Values() []float64 {
	values := make([]float64, len(d))
	for i, dp := range d {
		values[i] = dp.Value
	}

	return values
}

// AlignToBounds returns values aligned to given bounds. To belong to a step, values should be <= stepTime and not stale
func (d Datapoints) AlignToBounds(bounds models.Bounds) []Datapoints {
	numDatapoints := d.Len()
	steps := bounds.Steps()
	stepValues := make([]Datapoints, steps)
	dpIdx := 0
	stepSize := bounds.StepSize
	t := bounds.Start
	for i := 0; i < steps; i++ {
		singleStepValues := make(Datapoints, 0)

		for dpIdx < numDatapoints && !d[dpIdx].Timestamp.After(t) {
			point := d[dpIdx]
			// Skip stale values
			if t.Sub(point.Timestamp) > models.LookbackDelta {
				continue
			}

			singleStepValues = append(singleStepValues, point)
			dpIdx++
		}

		// If no point found for this interval, reuse the last point as long as its not stale
		if len(singleStepValues) == 0 && dpIdx > 0 {
			prevPoint := d[dpIdx-1]
			if t.Sub(prevPoint.Timestamp) <= models.LookbackDelta {
				singleStepValues = Datapoints{prevPoint}
			}
		}

		stepValues[i] = singleStepValues
		t = t.Add(stepSize)
	}

	return stepValues
}

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
}

type fixedResolutionValues struct {
	resolution time.Duration
	numSteps   int
	values     []float64
	startTime  time.Time
}

func (b *fixedResolutionValues) Len() int                  { return b.numSteps }
func (b *fixedResolutionValues) ValueAt(point int) float64 { return b.values[point] }
func (b *fixedResolutionValues) DatapointAt(point int) Datapoint {
	return Datapoint{
		Timestamp: b.StartTimeForStep(point),
		Value:     b.ValueAt(point),
	}
}
func (b *fixedResolutionValues) Datapoints() []Datapoint {
	datapoints := make([]Datapoint, 0, len(b.values))
	for i := range b.values {
		datapoints = append(datapoints, b.DatapointAt(i))
	}
	return datapoints
}

// AlignToBounds returns values aligned to given bounds.
// TODO: Consider bounds as well
func (b *fixedResolutionValues) AlignToBounds(_ models.Bounds) []Datapoints {
	values := make([]Datapoints, len(b.values))
	for i := 0; i < b.Len(); i++ {
		values[i] = Datapoints{b.DatapointAt(i)}
	}

	return values
}

// StartTime returns the time the values start
func (b *fixedResolutionValues) StartTime() time.Time {
	return b.startTime
}

// Resolution returns resolution per step
func (b *fixedResolutionValues) Resolution() time.Duration {
	return b.resolution
}

// StepAtTime returns the step within the block containing the given time
func (b *fixedResolutionValues) StepAtTime(t time.Time) int {
	return int(t.Sub(b.StartTime()) / b.Resolution())
}

// StartTimeForStep returns the time at which the given step starts
func (b *fixedResolutionValues) StartTimeForStep(n int) time.Time {
	return b.startTime.Add(time.Duration(n) * b.Resolution())
}

// SetValueAt sets the value at the given entry
func (b *fixedResolutionValues) SetValueAt(n int, v float64) {
	b.values[n] = v
}

// NewFixedStepValues returns mutable values with fixed resolution
func NewFixedStepValues(resolution time.Duration, numSteps int, initialValue float64, startTime time.Time) FixedResolutionMutableValues {
	return newFixedStepValues(resolution, numSteps, initialValue, startTime)
}

func newFixedStepValues(resolution time.Duration, numSteps int, initialValue float64, startTime time.Time) *fixedResolutionValues {
	values := make([]float64, numSteps)
	// Faster way to initialize an array instead of a loop
	Memset(values, initialValue)
	return &fixedResolutionValues{
		resolution: resolution,
		numSteps:   numSteps,
		startTime:  startTime,
		values:     values,
	}
}
