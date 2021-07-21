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

package consolidators

import (
	"time"

	"github.com/m3db/m3/src/dbnode/ts"
	xtime "github.com/m3db/m3/src/x/time"
)

func removeStale(
	earliestLookback xtime.UnixNano,
	dps []ts.Datapoint,
) []ts.Datapoint {
	for i, dp := range dps {
		if !dp.TimestampNanos.Before(earliestLookback) {
			return dps[i:]
		}
	}

	return dps[:0]
}

// StepLookbackConsolidator is a helper for consolidating series in a step-wise
// fashion. It takes a 'step' of values, which represents a vertical
// slice of time across a list of series, and consolidates when a
// valid step has been reached.
type StepLookbackConsolidator struct {
	lookbackDuration time.Duration
	stepSize         time.Duration
	earliestLookback xtime.UnixNano
	datapoints       []ts.Datapoint
	buffer           []float64
	unconsumed       []float64
	fn               ConsolidationFunc
}

// Ensure StepLookbackConsolidator satisfies StepCollector.
var _ StepCollector = (*StepLookbackConsolidator)(nil)

// NewStepLookbackConsolidator creates a multivalue consolidator used for
// step iteration across a series list with a given lookback.
func NewStepLookbackConsolidator(
	lookbackDuration, stepSize time.Duration,
	startTime xtime.UnixNano,
	fn ConsolidationFunc,
) *StepLookbackConsolidator {
	datapoints := make([]ts.Datapoint, 0, initLength)
	buffer := make([]float64, BufferSteps)
	return &StepLookbackConsolidator{
		lookbackDuration: lookbackDuration,
		stepSize:         stepSize,
		earliestLookback: startTime.Add(-1 * lookbackDuration),
		datapoints:       datapoints,
		buffer:           buffer,
		unconsumed:       buffer[:0],
		fn:               fn,
	}
}

// AddPoint adds a datapoint to a given step if it's within the valid
// time period; otherwise drops it silently, which is fine for consolidation.
func (c *StepLookbackConsolidator) AddPoint(dp ts.Datapoint) {
	if dp.TimestampNanos.Before(c.earliestLookback) {
		// this datapoint is too far in the past, it can be dropped.
		return
	}

	c.datapoints = append(c.datapoints, dp)
}

// BufferStep adds viable points to the next unconsumed buffer step.
func (c *StepLookbackConsolidator) BufferStep() {
	c.earliestLookback = c.earliestLookback.Add(c.stepSize)
	val := c.fn(c.datapoints)

	// Remove any datapoints not relevant to the next step now.
	datapointsRelevant := removeStale(c.earliestLookback, c.datapoints)
	if len(datapointsRelevant) > 0 {
		// Move them back to the start of the slice to reuse the slice
		// as best as possible.
		c.datapoints = c.datapoints[:len(datapointsRelevant)]
		copy(c.datapoints, datapointsRelevant)
	} else {
		// No relevant datapoints, repoint to the start of the buffer.
		c.datapoints = c.datapoints[:0]
	}

	// Blindly append to unconsumed.
	c.unconsumed = append(c.unconsumed, val)
}

// BufferStepCount indicates how many accumulated points are still unconsumed.
func (c *StepLookbackConsolidator) BufferStepCount() int {
	return len(c.unconsumed)
}

// ConsolidateAndMoveToNext consolidates the current values and moves the
// consolidator to the next given value, purging stale values.
func (c *StepLookbackConsolidator) ConsolidateAndMoveToNext() float64 {
	if len(c.unconsumed) == 0 {
		return c.fn(nil)
	}

	// Consume value.
	val := c.unconsumed[0]
	remaining := c.unconsumed[1:]

	if len(remaining) > 0 {
		// Move any unconsumed values to the front of unconsumed.
		c.unconsumed = c.buffer[:len(remaining)]
		copy(c.unconsumed, remaining)
	} else {
		// Otherwise just repoint to the start of the buffer.
		c.unconsumed = c.buffer[:0]
	}

	return val
}
