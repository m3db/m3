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

package consolidators

import (
	"time"

	"github.com/m3db/m3/src/dbnode/ts"
	xts "github.com/m3db/m3/src/query/ts"
	xtime "github.com/m3db/m3/src/x/time"
)

// StepLookbackAccumulator is a helper for accumulating series in a step-wise
// fashion. It takes a 'step' of values, which represents a vertical
// slice of time across a list of series, and accumulates them when a
// valid step has been reached.
type StepLookbackAccumulator struct {
	lookbackDuration time.Duration
	stepSize         time.Duration
	earliestLookback xtime.UnixNano
	datapoints       []xts.Datapoint
	buffer           [][]xts.Datapoint
	unconsumed       [][]xts.Datapoint
}

// Ensure StepLookbackAccumulator satisfies StepCollector.
var _ StepCollector = (*StepLookbackAccumulator)(nil)

// NewStepLookbackAccumulator creates an accumulator used for
// step iteration across a series list with a given lookback.
func NewStepLookbackAccumulator(
	lookbackDuration, stepSize time.Duration,
	startTime xtime.UnixNano,
) *StepLookbackAccumulator {
	datapoints := make([]xts.Datapoint, 0, initLength)
	buffer := make([][]xts.Datapoint, BufferSteps)
	return &StepLookbackAccumulator{
		lookbackDuration: lookbackDuration,
		stepSize:         stepSize,
		earliestLookback: startTime.Add(-1 * lookbackDuration),
		datapoints:       datapoints,
		buffer:           buffer,
		unconsumed:       buffer[:0],
	}
}

// AddPoint adds a datapoint to a given step if it's within the valid
// time period; otherwise drops it silently, which is fine for accumulation.
func (c *StepLookbackAccumulator) AddPoint(dp ts.Datapoint) {
	if dp.TimestampNanos.Before(c.earliestLookback) {
		// this datapoint is too far in the past, it can be dropped.
		return
	}

	c.datapoints = append(c.datapoints, xts.Datapoint{
		Timestamp: dp.TimestampNanos,
		Value:     dp.Value,
	})
}

// BufferStep adds viable points to the next unconsumed buffer step.
func (c *StepLookbackAccumulator) BufferStep() {
	// Update earliest lookback then remove stale values for the next
	// iteration of the datapoint set.
	c.earliestLookback = c.earliestLookback.Add(c.stepSize)
	if len(c.datapoints) == 0 {
		c.unconsumed = append(c.unconsumed, nil)
		return
	}

	accumulated := make([]xts.Datapoint, len(c.datapoints))
	copy(accumulated, c.datapoints)
	c.datapoints = c.datapoints[:0]
	c.unconsumed = append(c.unconsumed, accumulated)
}

// BufferStepCount indicates how many accumulated points are still unconsumed.
func (c *StepLookbackAccumulator) BufferStepCount() int {
	return len(c.unconsumed)
}

// AccumulateAndMoveToNext consolidates the current values and moves the
// consolidator to the next given value, purging stale values.
func (c *StepLookbackAccumulator) AccumulateAndMoveToNext() []xts.Datapoint {
	if len(c.unconsumed) == 0 {
		return nil
	}

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
