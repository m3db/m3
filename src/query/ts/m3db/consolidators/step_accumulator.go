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
)

func removeStaleAccumulated(
	earliestLookback time.Time,
	dps []xts.Datapoint,
) []xts.Datapoint {
	for i, dp := range dps {
		if !dp.Timestamp.Before(earliestLookback) {
			return dps[i:]
		}
	}

	return dps[:0]
}

// StepLookbackAccumulator is a helper for accumulating series in a step-wise
// fashion. It takes a 'step' of values, which represents a vertical
// slice of time across a list of series, and accumulates them when a
// valid step has been reached.
type StepLookbackAccumulator struct {
	lookbackDuration time.Duration
	stepSize         time.Duration
	earliestLookback time.Time
	datapoints       [][]xts.Datapoint
	reset            []bool
}

// NewStepLookbackAccumulator creates an accumulator used for
// step iteration across a series list with a given lookback.
func NewStepLookbackAccumulator(
	lookbackDuration, stepSize time.Duration,
	startTime time.Time,
	resultSize int,
) *StepLookbackAccumulator {
	datapoints := make([][]xts.Datapoint, resultSize)
	for i := range datapoints {
		datapoints[i] = make([]xts.Datapoint, 0, initLength)
	}

	reset := make([]bool, resultSize)
	return &StepLookbackAccumulator{
		lookbackDuration: lookbackDuration,
		stepSize:         stepSize,
		earliestLookback: startTime.Add(-1 * lookbackDuration),
		datapoints:       datapoints,
		reset:            reset,
	}
}

// AddPointForIterator adds a datapoint to a given step if it's within the valid
// time period; otherwise drops it silently, which is fine for accumulation.
func (c *StepLookbackAccumulator) AddPointForIterator(
	dp ts.Datapoint,
	i int,
) {
	if dp.Timestamp.Before(c.earliestLookback) {
		// this datapoint is too far in the past, it can be dropped.
		return
	}

	// TODO: the existing version of the step accumulator in Values.AlignToBounds
	// resets incoming data points after accumulation; i.e. it will only keep
	// points in the accumulation buffer if no other point comes in. This may not
	// be the correct behaviour; investigate if it should be converted to keep
	// the values instead.
	if c.reset[i] {
		c.datapoints[i] = c.datapoints[i][:0]
	}

	c.reset[i] = false
	c.datapoints[i] = append(c.datapoints[i], xts.Datapoint{
		Timestamp: dp.Timestamp,
		Value:     dp.Value,
	})
}

// AccumulateAndMoveToNext consolidates the current values and moves the
// consolidator to the next given value, purging stale values.
func (c *StepLookbackAccumulator) AccumulateAndMoveToNext() []xts.Datapoints {
	// Update earliest lookback then remove stale values for the next
	// iteration of the datapoint set.
	c.earliestLookback = c.earliestLookback.Add(c.stepSize)
	accumulated := make([]xts.Datapoints, len(c.datapoints))
	for i, dps := range c.datapoints {
		accumulated[i] = make(xts.Datapoints, len(dps))
		copy(accumulated[i], dps)
		c.datapoints[i] = removeStaleAccumulated(c.earliestLookback, dps)
		c.reset[i] = true
	}

	return accumulated
}
