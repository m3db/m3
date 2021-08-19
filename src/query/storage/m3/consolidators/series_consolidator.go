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
	"math"
	"time"

	"github.com/m3db/m3/src/dbnode/ts"
	xtime "github.com/m3db/m3/src/x/time"
)

// SeriesLookbackConsolidator is a helper for consolidating a full single
// series. It has some differences with the step consolidator in that it
// collects points for a single series, and is reset when the next series
// needs to be consolidated.
type SeriesLookbackConsolidator struct {
	lookbackDuration time.Duration
	stepSize         time.Duration
	earliestLookback xtime.UnixNano
	consolidated     float64
	datapoints       []ts.Datapoint
	fn               ConsolidationFunc
}

// NewSeriesLookbackConsolidator creates a single value consolidator used for
// series iteration with a given lookback.
func NewSeriesLookbackConsolidator(
	lookbackDuration, stepSize time.Duration,
	startTime xtime.UnixNano,
	fn ConsolidationFunc,
) *SeriesLookbackConsolidator {
	datapoints := make([]ts.Datapoint, 0, initLength)
	return &SeriesLookbackConsolidator{
		lookbackDuration: lookbackDuration,
		stepSize:         stepSize,
		earliestLookback: startTime.Add(-1 * lookbackDuration),
		consolidated:     math.NaN(),
		datapoints:       datapoints,
		fn:               fn,
	}
}

// AddPoint adds a datapoint if it's within the valid time period;
// otherwise drops it silently, which is fine for consolidation.
func (c *SeriesLookbackConsolidator) AddPoint(
	dp ts.Datapoint,
) {
	if dp.TimestampNanos.Before(c.earliestLookback) {
		// this datapoint is too far in the past, it can be dropped.
		return
	}

	c.datapoints = append(c.datapoints, dp)
}

// ConsolidateAndMoveToNext consolidates the current values and moves the
// consolidator to the next given value, purging stale values.
func (c *SeriesLookbackConsolidator) ConsolidateAndMoveToNext() float64 {
	c.earliestLookback = c.earliestLookback.Add(c.stepSize)
	c.consolidated = c.fn(c.datapoints)
	c.datapoints = removeStale(c.earliestLookback, c.datapoints)

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

	return c.consolidated
}

// Empty returns true if there are no datapoints in the consolidator. It
// is used to let consumers of the consolidators shortcircuit logic.
func (c *SeriesLookbackConsolidator) Empty() bool {
	return len(c.datapoints) == 0
}

// Reset purges all points from the consolidator. This should be called
// when moving to the next series to consolidate.
func (c *SeriesLookbackConsolidator) Reset(
	startTime xtime.UnixNano,
) {
	c.earliestLookback = startTime.Add(-1 * c.lookbackDuration)
	c.datapoints = c.datapoints[:0]
}
