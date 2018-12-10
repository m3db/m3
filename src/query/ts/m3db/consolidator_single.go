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

package m3db

import (
	"math"
	"time"

	"github.com/m3db/m3/src/dbnode/ts"
)

type singleLookbackConsolidator struct {
	lookbackDuration time.Duration
	stepSize         time.Duration
	earliestLookback time.Time
	consolidated     float64
	datapoints       []ts.Datapoint
	fn               ConsolidationFunc
}

// singleConsolidator is a helper for consolidating a full single series.
// It has some differences with the step consolidator in that it collects
// points for a single series, and is reset when the next series needs
// to be consolidated.
type singleConsolidator interface {
	addPoint(ts.Datapoint)
	consolidate() float64
	empty() bool
	reset(time.Time)
}

func buildSingleConsolidator(
	lookbackDuration, stepSize time.Duration,
	startTime time.Time,
	fn ConsolidationFunc,
) singleConsolidator {
	datapoints := make([]ts.Datapoint, 0, initLength)

	return &singleLookbackConsolidator{
		lookbackDuration: lookbackDuration,
		stepSize:         stepSize,
		earliestLookback: startTime.Add(-1 * lookbackDuration),
		consolidated:     math.NaN(),
		datapoints:       datapoints,
		fn:               fn,
	}
}

func (c *singleLookbackConsolidator) addPoint(
	dp ts.Datapoint,
) {
	if dp.Timestamp.Before(c.earliestLookback) {
		// this datapoint is too far in the past, it can be dropped
		return
	}

	c.datapoints = append(c.datapoints, dp)
}

func (c *singleLookbackConsolidator) consolidate() float64 {
	c.earliestLookback = c.earliestLookback.Add(c.stepSize)
	c.consolidated = c.fn(c.datapoints)
	c.datapoints = removeStale(c.earliestLookback, c.datapoints)
	return c.consolidated
}

func (c *singleLookbackConsolidator) empty() bool {
	return len(c.datapoints) == 0
}

func (c *singleLookbackConsolidator) reset(
	startTime time.Time,
) {
	c.earliestLookback = startTime.Add(-1 * c.lookbackDuration)
	c.datapoints = c.datapoints[:0]
}
