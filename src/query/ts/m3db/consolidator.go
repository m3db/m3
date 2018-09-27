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
	xts "github.com/m3db/m3/src/query/ts"
)

const initLength = 10

func removeStale(
	earliestLookback time.Time,
	dps []ts.Datapoint,
) []ts.Datapoint {
	var (
		i  int
		dp ts.Datapoint
	)
	for _, dp = range dps {
		// Timestamp is within lookback period
		if !dp.Timestamp.Before(earliestLookback) {
			break
		}

		i++
	}

	return dps[i:]
}

type lookbackConsolidator struct {
	lookback         time.Duration
	stepSize         time.Duration
	earliestLookback time.Time
	consolidated     []float64
	datapoints       [][]ts.Datapoint
	fn               ConsolidationFunc
}

type consolidator interface {
	addPointForIterator(ts.Datapoint, int)
	consolidate() []float64
}

func buildConsolidator(
	lookback, stepSize time.Duration,
	startTime time.Time,
	resultSize int,
	fn ConsolidationFunc,
) consolidator {
	consolidated := make([]float64, resultSize)
	xts.Memset(consolidated, math.NaN())
	datapoints := make([][]ts.Datapoint, resultSize)
	for i := range datapoints {
		datapoints[i] = make([]ts.Datapoint, 0, initLength)
	}

	return &lookbackConsolidator{
		lookback:         lookback,
		stepSize:         stepSize,
		earliestLookback: startTime.Add(-1 * lookback),
		consolidated:     consolidated,
		datapoints:       datapoints,
		fn:               fn,
	}
}

func (c *lookbackConsolidator) addPointForIterator(
	dp ts.Datapoint,
	i int,
) {
	if dp.Timestamp.Before(c.earliestLookback) {
		// this datapoint is too far in the past, it can be dropped
		return
	}

	c.datapoints[i] = append(c.datapoints[i], dp)
}

func (c *lookbackConsolidator) consolidate() []float64 {
	c.earliestLookback = c.earliestLookback.Add(c.stepSize)
	for i, dps := range c.datapoints {
		c.consolidated[i] = c.fn(dps)
		c.datapoints[i] = removeStale(c.earliestLookback, dps)
	}

	return c.consolidated
}
