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

package block

import (
	"math"
	"testing"
	"time"

	"github.com/m3db/m3db/src/coordinator/block"
	coordtest "github.com/m3db/m3db/src/coordinator/test"
	"github.com/m3db/m3db/src/dbnode/encoding"
	"github.com/m3db/m3db/src/dbnode/ts"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

var (
	now = time.Now()
	nan = math.NaN()
)

func createDatapoints(t *testing.T, timeInSeconds []int, vals []float64, now time.Time) []ts.Datapoint {
	dps := make([]ts.Datapoint, len(vals))
	for i, val := range vals {
		dps[i] = ts.Datapoint{
			Timestamp: now.Add(time.Duration(timeInSeconds[i]) * time.Second),
			Value:     val,
		}
	}

	return dps
}

func createNSBlockStepIter(iters []encoding.SeriesIterator, start, end time.Time, stepSize time.Duration) nsBlockStepIter {
	return nsBlockStepIter{
		m3dbIters: iters,
		bounds: block.Bounds{
			Start:    start,
			End:      end,
			StepSize: stepSize,
		},
		idx: -1,
	}
}

type nsBlockTestCase struct {
	start, end      time.Time
	stepSize        time.Duration
	dps             [][]ts.Datapoint
	expectedResults []float64
	description     string
}

func TestConsolidatedNSBlockStepIter(t *testing.T) {
	testCases := []nsBlockTestCase{
		{
			dps: [][]ts.Datapoint{
				createDatapoints(t, []int{5, 90, 100, 120, 300}, []float64{1, 2, 3, 4, 5}, now),
			},
			start:           now,
			end:             now.Add(600 * time.Second),
			stepSize:        60 * time.Second,
			expectedResults: []float64{1, 2, 4, nan, nan, 5, nan, nan, nan, nan, nan},
			description:     "testing single iterator with two values in one block (step size)",
		},
		{
			dps: [][]ts.Datapoint{
				createDatapoints(t, []int{90, 100, 120, 300}, []float64{2, 3, 4, 5}, now),
			},
			start:           now,
			end:             now.Add(600 * time.Second),
			stepSize:        60 * time.Second,
			expectedResults: []float64{nan, 2, 4, nan, nan, 5, nan, nan, nan, nan, nan},
			description:     "testing single iterator with no value in first step",
		},
		{
			dps: [][]ts.Datapoint{
				createDatapoints(t, []int{0, 90, 300}, []float64{1, 2, 3}, now),
				createDatapoints(t, []int{1020, 1145}, []float64{4, 5}, now)},
			start:    now,
			end:      now.Add(1200 * time.Second),
			stepSize: 60 * time.Second,
			expectedResults: []float64{1, 2, nan, nan, nan, 3, nan, nan, nan, nan,
				nan, nan, nan, nan, nan, nan, nan, 4, nan, 5, nan},
			description: "testing multiple iterators",
		},
		{
			dps: [][]ts.Datapoint{
				createDatapoints(t, []int{0, 90, 300, 1020, 1140}, []float64{1, 2, 3, 4, 5}, now)},
			start:    now,
			end:      now.Add(1200 * time.Second),
			stepSize: 60 * time.Second,
			expectedResults: []float64{1, 2, nan, nan, nan, 3, nan, nan, nan, nan,
				nan, nan, nan, nan, nan, nan, nan, 4, nan, 5, nan},
			description: "same test as above, but with one iterator",
		},
		{
			dps: [][]ts.Datapoint{
				createDatapoints(t, []int{0, 90, 300}, []float64{1, 2, 3}, now),
				createDatapoints(t, []int{1020, 1140, 5000}, []float64{4, 5, 6}, now)},
			start:    now,
			end:      now.Add(1200 * time.Second),
			stepSize: 60 * time.Second,
			expectedResults: []float64{1, 2, nan, nan, nan, 3, nan, nan, nan, nan,
				nan, nan, nan, nan, nan, nan, nan, 4, nan, 5, nan},
			description: "testing multiple iterators with one exceeding the step bound",
		},
		{
			dps: [][]ts.Datapoint{
				createDatapoints(t, []int{0, 10, 30, 50}, []float64{1, 2, 3, 4}, now)},
			start:           now,
			end:             now.Add(600 * time.Second),
			stepSize:        60 * time.Second,
			expectedResults: []float64{1, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
			description:     "testing multiple values within one step in beginning",
		},
		{
			dps: [][]ts.Datapoint{
				createDatapoints(t, []int{130, 140, 150, 160}, []float64{1, 2, 3, 4}, now)},
			start:           now,
			end:             now.Add(600 * time.Second),
			stepSize:        60 * time.Second,
			expectedResults: []float64{nan, nan, 1, nan, nan, nan, nan, nan, nan, nan, nan},
			description:     "testing multiple values within one step in middle",
		},
	}

	for _, test := range testCases {
		rawTagsOne := []string{"foo", "bar", "same", "tag"}
		ctrl := gomock.NewController(t)
		var iters []encoding.SeriesIterator
		for _, dp := range test.dps {
			iters = append(iters, newMockIterator(t, "test_one", rawTagsOne, ctrl, dp))
		}

		nsBlockStepIter := createNSBlockStepIter(iters, test.start, test.end, test.stepSize)

		var actualResults []float64
		for nsBlockStepIter.Next() {
			actualResults = append(actualResults, nsBlockStepIter.Current())
		}

		assert.Len(t, actualResults, len(test.expectedResults))
		coordtest.EqualsWithNans(t, test.expectedResults, actualResults)
	}
}

// func createNSBlockSeriesIter(iters []encoding.SeriesIterator, start, end time.Time, stepSize time.Duration) nsBlockSeriesIter {
// 	return nsBlockSeriesIter{
// 		m3dbIters: iters,
// 		bounds: block.Bounds{
// 			Start:    start,
// 			End:      end,
// 			StepSize: stepSize,
// 		},
// 		idx: 0, // 0 or -1??
// 	}
// }

// func TestConsolidatedNSBlockSeriesIter(t *testing.T) {
// 	testCases := []nsBlockTestCase{
// 		{
// 			dps: [][]ts.Datapoint{
// 				createDatapoints(t, []int{5, 90, 100, 120, 300}, []float64{1, 2, 3, 4, 5}, now),
// 			},
// 			start:           now,
// 			end:             now.Add(600 * time.Second),
// 			stepSize:        60 * time.Second,
// 			expectedResults: []float64{1, 2, 4, nan, nan, 5, nan, nan, nan, nan, nan},
// 			description:     "testing single iterator with two values in one block (step size)",
// 		},
// 		{
// 			dps: [][]ts.Datapoint{
// 				createDatapoints(t, []int{90, 100, 120, 300}, []float64{2, 3, 4, 5}, now),
// 			},
// 			start:           now,
// 			end:             now.Add(600 * time.Second),
// 			stepSize:        60 * time.Second,
// 			expectedResults: []float64{nan, 2, 4, nan, nan, 5, nan, nan, nan, nan, nan},
// 			description:     "testing single iterator with no value in first step",
// 		},
// 		{
// 			dps: [][]ts.Datapoint{
// 				createDatapoints(t, []int{0, 90, 300}, []float64{1, 2, 3}, now),
// 				createDatapoints(t, []int{1020, 1145}, []float64{4, 5}, now)},
// 			start:    now,
// 			end:      now.Add(1200 * time.Second),
// 			stepSize: 60 * time.Second,
// 			expectedResults: []float64{1, 2, nan, nan, nan, 3, nan, nan, nan, nan,
// 				nan, nan, nan, nan, nan, nan, nan, 4, nan, 5, nan},
// 			description: "testing multiple iterators",
// 		},
// 		{
// 			dps: [][]ts.Datapoint{
// 				createDatapoints(t, []int{0, 90, 300, 1020, 1140}, []float64{1, 2, 3, 4, 5}, now)},
// 			start:    now,
// 			end:      now.Add(1200 * time.Second),
// 			stepSize: 60 * time.Second,
// 			expectedResults: []float64{1, 2, nan, nan, nan, 3, nan, nan, nan, nan,
// 				nan, nan, nan, nan, nan, nan, nan, 4, nan, 5, nan},
// 			description: "same test as above, but with one iterator",
// 		},
// 		{
// 			dps: [][]ts.Datapoint{
// 				createDatapoints(t, []int{0, 90, 300}, []float64{1, 2, 3}, now),
// 				createDatapoints(t, []int{1020, 1140, 5000}, []float64{4, 5, 6}, now)},
// 			start:    now,
// 			end:      now.Add(1200 * time.Second),
// 			stepSize: 60 * time.Second,
// 			expectedResults: []float64{1, 2, nan, nan, nan, 3, nan, nan, nan, nan,
// 				nan, nan, nan, nan, nan, nan, nan, 4, nan, 5, nan},
// 			description: "testing multiple iterators with one exceeding the step bound",
// 		},
// 		{
// 			dps: [][]ts.Datapoint{
// 				createDatapoints(t, []int{0, 10, 30, 50}, []float64{1, 2, 3, 4}, now)},
// 			start:           now,
// 			end:             now.Add(600 * time.Second),
// 			stepSize:        60 * time.Second,
// 			expectedResults: []float64{1, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
// 			description:     "testing multiple values within one step in beginning",
// 		},
// 		{
// 			dps: [][]ts.Datapoint{
// 				createDatapoints(t, []int{130, 140, 150, 160}, []float64{1, 2, 3, 4}, now)},
// 			start:           now,
// 			end:             now.Add(600 * time.Second),
// 			stepSize:        60 * time.Second,
// 			expectedResults: []float64{nan, nan, 1, nan, nan, nan, nan, nan, nan, nan, nan},
// 			description:     "testing multiple values within one step in middle",
// 		},
// 	}

// 	for _, test := range testCases {
// 		rawTagsOne := []string{"foo", "bar", "same", "tag"}
// 		ctrl := gomock.NewController(t)
// 		var iters []encoding.SeriesIterator
// 		for _, dp := range test.dps {
// 			iters = append(iters, newMockIterator(t, "test_one", rawTagsOne, ctrl, dp))
// 		}

// 		nsBlockIter := createNSBlockSeriesIter(iters, test.start, test.end, test.stepSize)

// 		var actualResults []float64
// 		for nsBlockIter.Next() {
// 			actualResults = nsBlockIter.Current()
// 		}

// 		assert.Len(t, actualResults, len(test.expectedResults))
// 		coordtest.EqualsWithNans(t, test.expectedResults, actualResults)
// 	}
// }
