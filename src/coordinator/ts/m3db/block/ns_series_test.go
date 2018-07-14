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

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3db/src/coordinator/block"
	coordtest "github.com/m3db/m3db/src/coordinator/test"
	"github.com/m3db/m3db/src/dbnode/encoding"
	"github.com/m3db/m3db/src/dbnode/ts"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	now = time.Now()
	nan = math.NaN()
)

func createDatapoints(t *testing.T, timeInSeconds []int, vals []float64, now time.Time) []ts.Datapoint {
	if len(timeInSeconds) != len(vals) {
		require.Equal(t, len(timeInSeconds), len(vals))
	}

	dps := make([]ts.Datapoint, len(vals))
	for i, val := range vals {
		dps[i] = ts.Datapoint{
			Timestamp: now.Add(time.Duration(timeInSeconds[i]) * time.Second),
			Value:     val,
		}
	}

	return dps
}

func createNSBlockIter(iters []encoding.SeriesIterator, start, end time.Time, stepSize time.Duration) consolidatedNSBlockIter {
	return consolidatedNSBlockIter{
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

func TestConsolidatedNSBlockIter(t *testing.T) {
	testCases := []nsBlockTestCase{
		{
			dps: [][]ts.Datapoint{
				createDatapoints(t, []int{0, 90, 100, 300}, []float64{1, 2, 2.5, 3}, now),
			},
			start:           now,
			end:             now.Add(600 * time.Second),
			stepSize:        60 * time.Second,
			expectedResults: []float64{1, 2, nan, nan, nan, 3, nan, nan, nan, nan, nan},
			description:     "testing single iterator with two values in one block (step size)",
		},
		{
			dps: [][]ts.Datapoint{
				createDatapoints(t, []int{0, 90, 300}, []float64{1, 2, 3}, now),
				createDatapoints(t, []int{1020, 1140}, []float64{5, 6}, now)},
			start:    now,
			end:      now.Add(1200 * time.Second),
			stepSize: 60 * time.Second,
			expectedResults: []float64{1, 2, nan, nan, nan, 3, nan, nan, nan, nan,
				nan, nan, nan, nan, nan, nan, nan, 5, nan, 6, nan},
			description: "testing multiple iterators",
		},
	}

	for _, test := range testCases {
		rawTagsOne := []string{"foo", "bar", "same", "tag"}
		ctrl := gomock.NewController(t)
		var iters []encoding.SeriesIterator
		for _, dp := range test.dps {
			iters = append(iters, newMockIterator(t, "test_one", rawTagsOne, ctrl, dp))
		}

		nsBlockIter := createNSBlockIter(iters, test.start, test.end, test.stepSize)

		var actualResults []float64
		for nsBlockIter.Next() {
			actualResults = append(actualResults, nsBlockIter.Current())
		}

		assert.Len(t, actualResults, len(test.expectedResults))
		coordtest.EqualsWithNans(t, test.expectedResults, actualResults)
	}
}
