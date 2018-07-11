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
)

var (
	now = time.Now()
	nan = math.NaN()
)

// todo(braskin): Remove this stuff

// func makeResults() [][]float64 {
// 	var results [][]float64
// 	sliceOfNaNs := []float64{math.NaN(), math.NaN()}
// 	results = append(results, sliceOfNaNs)
// 	results = append(results, []float64{1, 1})
// 	results = append(results, sliceOfNaNs, sliceOfNaNs, sliceOfNaNs)
// 	results = append(results, []float64{3, 3})
// 	for i := 0; i < 11; i++ {
// 		results = append(results, sliceOfNaNs)
// 	}
// 	results = append(results, []float64{5, 5})
// 	results = append(results, sliceOfNaNs)
// 	results = append(results, []float64{6, 6})

// 	return results
// }

// func TestStepIteration(t *testing.T) {
// 	now := time.Now()
// 	ctrl := gomock.NewController(t)
// 	seriesOne, seriesTwo := newMultiNamespaceSeries(ctrl, now, t)

// 	multiNamespaceSeriesList := []MultiNamespaceSeries{seriesOne, seriesTwo}
// 	m3CoordBlocks, err := SeriesBlockToMultiSeriesBlocks(multiNamespaceSeriesList, nil, time.Minute)
// 	require.NoError(t, err)

// 	expectedResults := makeResults()
// 	var actualResults [][]float64

// 	seriesMeta := m3CoordBlocks[0].SeriesMeta()
// 	tags := []map[string]string{{"foo": "bar"}, {"biz": "baz"}}
// 	for i, tag := range tags {
// 		for k, v := range tag {
// 			assert.Equal(t, v, seriesMeta[i].Tags[k])
// 		}
// 	}

// 	for _, seriesBlock := range m3CoordBlocks {
// 		stepIter := seriesBlock.StepIter()
// 		for stepIter.Next() {
// 			actualResults = append(actualResults, stepIter.Current().Values())
// 		}
// 	}

// 	assert.Equal(t, len(expectedResults), len(actualResults))

// 	for i, expectedSlice := range expectedResults {
// 		for j, exp := range expectedSlice {
// 			if math.IsNaN(exp) {
// 				assert.True(t, math.IsNaN(actualResults[i][j]))
// 			} else {
// 				assert.Equal(t, exp, actualResults[i][j])
// 			}
// 		}
// 	}
// }

func createDatapoints(t *testing.T, timeInSeconds []int, vals []float64, now time.Time) []ts.Datapoint {
	if len(timeInSeconds) != len(vals) {
		t.Fatal("length of values must equal length of timestamps")
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
		consolidatedNSBlockSeriesIters: iters,
		bounds: block.Bounds{
			Start:    start,
			End:      end,
			StepSize: stepSize,
		},
		indexTime: start.Add(-1 * stepSize),
	}
}

type testCase struct {
	start, end                     time.Time
	stepSize                       time.Duration
	dps                            [][]ts.Datapoint
	expectedResults, actualResults []float64
	description                    string
}

func TestConsolidatedNSBlockIter(t *testing.T) {
	testCases := []testCase{
		{
			dps: [][]ts.Datapoint{
				createDatapoints(t, []int{0, 90, 100, 300}, []float64{1, 2, 2.5, 3}, now),
			},
			start:           now,
			end:             now.Add(600 * time.Second),
			stepSize:        60 * time.Second,
			expectedResults: []float64{1, 2, nan, nan, nan, 3, nan, nan, nan, nan},
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
				nan, nan, nan, nan, nan, nan, nan, 5, nan, 6},
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

		for nsBlockIter.Next() {
			test.actualResults = append(test.actualResults, nsBlockIter.Current())
		}

		assert.Len(t, test.actualResults, len(test.expectedResults))
		coordtest.EqualsWithNans(t, test.expectedResults, test.actualResults)
	}
}

type mockNSBlockIter struct {
	dps []float64
	idx int
}

func (m *mockNSBlockIter) Next() bool {
	m.idx++
	if m.idx < len(m.dps) {
		return true
	}
	return false
}

func (m *mockNSBlockIter) Current() float64 {
	return m.dps[m.idx]
}

func (m *mockNSBlockIter) Close() {}

func newMockNSBlockIter(dps [][]float64) []block.ValueIterator {
	var valueIters []block.ValueIterator
	for _, dp := range dps {
		valueIters = append(valueIters, &mockNSBlockIter{
			dps: dp,
			idx: -1,
		})
	}

	return valueIters
}

func createConsolidatedSeriesBlockIter(nsBlockIters []block.ValueIterator) consolidatedSeriesBlockIter {
	return consolidatedSeriesBlockIter{
		consolidatedNSBlockIters: nsBlockIters,
	}
}

type consolidatedSeriesTestCase struct {
	dps             [][]float64
	expectedResults []float64
	actualResults   []float64
	description     string
}

func TestConsolidatedSeriesBlockIter(t *testing.T) {
	testCases := []consolidatedSeriesTestCase{
		{
			dps:             [][]float64{{1, 2, 3, 4, 5}},
			expectedResults: []float64{1, 2, 3, 4, 5},
			description:     "only return one set of datapoints",
		},
		{
			dps:             [][]float64{{6, 7, 8, 9, 10}, {1, 2, 3, 4, 5}},
			expectedResults: []float64{6, 7, 8, 9, 10},
			description:     "return only the first set of datapoints (consolidation)",
		},
	}

	for _, test := range testCases {
		mockNSBlockIters := newMockNSBlockIter(test.dps)
		consolidatedSeriesBlock := createConsolidatedSeriesBlockIter(mockNSBlockIters)

		for consolidatedSeriesBlock.Next() {
			test.actualResults = append(test.actualResults, consolidatedSeriesBlock.Current())
		}
		coordtest.EqualsWithNans(t, test.expectedResults, test.actualResults)
	}
}

type mockSeriesBlockIter struct {
	dps []float64
	idx int
}

func (m *mockSeriesBlockIter) Next() bool {
	m.idx++
	if m.idx < len(m.dps) {
		return true
	}
	return false
}

func (m *mockSeriesBlockIter) Current() float64 {
	return m.dps[m.idx]
}

func (m *mockSeriesBlockIter) Close() {}

func newMockSeriesBlockIter(dps [][]float64) []block.ValueIterator {
	var valueIters []block.ValueIterator
	for _, dp := range dps {
		valueIters = append(valueIters, &mockSeriesBlockIter{
			dps: dp,
			idx: -1,
		})
	}

	return valueIters
}

func createMultiSeriesBlockStepIter(seriesBlockIters []block.ValueIterator) multiSeriesBlockStepIter {
	return multiSeriesBlockStepIter{
		seriesIters: seriesBlockIters,
	}
}

type multiSeriesBlockStepIterTestCase struct {
	dps             [][]float64
	expectedResults [][]float64
	actualResults   [][]float64
	description     string
}

func TestMultiSeriesBlockStepIter(t *testing.T) {
	testCases := []multiSeriesBlockStepIterTestCase{
		{
			dps:             [][]float64{{1, 2, 3, 4, 5}},
			expectedResults: [][]float64{{1}, {2}, {3}, {4}, {5}},
			description:     "only return one set of datapoints",
		},
		{
			dps:             [][]float64{{6, 7, 8, 9, 10}, {1, 2, 3, 4, 5}},
			expectedResults: [][]float64{{6, 1}, {7, 2}, {8, 3}, {9, 4}, {10, 5}},
			description:     "take only the first set of datapoints (consolidation)",
		},
	}

	for _, test := range testCases {
		mockNSBlockIters := newMockSeriesBlockIter(test.dps)
		consolidatedSeriesBlock := createMultiSeriesBlockStepIter(mockNSBlockIters)

		for consolidatedSeriesBlock.Next() {
			test.actualResults = append(test.actualResults, consolidatedSeriesBlock.Current().Values())
		}
		coordtest.EqualsWithNans(t, test.expectedResults, test.actualResults)
	}
}
