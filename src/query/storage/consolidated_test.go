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

package storage

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/ts"

	"github.com/stretchr/testify/assert"
)

var (
	nan = math.NaN()
)

func makeDatapoints(start time.Time) []ts.Datapoints {
	return []ts.Datapoints{
		[]ts.Datapoint{
			{Value: 1, Timestamp: start.Add(time.Minute * 6)},
			{Value: 2, Timestamp: start.Add(time.Minute*7 + time.Second)},
			{Value: 3, Timestamp: start.Add(time.Minute * 8)},
			{Value: 4, Timestamp: start.Add(time.Minute * 9)},
			{Value: 5, Timestamp: start.Add(time.Minute * 10)},
			{Value: 6, Timestamp: start.Add(time.Minute * 11)},
			{Value: 7, Timestamp: start.Add(time.Minute * 12)},
			{Value: 8, Timestamp: start.Add(time.Minute * 17)},
			{Value: 9, Timestamp: start.Add(time.Minute * 22)},
		},
		[]ts.Datapoint{
			{Value: 10, Timestamp: start.Add(time.Minute * 2)},
			{Value: 20, Timestamp: start.Add(time.Minute * 3)},
			{Value: 30, Timestamp: start.Add(time.Minute * 13)},
			{Value: 40, Timestamp: start.Add(time.Minute*18 + time.Second)},
		},
		[]ts.Datapoint{
			{Value: 100, Timestamp: start.Add(time.Minute * 3)},
			{Value: 200, Timestamp: start.Add(time.Minute * 9)},
			{Value: 300, Timestamp: start.Add(time.Minute * 15)},
			{Value: 400, Timestamp: start.Add(time.Minute * 21)},
			{Value: 500, Timestamp: start.Add(time.Minute * 24)},
		},
	}
}

var consolidationTests = []struct {
	name     string
	stepSize time.Duration
	expected [][]float64
}{
	{
		name:     "1 minute",
		stepSize: time.Minute,
		expected: [][]float64{
			{nan, nan, nan},
			{nan, nan, nan},
			{nan, 10, nan},
			{nan, 20, 100},
			{nan, 20, 100},
			{nan, nan, nan},
			{1, nan, nan},
			{1, nan, nan},
			{3, nan, nan},
			{4, nan, 200},
			{5, nan, 200},
			{6, nan, nan},
			{7, nan, nan},
			{7, 30, nan},
			{nan, 30, nan},
			{nan, nan, 300},
			{nan, nan, 300},
			{8, nan, nan},
			{8, nan, nan},
			{nan, 40, nan},
			{nan, nan, nan},
			{nan, nan, 400},
			{9, nan, 400},
			{9, nan, nan},
			{nan, nan, 500},
			{nan, nan, 500},
			{nan, nan, nan},
			{nan, nan, nan},
			{nan, nan, nan},
			{nan, nan, nan},
		},
	},
	{
		name:     "2 minute",
		stepSize: time.Minute * 2,
		expected: [][]float64{
			{nan, nan, nan},
			{nan, 10, nan},
			{nan, 20, 100},
			{1, nan, nan},
			{3, nan, nan},
			{5, nan, 200},
			{7, nan, nan},
			{nan, 30, nan},
			{nan, nan, 300},
			{8, nan, nan},
			{nan, nan, nan},
			{9, nan, 400},
			{nan, nan, 500},
			{nan, nan, nan},
			{nan, nan, nan},
		},
	},
	{
		name:     "3 minute",
		stepSize: time.Minute * 3,
		expected: [][]float64{
			{nan, nan, nan},
			{nan, 20, 100},
			{1, nan, nan},
			{4, nan, 200},
			{7, nan, nan},
			{nan, nan, 300},
			{8, nan, nan},
			{nan, nan, 400},
			{nan, nan, 500},
			{nan, nan, nan},
		},
	},
}

func TestConsolidation(t *testing.T) {
	start := time.Now().Truncate(time.Hour)
	datapoints := makeDatapoints(start)

	seriesList := make(ts.SeriesList, len(datapoints))
	for i, dp := range datapoints {
		seriesList[i] = ts.NewSeries(
			fmt.Sprintf("name_%d", i),
			dp,
			models.Tags{
				Opts: models.NewTagOptions(),
				Tags: []models.Tag{{
					Name:  []byte("a"),
					Value: []byte(fmt.Sprintf("b_%d", i)),
				}},
			},
		)
	}

	for _, tt := range consolidationTests {
		fetchQuery := &FetchQuery{
			Start:    start,
			End:      start.Add(time.Minute * 30),
			Interval: tt.stepSize,
		}

		unconsolidated, err := NewMultiSeriesBlock(seriesList, fetchQuery)
		assert.NoError(t, err)

		block, err := unconsolidated.Consolidate()
		assert.NoError(t, err)

		iter, err := block.StepIter()
		assert.NoError(t, err)

		i := 0
		for iter.Next() {
			step := iter.Current()
			equalsWithNans(t, step.Values(), tt.expected[i])
			i++
		}

		assert.NoError(t, iter.Err())
	}
}

func equalsWithNans(t *testing.T, expected, actual []float64) {
	assert.Equal(t, len(expected), len(actual))
	for i, ex := range expected {
		ac := actual[i]
		if math.IsNaN(ex) {
			assert.True(t, math.IsNaN(ac), fmt.Sprintf("Point at %d does not match", i))
		} else {
			assert.Equal(t, ex, ac, fmt.Sprintf("Point at %d does not match", i))
		}
	}
}
