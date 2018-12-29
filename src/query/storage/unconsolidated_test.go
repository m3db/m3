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
	"testing"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/ts"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildUnconsolidatedBlock(t *testing.T) block.UnconsolidatedBlock {
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

	fetchQuery := &FetchQuery{
		Start:    start,
		End:      start.Add(time.Minute * 30),
		Interval: time.Minute,
	}

	unconsolidated, err := NewMultiSeriesBlock(seriesList, fetchQuery)
	require.NoError(t, err)
	return unconsolidated
}

func datapointsToFloatSlices(t *testing.T, dps []ts.Datapoints) [][]float64 {
	vals := make([][]float64, len(dps))
	for i, dp := range dps {
		vals[i] = dp.Values()
	}

	return vals
}

func TestUnconsolidatedStep(t *testing.T) {
	expected := [][][]float64{
		{{}, {}, {}},
		{{}, {}, {}},
		{{}, {10}, {}},
		{{}, {20}, {100}},
		{{}, {20}, {100}},
		{{}, {}, {}},
		{{1}, {}, {}},
		{{1}, {}, {}},
		{{2, 3}, {}, {}},
		{{4}, {}, {200}},
		{{5}, {}, {200}},
		{{6}, {}, {}},
		{{7}, {}, {}},
		{{7}, {30}, {}},
		{{}, {30}, {}},
		{{}, {}, {300}},
		{{}, {}, {300}},
		{{8}, {}, {}},
		{{8}, {}, {}},
		{{}, {40}, {}},
		{{}, {}, {}},
		{{}, {}, {400}},
		{{9}, {}, {400}},
		{{9}, {}, {}},
		{{}, {}, {500}},
		{{}, {}, {500}},
		{{}, {}, {}},
		{{}, {}, {}},
		{{}, {}, {}},
		{{}, {}, {}},
	}

	unconsolidated := buildUnconsolidatedBlock(t)
	iter, err := unconsolidated.StepIter()
	assert.NoError(t, err)

	i := 0
	for iter.Next() {
		step := iter.Current()
		dps := datapointsToFloatSlices(t, step.Values())
		assert.Equal(t, expected[i], dps)
		i++
	}

	assert.Equal(t, len(expected), i)
	assert.NoError(t, iter.Err())
}

func TestUnconsolidatedSeries(t *testing.T) {
	expected := [][][]float64{
		{
			{}, {}, {}, {}, {}, {}, {1}, {1}, {2, 3}, {4}, {5}, {6}, {7}, {7},
			{}, {}, {}, {8}, {8}, {}, {}, {}, {9}, {9}, {}, {}, {}, {}, {}, {},
		},
		{
			{}, {}, {10}, {20}, {20}, {}, {}, {}, {}, {}, {}, {}, {}, {30},
			{30}, {}, {}, {}, {}, {40}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {},
		},
		{
			{}, {}, {}, {100}, {100}, {}, {}, {}, {}, {200}, {200}, {}, {}, {}, {},
			{300}, {300}, {}, {}, {}, {}, {400}, {400}, {}, {500}, {500}, {}, {}, {}, {},
		},
	}

	unconsolidated := buildUnconsolidatedBlock(t)
	iter, err := unconsolidated.SeriesIter()
	assert.NoError(t, err)

	i := 0
	for iter.Next() {
		series := iter.Current()
		dps := datapointsToFloatSlices(t, series.Datapoints())
		assert.Equal(t, expected[i], dps)
		i++
	}

	assert.Equal(t, len(expected), i)
	assert.NoError(t, iter.Err())
}
