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
	"testing"
	"time"

	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/ts"

	"github.com/stretchr/testify/require"
)

func datapointsToFloatSlices(t *testing.T, dps []ts.Datapoints) [][]float64 {
	vals := make([][]float64, len(dps))
	for i, dp := range dps {
		vals[i] = dp.Values()
	}

	return vals
}

func TestUnconsolidatedStepIterator(t *testing.T) {
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

	j := 0
	opts := NewOptions().
		SetLookbackDuration(1 * time.Minute).
		SetSplitSeriesByBlock(true)
	blocks, bounds := generateBlocks(t, time.Minute, opts)
	for i, block := range blocks {
		unconsolidated, err := block.Unconsolidated()
		require.NoError(t, err)

		iters, err := unconsolidated.StepIter()
		require.NoError(t, err)

		require.True(t, bounds.Equals(iters.Meta().Bounds))
		verifyMetas(t, i, iters.Meta(), iters.SeriesMeta())
		for iters.Next() {
			step := iters.Current()
			vals := step.Values()
			actual := datapointsToFloatSlices(t, vals)

			test.EqualsWithNans(t, expected[j], actual)
			j++
		}

		require.Equal(t, len(expected), j)
		require.NoError(t, iters.Err())
	}
}

func TestUnconsolidatedSeriesIterator(t *testing.T) {
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

	j := 0
	opts := NewOptions().
		SetLookbackDuration(1 * time.Minute).
		SetSplitSeriesByBlock(false)
	blocks, bounds := generateBlocks(t, time.Minute, opts)
	for i, block := range blocks {
		unconsolidated, err := block.Unconsolidated()
		require.NoError(t, err)

		iters, err := unconsolidated.SeriesIter()
		require.NoError(t, err)

		require.True(t, bounds.Equals(iters.Meta().Bounds))
		verifyMetas(t, i, iters.Meta(), iters.SeriesMeta())
		for iters.Next() {
			series := iters.Current()
			vals := series.Datapoints()
			actual := datapointsToFloatSlices(t, vals)
			test.EqualsWithNans(t, expected[j], actual)
			j++
		}

		require.Equal(t, len(expected), j)
		require.NoError(t, iters.Err())
	}
}
