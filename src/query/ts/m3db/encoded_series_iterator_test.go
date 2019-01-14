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

	"github.com/stretchr/testify/require"
)

var consolidatedSeriesIteratorTests = []struct {
	name     string
	stepSize time.Duration
	expected [][]float64
}{
	{
		name:     "1 minute",
		stepSize: time.Minute,
		expected: [][]float64{
			{
				nan, nan, nan, nan, nan, nan,
				1, 1, 3, 4, 5, 6,
				7, 7, nan, nan, nan, 8,
				8, nan, nan, nan, 9, 9,
				nan, nan, nan, nan, nan, nan,
			},
			{
				nan, nan, 10, 20, 20, nan,
				nan, nan, nan, nan, nan, nan,
				nan, 30, 30, nan, nan, nan,
				nan, 40, nan, nan, nan, nan,
				nan, nan, nan, nan, nan, nan,
			},
			{
				nan, nan, nan, 100, 100, nan,
				nan, nan, nan, 200, 200, nan,
				nan, nan, nan, 300, 300, nan,
				nan, nan, nan, 400, 400, nan,
				500, 500, nan, nan, nan, nan,
			},
		},
	},
	{
		name:     "2 minute",
		stepSize: time.Minute * 2,
		expected: [][]float64{
			{
				nan, nan, nan, 1, 3, 5, 7, nan,
				nan, 8, nan, 9, nan, nan, nan,
			},
			{
				nan, 10, 20, nan, nan, nan, nan, 30,
				nan, nan, nan, nan, nan, nan, nan,
			},
			{
				nan, nan, 100, nan, nan, 200, nan,
				nan, 300, nan, nan, 400, 500, nan, nan,
			},
		},
	},
	{
		name:     "3 minute",
		stepSize: time.Minute * 3,
		expected: [][]float64{
			{
				nan, nan, 1, 4, 7, nan, 8, nan, nan, nan,
			},
			{
				nan, 20, nan, nan, nan, nan, nan, nan, nan, nan,
			},
			{
				nan, 100, nan, 200, nan, 300, nan, 400, 500, nan,
			},
		},
	},
}

func TestConsolidatedSeriesIteratorWithLookback(t *testing.T) {
	for _, tt := range consolidatedSeriesIteratorTests {
		opts := NewOptions().
			SetLookbackDuration(1 * time.Minute).
			SetSplitSeriesByBlock(false)
		blocks, bounds := generateBlocks(t, tt.stepSize, opts)
		j := 0
		for i, block := range blocks {
			iters, err := block.SeriesIter()
			require.NoError(t, err)

			require.True(t, bounds.Equals(iters.Meta().Bounds))
			verifyMetas(t, i, iters.Meta(), iters.SeriesMeta())
			for iters.Next() {
				series := iters.Current()
				test.EqualsWithNans(t, tt.expected[j], series.Values())
				j++
			}

			require.NoError(t, iters.Err())
		}
	}
}

var consolidatedSeriesIteratorTestsSplitByBlock = []struct {
	name     string
	stepSize time.Duration
	expected [][][]float64
}{
	{
		name:     "1 minute",
		stepSize: time.Minute,
		expected: [][][]float64{
			{
				{nan, nan, nan, nan, nan, nan},
				{nan, nan, 10, 20, nan, nan},
				{nan, nan, nan, 100, nan, nan},
			},
			{
				{1, nan, 3, 4, 5, 6},
				{nan, nan, nan, nan, nan, nan},
				{nan, nan, nan, 200, nan, nan},
			},
			{
				{7, nan, nan, nan, nan, 8},
				{nan, 30, nan, nan, nan, nan},
				{nan, nan, nan, 300, nan, nan},
			},
			{
				{nan, nan, nan, nan, 9, nan},
				{nan, nan, nan, nan, nan, nan},
				{nan, nan, nan, 400, nan, nan},
			},
			{
				{nan, nan, nan, nan, nan, nan},
				{nan, nan, nan, nan, nan, nan},
				{500, nan, nan, nan, nan, nan},
			},
		},
	},
	{
		name:     "2 minute",
		stepSize: time.Minute * 2,
		expected: [][][]float64{
			{
				{nan, nan, nan},
				{nan, 10, nan},
				{nan, nan, nan},
			},
			{
				{1, 3, 5},
				{nan, nan, nan},
				{nan, nan, nan},
			},
			{
				{7, nan, nan},
				{nan, nan, nan},
				{nan, nan, nan},
			},
			{
				{nan, nan, 9},
				{nan, nan, nan},
				{nan, nan, nan},
			},
			{
				{nan, nan, nan},
				{nan, nan, nan},
				{500, nan, nan},
			},
		},
	},
	{
		name:     "3 minute",
		stepSize: time.Minute * 3,
		expected: [][][]float64{
			{
				{nan, nan},
				{nan, 20},
				{nan, 100},
			},
			{
				{1, 4},
				{nan, nan},
				{nan, 200},
			},
			{
				{7, nan},
				{nan, nan},
				{nan, 300},
			},
			{
				{nan, nan},
				{nan, nan},
				{nan, 400},
			},
			{
				{nan, nan},
				{nan, nan},
				{500, nan},
			},
		},
	},
}

func TestConsolidatedSeriesIteratorSplitByBlock(t *testing.T) {
	for _, tt := range consolidatedSeriesIteratorTestsSplitByBlock {
		opts := NewOptions().
			SetLookbackDuration(0).
			SetSplitSeriesByBlock(true)
		blocks, bounds := generateBlocks(t, tt.stepSize, opts)
		for i, block := range blocks {
			iters, err := block.SeriesIter()
			require.NoError(t, err)

			j := 0
			idx := verifyBoundsAndGetBlockIndex(t, bounds, iters.Meta().Bounds)
			verifyMetas(t, i, iters.Meta(), iters.SeriesMeta())
			for iters.Next() {
				series, err := iters.Current()
				require.NoError(t, err)
				test.EqualsWithNans(t, tt.expected[idx][j], series.Values())
				j++
			}
		}
	}
}
