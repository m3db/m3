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

package m3

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/test/compare"
	"github.com/m3db/m3/src/query/ts"
)

func datapointsToFloatSlices(t *testing.T, dps []ts.Datapoints) [][]float64 {
	vals := make([][]float64, len(dps))
	for i, dp := range dps {
		vals[i] = dp.Values()
	}

	return vals
}

func TestSeriesIterator(t *testing.T) {
	expected := [][]float64{
		{1, 2, 3, 4, 5, 6, 7, 8, 9},
		{10, 20, 30, 40},
		{100, 200, 300, 400, 500},
	}

	j := 0
	opts := NewOptions(encoding.NewOptions()).
		SetLookbackDuration(1 * time.Minute).
		SetSplitSeriesByBlock(false)
	require.NoError(t, opts.Validate())
	blocks, bounds := generateBlocks(t, time.Minute, opts)
	for i, block := range blocks {
		iters, err := block.SeriesIter()
		require.NoError(t, err)

		require.True(t, bounds.Equals(block.Meta().Bounds))
		verifyMetas(t, i, block.Meta(), iters.SeriesMeta())
		for iters.Next() {
			series := iters.Current()
			actual := make([]float64, 0, len(series.Datapoints()))
			for _, v := range series.Datapoints() {
				actual = append(actual, v.Value)
			}

			compare.EqualsWithNans(t, expected[j], actual)
			j++
		}

		require.Equal(t, len(expected), j)
		require.NoError(t, iters.Err())
	}
}

func verifySingleMeta(
	t *testing.T,
	i int,
	meta block.Metadata,
	metas []block.SeriesMeta,
) {
	require.Equal(t, 0, meta.Tags.Len())
	require.Equal(t, 1, len(metas))

	m := metas[0]
	assert.Equal(t, fmt.Sprintf("abc%d", i), string(m.Name))
	require.Equal(t, 2, m.Tags.Len())

	val, found := m.Tags.Get([]byte("a"))
	assert.True(t, found)
	assert.Equal(t, []byte("b"), val)

	val, found = m.Tags.Get([]byte("c"))
	assert.True(t, found)
	assert.Equal(t, []byte(fmt.Sprint(i)), val)
}

func TestSeriesIteratorBatch(t *testing.T) {
	expected := [][]float64{
		{1, 2, 3, 4, 5, 6, 7, 8, 9},
		{10, 20, 30, 40},
		{100, 200, 300, 400, 500},
	}

	count := 0
	opts := NewOptions(encoding.NewOptions()).
		SetLookbackDuration(1 * time.Minute).
		SetSplitSeriesByBlock(false)
	require.NoError(t, opts.Validate())
	blocks, bounds := generateBlocks(t, time.Minute, opts)
	for _, bl := range blocks {
		require.True(t, bounds.Equals(bl.Meta().Bounds))
		iters, err := bl.MultiSeriesIter(3)
		require.NoError(t, err)
		require.Equal(t, 3, len(iters))

		for i, itBatch := range iters {
			iter := itBatch.Iter
			require.Equal(t, 1, itBatch.Size)
			verifySingleMeta(t, i, bl.Meta(), iter.SeriesMeta())
			for iter.Next() {
				series := iter.Current()
				actual := make([]float64, 0, len(series.Datapoints()))
				for _, v := range series.Datapoints() {
					actual = append(actual, v.Value)
				}

				compare.EqualsWithNans(t, expected[i], actual)
				count++
			}

			require.NoError(t, iter.Err())
		}

		assert.Equal(t, 3, count)
	}
}
