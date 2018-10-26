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
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	Start     = time.Now().Truncate(time.Hour)
	blockSize = time.Minute * 6
	nan       = math.NaN()
)

func generateIterators(
	t *testing.T,
	stepSize time.Duration,
) (
	[]encoding.SeriesIterator,
	models.Bounds,
) {
	datapoints := [][][]test.Datapoint{
		{
			[]test.Datapoint{},
			[]test.Datapoint{
				{Value: 1, Offset: 0},
				{Value: 2, Offset: (time.Minute * 1) + time.Second},
				{Value: 3, Offset: (time.Minute * 2)},
				{Value: 4, Offset: (time.Minute * 3)},
				{Value: 5, Offset: (time.Minute * 4)},
				{Value: 6, Offset: (time.Minute * 5)},
			},
			[]test.Datapoint{
				{Value: 7, Offset: time.Minute * 0},
				{Value: 8, Offset: time.Minute * 5},
			},
			[]test.Datapoint{
				{Value: 9, Offset: time.Minute * 4},
			},
		},
		{
			[]test.Datapoint{
				{Value: 10, Offset: (time.Minute * 2)},
				{Value: 20, Offset: (time.Minute * 3)},
			},
			[]test.Datapoint{},
			[]test.Datapoint{
				{Value: 30, Offset: time.Minute},
			},
			[]test.Datapoint{
				{Value: 40, Offset: time.Second},
			},
		},
		{
			[]test.Datapoint{
				{Value: 100, Offset: (time.Minute * 3)},
			},
			[]test.Datapoint{
				{Value: 200, Offset: (time.Minute * 3)},
			},
			[]test.Datapoint{
				{Value: 300, Offset: (time.Minute * 3)},
			},
			[]test.Datapoint{
				{Value: 400, Offset: (time.Minute * 3)},
			},
			[]test.Datapoint{
				{Value: 500, Offset: 0},
			},
		},
	}

	iters := make([]encoding.SeriesIterator, len(datapoints))
	var (
		iter   encoding.SeriesIterator
		bounds models.Bounds
		start  = Start
	)
	for i, dps := range datapoints {
		iter, bounds = buildCustomIterator(t, i, start, stepSize, dps)
		iters[i] = iter
	}

	return iters, bounds
}

func buildCustomIterator(
	t *testing.T,
	i int,
	start time.Time,
	stepSize time.Duration,
	dps [][]test.Datapoint,
) (
	encoding.SeriesIterator,
	models.Bounds,
) {
	iter, bounds, err := test.BuildCustomIterator(
		dps,
		map[string]string{"a": "b", "c": fmt.Sprint(i)},
		fmt.Sprintf("abc%d", i), "namespace",
		start,
		blockSize, stepSize,
	)
	require.NoError(t, err)
	return iter, bounds
}

// verifies that given sub-bounds are valid, and returns the index of the
// generated block being checked.
func verifyBoundsAndGetBlockIndex(t *testing.T, bounds, sub models.Bounds) int {
	require.Equal(t, bounds.StepSize, sub.StepSize)
	require.Equal(t, blockSize, sub.Duration)
	diff := sub.Start.Sub(bounds.Start)
	require.Equal(t, 0, int(diff%blockSize))
	return int(diff / blockSize)
}

func verifyMetas(
	t *testing.T,
	i int,
	meta block.Metadata,
	metas []block.SeriesMeta,
) {
	require.Equal(t, 1, meta.Tags.Len())
	val, found := meta.Tags.Get([]byte("a"))
	assert.True(t, found)
	assert.Equal(t, []byte("b"), val)

	for i, m := range metas {
		assert.Equal(t, fmt.Sprintf("abc%d", i), m.Name)
		require.Equal(t, 1, m.Tags.Len())
		val, found := m.Tags.Get([]byte("c"))
		assert.True(t, found)
		assert.Equal(t, []byte(fmt.Sprint(i)), val)
	}
}

// NB: blocks are not necessarily generated in order; last block may be the first
// one in the returned array. This is fine since they are processed independently
// and are put back together at the read step, or at any temporal functions in
// the execution pipeline.
func generateBlocks(
	t *testing.T,
	stepSize time.Duration,
	opts Options,
) ([]block.Block, models.Bounds) {
	iterators, bounds := generateIterators(t, stepSize)
	blocks, err := ConvertM3DBSeriesIterators(
		encoding.NewSeriesIterators(iterators, nil),
		bounds,
		opts,
	)
	require.NoError(t, err)
	return blocks, bounds
}
