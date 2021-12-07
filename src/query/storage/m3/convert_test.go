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
	"bytes"
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	Start     = time.Now().Truncate(time.Hour)
	blockSize = time.Minute * 6
	nan       = math.NaN()
)

func generateIterators(
	t testing.TB,
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
	t testing.TB,
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
		xtime.ToUnixNano(start),
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

type ascByName []block.SeriesMeta

func (m ascByName) Len() int { return len(m) }
func (m ascByName) Less(i, j int) bool {
	return bytes.Compare(m[i].Name, m[j].Name) == -1
}
func (m ascByName) Swap(i, j int) { m[i], m[j] = m[j], m[i] }

func verifyMetas(
	t *testing.T,
	_ int,
	meta block.Metadata,
	metas []block.SeriesMeta,
) {
	require.Equal(t, 0, meta.Tags.Len())
	sort.Sort(ascByName(metas))
	for i, m := range metas {
		assert.Equal(t, fmt.Sprintf("abc%d", i), string(m.Name))
		require.Equal(t, 2, m.Tags.Len())

		val, found := m.Tags.Get([]byte("a"))
		assert.True(t, found)
		assert.Equal(t, "b", string(val))

		val, found = m.Tags.Get([]byte("c"))
		assert.True(t, found)
		require.Equal(t, fmt.Sprint(i), string(val))
	}
}

// NB: blocks are not necessarily generated in order; last block may be the first
// one in the returned array. This is fine since they are processed independently
// and are put back together at the read step, or at any temporal functions in
// the execution pipeline.
func generateBlocks(
	t testing.TB,
	stepSize time.Duration,
	opts Options,
) ([]block.Block, models.Bounds) {
	iterators, bounds := generateIterators(t, stepSize)
	res, err := iterToFetchResult(iterators)
	require.NoError(t, err)
	blocks, err := ConvertM3DBSeriesIterators(
		res,
		bounds,
		opts,
	)
	require.NoError(t, err)
	return blocks, bounds
}

func TestUpdateTimeBySteps(t *testing.T) {
	var tests = []struct {
		stepSize, blockSize, expected time.Duration
	}{
		{time.Minute * 15, time.Hour, time.Hour},
		{time.Minute * 14, time.Hour, time.Minute * 70},
		{time.Minute * 13, time.Hour, time.Minute * 65},
		{time.Minute * 59, time.Hour, time.Minute * 118},
	}

	for _, tt := range tests {
		updateDuration := blockDuration(tt.blockSize, tt.stepSize)
		assert.Equal(t, tt.expected/time.Minute, updateDuration/time.Minute)
	}
}

func TestPadSeriesBlocks(t *testing.T) {
	blockSize := time.Hour
	start := xtime.Now().Truncate(blockSize)
	readOffset := time.Minute
	itStart := start.Add(readOffset)

	var tests = []struct {
		blockStart       xtime.UnixNano
		stepSize         time.Duration
		expectedStart    xtime.UnixNano
		expectedStartTwo xtime.UnixNano
	}{
		{start, time.Minute * 30, itStart, start.Add(61 * time.Minute)},
		{
			start.Add(blockSize),
			time.Minute * 30,
			start.Add(61 * time.Minute),
			start.Add(121 * time.Minute),
		},
		// step 0: Start + 1  , Start + 37
		// step 1: Start + 73 , Start + 109
		// step 2: Start + 145
		// step 3: Start + 181, Start + 217
		// step 4: Start + 253, ...
		{
			start.Add(blockSize * 3),
			time.Minute * 36,
			start.Add(181 * time.Minute),
			start.Add(253 * time.Minute),
		},
		// step 0: Start + 1  , Start + 38
		// step 1: Start + 75 , Start + 112
		// step 2: Start + 149
		// step 3: Start + 186
		{
			start.Add(blockSize * 2),
			time.Minute * 37,
			start.Add(149 * time.Minute),
			start.Add(186 * time.Minute),
		},
		// step 0: Start + 1  , Start + 12 , Start + 23,
		//         Start + 34 , Start + 45 , Start + 56
		// step 1: Start + 67 , Start + 78 , Start + 89
		//         Start + 100, Start + 111
		// step 2: Start + 122 ...
		{
			start.Add(blockSize * 1),
			time.Minute * 11,
			start.Add(67 * time.Minute),
			start.Add(122 * time.Minute),
		},
	}

	for _, tt := range tests {
		blocks := seriesBlocks{
			{
				blockStart: tt.blockStart,
				blockSize:  blockSize,
				replicas:   []encoding.MultiReaderIterator{},
			},
			{
				blockStart: tt.blockStart.Add(blockSize),
				blockSize:  blockSize,
				replicas:   []encoding.MultiReaderIterator{},
			},
		}

		updated := updateSeriesBlockStarts(blocks, tt.stepSize, itStart)
		require.Equal(t, 2, len(updated))
		assert.Equal(t, tt.blockStart, updated[0].blockStart)
		assert.Equal(t, tt.expectedStart, updated[0].readStart)

		assert.Equal(t, tt.blockStart.Add(blockSize), updated[1].blockStart)
		assert.Equal(t, tt.expectedStartTwo, updated[1].readStart)
	}
}
