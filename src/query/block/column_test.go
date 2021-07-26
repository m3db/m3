// Copyright (c) 2019 Uber Technologies, Inc.
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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/ts"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func makeTestQueryContext() *models.QueryContext {
	return models.NewQueryContext(context.Background(),
		tally.NoopScope,
		models.QueryContextOptions{})
}

func TestColumnBuilderInfoTypes(t *testing.T) {
	ctx := makeTestQueryContext()
	builder := NewColumnBlockBuilder(ctx, Metadata{}, []SeriesMeta{})
	block := builder.Build()
	assert.Equal(t, BlockDecompressed, block.Info().blockType)

	block = builder.BuildAsType(BlockScalar)
	assert.Equal(t, BlockScalar, block.Info().blockType)
}

func namedMeta(i int) SeriesMeta {
	name := fmt.Sprint(i)

	return SeriesMeta{
		Name: []byte(name),
		Tags: models.MustMakeTags("name", name),
	}
}

func TestSetRow(t *testing.T) {
	size := 10
	metas := make([]SeriesMeta, size)
	for i := range metas {
		metas[i] = namedMeta(i)
	}

	ctx := makeTestQueryContext()
	builder := NewColumnBlockBuilder(ctx, Metadata{
		Bounds: models.Bounds{StepSize: time.Minute, Duration: time.Minute},
	}, nil)

	require.NoError(t, builder.AddCols(1))
	builder.PopulateColumns(size)
	// NB: set the row metas backwards.
	j := 0
	for i := size - 1; i >= 0; i-- {
		err := builder.SetRow(j, []float64{float64(i)}, metas[i])
		require.NoError(t, err)
		j++
	}

	bl := builder.Build()
	it, err := bl.StepIter()
	require.NoError(t, err)

	actualMetas := it.SeriesMeta()
	for i, m := range actualMetas {
		ex := fmt.Sprint(size - 1 - i)
		assert.Equal(t, ex, string(m.Name))
		require.Equal(t, 1, m.Tags.Len())
		tag, found := m.Tags.Get([]byte("name"))
		require.True(t, found)
		assert.Equal(t, ex, string(tag))
	}

	assert.True(t, it.Next())
	exVals := make([]float64, size)
	for i := range exVals {
		exVals[i] = float64(size - 1 - i)
	}

	vals := it.Current().Values()
	assert.Equal(t, exVals, vals)
	assert.False(t, it.Next())
	assert.NoError(t, it.Err())
}

type exStep struct {
	t xtime.UnixNano
	v []float64
}

type exSeries struct {
	dps  ts.Datapoints
	meta SeriesMeta
}

func TestIters(t *testing.T) {
	var (
		size  = 5
		start = xtime.Now().Truncate(time.Hour)
		steps = 3
		step  = time.Minute
		metas = make([]SeriesMeta, size)

		expectedSteps = []exStep{
			{t: start, v: []float64{0, 1, 2, 3, 4}},
			{t: start.Add(step), v: []float64{0, 10, 20, 30, 40}},
			{t: start.Add(step * 2), v: []float64{0, 100, 200, 300, 400}},
		}

		makeDps = func(v ...float64) ts.Datapoints {
			dps := make(ts.Datapoints, 0, len(v))
			for idx, val := range v {
				dps = append(dps, ts.Datapoint{
					Timestamp: start.Add(step * time.Duration(idx)),
					Value:     val,
				})
			}
			return dps
		}

		expectedSeries = []exSeries{
			{dps: makeDps(0, 0, 0), meta: namedMeta(0)},
			{dps: makeDps(1, 10, 100), meta: namedMeta(1)},
			{dps: makeDps(2, 20, 200), meta: namedMeta(2)},
			{dps: makeDps(3, 30, 300), meta: namedMeta(3)},
			{dps: makeDps(4, 40, 400), meta: namedMeta(4)},
		}
	)

	for i := range metas {
		metas[i] = namedMeta(i)
	}

	ctx := makeTestQueryContext()
	builder := NewColumnBlockBuilder(ctx, Metadata{
		Bounds: models.Bounds{
			Start:    start,
			StepSize: step,
			Duration: time.Duration(steps) * step,
		},
	}, nil)

	require.NoError(t, builder.AddCols(steps))
	builder.PopulateColumns(size)
	for i := 0; i < size; i++ {
		row := []float64{float64(i), float64(i * 10), float64(i * 100)}
		require.NoError(t, builder.SetRow(i, row, metas[i]))
	}

	bl := builder.Build()

	t.Run("series", func(t *testing.T) {
		iter, err := bl.SeriesIter()
		require.NoError(t, err)
		for i := 0; iter.Next(); i++ {
			c := iter.Current()
			require.Equal(t, expectedSeries[i].meta, c.Meta)
			require.Equal(t, expectedSeries[i].dps, c.datapoints)
		}

		require.NoError(t, iter.Err())
	})

	t.Run("multi series", func(t *testing.T) {
		iters, err := bl.MultiSeriesIter(2)
		require.NoError(t, err)

		exMulti := [][]exSeries{
			expectedSeries[:1],
			expectedSeries[1:],
		}

		for multiIdx, multi := range iters {
			iter := multi.Iter
			for i := 0; iter.Next(); i++ {
				c := iter.Current()
				require.Equal(t, exMulti[multiIdx][i].meta, c.Meta)
				require.Equal(t, exMulti[multiIdx][i].dps, c.datapoints)
			}

			require.Equal(t, len(exMulti[multiIdx]), multi.Size)
		}
	})

	t.Run("step", func(t *testing.T) {
		iter, err := bl.StepIter()
		require.NoError(t, err)
		for i := 0; iter.Next(); i++ {
			c := iter.Current()
			require.Equal(t, expectedSteps[i].t, c.Time())
			require.Equal(t, expectedSteps[i].v, c.Values())
		}

		require.NoError(t, iter.Err())
	})
}
