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
	"testing"
	"time"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/ts"
	"github.com/stretchr/testify/assert"
)

var (
	start = time.Now().Truncate(time.Hour)
	step1 = []float64{1, 2, 3}
	step2 = []float64{10, 20, 30}
	cols  = []column{step1, step2}

	meta = Metadata{
		Bounds: models.Bounds{
			Start:    start,
			Duration: time.Minute * 2,
			StepSize: time.Minute,
		},
	}

	metas = []SeriesMeta{
		SeriesMeta{Name: []byte("a")},
		SeriesMeta{Name: []byte("b")},
		SeriesMeta{Name: []byte("c")},
	}
)

func buildBlock() Block {
	return &columnBlock{
		blockType:  BlockScalar,
		columns:    cols,
		meta:       meta,
		seriesMeta: metas,
	}
}

func TestColumnBlock(t *testing.T) {
	bl := buildBlock()
	assert.Equal(t, BlockScalar, bl.Info().BaseType())
	assert.Equal(t, meta, bl.Meta())
	assert.NoError(t, bl.Close())
}

func TestColumnStepIter(t *testing.T) {
	stepIter, err := buildBlock().StepIter()
	assert.NoError(t, err)

	assert.Equal(t, 2, stepIter.StepCount())
	assert.Equal(t, metas, stepIter.SeriesMeta())
	assert.True(t, stepIter.Next())
	assert.Equal(t, step1, stepIter.Current().Values())
	assert.True(t, stepIter.Next())
	assert.Equal(t, step2, stepIter.Current().Values())
	assert.False(t, stepIter.Next())
	assert.NoError(t, stepIter.Err())
	stepIter.Close()
}

func makeTs(n float64) ts.Datapoints {
	return ts.Datapoints{
		ts.Datapoint{Timestamp: start, Value: n},
		ts.Datapoint{Timestamp: start.Add(time.Minute), Value: n * 10},
	}
}

func verifySeriesIter(t *testing.T, it SeriesIter,
	metas []SeriesMeta, start int) {
	numSeries := len(metas)
	assert.Equal(t, numSeries, it.SeriesCount())
	assert.Equal(t, metas, it.SeriesMeta())

	for i := 0; i < numSeries; i++ {
		assert.True(t, it.Next())
		assert.Equal(t, makeTs(float64(start+i)), it.Current().Datapoints())
	}

	assert.False(t, it.Next())
	assert.NoError(t, it.Err())
	it.Close()
}

func TestColumnSeriesIter(t *testing.T) {
	seriesIter, err := buildBlock().SeriesIter()
	assert.NoError(t, err)
	verifySeriesIter(t, seriesIter, metas, 1)
}

func TestColumnMultiSeriesIter(t *testing.T) {
	batches, err := buildBlock().MultiSeriesIter(2)
	assert.NoError(t, err)

	assert.Equal(t, 2, len(batches))

	assert.Equal(t, 2, batches[0].Size)
	it := batches[0].Iter
	verifySeriesIter(t, it, metas[:2], 1)

	assert.Equal(t, 1, batches[1].Size)
	it = batches[1].Iter
	verifySeriesIter(t, it, metas[2:], 3)
}

func TestColumnMultiSeriesIterLargeConcurrency(t *testing.T) {
	batches, err := buildBlock().MultiSeriesIter(5)
	assert.NoError(t, err)

	assert.Equal(t, 5, len(batches))

	for i := 0; i < 3; i++ {
		b := batches[i]
		assert.Equal(t, 1, b.Size)
		verifySeriesIter(t, b.Iter, metas[i:i+1], i+1)
	}

	for i := 3; i < 5; i++ {
		b := batches[i]
		assert.Equal(t, 0, b.Size)
		assert.False(t, b.Iter.Next())
	}
}
