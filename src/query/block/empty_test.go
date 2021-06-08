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

	"github.com/stretchr/testify/assert"

	"github.com/m3db/m3/src/query/models"
	xtime "github.com/m3db/m3/src/x/time"
)

var (
	emptyArgs = []interface{}{}
	start     = xtime.Now()
	steps     = 15
	testBound = models.Bounds{
		Start:    start,
		Duration: time.Minute * time.Duration(steps),
		StepSize: time.Minute,
	}
)

func TestEmptyBlock(t *testing.T) {
	meta := Metadata{
		Tags:   models.MustMakeTags("a", "b"),
		Bounds: testBound,
	}

	bl := NewEmptyBlock(meta)
	assert.True(t, meta.Equals(bl.Meta()))

	step, err := bl.StepIter()
	assert.NoError(t, err)
	assert.NoError(t, step.Err())
	assert.False(t, step.Next())
	assert.Equal(t, steps, step.StepCount())
	assert.Equal(t, []SeriesMeta{}, step.SeriesMeta())

	assert.NotPanics(t, func() {
		step.Close()
	})

	series, err := bl.SeriesIter()
	assert.NoError(t, err)
	assert.NoError(t, series.Err())
	assert.False(t, series.Next())
	assert.Equal(t, 0, series.SeriesCount())
	assert.Equal(t, []SeriesMeta{}, series.SeriesMeta())

	assert.NotPanics(t, func() {
		series.Close()
	})

	concurrency := 5
	batch, err := bl.MultiSeriesIter(concurrency)
	assert.NoError(t, err)
	assert.Equal(t, concurrency, len(batch))
	for _, b := range batch {
		assert.Equal(t, 1, b.Size)
		assert.False(t, b.Iter.Next())
	}

	assert.NoError(t, bl.Close())
}
