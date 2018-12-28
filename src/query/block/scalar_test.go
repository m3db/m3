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

package block

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/query/models"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	start  = time.Time{}
	val    = 13.37
	bounds = models.Bounds{
		Start:    start,
		Duration: time.Minute,
		StepSize: time.Second * 10,
	}
)

func TestScalarBlock(t *testing.T) {
	block := NewScalar(func(_ time.Time) float64 { return val }, bounds)

	require.IsType(t, block, &Scalar{})

	stepIter, err := block.StepIter()
	require.NoError(t, err)
	require.NotNil(t, stepIter)

	verifyMetas(t, stepIter.Meta(), stepIter.SeriesMeta())

	assert.Equal(t, 6, stepIter.StepCount())
	v := stepIter.Current()
	require.Error(t, stepIter.Err())
	require.Nil(t, v)

	valCounts := 0
	for stepIter.Next() {
		v = stepIter.Current()
		require.NotNil(t, v)

		expectedTime := start.Add(time.Duration(valCounts) * 10 * time.Second)
		assert.Equal(t, expectedTime, v.Time())

		vals := v.Values()
		require.Len(t, vals, 1)
		require.Equal(t, val, vals[0])

		valCounts++
	}

	require.NoError(t, stepIter.Err())
	assert.Equal(t, 6, valCounts)
	v = stepIter.Current()
	require.Error(t, stepIter.Err())
	require.Nil(t, v)

	seriesIter, err := block.SeriesIter()
	require.NoError(t, err)
	require.NotNil(t, seriesIter)

	verifyMetas(t, seriesIter.Meta(), seriesIter.SeriesMeta())
	require.Equal(t, 1, seriesIter.SeriesCount())

	series := seriesIter.Current()
	require.Error(t, seriesIter.Err())

	require.True(t, seriesIter.Next())
	series = seriesIter.Current()
	require.NoError(t, seriesIter.Err())

	assert.Equal(t, 6, series.Len())
	vals := series.Values()
	require.Len(t, vals, 6)
	for _, actual := range vals {
		assert.Equal(t, val, actual)
	}

	assert.Equal(t, 0, series.Meta.Tags.Len())
	assert.Equal(t, "", series.Meta.Name)

	require.False(t, seriesIter.Next())
	require.Error(t, seriesIter.Err())

	series = seriesIter.Current()
	err = block.Close()
	require.NoError(t, err)
}

func verifyMetas(t *testing.T, meta Metadata, seriesMeta []SeriesMeta) {
	// Verify meta
	assert.Equal(t, bounds, meta.Bounds)
	assert.Equal(t, 0, meta.Tags.Len())

	// Verify seriesMeta
	assert.Len(t, seriesMeta, 1)
	sMeta := seriesMeta[0]
	assert.Equal(t, 0, sMeta.Tags.Len())
	assert.Equal(t, "", sMeta.Name)
}
