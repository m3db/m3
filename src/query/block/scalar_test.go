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
	val    = 13.37
	bounds = models.Bounds{
		Start:    start,
		Duration: time.Minute,
		StepSize: time.Second * 10,
	}
)

func TestScalarBlock(t *testing.T) {
	tagOpts := models.NewTagOptions().SetBucketName([]byte("custom_bucket"))
	tags := models.NewTags(1, tagOpts).AddTag(models.Tag{
		Name:  []byte("a"),
		Value: []byte("b"),
	})

	block := NewScalar(
		val,
		Metadata{
			Bounds: bounds,
			Tags:   tags,
		},
	)

	assert.Equal(t, BlockScalar, block.Info().Type())
	require.IsType(t, block, &Scalar{})
	stepIter, err := block.StepIter()
	require.NoError(t, err)
	require.NotNil(t, stepIter)
	meta := block.Meta()
	require.True(t, meta.ResultMetadata.Exhaustive)
	require.True(t, meta.ResultMetadata.LocalOnly)
	require.Equal(t, 0, len(meta.ResultMetadata.Warnings))
	verifyMetas(t, block.Meta(), stepIter.SeriesMeta(), tagOpts)

	assert.Equal(t, 6, stepIter.StepCount())
	valCounts := 0
	for stepIter.Next() {
		v := stepIter.Current()
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
	meta = block.Meta()
	require.True(t, meta.ResultMetadata.Exhaustive)
	require.True(t, meta.ResultMetadata.LocalOnly)
	require.Equal(t, 0, len(meta.ResultMetadata.Warnings))

	require.NoError(t, block.Close())
}

func verifyMetas(
	t *testing.T,
	meta Metadata,
	seriesMeta []SeriesMeta,
	opts models.TagOptions,
) {
	// Verify meta
	assert.True(t, bounds.Equals(meta.Bounds))

	// Verify seriesMeta
	assert.Len(t, seriesMeta, 1)
	sMeta := seriesMeta[0]
	assert.Equal(t, 0, sMeta.Tags.Len())
	assert.Equal(t, []byte(nil), sMeta.Name)
}
