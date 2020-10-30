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

func TestSetRow(t *testing.T) {
	buildMeta := func(i int) SeriesMeta {
		name := fmt.Sprint(i)

		return SeriesMeta{
			Name: []byte(name),
			Tags: models.MustMakeTags("name", name),
		}
	}

	size := 10
	metas := make([]SeriesMeta, size)
	for i := range metas {
		metas[i] = buildMeta(i)
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
