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

	"github.com/m3db/m3db/src/dbnode/encoding"
	"github.com/m3db/m3x/ident"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newMultiNamespaceSeries(ctrl *gomock.Controller, now time.Time) ([]SeriesBlocks, []SeriesBlocks) {
	seriesBlocksOne := newM3SeriesBlock("test_one", ctrl, now)
	seriesOne := SeriesBlocks{
		Blocks: seriesBlocksOne,
		ID:     ident.StringID("test_one"),
	}

	seriesBlocksTwo := newM3SeriesBlock("test_two", ctrl, now)
	seriesTwo := SeriesBlocks{
		Blocks: seriesBlocksTwo,
		ID:     ident.StringID("test_two"),
	}

	return []SeriesBlocks{seriesOne}, []SeriesBlocks{seriesTwo}
}

func newM3SeriesBlock(id string, ctrl *gomock.Controller, now time.Time) []SeriesBlock {
	seriesIterOne := encoding.NewMockSeriesIterator(ctrl)
	seriesIterTwo := encoding.NewMockSeriesIterator(ctrl)

	seriesIterOne.EXPECT().ID().Return(ident.StringID(id))
	seriesIterTwo.EXPECT().ID().Return(ident.StringID(id))

	sOne := SeriesBlock{
		Start:          now,
		End:            now.Add(10 * time.Minute),
		SeriesIterator: seriesIterOne,
	}
	sTwo := SeriesBlock{
		Start:          now.Add(10 * time.Minute),
		End:            now.Add(20 * time.Minute),
		SeriesIterator: seriesIterTwo,
	}

	return []SeriesBlock{sOne, sTwo}
}

func TestConvertM3Blocks(t *testing.T) {
	now := time.Now()
	ctrl := gomock.NewController(t)
	seriesOne, seriesTwo := newMultiNamespaceSeries(ctrl, now)
	multiNamespaceSeriesList := []MultiNamespaceSeries{seriesOne, seriesTwo}
	m3CoordBlocks, err := SeriesBlockToMultiSeriesBlocks(multiNamespaceSeriesList, nil)
	require.NoError(t, err)

	assert.Equal(t, "test_one", m3CoordBlocks[0].Blocks[0].ConsolidatedNSBlocks[0].SeriesIterators.Iters()[0].ID().String())
	assert.Equal(t, "test_two", m3CoordBlocks[0].Blocks[1].ConsolidatedNSBlocks[0].SeriesIterators.Iters()[0].ID().String())
	assert.Equal(t, now, m3CoordBlocks[0].Start)
	assert.Equal(t, now.Add(10*time.Minute), m3CoordBlocks[0].End)

	assert.Equal(t, "test_one", m3CoordBlocks[1].Blocks[0].ConsolidatedNSBlocks[0].SeriesIterators.Iters()[0].ID().String())
	assert.Equal(t, "test_two", m3CoordBlocks[1].Blocks[1].ConsolidatedNSBlocks[0].SeriesIterators.Iters()[0].ID().String())
	assert.Equal(t, now.Add(10*time.Minute), m3CoordBlocks[1].Start)
	assert.Equal(t, now.Add(20*time.Minute), m3CoordBlocks[1].End)
}

func TestMultipleNamespacesSuccess(t *testing.T) {
	now := time.Now()
	ctrl := gomock.NewController(t)
	seriesOne, seriesTwo := newMultiNamespaceSeries(ctrl, now)
	seriesOne = append(seriesOne, seriesOne...)
	multiNamespaceSeriesList := []MultiNamespaceSeries{seriesOne, seriesTwo}
	m3CoordBlocks, err := SeriesBlockToMultiSeriesBlocks(multiNamespaceSeriesList, nil)
	require.NoError(t, err)

	assert.Equal(t, "test_one", m3CoordBlocks[0].Blocks[0].ConsolidatedNSBlocks[0].SeriesIterators.Iters()[0].ID().String())
	assert.Equal(t, "test_two", m3CoordBlocks[0].Blocks[1].ConsolidatedNSBlocks[0].SeriesIterators.Iters()[0].ID().String())
	assert.Equal(t, now, m3CoordBlocks[0].Start)
	assert.Equal(t, now.Add(10*time.Minute), m3CoordBlocks[0].End)
	// NB(braskin): once consolidating multiple namespaces is supported, the length will always be 1
	assert.Equal(t, 2, len(m3CoordBlocks[0].Blocks[0].ConsolidatedNSBlocks))

	assert.Equal(t, "test_one", m3CoordBlocks[1].Blocks[0].ConsolidatedNSBlocks[0].SeriesIterators.Iters()[0].ID().String())
	assert.Equal(t, "test_two", m3CoordBlocks[1].Blocks[1].ConsolidatedNSBlocks[0].SeriesIterators.Iters()[0].ID().String())
	assert.Equal(t, now.Add(10*time.Minute), m3CoordBlocks[1].Start)
	assert.Equal(t, now.Add(20*time.Minute), m3CoordBlocks[1].End)
	assert.Equal(t, 2, len(m3CoordBlocks[1].Blocks[0].ConsolidatedNSBlocks))
}

func TestBlockMisalignment(t *testing.T) {
	now := time.Now()
	ctrl := gomock.NewController(t)
	seriesOne, seriesTwo := newMultiNamespaceSeries(ctrl, now)
	seriesOne[0].Blocks[0].End = now.Add(100 * time.Minute)
	multiNamespaceSeriesList := []MultiNamespaceSeries{seriesOne, seriesTwo}
	_, err := SeriesBlockToMultiSeriesBlocks(multiNamespaceSeriesList, nil)
	require.Error(t, err)
}
