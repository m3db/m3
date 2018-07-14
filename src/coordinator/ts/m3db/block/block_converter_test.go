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

	"github.com/m3db/m3db/src/coordinator/models"
	"github.com/m3db/m3db/src/dbnode/encoding"
	"github.com/m3db/m3db/src/dbnode/ts"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newSeriesBlock(seriesIterator encoding.SeriesIterator, start, end time.Time) SeriesBlock {
	return SeriesBlock{
		start:          start,
		end:            end,
		seriesIterator: seriesIterator,
	}
}

func newMultiNamespaceSeries(ctrl *gomock.Controller, now time.Time, t *testing.T) ([]SeriesBlocks, []SeriesBlocks) {
	rawTagsOne := []string{"foo", "bar", "same", "tag"}
	seriesOneIterators := []encoding.SeriesIterator{
		newMockIterator(t, "test_one", rawTagsOne, ctrl, []ts.Datapoint{}),
		newMockIterator(t, "test_one", rawTagsOne, ctrl, []ts.Datapoint{}),
	}
	seriesOneBlockOne := newSeriesBlock(seriesOneIterators[0], now, now.Add(10*time.Minute))
	seriesOneBlockTwo := newSeriesBlock(seriesOneIterators[1], now.Add(10*time.Minute), now.Add(20*time.Minute))
	tagsOne, err := ident.NewTagStringsIterator(rawTagsOne...)
	require.NoError(t, err)

	seriesOne := SeriesBlocks{
		Blocks: []SeriesBlock{seriesOneBlockOne, seriesOneBlockTwo},
		ID:     ident.StringID("test_one"),
		Tags:   tagsOne,
	}

	rawTagsTwo := []string{"biz", "baz", "same", "tag"}
	seriesTwoIterators := []encoding.SeriesIterator{
		newMockIterator(t, "test_two", rawTagsTwo, ctrl, []ts.Datapoint{}),
		newMockIterator(t, "test_two", rawTagsTwo, ctrl, []ts.Datapoint{}),
	}

	seriesTwoBlockOne := newSeriesBlock(seriesTwoIterators[0], now, now.Add(10*time.Minute))
	seriesTwoBlockTwo := newSeriesBlock(seriesTwoIterators[1], now.Add(10*time.Minute), now.Add(20*time.Minute))
	tagsTwo, err := ident.NewTagStringsIterator(rawTagsTwo...)
	require.NoError(t, err)

	seriesTwo := SeriesBlocks{
		Blocks: []SeriesBlock{seriesTwoBlockOne, seriesTwoBlockTwo},
		ID:     ident.StringID("test_two"),
		Tags:   tagsTwo,
	}

	return []SeriesBlocks{seriesOne}, []SeriesBlocks{seriesTwo}
}

func newMockIterator(t *testing.T, id string, tags []string, ctrl *gomock.Controller, dps []ts.Datapoint) encoding.SeriesIterator {
	seriesIterator := encoding.NewMockSeriesIterator(ctrl)

	seriesIterator.EXPECT().ID().Return(ident.StringID(id))

	tagIter, err := ident.NewTagStringsIterator(tags...)
	require.NoError(t, err)
	seriesIterator.EXPECT().Tags().Return(tagIter)

	seriesIterator.EXPECT().Next().Return(true).Times(len(dps))
	seriesIterator.EXPECT().Next().Return(false)

	for _, dp := range dps {
		seriesIterator.EXPECT().Current().Return(ts.Datapoint{Timestamp: dp.Timestamp, Value: dp.Value}, xtime.Millisecond, nil).Times(1)
	}

	return seriesIterator
}

func TestConvertM3Blocks(t *testing.T) {
	now := time.Now()
	ctrl := gomock.NewController(t)
	seriesOne, seriesTwo := newMultiNamespaceSeries(ctrl, now, t)
	multiNamespaceSeriesList := []MultiNamespaceSeries{seriesOne, seriesTwo}
	m3CoordBlocks, err := SeriesBlockToMultiSeriesBlocks(multiNamespaceSeriesList, nil, time.Minute)
	require.NoError(t, err)

	require.Len(t, m3CoordBlocks, 2)
	require.Len(t, m3CoordBlocks[0].Blocks, 2)
	require.Len(t, m3CoordBlocks[1].Blocks, 2)

	assert.Equal(t, "test_one", m3CoordBlocks[0].Blocks[0].ConsolidatedNSBlocks[0].SeriesIterators.Iters()[0].ID().String())
	assert.Equal(t, models.Tags{"foo": "bar", "same": "tag"}, m3CoordBlocks[0].Blocks[0].Metadata.Tags)
	assert.Equal(t, "test_two", m3CoordBlocks[0].Blocks[1].ConsolidatedNSBlocks[0].SeriesIterators.Iters()[0].ID().String())
	assert.Equal(t, models.Tags{"biz": "baz", "same": "tag"}, m3CoordBlocks[0].Blocks[1].Metadata.Tags)
	assert.Equal(t, now, m3CoordBlocks[0].Metadata.Bounds.Start)
	assert.Equal(t, now.Add(10*time.Minute), m3CoordBlocks[0].Metadata.Bounds.End)

	assert.Equal(t, "test_one", m3CoordBlocks[1].Blocks[0].ConsolidatedNSBlocks[0].SeriesIterators.Iters()[0].ID().String())
	assert.Equal(t, "test_two", m3CoordBlocks[1].Blocks[1].ConsolidatedNSBlocks[0].SeriesIterators.Iters()[0].ID().String())
	assert.Equal(t, now.Add(10*time.Minute), m3CoordBlocks[1].Metadata.Bounds.Start)
	assert.Equal(t, now.Add(20*time.Minute), m3CoordBlocks[1].Metadata.Bounds.End)

	assert.Equal(t, models.Tags{"same": "tag"}, m3CoordBlocks[0].Metadata.Tags)
	assert.Equal(t, models.Tags{"same": "tag"}, m3CoordBlocks[1].Metadata.Tags)
}

func TestMultipleNamespacesSuccess(t *testing.T) {
	now := time.Now()
	ctrl := gomock.NewController(t)
	seriesOne, seriesTwo := newMultiNamespaceSeries(ctrl, now, t)
	seriesOne = append(seriesOne, seriesOne...)
	multiNamespaceSeriesList := []MultiNamespaceSeries{seriesOne, seriesTwo}
	m3CoordBlocks, err := SeriesBlockToMultiSeriesBlocks(multiNamespaceSeriesList, nil, time.Minute)
	require.NoError(t, err)

	require.Len(t, m3CoordBlocks, 2)
	require.Len(t, m3CoordBlocks[0].Blocks, 2)
	require.Len(t, m3CoordBlocks[1].Blocks, 2)

	assert.Equal(t, "test_one", m3CoordBlocks[0].Blocks[0].ConsolidatedNSBlocks[0].SeriesIterators.Iters()[0].ID().String())
	assert.Equal(t, models.Tags{"foo": "bar", "same": "tag"}, m3CoordBlocks[0].Blocks[0].Metadata.Tags)
	assert.Equal(t, "test_two", m3CoordBlocks[0].Blocks[1].ConsolidatedNSBlocks[0].SeriesIterators.Iters()[0].ID().String())
	assert.Equal(t, models.Tags{"biz": "baz", "same": "tag"}, m3CoordBlocks[0].Blocks[1].Metadata.Tags)
	assert.Equal(t, now, m3CoordBlocks[0].Metadata.Bounds.Start)
	assert.Equal(t, now.Add(10*time.Minute), m3CoordBlocks[0].Metadata.Bounds.End)
	// NB(braskin): once consolidating multiple namespaces is supported, the length will always be 1
	assert.Equal(t, 2, len(m3CoordBlocks[0].Blocks[0].ConsolidatedNSBlocks))

	assert.Equal(t, "test_one", m3CoordBlocks[1].Blocks[0].ConsolidatedNSBlocks[0].SeriesIterators.Iters()[0].ID().String())
	assert.Equal(t, "test_two", m3CoordBlocks[1].Blocks[1].ConsolidatedNSBlocks[0].SeriesIterators.Iters()[0].ID().String())
	assert.Equal(t, now.Add(10*time.Minute), m3CoordBlocks[1].Metadata.Bounds.Start)
	assert.Equal(t, now.Add(20*time.Minute), m3CoordBlocks[1].Metadata.Bounds.End)
	assert.Equal(t, 2, len(m3CoordBlocks[1].Blocks[0].ConsolidatedNSBlocks))

	assert.Equal(t, models.Tags{"same": "tag"}, m3CoordBlocks[0].Metadata.Tags)
	assert.Equal(t, models.Tags{"same": "tag"}, m3CoordBlocks[1].Metadata.Tags)
}

func TestBlockMisalignment(t *testing.T) {
	now := time.Now()
	ctrl := gomock.NewController(t)
	seriesOne, seriesTwo := newMultiNamespaceSeries(ctrl, now, t)
	seriesOne[0].Blocks[0].end = now.Add(100 * time.Minute)
	multiNamespaceSeriesList := []MultiNamespaceSeries{seriesOne, seriesTwo}
	_, err := SeriesBlockToMultiSeriesBlocks(multiNamespaceSeriesList, nil, time.Minute)
	require.Error(t, err)
}
