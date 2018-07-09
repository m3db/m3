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

	"github.com/m3db/m3db/src/coordinator/models"
	"github.com/m3db/m3db/src/dbnode/encoding"
	"github.com/m3db/m3db/src/dbnode/ts"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newMultiNamespaceSeries(ctrl *gomock.Controller, now time.Time, t *testing.T) ([]SeriesBlocks, []SeriesBlocks) {
	rawTagsOne := []string{"foo", "bar", "same", "tag"}
	seriesBlocksOne := newM3SeriesBlock("test_one", ctrl, now, rawTagsOne, t)
	tagsOne, err := ident.NewTagStringsIterator(rawTagsOne...)
	require.NoError(t, err)

	seriesOne := SeriesBlocks{
		Blocks: seriesBlocksOne,
		ID:     ident.StringID("test_one"),
		Tags:   tagsOne,
	}

	rawTagsTwo := []string{"biz", "baz", "same", "tag"}
	seriesBlocksTwo := newM3SeriesBlock("test_two", ctrl, now, rawTagsTwo, t)
	tagsTwo, err := ident.NewTagStringsIterator(rawTagsTwo...)
	require.NoError(t, err)

	seriesTwo := SeriesBlocks{
		Blocks: seriesBlocksTwo,
		ID:     ident.StringID("test_two"),
		Tags:   tagsTwo,
	}

	return []SeriesBlocks{seriesOne}, []SeriesBlocks{seriesTwo}
}

func newM3SeriesBlock(id string, ctrl *gomock.Controller, now time.Time, tags []string, t *testing.T) []SeriesBlock {
	seriesIterOne := encoding.NewMockSeriesIterator(ctrl)
	seriesIterTwo := encoding.NewMockSeriesIterator(ctrl)

	seriesIterOne.EXPECT().ID().Return(ident.StringID(id))
	seriesIterTwo.EXPECT().ID().Return(ident.StringID(id))

	tagIter, err := ident.NewTagStringsIterator(tags...)
	require.NoError(t, err)

	seriesIterOne.EXPECT().Tags().Return(tagIter)
	seriesIterTwo.EXPECT().Tags().Return(tagIter)

	seriesIterOne.EXPECT().Tags().Return(tagIter)
	seriesIterTwo.EXPECT().Tags().Return(tagIter)

	seriesIterOne.EXPECT().Next().Return(true).Times(3)
	seriesIterOne.EXPECT().Next().Return(false)

	seriesIterTwo.EXPECT().Next().Return(true).Times(3)
	seriesIterTwo.EXPECT().Next().Return(false)

	seriesIterOne.EXPECT().Current().Return(ts.Datapoint{Timestamp: now.Add(1 * time.Minute), Value: 1}, xtime.Millisecond, nil).Times(1)
	seriesIterOne.EXPECT().Current().Return(ts.Datapoint{Timestamp: now.Add(90 * time.Second), Value: 2}, xtime.Millisecond, nil).Times(1)
	seriesIterOne.EXPECT().Current().Return(ts.Datapoint{Timestamp: now.Add(5 * time.Minute), Value: 3}, xtime.Millisecond, nil).Times(1)

	seriesIterTwo.EXPECT().Current().Return(ts.Datapoint{}, xtime.Millisecond, nil).Times(1)
	seriesIterTwo.EXPECT().Current().Return(ts.Datapoint{Timestamp: now.Add(17 * time.Minute), Value: 5}, xtime.Millisecond, nil).Times(1)
	seriesIterTwo.EXPECT().Current().Return(ts.Datapoint{Timestamp: now.Add(19 * time.Minute), Value: 6}, xtime.Millisecond, nil).Times(1)

	sOne := SeriesBlock{
		start:          now,
		end:            now.Add(10 * time.Minute),
		seriesIterator: seriesIterOne,
	}
	sTwo := SeriesBlock{
		start:          now.Add(10 * time.Minute),
		end:            now.Add(20 * time.Minute),
		seriesIterator: seriesIterTwo,
	}

	return []SeriesBlock{sOne, sTwo}
}

func TestConvertM3Blocks(t *testing.T) {
	now := time.Now()
	ctrl := gomock.NewController(t)
	seriesOne, seriesTwo := newMultiNamespaceSeries(ctrl, now, t)
	multiNamespaceSeriesList := []MultiNamespaceSeries{seriesOne, seriesTwo}
	m3CoordBlocks, err := SeriesBlockToMultiSeriesBlocks(multiNamespaceSeriesList, nil, time.Minute)
	require.NoError(t, err)

	assert.Equal(t, "test_one", m3CoordBlocks[0].Blocks[0].ConsolidatedNSBlocks[0].SeriesIterators.Iters()[0].ID().String())
	assert.Equal(t, models.Tags{"foo": "bar"}, m3CoordBlocks[0].Blocks[0].Metadata.Tags)
	assert.Equal(t, "test_two", m3CoordBlocks[0].Blocks[1].ConsolidatedNSBlocks[0].SeriesIterators.Iters()[0].ID().String())
	assert.Equal(t, models.Tags{"biz": "baz"}, m3CoordBlocks[0].Blocks[1].Metadata.Tags)
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

	assert.Equal(t, "test_one", m3CoordBlocks[0].Blocks[0].ConsolidatedNSBlocks[0].SeriesIterators.Iters()[0].ID().String())
	assert.Equal(t, models.Tags{"foo": "bar"}, m3CoordBlocks[0].Blocks[0].Metadata.Tags)
	assert.Equal(t, "test_two", m3CoordBlocks[0].Blocks[1].ConsolidatedNSBlocks[0].SeriesIterators.Iters()[0].ID().String())
	assert.Equal(t, models.Tags{"biz": "baz"}, m3CoordBlocks[0].Blocks[1].Metadata.Tags)
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
