// Copyright (c) 2016 Uber Technologies, Inc.
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

package storage

import (
	"testing"
	"time"

	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/mocks"
	xerrors "github.com/m3db/m3db/x/errors"
	xtime "github.com/m3db/m3db/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newSeriesTestOptions() m3db.DatabaseOptions {
	return NewDatabaseOptions().
		BlockSize(2 * time.Minute).
		BufferFuture(10 * time.Second).
		BufferPast(10 * time.Second).
		BufferDrain(30 * time.Second).
		RetentionPeriod(time.Hour)
}

func TestSeriesEmpty(t *testing.T) {
	opts := newSeriesTestOptions()
	series := newDatabaseSeries("foo", bootstrapped, opts).(*dbSeries)
	assert.True(t, series.Empty())
}

func TestSeriesWriteFlush(t *testing.T) {
	opts := newSeriesTestOptions()
	curr := time.Now().Truncate(opts.GetBlockSize())
	start := curr
	opts = opts.NowFn(func() time.Time {
		return curr
	})
	series := newDatabaseSeries("foo", bootstrapped, opts).(*dbSeries)

	data := []value{
		{curr, 1, xtime.Second, nil},
		{curr.Add(mins(1)), 2, xtime.Second, nil},
		{curr.Add(mins(2)), 3, xtime.Second, nil},
		{curr.Add(mins(3)), 4, xtime.Second, nil},
	}

	for _, v := range data {
		curr = v.timestamp
		ctx := context.NewContext()
		assert.NoError(t, series.Write(ctx, v.timestamp, v.value, xtime.Second, v.annotation))
		ctx.Close()
	}

	assert.Equal(t, true, series.buffer.NeedsDrain())

	// Tick the series which should cause a drain
	assert.NoError(t, series.Tick())

	assert.Equal(t, false, series.buffer.NeedsDrain())

	blocks := series.blocks.GetAllBlocks()
	assert.Len(t, blocks, 1)

	block, ok := series.blocks.GetBlockAt(start)
	assert.Equal(t, true, ok)

	stream, err := block.Stream(nil)
	require.NoError(t, err)
	assertValuesEqual(t, data[:2], [][]m3db.SegmentReader{[]m3db.SegmentReader{
		stream,
	}}, opts)
}

func TestSeriesWriteFlushRead(t *testing.T) {
	opts := newSeriesTestOptions()
	curr := time.Now().Truncate(opts.GetBlockSize())
	start := curr
	opts = opts.NowFn(func() time.Time {
		return curr
	})
	series := newDatabaseSeries("foo", bootstrapped, opts).(*dbSeries)

	data := []value{
		{curr.Add(mins(1)), 2, xtime.Second, nil},
		{curr.Add(mins(3)), 3, xtime.Second, nil},
		{curr.Add(mins(5)), 4, xtime.Second, nil},
		{curr.Add(mins(7)), 4, xtime.Second, nil},
		{curr.Add(mins(9)), 4, xtime.Second, nil},
	}

	for _, v := range data {
		curr = v.timestamp
		ctx := context.NewContext()
		assert.NoError(t, series.Write(ctx, v.timestamp, v.value, xtime.Second, v.annotation))
		ctx.Close()
	}

	ctx := context.NewContext()
	defer ctx.Close()

	// Test fine grained range
	results, err := series.ReadEncoded(ctx, start, start.Add(mins(10)))
	assert.NoError(t, err)

	assertValuesEqual(t, data, results, opts)

	// Test wide range
	results, err = series.ReadEncoded(ctx, timeZero, timeDistantFuture)
	assert.NoError(t, err)

	assertValuesEqual(t, data, results, opts)
}

func TestSeriesReadEndBeforeStart(t *testing.T) {
	opts := newSeriesTestOptions()
	series := newDatabaseSeries("foo", bootstrapped, opts).(*dbSeries)

	ctx := context.NewContext()
	defer ctx.Close()

	results, err := series.ReadEncoded(ctx, time.Now(), time.Now().Add(-1*time.Second))
	assert.Error(t, err)
	assert.True(t, xerrors.IsInvalidParams(err))
	assert.Nil(t, results)
}

func TestSeriesFlushToDiskNoBlock(t *testing.T) {
	opts := newSeriesTestOptions()
	series := newDatabaseSeries("foo", bootstrapped, opts).(*dbSeries)
	flushTime := time.Unix(7200, 0)
	err := series.FlushToDisk(nil, nil, flushTime, nil)
	require.Nil(t, err)
}

func TestSeriesFlushToDisk(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSeriesTestOptions()
	series := newDatabaseSeries("foo", bootstrapped, opts).(*dbSeries)
	flushTime := time.Unix(7200, 0)
	head := []byte{0x1, 0x2}
	tail := []byte{0x3, 0x4}
	segmentHolder := [][]byte{head, tail}

	var res m3db.Segment
	encoder := mocks.NewMockEncoder(ctrl)
	reader := mocks.NewMockSegmentReader(ctrl)
	encoder.EXPECT().Stream().Return(reader)
	reader.EXPECT().Segment().Return(m3db.Segment{Head: head, Tail: tail})
	writer := mocks.NewMockFileSetWriter(ctrl)
	writer.EXPECT().WriteAll("foo", segmentHolder).Do(func(_ string, holder [][]byte) {
		res.Head = holder[0]
		res.Tail = holder[1]
	}).Return(nil)

	block := opts.GetDatabaseBlockPool().Get()
	block.Reset(flushTime, encoder)
	series.blocks.AddBlock(block)
	err := series.FlushToDisk(nil, writer, flushTime, segmentHolder)
	require.Nil(t, err)
}

func TestSeriesTickEmptySeries(t *testing.T) {
	opts := newSeriesTestOptions()
	series := newDatabaseSeries("foo", bootstrapped, opts).(*dbSeries)
	err := series.Tick()
	require.Equal(t, errSeriesAllDatapointsExpired, err)
}

func TestSeriesTickNeedsDrain(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSeriesTestOptions()
	series := newDatabaseSeries("foo", bootstrapped, opts).(*dbSeries)
	buffer := mocks.NewMockdatabaseBuffer(ctrl)
	series.buffer = buffer
	buffer.EXPECT().Empty().Return(false)
	buffer.EXPECT().NeedsDrain().Return(true)
	buffer.EXPECT().DrainAndReset(false)
	err := series.Tick()
	require.NoError(t, err)
}

func TestSeriesTickNeedsBlockExpiry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSeriesTestOptions()
	curr := time.Now().Truncate(opts.GetBlockSize())
	opts = opts.NowFn(func() time.Time {
		return curr
	})
	series := newDatabaseSeries("foo", bootstrapped, opts).(*dbSeries)
	blockStart := curr.Add(-opts.GetRetentionPeriod()).Add(-opts.GetBlockSize())
	block := mocks.NewMockDatabaseBlock(ctrl)
	block.EXPECT().StartTime().Return(blockStart)
	block.EXPECT().Close()
	series.blocks.AddBlock(block)
	block = mocks.NewMockDatabaseBlock(ctrl)
	block.EXPECT().StartTime().Return(curr)
	series.blocks.AddBlock(block)
	require.Equal(t, blockStart, series.blocks.GetMinTime())
	require.Equal(t, 2, series.blocks.Len())
	buffer := mocks.NewMockdatabaseBuffer(ctrl)
	series.buffer = buffer
	buffer.EXPECT().Empty().Return(true)
	buffer.EXPECT().NeedsDrain().Return(false)
	err := series.Tick()
	require.NoError(t, err)
	require.Equal(t, 1, series.blocks.Len())
	require.Equal(t, curr, series.blocks.GetMinTime())
	_, exists := series.blocks.GetAllBlocks()[curr]
	require.True(t, exists)
}

func TestShouldExpire(t *testing.T) {
	opts := newSeriesTestOptions()
	series := newDatabaseSeries("foo", bootstrapped, opts).(*dbSeries)
	now := time.Now()
	require.False(t, series.shouldExpire(now, now))
	require.True(t, series.shouldExpire(now, now.Add(-opts.GetRetentionPeriod()).Add(-opts.GetBlockSize())))
}
