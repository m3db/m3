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
	"errors"
	"testing"
	"time"

	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/ts"
	xio "github.com/m3db/m3db/x/io"
	"github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newSeriesTestOptions() Options {
	opts := NewOptions()
	opts = opts.
		SetRetentionOptions(opts.RetentionOptions().
			SetBlockSize(2 * time.Minute).
			SetBufferFuture(10 * time.Second).
			SetBufferPast(10 * time.Second).
			SetBufferDrain(30 * time.Second).
			SetRetentionPeriod(time.Hour)).
		SetDatabaseBlockOptions(opts.DatabaseBlockOptions().
			SetContextPool(opts.ContextPool()).
			SetEncoderPool(opts.EncoderPool()).
			SetSegmentReaderPool(opts.SegmentReaderPool()).
			SetBytesPool(opts.BytesPool()))
	return opts
}

func TestSeriesEmpty(t *testing.T) {
	opts := newSeriesTestOptions()
	series := newDatabaseSeries("foo", bootstrapped, opts).(*dbSeries)
	assert.True(t, series.Empty())
}

func TestSeriesWriteFlush(t *testing.T) {
	opts := newSeriesTestOptions()
	curr := time.Now().Truncate(opts.RetentionOptions().BlockSize())
	start := curr
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
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

	blocks := series.blocks.AllBlocks()
	assert.Len(t, blocks, 1)

	block, ok := series.blocks.BlockAt(start)
	assert.Equal(t, true, ok)

	stream, err := block.Stream(nil)
	require.NoError(t, err)
	assertValuesEqual(t, data[:2], [][]xio.SegmentReader{[]xio.SegmentReader{
		stream,
	}}, opts)
}

func TestSeriesWriteFlushRead(t *testing.T) {
	opts := newSeriesTestOptions()
	curr := time.Now().Truncate(opts.RetentionOptions().BlockSize())
	start := curr
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
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

func TestSeriesFlushNoBlock(t *testing.T) {
	opts := newSeriesTestOptions()
	series := newDatabaseSeries("foo", bootstrapped, opts).(*dbSeries)
	flushTime := time.Unix(7200, 0)
	err := series.Flush(nil, flushTime, nil)
	require.Nil(t, err)
}

func TestSeriesFlush(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSeriesTestOptions()
	series := newDatabaseSeries("foo", bootstrapped, opts).(*dbSeries)
	flushTime := time.Unix(7200, 0)
	head := []byte{0x1, 0x2}
	tail := []byte{0x3, 0x4}

	encoder := encoding.NewMockEncoder(ctrl)
	reader := xio.NewMockSegmentReader(ctrl)
	encoder.EXPECT().Stream().Return(reader).Times(2)
	reader.EXPECT().Segment().Return(ts.Segment{Head: head, Tail: tail}).Times(2)

	block := opts.DatabaseBlockOptions().DatabaseBlockPool().Get()
	block.Reset(flushTime, encoder)
	series.blocks.AddBlock(block)

	inputs := []error{errors.New("some error"), nil}
	for _, input := range inputs {
		persistFn := func(id string, segment ts.Segment) error { return input }
		err := series.Flush(nil, flushTime, persistFn)
		require.Equal(t, input, err)
	}
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
	buffer := NewMockdatabaseBuffer(ctrl)
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
	ropts := opts.RetentionOptions()
	curr := time.Now().Truncate(ropts.BlockSize())
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	series := newDatabaseSeries("foo", bootstrapped, opts).(*dbSeries)
	blockStart := curr.Add(-ropts.RetentionPeriod()).Add(-ropts.BlockSize())
	b := block.NewMockDatabaseBlock(ctrl)
	b.EXPECT().StartTime().Return(blockStart)
	b.EXPECT().Close()
	series.blocks.AddBlock(b)
	b = block.NewMockDatabaseBlock(ctrl)
	b.EXPECT().StartTime().Return(curr)
	b.EXPECT().IsSealed().Return(false)
	series.blocks.AddBlock(b)
	require.Equal(t, blockStart, series.blocks.MinTime())
	require.Equal(t, 2, series.blocks.Len())
	buffer := NewMockdatabaseBuffer(ctrl)
	series.buffer = buffer
	buffer.EXPECT().Empty().Return(true)
	buffer.EXPECT().NeedsDrain().Return(false)
	err := series.Tick()
	require.NoError(t, err)
	require.Equal(t, 1, series.blocks.Len())
	require.Equal(t, curr, series.blocks.MinTime())
	_, exists := series.blocks.AllBlocks()[curr]
	require.True(t, exists)
}

func TestSeriesTickNeedsBlockSeal(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSeriesTestOptions()
	ropts := opts.RetentionOptions()
	curr := time.Now().Truncate(ropts.BlockSize())
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	series := newDatabaseSeries("foo", bootstrapped, opts).(*dbSeries)
	b := block.NewMockDatabaseBlock(ctrl)
	b.EXPECT().StartTime().Return(curr)
	b.EXPECT().IsSealed().Return(false).MinTimes(1).MaxTimes(2)
	series.blocks.AddBlock(b)
	blockStart := curr.Add(-ropts.BufferPast()).Add(-2 * ropts.BlockSize())
	b = block.NewMockDatabaseBlock(ctrl)
	b.EXPECT().StartTime().Return(blockStart)
	b.EXPECT().IsSealed().Return(false).Times(2)
	b.EXPECT().Seal()
	series.blocks.AddBlock(b)
	buffer := NewMockdatabaseBuffer(ctrl)
	series.buffer = buffer
	buffer.EXPECT().Empty().Return(true)
	buffer.EXPECT().NeedsDrain().Return(false)
	err := series.Tick()
	require.NoError(t, err)
}

func TestSeriesBootstrapWithError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSeriesTestOptions()
	series := newDatabaseSeries("foo", bootstrapNotStarted, opts).(*dbSeries)
	buffer := NewMockdatabaseBuffer(ctrl)
	buffer.EXPECT().DrainAndReset(true)
	series.buffer = buffer

	faultyEncoder := opts.EncoderPool().Get()
	faultyEncoder.ResetSetData(time.Now(), []byte{0x1, 0x2, 0x3}, true)
	series.pendingBootstrap = []pendingBootstrapDrain{pendingBootstrapDrain{encoder: faultyEncoder}}
	err := series.Bootstrap(nil, time.Now())

	require.NotNil(t, err)
	require.Equal(t, "error occurred bootstrapping series foo: EOF", err.Error())
	require.Equal(t, bootstrapped, series.bs)
	require.Equal(t, 0, series.blocks.Len())
}

func TestShouldExpire(t *testing.T) {
	opts := newSeriesTestOptions()
	ropts := opts.RetentionOptions()
	series := newDatabaseSeries("foo", bootstrapped, opts).(*dbSeries)
	now := time.Now()
	require.False(t, series.shouldExpire(now, now))
	require.True(t, series.shouldExpire(now, now.Add(-ropts.RetentionPeriod()).Add(-ropts.BlockSize())))
}

func TestShouldSeal(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSeriesTestOptions()
	ropts := opts.RetentionOptions()
	series := newDatabaseSeries("foo", bootstrapped, opts).(*dbSeries)
	now := time.Now()
	inputs := []struct {
		blockStart     time.Time
		expectedSeal   bool
		expectedResult bool
	}{
		{now, true, false},
		{now, false, false},
		{now.Add(-ropts.BufferPast()).Add(-2 * ropts.BlockSize()), false, true},
		{now.Add(-ropts.BufferPast()).Add(-2 * ropts.BlockSize()), true, false},
	}

	for _, input := range inputs {
		block := block.NewMockDatabaseBlock(ctrl)
		block.EXPECT().IsSealed().Return(input.expectedSeal)
		require.Equal(t, input.expectedResult, series.shouldSeal(now, input.blockStart, block))
	}
}

func TestSeriesFetchBlocks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSeriesTestOptions()
	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	now := time.Now()
	starts := []time.Time{now, now.Add(time.Second), now.Add(-time.Second)}
	blocks := block.NewMockDatabaseSeriesBlocks(ctrl)

	// Set up the blocks
	b := block.NewMockDatabaseBlock(ctrl)
	b.EXPECT().Stream(ctx).Return(xio.NewSegmentReader(ts.Segment{}), nil)
	blocks.EXPECT().BlockAt(starts[0]).Return(b, true)
	b = block.NewMockDatabaseBlock(ctrl)
	b.EXPECT().Stream(ctx).Return(nil, errors.New("bar"))
	blocks.EXPECT().BlockAt(starts[1]).Return(b, true)
	blocks.EXPECT().BlockAt(starts[2]).Return(nil, false)

	// Set up the buffer
	buffer := NewMockdatabaseBuffer(ctrl)
	buffer.EXPECT().Empty().Return(false)
	buffer.EXPECT().FetchBlocks(ctx, starts).Return([]FetchBlockResult{newFetchBlockResult(starts[2], nil, nil)})

	series := newDatabaseSeries("foo", bootstrapped, opts).(*dbSeries)
	series.blocks = blocks
	series.buffer = buffer
	res := series.FetchBlocks(ctx, starts)

	expectedTimes := []time.Time{starts[2], starts[0], starts[1]}
	require.Equal(t, len(expectedTimes), len(res))
	for i := 0; i < len(starts); i++ {
		require.Equal(t, expectedTimes[i], res[i].Start())
		if i == 1 {
			require.NotNil(t, res[i].Readers())
		} else {
			require.Nil(t, res[i].Readers())
		}
		if i == 2 {
			require.Error(t, res[i].Err())
		} else {
			require.NoError(t, res[i].Err())
		}
	}
}

func TestSeriesFetchBlocksMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSeriesTestOptions()
	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	now := time.Now()
	starts := []time.Time{now, now.Add(time.Second), now.Add(-time.Second)}

	blocks := map[time.Time]block.DatabaseBlock{}
	b := block.NewMockDatabaseBlock(ctrl)
	expectedSegment := ts.Segment{Head: []byte{0x1, 0x2}, Tail: []byte{0x3, 0x4}}
	b.EXPECT().Stream(ctx).Return(xio.NewSegmentReader(expectedSegment), nil)
	blocks[starts[0]] = b

	b = block.NewMockDatabaseBlock(ctrl)
	b.EXPECT().Stream(ctx).Return(nil, errors.New("foo"))
	blocks[starts[1]] = b

	// Set up the buffer
	buffer := NewMockdatabaseBuffer(ctrl)
	buffer.EXPECT().Empty().Return(false)
	buffer.EXPECT().FetchBlocksMetadata(ctx, true).Return([]FetchBlockMetadataResult{newFetchBlockMetadataResult(starts[2], new(int64), nil)})

	series := newDatabaseSeries("bar", bootstrapped, opts).(*dbSeries)
	mockBlocks := block.NewMockDatabaseSeriesBlocks(ctrl)
	mockBlocks.EXPECT().AllBlocks().Return(blocks)
	series.blocks = mockBlocks
	series.buffer = buffer

	res := series.FetchBlocksMetadata(ctx, true)
	require.Equal(t, "bar", res.ID())
	metadata := res.Blocks()
	expected := []struct {
		t        time.Time
		s        *int64
		hasError bool
	}{
		{starts[2], nil, false},
		{starts[0], nil, false},
		{starts[1], nil, true},
	}
	expected[0].s = new(int64)
	tmp := int64(4)
	expected[1].s = &tmp
	require.Equal(t, len(expected), len(metadata))
	for i := 0; i < len(expected); i++ {
		require.Equal(t, expected[i].t, metadata[i].Start())
		if expected[i].s == nil {
			require.Nil(t, metadata[i].Size())
		} else {
			require.Equal(t, *expected[i].s, *metadata[i].Size())
		}
		if expected[i].hasError {
			require.Error(t, metadata[i].Err())
		} else {
			require.NoError(t, metadata[i].Err())
		}
	}
}
