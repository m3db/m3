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

package series

import (
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/digest"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/encoding/m3tsz"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/ts"
	xio "github.com/m3db/m3db/x/io"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/errors"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newSeriesTestOptions() Options {
	encoderPool := encoding.NewEncoderPool(nil)
	multiReaderIteratorPool := encoding.NewMultiReaderIteratorPool(nil)

	encodingOpts := encoding.NewOptions().SetEncoderPool(encoderPool)

	encoderPool.Init(func() encoding.Encoder {
		return m3tsz.NewEncoder(timeZero, nil, m3tsz.DefaultIntOptimizationEnabled, encodingOpts)
	})
	multiReaderIteratorPool.Init(func(r io.Reader) encoding.ReaderIterator {
		return m3tsz.NewReaderIterator(r, m3tsz.DefaultIntOptimizationEnabled, encodingOpts)
	})

	opts := NewOptions().
		SetEncoderPool(encoderPool).
		SetMultiReaderIteratorPool(multiReaderIteratorPool)
	opts = opts.
		SetRetentionOptions(opts.
			RetentionOptions().
			SetBlockSize(2 * time.Minute).
			SetBufferFuture(10 * time.Second).
			SetBufferPast(10 * time.Second).
			SetBufferDrain(30 * time.Second).
			SetRetentionPeriod(time.Hour)).
		SetDatabaseBlockOptions(opts.
			DatabaseBlockOptions().
			SetContextPool(opts.ContextPool()).
			SetEncoderPool(opts.EncoderPool()))
	return opts
}

func TestSeriesEmpty(t *testing.T) {
	opts := newSeriesTestOptions()
	series := NewDatabaseSeries(ts.StringID("foo"), opts).(*dbSeries)
	assert.NoError(t, series.Bootstrap(nil))
	assert.True(t, series.IsEmpty())
}

func TestSeriesWriteFlush(t *testing.T) {
	opts := newSeriesTestOptions()
	curr := time.Now().Truncate(opts.RetentionOptions().BlockSize())
	start := curr
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	series := NewDatabaseSeries(ts.StringID("foo"), opts).(*dbSeries)
	assert.NoError(t, series.Bootstrap(nil))

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
	_, err := series.Tick()
	assert.NoError(t, err)

	assert.Equal(t, false, series.buffer.NeedsDrain())

	blocks := series.blocks.AllBlocks()
	assert.Len(t, blocks, 1)

	block, ok := series.blocks.BlockAt(start)
	assert.Equal(t, true, ok)

	ctx := context.NewContext()
	defer ctx.Close()

	stream, err := block.Stream(ctx)
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
	series := NewDatabaseSeries(ts.StringID("foo"), opts).(*dbSeries)
	assert.NoError(t, series.Bootstrap(nil))

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
	series := NewDatabaseSeries(ts.StringID("foo"), opts).(*dbSeries)
	assert.NoError(t, series.Bootstrap(nil))

	ctx := context.NewContext()
	defer ctx.Close()

	results, err := series.ReadEncoded(ctx, time.Now(), time.Now().Add(-1*time.Second))
	assert.Error(t, err)
	assert.True(t, xerrors.IsInvalidParams(err))
	assert.Nil(t, results)
}

func TestSeriesFlushNoBlock(t *testing.T) {
	opts := newSeriesTestOptions()
	series := NewDatabaseSeries(ts.StringID("foo"), opts).(*dbSeries)
	assert.NoError(t, series.Bootstrap(nil))
	flushTime := time.Unix(7200, 0)
	err := series.Flush(nil, flushTime, nil)
	require.Nil(t, err)
}

func TestSeriesFlush(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSeriesTestOptions()
	series := NewDatabaseSeries(ts.StringID("foo"), opts).(*dbSeries)
	assert.NoError(t, series.Bootstrap(nil))
	flushTime := time.Unix(7200, 0)
	head := checked.NewBytes([]byte{0x1, 0x2}, nil)
	tail := checked.NewBytes([]byte{0x3, 0x4}, nil)

	block := opts.DatabaseBlockOptions().DatabaseBlockPool().Get()
	block.Reset(flushTime, ts.NewSegment(head, tail, ts.FinalizeNone))
	series.blocks.AddBlock(block)

	inputs := []error{errors.New("some error"), nil}
	for _, input := range inputs {
		persistFn := func(id ts.ID, segment ts.Segment, checksum uint32) error { return input }
		ctx := context.NewContext()
		err := series.Flush(ctx, flushTime, persistFn)
		ctx.BlockingClose()
		require.Equal(t, input, err)
	}
}

func TestSeriesTickEmptySeries(t *testing.T) {
	opts := newSeriesTestOptions()
	series := NewDatabaseSeries(ts.StringID("foo"), opts).(*dbSeries)
	assert.NoError(t, series.Bootstrap(nil))
	_, err := series.Tick()
	require.Equal(t, ErrSeriesAllDatapointsExpired, err)
}

func TestSeriesTickNeedsDrain(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSeriesTestOptions()
	series := NewDatabaseSeries(ts.StringID("foo"), opts).(*dbSeries)
	assert.NoError(t, series.Bootstrap(nil))
	buffer := NewMockdatabaseBuffer(ctrl)
	series.buffer = buffer
	buffer.EXPECT().IsEmpty().Return(false)
	buffer.EXPECT().NeedsDrain().Return(true)
	buffer.EXPECT().DrainAndReset()
	_, err := series.Tick()
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
	series := NewDatabaseSeries(ts.StringID("foo"), opts).(*dbSeries)
	assert.NoError(t, series.Bootstrap(nil))
	blockStart := curr.Add(-ropts.RetentionPeriod()).Add(-ropts.BlockSize())
	b := block.NewMockDatabaseBlock(ctrl)
	b.EXPECT().StartTime().Return(blockStart)
	b.EXPECT().Close()
	series.blocks.AddBlock(b)
	b = block.NewMockDatabaseBlock(ctrl)
	b.EXPECT().StartTime().Return(curr)
	series.blocks.AddBlock(b)
	require.Equal(t, blockStart, series.blocks.MinTime())
	require.Equal(t, 2, series.blocks.Len())
	buffer := NewMockdatabaseBuffer(ctrl)
	series.buffer = buffer
	buffer.EXPECT().IsEmpty().Return(true)
	buffer.EXPECT().NeedsDrain().Return(false)
	r, err := series.Tick()
	require.NoError(t, err)
	require.Equal(t, 1, r.ActiveBlocks)
	require.Equal(t, 1, r.ExpiredBlocks)
	require.Equal(t, 1, series.blocks.Len())
	require.Equal(t, curr, series.blocks.MinTime())
	_, exists := series.blocks.AllBlocks()[curr]
	require.True(t, exists)
}

func TestSeriesBootstrapWithError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSeriesTestOptions()
	now := time.Now()
	blockSize := 2 * time.Hour

	series := NewDatabaseSeries(ts.StringID("foo"), opts).(*dbSeries)

	bufferMin := now.Truncate(blockSize).Add(-blockSize)
	bufferMax := now.Truncate(blockSize).Add(2 * blockSize)

	buffer := NewMockdatabaseBuffer(ctrl)
	buffer.EXPECT().DrainAndReset()
	buffer.EXPECT().MinMax().Return(bufferMin, bufferMax)
	series.buffer = buffer

	errBlockStart := bufferMin
	blopts := opts.DatabaseBlockOptions()

	blocks := block.NewDatabaseSeriesBlocks(0, blopts)

	// Add block that buffer will fail to bootstrap
	bl := block.NewMockDatabaseBlock(ctrl)
	bl.EXPECT().StartTime().Return(errBlockStart).AnyTimes()
	blocks.AddBlock(bl)

	// Add block that will succeed
	bl = block.NewMockDatabaseBlock(ctrl)
	bl.EXPECT().StartTime().Return(bufferMin.Add(-blockSize)).AnyTimes()
	blocks.AddBlock(bl)

	// Expect to fail the bootstrap for block destined for buffer
	buffer.EXPECT().Bootstrap(bl).Return(fmt.Errorf("bar"))

	err := series.Bootstrap(blocks)
	require.NotNil(t, err)

	str := fmt.Sprintf("bootstrap series error occurred for %s block at %s: %s",
		series.ID().String(), errBlockStart.String(), "bar")
	require.Equal(t, str, err.Error())
	require.Equal(t, bootstrapped, series.bs)
	require.Equal(t, 1, series.blocks.Len())
}

func TestShouldExpire(t *testing.T) {
	opts := newSeriesTestOptions()
	ropts := opts.RetentionOptions()
	series := NewDatabaseSeries(ts.StringID("foo"), opts).(*dbSeries)
	assert.NoError(t, series.Bootstrap(nil))
	now := time.Now()
	require.False(t,
		series.shouldExpireBlockAt(
			now,
			now))
	require.True(t,
		series.shouldExpireBlockAt(
			now,
			now.Add(-ropts.RetentionPeriod()).Add(-ropts.BlockSize())))

	expiryPeriod := 10 * time.Minute
	ropts = ropts.SetShortExpiry(true).SetShortExpiryPeriod(expiryPeriod)
	opts = opts.SetRetentionOptions(ropts)
	series = NewDatabaseSeries(ts.StringID("foo"), opts).(*dbSeries)
	assert.NoError(t, series.Bootstrap(nil))
	require.False(t,
		series.shouldExpireBlockAt(
			now,
			now))
	require.True(t,
		series.shouldExpireBlockAt(
			now,
			now.Add(-2*expiryPeriod)))
	require.True(t,
		series.shouldExpireBlockAt(
			now,
			now.Add(-ropts.RetentionPeriod()).Add(-ropts.BlockSize())))
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
	ck0 := uint32(42)

	// Set up the blocks
	b := block.NewMockDatabaseBlock(ctrl)
	b.EXPECT().Stream(ctx).Return(xio.NewSegmentReader(ts.Segment{}), nil)
	b.EXPECT().Checksum().Return(ck0)
	blocks.EXPECT().BlockAt(starts[0]).Return(b, true)
	b = block.NewMockDatabaseBlock(ctrl)
	b.EXPECT().Stream(ctx).Return(nil, errors.New("bar"))
	blocks.EXPECT().BlockAt(starts[1]).Return(b, true)
	blocks.EXPECT().BlockAt(starts[2]).Return(nil, false)

	// Set up the buffer
	buffer := NewMockdatabaseBuffer(ctrl)
	buffer.EXPECT().IsEmpty().Return(false)
	buffer.EXPECT().FetchBlocks(ctx, starts).Return([]block.FetchBlockResult{block.NewFetchBlockResult(starts[2], nil, nil, nil)})

	series := NewDatabaseSeries(ts.StringID("foo"), opts).(*dbSeries)
	assert.NoError(t, series.Bootstrap(nil))
	series.blocks = blocks
	series.buffer = buffer
	res := series.FetchBlocks(ctx, starts)

	expectedTimes := []time.Time{starts[2], starts[0], starts[1]}
	require.Equal(t, len(expectedTimes), len(res))
	for i := 0; i < len(starts); i++ {
		require.Equal(t, expectedTimes[i], res[i].Start())
		if i == 1 {
			require.NotNil(t, res[i].Readers())
			require.Equal(t, &ck0, res[i].Checksum())
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
	start := now.Add(-time.Hour)
	end := now.Add(time.Hour)
	starts := []time.Time{now.Add(-time.Hour), now, now.Add(time.Second), now.Add(time.Hour)}

	blocks := map[time.Time]block.DatabaseBlock{}
	b := block.NewMockDatabaseBlock(ctrl)
	head := checked.NewBytes([]byte{0x1, 0x2}, nil)
	tail := checked.NewBytes([]byte{0x3, 0x4}, nil)
	expectedSegment := ts.NewSegment(head, tail, ts.FinalizeNone)
	b.EXPECT().Len().Return(expectedSegment.Len())
	expectedChecksum := digest.SegmentChecksum(expectedSegment)
	b.EXPECT().Checksum().Return(expectedChecksum)
	blocks[starts[0]] = b
	blocks[starts[3]] = nil

	// Set up the buffer
	buffer := NewMockdatabaseBuffer(ctrl)
	expectedResults := block.NewFetchBlockMetadataResults()
	expectedResults.Add(block.NewFetchBlockMetadataResult(starts[2], new(int64), nil, nil))
	buffer.EXPECT().IsEmpty().Return(false)
	buffer.EXPECT().
		FetchBlocksMetadata(ctx, start, end, true, true).
		Return(expectedResults)

	series := NewDatabaseSeries(ts.StringID("bar"), opts).(*dbSeries)
	assert.NoError(t, series.Bootstrap(nil))
	mockBlocks := block.NewMockDatabaseSeriesBlocks(ctrl)
	mockBlocks.EXPECT().AllBlocks().Return(blocks)
	series.blocks = mockBlocks
	series.buffer = buffer

	res := series.FetchBlocksMetadata(ctx, start, end, true, true)
	require.Equal(t, "bar", res.ID.String())

	metadata := res.Blocks.Results()
	expectedSize := int64(4)
	expected := []struct {
		t        time.Time
		s        *int64
		c        *uint32
		hasError bool
	}{
		{starts[0], &expectedSize, &expectedChecksum, false},
		{starts[2], new(int64), nil, false},
	}
	require.Equal(t, len(expected), len(metadata))
	for i := 0; i < len(expected); i++ {
		require.Equal(t, expected[i].t, metadata[i].Start)
		if expected[i].s == nil {
			require.Nil(t, metadata[i].Size)
		} else {
			require.Equal(t, *expected[i].s, *metadata[i].Size)
		}
		if expected[i].c == nil {
			require.Nil(t, metadata[i].Checksum)
		} else {
			require.Equal(t, *expected[i].c, *metadata[i].Checksum)
		}
		if expected[i].hasError {
			require.Error(t, metadata[i].Err)
		} else {
			require.NoError(t, metadata[i].Err)
		}
	}
}

func TestSeriesOutOfOrderWritesAndRotate(t *testing.T) {
	now := time.Unix(1477929600, 0)
	nowFn := func() time.Time { return now }
	clockOpts := clock.NewOptions().SetNowFn(nowFn)
	retentionOpts := retention.NewOptions()
	opts := newSeriesTestOptions().
		SetClockOptions(clockOpts).
		SetRetentionOptions(retentionOpts)

	var (
		ctx        = context.NewContext()
		id         = ts.StringID("foo")
		startValue = 1.0
		blockSize  = opts.RetentionOptions().BlockSize()
		numPoints  = 10
		numBlocks  = 7
		qStart     = now
		qEnd       = qStart.Add(time.Duration(numBlocks) * blockSize)
		expected   []ts.Datapoint
	)

	series := NewDatabaseSeries(id, opts).(*dbSeries)
	series.Reset(id, true, nil)

	for iter := 0; iter < numBlocks; iter++ {
		start := now
		value := startValue

		for i := 0; i < numPoints; i++ {
			require.NoError(t, series.Write(ctx, start, value, xtime.Second, nil))
			expected = append(expected, ts.Datapoint{Timestamp: start, Value: value})
			start = start.Add(10 * time.Second)
			value = value + 1.0
		}

		// Perform out-of-order writes
		start = now
		value = startValue
		for i := 0; i < numPoints/2; i++ {
			require.NoError(t, series.Write(ctx, start, value, xtime.Second, nil))
			start = start.Add(10 * time.Second)
			value = value + 1.0
		}

		now = now.Add(blockSize)
	}

	encoded, err := series.ReadEncoded(ctx, qStart, qEnd)
	require.NoError(t, err)

	multiIt := opts.MultiReaderIteratorPool().Get()
	multiIt.ResetSliceOfSlices(xio.NewReaderSliceOfSlicesFromSegmentReadersIterator(encoded))
	it := encoding.NewSeriesIterator(id.String(), qStart, qEnd, []encoding.Iterator{multiIt}, nil)
	defer it.Close()

	var actual []ts.Datapoint
	for it.Next() {
		dp, _, _ := it.Current()
		actual = append(actual, dp)
	}

	require.NoError(t, it.Err())
	require.Equal(t, expected, actual)
}
