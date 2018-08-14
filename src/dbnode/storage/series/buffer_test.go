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
	"io"
	"io/ioutil"
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3x/context"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newBufferTestOptions() Options {
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
		SetRetentionOptions(opts.RetentionOptions().
			SetBlockSize(2 * time.Minute).
			SetBufferFuture(10 * time.Second).
			SetBufferPast(10 * time.Second).
			SetMaxWritesBeforeFlush(5).
			SetFlushAfterNoMetricPeriod(30 * time.Second)).
		SetDatabaseBlockOptions(opts.DatabaseBlockOptions().
			SetContextPool(opts.ContextPool()).
			SetEncoderPool(opts.EncoderPool()))
	return opts
}

func TestBufferWriteTooFuture(t *testing.T) {
	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer := newDatabaseBuffer(nil).(*dbBuffer)
	buffer.Reset(opts)

	ctx := context.NewContext()
	defer ctx.Close()

	err := buffer.Write(ctx, curr.Add(rops.BufferFuture()), 1, xtime.Second, nil)
	assert.Error(t, err)
}

func TestBufferWriteTooPast(t *testing.T) {
	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer := newDatabaseBuffer(nil).(*dbBuffer)
	buffer.Reset(opts)

	ctx := context.NewContext()
	defer ctx.Close()

	err := buffer.Write(ctx, curr.Add(-1*rops.BufferPast()), 1, xtime.Second, nil)
	assert.Error(t, err)
}

func TestBufferWriteNotRealTimeDrain(t *testing.T) {
	var drained []block.DatabaseBlock
	drainFn := func(b block.DatabaseBlock) {
		drained = append(drained, b)
	}

	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	})).SetRetentionOptions(rops.SetAnyWriteTimeEnabled(true))
	buffer := newDatabaseBuffer(drainFn).(*dbBuffer)
	buffer.Reset(opts)

	firstWriteTime := curr.Add(rops.BlockSize())
	data := []value{
		{firstWriteTime.Add(mins(0.0)), 0, xtime.Second, nil},
		{firstWriteTime.Add(mins(0.1)), 1, xtime.Second, nil},
		{firstWriteTime.Add(mins(0.2)), 2, xtime.Second, nil},
		{firstWriteTime.Add(mins(0.3)), 3, xtime.Second, nil},
		{firstWriteTime.Add(mins(0.4)), 4, xtime.Second, nil},
		// Sixth datapoint in the same block should cause
		// a drain due to MaxWritesBeforeFlush() == 5
		{firstWriteTime.Add(mins(0.5)), 5, xtime.Second, nil},
	}

	for _, v := range data {
		ctx := context.NewContext()
		assert.NoError(t, buffer.Write(ctx, v.timestamp, v.value, v.unit, v.annotation))
		ctx.Close()
	}

	ctx := context.NewContext()
	defer ctx.Close()

	assertValuesEqual(t, data[:5], [][]xio.BlockReader{[]xio.BlockReader{
		xio.BlockReader{
			SegmentReader: requireDrainedStream(ctx, t, drained[0]),
		},
	}}, opts)

	results := buffer.ReadEncoded(ctx, firstWriteTime, timeDistantFuture)
	require.NotNil(t, results)
	assertValuesEqual(t, data[5:], results, opts)
}

func TestBufferWriteNotRealTimeStale(t *testing.T) {
	var drained []block.DatabaseBlock
	drainFn := func(b block.DatabaseBlock) {
		drained = append(drained, b)
	}

	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	})).SetRetentionOptions(rops.SetAnyWriteTimeEnabled(true))
	buffer := newDatabaseBuffer(drainFn).(*dbBuffer)
	buffer.Reset(opts)

	firstWriteTime := curr.Add(rops.BlockSize())
	data := []value{
		{firstWriteTime.Add(mins(0)), 0, xtime.Second, nil},
		{firstWriteTime.Add(mins(0.1)), 1, xtime.Second, nil},
		{firstWriteTime.Add(mins(0.2)), 2, xtime.Second, nil},
	}

	for _, v := range data {
		ctx := context.NewContext()
		assert.NoError(t, buffer.Write(ctx, v.timestamp, v.value, v.unit, v.annotation))
		ctx.Close()
	}

	buffer.nowFn = func() time.Time {
		return curr.Add(2 * rops.FlushAfterNoMetricPeriod())
	}

	// This tick should cause a drain and remove the bucket from the
	// non-realtime bucket map because the bucket is now stale
	buffer.Tick()

	ctx := context.NewContext()
	defer ctx.Close()

	assertValuesEqual(t, data[:3], [][]xio.BlockReader{[]xio.BlockReader{
		xio.BlockReader{
			SegmentReader: requireDrainedStream(ctx, t, drained[0]),
		},
	}}, opts)
}

func TestBufferWriteNotRealTimeMakeRealTime(t *testing.T) {
	var drained []block.DatabaseBlock
	drainFn := func(b block.DatabaseBlock) {
		drained = append(drained, b)
	}

	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	})).SetRetentionOptions(rops.SetAnyWriteTimeEnabled(true))
	buffer := newDatabaseBuffer(drainFn).(*dbBuffer)
	buffer.Reset(opts)

	// Non-realtime writes
	firstWriteTime := curr.Add(rops.BlockSize())
	data := []value{
		{firstWriteTime.Add(secs(0)), 0, xtime.Second, nil},
		{firstWriteTime.Add(secs(1)), 1, xtime.Second, nil},
		{firstWriteTime.Add(secs(2)), 2, xtime.Second, nil},
	}

	for _, v := range data {
		ctx := context.NewContext()
		assert.NoError(t, buffer.Write(ctx, v.timestamp, v.value, v.unit, v.annotation))
		ctx.Close()
	}

	// Make the previous writes now in the "realtime" window
	buffer.nowFn = func() time.Time {
		return firstWriteTime
	}

	v := value{firstWriteTime.Add(secs(0)), 0, xtime.Second, nil}
	ctx := context.NewContext()
	// This write should move the previous non-realtime bucket into the realtime bucket
	assert.NoError(t, buffer.Write(ctx, v.timestamp, v.value, v.unit, v.annotation))
	ctx.Close()

	assert.Len(t, buffer.bucketsNotRealTime, 0)

	results := buffer.ReadEncoded(ctx, firstWriteTime, timeDistantFuture)
	require.NotNil(t, results)
	assertValuesEqual(t, data, results, opts)
}

func TestBufferWriteRead(t *testing.T) {
	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer := newDatabaseBuffer(nil).(*dbBuffer)
	buffer.Reset(opts)

	data := []value{
		{curr.Add(secs(1)), 1, xtime.Second, nil},
		{curr.Add(secs(2)), 2, xtime.Second, nil},
		{curr.Add(secs(3)), 3, xtime.Second, nil},
	}

	for _, v := range data {
		ctx := context.NewContext()
		assert.NoError(t, buffer.Write(ctx, v.timestamp, v.value, v.unit, v.annotation))
		ctx.Close()
	}

	ctx := context.NewContext()
	defer ctx.Close()

	results := buffer.ReadEncoded(ctx, timeZero, timeDistantFuture)
	assert.NotNil(t, results)

	assertValuesEqual(t, data, results, opts)
}

func TestBufferReadOnlyMatchingBuckets(t *testing.T) {
	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())
	start := curr
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer := newDatabaseBuffer(nil).(*dbBuffer)
	buffer.Reset(opts)

	data := []value{
		{curr.Add(mins(1)), 1, xtime.Second, nil},
		{curr.Add(mins(3)), 2, xtime.Second, nil},
	}

	for _, v := range data {
		curr = v.timestamp
		ctx := context.NewContext()
		assert.NoError(t, buffer.Write(ctx, v.timestamp, v.value, v.unit, v.annotation))
		ctx.Close()
	}

	ctx := context.NewContext()
	defer ctx.Close()

	firstBucketStart := start.Truncate(time.Second)
	firstBucketEnd := start.Add(mins(2)).Truncate(time.Second)
	results := buffer.ReadEncoded(ctx, firstBucketStart, firstBucketEnd)
	assert.NotNil(t, results)
	assertValuesEqual(t, []value{data[0]}, results, opts)

	secondBucketStart := start.Add(mins(2)).Truncate(time.Second)
	secondBucketEnd := start.Add(mins(4)).Truncate(time.Second)
	results = buffer.ReadEncoded(ctx, secondBucketStart, secondBucketEnd)
	assert.NotNil(t, results)

	assertValuesEqual(t, []value{data[1]}, results, opts)
}

func TestBufferDrain(t *testing.T) {
	var drained []block.DatabaseBlock
	drainFn := func(b block.DatabaseBlock) {
		drained = append(drained, b)
	}

	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer := newDatabaseBuffer(drainFn).(*dbBuffer)
	buffer.Reset(opts)

	data := []value{
		{curr, 1, xtime.Second, nil},
		{curr.Add(mins(0.5)), 2, xtime.Second, nil},
		{curr.Add(mins(1.0)), 3, xtime.Second, nil},
		{curr.Add(mins(1.5)), 4, xtime.Second, nil},
		{curr.Add(mins(2.0)), 5, xtime.Second, nil},
		{curr.Add(mins(2.5)), 6, xtime.Second, nil},
	}

	for _, v := range data {
		curr = v.timestamp
		ctx := context.NewContext()
		assert.NoError(t, buffer.Write(ctx, v.timestamp, v.value, v.unit, v.annotation))
		ctx.Close()
	}

	assert.Equal(t, true, buffer.NeedsDrain())
	assert.Equal(t, 0, len(drained))

	buffer.DrainAndReset()

	assert.Equal(t, false, buffer.NeedsDrain())
	require.Equal(t, 1, len(drained))

	ctx := context.NewContext()
	defer ctx.Close()

	results := buffer.ReadEncoded(ctx, timeZero, timeDistantFuture)
	require.NotNil(t, results)

	assertValuesEqual(t, data[:4], [][]xio.BlockReader{[]xio.BlockReader{
		xio.BlockReader{
			SegmentReader: requireDrainedStream(ctx, t, drained[0]),
		},
	}}, opts)
	assertValuesEqual(t, data[4:], results, opts)
}

func TestBufferMinMax(t *testing.T) {
	// Setup
	drainFn := func(b block.DatabaseBlock) {}
	var (
		opts      = newBufferTestOptions()
		rops      = opts.RetentionOptions()
		blockSize = rops.BlockSize()
		start     = time.Now().Truncate(rops.BlockSize())
		curr      = start
		buffer    = newDatabaseBuffer(drainFn).(*dbBuffer)
	)

	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer.Reset(opts)

	// Verify standard behavior of MinMax()
	min, max, err := buffer.MinMax()
	require.NoError(t, err)
	expectedMin := start.Add(-blockSize)
	expectedMax := start.Add(blockSize)
	require.Equal(t, expectedMin, min)
	require.Equal(t, expectedMax, max)
	// Delta between max and min should be 2 * blockSize because there are three
	// available buckets
	require.Equal(t, expectedMax.Sub(expectedMin), 2*blockSize)

	// Data preparation to trigger a drain of the earliest bucket
	data := []value{
		{start, 1, xtime.Second, nil},
		{start.Add(mins(0.5)), 2, xtime.Second, nil},
		{start.Add(mins(1.0)), 3, xtime.Second, nil},
		{start.Add(mins(1.5)), 4, xtime.Second, nil},
		{start.Add(mins(2.0)), 5, xtime.Second, nil},
		{start.Add(mins(2.5)), 6, xtime.Second, nil},
	}
	for _, v := range data {
		curr = v.timestamp
		ctx := context.NewContext()
		assert.NoError(t, buffer.Write(ctx, v.timestamp, v.value, v.unit, v.annotation))
		ctx.Close()
	}

	// Drain the earliest bucket
	assert.Equal(t, true, buffer.NeedsDrain())
	buffer.DrainAndReset()
	assert.Equal(t, false, buffer.NeedsDrain())

	// Verify we exclude drained buckets from the MinMax calculation
	min, max, err = buffer.MinMax()
	require.NoError(t, err)
	expectedMin = start.Add(blockSize)
	expectedMax = start.Add(2 * blockSize)
	require.Equal(t, expectedMin, min)
	require.Equal(t, expectedMax, max)
	// Delta between max and min should be 1 * blockSize because there are two
	// available buckets (the earliest one is drained and marked as unavailable).
	require.Equal(t, expectedMax.Sub(expectedMin), blockSize)
}

func TestBufferBootstrapAlreadyDrained(t *testing.T) {
	// Setup
	drainFn := func(b block.DatabaseBlock) {}
	var (
		opts   = newBufferTestOptions()
		rops   = opts.RetentionOptions()
		start  = time.Now().Truncate(rops.BlockSize())
		curr   = start
		buffer = newDatabaseBuffer(drainFn).(*dbBuffer)
	)

	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer.Reset(opts)

	bucketStart := buffer.bucketsRealTime[0].start
	dbBlock := block.NewDatabaseBlock(bucketStart, 0, ts.Segment{}, block.NewOptions())

	// Make sure multiple adds dont cause an error
	require.NoError(t, buffer.Bootstrap(dbBlock))
	require.NoError(t, buffer.Bootstrap(dbBlock))

	buffer.bucketsRealTime[0].drained = true

	// Should return an error because we dont want to add bootstrapped blocks to
	// a bucket that is already drained because they'll never get flushed.
	require.Error(t, buffer.Bootstrap(dbBlock))
}

func TestBufferResetUndrainedBucketDrainsBucket(t *testing.T) {
	var drained []block.DatabaseBlock
	drainFn := func(b block.DatabaseBlock) {
		drained = append(drained, b)
	}

	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer := newDatabaseBuffer(drainFn).(*dbBuffer)
	buffer.Reset(opts)

	data := []value{
		{curr.Add(mins(1)), 1, xtime.Second, nil},
		{curr.Add(mins(3)), 2, xtime.Second, nil},
		{curr.Add(mins(5)), 2, xtime.Second, nil},
		{curr.Add(mins(7)), 2, xtime.Second, nil},
	}

	for _, v := range data {
		curr = v.timestamp
		ctx := context.NewContext()
		assert.NoError(t, buffer.Write(ctx, v.timestamp, v.value, v.unit, v.annotation))
		ctx.Close()
	}

	assert.Equal(t, true, buffer.NeedsDrain())
	assert.Len(t, drained, 2)

	ctx := context.NewContext()
	defer ctx.Close()

	results := buffer.ReadEncoded(ctx, timeZero, timeDistantFuture)
	assert.NotNil(t, results)

	assertValuesEqual(t, data[:2], [][]xio.BlockReader{[]xio.BlockReader{
		xio.BlockReader{
			SegmentReader: requireDrainedStream(ctx, t, drained[1]),
		},
		xio.BlockReader{
			SegmentReader: requireDrainedStream(ctx, t, drained[0]),
		},
	}}, opts)
	assertValuesEqual(t, data[2:], results, opts)
}

func TestBufferWriteOutOfOrder(t *testing.T) {
	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer := newDatabaseBuffer(nil).(*dbBuffer)
	buffer.Reset(opts)

	data := []value{
		{curr, 1, xtime.Second, nil},
		{curr.Add(secs(10)), 2, xtime.Second, nil},
		{curr.Add(secs(5)), 3, xtime.Second, nil},
	}

	for _, v := range data {
		if v.timestamp.After(curr) {
			curr = v.timestamp
		}
		ctx := context.NewContext()
		assert.NoError(t, buffer.Write(ctx, v.timestamp, v.value, v.unit, v.annotation))
		ctx.Close()
	}

	bucketIdx := (curr.UnixNano() / int64(rops.BlockSize())) % bucketsLen
	assert.Equal(t, 2, len(buffer.bucketsRealTime[bucketIdx].encoders))
	assert.False(t, buffer.bucketsRealTime[bucketIdx].empty)
	assert.Equal(t, data[1].timestamp, buffer.bucketsRealTime[bucketIdx].encoders[0].lastWriteAt)
	assert.Equal(t, data[2].timestamp, buffer.bucketsRealTime[bucketIdx].encoders[1].lastWriteAt)

	// Restore data to in order for comparison
	sort.Sort(valuesByTime(data))

	ctx := context.NewContext()
	defer ctx.Close()

	results := buffer.ReadEncoded(ctx, timeZero, timeDistantFuture)
	assert.NotNil(t, results)

	assertValuesEqual(t, data, results, opts)

	// Explicitly merge
	var mergedResults [][]xio.BlockReader
	for i := range buffer.bucketsRealTime {
		mergedResult, err := buffer.bucketsRealTime[i].discardMerged()
		require.NoError(t, err)

		block := mergedResult.block
		require.NotNil(t, block)

		if block.Len() > 0 {
			blockReader := xio.BlockReader{
				SegmentReader: requireDrainedStream(ctx, t, block),
			}
			result := []xio.BlockReader{blockReader}
			mergedResults = append(mergedResults, result)
		}
	}

	// Assert equal
	assertValuesEqual(t, data, mergedResults, opts)
}

func newTestBufferBucketWithData(t *testing.T) (*dbBufferBucket, Options, []value) {
	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())
	b := &dbBufferBucket{opts: opts}
	b.resetTo(curr, true)
	data := [][]value{
		{
			{curr, 1, xtime.Second, nil},
			{curr.Add(secs(10)), 2, xtime.Second, nil},
			{curr.Add(secs(50)), 3, xtime.Second, nil},
		},
		{
			{curr.Add(secs(20)), 4, xtime.Second, nil},
			{curr.Add(secs(40)), 5, xtime.Second, nil},
			{curr.Add(secs(60)), 6, xtime.Second, nil},
		},
		{
			{curr.Add(secs(30)), 4, xtime.Second, nil},
			{curr.Add(secs(70)), 5, xtime.Second, nil},
		},
		{
			{curr.Add(secs(35)), 6, xtime.Second, nil},
		},
	}

	// Empty all existing encoders
	b.encoders = nil

	var expected []value
	for i, d := range data {
		encoded := 0
		encoder := opts.EncoderPool().Get()
		encoder.Reset(curr, 0)
		for _, v := range data[i] {
			dp := ts.Datapoint{
				Timestamp: v.timestamp,
				Value:     v.value,
			}
			err := encoder.Encode(dp, v.unit, v.annotation)
			require.NoError(t, err)
			encoded++
		}
		b.encoders = append(b.encoders, inOrderEncoder{encoder: encoder})
		expected = append(expected, d...)
	}
	b.empty = false
	sort.Sort(valuesByTime(expected))
	return b, opts, expected
}

func TestBufferBucketMerge(t *testing.T) {
	b, opts, expected := newTestBufferBucketWithData(t)

	result, err := b.discardMerged()
	require.NoError(t, err)

	bl := result.block
	require.NotNil(t, bl)

	assert.Equal(t, 0, len(b.encoders))
	assert.Equal(t, 0, len(b.bootstrapped))
	assert.True(t, b.empty)

	ctx := context.NewContext()
	defer ctx.Close()

	assertValuesEqual(t, expected, [][]xio.BlockReader{[]xio.BlockReader{
		xio.BlockReader{
			SegmentReader: requireDrainedStream(ctx, t, bl),
		},
	}}, opts)
}

func TestBufferBucketMergeNilEncoderStreams(t *testing.T) {
	opts := newBufferTestOptions()
	ropts := opts.RetentionOptions()
	curr := time.Now().Truncate(ropts.BlockSize())

	b := &dbBufferBucket{opts: opts}
	b.resetTo(curr, true)
	emptyEncoder := opts.EncoderPool().Get()
	emptyEncoder.Reset(curr, 0)
	b.encoders = append(b.encoders, inOrderEncoder{encoder: emptyEncoder})
	require.Nil(t, b.encoders[0].encoder.Stream())

	encoder := opts.EncoderPool().Get()
	encoder.Reset(curr, 0)

	value := ts.Datapoint{Timestamp: curr, Value: 1.0}
	err := encoder.Encode(value, xtime.Second, nil)
	require.NoError(t, err)

	blopts := opts.DatabaseBlockOptions()
	newBlock := block.NewDatabaseBlock(curr, 0, encoder.Discard(), blopts)
	b.bootstrapped = append(b.bootstrapped, newBlock)
	ctx := opts.ContextPool().Get()
	stream, err := b.bootstrapped[0].Stream(ctx)
	require.NoError(t, err)
	require.NotNil(t, stream)

	b.discardMerged()

	require.Equal(t, 0, len(b.encoders))
	require.Nil(t, b.bootstrapped)
}

func TestBufferBucketWriteDuplicate(t *testing.T) {
	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())

	b := &dbBufferBucket{opts: opts}
	b.resetTo(curr, true)
	require.NoError(t, b.write(curr, curr, 1, xtime.Second, nil))
	require.Equal(t, 1, len(b.encoders))
	require.False(t, b.empty)

	encoded, err := b.encoders[0].encoder.Stream().Segment()
	require.NoError(t, err)
	require.NoError(t, b.write(curr, curr, 1, xtime.Second, nil))
	require.Equal(t, 1, len(b.encoders))

	result, err := b.discardMerged()
	require.NoError(t, err)

	bl := result.block
	require.NotNil(t, bl)

	ctx := context.NewContext()
	stream, err := bl.Stream(ctx)
	require.NoError(t, err)

	segment, err := stream.Segment()
	require.NoError(t, err)
	require.True(t, encoded.Equal(&segment))
}

func TestBufferFetchBlocks(t *testing.T) {
	b, opts, expected := newTestBufferBucketWithData(t)
	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	buffer := newDatabaseBuffer(nil).(*dbBuffer)
	buffer.Reset(opts)
	buffer.bucketsRealTime[0] = *b

	for i := 1; i < 3; i++ {
		newBucketStart := b.start.Add(time.Duration(i) * time.Minute)
		(&buffer.bucketsRealTime[i]).resetTo(newBucketStart, true)
		buffer.bucketsRealTime[i].encoders = []inOrderEncoder{{newBucketStart, nil}}
	}

	res := buffer.FetchBlocks(ctx, []time.Time{b.start, b.start.Add(time.Second)})
	require.Equal(t, 1, len(res))
	require.Equal(t, b.start, res[0].Start)
	assertValuesEqual(t, expected, [][]xio.BlockReader{res[0].Blocks}, opts)
}

func TestBufferFetchBlocksMetadata(t *testing.T) {
	b, opts, _ := newTestBufferBucketWithData(t)

	expectedLastRead := time.Now()
	b.lastReadUnixNanos = expectedLastRead.UnixNano()

	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	start := b.start.Add(-time.Second)
	end := b.start.Add(time.Second)

	buffer := newDatabaseBuffer(nil).(*dbBuffer)
	buffer.Reset(opts)
	buffer.bucketsRealTime[0] = *b

	expectedSize := int64(b.streamsLen())

	fetchOpts := FetchBlocksMetadataOptions{
		FetchBlocksMetadataOptions: block.FetchBlocksMetadataOptions{
			IncludeSizes:     true,
			IncludeChecksums: true,
			IncludeLastRead:  true,
		},
	}
	res := buffer.FetchBlocksMetadata(ctx, start, end, fetchOpts).Results()
	assert.Equal(t, 1, len(res))
	assert.Equal(t, b.start, res[0].Start)
	assert.Equal(t, expectedSize, res[0].Size)
	assert.Equal(t, (*uint32)(nil), res[0].Checksum) // checksum is never available for buffer block
	assert.True(t, expectedLastRead.Equal(res[0].LastRead))
}

func TestBufferReadEncodedValidAfterDrain(t *testing.T) {
	var drained []block.DatabaseBlock
	drainFn := func(b block.DatabaseBlock) {
		drained = append(drained, b)
	}

	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())
	start := curr
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer := newDatabaseBuffer(drainFn).(*dbBuffer)
	buffer.Reset(opts)

	// Perform out of order writes that will create two in order encoders
	data := []value{
		{curr, 1, xtime.Second, nil},
		{curr.Add(mins(0.5)), 2, xtime.Second, nil},
		{curr.Add(mins(0.5)).Add(-5 * time.Second), 3, xtime.Second, nil},
		{curr.Add(mins(1.0)), 4, xtime.Second, nil},
		{curr.Add(mins(1.5)), 5, xtime.Second, nil},
		{curr.Add(mins(1.5)).Add(-5 * time.Second), 6, xtime.Second, nil},
	}

	for _, v := range data {
		curr = v.timestamp
		ctx := context.NewContext()
		assert.NoError(t, buffer.Write(ctx, v.timestamp, v.value, v.unit, v.annotation))
		ctx.Close()
	}

	curr = start.
		Add(rops.BlockSize()).
		Add(rops.BufferPast()).
		Add(time.Nanosecond)

	var encoders []encoding.Encoder
	for i := range buffer.bucketsRealTime {
		if !buffer.bucketsRealTime[i].start.Equal(start) {
			continue
		}
		// Current bucket encoders should all have data in them
		for j := range buffer.bucketsRealTime[i].encoders {
			encoder := buffer.bucketsRealTime[i].encoders[j].encoder
			assert.NotNil(t, encoder.Stream())

			encoders = append(encoders, encoder)
		}
	}

	require.Equal(t, 2, len(encoders))

	var (
		expectedData []byte
		streams      []io.Reader
	)
	for _, encoder := range encoders {
		stream := encoder.Stream()
		clone, err := stream.Clone()
		require.NoError(t, err)
		streams = append(streams, clone)
		data, err := ioutil.ReadAll(stream)
		require.NoError(t, err)
		expectedData = append(expectedData, data...)
	}

	assert.Equal(t, true, buffer.NeedsDrain())
	assert.Equal(t, 0, len(drained))

	ctx := context.NewContext()
	defer ctx.Close()

	// Read and attach context lifetime to the data
	buffer.ReadEncoded(ctx, timeZero, timeDistantFuture)

	buffer.DrainAndReset()

	assert.Equal(t, false, buffer.NeedsDrain())
	assert.Equal(t, 1, len(drained))

	// Ensure all streams that were taken reference to still has data
	var actualData []byte
	for _, stream := range streams {
		data, err := ioutil.ReadAll(stream)
		require.NoError(t, err)
		actualData = append(actualData, data...)
	}
	assert.Equal(t, expectedData, actualData)
}

func TestBufferTickReordersOutOfOrderBuffers(t *testing.T) {
	var drained []block.DatabaseBlock
	drainFn := func(b block.DatabaseBlock) {
		drained = append(drained, b)
	}

	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())
	start := curr
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer := newDatabaseBuffer(drainFn).(*dbBuffer)
	buffer.Reset(opts)

	// Perform out of order writes that will create two in order encoders
	data := []value{
		{curr, 1, xtime.Second, nil},
		{curr.Add(mins(0.5)), 2, xtime.Second, nil},
		{curr.Add(mins(0.5)).Add(-5 * time.Second), 3, xtime.Second, nil},
		{curr.Add(mins(1.0)), 4, xtime.Second, nil},
		{curr.Add(mins(1.5)), 5, xtime.Second, nil},
		{curr.Add(mins(1.5)).Add(-5 * time.Second), 6, xtime.Second, nil},
	}
	end := data[len(data)-1].timestamp.Add(time.Nanosecond)

	for _, v := range data {
		curr = v.timestamp
		ctx := context.NewContext()
		assert.NoError(t, buffer.Write(ctx, v.timestamp, v.value, v.unit, v.annotation))
		ctx.Close()
	}

	var encoders []encoding.Encoder
	for i := range buffer.bucketsRealTime {
		if !buffer.bucketsRealTime[i].start.Equal(start) {
			continue
		}
		// Current bucket encoders should all have data in them
		for j := range buffer.bucketsRealTime[i].encoders {
			encoder := buffer.bucketsRealTime[i].encoders[j].encoder
			assert.NotNil(t, encoder.Stream())

			encoders = append(encoders, encoder)
		}
	}

	assert.Equal(t, 2, len(encoders))

	// Perform a tick and ensure merged out of order blocks
	r := buffer.Tick()
	assert.Equal(t, 1, r.mergedOutOfOrderBlocks)

	// Check values correct
	ctx := context.NewContext()
	defer ctx.Close()

	results := buffer.ReadEncoded(ctx, start, end)
	expected := make([]value, len(data))
	copy(expected, data)
	sort.Sort(valuesByTime(expected))
	assertValuesEqual(t, expected, results, opts)

	// Count the encoders again
	encoders = encoders[:0]
	for i := range buffer.bucketsRealTime {
		if !buffer.bucketsRealTime[i].start.Equal(start) {
			continue
		}
		// Current bucket encoders should all have data in them
		for j := range buffer.bucketsRealTime[i].encoders {
			encoder := buffer.bucketsRealTime[i].encoders[j].encoder
			assert.NotNil(t, encoder.Stream())

			encoders = append(encoders, encoder)
		}
	}

	// Ensure single encoder again
	assert.Equal(t, 1, len(encoders))
}

func TestBufferSnapshot(t *testing.T) {
	// Setup
	var (
		drained []block.DatabaseBlock
		drainFn = func(b block.DatabaseBlock) {
			drained = append(drained, b)
		}
		opts      = newBufferTestOptions()
		rops      = opts.RetentionOptions()
		blockSize = rops.BlockSize()
		curr      = time.Now().Truncate(blockSize)
		start     = curr
		buffer    = newDatabaseBuffer(drainFn).(*dbBuffer)
	)
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer.Reset(opts)

	// Create test data to perform out of order writes that will create two in-order
	// encoders so we can verify that Snapshot will perform a merge
	data := []value{
		{curr, 1, xtime.Second, nil},
		{curr.Add(mins(0.5)), 2, xtime.Second, nil},
		{curr.Add(mins(0.5)).Add(-5 * time.Second), 3, xtime.Second, nil},
		{curr.Add(mins(1.0)), 4, xtime.Second, nil},
		{curr.Add(mins(1.5)), 5, xtime.Second, nil},
		{curr.Add(mins(1.5)).Add(-5 * time.Second), 6, xtime.Second, nil},

		// Add one write for a different block to make sure Snapshot only returns
		// date for the requested block
		{curr.Add(blockSize), 6, xtime.Second, nil},
	}

	// Perform the writes
	for _, v := range data {
		curr = v.timestamp
		ctx := context.NewContext()
		assert.NoError(t, buffer.Write(ctx, v.timestamp, v.value, v.unit, v.annotation))
		ctx.Close()
	}

	// Verify internal state
	var encoders []encoding.Encoder
	for i := range buffer.bucketsRealTime {
		if !buffer.bucketsRealTime[i].start.Equal(start) {
			continue
		}
		// Current bucket encoders should all have data in them
		for j := range buffer.bucketsRealTime[i].encoders {
			encoder := buffer.bucketsRealTime[i].encoders[j].encoder
			assert.NotNil(t, encoder.Stream())

			encoders = append(encoders, encoder)
		}
	}
	assert.Equal(t, 2, len(encoders))

	// Perform a snapshot
	ctx := context.NewContext()
	defer ctx.Close()
	result, err := buffer.Snapshot(ctx, start)
	assert.NoError(t, err)

	// Check we got the right results
	expectedData := data[:len(data)-1] // -1 because we don't expect the last datapoint
	expectedCopy := make([]value, len(expectedData))
	copy(expectedCopy, expectedData)
	sort.Sort(valuesByTime(expectedCopy))
	actual := [][]xio.BlockReader{{
		xio.BlockReader{
			SegmentReader: result,
		},
	}}
	assertValuesEqual(t, expectedCopy, actual, opts)

	// Check internal state to make sure the merge happened and was persisted
	encoders = encoders[:0]
	for i := range buffer.bucketsRealTime {
		if !buffer.bucketsRealTime[i].start.Equal(start) {
			continue
		}
		// Current bucket encoders should all have data in them
		for j := range buffer.bucketsRealTime[i].encoders {
			encoder := buffer.bucketsRealTime[i].encoders[j].encoder
			assert.NotNil(t, encoder.Stream())

			encoders = append(encoders, encoder)
		}
	}

	// Ensure single encoder again
	assert.Equal(t, 1, len(encoders))
}
