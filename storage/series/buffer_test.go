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
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/encoding/m3tsz"
	"github.com/m3db/m3db/ts"
	xio "github.com/m3db/m3db/x/io"
	"github.com/m3db/m3x/errors"
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
			SetBufferDrain(30 * time.Second)).
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
	buffer := newDatabaseBuffer(nil, opts).(*dbBuffer)

	ctx := context.NewContext()
	defer ctx.Close()

	err := buffer.Write(ctx, curr.Add(rops.BufferFuture()), 1, xtime.Second, nil)
	assert.Error(t, err)
	assert.True(t, xerrors.IsInvalidParams(err))
}

func TestBufferWriteTooPast(t *testing.T) {
	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer := newDatabaseBuffer(nil, opts).(*dbBuffer)

	ctx := context.NewContext()
	defer ctx.Close()

	err := buffer.Write(ctx, curr.Add(-1*rops.BufferPast()), 1, xtime.Second, nil)
	assert.Error(t, err)
	assert.True(t, xerrors.IsInvalidParams(err))
}

func TestBufferWriteRead(t *testing.T) {
	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer := newDatabaseBuffer(nil, opts).(*dbBuffer)

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
	buffer := newDatabaseBuffer(nil, opts).(*dbBuffer)

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
	var drained []drain
	drainFn := func(start time.Time, encoder encoding.Encoder) {
		drained = append(drained, drain{start, encoder})
	}

	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer := newDatabaseBuffer(drainFn, opts).(*dbBuffer)

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
	assert.Equal(t, 1, len(drained))

	ctx := context.NewContext()
	defer ctx.Close()

	results := buffer.ReadEncoded(ctx, timeZero, timeDistantFuture)
	assert.NotNil(t, results)

	assertValuesEqual(t, data[:4], [][]xio.SegmentReader{[]xio.SegmentReader{
		drained[0].encoder.Stream(),
	}}, opts)
	assertValuesEqual(t, data[4:], results, opts)
}

func TestBufferResetUndrainedBucketDrainsBucket(t *testing.T) {
	var drained []drain
	drainFn := func(start time.Time, encoder encoding.Encoder) {
		drained = append(drained, drain{start, encoder})
	}

	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer := newDatabaseBuffer(drainFn, opts).(*dbBuffer)

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

	assertValuesEqual(t, data[:2], [][]xio.SegmentReader{[]xio.SegmentReader{
		drained[0].encoder.Stream(),
		drained[1].encoder.Stream(),
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
	buffer := newDatabaseBuffer(nil, opts).(*dbBuffer)

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
	assert.Equal(t, 2, len(buffer.buckets[bucketIdx].encoders))
	assert.False(t, buffer.buckets[bucketIdx].empty)
	assert.Equal(t, data[1].timestamp, buffer.buckets[bucketIdx].encoders[0].lastWriteAt)
	assert.Equal(t, data[2].timestamp, buffer.buckets[bucketIdx].encoders[1].lastWriteAt)

	// Restore data to in order for comparison
	sort.Sort(valuesByTime(data))

	ctx := context.NewContext()
	defer ctx.Close()

	results := buffer.ReadEncoded(ctx, timeZero, timeDistantFuture)
	assert.NotNil(t, results)

	assertValuesEqual(t, data, results, opts)

	// Explicitly merge
	for i := range buffer.buckets {
		buffer.buckets[i].merge()
	}

	// Ensure compacted encoders
	for i := range buffer.buckets {
		assert.Len(t, buffer.buckets[i].encoders, 1)
	}

	// Assert equal again
	results = buffer.ReadEncoded(ctx, timeZero, timeDistantFuture)
	assert.NotNil(t, results)

	assertValuesEqual(t, data, results, opts)
}

func initializeTestBufferBucket() (*dbBufferBucket, Options, []value) {
	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())
	b := &dbBufferBucket{opts: opts, start: curr, empty: false}
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

	var expected []value
	for i, d := range data {
		encoder := opts.EncoderPool().Get()
		encoder.Reset(curr, 0)
		for _, v := range data[i] {
			dp := ts.Datapoint{
				Timestamp: v.timestamp,
				Value:     v.value,
			}
			encoder.Encode(dp, v.unit, v.annotation)
		}
		b.encoders = append(b.encoders, inOrderEncoder{encoder: encoder})
		expected = append(expected, d...)
	}
	sort.Sort(valuesByTime(expected))
	return b, opts, expected
}

func TestBufferBucketMerge(t *testing.T) {
	b, opts, expected := initializeTestBufferBucket()

	b.merge()

	assert.Len(t, b.encoders, 1)
	assert.False(t, b.empty)
	assertValuesEqual(t, expected, [][]xio.SegmentReader{[]xio.SegmentReader{
		b.encoders[0].encoder.Stream(),
	}}, opts)
}

func TestBufferBucketWriteDuplicate(t *testing.T) {
	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())

	b := &dbBufferBucket{opts: opts, start: curr, empty: true}
	require.NoError(t, b.write(curr, 1, xtime.Second, nil))
	require.Equal(t, 1, len(b.encoders))
	require.False(t, b.empty)

	encoded := b.encoders[0].encoder.Stream().Segment()
	require.NoError(t, b.write(curr, 1, xtime.Second, nil))
	require.Equal(t, 1, len(b.encoders))
	require.True(t, b.merged())
	require.Equal(t, encoded, b.encoders[0].encoder.Stream().Segment())
}

func TestBufferFetchBlocks(t *testing.T) {
	b, opts, expected := initializeTestBufferBucket()
	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	buffer := newDatabaseBuffer(nil, opts).(*dbBuffer)
	buffer.buckets[0] = *b

	for i := 1; i < 3; i++ {
		newBucketStart := b.start.Add(time.Duration(i) * time.Minute)
		buffer.buckets[i] = dbBufferBucket{
			start:    newBucketStart,
			empty:    true,
			encoders: []inOrderEncoder{{newBucketStart, nil}},
		}
	}

	res := buffer.FetchBlocks(ctx, []time.Time{b.start, b.start.Add(time.Second)})
	require.Equal(t, 1, len(res))
	require.Equal(t, b.start, res[0].Start())
	assertValuesEqual(t, expected, [][]xio.SegmentReader{res[0].Readers()}, opts)
}

func TestBufferFetchBlocksMetadata(t *testing.T) {
	b, opts, _ := initializeTestBufferBucket()
	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	buffer := newDatabaseBuffer(nil, opts).(*dbBuffer)
	buffer.buckets[0] = *b
	var expectedSize int64
	for i := range b.encoders {
		segment := b.encoders[i].encoder.Stream().Segment()
		expectedSize += int64(len(segment.Head) + len(segment.Tail))
	}

	res := buffer.FetchBlocksMetadata(ctx, true, true)
	require.Equal(t, 1, len(res))
	require.Equal(t, b.start, res[0].Start())
	require.Equal(t, expectedSize, *res[0].Size())
	require.Nil(t, res[0].Checksum())
}
