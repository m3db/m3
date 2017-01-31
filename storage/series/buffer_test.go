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
	"github.com/m3db/m3db/storage/block"
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
	require.Equal(t, 1, len(drained))

	ctx := context.NewContext()
	defer ctx.Close()

	results := buffer.ReadEncoded(ctx, timeZero, timeDistantFuture)
	require.NotNil(t, results)

	assertValuesEqual(t, data[:4], [][]xio.SegmentReader{[]xio.SegmentReader{
		requireDrainedStream(ctx, t, drained[0]),
	}}, opts)
	assertValuesEqual(t, data[4:], results, opts)
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
		requireDrainedStream(ctx, t, drained[1]),
		requireDrainedStream(ctx, t, drained[0]),
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
	var mergedResults [][]xio.SegmentReader
	for i := range buffer.buckets {
		block := buffer.buckets[i].discardMerged()
		require.NotNil(t, block)

		if block.Len() > 0 {
			result := []xio.SegmentReader{requireDrainedStream(ctx, t, block)}
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
	b.resetTo(curr)
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

	bl := b.discardMerged()
	require.NotNil(t, bl)

	assert.Equal(t, 0, len(b.encoders))
	assert.Equal(t, 0, len(b.bootstrapped))
	assert.True(t, b.empty)

	ctx := context.NewContext()
	defer ctx.Close()

	assertValuesEqual(t, expected, [][]xio.SegmentReader{[]xio.SegmentReader{
		requireDrainedStream(ctx, t, bl),
	}}, opts)
}

func TestBufferBucketMergeNilEncoderStreams(t *testing.T) {
	opts := newBufferTestOptions()
	ropts := opts.RetentionOptions()
	curr := time.Now().Truncate(ropts.BlockSize())

	b := &dbBufferBucket{opts: opts}
	b.resetTo(curr)
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
	newBlock := block.NewDatabaseBlock(curr, encoder.Discard(), blopts)
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
	b.resetTo(curr)
	require.NoError(t, b.write(curr, 1, xtime.Second, nil))
	require.Equal(t, 1, len(b.encoders))
	require.False(t, b.empty)

	encoded, err := b.encoders[0].encoder.Stream().Segment()
	require.NoError(t, err)
	require.NoError(t, b.write(curr, 1, xtime.Second, nil))
	require.Equal(t, 1, len(b.encoders))

	bl := b.discardMerged()
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

	buffer := newDatabaseBuffer(nil, opts).(*dbBuffer)
	buffer.buckets[0] = *b

	for i := 1; i < 3; i++ {
		newBucketStart := b.start.Add(time.Duration(i) * time.Minute)
		(&buffer.buckets[i]).resetTo(newBucketStart)
		buffer.buckets[i].encoders = []inOrderEncoder{{newBucketStart, nil}}
	}

	res := buffer.FetchBlocks(ctx, []time.Time{b.start, b.start.Add(time.Second)})
	require.Equal(t, 1, len(res))
	require.Equal(t, b.start, res[0].Start())
	assertValuesEqual(t, expected, [][]xio.SegmentReader{res[0].Readers()}, opts)
}

func TestBufferFetchBlocksMetadata(t *testing.T) {
	b, opts, _ := newTestBufferBucketWithData(t)

	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	start := b.start.Add(-time.Second)
	end := b.start.Add(time.Second)

	buffer := newDatabaseBuffer(nil, opts).(*dbBuffer)
	buffer.buckets[0] = *b

	expectedSize := int64(b.streamsLen())

	res := buffer.FetchBlocksMetadata(ctx, start, end, true, true).Results()
	require.Equal(t, 1, len(res))
	require.Equal(t, b.start, res[0].Start)
	require.Equal(t, expectedSize, *res[0].Size)
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
	buffer := newDatabaseBuffer(drainFn, opts).(*dbBuffer)

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
	for i := range buffer.buckets {
		if !buffer.buckets[i].start.Equal(start) {
			continue
		}
		// Current bucket encoders should all have data in them
		for j := range buffer.buckets[i].encoders {
			encoder := buffer.buckets[i].encoders[j].encoder
			assert.NotNil(t, encoder.Stream())

			encoders = append(encoders, encoder)
		}
	}

	require.Equal(t, 2, len(encoders))

	assert.Equal(t, true, buffer.NeedsDrain())
	assert.Equal(t, 0, len(drained))

	ctx := context.NewContext()
	defer ctx.Close()

	// Read and attach context lifetime to the data
	buffer.ReadEncoded(ctx, timeZero, timeDistantFuture)

	buffer.DrainAndReset()

	assert.Equal(t, false, buffer.NeedsDrain())
	assert.Equal(t, 1, len(drained))

	// Ensure all encoders still has data
	for _, encoder := range encoders {
		assert.NotNil(t, encoder.Stream())
	}
}
