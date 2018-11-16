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

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3x/context"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
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
			SetBufferPast(10 * time.Second)).
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
	buffer := newDatabaseBuffer().(*dbBuffer)
	buffer.Reset(nil, opts)

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
	buffer := newDatabaseBuffer().(*dbBuffer)
	buffer.Reset(nil, opts)

	ctx := context.NewContext()
	defer ctx.Close()

	err := buffer.Write(ctx, curr.Add(-1*rops.BufferPast()), 1, xtime.Second, nil)
	assert.Error(t, err)
	assert.True(t, xerrors.IsInvalidParams(err))
}

func TestBufferWritePastFutureOutOfOrderEnabled(t *testing.T) {
	opts := newBufferTestOptions()
	rops := opts.RetentionOptions().SetOutOfOrderWritesEnabled(true)
	curr := time.Now().Truncate(rops.BlockSize())
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	})).SetRetentionOptions(rops)
	buffer := newDatabaseBuffer().(*dbBuffer)
	buffer.Reset(nil, opts)

	ctx := context.NewContext()
	defer ctx.Close()

	err := buffer.Write(ctx, curr.Add(-1*rops.BufferPast()), 1, xtime.Second, nil)
	assert.NoError(t, err)

	err = buffer.Write(ctx, curr.Add(rops.BufferFuture()), 1, xtime.Second, nil)
	assert.NoError(t, err)
}

func TestBufferWriteRead(t *testing.T) {
	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer := newDatabaseBuffer().(*dbBuffer)
	buffer.Reset(nil, opts)

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
	buffer := newDatabaseBuffer().(*dbBuffer)
	buffer.Reset(nil, opts)

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

func TestBufferWriteOutOfOrder(t *testing.T) {
	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	start := time.Now().Truncate(rops.BlockSize())
	curr := start
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer := newDatabaseBuffer().(*dbBuffer)
	buffer.Reset(nil, opts)

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

	bucket, ok := buffer.bucketAt(start)
	require.True(t, ok)
	assert.Equal(t, 2, len(bucket.encoders[realtimeType]))
	assert.False(t, bucket.isEmpty())
	assert.Equal(t, data[1].timestamp, mustGetLastEncoded(t, bucket.encoders[realtimeType][0]).Timestamp)
	assert.Equal(t, data[2].timestamp, mustGetLastEncoded(t, bucket.encoders[realtimeType][1]).Timestamp)

	// Restore data to in order for comparison
	sort.Sort(valuesByTime(data))

	ctx := context.NewContext()
	defer ctx.Close()

	results := buffer.ReadEncoded(ctx, timeZero, timeDistantFuture)
	assert.NotNil(t, results)

	assertValuesEqual(t, data, results, opts)
}

func newTestBufferBucketWithData(t *testing.T) (*dbBufferBucket, Options, []value) {
	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())
	b := &dbBufferBucket{opts: opts}
	b.resetTo(curr, opts)
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
	b.encoders[realtimeType] = nil

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
		b.encoders[realtimeType] = append(b.encoders[realtimeType], inOrderEncoder{encoder: encoder})
		expected = append(expected, d...)
	}
	sort.Sort(valuesByTime(expected))
	return b, opts, expected
}

func TestBufferBucketMerge(t *testing.T) {
	b, opts, expected := newTestBufferBucketWithData(t)
	result, err := b.merge(realtimeType)
	require.NoError(t, err)
	assert.Equal(t, 4, result.merges)

	assert.Equal(t, 1, len(b.encoders[realtimeType]))
	assert.Equal(t, 0, len(b.bootstrapped[realtimeType]))

	ctx := context.NewContext()
	defer ctx.Close()
	s, err := b.stream(ctx, realtimeType)
	require.NoError(t, err)

	assertValuesEqual(t, expected, [][]xio.BlockReader{[]xio.BlockReader{
		xio.BlockReader{
			SegmentReader: s,
		},
	}}, opts)
}

func TestBufferBucketMergeNilEncoderStreams(t *testing.T) {
	opts := newBufferTestOptions()
	ropts := opts.RetentionOptions()
	curr := time.Now().Truncate(ropts.BlockSize())

	b := &dbBufferBucket{}
	b.resetTo(curr, opts)
	emptyEncoder := opts.EncoderPool().Get()
	emptyEncoder.Reset(curr, 0)
	b.encoders[realtimeType] = append(b.encoders[realtimeType], inOrderEncoder{encoder: emptyEncoder})
	require.Nil(t, b.encoders[realtimeType][0].encoder.Stream())

	encoder := opts.EncoderPool().Get()
	encoder.Reset(curr, 0)

	value := ts.Datapoint{Timestamp: curr, Value: 1.0}
	err := encoder.Encode(value, xtime.Second, nil)
	require.NoError(t, err)

	blopts := opts.DatabaseBlockOptions()
	newBlock := block.NewDatabaseBlock(curr, 0, encoder.Discard(), blopts)
	b.bootstrapped[realtimeType] = append(b.bootstrapped[realtimeType], newBlock)
	ctx := opts.ContextPool().Get()
	stream, err := b.bootstrapped[realtimeType][0].Stream(ctx)
	require.NoError(t, err)
	require.NotNil(t, stream)

	mergeRes, err := b.merge(realtimeType)
	require.NoError(t, err)
	assert.Equal(t, 1, mergeRes.merges)
	assert.Equal(t, 1, len(b.encoders[realtimeType]))
	assert.Equal(t, 0, len(b.bootstrapped[realtimeType]))
}

func TestBufferBucketWriteDuplicateUpserts(t *testing.T) {
	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())

	b := &dbBufferBucket{}
	b.resetTo(curr, opts)

	data := [][]value{
		{
			{curr, 1, xtime.Second, nil},
			{curr.Add(secs(10)), 2, xtime.Second, nil},
			{curr.Add(secs(50)), 3, xtime.Second, nil},
			{curr.Add(secs(50)), 4, xtime.Second, nil},
		},
		{
			{curr.Add(secs(10)), 5, xtime.Second, nil},
			{curr.Add(secs(40)), 6, xtime.Second, nil},
			{curr.Add(secs(60)), 7, xtime.Second, nil},
		},
		{
			{curr.Add(secs(40)), 8, xtime.Second, nil},
			{curr.Add(secs(70)), 9, xtime.Second, nil},
		},
		{
			{curr.Add(secs(10)), 10, xtime.Second, nil},
			{curr.Add(secs(80)), 11, xtime.Second, nil},
		},
	}

	expected := []value{
		{curr, 1, xtime.Second, nil},
		{curr.Add(secs(10)), 10, xtime.Second, nil},
		{curr.Add(secs(40)), 8, xtime.Second, nil},
		{curr.Add(secs(50)), 4, xtime.Second, nil},
		{curr.Add(secs(60)), 7, xtime.Second, nil},
		{curr.Add(secs(70)), 9, xtime.Second, nil},
		{curr.Add(secs(80)), 11, xtime.Second, nil},
	}

	for _, values := range data {
		for _, value := range values {
			err := b.write(realtimeType, value.timestamp, value.value,
				value.unit, value.annotation)
			require.NoError(t, err)
		}
	}

	// First assert that streams() call is correct
	ctx := context.NewContext()
	defer ctx.Close()

	result := b.streams(ctx, realtimeType)
	require.NotNil(t, result)

	results := [][]xio.BlockReader{result}

	assertValuesEqual(t, expected, results, opts)

	// Now assert that discardMerged() returns same expected result
	stream, err := b.stream(ctx, realtimeType)
	require.NoError(t, err)

	results = [][]xio.BlockReader{[]xio.BlockReader{stream}}

	assertValuesEqual(t, expected, results, opts)
}

func TestBufferFetchBlocks(t *testing.T) {
	b, opts, expected := newTestBufferBucketWithData(t)
	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	buffer := newDatabaseBuffer().(*dbBuffer)
	buffer.Reset(nil, opts)
	buffer.buckets[xtime.ToUnixNano(b.start)] = b

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

	buffer := newDatabaseBuffer().(*dbBuffer)
	buffer.Reset(nil, opts)
	buffer.buckets[xtime.ToUnixNano(b.start)] = b

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

func TestBufferTickReordersOutOfOrderBuffers(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())
	start := curr
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	blockRetriever := NewMockQueryableBlockRetriever(ctrl)
	buffer := newDatabaseBuffer().(*dbBuffer)
	buffer.Reset(blockRetriever, opts)

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
	for i := range buffer.buckets {
		// Current bucket encoders should all have data in them
		for j := range buffer.buckets[i].encoders[realtimeType] {
			encoder := buffer.buckets[i].encoders[realtimeType][j].encoder
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
	for i := range buffer.buckets {
		if !buffer.buckets[i].start.Equal(start) {
			continue
		}
		// Current bucket encoders should all have data in them
		for j := range buffer.buckets[i].encoders[realtimeType] {
			encoder := buffer.buckets[i].encoders[realtimeType][j].encoder
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
		opts      = newBufferTestOptions()
		rops      = opts.RetentionOptions()
		blockSize = rops.BlockSize()
		curr      = time.Now().Truncate(blockSize)
		start     = curr
		buffer    = newDatabaseBuffer().(*dbBuffer)
	)
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer.Reset(nil, opts)

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
	for i := range buffer.buckets {
		if !buffer.buckets[i].start.Equal(start) {
			continue
		}
		// Current bucket encoders should all have data in them
		for j := range buffer.buckets[i].encoders[realtimeType] {
			encoder := buffer.buckets[i].encoders[realtimeType][j].encoder
			assert.NotNil(t, encoder.Stream())

			encoders = append(encoders, encoder)
		}
	}
	assert.Equal(t, 2, len(encoders))

	// Perform a snapshot
	ctx := context.NewContext()
	defer ctx.Close()
	result, err := buffer.Snapshot(ctx, realtimeType, start)
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
	for i := range buffer.buckets {
		if !buffer.buckets[i].start.Equal(start) {
			continue
		}
		// Current bucket encoders should all have data in them
		for j := range buffer.buckets[i].encoders[realtimeType] {
			encoder := buffer.buckets[i].encoders[realtimeType][j].encoder
			assert.NotNil(t, encoder.Stream())

			encoders = append(encoders, encoder)
		}
	}

	// Ensure single encoder again
	assert.Equal(t, 1, len(encoders))
}

func mustGetLastEncoded(t *testing.T, entry inOrderEncoder) ts.Datapoint {
	last, err := entry.encoder.LastEncoded()
	require.NoError(t, err)
	return last
}

func TestBufferFlushRemoveBucketMap(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newBufferTestOptions()

	curr := time.Now()
	blockSize := opts.RetentionOptions().BlockSize()
	blockStart := curr.Truncate(blockSize)
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer := newDatabaseBuffer().(*dbBuffer)
	blockRetriever := NewMockQueryableBlockRetriever(ctrl)
	blockRetriever.EXPECT().IsBlockRetrievable(blockStart).Return(true)
	blockRetriever.EXPECT().BlockLastSuccess(blockStart).Return(curr.Add(10 * time.Minute))
	buffer.Reset(blockRetriever, opts)

	ctx := context.NewContext()
	err := buffer.Write(ctx, curr, 42, xtime.Second, nil)
	ctx.BlockingClose()
	assert.NoError(t, err)

	ctx = context.NewContext()
	persistFn := func(_ ident.ID, _ ident.Tags, _ ts.Segment, _ uint32) error {
		return nil
	}
	outcome, err := buffer.Flush(ctx, blockStart, nil, ident.Tags{}, persistFn)
	ctx.BlockingClose()
	assert.Equal(t, FlushOutcomeFlushedToDisk, outcome)
	assert.NoError(t, err)

	bucket, exists := buffer.bucketAt(blockStart)
	require.True(t, exists)
	assert.True(t, bucket.isEmpty())

	tickRes := buffer.Tick()
	assert.Equal(t, bufferTickResult{
		mergedOutOfOrderBlocks: 0,
		bucketsRemoved:         []time.Time{blockStart},
	}, tickRes)
}
