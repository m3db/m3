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
	"github.com/m3db/m3/src/x/context"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

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

	bufferBucketPool := NewBufferBucketPool(nil)
	bufferBucketVersionsPool := NewBufferBucketVersionsPool(nil)

	opts := NewOptions().
		SetEncoderPool(encoderPool).
		SetMultiReaderIteratorPool(multiReaderIteratorPool).
		SetBufferBucketPool(bufferBucketPool).
		SetBufferBucketVersionsPool(bufferBucketVersionsPool)
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

// Writes to buffer, verifying no error and that further writes should happen.
func verifyWriteToBuffer(t *testing.T, buffer databaseBuffer, v value) {
	ctx := context.NewContext()
	wasWritten, err := buffer.Write(ctx, v.timestamp, v.value, v.unit, v.annotation, WriteOptions{})
	require.NoError(t, err)
	require.True(t, wasWritten)
	ctx.Close()
}

func TestBufferWriteTooFuture(t *testing.T) {
	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer := newDatabaseBuffer().(*dbBuffer)
	buffer.Reset(opts)
	ctx := context.NewContext()
	defer ctx.Close()

	wasWritten, err := buffer.Write(ctx, curr.Add(rops.BufferFuture()), 1,
		xtime.Second, nil, WriteOptions{})
	assert.False(t, wasWritten)
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
	buffer.Reset(opts)
	ctx := context.NewContext()
	defer ctx.Close()
	wasWritten, err := buffer.Write(ctx, curr.Add(-1*rops.BufferPast()), 1, xtime.Second,
		nil, WriteOptions{})
	assert.False(t, wasWritten)
	assert.Error(t, err)
	assert.True(t, xerrors.IsInvalidParams(err))
}

func TestBufferWriteError(t *testing.T) {
	var (
		opts   = newBufferTestOptions()
		rops   = opts.RetentionOptions()
		curr   = time.Now().Truncate(rops.BlockSize())
		ctx    = context.NewContext()
		buffer = newDatabaseBuffer().(*dbBuffer)
	)
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer.Reset(opts)
	defer ctx.Close()

	timeUnitNotExist := xtime.Unit(127)
	wasWritten, err := buffer.Write(ctx, curr, 1, timeUnitNotExist, nil, WriteOptions{})
	require.False(t, wasWritten)
	require.Error(t, err)
}

func TestBufferWriteRead(t *testing.T) {
	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer := newDatabaseBuffer().(*dbBuffer)
	buffer.Reset(opts)

	data := []value{
		{curr.Add(secs(1)), 1, xtime.Second, nil},
		{curr.Add(secs(2)), 2, xtime.Second, nil},
		{curr.Add(secs(3)), 3, xtime.Second, nil},
	}

	for _, v := range data {
		verifyWriteToBuffer(t, buffer, v)
	}

	ctx := context.NewContext()
	defer ctx.Close()

	results, err := buffer.ReadEncoded(ctx, timeZero, timeDistantFuture)
	assert.NoError(t, err)
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
	buffer.Reset(opts)

	data := []value{
		{curr.Add(mins(1)), 1, xtime.Second, nil},
		{curr.Add(mins(3)), 2, xtime.Second, nil},
	}

	for _, v := range data {
		curr = v.timestamp
		verifyWriteToBuffer(t, buffer, v)
	}

	ctx := context.NewContext()
	defer ctx.Close()

	firstBucketStart := start.Truncate(time.Second)
	firstBucketEnd := start.Add(mins(2)).Truncate(time.Second)
	results, err := buffer.ReadEncoded(ctx, firstBucketStart, firstBucketEnd)
	assert.NoError(t, err)
	assert.NotNil(t, results)
	assertValuesEqual(t, []value{data[0]}, results, opts)

	secondBucketStart := start.Add(mins(2)).Truncate(time.Second)
	secondBucketEnd := start.Add(mins(4)).Truncate(time.Second)
	results, err = buffer.ReadEncoded(ctx, secondBucketStart, secondBucketEnd)
	assert.NoError(t, err)
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
		verifyWriteToBuffer(t, buffer, v)
	}

	buckets, ok := buffer.bucketVersionsAt(start)
	require.True(t, ok)
	bucket, ok := buckets.writableBucket(WarmWrite)
	require.True(t, ok)
	assert.Equal(t, 2, len(bucket.encoders))
	assert.Equal(t, data[1].timestamp, mustGetLastEncoded(t, bucket.encoders[0]).Timestamp)
	assert.Equal(t, data[2].timestamp, mustGetLastEncoded(t, bucket.encoders[1]).Timestamp)

	// Restore data to in order for comparison.
	sort.Sort(valuesByTime(data))

	ctx := context.NewContext()
	defer ctx.Close()

	results, err := buffer.ReadEncoded(ctx, timeZero, timeDistantFuture)
	assert.NoError(t, err)
	assert.NotNil(t, results)

	assertValuesEqual(t, data, results, opts)
}

func newTestBufferBucketWithData(t *testing.T) (*BufferBucket, Options, []value) {
	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())
	b := &BufferBucket{opts: opts}
	b.resetTo(curr, WarmWrite, opts)
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

	// Empty all existing encoders.
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
	sort.Sort(valuesByTime(expected))
	return b, opts, expected
}

func newTestBufferBucketsWithData(t *testing.T) (*BufferBucketVersions, Options, []value) {
	newBucket, opts, vals := newTestBufferBucketWithData(t)
	return &BufferBucketVersions{
		buckets: []*BufferBucket{newBucket},
		start:   newBucket.start,
		opts:    opts,
	}, opts, vals
}

func TestBufferBucketMerge(t *testing.T) {
	b, opts, expected := newTestBufferBucketWithData(t)

	ctx := context.NewContext()
	defer ctx.Close()
	sr, ok, err := b.mergeToStream(ctx)
	require.NoError(t, err)
	require.True(t, ok)

	assertValuesEqual(t, expected, [][]xio.BlockReader{[]xio.BlockReader{
		xio.BlockReader{
			SegmentReader: sr,
		},
	}}, opts)
}

func TestBufferBucketMergeNilEncoderStreams(t *testing.T) {
	opts := newBufferTestOptions()
	ropts := opts.RetentionOptions()
	curr := time.Now().Truncate(ropts.BlockSize())

	b := &BufferBucket{}
	b.resetTo(curr, WarmWrite, opts)
	emptyEncoder := opts.EncoderPool().Get()
	emptyEncoder.Reset(curr, 0)
	b.encoders = append(b.encoders, inOrderEncoder{encoder: emptyEncoder})

	_, ok := b.encoders[0].encoder.Stream(encoding.StreamOptions{})
	require.False(t, ok)

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

	mergeRes, err := b.merge()
	require.NoError(t, err)
	assert.Equal(t, 1, mergeRes)
	assert.Equal(t, 1, len(b.encoders))
	assert.Equal(t, 0, len(b.bootstrapped))
}

func TestBufferBucketWriteDuplicateUpserts(t *testing.T) {
	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())

	b := &BufferBucket{}
	b.resetTo(curr, WarmWrite, opts)

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
			wasWritten, err := b.write(value.timestamp, value.value,
				value.unit, value.annotation)
			require.NoError(t, err)
			require.True(t, wasWritten)
		}
	}

	// First assert that streams() call is correct.
	ctx := context.NewContext()

	result := b.streams(ctx)
	require.NotNil(t, result)

	results := [][]xio.BlockReader{result}

	assertValuesEqual(t, expected, results, opts)

	// Now assert that mergeToStream() returns same expected result.
	stream, ok, err := b.mergeToStream(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	assertSegmentValuesEqual(t, expected, []xio.SegmentReader{stream}, opts)
}

func TestBufferBucketDuplicatePointsNotWrittenButUpserted(t *testing.T) {
	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())

	b := &BufferBucket{opts: opts}
	b.resetTo(curr, WarmWrite, opts)

	type dataWithShouldWrite struct {
		v value
		w bool
	}

	data := [][]dataWithShouldWrite{
		{
			{w: true, v: value{curr, 1, xtime.Second, nil}},
			{w: false, v: value{curr, 1, xtime.Second, nil}},
			{w: false, v: value{curr, 1, xtime.Second, nil}},
			{w: false, v: value{curr, 1, xtime.Second, nil}},
			{w: true, v: value{curr.Add(secs(10)), 2, xtime.Second, nil}},
		},
		{
			{w: true, v: value{curr, 1, xtime.Second, nil}},
			{w: false, v: value{curr.Add(secs(10)), 2, xtime.Second, nil}},
			{w: true, v: value{curr.Add(secs(10)), 5, xtime.Second, nil}},
		},
		{
			{w: true, v: value{curr, 1, xtime.Second, nil}},
			{w: true, v: value{curr.Add(secs(20)), 8, xtime.Second, nil}},
		},
		{
			{w: true, v: value{curr, 10, xtime.Second, nil}},
			{w: true, v: value{curr.Add(secs(20)), 10, xtime.Second, nil}},
		},
	}

	expected := []value{
		{curr, 10, xtime.Second, nil},
		{curr.Add(secs(10)), 5, xtime.Second, nil},
		{curr.Add(secs(20)), 10, xtime.Second, nil},
	}

	for _, valuesWithMeta := range data {
		for _, valueWithMeta := range valuesWithMeta {
			value := valueWithMeta.v
			wasWritten, err := b.write(value.timestamp, value.value,
				value.unit, value.annotation)
			require.NoError(t, err)
			assert.Equal(t, valueWithMeta.w, wasWritten)
		}
	}

	// First assert that Streams() call is correct.
	ctx := context.NewContext()
	defer ctx.Close()

	result := b.streams(ctx)
	require.NotNil(t, result)

	results := [][]xio.BlockReader{result}

	assertValuesEqual(t, expected, results, opts)

	// Now assert that mergeToStream() returns same expected result.
	stream, ok, err := b.mergeToStream(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	assertSegmentValuesEqual(t, expected, []xio.SegmentReader{stream}, opts)
}

func TestIndexedBufferWriteOnlyWritesSinglePoint(t *testing.T) {
	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer := newDatabaseBuffer().(*dbBuffer)
	buffer.Reset(opts)

	data := []value{
		{curr.Add(secs(1)), 1, xtime.Second, nil},
		{curr.Add(secs(2)), 2, xtime.Second, nil},
		{curr.Add(secs(3)), 3, xtime.Second, nil},
	}

	forceValue := 1.0
	for i, v := range data {
		ctx := context.NewContext()
		writeOpts := WriteOptions{
			TruncateType: TypeBlock,
			TransformOptions: WriteTransformOptions{
				ForceValueEnabled: true,
				ForceValue:        forceValue,
			},
		}
		wasWritten, err := buffer.Write(ctx, v.timestamp, v.value, v.unit,
			v.annotation, writeOpts)
		require.NoError(t, err)
		expectedWrite := i == 0
		require.Equal(t, expectedWrite, wasWritten)
		ctx.Close()
	}

	ctx := context.NewContext()
	defer ctx.Close()

	results, err := buffer.ReadEncoded(ctx, timeZero, timeDistantFuture)
	assert.NoError(t, err)
	assert.NotNil(t, results)

	ex := []value{
		{curr, forceValue, xtime.Second, nil},
	}

	assertValuesEqual(t, ex, results, opts)
}

func TestBufferFetchBlocks(t *testing.T) {
	b, opts, expected := newTestBufferBucketsWithData(t)
	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	buffer := newDatabaseBuffer().(*dbBuffer)
	buffer.Reset(opts)
	buffer.bucketsMap[xtime.ToUnixNano(b.start)] = b

	res := buffer.FetchBlocks(ctx, []time.Time{b.start})
	require.Equal(t, 1, len(res))
	require.Equal(t, b.start, res[0].Start)
	assertValuesEqual(t, expected, [][]xio.BlockReader{res[0].Blocks}, opts)
}

func TestBufferFetchBlocksOneResultPerBlock(t *testing.T) {
	opts := newBufferTestOptions()
	opts.SetColdWritesEnabled(true)
	rOpts := opts.RetentionOptions()
	curr := time.Now().Truncate(rOpts.BlockSize())

	// Set up buffer such that there is a warm and cold bucket for the same
	// block. After we run FetchBlocks, we should see one result per block,
	// even though there are multiple bucket versions with the same block.
	warmBucket := &BufferBucket{opts: opts}
	warmBucket.resetTo(curr, WarmWrite, opts)
	warmBucket.encoders = nil
	coldBucket := &BufferBucket{opts: opts}
	coldBucket.resetTo(curr, ColdWrite, opts)
	coldBucket.encoders = nil
	buckets := []*BufferBucket{warmBucket, coldBucket}
	warmEncoder := [][]value{
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
	coldEncoder := [][]value{
		{
			{curr.Add(secs(15)), 10, xtime.Second, nil},
			{curr.Add(secs(25)), 20, xtime.Second, nil},
			{curr.Add(secs(40)), 30, xtime.Second, nil},
		},
	}
	data := [][][]value{warmEncoder, coldEncoder}

	for i, bucket := range data {
		for _, d := range bucket {
			encoded := 0
			encoder := opts.EncoderPool().Get()
			encoder.Reset(curr, 0)
			for _, v := range d {
				dp := ts.Datapoint{
					Timestamp: v.timestamp,
					Value:     v.value,
				}
				err := encoder.Encode(dp, v.unit, v.annotation)
				require.NoError(t, err)
				encoded++
			}
			buckets[i].encoders = append(buckets[i].encoders, inOrderEncoder{encoder: encoder})
		}
	}

	b := &BufferBucketVersions{
		buckets: buckets,
	}
	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	buffer := newDatabaseBuffer().(*dbBuffer)
	buffer.Reset(opts)
	buffer.bucketsMap[xtime.ToUnixNano(b.start)] = b

	res := buffer.FetchBlocks(ctx, []time.Time{b.start, b.start.Add(time.Second)})
	require.Equal(t, 1, len(res))
	require.Equal(t, b.start, res[0].Start)
	require.Equal(t, 5, len(res[0].Blocks))
}

func TestBufferFetchBlocksMetadata(t *testing.T) {
	b, opts, _ := newTestBufferBucketsWithData(t)

	expectedLastRead := time.Now()
	b.lastReadUnixNanos = expectedLastRead.UnixNano()

	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	start := b.start.Add(-time.Second)
	end := b.start.Add(time.Second)

	buffer := newDatabaseBuffer().(*dbBuffer)
	buffer.Reset(opts)
	buffer.bucketsMap[xtime.ToUnixNano(b.start)] = b
	buffer.inOrderBlockStarts = append(buffer.inOrderBlockStarts, b.start)

	expectedSize := int64(b.streamsLen())

	fetchOpts := FetchBlocksMetadataOptions{
		FetchBlocksMetadataOptions: block.FetchBlocksMetadataOptions{
			IncludeSizes:     true,
			IncludeChecksums: true,
			IncludeLastRead:  true,
		},
	}
	metadata, err := buffer.FetchBlocksMetadata(ctx, start, end, fetchOpts)
	require.NoError(t, err)
	res := metadata.Results()
	require.Equal(t, 1, len(res))
	assert.Equal(t, b.start, res[0].Start)
	assert.Equal(t, expectedSize, res[0].Size)
	// checksum is never available for buffer block.
	assert.Equal(t, (*uint32)(nil), res[0].Checksum)
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
	buffer := newDatabaseBuffer().(*dbBuffer)
	buffer.Reset(opts)

	// Perform out of order writes that will create two in order encoders.
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
		verifyWriteToBuffer(t, buffer, v)
	}

	var encoders []encoding.Encoder
	for _, buckets := range buffer.bucketsMap {
		bucket, ok := buckets.writableBucket(WarmWrite)
		require.True(t, ok)
		// Current bucket encoders should all have data in them.
		for j := range bucket.encoders {
			encoder := bucket.encoders[j].encoder

			_, ok := encoder.Stream(encoding.StreamOptions{})
			require.True(t, ok)

			encoders = append(encoders, encoder)
		}
	}

	assert.Equal(t, 2, len(encoders))

	blockStates := make(map[xtime.UnixNano]BlockState)
	blockStates[xtime.ToUnixNano(start)] = BlockState{
		Retrievable: true,
		Version:     1,
	}
	// Perform a tick and ensure merged out of order blocks.
	r := buffer.Tick(blockStates)
	assert.Equal(t, 1, r.mergedOutOfOrderBlocks)

	// Check values correct.
	ctx := context.NewContext()
	defer ctx.Close()

	results, err := buffer.ReadEncoded(ctx, start, end)
	assert.NoError(t, err)
	expected := make([]value, len(data))
	copy(expected, data)
	sort.Sort(valuesByTime(expected))
	assertValuesEqual(t, expected, results, opts)

	// Count the encoders again.
	encoders = encoders[:0]
	buckets, ok := buffer.bucketVersionsAt(start)
	require.True(t, ok)
	bucket, ok := buckets.writableBucket(WarmWrite)
	require.True(t, ok)
	// Current bucket encoders should all have data in them.
	for j := range bucket.encoders {
		encoder := bucket.encoders[j].encoder

		_, ok := encoder.Stream(encoding.StreamOptions{})
		require.True(t, ok)

		encoders = append(encoders, encoder)
	}

	// Ensure single encoder again.
	assert.Equal(t, 1, len(encoders))
}

func TestBufferRemoveBucket(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())
	start := curr
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer := newDatabaseBuffer().(*dbBuffer)
	buffer.Reset(opts)

	// Perform out of order writes that will create two in order encoders.
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
		verifyWriteToBuffer(t, buffer, v)
	}

	buckets, exists := buffer.bucketVersionsAt(start)
	require.True(t, exists)
	bucket, exists := buckets.writableBucket(WarmWrite)
	require.True(t, exists)

	// Simulate that a flush has fully completed on this bucket so that it will.
	// get removed from the bucket.
	blockStates := make(map[xtime.UnixNano]BlockState)
	blockStates[xtime.ToUnixNano(start)] = BlockState{
		Retrievable: true,
		Version:     1,
	}
	bucket.version = 1

	// False because we just wrote to it.
	assert.False(t, buffer.IsEmpty())
	// Perform a tick to remove the bucket which has been flushed.
	buffer.Tick(blockStates)
	// True because we just removed the bucket.
	assert.True(t, buffer.IsEmpty())
}

func TestBuffertoStream(t *testing.T) {
	b, opts, expected := newTestBufferBucketsWithData(t)
	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	bucket, exists := b.writableBucket(WarmWrite)
	require.True(t, exists)
	assert.Len(t, bucket.encoders, 4)
	assert.Len(t, bucket.bootstrapped, 0)

	stream, err := b.mergeToStreams(ctx, streamsOptions{filterWriteType: false})
	require.NoError(t, err)
	assertSegmentValuesEqual(t, expected, stream, opts)
}

// TestBufferSnapshotEmptyEncoder ensures that snapshot behaves correctly even if an
// encoder is present but it has no data which can occur in some situations such as when
// an initial write fails leaving behind an empty encoder.
func TestBufferSnapshotEmptyEncoder(t *testing.T) {
	testBufferWithEmptyEncoder(t, true)
}

// TestBufferFlushEmptyEncoder ensures that flush behaves correctly even if an encoder
// is present but it has no data which can occur in some situations such as when an
// initial write fails leaving behind an empty encoder.
func TestBufferFlushEmptyEncoder(t *testing.T) {
	testBufferWithEmptyEncoder(t, false)
}

func testBufferWithEmptyEncoder(t *testing.T, testSnapshot bool) {
	// Setup.
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
	buffer.Reset(opts)

	// Perform one valid write to setup the state of the buffer.
	ctx := context.NewContext()
	wasWritten, err := buffer.Write(ctx, curr, 1, xtime.Second, nil, WriteOptions{})
	require.NoError(t, err)
	require.True(t, wasWritten)

	// Verify internal state.
	var encoders []encoding.Encoder
	buckets, ok := buffer.bucketVersionsAt(start)
	require.True(t, ok)
	bucket, ok := buckets.writableBucket(WarmWrite)
	require.True(t, ok)
	for j := range bucket.encoders {
		encoder := bucket.encoders[j].encoder

		_, ok := encoder.Stream(encoding.StreamOptions{})
		require.True(t, ok)

		// Reset the encoder to simulate the situation in which an encoder is present but
		// it is empty.
		encoder.Reset(curr, 0)

		encoders = append(encoders, encoder)
	}
	require.Equal(t, 1, len(encoders))

	assertPersistDataFn := func(id ident.ID, tags ident.Tags, segment ts.Segment, checlsum uint32) error {
		t.Fatal("persist fn should not have been called")
		return nil
	}

	if testSnapshot {
		ctx = context.NewContext()
		defer ctx.Close()
		err = buffer.Snapshot(ctx, start, ident.StringID("some-id"), ident.Tags{}, assertPersistDataFn)
		assert.NoError(t, err)
	} else {
		ctx = context.NewContext()
		defer ctx.Close()
		_, err = buffer.Flush(
			ctx, start, ident.StringID("some-id"), ident.Tags{}, assertPersistDataFn, 1)
		require.NoError(t, err)
	}
}

func TestBufferSnapshot(t *testing.T) {
	// Setup.
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
	buffer.Reset(opts)

	// Create test data to perform out of order writes that will create two in-order
	// encoders so we can verify that Snapshot will perform a merge.
	data := []value{
		{curr, 1, xtime.Second, nil},
		{curr.Add(mins(0.5)), 2, xtime.Second, nil},
		{curr.Add(mins(0.5)).Add(-5 * time.Second), 3, xtime.Second, nil},
		{curr.Add(mins(1.0)), 4, xtime.Second, nil},
		{curr.Add(mins(1.5)), 5, xtime.Second, nil},
		{curr.Add(mins(1.5)).Add(-5 * time.Second), 6, xtime.Second, nil},

		// Add one write for a different block to make sure Snapshot only returns
		// date for the requested block.
		{curr.Add(blockSize), 6, xtime.Second, nil},
	}

	// Perform the writes.
	for _, v := range data {
		curr = v.timestamp
		verifyWriteToBuffer(t, buffer, v)
	}

	// Verify internal state.
	var encoders []encoding.Encoder

	buckets, ok := buffer.bucketVersionsAt(start)
	require.True(t, ok)
	bucket, ok := buckets.writableBucket(WarmWrite)
	require.True(t, ok)
	// Current bucket encoders should all have data in them.
	for j := range bucket.encoders {
		encoder := bucket.encoders[j].encoder

		_, ok := encoder.Stream(encoding.StreamOptions{})
		require.True(t, ok)

		encoders = append(encoders, encoder)
	}

	assert.Equal(t, 2, len(encoders))

	assertPersistDataFn := func(id ident.ID, tags ident.Tags, segment ts.Segment, checlsum uint32) error {
		// Check we got the right results.
		expectedData := data[:len(data)-1] // -1 because we don't expect the last datapoint.
		expectedCopy := make([]value, len(expectedData))
		copy(expectedCopy, expectedData)
		sort.Sort(valuesByTime(expectedCopy))
		actual := [][]xio.BlockReader{{
			xio.BlockReader{
				SegmentReader: xio.NewSegmentReader(segment),
			},
		}}
		assertValuesEqual(t, expectedCopy, actual, opts)

		return nil
	}

	// Perform a snapshot.
	ctx := context.NewContext()
	defer ctx.Close()
	err := buffer.Snapshot(ctx, start, ident.StringID("some-id"), ident.Tags{}, assertPersistDataFn)
	assert.NoError(t, err)

	// Check internal state to make sure the merge happened and was persisted.
	encoders = encoders[:0]
	buckets, ok = buffer.bucketVersionsAt(start)
	require.True(t, ok)
	bucket, ok = buckets.writableBucket(WarmWrite)
	require.True(t, ok)
	// Current bucket encoders should all have data in them.
	for i := range bucket.encoders {
		encoder := bucket.encoders[i].encoder

		_, ok := encoder.Stream(encoding.StreamOptions{})
		require.True(t, ok)

		encoders = append(encoders, encoder)
	}

	// Ensure single encoder again.
	assert.Equal(t, 1, len(encoders))
}

func mustGetLastEncoded(t *testing.T, entry inOrderEncoder) ts.Datapoint {
	last, err := entry.encoder.LastEncoded()
	require.NoError(t, err)
	return last
}

func TestInOrderUnixNanosAddRemove(t *testing.T) {
	buffer := newDatabaseBuffer().(*dbBuffer)
	assertTimeSlicesEqual(t, []time.Time{}, buffer.inOrderBlockStarts)

	t3 := time.Unix(3, 0)
	t5 := time.Unix(5, 0)
	t7 := time.Unix(7, 0)
	t8 := time.Unix(8, 0)

	buffer.inOrderBlockStartsAdd(t5)
	assertTimeSlicesEqual(t, []time.Time{t5}, buffer.inOrderBlockStarts)

	buffer.inOrderBlockStartsAdd(t3)
	assertTimeSlicesEqual(t, []time.Time{t3, t5}, buffer.inOrderBlockStarts)

	buffer.inOrderBlockStartsAdd(t8)
	assertTimeSlicesEqual(t, []time.Time{t3, t5, t8}, buffer.inOrderBlockStarts)

	buffer.inOrderBlockStartsAdd(t7)
	assertTimeSlicesEqual(t, []time.Time{t3, t5, t7, t8}, buffer.inOrderBlockStarts)

	buffer.inOrderBlockStartsRemove(t5)
	assertTimeSlicesEqual(t, []time.Time{t3, t7, t8}, buffer.inOrderBlockStarts)

	buffer.inOrderBlockStartsRemove(t3)
	assertTimeSlicesEqual(t, []time.Time{t7, t8}, buffer.inOrderBlockStarts)

	buffer.inOrderBlockStartsRemove(t8)
	assertTimeSlicesEqual(t, []time.Time{t7}, buffer.inOrderBlockStarts)

	buffer.inOrderBlockStartsRemove(t7)
	assertTimeSlicesEqual(t, []time.Time{}, buffer.inOrderBlockStarts)
}

func assertTimeSlicesEqual(t *testing.T, t1, t2 []time.Time) {
	require.Equal(t, len(t1), len(t2))
	for i := range t1 {
		assert.Equal(t, t1[i], t2[i])
	}
}

func TestEvictedTimes(t *testing.T) {
	var times evictedTimes
	assert.Equal(t, 0, cap(times.slice))
	assert.Equal(t, 0, times.len())
	assert.False(t, times.contains(xtime.UnixNano(0)))

	// These adds should only go in the array.
	for i := 0; i < evictedTimesArraySize; i++ {
		tNano := xtime.UnixNano(i)
		times.add(tNano)

		assert.Equal(t, 0, cap(times.slice))
		assert.Equal(t, i+1, times.arrIdx)
		assert.Equal(t, i+1, times.len())
		assert.True(t, times.contains(tNano))
	}

	// These adds don't fit in the array any more, will go to the slice.
	for i := evictedTimesArraySize; i < evictedTimesArraySize+5; i++ {
		tNano := xtime.UnixNano(i)
		times.add(tNano)

		assert.Equal(t, evictedTimesArraySize, times.arrIdx)
		assert.Equal(t, i+1, times.len())
		assert.True(t, times.contains(tNano))
	}
}
