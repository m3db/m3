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
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/context"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/dbnode/namespace"
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
	multiReaderIteratorPool.Init(func(r io.Reader, descr namespace.SchemaDescr) encoding.ReaderIterator {
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
func verifyWriteToBuffer(t *testing.T, buffer databaseBuffer, v value, schema namespace.SchemaDescr) {
	ctx := context.NewContext()
	wasWritten, err := buffer.Write(ctx, v.timestamp, v.value, v.unit, v.annotation, WriteOptions{SchemaDesc: schema})
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
	buffer.Reset(ident.StringID("foo"), opts)
	ctx := context.NewContext()
	defer ctx.Close()

	wasWritten, err := buffer.Write(ctx, curr.Add(rops.BufferFuture()), 1,
		xtime.Second, nil, WriteOptions{})
	assert.False(t, wasWritten)
	assert.Error(t, err)
	assert.True(t, xerrors.IsInvalidParams(err))
	assert.True(t, strings.Contains(err.Error(), "datapoint too far in future"))
	assert.True(t, strings.Contains(err.Error(), "id=foo"))
	assert.True(t, strings.Contains(err.Error(), "timestamp="))
	assert.True(t, strings.Contains(err.Error(), "future_limit="))
}

func TestBufferWriteTooPast(t *testing.T) {
	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer := newDatabaseBuffer().(*dbBuffer)
	buffer.Reset(ident.StringID("foo"), opts)
	ctx := context.NewContext()
	defer ctx.Close()
	wasWritten, err := buffer.Write(ctx, curr.Add(-1*rops.BufferPast()), 1, xtime.Second,
		nil, WriteOptions{})
	assert.False(t, wasWritten)
	assert.Error(t, err)
	assert.True(t, xerrors.IsInvalidParams(err))
	assert.True(t, strings.Contains(err.Error(), "datapoint too far in past"))
	assert.True(t, strings.Contains(err.Error(), "id=foo"))
	assert.True(t, strings.Contains(err.Error(), "timestamp="))
	assert.True(t, strings.Contains(err.Error(), "past_limit="))
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
	buffer.Reset(ident.StringID("foo"), opts)
	defer ctx.Close()

	timeUnitNotExist := xtime.Unit(127)
	wasWritten, err := buffer.Write(ctx, curr, 1, timeUnitNotExist, nil, WriteOptions{})
	require.False(t, wasWritten)
	require.Error(t, err)
}

func TestBufferWriteRead(t *testing.T) {
	opts := newBufferTestOptions()
	testBufferWriteRead(t, opts, nil)
}

func testBufferWriteRead(t *testing.T, opts Options, setAnn setAnnotation) {
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer := newDatabaseBuffer().(*dbBuffer)
	buffer.Reset(ident.StringID("foo"), opts)

	data := []value{
		{curr.Add(secs(1)), 1, xtime.Second, nil},
		{curr.Add(secs(2)), 2, xtime.Second, nil},
		{curr.Add(secs(3)), 3, xtime.Second, nil},
	}
	var nsCtx namespace.Context
	if setAnn != nil {
		data = setAnn(data)
		nsCtx = namespace.Context{Schema: testSchemaDesc}
	}

	for _, v := range data {
		verifyWriteToBuffer(t, buffer, v, nsCtx.Schema)
	}

	ctx := context.NewContext()
	defer ctx.Close()

	results, err := buffer.ReadEncoded(ctx, timeZero, timeDistantFuture, nsCtx)
	assert.NoError(t, err)
	assert.NotNil(t, results)

	requireReaderValuesEqual(t, data, results, opts, nsCtx)
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
	buffer.Reset(ident.StringID("foo"), opts)

	data := []value{
		{curr.Add(mins(1)), 1, xtime.Second, nil},
		{curr.Add(mins(3)), 2, xtime.Second, nil},
	}

	for _, v := range data {
		curr = v.timestamp
		verifyWriteToBuffer(t, buffer, v, nil)
	}

	ctx := context.NewContext()
	defer ctx.Close()

	firstBucketStart := start.Truncate(time.Second)
	firstBucketEnd := start.Add(mins(2)).Truncate(time.Second)
	results, err := buffer.ReadEncoded(ctx, firstBucketStart, firstBucketEnd, namespace.Context{})
	assert.NoError(t, err)
	assert.NotNil(t, results)
	requireReaderValuesEqual(t, []value{data[0]}, results, opts, namespace.Context{})

	secondBucketStart := start.Add(mins(2)).Truncate(time.Second)
	secondBucketEnd := start.Add(mins(4)).Truncate(time.Second)
	results, err = buffer.ReadEncoded(ctx, secondBucketStart, secondBucketEnd, namespace.Context{})
	assert.NoError(t, err)
	assert.NotNil(t, results)

	requireReaderValuesEqual(t, []value{data[1]}, results, opts, namespace.Context{})
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
	buffer.Reset(ident.StringID("foo"), opts)

	data := []value{
		{curr, 1, xtime.Second, nil},
		{curr.Add(secs(10)), 2, xtime.Second, nil},
		{curr.Add(secs(5)), 3, xtime.Second, nil},
	}

	for _, v := range data {
		if v.timestamp.After(curr) {
			curr = v.timestamp
		}
		verifyWriteToBuffer(t, buffer, v, nil)
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

	results, err := buffer.ReadEncoded(ctx, timeZero, timeDistantFuture, namespace.Context{})
	assert.NoError(t, err)
	assert.NotNil(t, results)

	requireReaderValuesEqual(t, data, results, opts, namespace.Context{})
}

func newTestBufferBucketWithData(t *testing.T, opts Options, setAnn setAnnotation) (*BufferBucket, []value) {
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())

	bd := blockData{
		start:     curr,
		writeType: WarmWrite,
		data: [][]value{
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
		},
	}

	return newTestBufferBucketWithCustomData(t, bd, opts, setAnn)
}

func newTestBufferBucketWithCustomData(
	t *testing.T,
	bd blockData,
	opts Options,
	setAnn setAnnotation,
) (*BufferBucket, []value) {
	b := &BufferBucket{opts: opts}
	b.resetTo(bd.start, bd.writeType, opts)
	data := bd.data

	// Empty all existing encoders.
	b.encoders = nil

	var nsCtx namespace.Context
	if setAnn != nil {
		nsCtx = namespace.Context{Schema: testSchemaDesc}
	}
	var expected []value
	for i := 0; i < len(data); i++ {
		if setAnn != nil {
			data[i] = setAnn(data[i])
		}

		encoded := 0
		encoder := opts.EncoderPool().Get()
		encoder.Reset(bd.start, 0, nsCtx.Schema)
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
		expected = append(expected, data[i]...)
	}
	sort.Sort(valuesByTime(expected))
	return b, expected
}

func newTestBufferBucketsWithData(t *testing.T, opts Options, setAnn setAnnotation) (*BufferBucketVersions, []value) {
	newBucket, vals := newTestBufferBucketWithData(t, opts, setAnn)
	return &BufferBucketVersions{
		buckets: []*BufferBucket{newBucket},
		start:   newBucket.start,
		opts:    opts,
	}, vals
}

func newTestBufferBucketVersionsWithCustomData(
	t *testing.T,
	bd blockData,
	opts Options,
	setAnn setAnnotation,
) (*BufferBucketVersions, []value) {
	newBucket, vals := newTestBufferBucketWithCustomData(t, bd, opts, setAnn)
	return &BufferBucketVersions{
		buckets: []*BufferBucket{newBucket},
		start:   newBucket.start,
		opts:    opts,
	}, vals
}

func newTestBufferWithCustomData(
	t *testing.T,
	blockDatas []blockData,
	opts Options,
	setAnn setAnnotation,
) (*dbBuffer, map[xtime.UnixNano][]value) {
	buffer := newDatabaseBuffer().(*dbBuffer)
	buffer.Reset(ident.StringID("foo"), opts)
	expectedMap := make(map[xtime.UnixNano][]value)

	for _, bd := range blockDatas {
		bucketVersions, expected := newTestBufferBucketVersionsWithCustomData(t, bd, opts, setAnn)
		buffer.bucketsMap[xtime.ToUnixNano(bd.start)] = bucketVersions
		expectedMap[xtime.ToUnixNano(bd.start)] = expected
	}

	return buffer, expectedMap
}

func TestBufferBucketMerge(t *testing.T) {
	opts := newBufferTestOptions()

	testBufferBucketMerge(t, opts, nil)
}

func testBufferBucketMerge(t *testing.T, opts Options, setAnn setAnnotation) {
	b, expected := newTestBufferBucketWithData(t, opts, setAnn)

	ctx := context.NewContext()
	defer ctx.Close()

	nsCtx := namespace.Context{}
	if setAnn != nil {
		nsCtx.Schema = testSchemaDesc
	}
	sr, ok, err := b.mergeToStream(ctx, nsCtx)

	require.NoError(t, err)
	require.True(t, ok)

	requireReaderValuesEqual(t, expected, [][]xio.BlockReader{[]xio.BlockReader{
		xio.BlockReader{
			SegmentReader: sr,
		},
	}}, opts, nsCtx)
}

func TestBufferBucketMergeNilEncoderStreams(t *testing.T) {
	opts := newBufferTestOptions()
	ropts := opts.RetentionOptions()
	curr := time.Now().Truncate(ropts.BlockSize())

	b := &BufferBucket{}
	b.resetTo(curr, WarmWrite, opts)
	emptyEncoder := opts.EncoderPool().Get()
	emptyEncoder.Reset(curr, 0, nil)
	b.encoders = append(b.encoders, inOrderEncoder{encoder: emptyEncoder})

	_, ok := b.encoders[0].encoder.Stream(encoding.StreamOptions{})
	require.False(t, ok)

	encoder := opts.EncoderPool().Get()
	encoder.Reset(curr, 0, nil)

	value := ts.Datapoint{Timestamp: curr, Value: 1.0}
	err := encoder.Encode(value, xtime.Second, nil)
	require.NoError(t, err)

	blopts := opts.DatabaseBlockOptions()
	newBlock := block.NewDatabaseBlock(curr, 0, encoder.Discard(), blopts, namespace.Context{})
	b.loadedBlocks = append(b.loadedBlocks, newBlock)
	ctx := opts.ContextPool().Get()
	stream, err := b.loadedBlocks[0].Stream(ctx)
	require.NoError(t, err)
	require.NotNil(t, stream)

	mergeRes, err := b.merge(namespace.Context{})
	require.NoError(t, err)
	assert.Equal(t, 1, mergeRes)
	assert.Equal(t, 1, len(b.encoders))
	assert.Equal(t, 0, len(b.loadedBlocks))
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
				value.unit, value.annotation, nil)
			require.NoError(t, err)
			require.True(t, wasWritten)
		}
	}

	// First assert that streams() call is correct.
	ctx := context.NewContext()

	result := b.streams(ctx)
	require.NotNil(t, result)

	results := [][]xio.BlockReader{result}

	requireReaderValuesEqual(t, expected, results, opts, namespace.Context{})

	// Now assert that mergeToStream() returns same expected result.
	stream, ok, err := b.mergeToStream(ctx, namespace.Context{})
	require.NoError(t, err)
	require.True(t, ok)
	requireSegmentValuesEqual(t, expected, []xio.SegmentReader{stream}, opts, namespace.Context{})
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
				value.unit, value.annotation, nil)
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

	requireReaderValuesEqual(t, expected, results, opts, namespace.Context{})

	// Now assert that mergeToStream() returns same expected result.
	stream, ok, err := b.mergeToStream(ctx, namespace.Context{})
	require.NoError(t, err)
	require.True(t, ok)
	requireSegmentValuesEqual(t, expected, []xio.SegmentReader{stream}, opts, namespace.Context{})
}

func TestIndexedBufferWriteOnlyWritesSinglePoint(t *testing.T) {
	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	curr := time.Now().Truncate(rops.BlockSize())
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer := newDatabaseBuffer().(*dbBuffer)
	buffer.Reset(ident.StringID("foo"), opts)

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

	results, err := buffer.ReadEncoded(ctx, timeZero, timeDistantFuture, namespace.Context{})
	assert.NoError(t, err)
	assert.NotNil(t, results)

	ex := []value{
		{curr, forceValue, xtime.Second, nil},
	}

	requireReaderValuesEqual(t, ex, results, opts, namespace.Context{})
}

func TestBufferFetchBlocks(t *testing.T) {
	opts := newBufferTestOptions()
	testBufferFetchBlocks(t, opts, nil)
}

func testBufferFetchBlocks(t *testing.T, opts Options, setAnn setAnnotation) {
	b, expected := newTestBufferBucketsWithData(t, opts, setAnn)
	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	buffer := newDatabaseBuffer().(*dbBuffer)
	buffer.Reset(ident.StringID("foo"), opts)
	buffer.bucketsMap[xtime.ToUnixNano(b.start)] = b

	nsCtx := namespace.Context{}
	if setAnn != nil {
		nsCtx.Schema = testSchemaDesc
	}
	res := buffer.FetchBlocks(ctx, []time.Time{b.start}, nsCtx)
	require.Equal(t, 1, len(res))
	require.Equal(t, b.start, res[0].Start)
	requireReaderValuesEqual(t, expected, [][]xio.BlockReader{res[0].Blocks}, opts, nsCtx)
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
			encoder.Reset(curr, 0, nil)
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
	buffer.Reset(ident.StringID("foo"), opts)
	buffer.bucketsMap[xtime.ToUnixNano(b.start)] = b

	res := buffer.FetchBlocks(ctx, []time.Time{b.start, b.start.Add(time.Second)}, namespace.Context{})
	require.Equal(t, 1, len(res))
	require.Equal(t, b.start, res[0].Start)
	require.Equal(t, 5, len(res[0].Blocks))
}

func TestBufferFetchBlocksMetadata(t *testing.T) {
	opts := newBufferTestOptions()

	b, _ := newTestBufferBucketsWithData(t, opts, nil)

	expectedLastRead := time.Now()
	b.lastReadUnixNanos = expectedLastRead.UnixNano()

	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	start := b.start.Add(-time.Second)
	end := b.start.Add(time.Second)

	buffer := newDatabaseBuffer().(*dbBuffer)
	buffer.Reset(ident.StringID("foo"), opts)
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
	require.Equal(t, b.start, res[0].Start)
	require.Equal(t, expectedSize, res[0].Size)
	// Checksum not available since there are multiple streams.
	require.Equal(t, (*uint32)(nil), res[0].Checksum)
	require.True(t, expectedLastRead.Equal(res[0].LastRead))

	// Tick to merge all of the streams into one.
	buffer.Tick(ShardBlockStateSnapshot{}, namespace.Context{})
	metadata, err = buffer.FetchBlocksMetadata(ctx, start, end, fetchOpts)
	require.NoError(t, err)
	res = metadata.Results()
	require.Equal(t, 1, len(res))
	// Checksum should be available now since there was only one stream.
	require.NotNil(t, res[0].Checksum)
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
	buffer.Reset(ident.StringID("foo"), opts)

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
		verifyWriteToBuffer(t, buffer, v, nil)
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

	blockStates := BootstrappedBlockStateSnapshot{
		Snapshot: map[xtime.UnixNano]BlockState{
			xtime.ToUnixNano(start): BlockState{
				WarmRetrievable: true,
				ColdVersion:     1,
			},
		},
	}
	shardBlockState := NewShardBlockStateSnapshot(true, blockStates)
	// Perform a tick and ensure merged out of order blocks.
	r := buffer.Tick(shardBlockState, namespace.Context{})
	assert.Equal(t, 1, r.mergedOutOfOrderBlocks)

	// Check values correct.
	ctx := context.NewContext()
	defer ctx.Close()

	results, err := buffer.ReadEncoded(ctx, start, end, namespace.Context{})
	assert.NoError(t, err)
	expected := make([]value, len(data))
	copy(expected, data)
	sort.Sort(valuesByTime(expected))
	requireReaderValuesEqual(t, expected, results, opts, namespace.Context{})

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
	buffer.Reset(ident.StringID("foo"), opts)

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
		verifyWriteToBuffer(t, buffer, v, nil)
	}

	buckets, exists := buffer.bucketVersionsAt(start)
	require.True(t, exists)
	bucket, exists := buckets.writableBucket(WarmWrite)
	require.True(t, exists)

	// Simulate that a flush has fully completed on this bucket so that it will.
	// get removed from the bucket.
	blockStates := BootstrappedBlockStateSnapshot{
		Snapshot: map[xtime.UnixNano]BlockState{
			xtime.ToUnixNano(start): BlockState{
				WarmRetrievable: true,
				ColdVersion:     1,
			},
		},
	}
	shardBlockState := NewShardBlockStateSnapshot(true, blockStates)
	bucket.version = 1

	// False because we just wrote to it.
	assert.False(t, buffer.IsEmpty())
	// Perform a tick to remove the bucket which has been flushed.
	buffer.Tick(shardBlockState, namespace.Context{})
	// True because we just removed the bucket.
	assert.True(t, buffer.IsEmpty())
}

func TestBuffertoStream(t *testing.T) {
	opts := newBufferTestOptions()

	testBuffertoStream(t, opts, nil)
}

func testBuffertoStream(t *testing.T, opts Options, setAnn setAnnotation) {
	b, expected := newTestBufferBucketsWithData(t, opts, setAnn)
	ctx := opts.ContextPool().Get()
	defer ctx.Close()
	nsCtx := namespace.Context{}
	if setAnn != nil {
		nsCtx.Schema = testSchemaDesc
	}

	bucket, exists := b.writableBucket(WarmWrite)
	require.True(t, exists)
	assert.Len(t, bucket.encoders, 4)
	assert.Len(t, bucket.loadedBlocks, 0)

	stream, err := b.mergeToStreams(ctx, streamsOptions{filterWriteType: false, nsCtx: nsCtx})
	require.NoError(t, err)
	requireSegmentValuesEqual(t, expected, stream, opts, nsCtx)
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
	buffer.Reset(ident.StringID("foo"), opts)

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
		encoder.Reset(curr, 0, nil)

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
		err = buffer.Snapshot(ctx, start, ident.StringID("some-id"), ident.Tags{}, assertPersistDataFn, namespace.Context{})
		assert.NoError(t, err)
	} else {
		ctx = context.NewContext()
		defer ctx.Close()
		_, err = buffer.WarmFlush(
			ctx, start, ident.StringID("some-id"), ident.Tags{}, assertPersistDataFn, namespace.Context{})
		require.NoError(t, err)
	}
}

func TestBufferSnapshot(t *testing.T) {
	opts := newBufferTestOptions()
	testBufferSnapshot(t, opts, nil)
}

func testBufferSnapshot(t *testing.T, opts Options, setAnn setAnnotation) {
	// Setup
	var (
		rops      = opts.RetentionOptions()
		blockSize = rops.BlockSize()
		curr      = time.Now().Truncate(blockSize)
		start     = curr
		buffer    = newDatabaseBuffer().(*dbBuffer)
		nsCtx     namespace.Context
	)
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer.Reset(ident.StringID("foo"), opts)

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
	if setAnn != nil {
		data = setAnn(data)
		nsCtx = namespace.Context{Schema: testSchemaDesc}
	}

	// Perform the writes.
	for _, v := range data {
		curr = v.timestamp
		verifyWriteToBuffer(t, buffer, v, nsCtx.Schema)
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
		requireReaderValuesEqual(t, expectedCopy, actual, opts, nsCtx)

		return nil
	}

	// Perform a snapshot.
	ctx := context.NewContext()
	defer ctx.Close()
	err := buffer.Snapshot(ctx, start, ident.StringID("some-id"), ident.Tags{}, assertPersistDataFn, nsCtx)
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

func TestBufferSnapshotWithColdWrites(t *testing.T) {
	opts := newBufferTestOptions().SetColdWritesEnabled(true)

	var (
		rops      = opts.RetentionOptions()
		blockSize = rops.BlockSize()
		curr      = time.Now().Truncate(blockSize)
		start     = curr
		buffer    = newDatabaseBuffer().(*dbBuffer)
		nsCtx     namespace.Context
	)
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr
	}))
	buffer.Reset(ident.StringID("foo"), opts)

	// Create test data to perform warm writes that will create two in-order
	// encoders so we can verify that Snapshot will perform a merge.
	warmData := []value{
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

	// Perform warm writes.
	for _, v := range warmData {
		// Set curr so that every write is a warm write.
		curr = v.timestamp
		verifyWriteToBuffer(t, buffer, v, nsCtx.Schema)
	}

	// Also add cold writes to the buffer to verify that Snapshot will capture
	// cold writes as well and perform a merge across both warm and cold data.
	// The cold data itself is not in order, so we expect to have two in-order
	// encoders for these.
	curr = start.Add(mins(1.5))
	// In order for these writes to actually be cold, they all need to have
	// timestamps before `curr.Add(-rops.BufferPast())`. Take care to not use
	// the same timestamps used in the warm writes above, otherwise these will
	// overwrite them.
	// Buffer past/future in this test case is 10 seconds.
	coldData := []value{
		{start.Add(secs(2)), 11, xtime.Second, nil},
		{start.Add(secs(4)), 12, xtime.Second, nil},
		{start.Add(secs(6)), 13, xtime.Second, nil},
		{start.Add(secs(3)), 14, xtime.Second, nil},
		{start.Add(secs(5)), 15, xtime.Second, nil},
	}

	// Perform cold writes.
	for _, v := range coldData {
		verifyWriteToBuffer(t, buffer, v, nsCtx.Schema)
	}

	// Verify internal state.
	var (
		warmEncoders []encoding.Encoder
		coldEncoders []encoding.Encoder
	)

	buckets, ok := buffer.bucketVersionsAt(start)
	require.True(t, ok)

	bucket, ok := buckets.writableBucket(WarmWrite)
	require.True(t, ok)
	// Warm bucket encoders should all have data in them.
	for j := range bucket.encoders {
		encoder := bucket.encoders[j].encoder

		_, ok := encoder.Stream(encoding.StreamOptions{})
		require.True(t, ok)

		warmEncoders = append(warmEncoders, encoder)
	}
	assert.Equal(t, 2, len(warmEncoders))

	bucket, ok = buckets.writableBucket(ColdWrite)
	require.True(t, ok)
	// Cold bucket encoders should all have data in them.
	for j := range bucket.encoders {
		encoder := bucket.encoders[j].encoder

		_, ok := encoder.Stream(encoding.StreamOptions{})
		require.True(t, ok)

		coldEncoders = append(coldEncoders, encoder)
	}
	assert.Equal(t, 2, len(coldEncoders))

	assertPersistDataFn := func(id ident.ID, tags ident.Tags, segment ts.Segment, checlsum uint32) error {
		// Check we got the right results.
		// `len(warmData)-1` because we don't expect the last warm datapoint
		// since it's for a different block.
		expectedData := warmData[:len(warmData)-1]
		expectedData = append(expectedData, coldData...)
		expectedCopy := make([]value, len(expectedData))
		copy(expectedCopy, expectedData)
		sort.Sort(valuesByTime(expectedCopy))
		actual := [][]xio.BlockReader{{
			xio.BlockReader{
				SegmentReader: xio.NewSegmentReader(segment),
			},
		}}
		requireReaderValuesEqual(t, expectedCopy, actual, opts, nsCtx)

		return nil
	}

	// Perform a snapshot.
	ctx := context.NewContext()
	defer ctx.Close()
	err := buffer.Snapshot(ctx, start, ident.StringID("some-id"), ident.Tags{}, assertPersistDataFn, nsCtx)
	require.NoError(t, err)

	// Check internal state of warm bucket to make sure the merge happened and
	// was persisted.
	warmEncoders = warmEncoders[:0]
	buckets, ok = buffer.bucketVersionsAt(start)
	require.True(t, ok)
	bucket, ok = buckets.writableBucket(WarmWrite)
	require.True(t, ok)
	// Current bucket encoders should all have data in them.
	for i := range bucket.encoders {
		encoder := bucket.encoders[i].encoder

		_, ok := encoder.Stream(encoding.StreamOptions{})
		require.True(t, ok)

		warmEncoders = append(warmEncoders, encoder)
	}
	// Ensure single encoder again.
	assert.Equal(t, 1, len(warmEncoders))

	// Check internal state of cold bucket to make sure the merge happened and
	// was persisted.
	coldEncoders = coldEncoders[:0]
	buckets, ok = buffer.bucketVersionsAt(start)
	require.True(t, ok)
	bucket, ok = buckets.writableBucket(ColdWrite)
	require.True(t, ok)
	// Current bucket encoders should all have data in them.
	for i := range bucket.encoders {
		encoder := bucket.encoders[i].encoder

		_, ok := encoder.Stream(encoding.StreamOptions{})
		require.True(t, ok)

		coldEncoders = append(coldEncoders, encoder)
	}
	// Ensure single encoder again.
	assert.Equal(t, 1, len(coldEncoders))
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

func TestOptimizedTimes(t *testing.T) {
	var times OptimizedTimes
	assert.Equal(t, 0, cap(times.slice))
	assert.Equal(t, 0, times.Len())
	assert.False(t, times.Contains(xtime.UnixNano(0)))

	var expectedTimes []xtime.UnixNano
	var forEachTimes []xtime.UnixNano
	// ForEach should only call the provided func if there are actual times in
	// OptimizedTimes (OptimizedTimes contains an xtime.UnixNano array
	// internally and we don't want to run the func for those zero values unless
	// they were explicitly added).
	times.ForEach(func(tNano xtime.UnixNano) {
		forEachTimes = append(forEachTimes, tNano)
	})
	assertEqualUnixSlices(t, expectedTimes, forEachTimes)

	expectedTimes = expectedTimes[:0]
	forEachTimes = forEachTimes[:0]

	// These adds should only go in the array.
	for i := 0; i < optimizedTimesArraySize; i++ {
		tNano := xtime.UnixNano(i)
		times.Add(tNano)
		expectedTimes = append(expectedTimes, tNano)

		assert.Equal(t, 0, cap(times.slice))
		assert.Equal(t, i+1, times.arrIdx)
		assert.Equal(t, i+1, times.Len())
		assert.True(t, times.Contains(tNano))
	}

	numExtra := 5
	// These adds don't fit in the array any more, will go to the slice.
	for i := optimizedTimesArraySize; i < optimizedTimesArraySize+numExtra; i++ {
		tNano := xtime.UnixNano(i)
		times.Add(tNano)
		expectedTimes = append(expectedTimes, tNano)

		assert.Equal(t, optimizedTimesArraySize, times.arrIdx)
		assert.Equal(t, i+1, times.Len())
		assert.True(t, times.Contains(tNano))
	}

	times.ForEach(func(tNano xtime.UnixNano) {
		forEachTimes = append(forEachTimes, tNano)
	})

	assertEqualUnixSlices(t, expectedTimes, forEachTimes)
}

func assertEqualUnixSlices(t *testing.T, expected, actual []xtime.UnixNano) {
	require.Equal(t, len(expected), len(actual))
	for i := range expected {
		assert.Equal(t, expected[i], actual[i])
	}
}

func TestColdFlushBlockStarts(t *testing.T) {
	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	blockSize := rops.BlockSize()
	blockStart4 := time.Now().Truncate(blockSize)
	blockStart3 := blockStart4.Add(-2 * blockSize)
	blockStart2 := blockStart4.Add(-3 * blockSize)
	blockStart1 := blockStart4.Add(-4 * blockSize)

	bds := []blockData{
		blockData{
			start:     blockStart1,
			writeType: ColdWrite,
			data: [][]value{
				{
					{blockStart1, 1, xtime.Second, nil},
					{blockStart1.Add(secs(5)), 2, xtime.Second, nil},
					{blockStart1.Add(secs(10)), 3, xtime.Second, nil},
				},
			},
		},
		blockData{
			start:     blockStart2,
			writeType: ColdWrite,
			data: [][]value{
				{
					{blockStart2.Add(secs(2)), 4, xtime.Second, nil},
					{blockStart2.Add(secs(5)), 5, xtime.Second, nil},
					{blockStart2.Add(secs(11)), 6, xtime.Second, nil},
					{blockStart2.Add(secs(15)), 7, xtime.Second, nil},
					{blockStart2.Add(secs(40)), 8, xtime.Second, nil},
				},
			},
		},
		blockData{
			start:     blockStart3,
			writeType: ColdWrite,
			data: [][]value{
				{
					{blockStart3.Add(secs(71)), 9, xtime.Second, nil},
				},
			},
		},
		blockData{
			start:     blockStart4,
			writeType: WarmWrite,
			data: [][]value{
				{
					{blockStart4.Add(secs(57)), 10, xtime.Second, nil},
					{blockStart4.Add(secs(66)), 11, xtime.Second, nil},
					{blockStart4.Add(secs(80)), 12, xtime.Second, nil},
					{blockStart4.Add(secs(81)), 13, xtime.Second, nil},
					{blockStart4.Add(secs(82)), 14, xtime.Second, nil},
					{blockStart4.Add(secs(96)), 15, xtime.Second, nil},
				},
			},
		},
	}

	blockStartNano1 := xtime.ToUnixNano(blockStart1)
	blockStartNano2 := xtime.ToUnixNano(blockStart2)
	blockStartNano3 := xtime.ToUnixNano(blockStart3)

	buffer, _ := newTestBufferWithCustomData(t, bds, opts, nil)
	blockStates := make(map[xtime.UnixNano]BlockState)
	blockStates[blockStartNano1] = BlockState{
		WarmRetrievable: true,
		ColdVersion:     0,
	}
	blockStates[blockStartNano2] = BlockState{
		WarmRetrievable: true,
		ColdVersion:     0,
	}
	blockStates[blockStartNano3] = BlockState{
		WarmRetrievable: true,
		ColdVersion:     0,
	}
	flushStarts := buffer.ColdFlushBlockStarts(blockStates)

	// All three cold blocks should report that they are dirty.
	assert.Equal(t, 3, flushStarts.Len())
	assert.True(t, flushStarts.Contains(blockStartNano1))
	assert.True(t, flushStarts.Contains(blockStartNano2))
	assert.True(t, flushStarts.Contains(blockStartNano3))

	// Simulate that block2 and block3 are flushed (but not yet evicted from
	// memory), so only block1 should report as dirty.
	buffer.bucketsMap[blockStartNano2].buckets[0].version = 1
	buffer.bucketsMap[blockStartNano3].buckets[0].version = 1
	blockStates[blockStartNano2] = BlockState{
		WarmRetrievable: true,
		ColdVersion:     1,
	}
	blockStates[blockStartNano3] = BlockState{
		WarmRetrievable: true,
		ColdVersion:     1,
	}

	flushStarts = buffer.ColdFlushBlockStarts(blockStates)
	assert.Equal(t, 1, flushStarts.Len())
	assert.True(t, flushStarts.Contains(xtime.ToUnixNano(blockStart1)))

	// Simulate blockStart3 didn't get fully flushed, so it should be flushed
	// again.
	blockStates[blockStartNano3] = BlockState{
		WarmRetrievable: true,
		ColdVersion:     0,
	}
	flushStarts = buffer.ColdFlushBlockStarts(blockStates)
	assert.Equal(t, 2, flushStarts.Len())
	assert.True(t, flushStarts.Contains(xtime.ToUnixNano(blockStart1)))
	assert.True(t, flushStarts.Contains(xtime.ToUnixNano(blockStart3)))
}

func TestFetchBlocksForColdFlush(t *testing.T) {
	opts := newBufferTestOptions()
	rops := opts.RetentionOptions()
	blockSize := rops.BlockSize()
	blockStart4 := time.Now().Truncate(blockSize)
	blockStart3 := blockStart4.Add(-2 * blockSize)
	blockStartNano3 := xtime.ToUnixNano(blockStart3)
	blockStart2 := blockStart4.Add(-3 * blockSize)
	blockStart1 := blockStart4.Add(-4 * blockSize)
	blockStartNano1 := xtime.ToUnixNano(blockStart1)

	bds := []blockData{
		blockData{
			start:     blockStart1,
			writeType: ColdWrite,
			data: [][]value{
				{
					{blockStart1, 1, xtime.Second, nil},
					{blockStart1.Add(secs(5)), 2, xtime.Second, nil},
					{blockStart1.Add(secs(10)), 3, xtime.Second, nil},
				},
			},
		},
		blockData{
			start:     blockStart2,
			writeType: ColdWrite,
			data: [][]value{
				{
					{blockStart2.Add(secs(2)), 4, xtime.Second, nil},
					{blockStart2.Add(secs(5)), 5, xtime.Second, nil},
					{blockStart2.Add(secs(11)), 6, xtime.Second, nil},
					{blockStart2.Add(secs(15)), 7, xtime.Second, nil},
					{blockStart2.Add(secs(40)), 8, xtime.Second, nil},
				},
			},
		},
		blockData{
			start:     blockStart3,
			writeType: ColdWrite,
			data: [][]value{
				{
					{blockStart3.Add(secs(71)), 9, xtime.Second, nil},
				},
			},
		},
		blockData{
			start:     blockStart4,
			writeType: WarmWrite,
			data: [][]value{
				{
					{blockStart4.Add(secs(57)), 10, xtime.Second, nil},
					{blockStart4.Add(secs(66)), 11, xtime.Second, nil},
					{blockStart4.Add(secs(80)), 12, xtime.Second, nil},
					{blockStart4.Add(secs(81)), 13, xtime.Second, nil},
					{blockStart4.Add(secs(82)), 14, xtime.Second, nil},
					{blockStart4.Add(secs(96)), 15, xtime.Second, nil},
				},
			},
		},
	}

	buffer, expected := newTestBufferWithCustomData(t, bds, opts, nil)
	ctx := context.NewContext()
	defer ctx.Close()
	nsCtx := namespace.Context{Schema: testSchemaDesc}
	reader, err := buffer.FetchBlocksForColdFlush(ctx, blockStart1, 4, nsCtx)
	assert.NoError(t, err)
	// Verify that we got the correct data and that version is correct set.
	requireReaderValuesEqual(t, expected[blockStartNano1], [][]xio.BlockReader{reader}, opts, nsCtx)
	assert.Equal(t, 4, buffer.bucketsMap[blockStartNano1].buckets[0].version)

	// Try to fetch from block1 again, which should result in error since we
	// just fetched, which would mark those buckets as not dirty.
	_, err = buffer.FetchBlocksForColdFlush(ctx, blockStart1, 9, nsCtx)
	assert.Error(t, err)

	reader, err = buffer.FetchBlocksForColdFlush(ctx, blockStart3, 1, nsCtx)
	assert.NoError(t, err)
	requireReaderValuesEqual(t, expected[blockStartNano3], [][]xio.BlockReader{reader}, opts, nsCtx)
	assert.Equal(t, 1, buffer.bucketsMap[blockStartNano3].buckets[0].version)

	// Try to fetch from a block that only has warm buckets. It has no data
	// but is not an error.
	reader, err = buffer.FetchBlocksForColdFlush(ctx, blockStart4, 1, nsCtx)
	assert.NoError(t, err)
	requireReaderValuesEqual(t, []value{}, [][]xio.BlockReader{reader}, opts, nsCtx)
}

// TestBufferLoadWarmWrite tests the Load method, ensuring that blocks are successfully loaded into
// the buffer and treated as warm writes.
func TestBufferLoadWarmWrite(t *testing.T) {
	var (
		opts      = newBufferTestOptions()
		buffer    = newDatabaseBuffer()
		blockSize = opts.RetentionOptions().BlockSize()
		curr      = time.Now().Truncate(blockSize)
		nsCtx     = namespace.Context{}
	)
	buffer.Reset(nil, opts)
	encoded, err := buffer.ReadEncoded(context.NewContext(), curr, curr.Add(blockSize), nsCtx)
	require.NoError(t, err)
	require.Equal(t, 0, len(encoded))

	data := checked.NewBytes([]byte("some-data"), nil)
	data.IncRef()
	segment := ts.Segment{Head: data}
	block := block.NewDatabaseBlock(curr, blockSize, segment, opts.DatabaseBlockOptions(), nsCtx)
	buffer.Load(block, WarmWrite)

	// Ensure the bootstrapped block is loaded and readable.
	encoded, err = buffer.ReadEncoded(context.NewContext(), curr, curr.Add(blockSize), nsCtx)
	require.NoError(t, err)
	require.Equal(t, 1, len(encoded))

	// Ensure bootstrapped blocks are loaded as warm writes.
	coldFlushBlockStarts := buffer.ColdFlushBlockStarts(nil)
	require.Equal(t, 0, coldFlushBlockStarts.Len())
}

// TestBufferLoadColdWrite tests the Load method, ensuring that blocks are successfully loaded into
// the buffer and treated as cold writes.
func TestBufferLoadColdWrite(t *testing.T) {
	var (
		opts      = newBufferTestOptions()
		buffer    = newDatabaseBuffer()
		blockSize = opts.RetentionOptions().BlockSize()
		curr      = time.Now().Truncate(blockSize)
		nsCtx     = namespace.Context{}
	)
	buffer.Reset(nil, opts)
	encoded, err := buffer.ReadEncoded(context.NewContext(), curr, curr.Add(blockSize), nsCtx)
	require.NoError(t, err)
	require.Equal(t, 0, len(encoded))

	data := checked.NewBytes([]byte("some-data"), nil)
	data.IncRef()
	segment := ts.Segment{Head: data}
	block := block.NewDatabaseBlock(curr, blockSize, segment, opts.DatabaseBlockOptions(), nsCtx)
	buffer.Load(block, ColdWrite)

	// Ensure the bootstrapped block is loaded and readable.
	encoded, err = buffer.ReadEncoded(context.NewContext(), curr, curr.Add(blockSize), nsCtx)
	require.NoError(t, err)
	require.Equal(t, 1, len(encoded))

	// Ensure bootstrapped blocks are loaded as cold writes.
	coldFlushBlockStarts := buffer.ColdFlushBlockStarts(nil)
	require.Equal(t, 1, coldFlushBlockStarts.Len())
}
