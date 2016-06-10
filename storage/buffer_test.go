package storage

import (
	"io"
	"sort"
	"testing"
	"time"

	"code.uber.internal/infra/memtsdb"
	"code.uber.internal/infra/memtsdb/context"
	xerrors "code.uber.internal/infra/memtsdb/x/errors"
	xtime "code.uber.internal/infra/memtsdb/x/time"

	"github.com/stretchr/testify/assert"
)

func bufferTestOptions() memtsdb.DatabaseOptions {
	return NewDatabaseOptions().
		BlockSize(2 * time.Minute).
		BufferFuture(10 * time.Second).
		BufferPast(10 * time.Second).
		BufferFlush(30 * time.Second)
}

func TestBufferWriteTooFuture(t *testing.T) {
	opts := bufferTestOptions()
	curr := time.Now().Truncate(opts.GetBlockSize())
	opts = opts.NowFn(func() time.Time {
		return curr
	})
	buffer := newDatabaseBuffer(nil, opts).(*dbBuffer)

	ctx := context.NewContext()
	defer ctx.Close()

	err := buffer.write(ctx, curr.Add(opts.GetBufferFuture()), 1, xtime.Second, nil)
	assert.Error(t, err)
	assert.True(t, xerrors.IsInvalidParams(err))
}

func TestBufferWriteTooPast(t *testing.T) {
	opts := bufferTestOptions()
	curr := time.Now().Truncate(opts.GetBlockSize())
	opts = opts.NowFn(func() time.Time {
		return curr
	})
	buffer := newDatabaseBuffer(nil, opts).(*dbBuffer)

	ctx := context.NewContext()
	defer ctx.Close()

	err := buffer.write(ctx, curr.Add(-1*opts.GetBufferPast()), 1, xtime.Second, nil)
	assert.Error(t, err)
	assert.True(t, xerrors.IsInvalidParams(err))
}

func TestBufferWriteRead(t *testing.T) {
	opts := bufferTestOptions()
	curr := time.Now().Truncate(opts.GetBlockSize())
	opts = opts.NowFn(func() time.Time {
		return curr
	})
	buffer := newDatabaseBuffer(nil, opts).(*dbBuffer)

	data := []value{
		{curr.Add(secs(1)), 1, xtime.Second, nil},
		{curr.Add(secs(2)), 2, xtime.Second, nil},
		{curr.Add(secs(3)), 3, xtime.Second, nil},
	}

	for _, v := range data {
		ctx := context.NewContext()
		assert.NoError(t, buffer.write(ctx, v.timestamp, v.value, v.unit, v.annotation))
		ctx.Close()
	}

	ctx := context.NewContext()
	defer ctx.Close()

	results := buffer.readEncoded(ctx, timeZero, timeDistantFuture)
	assert.NotNil(t, results)

	assertValuesEqual(t, data, results, opts)
}

func TestBufferReadOnlyMatchingBuckets(t *testing.T) {
	opts := bufferTestOptions()
	curr := time.Now().Truncate(opts.GetBlockSize())
	start := curr
	opts = opts.NowFn(func() time.Time {
		return curr
	})
	buffer := newDatabaseBuffer(nil, opts).(*dbBuffer)

	data := []value{
		{curr.Add(mins(1)), 1, xtime.Second, nil},
		{curr.Add(mins(3)), 2, xtime.Second, nil},
	}

	for _, v := range data {
		curr = v.timestamp
		ctx := context.NewContext()
		assert.NoError(t, buffer.write(ctx, v.timestamp, v.value, v.unit, v.annotation))
		ctx.Close()
	}

	ctx := context.NewContext()
	defer ctx.Close()

	firstBucketStart := start.Truncate(time.Second)
	firstBucketEnd := start.Add(mins(2)).Truncate(time.Second)
	results := buffer.readEncoded(ctx, firstBucketStart, firstBucketEnd)
	assert.NotNil(t, results)

	assertValuesEqual(t, []value{data[0]}, results, opts)

	secondBucketStart := start.Add(mins(2)).Truncate(time.Second)
	secondBucketEnd := start.Add(mins(4)).Truncate(time.Second)
	results = buffer.readEncoded(ctx, secondBucketStart, secondBucketEnd)
	assert.NotNil(t, results)

	assertValuesEqual(t, []value{data[1]}, results, opts)
}

func TestBufferFlush(t *testing.T) {
	var flushed []flush
	flushFn := func(start time.Time, encoder memtsdb.Encoder) {
		flushed = append(flushed, flush{start, encoder})
	}

	opts := bufferTestOptions()
	curr := time.Now().Truncate(opts.GetBlockSize())
	opts = opts.NowFn(func() time.Time {
		return curr
	})
	buffer := newDatabaseBuffer(flushFn, opts).(*dbBuffer)

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
		assert.NoError(t, buffer.write(ctx, v.timestamp, v.value, v.unit, v.annotation))
		ctx.Close()
	}

	assert.Equal(t, true, buffer.needsFlush())
	assert.Len(t, flushed, 0)

	buffer.flushAndReset()

	assert.Equal(t, false, buffer.needsFlush())
	assert.Len(t, flushed, 1)

	ctx := context.NewContext()
	defer ctx.Close()

	results := buffer.readEncoded(ctx, timeZero, timeDistantFuture)
	assert.NotNil(t, results)

	assertValuesEqual(t, data[:4], []io.Reader{flushed[0].encoder.Stream()}, opts)
	assertValuesEqual(t, data[4:], results, opts)
}

func TestBufferResetUnflushedBucketFlushesBucket(t *testing.T) {
	var flushed []flush
	flushFn := func(start time.Time, encoder memtsdb.Encoder) {
		flushed = append(flushed, flush{start, encoder})
	}

	opts := bufferTestOptions()
	curr := time.Now().Truncate(opts.GetBlockSize())
	opts = opts.NowFn(func() time.Time {
		return curr
	})
	buffer := newDatabaseBuffer(flushFn, opts).(*dbBuffer)

	data := []value{
		{curr.Add(mins(1)), 1, xtime.Second, nil},
		{curr.Add(mins(3)), 2, xtime.Second, nil},
		{curr.Add(mins(5)), 2, xtime.Second, nil},
		{curr.Add(mins(7)), 2, xtime.Second, nil},
	}

	for _, v := range data {
		curr = v.timestamp
		ctx := context.NewContext()
		assert.NoError(t, buffer.write(ctx, v.timestamp, v.value, v.unit, v.annotation))
		ctx.Close()
	}

	assert.Equal(t, true, buffer.needsFlush())
	assert.Len(t, flushed, 2)

	ctx := context.NewContext()
	defer ctx.Close()

	results := buffer.readEncoded(ctx, timeZero, timeDistantFuture)
	assert.NotNil(t, results)

	assertValuesEqual(t, data[:2], []io.Reader{
		flushed[0].encoder.Stream(),
		flushed[1].encoder.Stream(),
	}, opts)
	assertValuesEqual(t, data[2:], results, opts)
}

func TestBufferWriteOutOfOrder(t *testing.T) {
	opts := bufferTestOptions()
	curr := time.Now().Truncate(opts.GetBlockSize())
	opts = opts.NowFn(func() time.Time {
		return curr
	})
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
		assert.NoError(t, buffer.write(ctx, v.timestamp, v.value, v.unit, v.annotation))
		ctx.Close()
	}

	// Restore data to in order for comparison
	sort.Sort(valuesByTime(data))

	ctx := context.NewContext()
	defer ctx.Close()

	results := buffer.readEncoded(ctx, timeZero, timeDistantFuture)
	assert.NotNil(t, results)

	assertValuesEqual(t, data, results, opts)

	// Explicitly sort
	for i := range buffer.buckets {
		buffer.buckets[i].sort()
	}

	// Ensure compacted encoders
	for i := range buffer.buckets {
		assert.Len(t, buffer.buckets[i].encoders, 1)
	}

	// Assert equal again
	results = buffer.readEncoded(ctx, timeZero, timeDistantFuture)
	assert.NotNil(t, results)

	assertValuesEqual(t, data, results, opts)
}
