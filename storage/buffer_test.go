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
	"io"
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3db"
	"github.com/m3db/m3db/context"
	xerrors "github.com/m3db/m3db/x/errors"
	xtime "github.com/m3db/m3db/x/time"

	"github.com/stretchr/testify/assert"
)

func bufferTestOptions() memtsdb.DatabaseOptions {
	return NewDatabaseOptions().
		BlockSize(2 * time.Minute).
		BufferFuture(10 * time.Second).
		BufferPast(10 * time.Second).
		BufferDrain(30 * time.Second)
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

	err := buffer.Write(ctx, curr.Add(opts.GetBufferFuture()), 1, xtime.Second, nil)
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

	err := buffer.Write(ctx, curr.Add(-1*opts.GetBufferPast()), 1, xtime.Second, nil)
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
	drainFn := func(start time.Time, encoder memtsdb.Encoder) {
		drained = append(drained, drain{start, encoder})
	}

	opts := bufferTestOptions()
	curr := time.Now().Truncate(opts.GetBlockSize())
	opts = opts.NowFn(func() time.Time {
		return curr
	})
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
	assert.Len(t, drained, 0)

	buffer.DrainAndReset()

	assert.Equal(t, false, buffer.NeedsDrain())
	assert.Len(t, drained, 1)

	ctx := context.NewContext()
	defer ctx.Close()

	results := buffer.ReadEncoded(ctx, timeZero, timeDistantFuture)
	assert.NotNil(t, results)

	assertValuesEqual(t, data[:4], []io.Reader{drained[0].encoder.Stream()}, opts)
	assertValuesEqual(t, data[4:], results, opts)
}

func TestBufferResetUndrainedBucketDrainsBucket(t *testing.T) {
	var drained []drain
	drainFn := func(start time.Time, encoder memtsdb.Encoder) {
		drained = append(drained, drain{start, encoder})
	}

	opts := bufferTestOptions()
	curr := time.Now().Truncate(opts.GetBlockSize())
	opts = opts.NowFn(func() time.Time {
		return curr
	})
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

	assertValuesEqual(t, data[:2], []io.Reader{
		drained[0].encoder.Stream(),
		drained[1].encoder.Stream(),
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
		assert.NoError(t, buffer.Write(ctx, v.timestamp, v.value, v.unit, v.annotation))
		ctx.Close()
	}

	// Restore data to in order for comparison
	sort.Sort(valuesByTime(data))

	ctx := context.NewContext()
	defer ctx.Close()

	results := buffer.ReadEncoded(ctx, timeZero, timeDistantFuture)
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
	results = buffer.ReadEncoded(ctx, timeZero, timeDistantFuture)
	assert.NotNil(t, results)

	assertValuesEqual(t, data, results, opts)
}
