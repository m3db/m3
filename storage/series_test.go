package storage

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"code.uber.internal/infra/memtsdb"
	"code.uber.internal/infra/memtsdb/context"
	xerrors "code.uber.internal/infra/memtsdb/x/errors"
	xtime "code.uber.internal/infra/memtsdb/x/time"
)

func seriesTestOptions() memtsdb.DatabaseOptions {
	return NewDatabaseOptions().
		BlockSize(2 * time.Minute).
		BufferFuture(10 * time.Second).
		BufferPast(10 * time.Second).
		BufferFlush(30 * time.Second)
}

func TestSeriesEmpty(t *testing.T) {
	opts := seriesTestOptions()
	series := newDatabaseSeries("foo", bootstrapped, opts).(*dbSeries)
	assert.True(t, series.empty())
}

func TestSeriesWriteFlush(t *testing.T) {
	opts := seriesTestOptions()
	curr := time.Now().Truncate(opts.GetBlockSize())
	start := curr
	opts = opts.NowFn(func() time.Time {
		return curr
	})
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
		assert.NoError(t, series.write(ctx, v.timestamp, v.value, xtime.Second, v.annotation))
		ctx.Close()
	}

	assert.Equal(t, true, series.buffer.needsFlush())

	// Tick the series which should cause a flush
	assert.NoError(t, series.tick())

	assert.Equal(t, false, series.buffer.needsFlush())

	blocks := series.blocks.GetAllBlocks()
	assert.Len(t, blocks, 1)

	block, ok := series.blocks.GetBlockAt(start)
	assert.Equal(t, true, ok)

	assertValuesEqual(t, data[:2], []io.Reader{block.Stream()}, opts)
}

func TestSeriesWriteFlushRead(t *testing.T) {
	opts := seriesTestOptions()
	curr := time.Now().Truncate(opts.GetBlockSize())
	start := curr
	opts = opts.NowFn(func() time.Time {
		return curr
	})
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
		assert.NoError(t, series.write(ctx, v.timestamp, v.value, xtime.Second, v.annotation))
		ctx.Close()
	}

	ctx := context.NewContext()
	defer ctx.Close()

	// Test fine grained range
	results, err := series.readEncoded(ctx, start, start.Add(mins(10)))
	assert.NoError(t, err)

	assertValuesEqual(t, data, results.Readers(), opts)

	// Test wide range
	results, err = series.readEncoded(ctx, timeZero, timeDistantFuture)
	assert.NoError(t, err)

	assertValuesEqual(t, data, results.Readers(), opts)
}

func TestSeriesReadEndBeforeStart(t *testing.T) {
	opts := seriesTestOptions()
	series := newDatabaseSeries("foo", bootstrapped, opts).(*dbSeries)

	ctx := context.NewContext()
	defer ctx.Close()

	results, err := series.readEncoded(ctx, time.Now(), time.Now().Add(-1*time.Second))
	assert.Error(t, err)
	assert.True(t, xerrors.IsInvalidParams(err))
	assert.Nil(t, results)
}
