package storage

import (
	"io"
	"testing"
	"time"

	"code.uber.internal/infra/memtsdb"
	"code.uber.internal/infra/memtsdb/context"
	"code.uber.internal/infra/memtsdb/mocks"
	xerrors "code.uber.internal/infra/memtsdb/x/errors"
	xtime "code.uber.internal/infra/memtsdb/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func seriesTestOptions() memtsdb.DatabaseOptions {
	return NewDatabaseOptions().
		BlockSize(2 * time.Minute).
		BufferFuture(10 * time.Second).
		BufferPast(10 * time.Second).
		BufferDrain(30 * time.Second)
}

func TestSeriesEmpty(t *testing.T) {
	opts := seriesTestOptions()
	series := newDatabaseSeries("foo", bootstrapped, opts).(*dbSeries)
	assert.True(t, series.Empty())
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
		assert.NoError(t, series.Write(ctx, v.timestamp, v.value, xtime.Second, v.annotation))
		ctx.Close()
	}

	assert.Equal(t, true, series.buffer.NeedsDrain())

	// Tick the series which should cause a drain
	assert.NoError(t, series.Tick())

	assert.Equal(t, false, series.buffer.NeedsDrain())

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
		assert.NoError(t, series.Write(ctx, v.timestamp, v.value, xtime.Second, v.annotation))
		ctx.Close()
	}

	ctx := context.NewContext()
	defer ctx.Close()

	// Test fine grained range
	results, err := series.ReadEncoded(ctx, start, start.Add(mins(10)))
	assert.NoError(t, err)

	assertValuesEqual(t, data, results.Readers(), opts)

	// Test wide range
	results, err = series.ReadEncoded(ctx, timeZero, timeDistantFuture)
	assert.NoError(t, err)

	assertValuesEqual(t, data, results.Readers(), opts)
}

func TestSeriesReadEndBeforeStart(t *testing.T) {
	opts := seriesTestOptions()
	series := newDatabaseSeries("foo", bootstrapped, opts).(*dbSeries)

	ctx := context.NewContext()
	defer ctx.Close()

	results, err := series.ReadEncoded(ctx, time.Now(), time.Now().Add(-1*time.Second))
	assert.Error(t, err)
	assert.True(t, xerrors.IsInvalidParams(err))
	assert.Nil(t, results)
}

func TestSeriesFlushToDiskNoBlock(t *testing.T) {
	opts := seriesTestOptions()
	series := newDatabaseSeries("foo", bootstrapped, opts).(*dbSeries)
	flushTime := time.Unix(7200, 0)
	err := series.FlushToDisk(nil, flushTime, nil)
	require.Nil(t, err)
}

func TestSeriesFlushToDisk(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := seriesTestOptions()
	series := newDatabaseSeries("foo", bootstrapped, opts).(*dbSeries)
	flushTime := time.Unix(7200, 0)
	head := []byte{0x1, 0x2}
	tail := []byte{0x3, 0x4}
	segmentHolder := [][]byte{head, tail}

	var res memtsdb.Segment
	encoder := mocks.NewMockEncoder(ctrl)
	reader := mocks.NewMockSegmentReader(ctrl)
	encoder.EXPECT().Stream().Return(reader)
	reader.EXPECT().Segment().Return(memtsdb.Segment{Head: head, Tail: tail})
	writer := mocks.NewMockWriter(ctrl)
	writer.EXPECT().WriteAll("foo", segmentHolder).Do(func(_ string, holder [][]byte) {
		res.Head = holder[0]
		res.Tail = holder[1]
	}).Return(nil)

	block := NewDatabaseBlock(flushTime, encoder, opts)
	series.blocks.AddBlock(block)
	err := series.FlushToDisk(writer, flushTime, segmentHolder)
	require.Nil(t, err)
}
