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
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/retention"
	m3dbruntime "github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/context"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newSeriesTestOptions() Options {
	encoderPool := encoding.NewEncoderPool(nil)
	multiReaderIteratorPool := encoding.NewMultiReaderIteratorPool(nil)

	encodingOpts := encoding.NewOptions().SetEncoderPool(encoderPool)

	encoderPool.Init(func() encoding.Encoder {
		return m3tsz.NewEncoder(0, nil, m3tsz.DefaultIntOptimizationEnabled, encodingOpts)
	})
	multiReaderIteratorPool.Init(m3tsz.DefaultReaderIteratorAllocFn(encodingOpts))

	bufferBucketPool := NewBufferBucketPool(nil)
	bufferBucketVersionsPool := NewBufferBucketVersionsPool(nil)

	opts := NewOptions().
		SetEncoderPool(encoderPool).
		SetMultiReaderIteratorPool(multiReaderIteratorPool).
		SetBufferBucketPool(bufferBucketPool).
		SetBufferBucketVersionsPool(bufferBucketVersionsPool).
		SetRuntimeOptionsManager(m3dbruntime.NewOptionsManager())
	opts = opts.
		SetRetentionOptions(opts.
			RetentionOptions().
			SetBlockSize(2 * time.Minute).
			SetBufferFuture(90 * time.Second).
			SetBufferPast(90 * time.Second).
			SetRetentionPeriod(time.Hour)).
		SetDatabaseBlockOptions(opts.
			DatabaseBlockOptions().
			SetContextPool(opts.ContextPool()).
			SetEncoderPool(opts.EncoderPool()))
	return opts
}

func TestSeriesEmpty(t *testing.T) {
	opts := newSeriesTestOptions()
	series := NewDatabaseSeries(DatabaseSeriesOptions{
		ID:      ident.StringID("foo"),
		Options: opts,
	})
	assert.True(t, series.IsEmpty())
}

// Writes to series, verifying no error and that further writes should happen.
func verifyWriteToSeries(t *testing.T, series *dbSeries, v DecodedTestValue) {
	ctx := context.NewBackground()
	wasWritten, _, err := series.Write(ctx, v.Timestamp, v.Value,
		v.Unit, v.Annotation, WriteOptions{})
	require.NoError(t, err)
	require.True(t, wasWritten)
	ctx.Close()
}

func TestSeriesWriteFlush(t *testing.T) {
	opts := newSeriesTestOptions()
	curr := xtime.Now().Truncate(opts.RetentionOptions().BlockSize())
	start := curr
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr.ToTime()
	}))

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bl := block.NewMockDatabaseBlock(ctrl)
	bl.EXPECT().StartTime().Return(curr).Times(2)
	bl.EXPECT().Stream(gomock.Any()).Return(xio.BlockReader{}, nil)
	bl.EXPECT().Close()

	blockRetriever := NewMockQueryableBlockRetriever(ctrl)
	blockRetriever.EXPECT().IsBlockRetrievable(gomock.Any()).Return(false, nil)

	series := NewDatabaseSeries(DatabaseSeriesOptions{
		ID:             ident.StringID("foo"),
		BlockRetriever: blockRetriever,
		Options:        opts,
	}).(*dbSeries)
	err := series.LoadBlock(bl, WarmWrite)
	assert.NoError(t, err)

	data := []DecodedTestValue{
		{curr, 1, xtime.Second, nil},
		{curr.Add(mins(1)), 2, xtime.Second, nil},
		{curr.Add(mins(2)), 3, xtime.Second, nil},
		{curr.Add(mins(3)), 4, xtime.Second, nil},
	}

	for _, v := range data {
		curr = v.Timestamp
		verifyWriteToSeries(t, series, v)
	}

	ctx := context.NewBackground()
	defer ctx.Close()

	buckets, exists := series.buffer.(*dbBuffer).bucketVersionsAt(start)
	require.True(t, exists)
	streams, err := buckets.mergeToStreams(ctx, streamsOptions{filterWriteType: false}, nil)
	require.NoError(t, err)
	require.Len(t, streams, 1)
	requireSegmentValuesEqual(t, data[:2], streams, opts, namespace.Context{})
}

func TestSeriesSamePointDoesNotWrite(t *testing.T) {
	opts := newSeriesTestOptions()
	rops := opts.RetentionOptions()
	curr := xtime.Now().Truncate(rops.BlockSize())
	start := curr
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr.ToTime()
	}))

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bl := block.NewMockDatabaseBlock(ctrl)
	bl.EXPECT().StartTime().Return(curr).Times(2)
	bl.EXPECT().Stream(gomock.Any()).Return(xio.BlockReader{}, nil)
	bl.EXPECT().Close()

	blockRetriever := NewMockQueryableBlockRetriever(ctrl)
	blockRetriever.EXPECT().IsBlockRetrievable(gomock.Any()).Return(false, nil)

	series := NewDatabaseSeries(DatabaseSeriesOptions{
		ID:             ident.StringID("foo"),
		BlockRetriever: blockRetriever,
		Options:        opts,
	}).(*dbSeries)

	err := series.LoadBlock(bl, WarmWrite)
	assert.NoError(t, err)

	data := []DecodedTestValue{
		{curr, 1, xtime.Second, nil},
		{curr, 1, xtime.Second, nil},
		{curr, 1, xtime.Second, nil},
		{curr, 1, xtime.Second, nil},
		{curr.Add(rops.BlockSize()).Add(rops.BufferPast() * 2), 1, xtime.Second, nil},
	}

	for i, v := range data {
		curr = v.Timestamp
		ctx := context.NewBackground()
		wasWritten, _, err := series.Write(ctx, v.Timestamp, v.Value, v.Unit, v.Annotation, WriteOptions{})
		require.NoError(t, err)
		if i == 0 || i == len(data)-1 {
			require.True(t, wasWritten)
		} else {
			require.False(t, wasWritten)
		}
		ctx.Close()
	}

	ctx := context.NewBackground()
	defer ctx.Close()

	buckets, exists := series.buffer.(*dbBuffer).bucketVersionsAt(start)
	require.True(t, exists)
	streams, err := buckets.mergeToStreams(ctx, streamsOptions{filterWriteType: false}, nil)
	require.NoError(t, err)
	require.Len(t, streams, 1)
	requireSegmentValuesEqual(t, data[:1], streams, opts, namespace.Context{})
}

func TestSeriesWriteFlushRead(t *testing.T) {
	opts := newSeriesTestOptions()
	curr := xtime.Now().Truncate(opts.RetentionOptions().BlockSize())
	start := curr
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr.ToTime()
	}))

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bl := block.NewMockDatabaseBlock(ctrl)
	bl.EXPECT().StartTime().Return(curr).Times(2)
	bl.EXPECT().Len().Return(0).Times(2)

	blockRetriever := NewMockQueryableBlockRetriever(ctrl)
	blockRetriever.EXPECT().
		IsBlockRetrievable(gomock.Any()).
		Return(false, nil).
		AnyTimes()

	series := NewDatabaseSeries(DatabaseSeriesOptions{
		ID:             ident.StringID("foo"),
		BlockRetriever: blockRetriever,
		Options:        opts,
	}).(*dbSeries)

	err := series.LoadBlock(bl, WarmWrite)
	assert.NoError(t, err)

	data := []DecodedTestValue{
		{curr.Add(mins(1)), 2, xtime.Second, nil},
		{curr.Add(mins(3)), 3, xtime.Second, nil},
		{curr.Add(mins(5)), 4, xtime.Second, nil},
		{curr.Add(mins(7)), 5, xtime.Second, nil},
		{curr.Add(mins(9)), 6, xtime.Second, nil},
	}

	for _, v := range data {
		curr = v.Timestamp
		verifyWriteToSeries(t, series, v)
	}

	ctx := context.NewBackground()
	defer ctx.Close()
	nsCtx := namespace.Context{}

	// Test fine grained range
	iter, err := series.ReadEncoded(ctx, start, start.Add(mins(10)), nsCtx)
	assert.NoError(t, err)
	results, err := iter.ToSlices(ctx)
	assert.NoError(t, err)

	requireReaderValuesEqual(t, data, results, opts, nsCtx)

	// Test wide range
	iter, err = series.ReadEncoded(ctx, 0, timeDistantFuture, nsCtx)
	assert.NoError(t, err)
	results, err = iter.ToSlices(ctx)
	assert.NoError(t, err)

	requireReaderValuesEqual(t, data, results, opts, nsCtx)
}

// TestSeriesLoad tests the behavior the Bootstrap()/Load()s method by ensuring that they actually load
// data into the series and that the data (merged with any existing data) can be retrieved.
//
// It also ensures that blocks for the bootstrap path blockStarts that have not been warm flushed yet
// are loaded as warm writes and block for blockStarts that have already been warm flushed are loaded as
// cold writes and that for the load path everything is loaded as cold writes.
func TestSeriesBootstrapAndLoad(t *testing.T) {
	testCases := []struct {
		title         string
		bootstrapping bool
	}{
		{
			title:         "load",
			bootstrapping: false,
		},
		{
			title:         "bootstrap",
			bootstrapping: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.title, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			var (
				opts      = newSeriesTestOptions()
				blockSize = opts.RetentionOptions().BlockSize()
				curr      = xtime.Now().Truncate(blockSize)
				start     = curr
			)
			opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
				return curr.ToTime()
			}))

			var (
				loadWrites = []DecodedTestValue{
					// Ensure each value is in a separate block so since block.DatabaseSeriesBlocks
					// can only store a single block per block start).
					{curr.Add(blockSize), 5, xtime.Second, nil},
					{curr.Add(2 * blockSize), 6, xtime.Second, nil},
				}
				nsCtx                        = namespace.Context{}
				blockOpts                    = opts.DatabaseBlockOptions()
				alreadyWarmFlushedBlockStart = curr.Add(blockSize).Truncate(blockSize)
				notYetWarmFlushedBlockStart  = curr.Add(2 * blockSize).Truncate(blockSize)
				blockStates                  = BootstrappedBlockStateSnapshot{
					Snapshot: map[xtime.UnixNano]BlockState{
						// Exercise both code paths.
						alreadyWarmFlushedBlockStart: {
							WarmRetrievable: true,
						},
						notYetWarmFlushedBlockStart: {
							WarmRetrievable: false,
						},
					},
				}
			)
			blockRetriever := NewMockQueryableBlockRetriever(ctrl)
			blockRetriever.EXPECT().
				IsBlockRetrievable(gomock.Any()).
				DoAndReturn(func(at xtime.UnixNano) (bool, error) {
					value, exists := blockStates.Snapshot[at]
					if !exists {
						// No block exists, should be a warm write.
						return false, nil
					}
					return value.WarmRetrievable, nil
				}).
				AnyTimes()
			blockRetriever.EXPECT().
				Stream(gomock.Any(), gomock.Any(), gomock.Any(),
					gomock.Any(), gomock.Any()).
				Return(xio.EmptyBlockReader, nil).
				AnyTimes()

			series := NewDatabaseSeries(DatabaseSeriesOptions{
				ID:             ident.StringID("foo"),
				BlockRetriever: blockRetriever,
				Options:        opts,
			}).(*dbSeries)

			rawWrites := []DecodedTestValue{
				{curr.Add(mins(1)), 2, xtime.Second, nil},
				{curr.Add(mins(3)), 3, xtime.Second, nil},
				{curr.Add(mins(5)), 4, xtime.Second, nil},
			}

			for _, v := range rawWrites {
				curr = v.Timestamp
				verifyWriteToSeries(t, series, v)
			}

			for _, v := range loadWrites {
				curr = v.Timestamp
				enc := opts.EncoderPool().Get()
				blockStart := v.Timestamp.Truncate(blockSize)
				enc.Reset(blockStart, 0, nil)
				dp := ts.Datapoint{TimestampNanos: v.Timestamp, Value: v.Value}
				require.NoError(t, enc.Encode(dp, v.Unit, nil))

				dbBlock := block.NewDatabaseBlock(blockStart, blockSize, enc.Discard(), blockOpts, nsCtx)

				writeType := ColdWrite
				if tc.bootstrapping {
					if blockStart.Equal(notYetWarmFlushedBlockStart) {
						writeType = WarmWrite
					}
				}

				err := series.LoadBlock(dbBlock, writeType)
				require.NoError(t, err)
			}

			t.Run("Data can be read", func(t *testing.T) {
				ctx := context.NewBackground()
				defer ctx.Close()

				iter, err := series.ReadEncoded(ctx, start, start.Add(10*blockSize), nsCtx)
				require.NoError(t, err)
				results, err := iter.ToSlices(ctx)
				require.NoError(t, err)

				var expectedData []DecodedTestValue
				expectedData = append(expectedData, rawWrites...)
				expectedData = append(expectedData, loadWrites...)
				sort.Sort(ValuesByTime(expectedData))
				requireReaderValuesEqual(t, expectedData, results, opts, nsCtx)
			})

			t.Run("blocks loaded as warm/cold writes correctly", func(t *testing.T) {
				optimizedTimes := series.ColdFlushBlockStarts(blockStates)
				coldFlushBlockStarts := []xtime.UnixNano{}
				optimizedTimes.ForEach(func(blockStart xtime.UnixNano) {
					coldFlushBlockStarts = append(coldFlushBlockStarts, blockStart)
				})
				// Cold flush block starts don't come back in any particular order so
				// sort them for easier comparisons.
				sort.Slice(coldFlushBlockStarts, func(i, j int) bool {
					return coldFlushBlockStarts[i] < coldFlushBlockStarts[j]
				})

				if tc.bootstrapping {
					// If its a bootstrap then we need to make sure that everything gets loaded as warm/cold writes
					// correctly based on the flush state.
					expectedColdFlushBlockStarts := []xtime.UnixNano{alreadyWarmFlushedBlockStart}
					assert.Equal(t, expectedColdFlushBlockStarts, coldFlushBlockStarts)
				} else {
					// If its just a regular load then everything should be loaded as cold writes for correctness
					// since flushes and loads can happen concurrently.
					expectedColdFlushBlockStarts := []xtime.UnixNano{
						alreadyWarmFlushedBlockStart,
						notYetWarmFlushedBlockStart,
					}
					assert.Equal(t, expectedColdFlushBlockStarts, coldFlushBlockStarts)
				}
			})
		})
	}
}

func TestSeriesReadEndBeforeStart(t *testing.T) {
	opts := newSeriesTestOptions()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bl := block.NewMockDatabaseBlock(ctrl)
	bl.EXPECT().StartTime().Return(xtime.Now()).Times(2)

	blockRetriever := NewMockQueryableBlockRetriever(ctrl)
	blockRetriever.EXPECT().
		IsBlockRetrievable(gomock.Any()).
		Return(false, nil).
		AnyTimes()

	series := NewDatabaseSeries(DatabaseSeriesOptions{
		ID:             ident.StringID("foo"),
		BlockRetriever: blockRetriever,
		Options:        opts,
	})

	err := series.LoadBlock(bl, WarmWrite)
	assert.NoError(t, err)

	ctx := context.NewBackground()
	defer ctx.Close()
	nsCtx := namespace.Context{}

	now := xtime.Now()
	iter, err := series.ReadEncoded(ctx, now, now.Add(-1*time.Second), nsCtx)
	assert.Error(t, err)
	assert.True(t, xerrors.IsInvalidParams(err))
	assert.Nil(t, iter)
}

func TestSeriesFlushNoBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bl := block.NewMockDatabaseBlock(ctrl)
	bl.EXPECT().StartTime().Return(xtime.Now()).Times(2)

	opts := newSeriesTestOptions()

	blockRetriever := NewMockQueryableBlockRetriever(ctrl)
	blockRetriever.EXPECT().
		IsBlockRetrievable(gomock.Any()).
		Return(false, nil).
		AnyTimes()

	series := NewDatabaseSeries(DatabaseSeriesOptions{
		ID:             ident.StringID("foo"),
		BlockRetriever: blockRetriever,
		Options:        opts,
	}).(*dbSeries)

	err := series.LoadBlock(bl, WarmWrite)
	require.NoError(t, err)

	flushTime := xtime.FromSeconds(7200)
	outcome, err := series.WarmFlush(nil, flushTime, nil, namespace.Context{})
	require.Nil(t, err)
	require.Equal(t, FlushOutcomeBlockDoesNotExist, outcome)
}

func TestSeriesFlush(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bl := block.NewMockDatabaseBlock(ctrl)
	bl.EXPECT().StartTime().Return(xtime.Now()).Times(2)

	curr := xtime.FromSeconds(7200)
	opts := newSeriesTestOptions()
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr.ToTime()
	}))

	blockRetriever := NewMockQueryableBlockRetriever(ctrl)
	blockRetriever.EXPECT().
		IsBlockRetrievable(gomock.Any()).
		Return(false, nil).
		AnyTimes()

	series := NewDatabaseSeries(DatabaseSeriesOptions{
		BlockRetriever: blockRetriever,
		Options:        opts,
	}).(*dbSeries)

	err := series.LoadBlock(bl, WarmWrite)
	assert.NoError(t, err)

	ctx := context.NewBackground()
	series.buffer.Write(ctx, testID, curr, 1234, xtime.Second, nil, WriteOptions{})
	ctx.BlockingClose()

	inputs := []error{errors.New("some error"), nil}
	for _, input := range inputs {
		persistFn := func(_ persist.Metadata, _ ts.Segment, _ uint32) error {
			return input
		}
		ctx := context.NewBackground()
		outcome, err := series.WarmFlush(ctx, curr, persistFn, namespace.Context{})
		ctx.BlockingClose()
		require.Equal(t, input, err)
		if input == nil {
			require.Equal(t, FlushOutcomeFlushedToDisk, outcome)
		} else {
			require.Equal(t, FlushOutcomeErr, outcome)
		}
	}
}

func TestSeriesTickEmptySeries(t *testing.T) {
	opts := newSeriesTestOptions()
	series := NewDatabaseSeries(DatabaseSeriesOptions{
		ID:      ident.StringID("foo"),
		Options: opts,
	}).(*dbSeries)
	_, err := series.Tick(NewShardBlockStateSnapshot(true, BootstrappedBlockStateSnapshot{}), namespace.Context{})
	require.Equal(t, ErrSeriesAllDatapointsExpired, err)
}

func TestSeriesTickDrainAndResetBuffer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bl := block.NewMockDatabaseBlock(ctrl)
	bl.EXPECT().StartTime().Return(xtime.Now()).Times(2)

	opts := newSeriesTestOptions()

	blockRetriever := NewMockQueryableBlockRetriever(ctrl)
	blockRetriever.EXPECT().
		IsBlockRetrievable(gomock.Any()).
		Return(false, nil).
		AnyTimes()

	series := NewDatabaseSeries(DatabaseSeriesOptions{
		ID:             ident.StringID("foo"),
		BlockRetriever: blockRetriever,
		Options:        opts,
	}).(*dbSeries)

	err := series.LoadBlock(bl, WarmWrite)
	require.NoError(t, err)

	buffer := NewMockdatabaseBuffer(ctrl)
	series.buffer = buffer
	buffer.EXPECT().Tick(gomock.Any(), gomock.Any()).Return(bufferTickResult{})
	buffer.EXPECT().Stats().Return(bufferStats{wiredBlocks: 1})
	r, err := series.Tick(NewShardBlockStateSnapshot(true, BootstrappedBlockStateSnapshot{}), namespace.Context{})
	require.NoError(t, err)
	assert.Equal(t, 1, r.ActiveBlocks)
	assert.Equal(t, 1, r.WiredBlocks)
	assert.Equal(t, 0, r.UnwiredBlocks)
}

func TestSeriesTickNeedsBlockExpiry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bl := block.NewMockDatabaseBlock(ctrl)
	bl.EXPECT().StartTime().Return(xtime.Now()).Times(2)

	opts := newSeriesTestOptions()
	opts = opts.SetCachePolicy(CacheRecentlyRead)
	ropts := opts.RetentionOptions()
	curr := xtime.Now().Truncate(ropts.BlockSize())
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr.ToTime()
	}))

	blockRetriever := NewMockQueryableBlockRetriever(ctrl)
	blockRetriever.EXPECT().
		IsBlockRetrievable(gomock.Any()).
		Return(false, nil).
		AnyTimes()

	series := NewDatabaseSeries(DatabaseSeriesOptions{
		ID:             ident.StringID("foo"),
		BlockRetriever: blockRetriever,
		Options:        opts,
	}).(*dbSeries)

	err := series.LoadBlock(bl, WarmWrite)
	require.NoError(t, err)

	blockStart := curr.Add(-ropts.RetentionPeriod()).Add(-ropts.BlockSize())
	b := block.NewMockDatabaseBlock(ctrl)
	b.EXPECT().StartTime().Return(blockStart)
	b.EXPECT().Close()
	series.cachedBlocks.AddBlock(b)
	b = block.NewMockDatabaseBlock(ctrl)
	b.EXPECT().StartTime().Return(curr)
	b.EXPECT().HasMergeTarget().Return(false)
	series.cachedBlocks.AddBlock(b)
	require.Equal(t, blockStart, series.cachedBlocks.MinTime())
	require.Equal(t, 2, series.cachedBlocks.Len())
	buffer := NewMockdatabaseBuffer(ctrl)
	series.buffer = buffer
	buffer.EXPECT().Tick(gomock.Any(), gomock.Any()).Return(bufferTickResult{})
	buffer.EXPECT().Stats().Return(bufferStats{wiredBlocks: 1})
	blockStates := BootstrappedBlockStateSnapshot{
		Snapshot: map[xtime.UnixNano]BlockState{
			blockStart: {
				WarmRetrievable: false,
				ColdVersion:     0,
			},
			curr: {
				WarmRetrievable: false,
				ColdVersion:     0,
			},
		},
	}
	r, err := series.Tick(NewShardBlockStateSnapshot(true, blockStates), namespace.Context{})
	require.NoError(t, err)
	require.Equal(t, 2, r.ActiveBlocks)
	require.Equal(t, 2, r.WiredBlocks)
	require.Equal(t, 1, r.MadeExpiredBlocks)
	require.Equal(t, 1, series.cachedBlocks.Len())
	require.Equal(t, curr, series.cachedBlocks.MinTime())
	_, exists := series.cachedBlocks.AllBlocks()[curr]
	require.True(t, exists)
}

func TestSeriesTickRecentlyRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bl := block.NewMockDatabaseBlock(ctrl)
	bl.EXPECT().StartTime().Return(xtime.Now()).Times(2)

	opts := newSeriesTestOptions()
	opts = opts.
		SetCachePolicy(CacheRecentlyRead).
		SetRetentionOptions(opts.RetentionOptions().SetBlockDataExpiryAfterNotAccessedPeriod(10 * time.Minute))
	ropts := opts.RetentionOptions()
	curr := xtime.Now().Truncate(ropts.BlockSize())
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr.ToTime()
	}))

	blockRetriever := NewMockQueryableBlockRetriever(ctrl)
	blockRetriever.EXPECT().
		IsBlockRetrievable(gomock.Any()).
		Return(false, nil).
		AnyTimes()

	series := NewDatabaseSeries(DatabaseSeriesOptions{
		ID:             ident.StringID("foo"),
		BlockRetriever: blockRetriever,
		Options:        opts,
	}).(*dbSeries)

	err := series.LoadBlock(bl, WarmWrite)
	require.NoError(t, err)

	// Test case where block has been read within expiry period - won't be removed
	b := block.NewMockDatabaseBlock(ctrl)
	b.EXPECT().StartTime().Return(curr)
	b.EXPECT().LastReadTime().Return(
		curr.Add(-opts.RetentionOptions().BlockDataExpiryAfterNotAccessedPeriod() / 2))
	b.EXPECT().HasMergeTarget().Return(true)
	series.cachedBlocks.AddBlock(b)

	blockStates := BootstrappedBlockStateSnapshot{
		Snapshot: map[xtime.UnixNano]BlockState{
			curr: {
				WarmRetrievable: true,
				ColdVersion:     1,
			},
		},
	}
	shardBlockStates := NewShardBlockStateSnapshot(true, blockStates)
	tickResult, err := series.Tick(shardBlockStates, namespace.Context{})
	require.NoError(t, err)
	require.Equal(t, 0, tickResult.UnwiredBlocks)
	require.Equal(t, 1, tickResult.PendingMergeBlocks)

	// Test case where block has not been read within expiry period - will be removed
	b = block.NewMockDatabaseBlock(ctrl)
	b.EXPECT().StartTime().Return(curr)
	b.EXPECT().LastReadTime().Return(
		curr.Add(-opts.RetentionOptions().BlockDataExpiryAfterNotAccessedPeriod() * 2))
	b.EXPECT().Close().Return()
	series.cachedBlocks.AddBlock(b)

	tickResult, err = series.Tick(shardBlockStates, namespace.Context{})
	require.NoError(t, err)
	require.Equal(t, 1, tickResult.UnwiredBlocks)
	require.Equal(t, 0, tickResult.PendingMergeBlocks)

	// Test case where block is not flushed yet (not retrievable) - Will not be removed
	b = block.NewMockDatabaseBlock(ctrl)
	b.EXPECT().StartTime().Return(curr)
	b.EXPECT().HasMergeTarget().Return(true)
	series.cachedBlocks.AddBlock(b)

	blockStates = BootstrappedBlockStateSnapshot{
		Snapshot: map[xtime.UnixNano]BlockState{
			curr: {
				WarmRetrievable: false,
				ColdVersion:     0,
			},
		},
	}
	shardBlockStates = NewShardBlockStateSnapshot(true, blockStates)
	tickResult, err = series.Tick(shardBlockStates, namespace.Context{})
	require.NoError(t, err)
	require.Equal(t, 0, tickResult.UnwiredBlocks)
	require.Equal(t, 1, tickResult.PendingMergeBlocks)
}

func TestSeriesTickCacheLRU(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bl := block.NewMockDatabaseBlock(ctrl)
	bl.EXPECT().StartTime().Return(xtime.Now()).Times(2)

	retentionPeriod := time.Hour
	opts := newSeriesTestOptions()
	opts = opts.
		SetCachePolicy(CacheLRU).
		SetRetentionOptions(opts.RetentionOptions().SetRetentionPeriod(retentionPeriod))
	ropts := opts.RetentionOptions()
	curr := xtime.Now().Truncate(ropts.BlockSize())
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr.ToTime()
	}))

	blockRetriever := NewMockQueryableBlockRetriever(ctrl)
	blockRetriever.EXPECT().
		IsBlockRetrievable(gomock.Any()).
		Return(false, nil).
		AnyTimes()

	series := NewDatabaseSeries(DatabaseSeriesOptions{
		ID:             ident.StringID("foo"),
		BlockRetriever: blockRetriever,
		Options:        opts,
	}).(*dbSeries)

	err := series.LoadBlock(bl, WarmWrite)
	require.NoError(t, err)

	// Test case where block was not retrieved from disk - Will be removed
	b := block.NewMockDatabaseBlock(ctrl)
	b.EXPECT().StartTime().Return(curr)
	b.EXPECT().WasRetrievedFromDisk().Return(false)
	b.EXPECT().Close().Return()
	series.cachedBlocks.AddBlock(b)

	blockStates := BootstrappedBlockStateSnapshot{
		Snapshot: map[xtime.UnixNano]BlockState{
			curr: {
				WarmRetrievable: true,
				ColdVersion:     1,
			},
		},
	}
	shardBlockStates := NewShardBlockStateSnapshot(true, blockStates)
	tickResult, err := series.Tick(shardBlockStates, namespace.Context{})
	require.NoError(t, err)
	require.Equal(t, 1, tickResult.UnwiredBlocks)
	require.Equal(t, 0, tickResult.PendingMergeBlocks)

	// Test case where block was retrieved from disk - Will not be removed
	b = block.NewMockDatabaseBlock(ctrl)
	b.EXPECT().StartTime().Return(curr)
	b.EXPECT().HasMergeTarget().Return(true)
	b.EXPECT().WasRetrievedFromDisk().Return(true)
	series.cachedBlocks.AddBlock(b)

	tickResult, err = series.Tick(shardBlockStates, namespace.Context{})
	require.NoError(t, err)
	require.Equal(t, 0, tickResult.UnwiredBlocks)
	require.Equal(t, 1, tickResult.PendingMergeBlocks)

	// Test case where block is not flushed yet (not retrievable) - Will not be removed
	b = block.NewMockDatabaseBlock(ctrl)
	b.EXPECT().StartTime().Return(curr)
	b.EXPECT().HasMergeTarget().Return(true)
	series.cachedBlocks.AddBlock(b)

	// Test case where block was retrieved from disk and is out of retention. Will be removed, but not closed.
	b = block.NewMockDatabaseBlock(ctrl)
	b.EXPECT().StartTime().Return(curr.Add(-2 * retentionPeriod))
	b.EXPECT().WasRetrievedFromDisk().Return(true)
	series.cachedBlocks.AddBlock(b)
	_, expiredBlockExists := series.cachedBlocks.BlockAt(curr.Add(-2 * retentionPeriod))
	require.Equal(t, true, expiredBlockExists)

	blockStates = BootstrappedBlockStateSnapshot{
		Snapshot: map[xtime.UnixNano]BlockState{
			curr: {
				WarmRetrievable: false,
				ColdVersion:     0,
			},
		},
	}
	shardBlockStates = NewShardBlockStateSnapshot(true, blockStates)
	tickResult, err = series.Tick(shardBlockStates, namespace.Context{})
	require.NoError(t, err)
	require.Equal(t, 0, tickResult.UnwiredBlocks)
	require.Equal(t, 1, tickResult.PendingMergeBlocks)
	_, expiredBlockExists = series.cachedBlocks.BlockAt(curr.Add(-2 * retentionPeriod))
	require.Equal(t, false, expiredBlockExists)
}

func TestSeriesTickCacheNone(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bl := block.NewMockDatabaseBlock(ctrl)
	bl.EXPECT().StartTime().Return(xtime.Now()).Times(2)

	opts := newSeriesTestOptions()
	opts = opts.
		SetCachePolicy(CacheNone).
		SetRetentionOptions(opts.RetentionOptions().SetBlockDataExpiryAfterNotAccessedPeriod(10 * time.Minute))
	ropts := opts.RetentionOptions()
	curr := xtime.Now().Truncate(ropts.BlockSize())
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr.ToTime()
	}))

	blockRetriever := NewMockQueryableBlockRetriever(ctrl)
	blockRetriever.EXPECT().
		IsBlockRetrievable(gomock.Any()).
		Return(false, nil).
		AnyTimes()

	series := NewDatabaseSeries(DatabaseSeriesOptions{
		ID:             ident.StringID("foo"),
		BlockRetriever: blockRetriever,
		Options:        opts,
	}).(*dbSeries)

	err := series.LoadBlock(bl, WarmWrite)
	require.NoError(t, err)

	// Retrievable blocks should be removed
	b := block.NewMockDatabaseBlock(ctrl)
	b.EXPECT().StartTime().Return(curr)
	b.EXPECT().Close().Return()
	series.cachedBlocks.AddBlock(b)

	blockStates := BootstrappedBlockStateSnapshot{
		Snapshot: map[xtime.UnixNano]BlockState{
			curr: {
				WarmRetrievable: true,
				ColdVersion:     1,
			},
		},
	}
	shardBlockStates := NewShardBlockStateSnapshot(true, blockStates)
	tickResult, err := series.Tick(shardBlockStates, namespace.Context{})
	require.NoError(t, err)
	require.Equal(t, 1, tickResult.UnwiredBlocks)
	require.Equal(t, 0, tickResult.PendingMergeBlocks)

	// Non-retrievable blocks should not be removed
	b = block.NewMockDatabaseBlock(ctrl)
	b.EXPECT().StartTime().Return(curr)
	b.EXPECT().HasMergeTarget().Return(true)
	series.cachedBlocks.AddBlock(b)

	blockStates = BootstrappedBlockStateSnapshot{
		Snapshot: map[xtime.UnixNano]BlockState{
			curr: {
				WarmRetrievable: false,
				ColdVersion:     0,
			},
		},
	}
	shardBlockStates = NewShardBlockStateSnapshot(true, blockStates)
	tickResult, err = series.Tick(shardBlockStates, namespace.Context{})
	require.NoError(t, err)
	require.Equal(t, 0, tickResult.UnwiredBlocks)
	require.Equal(t, 1, tickResult.PendingMergeBlocks)
}

func TestSeriesTickCachedBlockRemove(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSeriesTestOptions()
	opts = opts.SetCachePolicy(CacheAll)
	ropts := opts.RetentionOptions()
	curr := xtime.Now().Truncate(ropts.BlockSize())
	series := NewDatabaseSeries(DatabaseSeriesOptions{
		ID:      ident.StringID("foo"),
		Options: opts,
	}).(*dbSeries)

	// Add current block
	b := block.NewMockDatabaseBlock(ctrl)
	b.EXPECT().StartTime().Return(curr)
	series.cachedBlocks.AddBlock(b)
	// Add (current - 1) block
	b = block.NewMockDatabaseBlock(ctrl)
	b.EXPECT().StartTime().Return(curr.Add(-ropts.BlockSize()))
	b.EXPECT().Close().Return()
	series.cachedBlocks.AddBlock(b)
	// Add (current - 2) block
	b = block.NewMockDatabaseBlock(ctrl)
	b.EXPECT().StartTime().Return(curr.Add(-2 * ropts.BlockSize()))
	b.EXPECT().Close().Return()
	series.cachedBlocks.AddBlock(b)

	// Set up the buffer
	buffer := NewMockdatabaseBuffer(ctrl)
	buffer.EXPECT().
		Stats().
		Return(bufferStats{
			wiredBlocks: 0,
		})
	buffer.EXPECT().
		Tick(gomock.Any(), gomock.Any()).
		Return(bufferTickResult{
			// This means that (curr - 1 block) and (curr - 2 blocks) should
			// be removed after the tick.
			evictedBucketTimes: OptimizedTimes{
				arrIdx: 2,
				arr: [optimizedTimesArraySize]xtime.UnixNano{
					curr.Add(-ropts.BlockSize()),
					curr.Add(-2 * ropts.BlockSize()),
				},
			},
		})
	series.buffer = buffer

	assert.Equal(t, 3, series.cachedBlocks.Len())
	blockStates := BootstrappedBlockStateSnapshot{}
	shardBlockStates := NewShardBlockStateSnapshot(true, blockStates)
	_, err := series.Tick(shardBlockStates, namespace.Context{})
	require.NoError(t, err)
	assert.Equal(t, 1, series.cachedBlocks.Len())
}

func TestSeriesFetchBlocks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSeriesTestOptions()
	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	now := xtime.Now()
	starts := []xtime.UnixNano{now, now.Add(time.Second), now.Add(-time.Second)}
	blocks := block.NewMockDatabaseSeriesBlocks(ctrl)

	// Set up the blocks
	b := block.NewMockDatabaseBlock(ctrl)
	b.EXPECT().Stream(ctx).Return(xio.BlockReader{
		SegmentReader: xio.NewSegmentReader(ts.Segment{}),
	}, nil)
	blocks.EXPECT().BlockAt(starts[0]).Return(b, true)

	b = block.NewMockDatabaseBlock(ctrl)
	b.EXPECT().StartTime().Return(starts[1]).AnyTimes()
	b.EXPECT().Stream(ctx).Return(xio.EmptyBlockReader, errors.New("bar"))
	blocks.EXPECT().BlockAt(starts[1]).Return(b, true)

	blocks.EXPECT().BlockAt(starts[2]).Return(nil, false)

	// Set up the buffer
	buffer := NewMockdatabaseBuffer(ctrl)
	buffer.EXPECT().IsEmpty().Return(false)
	buffer.EXPECT().
		FetchBlocks(ctx, starts, namespace.Context{}).
		Return([]block.FetchBlockResult{block.NewFetchBlockResult(starts[2], nil, nil)})

	blockRetriever := NewMockQueryableBlockRetriever(ctrl)
	blockRetriever.EXPECT().
		IsBlockRetrievable(gomock.Any()).
		Return(false, nil).
		AnyTimes()

	series := NewDatabaseSeries(DatabaseSeriesOptions{
		ID:             ident.StringID("foo"),
		BlockRetriever: blockRetriever,
		Options:        opts,
	}).(*dbSeries)

	err := series.LoadBlock(b, WarmWrite)
	require.NoError(t, err)

	series.cachedBlocks = blocks
	series.buffer = buffer
	res, err := series.FetchBlocks(ctx, starts, namespace.Context{})
	require.NoError(t, err)

	expectedTimes := []xtime.UnixNano{starts[2], starts[0], starts[1]}
	require.Equal(t, len(expectedTimes), len(res))
	for i := 0; i < len(starts); i++ {
		assert.Equal(t, expectedTimes[i], res[i].Start)
		if i == 1 {
			assert.NotNil(t, res[i].Blocks)
		} else {
			assert.Nil(t, res[i].Blocks)
		}
		if i == 2 {
			assert.Error(t, res[i].Err)
		} else {
			assert.NoError(t, res[i].Err)
		}
	}
}

func TestSeriesFetchBlocksMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSeriesTestOptions()
	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	var (
		now    = xtime.Now()
		start  = now.Add(-time.Hour)
		end    = now.Add(time.Hour)
		starts = []xtime.UnixNano{now.Add(-time.Hour), now, now.Add(time.Second), now.Add(time.Hour)}
	)
	// Set up the buffer
	buffer := NewMockdatabaseBuffer(ctrl)
	expectedResults := block.NewFetchBlockMetadataResults()
	expectedResults.Add(block.FetchBlockMetadataResult{Start: starts[2]})

	fetchOpts := FetchBlocksMetadataOptions{
		FetchBlocksMetadataOptions: block.FetchBlocksMetadataOptions{
			IncludeSizes:     true,
			IncludeChecksums: true,
			IncludeLastRead:  true,
		},
	}
	buffer.EXPECT().IsEmpty().Return(false)
	buffer.EXPECT().
		FetchBlocksMetadata(ctx, start, end, fetchOpts).
		Return(expectedResults, nil)

	bl := block.NewMockDatabaseBlock(ctrl)
	bl.EXPECT().StartTime().Return(start).AnyTimes()

	blockRetriever := NewMockQueryableBlockRetriever(ctrl)
	blockRetriever.EXPECT().
		IsBlockRetrievable(gomock.Any()).
		Return(false, nil).
		AnyTimes()

	series := NewDatabaseSeries(DatabaseSeriesOptions{
		ID:             ident.StringID("bar"),
		BlockRetriever: blockRetriever,
		Options:        opts,
	}).(*dbSeries)

	err := series.LoadBlock(bl, WarmWrite)
	require.NoError(t, err)

	series.buffer = buffer

	res, err := series.FetchBlocksMetadata(ctx, start, end, fetchOpts)
	require.NoError(t, err)
	require.Equal(t, "bar", res.ID.String())

	metadata := res.Blocks.Results()
	expected := []struct {
		start    xtime.UnixNano
		size     int64
		checksum *uint32
		lastRead xtime.UnixNano
		hasError bool
	}{
		{starts[2], 0, nil, 0, false},
	}
	require.Equal(t, len(expected), len(metadata))
	for i := 0; i < len(expected); i++ {
		require.True(t, expected[i].start.Equal(metadata[i].Start))
		require.Equal(t, expected[i].size, metadata[i].Size)
		if expected[i].checksum == nil {
			require.Nil(t, metadata[i].Checksum)
		} else {
			require.Equal(t, *expected[i].checksum, *metadata[i].Checksum)
		}
		require.True(t, expected[i].lastRead.Equal(metadata[i].LastRead))
		if expected[i].hasError {
			require.Error(t, metadata[i].Err)
		} else {
			require.NoError(t, metadata[i].Err)
		}
	}
}

func TestSeriesOutOfOrderWritesAndRotate(t *testing.T) {
	now := xtime.FromSeconds(1477929600)
	nowFn := func() time.Time { return now.ToTime() }
	clockOpts := clock.NewOptions().SetNowFn(nowFn)
	retentionOpts := retention.NewOptions()
	opts := newSeriesTestOptions().
		SetClockOptions(clockOpts).
		SetRetentionOptions(retentionOpts)

	var (
		ctx        = context.NewBackground()
		id         = ident.StringID("foo")
		nsID       = ident.StringID("bar")
		tags       = ident.NewTags(ident.StringTag("name", "value"))
		startValue = 1.0
		blockSize  = opts.RetentionOptions().BlockSize()
		numPoints  = 10
		numBlocks  = 7
		qStart     = now
		qEnd       = qStart.Add(time.Duration(numBlocks) * blockSize)
		expected   []ts.Datapoint
	)

	metadata, err := convert.FromSeriesIDAndTags(id, tags)
	require.NoError(t, err)

	series := NewDatabaseSeries(DatabaseSeriesOptions{
		ID:       id,
		Metadata: metadata,
		Options:  opts,
	}).(*dbSeries)

	for iter := 0; iter < numBlocks; iter++ {
		start := now
		value := startValue

		for i := 0; i < numPoints; i++ {
			wasWritten, _, err := series.Write(ctx, start, value, xtime.Second, nil, WriteOptions{})
			require.NoError(t, err)
			assert.True(t, wasWritten)
			expected = append(expected, ts.Datapoint{TimestampNanos: start, Value: value})
			start = start.Add(10 * time.Second)
			value = value + 1.0
		}

		// Perform out-of-order writes
		start = now
		value = startValue
		for i := 0; i < numPoints/2; i++ {
			wasWritten, _, err := series.Write(ctx, start, value, xtime.Second, nil, WriteOptions{})
			require.NoError(t, err)
			assert.True(t, wasWritten)
			start = start.Add(10 * time.Second)
			value = value + 1.0
		}

		now = now.Add(blockSize)
	}

	iter, err := series.ReadEncoded(ctx, qStart, qEnd, namespace.Context{})
	require.NoError(t, err)
	encoded, err := iter.ToSlices(ctx)
	require.NoError(t, err)

	multiIt := opts.MultiReaderIteratorPool().Get()

	multiIt.ResetSliceOfSlices(xio.NewReaderSliceOfSlicesFromBlockReadersIterator(encoded), nil)
	it := encoding.NewSeriesIterator(encoding.SeriesIteratorOptions{
		ID:             id,
		Namespace:      nsID,
		Tags:           ident.NewTagsIterator(tags),
		StartInclusive: qStart,
		EndExclusive:   qEnd,
		Replicas:       []encoding.MultiReaderIterator{multiIt},
	}, nil)
	defer it.Close()

	var actual []ts.Datapoint
	for it.Next() {
		dp, _, _ := it.Current()
		actual = append(actual, dp)
	}

	require.NoError(t, it.Err())
	require.Equal(t, expected, actual)
}

func TestSeriesWriteReadFromTheSameBucket(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bl := block.NewMockDatabaseBlock(ctrl)
	bl.EXPECT().StartTime().Return(xtime.Now()).AnyTimes()
	bl.EXPECT().Len().Return(0).AnyTimes()

	opts := newSeriesTestOptions()
	opts = opts.SetRetentionOptions(opts.RetentionOptions().
		SetRetentionPeriod(40 * 24 * time.Hour).
		// A block size of 5 days is not equally as divisible as seconds from time
		// zero and seconds from time epoch.
		// now := time.Now()
		// blockSize := 5 * 24 * time.Hour
		// fmt.Println(now) -> 2018-01-24 14:29:31.624265 -0500 EST m=+0.003810489
		// fmt.Println(now.Truncate(blockSize)) -> 2018-01-21 19:00:00 -0500 EST
		// fmt.Println(time.Unix(0, now.UnixNano()/int64(blockSize)*int64(blockSize)))
		//                                       -> 2018-01-23 19:00:00 -0500 EST
		SetBlockSize(5 * 24 * time.Hour).
		SetBufferFuture(10 * time.Minute).
		SetBufferPast(20 * time.Minute))
	curr := xtime.Now()
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return curr.ToTime()
	}))

	blockRetriever := NewMockQueryableBlockRetriever(ctrl)
	blockRetriever.EXPECT().
		IsBlockRetrievable(gomock.Any()).
		Return(false, nil).
		AnyTimes()

	series := NewDatabaseSeries(DatabaseSeriesOptions{
		ID:             ident.StringID("foo"),
		BlockRetriever: blockRetriever,
		Options:        opts,
	}).(*dbSeries)

	err := series.LoadBlock(bl, WarmWrite)
	require.NoError(t, err)

	ctx := context.NewBackground()
	defer ctx.Close()

	wasWritten, _, err := series.Write(ctx, curr.Add(-3*time.Minute),
		1, xtime.Second, nil, WriteOptions{})
	assert.NoError(t, err)
	assert.True(t, wasWritten)
	wasWritten, _, err = series.Write(ctx, curr.Add(-2*time.Minute),
		2, xtime.Second, nil, WriteOptions{})
	assert.NoError(t, err)
	assert.True(t, wasWritten)
	wasWritten, _, err = series.Write(ctx, curr.Add(-1*time.Minute),
		3, xtime.Second, nil, WriteOptions{})
	assert.NoError(t, err)
	assert.True(t, wasWritten)

	iter, err := series.ReadEncoded(ctx, curr.Add(-5*time.Minute),
		curr.Add(time.Minute), namespace.Context{})
	require.NoError(t, err)
	results, err := iter.ToSlices(ctx)
	require.NoError(t, err)
	values, err := decodedReaderValues(results, opts, namespace.Context{})
	require.NoError(t, err)

	require.Equal(t, 3, len(values))
}

func TestSeriesCloseNonCacheLRUPolicy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSeriesTestOptions().
		SetCachePolicy(CacheRecentlyRead)
	series := NewDatabaseSeries(DatabaseSeriesOptions{
		ID:      ident.StringID("foo"),
		Options: opts,
	}).(*dbSeries)

	start := xtime.Now()
	blocks := block.NewDatabaseSeriesBlocks(0)
	diskBlock := block.NewMockDatabaseBlock(ctrl)
	diskBlock.EXPECT().StartTime().Return(start).AnyTimes()
	diskBlock.EXPECT().Close()
	blocks.AddBlock(diskBlock)

	series.cachedBlocks = blocks
	series.Close()
}

func TestSeriesCloseCacheLRUPolicy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newSeriesTestOptions().
		SetCachePolicy(CacheLRU)
	series := NewDatabaseSeries(DatabaseSeriesOptions{
		ID:      ident.StringID("foo"),
		Options: opts,
	}).(*dbSeries)

	start := xtime.Now()
	blocks := block.NewDatabaseSeriesBlocks(0)
	// Add a block that was retrieved from disk
	diskBlock := block.NewMockDatabaseBlock(ctrl)
	diskBlock.EXPECT().StartTime().Return(start).AnyTimes()
	blocks.AddBlock(diskBlock)

	// Add block that was not retrieved from disk
	nonDiskBlock := block.NewMockDatabaseBlock(ctrl)
	nonDiskBlock.EXPECT().StartTime().
		Return(start.Add(opts.RetentionOptions().BlockSize())).AnyTimes()
	blocks.AddBlock(nonDiskBlock)

	series.cachedBlocks = blocks
	series.Close()
}
