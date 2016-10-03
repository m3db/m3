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
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/series"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

type testIncreasingIndex struct {
	created uint64
}

func (i *testIncreasingIndex) nextIndex() uint64 {
	created := atomic.AddUint64(&i.created, 1)
	return created - 1
}

func testDatabaseShard(opts Options) *dbShard {
	return newDatabaseShard(0, &testIncreasingIndex{}, commitLogWriteNoOp, true, opts).(*dbShard)
}

func addMockSeries(ctrl *gomock.Controller, shard *dbShard, id string, index uint64) *series.MockDatabaseSeries {
	series := series.NewMockDatabaseSeries(ctrl)
	shard.lookup[id] = shard.list.PushBack(&dbShardEntry{series: series, index: index})
	return series
}

func TestShardDontNeedBootstrap(t *testing.T) {
	opts := testDatabaseOptions()
	shard := newDatabaseShard(0, &testIncreasingIndex{}, commitLogWriteNoOp, false, opts).(*dbShard)
	require.Equal(t, bootstrapped, shard.bs)
	require.True(t, shard.newSeriesBootstrapped)
}

func TestShardBootstrapWithError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	writeStart := time.Now()
	opts := testDatabaseOptions()
	s := testDatabaseShard(opts)
	fooSeries := series.NewMockDatabaseSeries(ctrl)
	barSeries := series.NewMockDatabaseSeries(ctrl)
	s.lookup["foo"] = s.list.PushBack(&dbShardEntry{series: fooSeries})
	s.lookup["bar"] = s.list.PushBack(&dbShardEntry{series: barSeries})

	fooBlocks := block.NewMockDatabaseSeriesBlocks(ctrl)
	barBlocks := block.NewMockDatabaseSeriesBlocks(ctrl)
	fooSeries.EXPECT().Bootstrap(fooBlocks).Return(nil)
	fooSeries.EXPECT().IsBootstrapped().Return(true)
	barSeries.EXPECT().Bootstrap(barBlocks).Return(errors.New("series error"))
	barSeries.EXPECT().IsBootstrapped().Return(true)
	bootstrappedSeries := map[string]block.DatabaseSeriesBlocks{
		"foo": fooBlocks,
		"bar": barBlocks,
	}

	err := s.Bootstrap(bootstrappedSeries, writeStart)

	require.NotNil(t, err)
	require.Equal(t, "series error", err.Error())
	require.Equal(t, bootstrapped, s.bs)
}

func TestShardFlushDuringBootstrap(t *testing.T) {
	s := testDatabaseShard(testDatabaseOptions())
	s.bs = bootstrapping
	err := s.Flush(testNamespaceName, time.Now(), nil)
	require.Equal(t, err, errShardNotBootstrapped)
}

func TestShardFlushNoPersistFuncNoError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s := testDatabaseShard(testDatabaseOptions())
	s.bs = bootstrapped
	blockStart := time.Unix(21600, 0)
	pm := persist.NewMockManager(ctrl)
	prepared := persist.PreparedPersist{}
	pm.EXPECT().Prepare(testNamespaceName, s.shard, blockStart).Return(prepared, nil)
	require.Nil(t, s.Flush(testNamespaceName, blockStart, pm))
}

func TestShardFlushNoPersistFuncWithError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s := testDatabaseShard(testDatabaseOptions())
	s.bs = bootstrapped
	blockStart := time.Unix(21600, 0)
	pm := persist.NewMockManager(ctrl)
	prepared := persist.PreparedPersist{}
	expectedErr := errors.New("some error")
	pm.EXPECT().Prepare(testNamespaceName, s.shard, blockStart).Return(prepared, expectedErr)
	actualErr := s.Flush(testNamespaceName, blockStart, pm)
	require.NotNil(t, actualErr)
	require.Equal(t, "some error", actualErr.Error())
}

func TestShardFlushSeriesFlushError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s := testDatabaseShard(testDatabaseOptions())
	s.bs = bootstrapped

	var closed bool
	blockStart := time.Unix(21600, 0)
	pm := persist.NewMockManager(ctrl)
	prepared := persist.PreparedPersist{
		Persist: func(string, ts.Segment) error { return nil },
		Close:   func() { closed = true },
	}
	expectedErr := errors.New("error foo")
	pm.EXPECT().Prepare(testNamespaceName, s.shard, blockStart).Return(prepared, expectedErr)

	flushed := make(map[int]struct{})
	for i := 0; i < 2; i++ {
		i := i
		var expectedErr error
		if i == 1 {
			expectedErr = errors.New("error bar")
		}
		series := series.NewMockDatabaseSeries(ctrl)
		series.EXPECT().
			Flush(gomock.Any(), blockStart, gomock.Any()).
			Do(func(context.Context, time.Time, persist.Fn) {
				flushed[i] = struct{}{}
			}).
			Return(expectedErr)
		s.list.PushBack(&dbShardEntry{series: series})
	}
	err := s.Flush(testNamespaceName, blockStart, pm)

	require.Equal(t, len(flushed), 2)
	for i := 0; i < 2; i++ {
		_, ok := flushed[i]
		require.True(t, ok)
	}

	require.True(t, closed)
	require.NotNil(t, err)
	require.Equal(t, "error foo\nerror bar", err.Error())
}

func addTestSeries(shard *dbShard, id string) series.DatabaseSeries {
	series := series.NewDatabaseSeries(id, NewSeriesOptionsFromOptions(shard.opts))
	series.Bootstrap(nil)
	shard.lookup[id] = shard.list.PushBack(&dbShardEntry{series: series})
	return series
}

func TestShardTick(t *testing.T) {
	now := time.Now()
	nowLock := sync.RWMutex{}
	nowFn := func() time.Time {
		nowLock.RLock()
		value := now
		nowLock.RUnlock()
		return value
	}
	setNow := func(t time.Time) {
		nowLock.Lock()
		now = t
		nowLock.Unlock()
	}

	opts := testDatabaseOptions()
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(nowFn))
	shard := testDatabaseShard(opts)

	var slept time.Duration
	shard.sleepFn = func(t time.Duration) {
		slept += t
		setNow(nowFn().Add(t))
	}
	shard.tickSleepIfAheadEvery = 1

	ctx := context.NewContext()
	defer ctx.Close()

	shard.Write(ctx, "foo", nowFn(), 1.0, xtime.Second, nil)
	shard.Write(ctx, "bar", nowFn(), 2.0, xtime.Second, nil)
	shard.Write(ctx, "baz", nowFn(), 3.0, xtime.Second, nil)
	expired := shard.tickAndExpire(6 * time.Millisecond)

	require.Equal(t, 0, expired)
	require.Equal(t, 4*time.Millisecond, slept)
}

// This tests the scenario where an empty series is expired.
func TestPurgeExpiredSeriesEmptySeries(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testDatabaseOptions()
	shard := testDatabaseShard(opts)
	addTestSeries(shard, "foo")
	shard.Tick(0)
	require.Equal(t, 0, len(shard.lookup))
}

// This tests the scenario where a non-empty series is not expired.
func TestPurgeExpiredSeriesNonEmptySeries(t *testing.T) {
	opts := testDatabaseOptions()
	shard := testDatabaseShard(opts)
	ctx := opts.ContextPool().Get()
	nowFn := opts.ClockOptions().NowFn()
	shard.Write(ctx, "foo", nowFn(), 1.0, xtime.Second, nil)
	expired := shard.tickAndExpire(0)
	require.Equal(t, 0, expired)
}

// This tests the scenario where a series is empty when series.Tick() is called,
// but receives writes after tickForEachSeries finishes but before purgeExpiredSeries
// starts. The expected behavior is not to expire series in this case.
func TestPurgeExpiredSeriesWriteAfterTicking(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testDatabaseOptions()
	shard := testDatabaseShard(opts)
	s := addMockSeries(ctrl, shard, "foo", 0)
	s.EXPECT().ID().Return("foo")
	s.EXPECT().Tick().Do(func() {
		// Emulate a write taking place just after tick for this series
		s.EXPECT().Write(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

		ctx := opts.ContextPool().Get()
		nowFn := opts.ClockOptions().NowFn()
		shard.Write(ctx, "foo", nowFn(), 1.0, xtime.Second, nil)

		s.EXPECT().IsEmpty().Return(false)
	}).Return(series.ErrSeriesAllDatapointsExpired)

	expired := shard.tickAndExpire(0)
	require.Equal(t, 1, expired)
	require.Equal(t, 1, len(shard.lookup))
}

// This tests the scenario where tickForEachSeries finishes, and before purgeExpiredSeries
// starts, we receive a write for a series, then purgeExpiredSeries runs, then we write to
// the series. The expected behavior is not to expire series in this case.
func TestPurgeExpiredSeriesWriteAfterPurging(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var writeCompletionFn func()

	opts := testDatabaseOptions()
	shard := testDatabaseShard(opts)
	s := addMockSeries(ctrl, shard, "foo", 0)
	s.EXPECT().ID().Return("foo")
	s.EXPECT().Tick().Do(func() {
		// Emulate a write taking place and staying open just after tick for this series
		_, _, writeCompletionFn = shard.writableSeries("foo")
	}).Return(series.ErrSeriesAllDatapointsExpired)

	expired := shard.tickAndExpire(0)
	require.Equal(t, 1, expired)
	require.Equal(t, 1, len(shard.lookup))

	writeCompletionFn()
	require.Equal(t, 1, len(shard.lookup))
}

func TestForEachShardEntry(t *testing.T) {
	opts := testDatabaseOptions()
	shard := testDatabaseShard(opts)
	for i := 0; i < 10; i++ {
		addTestSeries(shard, fmt.Sprintf("foo.%d", i))
	}

	count := 0
	entryFn := func(entry *dbShardEntry) error {
		count++
		if entry.series.ID() == "foo.5" {
			return errors.New("foo")
		}
		return nil
	}

	stopIterFn := func(entry *dbShardEntry) bool {
		return entry.series.ID() == "foo.8"
	}

	shard.forEachShardEntry(true, entryFn, stopIterFn)
	require.Equal(t, 8, count)

	count = 0
	shard.forEachShardEntry(false, entryFn, stopIterFn)
	require.Equal(t, 6, count)
}

func TestShardFetchBlocksIDNotExists(t *testing.T) {
	opts := testDatabaseOptions()
	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	shard := testDatabaseShard(opts)
	require.Nil(t, shard.FetchBlocks(ctx, "foo", nil))
}

func TestShardFetchBlocksIDExists(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testDatabaseOptions()
	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	shard := testDatabaseShard(opts)
	id := "foo"
	series := addMockSeries(ctrl, shard, id, 0)
	now := time.Now()
	starts := []time.Time{now}
	expected := []block.FetchBlockResult{block.NewFetchBlockResult(now, nil, nil)}
	series.EXPECT().FetchBlocks(ctx, starts).Return(expected)
	res := shard.FetchBlocks(ctx, id, starts)
	require.Equal(t, expected, res)
}

func TestShardFetchBlocksMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testDatabaseOptions()
	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	shard := testDatabaseShard(opts)
	ids := make([]string, 0, 5)
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("foo.%d", i)
		series := addMockSeries(ctrl, shard, id, uint64(i))
		if i >= 2 && i < 7 {
			ids = append(ids, id)
			series.EXPECT().FetchBlocksMetadata(ctx, true, true).Return(block.NewFetchBlocksMetadataResult(id, nil))
		}
	}

	res, nextPageToken := shard.FetchBlocksMetadata(ctx, 5, 2, true, true)
	require.Equal(t, len(ids), len(res))
	require.Equal(t, int64(7), *nextPageToken)
	for i := 0; i < len(res); i++ {
		require.Equal(t, ids[i], res[i].ID())
	}
}

func TestShardCleanupFileset(t *testing.T) {
	opts := testDatabaseOptions()
	shard := testDatabaseShard(opts)
	shard.filesetBeforeFn = func(_ string, namespace string, shardID uint32, t time.Time) ([]string, error) {
		return []string{namespace, strconv.Itoa(int(shardID))}, nil
	}
	var deletedFiles []string
	shard.deleteFilesFn = func(files []string) error {
		deletedFiles = append(deletedFiles, files...)
		return nil
	}
	require.NoError(t, shard.CleanupFileset(testNamespaceName, time.Now()))
	require.Equal(t, []string{testNamespaceName, "0"}, deletedFiles)
}
