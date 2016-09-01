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
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/bootstrap"
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
	return newDatabaseShard(0, &testIncreasingIndex{}, commitLogWriteNoOp, opts).(*dbShard)
}

func addMockSeries(ctrl *gomock.Controller, shard *dbShard, id string, index uint64) *MockdatabaseSeries {
	series := NewMockdatabaseSeries(ctrl)
	shard.lookup[id] = shard.list.PushBack(&dbShardEntry{series: series, index: index})
	return series
}

func TestShardBootstrapWithError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	writeStart := time.Now()
	cutover := time.Now().Add(-time.Minute)
	opts := testDatabaseOptions()
	s := testDatabaseShard(opts)
	fooSeries := NewMockdatabaseSeries(ctrl)
	fooSeries.EXPECT().ID().Return("foo")
	barSeries := NewMockdatabaseSeries(ctrl)
	barSeries.EXPECT().ID().Return("bar")
	s.lookup["foo"] = s.list.PushBack(&dbShardEntry{series: fooSeries})
	s.lookup["bar"] = s.list.PushBack(&dbShardEntry{series: barSeries})

	fooBlocks := block.NewMockDatabaseSeriesBlocks(ctrl)
	barBlocks := block.NewMockDatabaseSeriesBlocks(ctrl)
	fooSeries.EXPECT().Bootstrap(fooBlocks, cutover).Return(nil)
	barSeries.EXPECT().Bootstrap(barBlocks, cutover).Return(errors.New("series error"))
	bootstrappedSeries := map[string]block.DatabaseSeriesBlocks{
		"foo": fooBlocks,
		"bar": barBlocks,
	}
	shardResult := bootstrap.NewMockShardResult(ctrl)
	shardResult.EXPECT().GetAllSeries().Return(bootstrappedSeries)

	bs := bootstrap.NewMockBootstrap(ctrl)
	bs.EXPECT().Run(writeStart, testNamespaceName, s.shard).Return(shardResult, errors.New("bootstrap error"))
	err := s.Bootstrap(bs, testNamespaceName, writeStart, cutover)

	require.NotNil(t, err)
	require.Equal(t, "error occurred bootstrapping shard 0 from external sources: bootstrap error\nseries error", err.Error())
	require.Equal(t, bootstrapped, s.bs)
}

func TestShardFlushDuringBootstrap(t *testing.T) {
	s := testDatabaseShard(testDatabaseOptions())
	s.bs = bootstrapping
	err := s.Flush(nil, testNamespaceName, time.Now(), nil)
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
	require.Nil(t, s.Flush(nil, testNamespaceName, blockStart, pm))
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
	actualErr := s.Flush(nil, testNamespaceName, blockStart, pm)
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
		series := NewMockdatabaseSeries(ctrl)
		series.EXPECT().
			Flush(nil, blockStart, gomock.Any()).
			Do(func(context.Context, time.Time, persist.Fn) {
				flushed[i] = struct{}{}
			}).
			Return(expectedErr)
		s.list.PushBack(&dbShardEntry{series: series})
	}
	err := s.Flush(nil, testNamespaceName, blockStart, pm)

	require.Equal(t, len(flushed), 2)
	for i := 0; i < 2; i++ {
		_, ok := flushed[i]
		require.True(t, ok)
	}

	require.True(t, closed)
	require.NotNil(t, err)
	require.Equal(t, "error foo\nerror bar", err.Error())
}

func addTestSeries(shard *dbShard, id string) databaseSeries {
	series := newDatabaseSeries(id, bootstrapped, shard.opts)
	shard.lookup[id] = shard.list.PushBack(&dbShardEntry{series: series})
	return series
}

// This tests the scenario where an empty series is expired.
func TestPurgeExpiredSeriesEmptySeries(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testDatabaseOptions()
	shard := testDatabaseShard(opts)
	addTestSeries(shard, "foo")
	shard.Tick()
	require.Equal(t, 0, len(shard.lookup))
}

// This tests the scenario where a non-empty series is not expired.
func TestPurgeExpiredSeriesNonEmptySeries(t *testing.T) {
	opts := testDatabaseOptions()
	shard := testDatabaseShard(opts)
	ctx := opts.GetContextPool().Get()
	nowFn := opts.GetClockOptions().GetNowFn()
	shard.Write(ctx, "foo", nowFn(), 1.0, xtime.Second, nil)
	expired := shard.tickForEachSeries()
	require.Len(t, expired, 0)
}

// This tests the scenario where a series is empty when series.Tick() is called,
// but receives writes after tickForEachSeries finishes but before purgeExpiredSeries
// starts. The expected behavior is not to expire series in this case.
func TestPurgeExpiredSeriesWriteAfterTicking(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testDatabaseOptions()
	shard := testDatabaseShard(opts)
	series := addTestSeries(shard, "foo")
	expired := shard.tickForEachSeries()
	require.Len(t, expired, 1)

	ctx := opts.GetContextPool().Get()
	nowFn := opts.GetClockOptions().GetNowFn()
	shard.Write(ctx, "foo", nowFn(), 1.0, xtime.Second, nil)
	require.False(t, series.Empty())

	shard.purgeExpiredSeries(expired)
	require.Equal(t, 1, len(shard.lookup))
}

// This tests the scenario where tickForEachSeries finishes, and before purgeExpiredSeries
// starts, we receive a write for a series, then purgeExpiredSeries runs, then we write to
// the series. The expected behavior is not to expire series in this case.
func TestPurgeExpiredSeriesWriteAfterPurging(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testDatabaseOptions()
	shard := testDatabaseShard(opts)
	addTestSeries(shard, "foo")
	expired := shard.tickForEachSeries()
	require.Len(t, expired, 1)

	ctx := opts.GetContextPool().Get()
	nowFn := opts.GetClockOptions().GetNowFn()
	series, _, completionFn := shard.writableSeries("foo")
	shard.purgeExpiredSeries(expired)
	series.Write(ctx, nowFn(), 1.0, xtime.Second, nil)
	completionFn()

	require.False(t, series.Empty())
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
	ctx := opts.GetContextPool().Get()
	defer ctx.Close()

	shard := testDatabaseShard(opts)
	require.Nil(t, shard.FetchBlocks(ctx, "foo", nil))
}

func TestShardFetchBlocksIDExists(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testDatabaseOptions()
	ctx := opts.GetContextPool().Get()
	defer ctx.Close()

	shard := testDatabaseShard(opts)
	id := "foo"
	series := addMockSeries(ctrl, shard, id, 0)
	now := time.Now()
	starts := []time.Time{now}
	expected := []FetchBlockResult{newFetchBlockResult(now, nil, nil)}
	series.EXPECT().FetchBlocks(ctx, starts).Return(expected)
	res := shard.FetchBlocks(ctx, id, starts)
	require.Equal(t, expected, res)
}

func TestShardFetchBlocksMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testDatabaseOptions()
	ctx := opts.GetContextPool().Get()
	defer ctx.Close()

	shard := testDatabaseShard(opts)
	ids := make([]string, 0, 5)
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("foo.%d", i)
		series := addMockSeries(ctrl, shard, id, uint64(i))
		if i >= 2 && i < 7 {
			ids = append(ids, id)
			series.EXPECT().FetchBlocksMetadata(ctx, true).Return(newFetchBlocksMetadataResult(id, nil))
		}
	}

	res, nextPageToken := shard.FetchBlocksMetadata(ctx, 5, 2, true)
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
