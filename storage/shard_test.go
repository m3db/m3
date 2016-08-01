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
	"testing"
	"time"

	"github.com/m3db/m3db/generated/mocks/mocks"
	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func testDatabaseShard(opts m3db.DatabaseOptions) *dbShard {
	return newDatabaseShard(0, opts).(*dbShard)
}

func TestShardBootstrapWithError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	writeStart := time.Now()
	cutover := time.Now().Add(-time.Minute)
	opts := testDatabaseOptions()
	s := testDatabaseShard(opts)
	fooSeries := mocks.NewMockdatabaseSeries(ctrl)
	barSeries := mocks.NewMockdatabaseSeries(ctrl)
	s.lookup["foo"] = &dbShardEntry{series: fooSeries}
	s.lookup["bar"] = &dbShardEntry{series: barSeries}

	fooBlocks := mocks.NewMockDatabaseSeriesBlocks(ctrl)
	barBlocks := mocks.NewMockDatabaseSeriesBlocks(ctrl)
	fooSeries.EXPECT().Bootstrap(fooBlocks, cutover).Return(nil)
	barSeries.EXPECT().Bootstrap(barBlocks, cutover).Return(errors.New("series error"))
	bootstrappedSeries := map[string]m3db.DatabaseSeriesBlocks{
		"foo": fooBlocks,
		"bar": barBlocks,
	}
	shardResult := mocks.NewMockShardResult(ctrl)
	shardResult.EXPECT().GetAllSeries().Return(bootstrappedSeries)

	bs := mocks.NewMockBootstrap(ctrl)
	bs.EXPECT().Run(writeStart, s.shard).Return(shardResult, errors.New("bootstrap error"))
	err := s.Bootstrap(bs, writeStart, cutover)

	require.NotNil(t, err)
	require.Equal(t, "error occurred bootstrapping shard 0 from external sources: bootstrap error\nseries error", err.Error())
	require.Equal(t, bootstrapped, s.bs)
}

func TestShardFlushDuringBootstrap(t *testing.T) {
	s := testDatabaseShard(testDatabaseOptions())
	s.bs = bootstrapping
	err := s.Flush(nil, time.Now(), nil)
	require.Equal(t, err, errShardNotBootstrapped)
}

func TestShardFlushNoPersistFuncNoError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s := testDatabaseShard(testDatabaseOptions())
	s.bs = bootstrapped
	blockStart := time.Unix(21600, 0)
	pm := mocks.NewMockPersistenceManager(ctrl)
	prepared := m3db.PreparedPersistence{}
	pm.EXPECT().Prepare(s.shard, blockStart).Return(prepared, nil)
	require.Nil(t, s.Flush(nil, blockStart, pm))
}

func TestShardFlushNoPersistFuncWithError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s := testDatabaseShard(testDatabaseOptions())
	s.bs = bootstrapped
	blockStart := time.Unix(21600, 0)
	pm := mocks.NewMockPersistenceManager(ctrl)
	prepared := m3db.PreparedPersistence{}
	expectedErr := errors.New("some error")
	pm.EXPECT().Prepare(s.shard, blockStart).Return(prepared, expectedErr)
	actualErr := s.Flush(nil, blockStart, pm)
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
	pm := mocks.NewMockPersistenceManager(ctrl)
	prepared := m3db.PreparedPersistence{
		Persist: func(string, m3db.Segment) error { return nil },
		Close:   func() { closed = true },
	}
	expectedErr := errors.New("error foo")
	pm.EXPECT().Prepare(s.shard, blockStart).Return(prepared, expectedErr)

	flushed := make(map[int]struct{})
	for i := 0; i < 2; i++ {
		i := i
		var expectedErr error
		if i == 1 {
			expectedErr = errors.New("error bar")
		}
		series := mocks.NewMockdatabaseSeries(ctrl)
		series.EXPECT().
			Flush(nil, blockStart, gomock.Any()).
			Do(func(m3db.Context, time.Time, m3db.PersistenceFn) {
				flushed[i] = struct{}{}
			}).
			Return(expectedErr)
		s.list.PushBack(series)
	}
	err := s.Flush(nil, blockStart, pm)

	require.Equal(t, len(flushed), 2)
	for i := 0; i < 2; i++ {
		_, ok := flushed[i]
		require.True(t, ok)
	}

	require.True(t, closed)
	require.NotNil(t, err)
	require.Equal(t, "error foo\nerror bar", err.Error())
}

func addTestSeries(t *testing.T, shard *dbShard, id string) databaseSeries {
	series := newDatabaseSeries(id, bootstrapped, shard.opts)
	elem := shard.list.PushBack(series)
	entry := &dbShardEntry{series: series, elem: elem, curWriters: 0}
	shard.lookup[id] = entry
	return series
}

// This tests the scenario where an empty series is expired.
func TestPurgeExpiredSeriesEmptySeries(t *testing.T) {
	opts := testDatabaseOptions()
	shard := testDatabaseShard(opts)
	addTestSeries(t, shard, "foo")
	shard.Tick()
	require.Equal(t, 0, len(shard.lookup))
}

// This tests the scenario where a non-empty series is not expired.
func TestPurgeExpiredSeriesNonEmptySeries(t *testing.T) {
	opts := testDatabaseOptions()
	shard := testDatabaseShard(opts)
	ctx := opts.GetContextPool().Get()
	nowFn := opts.GetNowFn()
	shard.Write(ctx, "foo", nowFn(), 1.0, xtime.Second, nil)
	expired := shard.tickForEachSeries()
	require.Len(t, expired, 0)
}

// This tests the scenario where a series is empty when series.Tick() is called,
// but receives writes after tickForEachSeries finishes but before purgeExpiredSeries
// starts. The expected behavior is not to expire series in this case.
func TestPurgeExpiredSeriesWriteAfterTicking(t *testing.T) {
	opts := testDatabaseOptions()
	shard := testDatabaseShard(opts)
	series := addTestSeries(t, shard, "foo")
	expired := shard.tickForEachSeries()
	require.Len(t, expired, 1)

	ctx := opts.GetContextPool().Get()
	nowFn := opts.GetNowFn()
	shard.Write(ctx, "foo", nowFn(), 1.0, xtime.Second, nil)
	require.False(t, series.Empty())

	shard.purgeExpiredSeries(expired)
	require.Equal(t, 1, len(shard.lookup))
}

// This tests the scenario where tickForEachSeries finishes, and before purgeExpiredSeries
// starts, we receive a write for a series, then purgeExpiredSeries runs, then we write to
// the series. The expected behavior is not to expire series in this case.
func TestPurgeExpiredSeriesWriteAfterPurging(t *testing.T) {
	opts := testDatabaseOptions()
	shard := testDatabaseShard(opts)
	addTestSeries(t, shard, "foo")
	expired := shard.tickForEachSeries()
	require.Len(t, expired, 1)

	ctx := opts.GetContextPool().Get()
	nowFn := opts.GetNowFn()
	series, completionFn := shard.writableSeries("foo")
	shard.purgeExpiredSeries(expired)
	series.Write(ctx, nowFn(), 1.0, xtime.Second, nil)
	completionFn()

	require.False(t, series.Empty())
	require.Equal(t, 1, len(shard.lookup))
}
