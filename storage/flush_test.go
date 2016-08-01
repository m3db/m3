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

type mockDatabase struct {
	opts   m3db.DatabaseOptions
	shards []databaseShard
	bs     bootstrapState
}

func newMockDatabase() *mockDatabase                    { return &mockDatabase{opts: testDatabaseOptions()} }
func (d *mockDatabase) Options() m3db.DatabaseOptions   { return d.opts }
func (d *mockDatabase) Open() error                     { return nil }
func (d *mockDatabase) Close() error                    { return nil }
func (d *mockDatabase) Bootstrap() error                { return nil }
func (d *mockDatabase) IsBootstrapped() bool            { return d.bs == bootstrapped }
func (d *mockDatabase) getOwnedShards() []databaseShard { return d.shards }
func (d *mockDatabase) flush(t time.Time, async bool)   {}
func (d *mockDatabase) Write(m3db.Context, string, time.Time, float64, xtime.Unit, []byte) error {
	return nil
}
func (d *mockDatabase) ReadEncoded(m3db.Context, string, time.Time, time.Time) ([][]m3db.SegmentReader, error) {
	return nil, nil
}

func TestNeedFlushDuringBootstrap(t *testing.T) {
	database := newMockDatabase()
	fm := newFlushManager(database)
	now := database.Options().GetNowFn()()
	require.False(t, fm.NeedsFlush(now))
	database.bs = bootstrapped
	require.True(t, fm.NeedsFlush(now))
}

func TestNeedFlushWhileFlushing(t *testing.T) {
	database := newMockDatabase()
	database.bs = bootstrapped
	fm := newFlushManager(database).(*flushManager)
	now := database.Options().GetNowFn()()
	require.True(t, fm.NeedsFlush(now))
	fm.fs = flushInProgress
	require.False(t, fm.NeedsFlush(now))
}

func TestNeedFlushAttemptedBefore(t *testing.T) {
	database := newMockDatabase()
	database.bs = bootstrapped
	fm := newFlushManager(database).(*flushManager)
	now := database.Options().GetNowFn()()
	require.True(t, fm.NeedsFlush(now))
	firstBlockStart := now.Add(-2 * time.Hour).Add(-10 * time.Minute).Truncate(2 * time.Hour)
	fm.flushAttempted[firstBlockStart] = flushState{Status: flushInProgress}
	require.False(t, fm.NeedsFlush(now))
}

func TestGetFirstBlockStart(t *testing.T) {
	inputs := []struct {
		tickStart time.Time
		expected  time.Time
	}{
		{time.Unix(14900, 0), time.Unix(0, 0)},
		{time.Unix(15000, 0), time.Unix(7200, 0)},
		{time.Unix(15100, 0), time.Unix(7200, 0)},
	}
	database := newMockDatabase()
	fm := newFlushManager(database).(*flushManager)
	for _, input := range inputs {
		require.Equal(t, input.expected, fm.getFirstBlockStart(input.tickStart))
	}
}

func TestFlush(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	inputTimes := []struct {
		bs time.Time
		fs flushState
	}{
		{time.Unix(21600, 0), flushState{flushFailed, 2}},
		{time.Unix(28800, 0), flushState{flushFailed, 3}},
		{time.Unix(36000, 0), flushState{flushInProgress, 0}},
		{time.Unix(43200, 0), flushState{flushSuccess, 1}},
	}
	notFlushed := make(map[time.Time]struct{})
	for i := 1; i < 4; i++ {
		notFlushed[inputTimes[i].bs] = struct{}{}
	}

	tickStart := time.Unix(188000, 0)
	database := newMockDatabase()
	fm := newFlushManager(database).(*flushManager)
	for _, input := range inputTimes {
		fm.flushAttempted[input.bs] = input.fs
	}
	endTime := time.Unix(0, 0).Add(2 * 24 * time.Hour)
	for shard := 0; shard < 2; shard++ {
		m := mocks.NewMockdatabaseShard(ctrl)
		database.shards = append(database.shards, m)
		m.EXPECT().ShardNum().Return(uint32(shard))
		cur := inputTimes[0].bs
		for !cur.After(endTime) {
			if _, excluded := notFlushed[cur]; !excluded {
				m.EXPECT().Flush(gomock.Any(), cur, fm.pm).Return(nil)
			}
			cur = cur.Add(2 * time.Hour)
		}
		m.EXPECT().Flush(gomock.Any(), cur, fm.pm).Return(errors.New("some errors"))
	}

	fm.Flush(tickStart, false)

	j := 0
	for i := 0; i < 19; i++ {
		if i == 1 {
			j += 3
		}
		expectedTime := time.Unix(int64(21600+j*7200), 0)
		require.Equal(t, flushSuccess, fm.flushAttempted[expectedTime].Status)
		j++
	}
	expectedTime := time.Unix(int64(180000), 0)
	require.Equal(t, flushFailed, fm.flushAttempted[expectedTime].Status)
	require.Equal(t, 1, fm.flushAttempted[expectedTime].NumFailures)
	require.Equal(t, flushNotStarted, fm.fs)
}

func TestGetTimesToFlush(t *testing.T) {
	inputTimes := []struct {
		bs time.Time
		fs flushState
	}{
		{time.Unix(21600, 0), flushState{flushFailed, 2}},
		{time.Unix(28800, 0), flushState{flushFailed, 3}},
		{time.Unix(36000, 0), flushState{flushInProgress, 0}},
		{time.Unix(43200, 0), flushState{flushSuccess, 1}},
	}
	database := newMockDatabase()
	fm := newFlushManager(database).(*flushManager)
	for _, input := range inputTimes {
		fm.flushAttempted[input.bs] = input.fs
	}
	tickStart := time.Unix(188000, 0)
	res := fm.getTimesToFlush(tickStart)
	require.Equal(t, 20, len(res))
	j := 0
	for i := 0; i < 20; i++ {
		if i == 1 {
			j += 3
		}
		expectedTime := time.Unix(int64(21600+j*7200), 0)
		require.Equal(t, expectedTime, res[19-i])
		require.Equal(t, flushInProgress, fm.flushAttempted[expectedTime].Status)
		j++
	}
}

func TestFlushWithTimes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	database := newMockDatabase()
	fm := newFlushManager(database).(*flushManager)
	ctx := fm.opts.GetContextPool().Get()
	defer ctx.Close()

	flushTime := time.Unix(7200, 0)
	for i := 0; i < 2; i++ {
		m := mocks.NewMockdatabaseShard(ctrl)
		database.shards = append(database.shards, m)
		m.EXPECT().Flush(ctx, flushTime, fm.pm).Return(nil)
	}
	require.True(t, fm.flushWithTime(ctx, flushTime))

	m := mocks.NewMockdatabaseShard(ctrl)
	database.shards[0] = m
	m.EXPECT().Flush(ctx, flushTime, fm.pm).Return(nil)

	m = mocks.NewMockdatabaseShard(ctrl)
	database.shards[1] = m
	m.EXPECT().Flush(ctx, flushTime, fm.pm).Return(errors.New("some errors"))
	m.EXPECT().ShardNum().Return(uint32(1))

	require.False(t, fm.flushWithTime(ctx, flushTime))
}
