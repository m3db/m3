// Copyright (c) 2017 Uber Technologies, Inc.
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

package aggregator

import (
	"errors"
	"testing"
	"time"

	schema "github.com/m3db/m3aggregator/generated/proto/flush"
	"github.com/m3db/m3cluster/shard"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

var (
	testFlushTimes = &schema.ShardSetFlushTimes{
		ByShard: map[uint32]*schema.ShardFlushTimes{
			0: &schema.ShardFlushTimes{
				ByResolution: map[int64]int64{
					1000000000:  3663000000000,
					60000000000: 3660000000000,
				},
			},
			1: &schema.ShardFlushTimes{
				ByResolution: map[int64]int64{
					1000000000: 3658000000000,
				},
			},
			2: &schema.ShardFlushTimes{
				ByResolution: map[int64]int64{
					3600000000000: 3600000000000,
				},
			},
		},
	}

	testFlushTimes2 = &schema.ShardSetFlushTimes{
		ByShard: map[uint32]*schema.ShardFlushTimes{
			0: &schema.ShardFlushTimes{
				ByResolution: map[int64]int64{
					1000000000:  3669000000000,
					60000000000: 3660000000000,
				},
				Tombstoned: false,
			},
			1: &schema.ShardFlushTimes{
				ByResolution: map[int64]int64{
					1000000000: 3658000000000,
				},
				Tombstoned: true,
			},
			2: &schema.ShardFlushTimes{
				ByResolution: map[int64]int64{
					3600000000000: 3600000000000,
				},
				Tombstoned: true,
			},
			3: &schema.ShardFlushTimes{
				ByResolution: map[int64]int64{
					3600000000000: 7200000000000,
				},
				Tombstoned: false,
			},
		},
	}
)

func TestLeaderFlushManagerInit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	now := time.Unix(1234, 0)
	nowFn := func() time.Time { return now }
	doneCh := make(chan struct{})
	opts := NewFlushManagerOptions().SetJitterEnabled(false)
	mgr := newLeaderFlushManager(doneCh, opts).(*leaderFlushManager)
	mgr.nowFn = nowFn

	mgr.Init(testFlushBuckets(ctrl))
	expectedFlushTimes := []flushMetadata{
		{timeNanos: 1235000000000, bucketIdx: 0},
		{timeNanos: 1294000000000, bucketIdx: 1},
		{timeNanos: 4834000000000, bucketIdx: 2},
	}
	require.Equal(t, flushMetadataHeap(expectedFlushTimes), mgr.flushTimes)
}

func TestLeaderFlushManagerPrepareNoFlushNoPersist(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		storeAsyncCount int
		now             = time.Unix(1234, 0)
		nowFn           = func() time.Time { return now }
		doneCh          = make(chan struct{})
	)

	flushTimesManager := NewMockFlushTimesManager(ctrl)
	opts := NewFlushManagerOptions().SetJitterEnabled(false)
	mgr := newLeaderFlushManager(doneCh, opts).(*leaderFlushManager)
	mgr.nowFn = nowFn
	mgr.lastPersistAtNanos = now.UnixNano()
	mgr.flushTimesManager = flushTimesManager

	buckets := testFlushBuckets(ctrl)
	mgr.Init(buckets)
	now = now.Add(100 * time.Millisecond)
	flushTask, dur := mgr.Prepare(buckets)
	require.Nil(t, flushTask)
	require.Equal(t, 900*time.Millisecond, dur)
	require.Equal(t, 0, storeAsyncCount)
}

func TestLeaderFlushManagerPrepareNoFlushWithPersistOnce(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		storeAsyncCount int
		stored          *schema.ShardSetFlushTimes
		now             = time.Unix(1234, 0)
		nowFn           = func() time.Time { return now }
		doneCh          = make(chan struct{})
	)

	flushTimesManager := NewMockFlushTimesManager(ctrl)
	flushTimesManager.EXPECT().
		StoreAsync(gomock.Any()).
		DoAndReturn(func(value *schema.ShardSetFlushTimes) error {
			storeAsyncCount++
			stored = value
			return nil
		})

	opts := NewFlushManagerOptions().
		SetJitterEnabled(false).
		SetFlushTimesPersistEvery(time.Second)
	mgr := newLeaderFlushManager(doneCh, opts).(*leaderFlushManager)
	mgr.nowFn = nowFn
	mgr.lastPersistAtNanos = now.Add(-2 * time.Second).UnixNano()
	mgr.flushedSincePersist = true
	mgr.flushTimesManager = flushTimesManager

	buckets := testFlushBuckets(ctrl)
	mgr.Init(buckets)
	flushTask, dur := mgr.Prepare(buckets)
	require.Nil(t, flushTask)
	require.Equal(t, time.Second, dur)
	require.False(t, mgr.flushedSincePersist)
	require.Equal(t, 1, storeAsyncCount)
	require.Equal(t, testFlushTimes, stored)
}

func TestLeaderFlushManagerPrepareNoFlushWithPersistTwice(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		storeAsyncCount int
		stored          *schema.ShardSetFlushTimes
		now             = time.Unix(1234, 0)
		nowFn           = func() time.Time { return now }
		doneCh          = make(chan struct{})
	)

	flushTimesManager := NewMockFlushTimesManager(ctrl)
	flushTimesManager.EXPECT().
		StoreAsync(gomock.Any()).
		DoAndReturn(func(value *schema.ShardSetFlushTimes) error {
			storeAsyncCount++
			stored = value
			return nil
		}).
		Times(2)

	opts := NewFlushManagerOptions().
		SetJitterEnabled(false).
		SetFlushTimesPersistEvery(time.Second)
	mgr := newLeaderFlushManager(doneCh, opts).(*leaderFlushManager)
	mgr.nowFn = nowFn
	mgr.lastPersistAtNanos = now.Add(-2 * time.Second).UnixNano()
	mgr.flushedSincePersist = true
	mgr.flushTimesManager = flushTimesManager

	// Persist for the first time.
	buckets := testFlushBuckets(ctrl)
	mgr.Init(buckets)
	flushTask, dur := mgr.Prepare(buckets)
	require.Nil(t, flushTask)
	require.Equal(t, time.Second, dur)
	require.False(t, mgr.flushedSincePersist)
	require.Equal(t, 1, storeAsyncCount)
	require.Equal(t, testFlushTimes, stored)

	// Reset state in preparation for the second persistence.
	mgr.flushedSincePersist = true
	mgr.lastPersistAtNanos = now.Add(-2 * time.Second).UnixNano()

	// Persist for the second time.
	buckets2 := testFlushBuckets2(ctrl)
	mgr.Init(buckets2)
	flushTask, dur = mgr.Prepare(buckets2)
	require.Nil(t, flushTask)
	require.Equal(t, time.Second, dur)
	require.False(t, mgr.flushedSincePersist)
	require.Equal(t, 2, storeAsyncCount)
	require.Equal(t, testFlushTimes2, stored)
}

func TestLeaderFlushManagerPrepareWithFlushAndPersist(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		storeAsyncCount int
		stored          *schema.ShardSetFlushTimes
		now             = time.Unix(1234, 0)
		nowFn           = func() time.Time { return now }
		doneCh          = make(chan struct{})
	)

	flushTimesManager := NewMockFlushTimesManager(ctrl)
	flushTimesManager.EXPECT().
		StoreAsync(gomock.Any()).
		DoAndReturn(func(value *schema.ShardSetFlushTimes) error {
			storeAsyncCount++
			stored = value
			return nil
		})

	opts := NewFlushManagerOptions().
		SetJitterEnabled(false).
		SetFlushTimesPersistEvery(time.Second)
	mgr := newLeaderFlushManager(doneCh, opts).(*leaderFlushManager)
	mgr.nowFn = nowFn
	mgr.lastPersistAtNanos = now.UnixNano()
	mgr.flushedSincePersist = true
	mgr.flushTimesManager = flushTimesManager

	buckets := testFlushBuckets(ctrl)
	mgr.Init(buckets)
	now = now.Add(2 * time.Second)
	flushTask, dur := mgr.Prepare(buckets)

	expectedFlushTimes := []flushMetadata{
		{timeNanos: 1236000000000, bucketIdx: 0},
		{timeNanos: 4834000000000, bucketIdx: 2},
		{timeNanos: 1294000000000, bucketIdx: 1},
	}
	require.NotNil(t, flushTask)
	require.Equal(t, time.Duration(0), dur)
	require.False(t, mgr.flushedSincePersist)
	task := flushTask.(*leaderFlushTask)
	require.Equal(t, buckets[0].flushers, task.flushers)
	require.Equal(t, flushMetadataHeap(expectedFlushTimes), mgr.flushTimes)
	require.Equal(t, 1, storeAsyncCount)
	require.Equal(t, testFlushTimes, stored)
}

func TestLeaderFlushManagerOnBucketAdded(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	now := time.Unix(1234, 0)
	nowFn := func() time.Time { return now }
	doneCh := make(chan struct{})
	opts := NewFlushManagerOptions().SetJitterEnabled(false)
	mgr := newLeaderFlushManager(doneCh, opts).(*leaderFlushManager)
	mgr.nowFn = nowFn

	buckets := testFlushBuckets(ctrl)
	mgr.OnBucketAdded(0, buckets[0])
	expectedFlushTimes := []flushMetadata{
		{timeNanos: 1235000000000, bucketIdx: 0},
	}
	require.Equal(t, flushMetadataHeap(expectedFlushTimes), mgr.flushTimes)
}

func TestLeaderFlushManagerComputeNextFlushNanosJitterDisabled(t *testing.T) {
	now := time.Unix(1234, 0)
	nowFn := func() time.Time { return now }
	doneCh := make(chan struct{})
	opts := NewFlushManagerOptions().
		SetJitterEnabled(false)
	mgr := newLeaderFlushManager(doneCh, opts).(*leaderFlushManager)
	mgr.nowFn = nowFn

	for _, input := range []struct {
		interval      time.Duration
		expectedNanos int64
	}{
		{interval: time.Second, expectedNanos: time.Unix(1235, 0).UnixNano()},
		{interval: 10 * time.Second, expectedNanos: time.Unix(1244, 0).UnixNano()},
		{interval: time.Minute, expectedNanos: time.Unix(1294, 0).UnixNano()},
	} {
		require.Equal(t, input.expectedNanos, mgr.computeNextFlushNanos(input.interval))
	}
}

func TestLeaderFlushManagerComputeNextFlushNanosJitterEnabled(t *testing.T) {
	now := time.Unix(1234, 0)
	nowFn := func() time.Time { return now }
	maxJitterFn := func(interval time.Duration) time.Duration {
		return time.Duration(0.5 * float64(interval))
	}
	randFn := func(n int64) int64 { return int64(0.5 * float64(n)) }
	doneCh := make(chan struct{})
	opts := NewFlushManagerOptions().
		SetJitterEnabled(true).
		SetMaxJitterFn(maxJitterFn)
	mgr := newLeaderFlushManager(doneCh, opts).(*leaderFlushManager)
	mgr.nowFn = nowFn
	mgr.randFn = randFn

	for _, input := range []struct {
		interval      time.Duration
		expectedNanos int64
	}{
		{interval: time.Second, expectedNanos: time.Unix(1234, 250000000).UnixNano()},
		{interval: 10 * time.Second, expectedNanos: time.Unix(1242, 500000000).UnixNano()},
		{interval: time.Minute, expectedNanos: time.Unix(1275, 0).UnixNano()},
	} {
		require.Equal(t, input.expectedNanos, mgr.computeNextFlushNanos(input.interval))
	}
}

func TestLeaderFlushTaskRunShardsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var flushRequest *FlushRequest
	flushers := []PeriodicFlusher{NewMockPeriodicFlusher(ctrl)}
	doneCh := make(chan struct{})
	errShards := errors.New("error getting shards")
	placementManager := NewMockPlacementManager(ctrl)
	placementManager.EXPECT().Shards().Return(nil, errShards)

	opts := NewFlushManagerOptions()
	mgr := newLeaderFlushManager(doneCh, opts).(*leaderFlushManager)
	mgr.placementManager = placementManager
	flushTask := &leaderFlushTask{
		mgr:      mgr,
		duration: tally.NoopScope.Timer("foo"),
		flushers: flushers,
	}
	flushTask.Run()
	require.Nil(t, flushRequest)
}

func TestLeaderFlushTaskRunShardNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var flushRequest *FlushRequest
	flusher := NewMockPeriodicFlusher(ctrl)
	flusher.EXPECT().Shard().Return(uint32(2)).AnyTimes()
	flusher.EXPECT().
		Flush(gomock.Any()).
		Do(func(req FlushRequest) {
			flushRequest = &req
		})

	flushers := []PeriodicFlusher{flusher}
	doneCh := make(chan struct{})
	placementManager := NewMockPlacementManager(ctrl)
	placementManager.EXPECT().Shards().Return(shard.NewShards(nil), nil)

	opts := NewFlushManagerOptions()
	mgr := newLeaderFlushManager(doneCh, opts).(*leaderFlushManager)
	mgr.placementManager = placementManager
	flushTask := &leaderFlushTask{
		mgr:      mgr,
		duration: tally.NoopScope.Timer("foo"),
		flushers: flushers,
	}
	flushTask.Run()

	expected := FlushRequest{
		CutoverNanos:      0,
		CutoffNanos:       0,
		BufferAfterCutoff: mgr.maxBufferSize,
	}
	require.Equal(t, expected, *flushRequest)
}

func TestLeaderFlushTaskRunWithFlushes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	requests := make([]FlushRequest, 2)
	flusher1 := NewMockPeriodicFlusher(ctrl)
	flusher1.EXPECT().Shard().Return(uint32(0)).AnyTimes()
	flusher1.EXPECT().
		Flush(gomock.Any()).
		Do(func(req FlushRequest) {
			requests[0] = req
		})
	flusher2 := NewMockPeriodicFlusher(ctrl)
	flusher2.EXPECT().Shard().Return(uint32(1)).AnyTimes()
	flusher2.EXPECT().
		Flush(gomock.Any()).
		Do(func(req FlushRequest) {
			requests[1] = req
		})

	flushers := []PeriodicFlusher{flusher1, flusher2}
	doneCh := make(chan struct{})
	shards := []shard.Shard{
		shard.NewShard(0).SetState(shard.Initializing).SetCutoverNanos(5000).SetCutoffNanos(20000),
		shard.NewShard(1).SetState(shard.Initializing).SetCutoverNanos(5500).SetCutoffNanos(25000),
	}
	placementManager := NewMockPlacementManager(ctrl)
	placementManager.EXPECT().
		Shards().
		Return(shard.NewShards(shards), nil)

	opts := NewFlushManagerOptions().SetJitterEnabled(false)
	mgr := newLeaderFlushManager(doneCh, opts).(*leaderFlushManager)
	mgr.placementManager = placementManager
	flushTask := &leaderFlushTask{
		mgr:      mgr,
		duration: tally.NoopScope.Timer("foo"),
		flushers: flushers,
	}
	flushTask.Run()

	expected := []FlushRequest{
		{
			CutoverNanos:      5000,
			CutoffNanos:       20000,
			BufferAfterCutoff: mgr.maxBufferSize,
		},
		{
			CutoverNanos:      5500,
			CutoffNanos:       25000,
			BufferAfterCutoff: mgr.maxBufferSize,
		},
	}
	require.Equal(t, expected, requests)
}

func testFlushBuckets(ctrl *gomock.Controller) []*flushBucket {
	flusher1 := NewMockPeriodicFlusher(ctrl)
	flusher1.EXPECT().Shard().Return(uint32(0)).AnyTimes()
	flusher1.EXPECT().Resolution().Return(time.Second).AnyTimes()
	flusher1.EXPECT().FlushInterval().Return(time.Second).AnyTimes()
	flusher1.EXPECT().LastFlushedNanos().Return(int64(3663000000000)).AnyTimes()

	flusher2 := NewMockPeriodicFlusher(ctrl)
	flusher2.EXPECT().Shard().Return(uint32(1)).AnyTimes()
	flusher2.EXPECT().Resolution().Return(time.Second).AnyTimes()
	flusher2.EXPECT().FlushInterval().Return(time.Second).AnyTimes()
	flusher2.EXPECT().LastFlushedNanos().Return(int64(3658000000000)).AnyTimes()

	flusher3 := NewMockPeriodicFlusher(ctrl)
	flusher3.EXPECT().Shard().Return(uint32(0)).AnyTimes()
	flusher3.EXPECT().Resolution().Return(time.Minute).AnyTimes()
	flusher3.EXPECT().FlushInterval().Return(time.Minute).AnyTimes()
	flusher3.EXPECT().LastFlushedNanos().Return(int64(3660000000000)).AnyTimes()

	flusher4 := NewMockPeriodicFlusher(ctrl)
	flusher4.EXPECT().Shard().Return(uint32(2)).AnyTimes()
	flusher4.EXPECT().Resolution().Return(time.Hour).AnyTimes()
	flusher4.EXPECT().FlushInterval().Return(time.Hour).AnyTimes()
	flusher4.EXPECT().LastFlushedNanos().Return(int64(3600000000000)).AnyTimes()

	return []*flushBucket{
		&flushBucket{
			interval: time.Second,
			flushers: []PeriodicFlusher{flusher1, flusher2},
		},
		&flushBucket{
			interval: time.Minute,
			flushers: []PeriodicFlusher{flusher3},
		},
		&flushBucket{
			interval: time.Hour,
			flushers: []PeriodicFlusher{flusher4},
		},
	}
}

func testFlushBuckets2(ctrl *gomock.Controller) []*flushBucket {
	flusher1 := NewMockPeriodicFlusher(ctrl)
	flusher1.EXPECT().Shard().Return(uint32(0)).AnyTimes()
	flusher1.EXPECT().Resolution().Return(time.Second).AnyTimes()
	flusher1.EXPECT().FlushInterval().Return(time.Second).AnyTimes()
	flusher1.EXPECT().LastFlushedNanos().Return(int64(3669000000000)).AnyTimes()

	flusher2 := NewMockPeriodicFlusher(ctrl)
	flusher2.EXPECT().Shard().Return(uint32(3)).AnyTimes()
	flusher2.EXPECT().Resolution().Return(time.Hour).AnyTimes()
	flusher2.EXPECT().FlushInterval().Return(time.Hour).AnyTimes()
	flusher2.EXPECT().LastFlushedNanos().Return(int64(7200000000000)).AnyTimes()

	return []*flushBucket{
		&flushBucket{
			interval: time.Second,
			flushers: []PeriodicFlusher{flusher1},
		},
		&flushBucket{
			interval: time.Hour,
			flushers: []PeriodicFlusher{flusher2},
		},
	}
}
