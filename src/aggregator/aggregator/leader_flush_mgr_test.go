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

	schema "github.com/m3db/m3/src/aggregator/generated/proto/flush"
	"github.com/m3db/m3/src/cluster/shard"

	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

var (
	testFlushTimes = &schema.ShardSetFlushTimes{
		ByShard: map[uint32]*schema.ShardFlushTimes{
			0: &schema.ShardFlushTimes{
				StandardByResolution: map[int64]int64{
					1000000000:  3663000000000,
					60000000000: 3660000000000,
				},
				ForwardedByResolution: map[int64]*schema.ForwardedFlushTimesForResolution{
					1000000000: &schema.ForwardedFlushTimesForResolution{
						ByNumForwardedTimes: map[int32]int64{
							1: 3663000000000,
						},
					},
					60000000000: &schema.ForwardedFlushTimesForResolution{
						ByNumForwardedTimes: map[int32]int64{
							2: 3660000000000,
							3: 3600000000000,
						},
					},
				},
			},
			1: &schema.ShardFlushTimes{
				StandardByResolution: map[int64]int64{
					1000000000: 3658000000000,
				},
				ForwardedByResolution: map[int64]*schema.ForwardedFlushTimesForResolution{
					1000000000: &schema.ForwardedFlushTimesForResolution{
						ByNumForwardedTimes: map[int32]int64{
							1: 3658000000000,
						},
					},
				},
			},
			2: &schema.ShardFlushTimes{
				StandardByResolution: map[int64]int64{
					3600000000000: 3600000000000,
				},
			},
		},
	}

	testFlushTimes2 = &schema.ShardSetFlushTimes{
		ByShard: map[uint32]*schema.ShardFlushTimes{
			0: &schema.ShardFlushTimes{
				StandardByResolution: map[int64]int64{
					1000000000:  3669000000000,
					60000000000: 3660000000000,
				},
				TimedByResolution: map[int64]int64{
					1000000000: 3600000000000,
				},
				ForwardedByResolution: map[int64]*schema.ForwardedFlushTimesForResolution{
					1000000000: &schema.ForwardedFlushTimesForResolution{
						ByNumForwardedTimes: map[int32]int64{
							1: 3681000000000,
						},
					},
					60000000000: &schema.ForwardedFlushTimesForResolution{
						ByNumForwardedTimes: map[int32]int64{
							2: 3660000000000,
							3: 3600000000000,
						},
					},
				},
				Tombstoned: false,
			},
			1: &schema.ShardFlushTimes{
				StandardByResolution: map[int64]int64{
					1000000000: 3658000000000,
				},
				ForwardedByResolution: map[int64]*schema.ForwardedFlushTimesForResolution{
					1000000000: &schema.ForwardedFlushTimesForResolution{
						ByNumForwardedTimes: map[int32]int64{
							1: 3658000000000,
						},
					},
				},
				Tombstoned: true,
			},
			2: &schema.ShardFlushTimes{
				StandardByResolution: map[int64]int64{
					3600000000000: 3600000000000,
				},
				Tombstoned: true,
			},
			3: &schema.ShardFlushTimes{
				StandardByResolution: map[int64]int64{
					3600000000000: 7200000000000,
				},
				Tombstoned: false,
			},
			4: &schema.ShardFlushTimes{
				ForwardedByResolution: map[int64]*schema.ForwardedFlushTimesForResolution{
					60000000000: &schema.ForwardedFlushTimesForResolution{
						ByNumForwardedTimes: map[int32]int64{
							2: 3658000000000,
						},
					},
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
	opts := NewFlushManagerOptions()
	mgr := newLeaderFlushManager(doneCh, opts).(*leaderFlushManager)
	mgr.nowFn = nowFn

	mgr.Init(testFlushBuckets(ctrl))

	expectedFlushTimes := []flushMetadata{
		{timeNanos: 60000000000, bucketIdx: 2},
		{timeNanos: 1200000000000, bucketIdx: 5},
		{timeNanos: 1201000000000, bucketIdx: 4},
		{timeNanos: 1212000000000, bucketIdx: 1},
		{timeNanos: 1234100000000, bucketIdx: 3},
		{timeNanos: 1234250000000, bucketIdx: 0},
	}
	validateFlushMetadataHeap(t, expectedFlushTimes, mgr.flushTimes)
}

func TestLeaderFlushManagerOnFlusherAdded(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	now := time.Unix(1234, 0)
	nowFn := func() time.Time { return now }
	doneCh := make(chan struct{})
	opts := NewFlushManagerOptions()
	mgr := newLeaderFlushManager(doneCh, opts).(*leaderFlushManager)
	mgr.nowFn = nowFn

	buckets := testFlushBuckets(ctrl)
	mgr.Init(buckets)

	expectedFlushTimes := []flushMetadata{
		{timeNanos: 60000000000, bucketIdx: 2},
		{timeNanos: 1200000000000, bucketIdx: 5},
		{timeNanos: 1201000000000, bucketIdx: 4},
		{timeNanos: 1212000000000, bucketIdx: 1},
		{timeNanos: 1234100000000, bucketIdx: 3},
		{timeNanos: 1234250000000, bucketIdx: 0},
	}
	validateFlushMetadataHeap(t, expectedFlushTimes, mgr.flushTimes)

	// Add a new flusher to bucket 1 without moving time forward
	// and expect no change to the flush times.
	mgr.OnFlusherAdded(1, buckets[1], nil)
	validateFlushMetadataHeap(t, expectedFlushTimes, mgr.flushTimes)

	// Pop the flush metadata, update its next flush times, and push it back to the heap
	// to simulate the next flush.
	metadata := mgr.flushTimes.Pop()
	metadata.timeNanos = metadata.timeNanos + buckets[metadata.bucketIdx].interval.Nanoseconds()
	mgr.flushTimes.Push(metadata)
	expectedFlushTimes = []flushMetadata{
		{timeNanos: 1200000000000, bucketIdx: 5},
		{timeNanos: 1201000000000, bucketIdx: 4},
		{timeNanos: 1212000000000, bucketIdx: 1},
		{timeNanos: 1234100000000, bucketIdx: 3},
		{timeNanos: 1234250000000, bucketIdx: 0},
		{timeNanos: 3660000000000, bucketIdx: 2},
	}
	validateFlushMetadataHeap(t, expectedFlushTimes, mgr.flushTimes)

	// Move time forward slightly and add a new flusher to bucket 2 and expect bucket 2
	// to have an updated next flush time.
	now = now.Add(time.Minute)
	mgr.OnFlusherAdded(2, buckets[2], nil)
	expectedFlushTimes = []flushMetadata{
		{timeNanos: 60000000000, bucketIdx: 2},
		{timeNanos: 1200000000000, bucketIdx: 5},
		{timeNanos: 1201000000000, bucketIdx: 4},
		{timeNanos: 1212000000000, bucketIdx: 1},
		{timeNanos: 1234100000000, bucketIdx: 3},
		{timeNanos: 1234250000000, bucketIdx: 0},
	}
	validateFlushMetadataHeap(t, expectedFlushTimes, mgr.flushTimes)
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
	mgr.flushTimesManager = flushTimesManager

	buckets := testFlushBuckets(ctrl)
	mgr.Init(buckets)

	now = time.Unix(59, 100000000)
	mgr.lastPersistAtNanos = now.UnixNano()
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
	mgr.flushedSincePersist = true
	mgr.flushTimesManager = flushTimesManager

	buckets := testFlushBuckets(ctrl)
	mgr.Init(buckets)

	now = time.Unix(10, 0)
	mgr.lastPersistAtNanos = now.Add(-2 * time.Second).UnixNano()
	flushTask, dur := mgr.Prepare(buckets)
	require.Nil(t, flushTask)
	require.Equal(t, time.Second, dur)
	require.False(t, mgr.flushedSincePersist)
	require.Equal(t, 1, storeAsyncCount)
	validateShardSetFlushTimes(t, testFlushTimes, stored)
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

	now = time.Unix(10, 0)
	mgr.lastPersistAtNanos = now.Add(-2 * time.Second).UnixNano()
	flushTask, dur := mgr.Prepare(buckets)
	require.Nil(t, flushTask)
	require.Equal(t, time.Second, dur)
	require.False(t, mgr.flushedSincePersist)
	require.Equal(t, 1, storeAsyncCount)
	validateShardSetFlushTimes(t, testFlushTimes, stored)

	// Reset state in preparation for the second persistence.
	now = time.Unix(1234, 0)

	// Persist for the second time.
	buckets2 := testFlushBuckets2(ctrl)
	mgr.Init(buckets2)

	now = time.Unix(10, 0)
	mgr.flushedSincePersist = true
	mgr.lastPersistAtNanos = now.Add(-2 * time.Second).UnixNano()
	flushTask, dur = mgr.Prepare(buckets2)
	require.Nil(t, flushTask)
	require.Equal(t, time.Second, dur)
	require.False(t, mgr.flushedSincePersist)
	require.Equal(t, 2, storeAsyncCount)
	validateShardSetFlushTimes(t, testFlushTimes2, stored)
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

	// Validate flush times persisted match expectation.
	require.NotNil(t, flushTask)
	require.Equal(t, time.Duration(0), dur)
	require.False(t, mgr.flushedSincePersist)
	task := flushTask.(*leaderFlushTask)
	require.Equal(t, buckets[2].flushers, task.flushers)
	require.Equal(t, 1, storeAsyncCount)
	validateShardSetFlushTimes(t, testFlushTimes, stored)

	// Validate flush metadatas in the heap match expectation.
	expectedFlushTimes := []flushMetadata{
		{timeNanos: 1200000000000, bucketIdx: 5},
		{timeNanos: 1201000000000, bucketIdx: 4},
		{timeNanos: 1212000000000, bucketIdx: 1},
		{timeNanos: 1234100000000, bucketIdx: 3},
		{timeNanos: 1234250000000, bucketIdx: 0},
		{timeNanos: 3660000000000, bucketIdx: 2},
	}
	validateFlushMetadataHeap(t, expectedFlushTimes, mgr.flushTimes)
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
		{timeNanos: 1234250000000, bucketIdx: 0},
	}
	validateFlushMetadataHeap(t, expectedFlushTimes, mgr.flushTimes)
}

func TestCloneFlushTimesByShard(t *testing.T) {
	cloned := cloneFlushTimesByShard(testFlushTimes2.ByShard)
	actual := &schema.ShardSetFlushTimes{ByShard: cloned}
	validateShardSetFlushTimes(t, testFlushTimes2, actual)

	// Assert that mutating a clone does not alter the original data.
	cloned2 := cloneFlushTimesByShard(testFlushTimes2.ByShard)
	require.Equal(t, cloned, cloned2)
	cloned2[0].StandardByResolution[1000000000] = 1000
	cloned2[0].TimedByResolution[1000000000] = 2000
	cloned2[1].ForwardedByResolution[1000000000].ByNumForwardedTimes[1] = 2000
	cloned2[1].ForwardedByResolution[1000000000].ByNumForwardedTimes[2] = 3000
	cloned2[3] = &schema.ShardFlushTimes{
		StandardByResolution: map[int64]int64{
			3600000000000: 3600000000000,
		},
	}
	require.NotEqual(t, cloned, cloned2)
	validateShardSetFlushTimes(t, testFlushTimes2, actual)
}

func TestLeaderFlushTaskRunShardsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var flushRequest *flushRequest
	flushers := []flushingMetricList{NewMockflushingMetricList(ctrl)}
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

	var request *flushRequest
	flusher := NewMockflushingMetricList(ctrl)
	flusher.EXPECT().Shard().Return(uint32(2)).AnyTimes()
	flusher.EXPECT().
		Flush(gomock.Any()).
		Do(func(req flushRequest) {
			request = &req
		})

	flushers := []flushingMetricList{flusher}
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

	expected := flushRequest{
		CutoverNanos:      0,
		CutoffNanos:       0,
		BufferAfterCutoff: mgr.maxBufferSize,
	}
	require.Equal(t, expected, *request)
}

func TestLeaderFlushTaskRunWithFlushes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	requests := make([]flushRequest, 2)
	flusher1 := NewMockflushingMetricList(ctrl)
	flusher1.EXPECT().Shard().Return(uint32(0)).AnyTimes()
	flusher1.EXPECT().
		Flush(gomock.Any()).
		Do(func(req flushRequest) {
			requests[0] = req
		})
	flusher2 := NewMockflushingMetricList(ctrl)
	flusher2.EXPECT().Shard().Return(uint32(1)).AnyTimes()
	flusher2.EXPECT().
		Flush(gomock.Any()).
		Do(func(req flushRequest) {
			requests[1] = req
		})

	flushers := []flushingMetricList{flusher1, flusher2}
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

	expected := []flushRequest{
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

func validateShardSetFlushTimes(t *testing.T, expected, actual *schema.ShardSetFlushTimes) {
	standardFlushTimesComparer := cmp.Comparer(func(a, b map[int64]int64) bool {
		if len(a) != len(b) {
			return false
		}
		for k, va := range a {
			vb, exists := b[k]
			if !exists {
				return false
			}
			if va != vb {
				return false
			}
		}
		return true
	})
	forwardFlushTimesComparer := cmp.Comparer(func(a, b map[int64]*schema.ForwardedFlushTimesForResolution) bool {
		if len(a) != len(b) {
			return false
		}
		for k, va := range a {
			vb, exists := b[k]
			if !exists {
				return false
			}
			if !cmp.Equal(va, vb) {
				return false
			}
		}
		return true
	})
	require.True(t, cmp.Equal(expected, actual, standardFlushTimesComparer, forwardFlushTimesComparer))
}

func validateFlushMetadataHeap(t *testing.T, expected []flushMetadata, actual flushMetadataHeap) {
	cloned := make(flushMetadataHeap, len(actual))
	copy(cloned, actual)

	var res []flushMetadata
	for cloned.Len() > 0 {
		res = append(res, cloned.Min())
		cloned.Pop()
	}
	require.Equal(t, expected, res)
}

func testFlushBuckets(ctrl *gomock.Controller) []*flushBucket {
	standardFlusher1 := NewMockflushingMetricList(ctrl)
	standardFlusher1.EXPECT().Shard().Return(uint32(0)).AnyTimes()
	standardFlusher1.EXPECT().FlushInterval().Return(time.Second).AnyTimes()
	standardFlusher1.EXPECT().LastFlushedNanos().Return(int64(3663000000000)).AnyTimes()

	standardFlusher2 := NewMockflushingMetricList(ctrl)
	standardFlusher2.EXPECT().Shard().Return(uint32(1)).AnyTimes()
	standardFlusher2.EXPECT().FlushInterval().Return(time.Second).AnyTimes()
	standardFlusher2.EXPECT().LastFlushedNanos().Return(int64(3658000000000)).AnyTimes()

	standardFlusher3 := NewMockflushingMetricList(ctrl)
	standardFlusher3.EXPECT().Shard().Return(uint32(0)).AnyTimes()
	standardFlusher3.EXPECT().FlushInterval().Return(time.Minute).AnyTimes()
	standardFlusher3.EXPECT().LastFlushedNanos().Return(int64(3660000000000)).AnyTimes()

	standardFlusher4 := NewMockflushingMetricList(ctrl)
	standardFlusher4.EXPECT().Shard().Return(uint32(2)).AnyTimes()
	standardFlusher4.EXPECT().FlushInterval().Return(time.Hour).AnyTimes()
	standardFlusher4.EXPECT().LastFlushedNanos().Return(int64(3600000000000)).AnyTimes()

	forwardedFlusher1 := NewMockflushingMetricList(ctrl)
	forwardedFlusher1.EXPECT().Shard().Return(uint32(0)).AnyTimes()
	forwardedFlusher1.EXPECT().FlushInterval().Return(time.Second).AnyTimes()
	forwardedFlusher1.EXPECT().LastFlushedNanos().Return(int64(3663000000000)).AnyTimes()

	forwardedFlusher2 := NewMockflushingMetricList(ctrl)
	forwardedFlusher2.EXPECT().Shard().Return(uint32(1)).AnyTimes()
	forwardedFlusher2.EXPECT().FlushInterval().Return(time.Second).AnyTimes()
	forwardedFlusher2.EXPECT().LastFlushedNanos().Return(int64(3658000000000)).AnyTimes()

	forwardedFlusher3 := NewMockflushingMetricList(ctrl)
	forwardedFlusher3.EXPECT().Shard().Return(uint32(0)).AnyTimes()
	forwardedFlusher3.EXPECT().FlushInterval().Return(time.Minute).AnyTimes()
	forwardedFlusher3.EXPECT().LastFlushedNanos().Return(int64(3660000000000)).AnyTimes()

	forwardedFlusher4 := NewMockflushingMetricList(ctrl)
	forwardedFlusher4.EXPECT().Shard().Return(uint32(0)).AnyTimes()
	forwardedFlusher4.EXPECT().FlushInterval().Return(time.Minute).AnyTimes()
	forwardedFlusher4.EXPECT().LastFlushedNanos().Return(int64(3600000000000)).AnyTimes()

	return []*flushBucket{
		// Standard flushing metric lists.
		&flushBucket{
			bucketID: standardMetricListID{resolution: time.Second}.toMetricListID(),
			interval: time.Second,
			offset:   250 * time.Millisecond,
			flushers: []flushingMetricList{standardFlusher1, standardFlusher2},
		},
		&flushBucket{
			bucketID: standardMetricListID{resolution: time.Minute}.toMetricListID(),
			interval: time.Minute,
			offset:   12 * time.Second,
			flushers: []flushingMetricList{standardFlusher3},
		},
		&flushBucket{
			bucketID: standardMetricListID{resolution: time.Hour}.toMetricListID(),
			interval: time.Hour,
			offset:   time.Minute,
			flushers: []flushingMetricList{standardFlusher4},
		},
		// Forwarded flushing metric lists.
		&flushBucket{
			bucketID: forwardedMetricListID{resolution: time.Second, numForwardedTimes: 1}.toMetricListID(),
			interval: time.Second,
			offset:   100 * time.Millisecond,
			flushers: []flushingMetricList{forwardedFlusher1, forwardedFlusher2},
		},
		&flushBucket{
			bucketID: forwardedMetricListID{resolution: time.Minute, numForwardedTimes: 2}.toMetricListID(),
			interval: time.Minute,
			offset:   time.Second,
			flushers: []flushingMetricList{forwardedFlusher3},
		},
		&flushBucket{
			bucketID: forwardedMetricListID{resolution: time.Minute, numForwardedTimes: 3}.toMetricListID(),
			interval: time.Minute,
			offset:   0,
			flushers: []flushingMetricList{forwardedFlusher4},
		},
	}
}

func testFlushBuckets2(ctrl *gomock.Controller) []*flushBucket {
	standardFlusher1 := NewMockflushingMetricList(ctrl)
	standardFlusher1.EXPECT().Shard().Return(uint32(0)).AnyTimes()
	standardFlusher1.EXPECT().FlushInterval().Return(time.Second).AnyTimes()
	standardFlusher1.EXPECT().LastFlushedNanos().Return(int64(3669000000000)).AnyTimes()

	standardFlusher2 := NewMockflushingMetricList(ctrl)
	standardFlusher2.EXPECT().Shard().Return(uint32(3)).AnyTimes()
	standardFlusher2.EXPECT().FlushInterval().Return(time.Hour).AnyTimes()
	standardFlusher2.EXPECT().LastFlushedNanos().Return(int64(7200000000000)).AnyTimes()

	timedFlusher1 := NewMockflushingMetricList(ctrl)
	timedFlusher1.EXPECT().Shard().Return(uint32(0)).AnyTimes()
	timedFlusher1.EXPECT().FlushInterval().Return(time.Second).AnyTimes()
	timedFlusher1.EXPECT().LastFlushedNanos().Return(int64(3600000000000)).AnyTimes()

	forwardedFlusher1 := NewMockflushingMetricList(ctrl)
	forwardedFlusher1.EXPECT().Shard().Return(uint32(0)).AnyTimes()
	forwardedFlusher1.EXPECT().FlushInterval().Return(time.Second).AnyTimes()
	forwardedFlusher1.EXPECT().LastFlushedNanos().Return(int64(3681000000000)).AnyTimes()

	forwardedFlusher2 := NewMockflushingMetricList(ctrl)
	forwardedFlusher2.EXPECT().Shard().Return(uint32(4)).AnyTimes()
	forwardedFlusher2.EXPECT().FlushInterval().Return(time.Minute).AnyTimes()
	forwardedFlusher2.EXPECT().LastFlushedNanos().Return(int64(3658000000000)).AnyTimes()

	return []*flushBucket{
		&flushBucket{
			bucketID: standardMetricListID{resolution: time.Second}.toMetricListID(),
			interval: time.Second,
			offset:   250 * time.Millisecond,
			flushers: []flushingMetricList{standardFlusher1},
		},
		&flushBucket{
			bucketID: standardMetricListID{resolution: time.Hour}.toMetricListID(),
			interval: time.Hour,
			offset:   time.Minute,
			flushers: []flushingMetricList{standardFlusher2},
		},
		&flushBucket{
			bucketID: timedMetricListID{resolution: time.Second}.toMetricListID(),
			interval: time.Second,
			offset:   250 * time.Millisecond,
			flushers: []flushingMetricList{timedFlusher1},
		},
		&flushBucket{
			bucketID: forwardedMetricListID{resolution: time.Second, numForwardedTimes: 1}.toMetricListID(),
			interval: time.Second,
			offset:   100 * time.Millisecond,
			flushers: []flushingMetricList{forwardedFlusher1},
		},
		&flushBucket{
			bucketID: forwardedMetricListID{resolution: time.Minute, numForwardedTimes: 2}.toMetricListID(),
			interval: time.Minute,
			offset:   time.Second,
			flushers: []flushingMetricList{forwardedFlusher2},
		},
	}
}
