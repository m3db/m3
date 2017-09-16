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
	"github.com/uber-go/tally"

	"github.com/stretchr/testify/require"
)

var (
	testFlushBuckets = []*flushBucket{
		&flushBucket{
			interval: time.Second,
			flushers: []PeriodicFlusher{
				&mockFlusher{
					shard:            0,
					resolution:       time.Second,
					flushInterval:    time.Second,
					lastFlushedNanos: 3663000000000,
				},
				&mockFlusher{
					shard:            1,
					resolution:       time.Second,
					flushInterval:    time.Second,
					lastFlushedNanos: 3668000000000,
				},
			},
		},
		&flushBucket{
			interval: time.Minute,
			flushers: []PeriodicFlusher{
				&mockFlusher{
					shard:            0,
					resolution:       time.Minute,
					flushInterval:    time.Minute,
					lastFlushedNanos: 3660000000000,
				},
			},
		},
		&flushBucket{
			interval: time.Hour,
			flushers: []PeriodicFlusher{
				&mockFlusher{
					shard:            2,
					resolution:       time.Hour,
					flushInterval:    time.Hour,
					lastFlushedNanos: 3600000000000,
				},
			},
		},
	}

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
					1000000000: 3668000000000,
				},
			},
			2: &schema.ShardFlushTimes{
				ByResolution: map[int64]int64{
					3600000000000: 3600000000000,
				},
			},
		},
	}
)

func TestLeaderFlushManagerInit(t *testing.T) {
	now := time.Unix(1234, 0)
	nowFn := func() time.Time { return now }
	doneCh := make(chan struct{})
	opts := NewFlushManagerOptions().SetJitterEnabled(false)
	mgr := newLeaderFlushManager(doneCh, opts).(*leaderFlushManager)
	mgr.nowFn = nowFn

	mgr.Init(testFlushBuckets)
	expectedFlushTimes := []flushMetadata{
		{timeNanos: 1235000000000, bucketIdx: 0},
		{timeNanos: 1294000000000, bucketIdx: 1},
		{timeNanos: 4834000000000, bucketIdx: 2},
	}
	require.Equal(t, flushMetadataHeap(expectedFlushTimes), mgr.flushTimes)
}

func TestLeaderFlushManagerPrepareNoFlushNoPersist(t *testing.T) {
	var (
		storeAsyncCount int
		now             = time.Unix(1234, 0)
		nowFn           = func() time.Time { return now }
		doneCh          = make(chan struct{})
	)
	opts := NewFlushManagerOptions().SetJitterEnabled(false)
	mgr := newLeaderFlushManager(doneCh, opts).(*leaderFlushManager)
	mgr.nowFn = nowFn
	mgr.lastPersistAtNanos = now.UnixNano()
	mgr.flushTimesManager = &mockFlushTimesManager{
		storeAsyncFn: func(value *schema.ShardSetFlushTimes) error {
			storeAsyncCount++
			return nil
		},
	}

	mgr.Init(testFlushBuckets)
	now = now.Add(100 * time.Millisecond)
	flushTask, dur := mgr.Prepare(testFlushBuckets)
	require.Nil(t, flushTask)
	require.Equal(t, 900*time.Millisecond, dur)
	require.Equal(t, 0, storeAsyncCount)
}

func TestLeaderFlushManagerPrepareNoFlushWithPersist(t *testing.T) {
	var (
		storeAsyncCount int
		stored          *schema.ShardSetFlushTimes
		now             = time.Unix(1234, 0)
		nowFn           = func() time.Time { return now }
		doneCh          = make(chan struct{})
	)
	opts := NewFlushManagerOptions().
		SetJitterEnabled(false).
		SetFlushTimesPersistEvery(time.Second)
	mgr := newLeaderFlushManager(doneCh, opts).(*leaderFlushManager)
	mgr.nowFn = nowFn
	mgr.lastPersistAtNanos = now.Add(-2 * time.Second).UnixNano()
	mgr.flushedSincePersist = true
	mgr.flushTimesManager = &mockFlushTimesManager{
		storeAsyncFn: func(value *schema.ShardSetFlushTimes) error {
			storeAsyncCount++
			stored = value
			return nil
		},
	}

	mgr.Init(testFlushBuckets)
	flushTask, dur := mgr.Prepare(testFlushBuckets)
	require.Nil(t, flushTask)
	require.Equal(t, time.Second, dur)
	require.False(t, mgr.flushedSincePersist)
	require.Equal(t, 1, storeAsyncCount)
	require.Equal(t, testFlushTimes, stored)
}

func TestLeaderFlushManagerPrepareWithFlushAndPersist(t *testing.T) {
	var (
		storeAsyncCount int
		stored          *schema.ShardSetFlushTimes
		now             = time.Unix(1234, 0)
		nowFn           = func() time.Time { return now }
		doneCh          = make(chan struct{})
	)
	opts := NewFlushManagerOptions().
		SetJitterEnabled(false).
		SetFlushTimesPersistEvery(time.Second)
	mgr := newLeaderFlushManager(doneCh, opts).(*leaderFlushManager)
	mgr.nowFn = nowFn
	mgr.lastPersistAtNanos = now.UnixNano()
	mgr.flushedSincePersist = true
	mgr.flushTimesManager = &mockFlushTimesManager{
		storeAsyncFn: func(value *schema.ShardSetFlushTimes) error {
			storeAsyncCount++
			stored = value
			return nil
		},
	}

	mgr.Init(testFlushBuckets)
	now = now.Add(2 * time.Second)
	flushTask, dur := mgr.Prepare(testFlushBuckets)

	expectedFlushTimes := []flushMetadata{
		{timeNanos: 1236000000000, bucketIdx: 0},
		{timeNanos: 4834000000000, bucketIdx: 2},
		{timeNanos: 1294000000000, bucketIdx: 1},
	}
	require.NotNil(t, flushTask)
	require.Equal(t, time.Duration(0), dur)
	require.False(t, mgr.flushedSincePersist)
	task := flushTask.(*leaderFlushTask)
	require.Equal(t, testFlushBuckets[0].flushers, task.flushers)
	require.Equal(t, flushMetadataHeap(expectedFlushTimes), mgr.flushTimes)
	require.Equal(t, 1, storeAsyncCount)
	require.Equal(t, testFlushTimes, stored)
}

func TestLeaderFlushManagerOnBucketAdded(t *testing.T) {
	now := time.Unix(1234, 0)
	nowFn := func() time.Time { return now }
	doneCh := make(chan struct{})
	opts := NewFlushManagerOptions().SetJitterEnabled(false)
	mgr := newLeaderFlushManager(doneCh, opts).(*leaderFlushManager)
	mgr.nowFn = nowFn

	mgr.OnBucketAdded(0, testFlushBuckets[0])
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
	var flushRequest *FlushRequest
	flushers := []PeriodicFlusher{
		&mockFlusher{
			shard:   0,
			flushFn: func(req FlushRequest) { flushRequest = &req },
		},
	}
	doneCh := make(chan struct{})
	opts := NewFlushManagerOptions()
	errShards := errors.New("error getting shards")
	mgr := newLeaderFlushManager(doneCh, opts).(*leaderFlushManager)
	mgr.placementManager = &mockPlacementManager{
		shardsFn: func() (shard.Shards, error) {
			return nil, errShards
		},
	}
	flushTask := &leaderFlushTask{
		mgr:      mgr,
		duration: tally.NoopScope.Timer("foo"),
		flushers: flushers,
	}
	flushTask.Run()
	require.Nil(t, flushRequest)
}

func TestLeaderFlushTaskRunShardNotFound(t *testing.T) {
	var flushRequest *FlushRequest
	flushers := []PeriodicFlusher{
		&mockFlusher{
			shard:   2,
			flushFn: func(req FlushRequest) { flushRequest = &req },
		},
	}
	doneCh := make(chan struct{})
	opts := NewFlushManagerOptions()
	mgr := newLeaderFlushManager(doneCh, opts).(*leaderFlushManager)
	mgr.placementManager = &mockPlacementManager{
		shardsFn: func() (shard.Shards, error) {
			return shard.NewShards(nil), nil
		},
	}
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
	requests := make([]FlushRequest, 2)
	flushers := []PeriodicFlusher{
		&mockFlusher{
			shard:   0,
			flushFn: func(req FlushRequest) { requests[0] = req },
		},
		&mockFlusher{
			shard:   1,
			flushFn: func(req FlushRequest) { requests[1] = req },
		},
	}
	doneCh := make(chan struct{})
	opts := NewFlushManagerOptions().SetJitterEnabled(false)
	mgr := newLeaderFlushManager(doneCh, opts).(*leaderFlushManager)
	mgr.placementManager = &mockPlacementManager{
		shardsFn: func() (shard.Shards, error) {
			shards := []shard.Shard{
				shard.NewShard(0).SetState(shard.Initializing).SetCutoverNanos(5000).SetCutoffNanos(20000),
				shard.NewShard(1).SetState(shard.Initializing).SetCutoverNanos(5500).SetCutoffNanos(25000),
			}
			return shard.NewShards(shards), nil
		},
	}
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
