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
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3x/clock"

	"github.com/stretchr/testify/require"
)

func TestFlushManagerOpenAlreadyOpen(t *testing.T) {
	mgr, _ := testFlushManager()
	mgr.state = flushManagerOpen
	require.Equal(t, errFlushManagerAlreadyOpenOrClosed, mgr.Open(testShardSetID))
}

func TestFlushManagerOpenSuccess(t *testing.T) {
	mgr, _ := testFlushManager()
	mgr.leaderMgr = &mockRoleBasedFlushManager{
		openFn: func(uint32) error { return nil },
	}
	mgr.followerMgr = &mockRoleBasedFlushManager{
		openFn: func(uint32) error { return nil },
	}
	require.NoError(t, mgr.Open(testShardSetID))
}

func TestFlushManagerRegisterClosed(t *testing.T) {
	mgr, _ := testFlushManager()
	mgr.state = flushManagerClosed
	require.Equal(t, errFlushManagerNotOpenOrClosed, mgr.Register(nil))
}

func TestFlushManagerRegisterSuccess(t *testing.T) {
	mgr, now := testFlushManager()
	*now = time.Unix(1234, 0)

	var (
		bucketIndices []int
		buckets       []*flushBucket
	)
	mgr.leaderMgr = &mockRoleBasedFlushManager{
		openFn: func(uint32) error { return nil },
	}
	mgr.followerMgr = &mockRoleBasedFlushManager{
		openFn: func(uint32) error { return nil },
		onBucketAddedFn: func(bucketIdx int, bucket *flushBucket) {
			bucketIndices = append(bucketIndices, bucketIdx)
			buckets = append(buckets, bucket)
		},
	}
	flushers := []PeriodicFlusher{
		&mockFlusher{flushInterval: time.Second},
		&mockFlusher{flushInterval: time.Minute},
		&mockFlusher{flushInterval: time.Second},
		&mockFlusher{flushInterval: time.Hour},
	}

	require.NoError(t, mgr.Open(testShardSetID))
	for _, flusher := range flushers {
		require.NoError(t, mgr.Register(flusher))
	}

	expectedBuckets := []*flushBucket{
		&flushBucket{
			interval: time.Second,
			flushers: []PeriodicFlusher{flushers[0], flushers[2]},
		},
		&flushBucket{
			interval: time.Minute,
			flushers: []PeriodicFlusher{flushers[1]},
		},
		&flushBucket{
			interval: time.Hour,
			flushers: []PeriodicFlusher{flushers[3]},
		},
	}
	require.Equal(t, len(expectedBuckets), len(mgr.buckets))
	for i := 0; i < len(expectedBuckets); i++ {
		require.Equal(t, expectedBuckets[i].interval, mgr.buckets[i].interval)
		require.Equal(t, expectedBuckets[i].flushers, mgr.buckets[i].flushers)
	}
	for i := 0; i < len(expectedBuckets); i++ {
		require.Equal(t, i, bucketIndices[i])
		require.Equal(t, expectedBuckets[i].interval, buckets[i].interval)
		require.Equal(t, expectedBuckets[i].flushers, buckets[i].flushers)
	}
}

func TestFlushManagerStatus(t *testing.T) {
	mgr, _ := testFlushManager()
	mgr.leaderMgr = &mockRoleBasedFlushManager{
		canLead: false,
	}
	mgr.followerMgr = &mockRoleBasedFlushManager{
		canLead: true,
	}
	expected := FlushStatus{
		ElectionState: FollowerState,
		CanLead:       true,
	}
	require.Equal(t, expected, mgr.Status())
}

func TestFlushManagerCloseAlreadyClosed(t *testing.T) {
	mgr, _ := testFlushManager()
	mgr.state = flushManagerClosed
	require.Equal(t, errFlushManagerNotOpenOrClosed, mgr.Close())
}

func TestFlushManagerCloseSuccess(t *testing.T) {
	opts, _ := testFlushManagerOptions()
	opts = opts.SetCheckEvery(time.Second)
	mgr := NewFlushManager(opts).(*flushManager)
	mgr.state = flushManagerOpen

	// Wait a little for the flush goroutine to start.
	time.Sleep(100 * time.Millisecond)

	mgr.Close()
	require.Equal(t, flushManagerClosed, mgr.state)
	require.Panics(t, func() { mgr.wgFlush.Done() })
}

func TestFlushManagerFlush(t *testing.T) {
	var (
		slept           int32
		followerFlushes int
		followerInits   int
		leaderFlushes   int
		leaderInits     int
		signalCh        = make(chan struct{})
		captured        []*flushBucket
	)
	followerFlushTask := &mockFlushTask{
		runFn: func() { followerFlushes++ },
	}
	leaderFlushTask := &mockFlushTask{
		runFn: func() { leaderFlushes++ },
	}
	sleepFn := func(time.Duration) {
		atomic.AddInt32(&slept, 1)
		<-signalCh
	}
	waitUntilSlept := func(v int) {
		for {
			if atomic.LoadInt32(&slept) == int32(v) {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	electionManager := &mockElectionManager{
		electionState: FollowerState,
	}

	// Initialize flush manager.
	opts := NewFlushManagerOptions().
		SetCheckEvery(100 * time.Millisecond).
		SetJitterEnabled(false)
	mgr := NewFlushManager(opts).(*flushManager)
	mgr.sleepFn = sleepFn
	mgr.electionMgr = electionManager
	mgr.leaderMgr = &mockRoleBasedFlushManager{
		openFn: func(uint32) error { return nil },
		initFn: func(buckets []*flushBucket) { leaderInits++ },
		prepareFn: func(buckets []*flushBucket) (flushTask, time.Duration) {
			captured = buckets
			return leaderFlushTask, time.Second
		},
	}
	mgr.followerMgr = &mockRoleBasedFlushManager{
		openFn: func(uint32) error { return nil },
		initFn: func(buckets []*flushBucket) { followerInits++ },
		prepareFn: func(buckets []*flushBucket) (flushTask, time.Duration) {
			captured = buckets
			return followerFlushTask, time.Second
		},
		onBucketAddedFn: func(bucketIdx int, bucket *flushBucket) {},
	}

	// Flush as a follower.
	require.NoError(t, mgr.Open(testShardSetID))
	flushers := []PeriodicFlusher{
		&mockFlusher{flushInterval: 100 * time.Millisecond},
		&mockFlusher{flushInterval: 200 * time.Millisecond},
		&mockFlusher{flushInterval: 100 * time.Millisecond},
		&mockFlusher{flushInterval: 500 * time.Millisecond},
	}
	for _, flusher := range flushers {
		require.NoError(t, mgr.Register(flusher))
	}
	waitUntilSlept(1)
	require.Equal(t, 1, followerFlushes)
	require.Equal(t, 0, leaderFlushes)
	require.Equal(t, 0, followerInits)
	require.Equal(t, 0, leaderInits)

	// Transition to leader.
	electionManager.Lock()
	electionManager.electionState = LeaderState
	electionManager.Unlock()
	signalCh <- struct{}{}
	waitUntilSlept(2)
	require.Equal(t, 1, followerFlushes)
	require.Equal(t, 1, leaderFlushes)
	require.Equal(t, 0, followerInits)
	require.Equal(t, 1, leaderInits)

	// Transition to follower.
	electionManager.Lock()
	electionManager.electionState = FollowerState
	electionManager.Unlock()
	signalCh <- struct{}{}
	waitUntilSlept(3)
	require.Equal(t, 2, followerFlushes)
	require.Equal(t, 1, leaderFlushes)
	require.Equal(t, 1, followerInits)
	require.Equal(t, 1, leaderInits)

	expectedBuckets := []*flushBucket{
		&flushBucket{
			interval: 100 * time.Millisecond,
			flushers: []PeriodicFlusher{flushers[0], flushers[2]},
		},
		&flushBucket{
			interval: 200 * time.Millisecond,
			flushers: []PeriodicFlusher{flushers[1]},
		},
		&flushBucket{
			interval: 500 * time.Millisecond,
			flushers: []PeriodicFlusher{flushers[3]},
		},
	}
	require.Equal(t, len(expectedBuckets), len(captured))
	for i := range expectedBuckets {
		require.Equal(t, expectedBuckets[i].interval, captured[i].interval)
		require.Equal(t, expectedBuckets[i].flushers, captured[i].flushers)
	}

	mgr.state = flushManagerClosed
	close(signalCh)
}

func testFlushManager() (*flushManager, *time.Time) {
	opts, now := testFlushManagerOptions()
	return NewFlushManager(opts).(*flushManager), now
}

func testFlushManagerOptions() (FlushManagerOptions, *time.Time) {
	var now time.Time
	nowFn := func() time.Time { return now }
	clockOpts := clock.NewOptions().SetNowFn(nowFn)
	return NewFlushManagerOptions().
		SetClockOptions(clockOpts).
		SetCheckEvery(0).
		SetJitterEnabled(false), &now
}

type flushFn func()
type discardBeforeFn func(beforeNanos int64)

type mockFlusher struct {
	shard            uint32
	resolution       time.Duration
	flushInterval    time.Duration
	lastFlushedNanos int64
	flushFn          flushFn
	discardBeforeFn  discardBeforeFn
}

func (f *mockFlusher) Shard() uint32                   { return f.shard }
func (f *mockFlusher) Resolution() time.Duration       { return f.resolution }
func (f *mockFlusher) FlushInterval() time.Duration    { return f.flushInterval }
func (f *mockFlusher) LastFlushedNanos() int64         { return f.lastFlushedNanos }
func (f *mockFlusher) Flush()                          { f.flushFn() }
func (f *mockFlusher) DiscardBefore(beforeNanos int64) { f.discardBeforeFn(beforeNanos) }

type flushOpenFn func(shardSetID uint32) error
type bucketInitFn func(buckets []*flushBucket)
type bucketPrepareFn func(buckets []*flushBucket) (flushTask, time.Duration)
type onBucketAddedFn func(bucketIdx int, bucket *flushBucket)

type mockRoleBasedFlushManager struct {
	openFn          flushOpenFn
	initFn          bucketInitFn
	prepareFn       bucketPrepareFn
	onBucketAddedFn onBucketAddedFn
	canLead         bool
}

func (m *mockRoleBasedFlushManager) Open(shardSetID uint32) error { return m.openFn(shardSetID) }

func (m *mockRoleBasedFlushManager) Init(buckets []*flushBucket) {
	m.initFn(buckets)
}

func (m *mockRoleBasedFlushManager) Prepare(buckets []*flushBucket) (flushTask, time.Duration) {
	return m.prepareFn(buckets)
}

func (m *mockRoleBasedFlushManager) OnBucketAdded(bucketIdx int, bucket *flushBucket) {
	m.onBucketAddedFn(bucketIdx, bucket)
}

func (m *mockRoleBasedFlushManager) CanLead() bool { return m.canLead }

type runFn func()

type mockFlushTask struct {
	runFn runFn
}

func (t *mockFlushTask) Run() { t.runFn() }
