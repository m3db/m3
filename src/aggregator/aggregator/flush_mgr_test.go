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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/watch"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestFlushManagerReset(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mgr, _ := testFlushManager(t, ctrl)

	// Reseting an unopened manager is a no op.
	require.NoError(t, mgr.Reset())
	require.NoError(t, mgr.Open())
	require.NoError(t, mgr.Close())

	// Opening a closed manager causes an error.
	require.Error(t, mgr.Open())

	// Resetting the manager allows the manager to be reopened.
	require.NoError(t, mgr.Reset())
	require.NoError(t, mgr.Open())
	require.NoError(t, mgr.Close())

	// Resetting an open manager causes an error.
	mgr.state = flushManagerOpen
	require.Equal(t, errFlushManagerOpen, mgr.Reset())
}

func TestFlushManagerOpenAlreadyOpen(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mgr, _ := testFlushManager(t, ctrl)
	mgr.state = flushManagerOpen
	require.Equal(t, errFlushManagerAlreadyOpenOrClosed, mgr.Open())
}

func TestFlushManagerOpenSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	leaderMgr := NewMockroleBasedFlushManager(ctrl)
	leaderMgr.EXPECT().Open()
	followerMgr := NewMockroleBasedFlushManager(ctrl)
	followerMgr.EXPECT().Open()

	mgr, _ := testFlushManager(t, ctrl)
	mgr.leaderMgr = leaderMgr
	mgr.followerMgr = followerMgr
	require.NoError(t, mgr.Open())
}

func TestFlushManagerRegisterSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mgr, now := testFlushManager(t, ctrl)
	*now = time.Unix(1234, 0)

	var (
		bucketIndices []int
		buckets       []*flushBucket
		flushers      []PeriodicFlusher
	)
	for _, intv := range []time.Duration{
		time.Second,
		time.Minute,
		time.Second,
		time.Hour,
	} {
		flusher := NewMockPeriodicFlusher(ctrl)
		flusher.EXPECT().FlushInterval().Return(intv).AnyTimes()
		flushers = append(flushers, flusher)
	}

	leaderMgr := NewMockroleBasedFlushManager(ctrl)
	leaderMgr.EXPECT().Open().AnyTimes()
	followerMgr := NewMockroleBasedFlushManager(ctrl)
	followerMgr.EXPECT().Open().AnyTimes()
	followerMgr.EXPECT().
		OnBucketAdded(gomock.Any(), gomock.Any()).
		Do(func(bucketIdx int, bucket *flushBucket) {
			bucketIndices = append(bucketIndices, bucketIdx)
			buckets = append(buckets, bucket)
		}).
		AnyTimes()
	mgr.leaderMgr = leaderMgr
	mgr.followerMgr = followerMgr
	require.NoError(t, mgr.Open())
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

func TestFlushManagerUnregisterBucketNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var flushers []PeriodicFlusher
	for _, intv := range []time.Duration{
		time.Second,
		time.Minute,
	} {
		flusher := NewMockPeriodicFlusher(ctrl)
		flusher.EXPECT().FlushInterval().Return(intv).AnyTimes()
		flushers = append(flushers, flusher)
	}

	mgr, _ := testFlushManager(t, ctrl)
	mgr.state = flushManagerOpen
	mgr.buckets = []*flushBucket{
		&flushBucket{
			interval: time.Second,
			flushers: []PeriodicFlusher{flushers[0]},
		},
	}
	require.Equal(t, errBucketNotFound, mgr.Unregister(flushers[1]))
}

func TestFlushManagerUnregisterFlusherNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	flusher := NewMockPeriodicFlusher(ctrl)
	flusher.EXPECT().FlushInterval().Return(time.Second).AnyTimes()

	mgr, _ := testFlushManager(t, ctrl)
	mgr.state = flushManagerOpen
	mgr.buckets = []*flushBucket{
		&flushBucket{
			interval: time.Second,
			flushers: []PeriodicFlusher{flusher},
		},
	}

	flusher2 := NewMockPeriodicFlusher(ctrl)
	flusher2.EXPECT().FlushInterval().Return(time.Second).AnyTimes()
	require.Equal(t, errFlusherNotFound, mgr.Unregister(flusher2))
}

func TestFlushManagerUnregisterSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var flushers []PeriodicFlusher
	for _, intv := range []time.Duration{
		time.Second,
		time.Minute,
		time.Second,
		time.Second,
	} {
		flusher := NewMockPeriodicFlusher(ctrl)
		flusher.EXPECT().FlushInterval().Return(intv).AnyTimes()
		flushers = append(flushers, flusher)
	}

	mgr, _ := testFlushManager(t, ctrl)
	mgr.state = flushManagerOpen
	mgr.buckets = []*flushBucket{
		&flushBucket{
			interval: time.Second,
			flushers: []PeriodicFlusher{flushers[0], flushers[2], flushers[3]},
		},
		&flushBucket{
			interval: time.Minute,
			flushers: []PeriodicFlusher{flushers[1]},
		},
	}
	require.NoError(t, mgr.Unregister(flushers[0]))
	require.NoError(t, mgr.Unregister(flushers[2]))

	found := false
	for _, b := range mgr.buckets {
		if b.interval == time.Second {
			found = true
			require.Equal(t, []PeriodicFlusher{flushers[3]}, b.flushers)
		}
	}
	require.True(t, found)
}

func TestFlushManagerStatus(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	leaderMgr := NewMockroleBasedFlushManager(ctrl)
	leaderMgr.EXPECT().CanLead().Return(false).AnyTimes()
	followerMgr := NewMockroleBasedFlushManager(ctrl)
	followerMgr.EXPECT().CanLead().Return(true).AnyTimes()
	mgr, _ := testFlushManager(t, ctrl)
	mgr.leaderMgr = leaderMgr
	mgr.followerMgr = followerMgr
	expected := FlushStatus{
		ElectionState: FollowerState,
		CanLead:       true,
	}
	require.Equal(t, expected, mgr.Status())
}

func TestFlushManagerCloseAlreadyClosed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mgr, _ := testFlushManager(t, ctrl)
	mgr.state = flushManagerClosed
	require.Equal(t, errFlushManagerNotOpenOrClosed, mgr.Close())
}

func TestFlushManagerCloseSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts, _ := testFlushManagerOptions(t, ctrl)
	opts = opts.SetCheckEvery(time.Second)
	mgr := NewFlushManager(opts).(*flushManager)
	mgr.state = flushManagerOpen

	// Wait a little for the flush goroutine to start.
	time.Sleep(100 * time.Millisecond)

	mgr.Close()
	require.Equal(t, flushManagerClosed, mgr.state)
	require.Panics(t, func() { mgr.Done() })
}

func TestFlushManagerFlush(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		slept             int32
		followerFlushes   int
		followerInits     int
		leaderFlushes     int
		leaderInits       int
		electionStateLock sync.Mutex
		electionState     = FollowerState
		signalCh          = make(chan struct{})
		captured          []*flushBucket
	)
	followerFlushTask := NewMockflushTask(ctrl)
	followerFlushTask.EXPECT().
		Run().
		Do(func() { followerFlushes++ }).
		AnyTimes()
	leaderFlushTask := NewMockflushTask(ctrl)
	leaderFlushTask.EXPECT().
		Run().
		Do(func() { leaderFlushes++ }).
		AnyTimes()

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
	electionManager := NewMockElectionManager(ctrl)
	electionManager.EXPECT().
		ElectionState().
		DoAndReturn(func() ElectionState {
			electionStateLock.Lock()
			defer electionStateLock.Unlock()
			return electionState
		}).
		AnyTimes()

	// Initialize flush manager.
	opts := NewFlushManagerOptions().
		SetCheckEvery(100 * time.Millisecond).
		SetJitterEnabled(false)
	mgr := NewFlushManager(opts).(*flushManager)
	mgr.sleepFn = sleepFn
	mgr.electionMgr = electionManager

	leaderMgr := NewMockroleBasedFlushManager(ctrl)
	leaderMgr.EXPECT().Open().AnyTimes()
	leaderMgr.EXPECT().
		Init(gomock.Any()).
		Do(func([]*flushBucket) { leaderInits++ }).
		AnyTimes()
	leaderMgr.EXPECT().
		Prepare(gomock.Any()).
		DoAndReturn(func(buckets []*flushBucket) (flushTask, time.Duration) {
			captured = buckets
			return leaderFlushTask, time.Second
		}).
		AnyTimes()
	followerMgr := NewMockroleBasedFlushManager(ctrl)
	followerMgr.EXPECT().Open().AnyTimes()
	followerMgr.EXPECT().
		Init(gomock.Any()).
		Do(func([]*flushBucket) { followerInits++ }).
		AnyTimes()
	followerMgr.EXPECT().
		Prepare(gomock.Any()).
		DoAndReturn(func(buckets []*flushBucket) (flushTask, time.Duration) {
			captured = buckets
			return followerFlushTask, time.Second
		}).
		AnyTimes()
	followerMgr.EXPECT().OnBucketAdded(gomock.Any(), gomock.Any()).AnyTimes()
	mgr.leaderMgr = leaderMgr
	mgr.followerMgr = followerMgr

	// Flush as a follower.
	require.NoError(t, mgr.Open())

	var flushers []PeriodicFlusher
	for _, intv := range []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		100 * time.Millisecond,
		500 * time.Millisecond,
	} {
		flusher := NewMockPeriodicFlusher(ctrl)
		flusher.EXPECT().FlushInterval().Return(intv).AnyTimes()
		flushers = append(flushers, flusher)
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
	electionStateLock.Lock()
	electionState = LeaderState
	electionStateLock.Unlock()
	signalCh <- struct{}{}
	waitUntilSlept(2)
	require.Equal(t, 1, followerFlushes)
	require.Equal(t, 1, leaderFlushes)
	require.Equal(t, 0, followerInits)
	require.Equal(t, 1, leaderInits)

	// Transition to follower.
	electionStateLock.Lock()
	electionState = FollowerState
	electionStateLock.Unlock()
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

func testFlushManager(t *testing.T, ctrl *gomock.Controller) (*flushManager, *time.Time) {
	opts, now := testFlushManagerOptions(t, ctrl)
	return NewFlushManager(opts).(*flushManager), now
}

func testFlushManagerOptions(
	t *testing.T,
	ctrl *gomock.Controller,
) (FlushManagerOptions, *time.Time) {
	var now time.Time
	nowFn := func() time.Time { return now }
	clockOpts := clock.NewOptions().SetNowFn(nowFn)
	watchable := watch.NewWatchable()
	_, w, err := watchable.Watch()
	require.NoError(t, err)

	flushTimesManager := NewMockFlushTimesManager(ctrl)
	flushTimesManager.EXPECT().Reset().Return(nil).AnyTimes()
	flushTimesManager.EXPECT().Watch().Return(w, nil).AnyTimes()
	flushTimesManager.EXPECT().Close().Return(nil).AnyTimes()

	return NewFlushManagerOptions().
		SetClockOptions(clockOpts).
		SetFlushTimesManager(flushTimesManager).
		SetCheckEvery(0).
		SetJitterEnabled(false), &now
}
