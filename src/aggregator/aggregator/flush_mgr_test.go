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

	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/watch"

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

func TestFlushManagerRegisterStandardFlushingMetricList(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mgr, now := testFlushManager(t, ctrl)
	mgr.jitterEnabled = true
	mgr.maxJitterFn = func(d time.Duration) time.Duration { return d / 2 }
	mgr.randFn = func(n int64) int64 { return n / 2 }
	*now = time.Unix(1234, 0)

	var (
		flushers                []flushingMetricList
		newBucketIndices        []int
		newBuckets              []*flushBucket
		newFlusherBucketIndices []int
		newFlusherBuckets       []*flushBucket
		newFlushers             []flushingMetricList
	)
	for _, intv := range []time.Duration{
		time.Second,
		time.Minute,
		time.Second,
		time.Hour,
	} {
		flusher := NewMockflushingMetricList(ctrl)
		flusher.EXPECT().ID().Return(standardMetricListID{resolution: intv}.toMetricListID()).AnyTimes()
		flusher.EXPECT().FlushInterval().Return(intv).AnyTimes()
		flusher.EXPECT().FixedFlushOffset().Return(time.Duration(0), false).AnyTimes()
		flushers = append(flushers, flusher)
	}

	leaderMgr := NewMockroleBasedFlushManager(ctrl)
	leaderMgr.EXPECT().Open().AnyTimes()
	followerMgr := NewMockroleBasedFlushManager(ctrl)
	followerMgr.EXPECT().Open().AnyTimes()
	followerMgr.EXPECT().
		OnBucketAdded(gomock.Any(), gomock.Any()).
		Do(func(bucketIdx int, bucket *flushBucket) {
			newBucketIndices = append(newBucketIndices, bucketIdx)
			newBuckets = append(newBuckets, bucket)
		}).
		Times(3)
	followerMgr.EXPECT().
		OnFlusherAdded(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(bucketIdx int, bucket *flushBucket, flusher flushingMetricList) {
			newFlusherBucketIndices = append(newFlusherBucketIndices, bucketIdx)
			newFlusherBuckets = append(newFlusherBuckets, bucket)
			newFlushers = append(newFlushers, flusher)
		}).
		Times(4)
	mgr.leaderMgr = leaderMgr
	mgr.followerMgr = followerMgr
	require.NoError(t, mgr.Open())
	for _, flusher := range flushers {
		require.NoError(t, mgr.Register(flusher))
	}

	expectedNewBuckets := []*flushBucket{
		{
			bucketID: standardMetricListID{resolution: time.Second}.toMetricListID(),
			interval: time.Second,
			offset:   250 * time.Millisecond,
			flushers: []flushingMetricList{flushers[0], flushers[2]},
		},
		{
			bucketID: standardMetricListID{resolution: time.Minute}.toMetricListID(),
			interval: time.Minute,
			offset:   15 * time.Second,
			flushers: []flushingMetricList{flushers[1]},
		},
		{
			bucketID: standardMetricListID{resolution: time.Hour}.toMetricListID(),
			interval: time.Hour,
			offset:   15 * time.Minute,
			flushers: []flushingMetricList{flushers[3]},
		},
	}
	require.Equal(t, len(expectedNewBuckets), len(mgr.buckets))
	for i := 0; i < len(expectedNewBuckets); i++ {
		require.Equal(t, expectedNewBuckets[i].bucketID, mgr.buckets[i].bucketID)
		require.Equal(t, expectedNewBuckets[i].interval, mgr.buckets[i].interval)
		require.Equal(t, expectedNewBuckets[i].offset, mgr.buckets[i].offset)
		require.Equal(t, expectedNewBuckets[i].flushers, mgr.buckets[i].flushers)
	}
	for i := 0; i < len(expectedNewBuckets); i++ {
		require.Equal(t, i, newBucketIndices[i])
		require.Equal(t, expectedNewBuckets[i].bucketID, newBuckets[i].bucketID)
		require.Equal(t, expectedNewBuckets[i].interval, newBuckets[i].interval)
		require.Equal(t, expectedNewBuckets[i].offset, newBuckets[i].offset)
		require.Equal(t, expectedNewBuckets[i].flushers, newBuckets[i].flushers)
	}

	expectedNewFlusherBuckets := []*flushBucket{
		expectedNewBuckets[0],
		expectedNewBuckets[1],
		expectedNewBuckets[0],
		expectedNewBuckets[2],
	}
	require.Equal(t, len(expectedNewFlusherBuckets), len(newFlusherBuckets))
	for i := 0; i < len(expectedNewFlusherBuckets); i++ {
		require.Equal(t, expectedNewFlusherBuckets[i].bucketID, newFlusherBuckets[i].bucketID)
		require.Equal(t, expectedNewFlusherBuckets[i].interval, newFlusherBuckets[i].interval)
		require.Equal(t, expectedNewFlusherBuckets[i].offset, newFlusherBuckets[i].offset)
		require.Equal(t, expectedNewFlusherBuckets[i].flushers, newFlusherBuckets[i].flushers)
	}
	require.Equal(t, []int{0, 1, 0, 2}, newFlusherBucketIndices)
	require.Equal(t, flushers, newFlushers)
}

func TestFlushManagerRegisterForwardedFlushingMetricList(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mgr, now := testFlushManager(t, ctrl)
	*now = time.Unix(1234, 0)

	var (
		flushers                []flushingMetricList
		newBucketIndices        []int
		newBuckets              []*flushBucket
		newFlusherBucketIndices []int
		newFlusherBuckets       []*flushBucket
		newFlushers             []flushingMetricList
	)
	for _, intv := range []struct {
		resolution        time.Duration
		numForwardedTimes int
		flushOffset       time.Duration
	}{
		{resolution: time.Second, numForwardedTimes: 1, flushOffset: 5 * time.Second},
		{resolution: time.Minute, numForwardedTimes: 2, flushOffset: 6 * time.Second},
		{resolution: time.Second, numForwardedTimes: 3, flushOffset: 7 * time.Second},
		{resolution: time.Minute, numForwardedTimes: 2, flushOffset: 6 * time.Second},
	} {
		flusher := NewMockflushingMetricList(ctrl)
		flusher.EXPECT().ID().Return(forwardedMetricListID{
			resolution:        intv.resolution,
			numForwardedTimes: intv.numForwardedTimes,
		}.toMetricListID()).MinTimes(1)
		flusher.EXPECT().FlushInterval().Return(intv.resolution).AnyTimes()
		flusher.EXPECT().FixedFlushOffset().Return(intv.flushOffset, true).AnyTimes()
		flushers = append(flushers, flusher)
	}

	leaderMgr := NewMockroleBasedFlushManager(ctrl)
	leaderMgr.EXPECT().Open().AnyTimes()
	followerMgr := NewMockroleBasedFlushManager(ctrl)
	followerMgr.EXPECT().Open().AnyTimes()
	followerMgr.EXPECT().
		OnBucketAdded(gomock.Any(), gomock.Any()).
		Do(func(bucketIdx int, bucket *flushBucket) {
			newBucketIndices = append(newBucketIndices, bucketIdx)
			newBuckets = append(newBuckets, bucket)
		}).
		Times(3)
	followerMgr.EXPECT().
		OnFlusherAdded(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(bucketIdx int, bucket *flushBucket, flusher flushingMetricList) {
			newFlusherBucketIndices = append(newFlusherBucketIndices, bucketIdx)
			newFlusherBuckets = append(newFlusherBuckets, bucket)
			newFlushers = append(newFlushers, flusher)
		}).
		Times(4)
	mgr.leaderMgr = leaderMgr
	mgr.followerMgr = followerMgr
	require.NoError(t, mgr.Open())
	for _, flusher := range flushers {
		require.NoError(t, mgr.Register(flusher))
	}

	expectedNewBuckets := []*flushBucket{
		{
			bucketID: forwardedMetricListID{
				resolution:        time.Second,
				numForwardedTimes: 1,
			}.toMetricListID(),
			interval: time.Second,
			offset:   5 * time.Second,
			flushers: []flushingMetricList{flushers[0]},
		},
		{
			bucketID: forwardedMetricListID{
				resolution:        time.Minute,
				numForwardedTimes: 2,
			}.toMetricListID(),
			interval: time.Minute,
			offset:   6 * time.Second,
			flushers: []flushingMetricList{flushers[1], flushers[3]},
		},
		{
			bucketID: forwardedMetricListID{
				resolution:        time.Second,
				numForwardedTimes: 3,
			}.toMetricListID(),
			interval: time.Second,
			offset:   7 * time.Second,
			flushers: []flushingMetricList{flushers[2]},
		},
	}
	require.Equal(t, len(expectedNewBuckets), len(mgr.buckets))
	for i := 0; i < len(expectedNewBuckets); i++ {
		require.Equal(t, expectedNewBuckets[i].bucketID, mgr.buckets[i].bucketID)
		require.Equal(t, expectedNewBuckets[i].interval, mgr.buckets[i].interval)
		require.Equal(t, expectedNewBuckets[i].offset, mgr.buckets[i].offset)
		require.Equal(t, expectedNewBuckets[i].flushers, mgr.buckets[i].flushers)
	}
	for i := 0; i < len(expectedNewBuckets); i++ {
		require.Equal(t, i, newBucketIndices[i])
		require.Equal(t, expectedNewBuckets[i].bucketID, newBuckets[i].bucketID)
		require.Equal(t, expectedNewBuckets[i].interval, newBuckets[i].interval)
		require.Equal(t, expectedNewBuckets[i].offset, newBuckets[i].offset)
		require.Equal(t, expectedNewBuckets[i].flushers, newBuckets[i].flushers)
	}

	expectedNewFlusherBuckets := []*flushBucket{
		expectedNewBuckets[0],
		expectedNewBuckets[1],
		expectedNewBuckets[2],
		expectedNewBuckets[1],
	}
	require.Equal(t, len(expectedNewFlusherBuckets), len(newFlusherBuckets))
	for i := 0; i < len(expectedNewFlusherBuckets); i++ {
		require.Equal(t, expectedNewFlusherBuckets[i].bucketID, newFlusherBuckets[i].bucketID)
		require.Equal(t, expectedNewFlusherBuckets[i].interval, newFlusherBuckets[i].interval)
		require.Equal(t, expectedNewFlusherBuckets[i].offset, newFlusherBuckets[i].offset)
		require.Equal(t, expectedNewFlusherBuckets[i].flushers, newFlusherBuckets[i].flushers)
	}
	require.Equal(t, []int{0, 1, 2, 1}, newFlusherBucketIndices)
	require.Equal(t, flushers, newFlushers)
}

func TestFlushManagerUnregisterBucketNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var flushers []flushingMetricList
	for _, intv := range []time.Duration{
		time.Second,
		time.Minute,
	} {
		flusher := NewMockflushingMetricList(ctrl)
		flusher.EXPECT().ID().Return(standardMetricListID{resolution: intv}.toMetricListID()).AnyTimes()
		flushers = append(flushers, flusher)
	}

	mgr, _ := testFlushManager(t, ctrl)
	mgr.state = flushManagerOpen
	mgr.buckets = []*flushBucket{
		{
			bucketID: standardMetricListID{resolution: time.Second}.toMetricListID(),
			interval: time.Second,
			flushers: []flushingMetricList{flushers[0]},
		},
	}
	require.Equal(t, errBucketNotFound, mgr.Unregister(flushers[1]))
}

func TestFlushManagerUnregisterFlusherNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mgr, _ := testFlushManager(t, ctrl)
	mgr.state = flushManagerOpen
	mgr.buckets = []*flushBucket{
		{
			bucketID: standardMetricListID{resolution: time.Second}.toMetricListID(),
			interval: time.Second,
			flushers: []flushingMetricList{NewMockflushingMetricList(ctrl)},
		},
	}

	flusher2 := NewMockflushingMetricList(ctrl)
	flusher2.EXPECT().ID().Return(standardMetricListID{resolution: time.Second}.toMetricListID()).AnyTimes()
	require.Equal(t, errFlusherNotFound, mgr.Unregister(flusher2))
}

func TestFlushManagerUnregisterSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var flushers []flushingMetricList
	for _, intv := range []time.Duration{
		time.Second,
		time.Minute,
		time.Second,
		time.Second,
	} {
		flusher := NewMockflushingMetricList(ctrl)
		flusher.EXPECT().ID().Return(standardMetricListID{resolution: intv}.toMetricListID()).AnyTimes()
		flusher.EXPECT().FlushInterval().Return(intv).AnyTimes()
		flushers = append(flushers, flusher)
	}

	mgr, _ := testFlushManager(t, ctrl)
	mgr.state = flushManagerOpen
	mgr.buckets = []*flushBucket{
		{
			bucketID: standardMetricListID{resolution: time.Second}.toMetricListID(),
			interval: time.Second,
			flushers: []flushingMetricList{flushers[0], flushers[2], flushers[3]},
		},
		{
			bucketID: standardMetricListID{resolution: time.Minute}.toMetricListID(),
			interval: time.Minute,
			flushers: []flushingMetricList{flushers[1]},
		},
	}
	require.NoError(t, mgr.Unregister(flushers[0]))
	require.NoError(t, mgr.Unregister(flushers[2]))

	found := false
	for _, b := range mgr.buckets {
		if b.interval == time.Second {
			found = true
			require.Equal(t, []flushingMetricList{flushers[3]}, b.flushers)
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
	followerMgr.EXPECT().OnFlusherAdded(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	mgr.leaderMgr = leaderMgr
	mgr.followerMgr = followerMgr

	// Flush as a follower.
	require.NoError(t, mgr.Open())

	var flushers []flushingMetricList
	for _, intv := range []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		100 * time.Millisecond,
		500 * time.Millisecond,
	} {
		flusher := NewMockflushingMetricList(ctrl)
		flusher.EXPECT().ID().Return(standardMetricListID{resolution: intv}.toMetricListID()).AnyTimes()
		flusher.EXPECT().FlushInterval().Return(intv).AnyTimes()
		flusher.EXPECT().FixedFlushOffset().Return(time.Duration(0), false).AnyTimes()
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
		{
			interval: 100 * time.Millisecond,
			flushers: []flushingMetricList{flushers[0], flushers[2]},
		},
		{
			interval: 200 * time.Millisecond,
			flushers: []flushingMetricList{flushers[1]},
		},
		{
			interval: 500 * time.Millisecond,
			flushers: []flushingMetricList{flushers[3]},
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

func TestFlushManagerComputeFlushIntervalOffsetJitterEnabled(t *testing.T) {
	now := time.Unix(1234, 0)
	nowFn := func() time.Time { return now }
	maxJitterFn := func(interval time.Duration) time.Duration {
		return time.Duration(0.5 * float64(interval))
	}
	randFn := func(n int64) int64 { return int64(0.5 * float64(n)) }
	opts := NewFlushManagerOptions().
		SetJitterEnabled(true).
		SetMaxJitterFn(maxJitterFn)
	mgr := NewFlushManager(opts).(*flushManager)
	mgr.nowFn = nowFn
	mgr.randFn = randFn

	for _, input := range []struct {
		interval time.Duration
		expected time.Duration
	}{
		{interval: time.Second, expected: 250000000 * time.Nanosecond},
		{interval: 10 * time.Second, expected: 2500000000 * time.Nanosecond},
		{interval: time.Minute, expected: 15 * time.Second},
	} {
		require.Equal(t, input.expected, mgr.computeFlushIntervalOffset(input.interval))
	}
}

func TestFlushManagerComputeFlushIntervalOffsetJitterDisabled(t *testing.T) {
	now := time.Unix(1234, 0)
	nowFn := func() time.Time { return now }
	opts := NewFlushManagerOptions().SetJitterEnabled(false)
	mgr := NewFlushManager(opts).(*flushManager)
	mgr.nowFn = nowFn

	for _, input := range []struct {
		interval time.Duration
		expected time.Duration
	}{
		{interval: time.Second, expected: 0},
		{interval: 10 * time.Second, expected: 4 * time.Second},
		{interval: time.Minute, expected: 34 * time.Second},
	} {
		require.Equal(t, input.expected, mgr.computeFlushIntervalOffset(input.interval))
	}
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

func TestFlushManagerFlushTaskBlocksCloseAsLeader(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Initialize flush manager.
	opts := NewFlushManagerOptions().SetCheckEvery(100 * time.Millisecond)
	mgr := NewFlushManager(opts).(*flushManager)

	waitFor := time.Second
	mgr.sleepFn = func(d time.Duration) {
		require.Equal(t, waitFor, d, "unexpected wait time")
	}

	electionManager := NewMockElectionManager(ctrl)
	electionCh := make(chan struct{})
	electionManager.EXPECT().ElectionState().DoAndReturn(func() ElectionState {
		electionCh <- struct{}{}
		return LeaderState
	})

	mgr.electionMgr = electionManager

	followerMgr := NewMockroleBasedFlushManager(ctrl)
	followerMgr.EXPECT().Open()
	followerMgr.EXPECT().Close()
	mgr.followerMgr = followerMgr

	leaderFlushTask := NewMockflushTask(ctrl)
	leaderFlushCh := make(chan struct{})
	leaderFlushTask.EXPECT().Run().Do(func() {
		<-leaderFlushCh
		// NB: add some runtime to this task to ensure that Close does not finish
		// before waiting for the UpdateFlushTimes call.
		time.Sleep(time.Millisecond * 100)
	})

	leaderMgr := NewMockroleBasedFlushManager(ctrl)
	leaderMgr.EXPECT().Open()
	leaderMgr.EXPECT().Init(gomock.Any())
	leaderMgr.EXPECT().Prepare(gomock.Any()).Return(leaderFlushTask, waitFor)
	leaderMgr.EXPECT().Close().Do(func() {
		leaderFlushCh <- struct{}{}
	})
	mgr.leaderMgr = leaderMgr

	// Flush as a leader.
	require.NoError(t, mgr.Open())

	// NB: blocking on this channel will ensure that we are in a flush loop, since
	// it runs in a goroutine.
	<-electionCh

	// NB: Close should be blocked until leaderFlushChan is signaled.
	require.NoError(t, mgr.Close())
}
