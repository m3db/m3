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

package watchmanager

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/mocks"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/integration"
	"golang.org/x/net/context"
)

func TestWatchChan(t *testing.T) {
	wh, ec, _, _, _, closer := testSetup(t)
	defer closer()

	wc, _, err := wh.watchChanWithTimeout("foo", 0)
	require.NoError(t, err)
	require.Equal(t, 0, len(wc))

	_, err = ec.Put(context.Background(), "foo", "v")
	require.NoError(t, err)

	select {
	case <-wc:
	case <-time.After(time.Second):
		require.Fail(t, "could not get notification")
	}

	mw := mocks.NewBlackholeWatcher(ec, 3, func() { time.Sleep(time.Minute) })
	wh.opts = wh.opts.SetWatcher(mw).SetWatchChanInitTimeout(100 * time.Millisecond)

	before := time.Now()
	_, _, err = wh.watchChanWithTimeout("foo", 0)
	require.WithinDuration(t, time.Now(), before, 150*time.Millisecond)
	require.Error(t, err)
}

func TestWatchSimple(t *testing.T) {
	wh, ec, updateCalled, shouldStop, doneCh, closer := testSetup(t)
	defer closer()
	require.Equal(t, int32(0), atomic.LoadInt32(updateCalled))

	go wh.Watch("foo")

	time.Sleep(3 * wh.opts.WatchChanInitTimeout())

	lastRead := atomic.LoadInt32(updateCalled)
	_, err := ec.Put(context.Background(), "foo", "v")
	require.NoError(t, err)

	for {
		if atomic.LoadInt32(updateCalled) >= lastRead+1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	lastRead = atomic.LoadInt32(updateCalled)
	_, err = ec.Put(context.Background(), "foo", "v")
	require.NoError(t, err)

	for {
		if atomic.LoadInt32(updateCalled) >= lastRead+1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// trigger CheckAndStop
	atomic.AddInt32(shouldStop, 1)
	<-doneCh

	lastRead = atomic.LoadInt32(updateCalled)
	_, err = ec.Put(context.Background(), "foo", "v")
	require.NoError(t, err)
	// put no longer triggers anything
	require.Equal(t, lastRead, atomic.LoadInt32(updateCalled))

	// sleep enough time and make sure nothing happens
	time.Sleep(3 * wh.opts.WatchChanCheckInterval())

	require.Equal(t, lastRead, atomic.LoadInt32(updateCalled))
}

func TestWatchRecreate(t *testing.T) {
	wh, ec, updateCalled, shouldStop, doneCh, closer := testSetup(t)
	defer closer()

	failTotal := 2
	mw := mocks.NewBlackholeWatcher(ec, failTotal, func() { time.Sleep(time.Minute) })
	wh.opts = wh.opts.
		SetWatcher(mw).
		SetWatchChanInitTimeout(200 * time.Millisecond).
		SetWatchChanResetInterval(100 * time.Millisecond)

	go wh.Watch("foo")

	// watch will error out but updateFn will be tried
	for {
		if atomic.LoadInt32(updateCalled) == int32(failTotal) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// now we have retried failTotal times, give enough time for reset to happen
	time.Sleep(3 * (wh.opts.WatchChanResetInterval()))

	// there should be a valid watch now, trigger a notification
	_, err := ec.Put(context.Background(), "foo", "v")
	require.NoError(t, err)

	for {
		if atomic.LoadInt32(updateCalled) == int32(failTotal+1) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// clean up the background go routine
	atomic.AddInt32(shouldStop, 1)
	<-doneCh
}

func TestWatchNoLeader(t *testing.T) {
	const (
		watchInitAndRetryDelay = 200 * time.Millisecond
		watchCheckInterval     = 50 * time.Millisecond
	)

	ecluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 3})
	defer ecluster.Terminate(t)

	var (
		ec              = ecluster.Client(0)
		tickDuration    = 10 * time.Millisecond
		electionTimeout = time.Duration(3*ecluster.Members[0].ElectionTicks) * tickDuration
		doneCh          = make(chan struct{}, 1)
		eventLog        = []*clientv3.Event{}
		updateCalled    int32
		shouldStop      int32
	)

	opts := NewOptions().
		SetWatcher(ec.Watcher).
		SetUpdateFn(
			func(_ string, e []*clientv3.Event) error {
				atomic.AddInt32(&updateCalled, 1)
				if len(e) > 0 {
					eventLog = append(eventLog, e...)
				}
				return nil
			},
		).
		SetTickAndStopFn(
			func(string) bool {
				if atomic.LoadInt32(&shouldStop) == 0 {
					return false
				}

				close(doneCh)

				return true
			},
		).
		SetWatchChanInitTimeout(watchInitAndRetryDelay).
		SetWatchChanCheckInterval(watchCheckInterval)

	wh, err := NewWatchManager(opts)
	require.NoError(t, err)

	go wh.Watch("foo")

	runtime.Gosched()
	time.Sleep(10 * time.Millisecond)

	// there should be a valid watch now, trigger a notification
	_, err = ec.Put(context.Background(), "foo", "bar")
	require.NoError(t, err)

	leaderIdx := ecluster.WaitLeader(t)
	require.True(t, leaderIdx >= 0 && leaderIdx < len(ecluster.Members), "got invalid leader")

	for i := 0; i < 10; i++ {
		if atomic.LoadInt32(&updateCalled) == int32(3) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// simulate quorum loss
	ecluster.Members[1].Stop(t)
	ecluster.Members[2].Stop(t)

	// wait for election timeout, then member[0] will not have a leader.
	time.Sleep(electionTimeout)

	for i := 0; i < 100; i++ { // 10ms * 100 = 1s
		// test that leader loss is retried - even on error, we should attempt update.
		// 5 is an arbitraty number greater than amount of actual updates
		if atomic.LoadInt32(&updateCalled) >= 10 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	updates := atomic.LoadInt32(&updateCalled)
	if updates < 10 {
		require.Fail(t,
			"insufficient update calls",
			"expected at least 10 update attempts, got %d during a partition",
			updates)
	}

	require.NoError(t, ecluster.Members[1].Restart(t))
	require.NoError(t, ecluster.Members[2].Restart(t))
	// wait for leader + election delay just in case
	time.Sleep(time.Duration(3*ecluster.Members[0].ElectionTicks) * tickDuration)

	leaderIdx = ecluster.WaitLeader(t)
	require.True(t, leaderIdx >= 0 && leaderIdx < len(ecluster.Members), "got invalid leader")

	_, err = ec.Put(context.Background(), "foo", "baz")
	require.NoError(t, err)

	// give some time for watch to be updated
	runtime.Gosched()
	time.Sleep(watchInitAndRetryDelay)

	atomic.AddInt32(&shouldStop, 1)
	<-doneCh

	require.Len(t, eventLog, 2)
	require.NotNil(t, eventLog[0])
	require.Equal(t, eventLog[0].Kv.Key, []byte("foo"))
	require.Equal(t, eventLog[0].Kv.Value, []byte("bar"))
	require.NotNil(t, eventLog[1])
	require.Equal(t, eventLog[1].Kv.Key, []byte("foo"))
	require.Equal(t, eventLog[1].Kv.Value, []byte("baz"))
}

func TestWatchCompactedRevision(t *testing.T) {
	wh, ec, updateCalled, shouldStop, doneCh, closer := testSetup(t)
	defer closer()

	ts := tally.NewTestScope("", nil)
	errC := ts.Counter("errors")
	wh.m.etcdWatchError = errC

	var compactRev int64
	for i := 1; i <= 10; i++ {
		resp, err := ec.Put(context.Background(), "foo", fmt.Sprintf("bar-%d", i))
		require.NoError(t, err)
		compactRev = resp.Header.Revision
	}

	_, err := ec.Compact(context.Background(), compactRev)
	require.NoError(t, err)

	wh.opts = wh.opts.SetWatchOptions([]clientv3.OpOption{
		clientv3.WithCreatedNotify(),
		clientv3.WithRev(1),
	})

	go wh.Watch("foo")
	time.Sleep(3 * wh.opts.WatchChanInitTimeout())

	assert.Equal(t, int32(4), atomic.LoadInt32(updateCalled))

	lastRead := atomic.LoadInt32(updateCalled)
	ec.Put(context.Background(), "foo", "bar-11")

	for atomic.LoadInt32(updateCalled) <= lastRead {
		time.Sleep(10 * time.Millisecond)
	}

	errN := ts.Snapshot().Counters()["errors+"].Value()
	assert.Equal(t, int64(1), errN, "expected to encounter watch error")

	atomic.AddInt32(shouldStop, 1)
	<-doneCh
}

func testSetup(t *testing.T) (*manager, *clientv3.Client, *int32, *int32, chan struct{}, func()) {
	ecluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	ec := ecluster.RandClient()

	closer := func() {
		ecluster.Terminate(t)
	}

	var (
		updateCalled int32
		shouldStop   int32
	)
	doneCh := make(chan struct{}, 1)
	opts := NewOptions().
		SetWatcher(ec.Watcher).
		SetUpdateFn(func(string, []*clientv3.Event) error {
			atomic.AddInt32(&updateCalled, 1)
			return nil
		}).
		SetTickAndStopFn(func(string) bool {
			if atomic.LoadInt32(&shouldStop) == 0 {
				return false
			}

			close(doneCh)

			return true
		}).
		SetWatchChanCheckInterval(100 * time.Millisecond).
		SetWatchChanInitTimeout(100 * time.Millisecond)

	wh, err := NewWatchManager(opts)
	require.NoError(t, err)

	return wh.(*manager), ec, &updateCalled, &shouldStop, doneCh, closer
}
