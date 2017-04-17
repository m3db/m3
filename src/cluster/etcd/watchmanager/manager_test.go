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
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3cluster/mocks"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/integration"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

func TestWatchChan(t *testing.T) {
	wh, ec, _, _, _, closer := testSetup(t)
	defer closer()

	wc, err := wh.watchChanWithTimeout("foo")
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
	_, err = wh.watchChanWithTimeout("foo")
	require.WithinDuration(t, time.Now(), before, 150*time.Millisecond)
	require.Error(t, err)
}

func TestWatchSimple(t *testing.T) {
	wh, ec, updateCalled, shouldStop, doneCh, closer := testSetup(t)
	defer closer()

	go wh.Watch("foo")

	require.Equal(t, int32(0), atomic.LoadInt32(updateCalled))

	time.Sleep(3 * wh.opts.WatchChanInitTimeout())
	_, err := ec.Put(context.Background(), "foo", "v")
	require.NoError(t, err)

	for {
		if atomic.LoadInt32(updateCalled) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	_, err = ec.Put(context.Background(), "foo", "v")
	require.NoError(t, err)

	for {
		if atomic.LoadInt32(updateCalled) == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// trigger CheckAndStop
	atomic.AddInt32(shouldStop, 1)
	<-doneCh

	_, err = ec.Put(context.Background(), "foo", "v")
	require.NoError(t, err)
	// put no longer triggers anything
	require.Equal(t, int32(2), atomic.LoadInt32(updateCalled))

	// sleep enough time and make sure nothing happens
	time.Sleep(3 * wh.opts.WatchChanCheckInterval())

	require.Equal(t, int32(2), atomic.LoadInt32(updateCalled))
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
	ecluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 3})
	defer ecluster.Terminate(t)

	ec := ecluster.Client(0)

	var (
		updateCalled int32
		shouldStop   int32
	)
	doneCh := make(chan struct{}, 1)
	opts := NewOptions().
		SetWatcher(ec.Watcher).
		SetUpdateFn(
			func(string) error {
				atomic.AddInt32(&updateCalled, 1)
				return nil
			},
		).
		SetTickAndStopFn(
			func(string) bool {
				if atomic.LoadInt32(&shouldStop) == 0 {
					return false
				}

				close(doneCh)

				// stopped = true
				return true
			},
		).
		SetWatchChanInitTimeout(200 * time.Millisecond)

	wh, err := NewWatchManager(opts)
	require.NoError(t, err)

	go wh.Watch("foo")

	time.Sleep(2 * time.Second)
	// there should be a valid watch now, trigger a notification
	_, err = ec.Put(context.Background(), "foo", "v")
	require.NoError(t, err)

	for {
		if atomic.LoadInt32(&updateCalled) == int32(1) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	ecluster.Members[1].Stop(t)
	ecluster.Members[2].Stop(t)
	ecluster.Client(1).Close()
	ecluster.Client(2).Close()
	ecluster.TakeClient(1)
	ecluster.TakeClient(2)

	// wait for election timeout, then member[0] will not have a leader.
	tickDuration := 10 * time.Millisecond
	time.Sleep(time.Duration(3*ecluster.Members[0].ElectionTicks) * tickDuration)

	for {
		if atomic.LoadInt32(&updateCalled) == 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// clean up the background go routine
	atomic.AddInt32(&shouldStop, 1)
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
		SetUpdateFn(func(string) error {
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
