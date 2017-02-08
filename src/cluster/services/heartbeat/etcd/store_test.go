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

package etcd

import (
	"testing"
	"time"

	"github.com/m3db/m3cluster/mocks"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/integration"
	"github.com/stretchr/testify/require"
)

func TestKeys(t *testing.T) {
	s := "service"
	id := "instance"

	require.Equal(t, "_hb/service/instance", heartbeatKey(s, id))
	require.Equal(t, "_hb/service", servicePrefix(s))
	require.Equal(t, "instance", instanceFromKey(heartbeatKey(s, id), servicePrefix(s)))
	require.Equal(t, "_hb/service/instance/1m0s", leaseKey(s, id, time.Minute))
}

func TestReuseLeaseID(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	c, err := NewStore(ec, opts)
	require.NoError(t, err)
	store := c.(*client)

	err = store.Heartbeat("s", "i1", time.Minute)
	require.NoError(t, err)

	store.RLock()
	require.Equal(t, 1, len(store.leases))
	var leaseID clientv3.LeaseID
	for _, v := range store.leases {
		leaseID = v
	}
	store.RUnlock()

	err = store.Heartbeat("s", "i1", time.Minute)
	require.NoError(t, err)

	store.RLock()
	require.Equal(t, 1, len(store.leases))
	for _, v := range store.leases {
		require.Equal(t, leaseID, v)
	}
	store.RUnlock()
}

func TestHeartbeat(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	c, err := NewStore(ec, opts)
	require.NoError(t, err)
	store := c.(*client)

	err = store.Heartbeat("s", "i1", 1*time.Second)
	require.NoError(t, err)
	err = store.Heartbeat("s", "i2", 2*time.Second)
	require.NoError(t, err)

	ids, err := store.Get("s")
	require.NoError(t, err)
	require.Equal(t, 2, len(ids))
	require.Contains(t, ids, "i1")
	require.Contains(t, ids, "i2")

	for {
		ids, err = store.Get("s")
		require.NoError(t, err)
		if len(ids) == 1 {
			break
		}
	}
	require.Equal(t, 1, len(ids))
	require.NotContains(t, ids, "i1")
	require.Contains(t, ids, "i2")

	for {
		ids, err = store.Get("s")
		require.NoError(t, err)
		if len(ids) == 0 {
			break
		}
	}
	require.Equal(t, 0, len(ids))
	require.NotContains(t, ids, "i1")
	require.NotContains(t, ids, "i2")
}

func TestWatch(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, opts)
	require.NoError(t, err)

	w1, err := store.Watch("foo")
	require.NoError(t, err)
	require.Equal(t, 0, len(w1.C()))
	require.Nil(t, w1.Get())

	err = store.Heartbeat("foo", "i1", 2*time.Second)
	require.NoError(t, err)

	for range w1.C() {
		if len(w1.Get().([]string)) == 1 {
			break
		}
	}
	require.Equal(t, []string{"i1"}, w1.Get())

	err = store.Heartbeat("foo", "i2", 2*time.Second)
	require.NoError(t, err)

	for range w1.C() {
		if len(w1.Get().([]string)) == 2 {
			break
		}
	}
	require.Equal(t, []string{"i1", "i2"}, w1.Get())

	for range w1.C() {
		if len(w1.Get().([]string)) == 0 {
			break
		}
	}
	<-w1.C()
	require.Equal(t, 0, len(w1.C()))
	require.Equal(t, []string{}, w1.Get())

	err = store.Heartbeat("foo", "i2", time.Second)
	require.NoError(t, err)

	<-w1.C()
	require.Equal(t, 0, len(w1.C()))
	require.Equal(t, []string{"i2"}, w1.Get())

	w1.Close()
}

func TestWatchClose(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, opts.SetWatchChanCheckInterval(10*time.Millisecond))
	require.NoError(t, err)

	err = store.Heartbeat("foo", "i1", 100*time.Second)
	require.NoError(t, err)

	w1, err := store.Watch("foo")
	require.NoError(t, err)
	<-w1.C()
	require.Equal(t, []string{"i1"}, w1.Get())

	c := store.(*client)
	_, ok := c.watchables["_hb/foo"]
	require.True(t, ok)

	// closing w1 will close the go routine for the watch updates
	w1.Close()

	// waits until the original watchable is cleaned up
	for {
		c.RLock()
		_, ok = c.watchables["_hb/foo"]
		c.RUnlock()
		if !ok {
			break
		}
	}

	// getting a new watch will create a new watchale and thread to watch for updates
	w2, err := store.Watch("foo")
	require.NoError(t, err)
	<-w2.C()
	require.Equal(t, []string{"i1"}, w2.Get())

	// verify that w1 will no longer be updated because the original watchable is closed
	err = store.Heartbeat("foo", "i2", 100*time.Second)
	require.NoError(t, err)
	<-w2.C()
	require.Equal(t, []string{"i1", "i2"}, w2.Get())
	require.Equal(t, []string{"i1"}, w1.Get())

	w1.Close()
	w2.Close()
}

func TestMultipleWatchesFromNotExist(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, opts)
	require.NoError(t, err)

	w1, err := store.Watch("foo")
	require.NoError(t, err)
	require.Equal(t, 0, len(w1.C()))
	require.Nil(t, w1.Get())

	w2, err := store.Watch("foo")
	require.NoError(t, err)
	require.Equal(t, 0, len(w2.C()))
	require.Nil(t, w2.Get())

	err = store.Heartbeat("foo", "i1", 1*time.Second)
	require.NoError(t, err)

	for {
		g := w1.Get()
		if g == nil {
			continue
		}
		if len(g.([]string)) == 1 {
			break
		}
	}

	<-w1.C()
	require.Equal(t, 0, len(w1.C()))
	require.Equal(t, []string{"i1"}, w1.Get())

	<-w2.C()
	require.Equal(t, 0, len(w2.C()))
	require.Equal(t, []string{"i1"}, w2.Get())

	err = store.Heartbeat("foo", "i2", 2*time.Second)
	require.NoError(t, err)

	for {
		if len(w1.Get().([]string)) == 2 {
			break
		}
	}
	<-w1.C()
	require.Equal(t, []string{"i1", "i2"}, w1.Get())
	<-w2.C()
	require.Equal(t, []string{"i1", "i2"}, w2.Get())

	for {
		if len(w1.Get().([]string)) == 1 {
			break
		}
	}
	<-w1.C()
	require.Equal(t, []string{"i2"}, w1.Get())
	<-w2.C()
	require.Equal(t, []string{"i2"}, w2.Get())

	for {
		if len(w1.Get().([]string)) == 0 {
			break
		}
	}
	<-w1.C()
	require.Equal(t, []string{}, w1.Get())
	<-w2.C()
	require.Equal(t, []string{}, w2.Get())

	w1.Close()
	w2.Close()
}

func TestWatchNonBlocking(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	opts = opts.SetWatchChanResetInterval(200 * time.Millisecond).SetWatchChanInitTimeout(200 * time.Millisecond)

	store, err := NewStore(ec, opts)
	require.NoError(t, err)
	c := store.(*client)

	err = store.Heartbeat("foo", "i1", 100*time.Second)
	require.NoError(t, err)

	failTotal := 1
	mw := mocks.NewBlackholeWatcher(ec, failTotal, func() { time.Sleep(time.Minute) })
	c.watcher = mw

	before := time.Now()
	w1, err := c.Watch("foo")
	require.WithinDuration(t, time.Now(), before, 100*time.Millisecond)
	require.NoError(t, err)

	// watch channel will error out, but Get() will be tried
	select {
	case <-w1.C():
	case <-time.After(200 * time.Millisecond):
		require.Fail(t, "notification came too late")
	}
	require.Equal(t, []string{"i1"}, w1.Get())

	time.Sleep(5 * (opts.WatchChanResetInterval() + opts.WatchChanInitTimeout()))

	err = store.Heartbeat("foo", "i2", 100*time.Second)
	for {
		if len(w1.Get().([]string)) == 2 {
			break
		}
	}
	require.Equal(t, []string{"i1", "i2"}, w1.Get())

	w1.Close()
}

func testStore(t *testing.T) (*clientv3.Client, Options, func()) {
	ecluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	ec := ecluster.RandClient()

	closer := func() {
		ecluster.Terminate(t)
	}
	return ec, NewOptions(), closer
}
