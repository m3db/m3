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

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/integration"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeys(t *testing.T) {
	s := "service"
	id := "instance"

	require.Equal(t, "_hb/service/instance", heartbeatKey(s, id))
	require.Equal(t, "_hb/service", servicePrefix(s))
	require.Equal(t, "instance", instanceFromKey(heartbeatKey(s, id), s))
	require.Equal(t, "_hb/service/instance/1m0s", leaseKey(s, id, time.Minute))
}

func TestReuseLeaseID(t *testing.T) {
	cli, opts, closeFn := testStore(t)
	defer closeFn()

	store := NewStore(cli, opts).(*client)
	err := store.Heartbeat("s", "i1", time.Minute)
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
	cli, opts, closeFn := testStore(t)
	defer closeFn()

	store := NewStore(cli, opts)
	err := store.Heartbeat("s", "i1", 1*time.Second)
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

	store := NewStore(ec, opts)
	w1, err := store.Watch("foo")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(w1.C()))
	assert.Nil(t, w1.Get())

	err = store.Heartbeat("foo", "i1", 2*time.Second)
	assert.NoError(t, err)

	for range w1.C() {
		if len(w1.Get().([]string)) == 1 {
			break
		}
	}
	assert.Equal(t, []string{"i1"}, w1.Get())

	time.Sleep(time.Second)

	err = store.Heartbeat("foo", "i2", 2*time.Second)
	assert.NoError(t, err)

	<-w1.C()
	assert.Equal(t, 0, len(w1.C()))
	assert.Equal(t, []string{"i1", "i2"}, w1.Get())

	<-w1.C()
	assert.Equal(t, 0, len(w1.C()))
	assert.Equal(t, []string{"i2"}, w1.Get())

	<-w1.C()
	assert.Equal(t, 0, len(w1.C()))
	assert.Equal(t, []string{}, w1.Get())

	err = store.Heartbeat("foo", "i2", time.Second)
	assert.NoError(t, err)

	<-w1.C()
	assert.Equal(t, 0, len(w1.C()))
	assert.Equal(t, []string{"i2"}, w1.Get())

	w1.Close()
}

func TestWatchClose(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store := NewStore(ec, opts)

	err := store.Heartbeat("foo", "i1", 100*time.Second)
	assert.NoError(t, err)

	w1, err := store.Watch("foo")
	assert.NoError(t, err)
	<-w1.C()
	assert.Equal(t, []string{"i1"}, w1.Get())

	c := store.(*client)
	_, ok := c.watchables["foo"]
	assert.True(t, ok)

	// closing w1 will close the go routine for the watch updates
	w1.Close()

	// waits until the original watchable is cleaned up
	for {
		c.RLock()
		_, ok = c.watchables["foo"]
		c.RUnlock()
		if !ok {
			break
		}
	}

	// getting a new watch will create a new watchale and thread to watch for updates
	w2, err := store.Watch("foo")
	assert.NoError(t, err)
	<-w2.C()
	assert.Equal(t, []string{"i1"}, w2.Get())

	// verify that w1 will no longer be updated because the original watchable is closed
	err = store.Heartbeat("foo", "i2", 100*time.Second)
	assert.NoError(t, err)
	<-w2.C()
	assert.Equal(t, []string{"i1", "i2"}, w2.Get())
	assert.Equal(t, []string{"i1"}, w1.Get())

	w1.Close()
	w2.Close()
}

func TestRenewLeaseDoNotTriggerWatch(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store := NewStore(ec, opts)

	w1, err := store.Watch("foo")
	assert.NoError(t, err)

	err = store.Heartbeat("foo", "i1", 2*time.Second)
	assert.NoError(t, err)

	for range w1.C() {
		if len(w1.Get().([]string)) == 1 {
			break
		}
	}
	assert.Equal(t, []string{"i1"}, w1.Get())

	err = store.Heartbeat("foo", "i1", 2*time.Second)
	assert.NoError(t, err)

	select {
	case <-w1.C():
		assert.FailNow(t, "unexpected notification")
	case <-time.After(200 * time.Millisecond):
	}

	w1.Close()
}

func TestMultipleWatchesFromNotExist(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store := NewStore(ec, opts)
	w1, err := store.Watch("foo")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(w1.C()))
	assert.Nil(t, w1.Get())

	w2, err := store.Watch("foo")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(w2.C()))
	assert.Nil(t, w2.Get())

	err = store.Heartbeat("foo", "i1", 2*time.Second)
	assert.NoError(t, err)

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
	assert.Equal(t, 0, len(w1.C()))
	assert.Equal(t, []string{"i1"}, w1.Get())

	<-w2.C()
	assert.Equal(t, 0, len(w2.C()))
	assert.Equal(t, []string{"i1"}, w2.Get())

	time.Sleep(time.Second)

	err = store.Heartbeat("foo", "i2", 2*time.Second)
	assert.NoError(t, err)

	<-w1.C()
	assert.Equal(t, 0, len(w1.C()))
	assert.Equal(t, []string{"i1", "i2"}, w1.Get())

	<-w2.C()
	assert.Equal(t, 0, len(w2.C()))
	assert.Equal(t, []string{"i1", "i2"}, w2.Get())

	for {
		g := w1.Get()
		if g == nil {
			continue
		}
		if len(g.([]string)) == 0 {
			break
		}
	}

	<-w1.C()
	assert.Equal(t, 0, len(w1.C()))
	assert.Equal(t, []string{}, w1.Get())

	<-w2.C()
	assert.Equal(t, 0, len(w2.C()))
	assert.Equal(t, []string{}, w2.Get())

	w1.Close()
	w2.Close()
}

func testStore(t *testing.T) (*clientv3.Client, Options, func()) {
	ecluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	ec := ecluster.Client(0)

	closer := func() {
		ecluster.Terminate(t)
		ec.Watcher.Close()
	}
	return ec, NewOptions().SetWatchChanCheckInterval(10 * time.Millisecond), closer
}
