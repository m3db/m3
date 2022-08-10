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

/*
import (
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/mocks"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/services"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

func TestKeys(t *testing.T) {
	sid := services.NewServiceID().SetName("service")
	id := "instance"

	require.Equal(t, "_hb/service", servicePrefix(sid))
	require.Equal(t, "_hb/service/instance", heartbeatKey(sid, id))

	sid = sid.SetEnvironment("test")
	require.Equal(t, "_hb/test/service/instance", heartbeatKey(sid, id))
	require.Equal(t, "_hb/test/service", servicePrefix(sid))
	require.Equal(t, "instance", instanceFromKey(heartbeatKey(sid, id), servicePrefix(sid)))
}

func TestReuseLeaseID(t *testing.T) {
	sid := services.NewServiceID().SetName("s1").SetEnvironment("e1")
	ec, opts, closeFn := testStore(t, sid)
	defer closeFn()

	i1 := placement.NewInstance().SetID("i1")

	c, err := NewStore(ec, opts)
	require.NoError(t, err)
	store := c.(*client)

	err = store.Heartbeat(i1, time.Minute)
	require.NoError(t, err)

	store.RLock()
	require.Equal(t, 1, len(store.cache.leases))
	var leaseID clientv3.LeaseID
	for _, leases := range store.cache.leases {
		for _, v := range leases {
			leaseID = v
		}
	}
	store.RUnlock()

	err = store.Heartbeat(i1, time.Minute)
	require.NoError(t, err)

	store.RLock()
	require.Equal(t, 1, len(store.cache.leases))
	for _, leases := range store.cache.leases {
		for _, v := range leases {
			require.Equal(t, leaseID, v)
		}
	}
	store.RUnlock()
}

func TestHeartbeat(t *testing.T) {
	sid := services.NewServiceID().SetName("s1").SetEnvironment("e1")
	ec, opts, closeFn := testStore(t, sid)
	defer closeFn()

	i1 := placement.NewInstance().SetID("i1")
	i2 := placement.NewInstance().SetID("i2")

	c, err := NewStore(ec, opts)
	require.NoError(t, err)
	store := c.(*client)

	ids, err := store.Get()
	require.Equal(t, 0, len(ids))
	require.NoError(t, err)

	err = store.Heartbeat(i1, 2*time.Second)
	require.NoError(t, err)
	err = store.Heartbeat(i2, time.Minute)
	require.NoError(t, err)

	ids, err = store.Get()
	require.NoError(t, err)
	require.Equal(t, 2, len(ids))
	require.Contains(t, ids, "i1")
	require.Contains(t, ids, "i2")

	// ensure that both Get and GetInstances return the same instances
	// with their respective serialization methods
	instances, err := store.GetInstances()
	require.NoError(t, err)
	require.Equal(t, 2, len(instances))
	require.Contains(t, instances, i1)
	require.Contains(t, instances, i2)

	for {
		ids, err = store.Get()
		require.NoError(t, err)
		instances, err2 := store.GetInstances()
		require.NoError(t, err)
		require.NoError(t, err2)
		if len(ids) == 1 && len(instances) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.Equal(t, 1, len(ids))
	require.NotContains(t, ids, "i1")
	require.Contains(t, ids, "i2")

	err = store.Delete(i2.ID())
	require.NoError(t, err)

	ids, err = store.Get()
	require.NoError(t, err)
	require.Equal(t, 0, len(ids))
	require.NotContains(t, ids, "i1")
	require.NotContains(t, ids, "i2")
}

func TestDelete(t *testing.T) {
	sid := services.NewServiceID().SetName("s1").SetEnvironment("e1")
	ec, opts, closeFn := testStore(t, sid)
	defer closeFn()

	i1 := placement.NewInstance().SetID("i1")
	i2 := placement.NewInstance().SetID("i2")

	c, err := NewStore(ec, opts)
	require.NoError(t, err)
	store := c.(*client)

	err = store.Heartbeat(i1, time.Hour)
	require.NoError(t, err)

	err = store.Heartbeat(i2, time.Hour)
	require.NoError(t, err)

	ids, err := store.Get()
	require.NoError(t, err)
	require.Equal(t, 2, len(ids))
	require.Contains(t, ids, "i1")
	require.Contains(t, ids, "i2")

	instances, err := store.GetInstances()
	require.NoError(t, err)
	require.Equal(t, 2, len(instances))
	require.Contains(t, instances, i1)
	require.Contains(t, instances, i2)

	err = store.Delete(i1.ID())
	require.NoError(t, err)

	err = store.Delete(i1.ID())
	require.Error(t, err)

	ids, err = store.Get()
	require.NoError(t, err)
	require.Equal(t, 1, len(ids))
	require.Contains(t, ids, "i2")

	instances, err = store.GetInstances()
	require.NoError(t, err)
	require.Equal(t, 1, len(instances))
	require.Contains(t, instances, i2)

	err = store.Heartbeat(i1, time.Hour)
	require.NoError(t, err)

	for {
		ids, _ = store.Get()
		instances, _ = store.GetInstances()
		if len(ids) == 2 && len(instances) == 2 {
			break
		}
	}
}

func TestWatch(t *testing.T) {
	sid := services.NewServiceID().SetName("s2").SetEnvironment("e2")
	ec, opts, closeFn := testStore(t, sid)
	defer closeFn()

	store, err := NewStore(ec, opts)
	require.NoError(t, err)

	i1 := placement.NewInstance().SetID("i1")
	i2 := placement.NewInstance().SetID("i2")

	w1, err := store.Watch()
	require.NoError(t, err)
	<-w1.C()
	require.Empty(t, w1.Get())

	err = store.Heartbeat(i1, 2*time.Second)
	require.NoError(t, err)

	for range w1.C() {
		if len(w1.Get().([]string)) == 1 {
			break
		}
	}
	require.Equal(t, []string{"i1"}, w1.Get())

	err = store.Heartbeat(i2, 2*time.Second)
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

	err = store.Heartbeat(i2, time.Second)
	require.NoError(t, err)

	for range w1.C() {
		val := w1.Get().([]string)
		if len(val) == 1 && val[0] == "i2" {
			break
		}
	}

	w1.Close()
}

func TestWatchClose(t *testing.T) {
	sid := services.NewServiceID().SetName("s1").SetEnvironment("e1")
	ec, opts, closeFn := testStore(t, sid)
	defer closeFn()

	store, err := NewStore(ec, opts.SetWatchChanCheckInterval(10*time.Millisecond))
	require.NoError(t, err)

	i1 := placement.NewInstance().SetID("i1")
	i2 := placement.NewInstance().SetID("i2")

	err = store.Heartbeat(i1, 100*time.Second)
	require.NoError(t, err)

	w1, err := store.Watch()
	require.NoError(t, err)
	<-w1.C()
	require.Equal(t, []string{"i1"}, w1.Get())

	c := store.(*client)
	_, ok := c.watchables["_hb/e1/s1"]
	require.True(t, ok)

	// closing w1 will close the go routine for the watch updates
	w1.Close()

	// waits until the original watchable is cleaned up
	for {
		c.RLock()
		_, ok = c.watchables["_hb/e1/s1"]
		c.RUnlock()
		if !ok {
			break
		}
	}

	// getting a new watch will create a new watchale and thread to watch for updates
	w2, err := store.Watch()
	require.NoError(t, err)
	<-w2.C()
	require.Equal(t, []string{"i1"}, w2.Get())

	// verify that w1 will no longer be updated because the original watchable is closed
	err = store.Heartbeat(i2, 100*time.Second)
	require.NoError(t, err)
	<-w2.C()
	require.Equal(t, []string{"i1", "i2"}, w2.Get())
	require.Equal(t, []string{"i1"}, w1.Get())

	w1.Close()
	w2.Close()
}

func TestMultipleWatchesFromNotExist(t *testing.T) {
	sid := services.NewServiceID().SetName("s1").SetEnvironment("e1")
	ec, opts, closeFn := testStore(t, sid)
	defer closeFn()

	store, err := NewStore(ec, opts)
	require.NoError(t, err)

	i1 := placement.NewInstance().SetID("i1")
	i2 := placement.NewInstance().SetID("i2")

	w1, err := store.Watch()
	require.NoError(t, err)
	<-w1.C()
	require.Empty(t, w1.Get())

	w2, err := store.Watch()
	require.NoError(t, err)
	<-w2.C()
	require.Empty(t, w2.Get())

	err = store.Heartbeat(i1, 2*time.Second)
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

	err = store.Heartbeat(i2, 4*time.Second)
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
	sid := services.NewServiceID().SetName("s1").SetEnvironment("e1")
	ec, opts, closeFn := testStore(t, sid)
	defer closeFn()

	opts = opts.SetWatchChanResetInterval(200 * time.Millisecond).SetWatchChanInitTimeout(200 * time.Millisecond)

	i1 := placement.NewInstance().SetID("i1")
	i2 := placement.NewInstance().SetID("i2")

	store, err := NewStore(ec, opts)
	require.NoError(t, err)
	c := store.(*client)

	err = store.Heartbeat(i1, 100*time.Second)
	require.NoError(t, err)

	failTotal := 1
	mw := mocks.NewBlackholeWatcher(ec, failTotal, func() { time.Sleep(time.Minute) })
	c.watcher = mw

	before := time.Now()
	w1, err := c.Watch()
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

	err = store.Heartbeat(i2, 100*time.Second)
	require.NoError(t, err)

	for {
		if len(w1.Get().([]string)) == 2 {
			break
		}
	}
	require.Equal(t, []string{"i1", "i2"}, w1.Get())

	w1.Close()
}

func testStore(t *testing.T, sid services.ServiceID) (*clientv3.Client, Options, func()) {
	integration.BeforeTestExternal(t)
	ecluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	ec := ecluster.RandClient()

	closer := func() {
		ecluster.Terminate(t)
		ec.Close()
	}
	return ec, NewOptions().SetServiceID(sid).SetRequestTimeout(20 * time.Second), closer
}
*/
