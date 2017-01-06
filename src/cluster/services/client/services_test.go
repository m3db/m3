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

package client

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/coreos/etcd/integration"
	"github.com/m3db/m3cluster/kv"
	etcdKV "github.com/m3db/m3cluster/kv/etcd"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/heartbeat"
	"github.com/m3db/m3cluster/services/placement"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3x/instrument"
	"github.com/stretchr/testify/require"
)

func TestOptions(t *testing.T) {
	opts := NewOptions()
	require.Equal(t, errNoKVGen, opts.Validate())

	opts = opts.SetKVGen(func(zone string) (kv.Store, error) {
		return nil, nil
	})
	require.Equal(t, errNoHeartbeatGen, opts.Validate())

	opts = opts.SetHeartbeatGen(func(zone string) (heartbeat.Store, error) {
		return nil, nil
	})
	require.NoError(t, opts.Validate())

	opts = opts.SetInitTimeout(0)
	require.Equal(t, errInvalidInitTimeout, opts.Validate())

	opts = opts.SetHeartbeatCheckInterval(0)
	require.Equal(t, errInvalidHeartbeatInterval, opts.Validate())
}

func TestMetadata(t *testing.T) {
	opts, closer, _ := testSetup(t)
	defer closer()

	sd, err := NewServices(opts)
	require.NoError(t, err)

	sid := services.NewServiceID()
	_, err = sd.Metadata(sid)
	require.Error(t, err)
	require.Equal(t, errNoServiceName, err)

	sid = sid.SetName("m3db")
	_, err = sd.Metadata(sid)
	require.Error(t, err)
	require.Equal(t, kv.ErrNotFound, err)

	m := services.NewMetadata().
		SetPort(1).
		SetLivenessInterval(30 * time.Second).
		SetHeartbeatInterval(10 * time.Second)
	err = sd.SetMetadata(sid, m)
	require.NoError(t, err)

	mGet, err := sd.Metadata(sid)
	require.NoError(t, err)
	require.Equal(t, m, mGet)
}

func TestAdvertiseErrors(t *testing.T) {
	opts, closer, _ := testSetup(t)
	defer closer()

	sd, err := NewServices(opts)
	require.NoError(t, err)

	ad := services.NewAdvertisement()
	err = sd.Advertise(ad)
	require.Error(t, err)
	require.Equal(t, errNoServiceID, err)

	sid := services.NewServiceID()
	ad = ad.SetServiceID(sid)
	err = sd.Advertise(ad)
	require.Error(t, err)
	require.Equal(t, errNoInstanceID, err)

	ad = ad.SetInstanceID("i1")
	err = sd.Advertise(ad)
	require.Error(t, err)
	require.Equal(t, errNoServiceName, err)

	sid = sid.SetName("m3db")
	ad = ad.SetServiceID(sid)
	err = sd.Advertise(ad)
	require.Error(t, err)
	require.Equal(t, kv.ErrNotFound, err)

	err = sd.SetMetadata(
		sid,
		services.NewMetadata().
			SetLivenessInterval(2*time.Second).
			SetHeartbeatInterval(time.Second),
	)
	require.NoError(t, err)

	err = sd.Advertise(ad)
	require.NoError(t, err)

	// the service and instance is already being advertised
	err = sd.Advertise(ad)
	require.Error(t, err)
}

func TestUnadvertiseErrors(t *testing.T) {
	opts, closer, _ := testSetup(t)
	defer closer()

	sd, err := NewServices(opts)
	require.NoError(t, err)

	err = sd.Unadvertise(nil, "")
	require.Error(t, err)
	require.Equal(t, errNoServiceID, err)

	sid := services.NewServiceID()
	err = sd.Unadvertise(sid, "")
	require.Error(t, err)
	require.Equal(t, errNoInstanceID, err)

	// the service and instance is not being advertised
	err = sd.Unadvertise(sid, "i1")
	require.Error(t, err)
}

func TestAdvertiseUnadvertise(t *testing.T) {
	opts, closer, m := testSetup(t)
	defer closer()

	sd, err := NewServices(opts)
	require.NoError(t, err)

	sid := services.NewServiceID().SetName("m3db").SetZone("zone1")
	hbInterval := 10 * time.Millisecond
	err = sd.SetMetadata(
		sid,
		services.NewMetadata().
			SetLivenessInterval(2*time.Second).
			SetHeartbeatInterval(hbInterval),
	)
	require.NoError(t, err)

	ad := services.NewAdvertisement().SetServiceID(sid).SetInstanceID("i1")

	require.NoError(t, sd.Advertise(ad))
	s, ok := m.getMockStore("zone1")
	require.True(t, ok)

	// wait for one heartbeat
	for {
		_, ok := s.lastHeartbeatTime(serviceKey(sid), "i1")
		if ok {
			break
		}
	}

	require.NoError(t, sd.Unadvertise(sid, "i1"))
	now := time.Now()
	// give enough time for another heartbeat
	time.Sleep(hbInterval)
	hb, ok := s.lastHeartbeatTime(serviceKey(sid), "i1")
	require.True(t, ok)
	require.True(t, hb.Before(now))

	// resume heartbeat
	require.NoError(t, sd.Advertise(ad))
	now = time.Now()
	for {
		hb, ok = s.lastHeartbeatTime("m3db", "i1")
		if hb.After(now) {
			break
		}
	}
}

func TestIsHealthy(t *testing.T) {
	require.True(t, isHealthy(services.NewAdvertisement()))

	require.True(t, isHealthy(services.NewAdvertisement().SetHealth(func() error { return nil })))

	require.False(t, isHealthy(services.NewAdvertisement().SetHealth(func() error { return errors.New("err") })))
}

func TestQueryIncludeUnhealthy(t *testing.T) {
	opts, closer, _ := testSetup(t)
	defer closer()

	sd, err := NewServices(opts)
	require.NoError(t, err)

	sid := services.NewServiceID()
	qopts := services.NewQueryOptions().SetIncludeUnhealthy(true)
	_, err = sd.Query(sid, qopts)
	require.Error(t, err)
	require.Equal(t, errNoServiceName, err)

	sid = sid.SetName("m3db")
	_, err = sd.Query(sid, qopts)
	require.Error(t, err)
	require.Equal(t, kv.ErrNotFound, err)

	p := placement.NewPlacement([]services.PlacementInstance{
		placement.NewInstance().
			SetID("i1").
			SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)})),
		placement.NewInstance().
			SetID("i2").
			SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)})),
	}, []uint32{1}, 2)

	ps, err := NewPlacementStorage(opts)
	require.NoError(t, err)

	err = ps.SetIfNotExist(sid, p)
	require.NoError(t, err)

	s, err := sd.Query(sid, qopts)
	require.NoError(t, err)
	require.Equal(t, 2, len(s.Instances()))
	require.Equal(t, sid, s.Instances()[0].ServiceID())
	require.Equal(t, 1, s.Sharding().NumShards())
	require.Equal(t, 2, s.Replication().Replicas())
}

func TestQueryNotIncludeUnhealthy(t *testing.T) {
	opts, closer, _ := testSetup(t)
	defer closer()

	sd, err := NewServices(opts)
	require.NoError(t, err)

	sid := services.NewServiceID()
	qopts := services.NewQueryOptions()
	_, err = sd.Query(sid, qopts)
	require.Error(t, err)
	require.Equal(t, errNoServiceName, err)

	sid = sid.SetName("m3db").SetZone("zone1")
	_, err = sd.Query(sid, qopts)
	require.Error(t, err)
	require.Equal(t, kv.ErrNotFound, err)

	p := placement.NewPlacement([]services.PlacementInstance{
		placement.NewInstance().
			SetID("i1").
			SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)})),
		placement.NewInstance().
			SetID("i2").
			SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)})),
	}, []uint32{1}, 2)

	ps, err := NewPlacementStorage(opts)
	require.NoError(t, err)

	err = ps.SetIfNotExist(sid, p)
	require.NoError(t, err)

	s, err := sd.Query(sid, qopts)
	require.NoError(t, err)
	require.Equal(t, 0, len(s.Instances()))
	require.Equal(t, 1, s.Sharding().NumShards())
	require.Equal(t, 2, s.Replication().Replicas())

	hb, err := opts.HeartbeatGen()("zone1")
	require.NoError(t, err)

	err = hb.Heartbeat("m3db", "i1", time.Second)
	require.NoError(t, err)

	s, err = sd.Query(sid, qopts)
	require.NoError(t, err)
	require.Equal(t, 1, len(s.Instances()))
	si := s.Instances()[0]
	require.Equal(t, sid, si.ServiceID())
	require.Equal(t, "i1", si.InstanceID())
	require.Equal(t, 1, s.Sharding().NumShards())
	require.Equal(t, 2, s.Replication().Replicas())
}

func TestWatchIncludeUnhealthy(t *testing.T) {
	opts, closer, _ := testSetup(t)
	defer closer()

	sd, err := NewServices(opts)
	require.NoError(t, err)

	qopts := services.NewQueryOptions().SetIncludeUnhealthy(true)
	sid := services.NewServiceID()
	_, err = sd.Watch(sid, qopts)
	require.Error(t, err)
	require.Equal(t, errNoServiceName, err)

	sid = sid.SetName("m3db")
	_, err = sd.Watch(sid, qopts)
	require.Error(t, err)
	require.Equal(t, errWatchInitTimeout, err)

	p := placement.NewPlacement([]services.PlacementInstance{
		placement.NewInstance().
			SetID("i1").
			SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)})),
		placement.NewInstance().
			SetID("i2").
			SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)})),
	}, []uint32{1}, 2)

	ps, err := NewPlacementStorage(opts)
	require.NoError(t, err)

	err = ps.SetIfNotExist(sid, p)
	require.NoError(t, err)

	w, err := sd.Watch(sid, qopts)
	require.NoError(t, err)
	<-w.C()
	s := w.Get().(services.Service)
	require.Equal(t, 2, len(s.Instances()))
	require.Equal(t, 1, s.Sharding().NumShards())
	require.Equal(t, 2, s.Replication().Replicas())

	p = placement.NewPlacement([]services.PlacementInstance{
		placement.NewInstance().
			SetID("i1").
			SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)})),
		placement.NewInstance().
			SetID("i2").
			SetShards(shard.NewShards([]shard.Shard{shard.NewShard(2)})),
	}, []uint32{1, 2}, 1)

	err = ps.CheckAndSet(sid, p, 1)
	require.NoError(t, err)
	<-w.C()
	s = w.Get().(services.Service)
	require.Equal(t, 2, len(s.Instances()))
	require.Equal(t, 2, s.Sharding().NumShards())
	require.Equal(t, 1, s.Replication().Replicas())
}

func TestWatchNotIncludeUnhealthy(t *testing.T) {
	opts, closer, m := testSetup(t)
	defer closer()

	sd, err := NewServices(opts)
	require.NoError(t, err)

	qopts := services.NewQueryOptions()
	sid := services.NewServiceID().SetName("m3db").SetZone("zone1")

	_, err = sd.Watch(sid, qopts)
	require.Error(t, err)
	require.Equal(t, errWatchInitTimeout, err)

	p := placement.NewPlacement([]services.PlacementInstance{
		placement.NewInstance().
			SetID("i1").
			SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)})),
		placement.NewInstance().
			SetID("i2").
			SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)})),
	}, []uint32{1}, 2)

	ps, err := NewPlacementStorage(opts)
	require.NoError(t, err)

	err = ps.SetIfNotExist(sid, p)
	require.NoError(t, err)

	w, err := sd.Watch(sid, qopts)
	require.NoError(t, err)
	<-w.C()
	s := w.Get().(services.Service)
	require.Equal(t, 0, len(s.Instances()))
	require.Equal(t, 1, s.Sharding().NumShards())
	require.Equal(t, 2, s.Replication().Replicas())

	mockHB, err := m.genMockStore("zone1")
	require.NoError(t, err)

	// heartbeat
	require.NoError(t, mockHB.Heartbeat(serviceKey(sid), "i1", time.Second))
	<-w.C()
	s = w.Get().(services.Service)
	require.Equal(t, 1, len(s.Instances()))
	require.Equal(t, "i1", s.Instances()[0].InstanceID())
	require.Equal(t, sid, s.Instances()[0].ServiceID())
	require.Equal(t, 1, s.Sharding().NumShards())
	require.Equal(t, 2, s.Replication().Replicas())

	require.NoError(t, mockHB.Heartbeat(serviceKey(sid), "i2", time.Second))
	<-w.C()
	s = w.Get().(services.Service)
	require.Equal(t, 2, len(s.Instances()))
	require.Equal(t, 1, s.Sharding().NumShards())
	require.Equal(t, 2, s.Replication().Replicas())

	mockHB.cleanup(serviceKey(sid), "i1")
	mockHB.cleanup(serviceKey(sid), "i2")
	mockHB, ok := m.getMockStore("zone1")
	require.True(t, ok)

	// make sure it does another tick
	time.Sleep(opts.HeartbeatCheckInterval())

	<-w.C()
	s = w.Get().(services.Service)
	require.Equal(t, 0, len(s.Instances()))
	require.Equal(t, 1, s.Sharding().NumShards())
	require.Equal(t, 2, s.Replication().Replicas())
}

func TestMultipleWatches(t *testing.T) {
	opts, closer, _ := testSetup(t)
	defer closer()

	qopts := services.NewQueryOptions()
	sid := services.NewServiceID().SetName("m3db").SetZone("zone1")

	p := placement.NewPlacement([]services.PlacementInstance{
		placement.NewInstance().
			SetID("i1").
			SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)})),
		placement.NewInstance().
			SetID("i2").
			SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)})),
	}, []uint32{1}, 2)

	ps, err := NewPlacementStorage(opts)
	require.NoError(t, err)

	err = ps.SetIfNotExist(sid, p)
	require.NoError(t, err)

	sd, err := NewServices(opts)
	require.NoError(t, err)

	w1, err := sd.Watch(sid, qopts)
	require.NoError(t, err)

	w2, err := sd.Watch(sid, qopts)
	require.NoError(t, err)

	kvm, ok := sd.(*client).kvManagers["zone1"]
	require.True(t, ok)

	require.Equal(t, 1, len(kvm.serviceWatchables))
	for _, w := range kvm.serviceWatchables {
		require.Equal(t, 2, w.NumWatches())
	}
	<-w1.C()
	<-w2.C()

	require.Equal(t, w1.Get(), w2.Get())
}

func TestEqualIDs(t *testing.T) {
	require.True(t, equalIDs(nil, nil))
	require.False(t, equalIDs(nil, []string{"i1"}))
	require.True(t, equalIDs([]string{"i1"}, []string{"i1"}))
	require.False(t, equalIDs([]string{"i2"}, []string{"i1"}))
	require.False(t, equalIDs([]string{"i1", "i2"}, []string{"i1"}))
}

func testSetup(t *testing.T) (Options, func(), *mockHBGen) {
	ecluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 3})
	ec := ecluster.Client(rand.Intn(3))

	closer := func() {
		ecluster.Terminate(t)
		ec.Watcher.Close()
		ec.Lease.Close()
	}

	kvGen := func(zone string) (kv.Store, error) {
		return etcdKV.NewStore(
			ec,
			etcdKV.NewOptions().
				SetWatchChanCheckInterval(100*time.Millisecond).
				SetKeyFn(func(key string) string {
					return fmt.Sprintf("[%s][%s]", zone, key)
				}),
		), nil
	}

	m := &mockHBGen{hbs: map[string]*mockHBStore{}}
	hbGen := func(zone string) (heartbeat.Store, error) {
		return m.genMockStore(zone)
	}

	return NewOptions().
		SetKVGen(kvGen).
		SetHeartbeatGen(hbGen).
		SetInitTimeout(100 * time.Millisecond).
		SetHeartbeatCheckInterval(100 * time.Millisecond).
		SetInstrumentsOptions(instrument.NewOptions()), closer, m
}

type mockHBGen struct {
	sync.Mutex

	hbs map[string]*mockHBStore
}

func (m *mockHBGen) genMockStore(zone string) (*mockHBStore, error) {
	m.Lock()
	defer m.Unlock()

	s, ok := m.hbs[zone]
	if ok {
		return s, nil
	}
	s = &mockHBStore{hbs: map[string]map[string]time.Time{}}

	m.hbs[zone] = s
	return s, nil
}

func (m *mockHBGen) getMockStore(zone string) (*mockHBStore, bool) {
	m.Lock()
	defer m.Unlock()

	s, ok := m.hbs[zone]
	return s, ok
}

type mockHBStore struct {
	sync.Mutex

	hbs map[string]map[string]time.Time
}

func (hb *mockHBStore) Heartbeat(s string, id string, ttl time.Duration) error {
	hb.Lock()
	defer hb.Unlock()

	hbMap, ok := hb.hbs[s]
	if !ok {
		hbMap = map[string]time.Time{}
		hb.hbs[s] = hbMap
	}
	hbMap[id] = time.Now()
	return nil
}

func (hb *mockHBStore) Get(s string) ([]string, error) {
	hb.Lock()
	defer hb.Unlock()

	var r []string
	hbMap, ok := hb.hbs[s]
	if !ok {
		return r, nil
	}

	r = make([]string, len(hbMap))
	for k := range hbMap {
		r = append(r, k)
	}

	return r, nil
}

func (hb *mockHBStore) cleanup(s, id string) {
	hb.Lock()
	defer hb.Unlock()

	hbMap, ok := hb.hbs[s]
	if !ok {
		return
	}
	delete(hbMap, id)
}

func (hb *mockHBStore) lastHeartbeatTime(s, id string) (time.Time, bool) {
	hb.Lock()
	defer hb.Unlock()

	hbMap, ok := hb.hbs[s]
	if !ok {
		return time.Time{}, false
	}

	t, ok := hbMap[id]
	return t, ok
}
