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
	"sync"
	"testing"
	"time"

	"github.com/coreos/etcd/integration"
	metadataproto "github.com/m3db/m3cluster/generated/proto/metadata"
	"github.com/m3db/m3cluster/kv"
	etcdKV "github.com/m3db/m3cluster/kv/etcd"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/heartbeat"
	"github.com/m3db/m3cluster/services/placement"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/watch"
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

	// could not find heartbeat from this service instance
	err = sd.Unadvertise(sid, "i1")
	require.Error(t, err)
}

func TestUnadvertise(t *testing.T) {
	opts, closer, m := testSetup(t)
	defer closer()

	sd, err := NewServices(opts)
	require.NoError(t, err)

	sid := services.NewServiceID().SetName("m3db").SetZone("zone1")

	err = sd.Unadvertise(sid, "i1")
	require.Error(t, err)

	s, ok := m.getMockStore("zone1")
	require.True(t, ok)

	err = s.Heartbeat(serviceKey(sid), "i1", time.Hour)
	require.NoError(t, err)

	err = sd.Unadvertise(sid, "i1")
	require.NoError(t, err)

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
		ids, _ := s.Get(serviceKey(sid))
		if len(ids) == 1 {
			break
		}
	}

	require.NoError(t, sd.Unadvertise(sid, "i1"))
	ids, err := s.Get(serviceKey(sid))
	require.NoError(t, err)
	require.Equal(t, 0, len(ids))

	// give enough time for another heartbeat
	time.Sleep(hbInterval)
	ids, err = s.Get(serviceKey(sid))
	require.NoError(t, err)
	require.Equal(t, 0, len(ids))

	// resume heartbeat
	require.NoError(t, sd.Advertise(ad))
	for {
		ids, err = s.Get(serviceKey(sid))
		if len(ids) == 1 {
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

	p := placement.NewPlacement().SetInstances([]services.PlacementInstance{
		placement.NewInstance().
			SetID("i1").
			SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)})),
		placement.NewInstance().
			SetID("i2").
			SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)})),
	}).SetShards([]uint32{1}).SetReplicaFactor(2)

	ps, err := newPlacementStorage(opts)
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

	p := placement.NewPlacement().SetInstances([]services.PlacementInstance{
		placement.NewInstance().
			SetID("i1").
			SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)})),
		placement.NewInstance().
			SetID("i2").
			SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)})),
	}).SetShards([]uint32{1}).SetReplicaFactor(2)

	ps, err := newPlacementStorage(opts)
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

	sid = sid.SetName("m3db").SetZone("zone1")
	_, err = sd.Watch(sid, qopts)
	require.Error(t, err)
	require.Equal(t, errWatchInitTimeout, err)

	sd, err = NewServices(opts.SetInitTimeout(defaultInitTimeout))
	require.NoError(t, err)

	p := placement.NewPlacement().
		SetInstances([]services.PlacementInstance{
			placement.NewInstance().
				SetID("i1").
				SetEndpoint("e1").
				SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)})),
			placement.NewInstance().
				SetID("i2").
				SetEndpoint("e2").
				SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)})),
		}).
		SetShards([]uint32{1}).
		SetReplicaFactor(2).
		SetIsSharded(true)

	ps, err := sd.PlacementService(sid, placement.NewOptions())
	require.NoError(t, err)
	err = ps.SetPlacement(p)
	require.NoError(t, err)

	w, err := sd.Watch(sid, qopts)
	require.NoError(t, err)
	<-w.C()
	s := w.Get().(services.Service)
	require.Equal(t, 2, len(s.Instances()))
	require.Equal(t, 1, s.Sharding().NumShards())
	require.Equal(t, 2, s.Replication().Replicas())

	p = placement.NewPlacement().
		SetInstances([]services.PlacementInstance{
			placement.NewInstance().
				SetID("i1").
				SetEndpoint("e1").
				SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)})),
			placement.NewInstance().
				SetID("i2").
				SetEndpoint("e2").
				SetShards(shard.NewShards([]shard.Shard{shard.NewShard(2)})),
		}).
		SetShards([]uint32{1, 2}).
		SetReplicaFactor(1).
		SetIsSharded(true)

	err = ps.SetPlacement(p)
	require.NoError(t, err)

	<-w.C()
	s = w.Get().(services.Service)
	require.Equal(t, 2, len(s.Instances()))
	require.Equal(t, 2, s.Sharding().NumShards())
	require.Equal(t, 1, s.Replication().Replicas())

	c := sd.(*client)

	c.RLock()
	kvm, ok := c.kvManagers["zone1"]
	c.RUnlock()
	require.True(t, ok)

	// set a bad value for placement
	v, err := kvm.kv.Set(placementKey(sid), &metadataproto.Metadata{Port: 1})
	require.NoError(t, err)
	require.Equal(t, 3, v)

	// make sure the newly set bad value has been propagated to watches
	testWatch, err := kvm.kv.Watch(placementKey(sid))
	require.NoError(t, err)
	for range testWatch.C() {
		if testWatch.Get().Version() == 3 {
			break
		}
	}
	testWatch.Close()

	// make sure the bad value has been ignored
	s = w.Get().(services.Service)
	require.Equal(t, 0, len(w.C()))
	require.Equal(t, 2, len(s.Instances()))
	require.Equal(t, 2, s.Sharding().NumShards())
	require.Equal(t, 1, s.Replication().Replicas())

	// delete the placement
	err = ps.Delete()
	require.NoError(t, err)

	select {
	case <-w.C():
		require.Fail(t, "should not receive notification on delete")
	case <-time.After(500 * time.Millisecond):
	}

	p = placement.NewPlacement().
		SetInstances([]services.PlacementInstance{
			placement.NewInstance().
				SetID("i1").
				SetEndpoint("e1").
				SetShards(shard.NewShards([]shard.Shard{shard.NewShard(0)})),
		}).
		SetShards([]uint32{0}).
		SetReplicaFactor(1).
		SetIsSharded(true)

	err = ps.SetPlacement(p)
	require.NoError(t, err)

	// when the next valid placement came through, the watch will be updated
	<-w.C()
	s = w.Get().(services.Service)
	require.Equal(t, 1, len(s.Instances()))
	require.Equal(t, 1, s.Sharding().NumShards())
	require.Equal(t, 1, s.Replication().Replicas())
	require.Equal(t, true, s.Sharding().IsSharded())

	w.Close()
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

	sd, err = NewServices(opts.SetInitTimeout(defaultInitTimeout))
	require.NoError(t, err)

	p := placement.NewPlacement().
		SetInstances([]services.PlacementInstance{
			placement.NewInstance().
				SetID("i1").
				SetEndpoint("e1").
				SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)})),
			placement.NewInstance().
				SetID("i2").
				SetEndpoint("e2").
				SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)})),
		}).
		SetShards([]uint32{1}).
		SetReplicaFactor(2).
		SetIsSharded(true)

	ps, err := sd.PlacementService(sid, placement.NewOptions())
	require.NoError(t, err)
	err = ps.SetPlacement(p)
	require.NoError(t, err)

	w, err := sd.Watch(sid, qopts)
	require.NoError(t, err)
	<-w.C()
	s := w.Get().(services.Service)
	// the heartbeat has nil value, so no filtering
	require.Equal(t, 2, len(s.Instances()))
	require.Equal(t, 1, s.Sharding().NumShards())
	require.Equal(t, 2, s.Replication().Replicas())

	mockHB, ok := m.getMockStore("zone1")
	require.True(t, ok)

	hbWatchable, ok := mockHB.getWatchable(serviceKey(sid))
	require.True(t, ok)

	// heartbeat
	hbWatchable.Update([]string{"i1"})
	<-w.C()
	s = w.Get().(services.Service)
	require.Equal(t, 1, len(s.Instances()))
	require.Equal(t, "i1", s.Instances()[0].InstanceID())
	require.Equal(t, sid, s.Instances()[0].ServiceID())
	require.Equal(t, 1, s.Sharding().NumShards())
	require.Equal(t, 2, s.Replication().Replicas())

	hbWatchable.Update([]string{"i1", "i2"})
	<-w.C()
	s = w.Get().(services.Service)
	require.Equal(t, 2, len(s.Instances()))
	require.Equal(t, 1, s.Sharding().NumShards())
	require.Equal(t, 2, s.Replication().Replicas())

	hbWatchable.Update([]string{})

	<-w.C()
	s = w.Get().(services.Service)
	require.Equal(t, 0, len(s.Instances()))
	require.Equal(t, 1, s.Sharding().NumShards())
	require.Equal(t, 2, s.Replication().Replicas())

	hbWatchable.Update([]string{"i2"})

	<-w.C()
	s = w.Get().(services.Service)
	require.Equal(t, 1, len(s.Instances()))
	require.Equal(t, 1, s.Sharding().NumShards())
	require.True(t, s.Sharding().IsSharded())
	require.Equal(t, 2, s.Replication().Replicas())

	c := sd.(*client)

	c.RLock()
	kvm, ok := c.kvManagers["zone1"]
	c.RUnlock()
	require.True(t, ok)

	// set a bad value for placement
	v, err := kvm.kv.Set(placementKey(sid), &metadataproto.Metadata{Port: 1})
	require.NoError(t, err)
	require.Equal(t, 2, v)

	// make sure the newly set bad value has been propagated to watches
	testWatch, err := kvm.kv.Watch(placementKey(sid))
	require.NoError(t, err)
	for range testWatch.C() {
		if testWatch.Get().Version() == 2 {
			break
		}
	}
	testWatch.Close()

	// make sure the bad value has been ignored
	require.Equal(t, 0, len(w.C()))
	s = w.Get().(services.Service)
	require.Equal(t, 1, len(s.Instances()))
	require.Equal(t, 1, s.Sharding().NumShards())
	require.True(t, s.Sharding().IsSharded())
	require.Equal(t, 2, s.Replication().Replicas())

	// now receive a update from heartbeat Store
	// will try to merge it with existing valid placement
	hbWatchable.Update([]string{"i1", "i2"})

	<-w.C()
	s = w.Get().(services.Service)
	require.Equal(t, 2, len(s.Instances()))
	require.Equal(t, 1, s.Sharding().NumShards())
	require.True(t, s.Sharding().IsSharded())
	require.Equal(t, 2, s.Replication().Replicas())

	// delete the placement
	err = ps.Delete()
	require.NoError(t, err)

	select {
	case <-w.C():
		require.Fail(t, "should not receive notification on delete")
	case <-time.After(500 * time.Millisecond):
	}

	// the heartbeat update will be merged with the last known valid placement
	hbWatchable.Update([]string{"i1", "i2"})

	<-w.C()
	s = w.Get().(services.Service)
	require.Equal(t, 2, len(s.Instances()))
	require.Equal(t, 1, s.Sharding().NumShards())
	require.True(t, s.Sharding().IsSharded())
	require.Equal(t, 2, s.Replication().Replicas())

	p = placement.NewPlacement().
		SetInstances([]services.PlacementInstance{
			placement.NewInstance().
				SetID("i1").
				SetEndpoint("e1").
				SetShards(shard.NewShards([]shard.Shard{shard.NewShard(0)})),
		}).
		SetShards([]uint32{0}).
		SetReplicaFactor(1).
		SetIsSharded(true)

	err = ps.SetPlacement(p)
	require.NoError(t, err)

	// when the next valid placement came through, the watch will be updated
	<-w.C()
	s = w.Get().(services.Service)
	require.Equal(t, 1, len(s.Instances()))
	require.Equal(t, 1, s.Sharding().NumShards())
	require.Equal(t, 1, s.Replication().Replicas())
	require.Equal(t, true, s.Sharding().IsSharded())
}

func TestMultipleWatches(t *testing.T) {
	opts, closer, _ := testSetup(t)
	defer closer()

	qopts := services.NewQueryOptions()
	sid := services.NewServiceID().SetName("m3db").SetZone("zone1")

	p := placement.NewPlacement().SetInstances([]services.PlacementInstance{
		placement.NewInstance().
			SetID("i1").
			SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)})),
		placement.NewInstance().
			SetID("i2").
			SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1)})),
	}).SetShards([]uint32{1}).SetReplicaFactor(2)

	ps, err := newPlacementStorage(opts)
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

	w1.Close()
	w2.Close()
}

func testSetup(t *testing.T) (Options, func(), *mockHBGen) {
	ecluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	ec := ecluster.RandClient()

	closer := func() {
		ecluster.Terminate(t)
	}

	kvGen := func(zone string) (kv.Store, error) {
		return etcdKV.NewStore(
			ec,
			etcdKV.NewOptions().
				SetWatchChanCheckInterval(100*time.Millisecond).
				SetPrefix(fmt.Sprintf("%s/", zone)),
		)
	}

	m := &mockHBGen{
		hbs: map[string]*mockHBStore{},
	}
	hbGen := func(zone string) (heartbeat.Store, error) {
		return m.genMockStore(zone)
	}

	return NewOptions().
		SetKVGen(kvGen).
		SetHeartbeatGen(hbGen).
		SetInitTimeout(100 * time.Millisecond).
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
	s = &mockHBStore{
		hbs:        map[string]map[string]time.Time{},
		watchables: map[string]xwatch.Watchable{},
	}

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

	hbs        map[string]map[string]time.Time
	watchables map[string]xwatch.Watchable
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

	r = make([]string, 0, len(hbMap))
	for k := range hbMap {
		r = append(r, k)
	}
	return r, nil
}

func (hb *mockHBStore) Watch(s string) (xwatch.Watch, error) {
	hb.Lock()
	defer hb.Unlock()

	watchable, ok := hb.watchables[s]
	if ok {
		_, w, err := watchable.Watch()
		return w, err
	}

	watchable = xwatch.NewWatchable()
	hb.watchables[s] = watchable

	_, w, err := watchable.Watch()
	return w, err
}

func (hb *mockHBStore) getWatchable(s string) (xwatch.Watchable, bool) {
	hb.Lock()
	defer hb.Unlock()

	w, ok := hb.watchables[s]
	return w, ok
}

func (hb *mockHBStore) Delete(s, id string) error {
	hb.Lock()
	defer hb.Unlock()

	hbMap, ok := hb.hbs[s]
	if !ok {
		return errors.New("no hb found")
	}

	_, ok = hbMap[id]
	if !ok {
		return errors.New("no hb found")
	}

	delete(hbMap, id)
	return nil
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
