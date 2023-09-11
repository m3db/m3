// Copyright (c) 2018 Uber Technologies, Inc.
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

package services

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/generated/proto/metadatapb"
	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/placement/storage"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/x/instrument"
	xos "github.com/m3db/m3/src/x/os"
	xwatch "github.com/m3db/m3/src/x/watch"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvertBetweenProtoAndService(t *testing.T) {
	protoShards := getProtoShards([]uint32{0, 1, 2})
	sid := NewServiceID().
		SetName("test_service").
		SetEnvironment("test_env").
		SetZone("test_zone")
	p := &placementpb.Placement{
		Instances: map[string]*placementpb.Instance{
			"i1": {
				Id:             "i1",
				IsolationGroup: "r1",
				Zone:           "z1",
				Endpoint:       "e1",
				Weight:         1,
				Shards:         protoShards,
			},
			"i2": {
				Id:             "i2",
				IsolationGroup: "r2",
				Zone:           "z1",
				Endpoint:       "e2",
				Weight:         1,
				Shards:         protoShards,
			},
		},
		ReplicaFactor: 2,
		NumShards:     3,
		IsSharded:     true,
	}

	s, err := NewServiceFromProto(p, sid)
	assert.NoError(t, err)
	assert.Equal(t, 2, s.Replication().Replicas())
	assert.Equal(t, 3, s.Sharding().NumShards())
	assert.True(t, s.Sharding().IsSharded())

	i1, err := s.Instance("i1")
	assert.NoError(t, err)
	assert.Equal(t, "i1", i1.InstanceID())
	assert.Equal(t, "e1", i1.Endpoint())
	assert.Equal(t, 3, i1.Shards().NumShards())
	assert.Equal(t, sid, i1.ServiceID())
	assert.True(t, i1.Shards().Contains(0))
	assert.True(t, i1.Shards().Contains(1))
	assert.True(t, i1.Shards().Contains(2))

	i2, err := s.Instance("i2")
	assert.NoError(t, err)
	assert.Equal(t, "i2", i2.InstanceID())
	assert.Equal(t, "e2", i2.Endpoint())
	assert.Equal(t, 3, i2.Shards().NumShards())
	assert.Equal(t, sid, i2.ServiceID())
	assert.True(t, i2.Shards().Contains(0))
	assert.True(t, i2.Shards().Contains(1))
	assert.True(t, i2.Shards().Contains(2))
}

func getProtoShards(ids []uint32) []*placementpb.Shard {
	r := make([]*placementpb.Shard, len(ids))
	for i, id := range ids {
		r[i] = &placementpb.Shard{
			Id:    id,
			State: placementpb.ShardState_AVAILABLE,
		}
	}
	return r
}

func TestMetadata(t *testing.T) {
	opts, _ := testSetup()

	sd, err := NewServices(opts)
	require.NoError(t, err)

	sid := NewServiceID()
	_, err = sd.Metadata(sid)
	require.Error(t, err)
	require.Equal(t, errNoServiceName, err)

	sid = sid.SetName("m3db")
	_, err = sd.Metadata(sid)
	require.Error(t, err)
	require.Equal(t, kv.ErrNotFound, err)

	m := NewMetadata().
		SetPort(1).
		SetLivenessInterval(30 * time.Second).
		SetHeartbeatInterval(10 * time.Second)
	err = sd.SetMetadata(sid, m)
	require.NoError(t, err)

	mGet, err := sd.Metadata(sid)
	require.NoError(t, err)
	require.Equal(t, m, mGet)

	err = sd.DeleteMetadata(sid)
	require.NoError(t, err)

	mGet, err = sd.Metadata(sid)
	require.Error(t, err)
	require.Nil(t, mGet)
}

func TestAdvertiseErrors(t *testing.T) {
	opts, _ := testSetup()

	sd, err := NewServices(opts)
	require.NoError(t, err)

	ad := NewAdvertisement()
	err = sd.Advertise(ad)
	require.Error(t, err)
	require.Equal(t, errAdPlacementMissing, err)

	ad = NewAdvertisement().
		SetPlacementInstance(placement.NewInstance())
	err = sd.Advertise(ad)
	require.Error(t, err)
	require.Equal(t, errNoServiceID, err)

	sid := NewServiceID()
	ad = ad.SetServiceID(sid)
	err = sd.Advertise(ad)
	require.Error(t, err)
	require.Equal(t, errNoInstanceID, err)

	i1 := placement.NewInstance().SetID("i1")

	ad = ad.SetPlacementInstance(i1)
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
		NewMetadata().
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

func TestAdvertise_NoDelay(t *testing.T) {
	opts, hbGen := testSetup()

	sd, err := NewServices(opts)
	require.NoError(t, err)

	ad := NewAdvertisement()
	err = sd.Advertise(ad)
	require.Error(t, err)
	require.Equal(t, errAdPlacementMissing, err)

	ad = NewAdvertisement().
		SetPlacementInstance(placement.NewInstance())
	err = sd.Advertise(ad)
	require.Error(t, err)
	require.Equal(t, errNoServiceID, err)

	sid := NewServiceID()
	ad = ad.SetServiceID(sid)
	err = sd.Advertise(ad)
	require.Error(t, err)
	require.Equal(t, errNoInstanceID, err)

	i1 := placement.NewInstance().SetID("i1")

	ad = ad.SetPlacementInstance(i1)
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
		NewMetadata().
			SetLivenessInterval(time.Hour).
			SetHeartbeatInterval(30*time.Minute),
	)
	require.NoError(t, err)

	err = sd.Advertise(ad)
	require.NoError(t, err)

	hbGen.Lock()
	hb := hbGen.hbs[serviceKey(sid)]
	hbGen.Unlock()

	// hb store should show advertisement (almost) immediately
	var insts []string
	for {
		insts, err = hb.Get()
		if len(insts) == 1 || err != nil {
			break
		}
	}
	assert.NoError(t, err)
	assert.Equal(t, []string{"i1"}, insts)
}

func TestUnadvertiseErrors(t *testing.T) {
	opts, _ := testSetup()

	sd, err := NewServices(opts)
	require.NoError(t, err)

	err = sd.Unadvertise(nil, "")
	require.Error(t, err)
	require.Equal(t, errNoServiceID, err)

	sid := NewServiceID()
	err = sd.Unadvertise(sid, "")
	require.Error(t, err)
	require.Equal(t, errNoInstanceID, err)

	// could not find heartbeat from this service instance
	err = sd.Unadvertise(sid, "i1")
	require.Error(t, err)
}

func TestUnadvertise(t *testing.T) {
	opts, m := testSetup()

	sd, err := NewServices(opts)
	require.NoError(t, err)

	sid := NewServiceID().SetName("m3db").SetZone("zone1")

	err = sd.Unadvertise(sid, "i1")
	require.Error(t, err)

	s, ok := m.getMockStore(sid)
	require.True(t, ok)

	i1 := placement.NewInstance().SetID("i1")

	err = s.Heartbeat(i1, time.Hour)
	require.NoError(t, err)

	err = sd.Unadvertise(sid, "i1")
	require.NoError(t, err)

	err = sd.Unadvertise(sid, "i1")
	require.Error(t, err)
}

func TestAdvertiseUnadvertise(t *testing.T) {
	opts, m := testSetup()

	sd, err := NewServices(opts)
	require.NoError(t, err)

	sid := NewServiceID().SetName("m3db").SetZone("zone1")
	hbInterval := 10 * time.Millisecond
	err = sd.SetMetadata(
		sid,
		NewMetadata().
			SetLivenessInterval(2*time.Second).
			SetHeartbeatInterval(hbInterval),
	)
	require.NoError(t, err)

	ad := NewAdvertisement().
		SetServiceID(sid).
		SetPlacementInstance(placement.NewInstance().SetID("i1"))

	require.NoError(t, sd.Advertise(ad))
	s, ok := m.getMockStore(sid)
	require.True(t, ok)

	// wait for one heartbeat
	for {
		ids, _ := s.Get()
		if len(ids) == 1 {
			break
		}
	}

	require.NoError(t, sd.Unadvertise(sid, "i1"))
	ids, err := s.Get()
	require.NoError(t, err)
	require.Equal(t, 0, len(ids), fmt.Sprintf("ids: %v", ids))

	// give enough time for another heartbeat
	time.Sleep(hbInterval)
	ids, err = s.Get()
	require.NoError(t, err)
	require.Equal(t, 0, len(ids), fmt.Sprintf("ids: %v", ids))

	// resume heartbeat
	require.NoError(t, sd.Advertise(ad))
	for {
		ids, err = s.Get()
		require.NoError(t, err)
		if len(ids) == 1 {
			break
		}
	}
}

func TestIsHealthy(t *testing.T) {
	require.True(t, isHealthy(NewAdvertisement()))

	require.True(t, isHealthy(NewAdvertisement().SetHealth(func() error { return nil })))

	require.False(t, isHealthy(NewAdvertisement().SetHealth(func() error { return errors.New("err") })))
}

func TestQueryIncludeUnhealthy(t *testing.T) {
	opts, _ := testSetup()

	sd, err := NewServices(opts)
	require.NoError(t, err)

	sid := NewServiceID()
	qopts := NewQueryOptions().SetIncludeUnhealthy(true)
	_, err = sd.Query(sid, qopts)
	require.Error(t, err)
	require.Equal(t, errNoServiceName, err)

	sid = sid.SetName("m3db")
	_, err = sd.Query(sid, qopts)
	require.Error(t, err)
	require.Equal(t, kv.ErrNotFound, err)

	p := placement.NewPlacement().SetInstances([]placement.Instance{
		placement.NewInstance().
			SetID("i1").
			SetEndpoint("e1").
			SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1).SetState(shard.Initializing)})),
		placement.NewInstance().
			SetID("i2").
			SetEndpoint("e2").
			SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1).SetState(shard.Initializing)})),
	}).SetShards([]uint32{1}).SetReplicaFactor(2).SetIsSharded(true)

	ps, err := newTestPlacementStorage(sid, opts, placement.NewOptions())
	require.NoError(t, err)

	_, err = ps.SetIfNotExist(p)
	require.NoError(t, err)

	s, err := sd.Query(sid, qopts)
	require.NoError(t, err)
	require.Equal(t, 2, len(s.Instances()))
	require.Equal(t, sid, s.Instances()[0].ServiceID())
	require.Equal(t, 1, s.Sharding().NumShards())
	require.Equal(t, 2, s.Replication().Replicas())
}

func TestQueryNotIncludeUnhealthy(t *testing.T) {
	opts, _ := testSetup()

	sd, err := NewServices(opts)
	require.NoError(t, err)

	sid := NewServiceID()
	qopts := NewQueryOptions()
	_, err = sd.Query(sid, qopts)
	require.Error(t, err)
	require.Equal(t, errNoServiceName, err)

	sid = sid.SetName("m3db").SetZone("zone1")
	_, err = sd.Query(sid, qopts)
	require.Error(t, err)
	require.Equal(t, kv.ErrNotFound, err)

	p := placement.NewPlacement().SetInstances([]placement.Instance{
		placement.NewInstance().
			SetID("i1").
			SetEndpoint("e1").
			SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1).SetState(shard.Initializing)})),
		placement.NewInstance().
			SetID("i2").
			SetEndpoint("e2").
			SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1).SetState(shard.Initializing)})),
	}).SetShards([]uint32{1}).SetReplicaFactor(2).SetIsSharded(true)

	ps, err := newTestPlacementStorage(sid, opts, placement.NewOptions())
	require.NoError(t, err)

	_, err = ps.SetIfNotExist(p)
	require.NoError(t, err)

	s, err := sd.Query(sid, qopts)
	require.NoError(t, err)
	require.Equal(t, 0, len(s.Instances()))
	require.Equal(t, 1, s.Sharding().NumShards())
	require.Equal(t, 2, s.Replication().Replicas())

	hb, err := opts.HeartbeatGen()(sid)
	require.NoError(t, err)

	i1 := placement.NewInstance().SetID("i1")

	err = hb.Heartbeat(i1, time.Second)
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
	opts, _ := testSetup()

	sd, err := NewServices(opts)
	require.NoError(t, err)

	qopts := NewQueryOptions().SetIncludeUnhealthy(true)
	sid := NewServiceID()
	_, err = sd.Watch(sid, qopts)
	require.Error(t, err)
	require.Equal(t, errNoServiceName, err)

	sid = sid.SetName("m3db").SetZone("zone1")
	_, err = sd.Watch(sid, qopts)
	require.Error(t, err)

	sd, err = NewServices(opts.SetInitTimeout(defaultInitTimeout))
	require.NoError(t, err)

	p := placement.NewPlacement().
		SetInstances([]placement.Instance{
			placement.NewInstance().
				SetID("i1").
				SetEndpoint("e1").
				SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1).SetState(shard.Initializing)})),
			placement.NewInstance().
				SetID("i2").
				SetEndpoint("e2").
				SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1).SetState(shard.Initializing)})),
		}).
		SetShards([]uint32{1}).
		SetReplicaFactor(2).
		SetIsSharded(true)

	ps, err := sd.PlacementService(sid, placement.NewOptions())
	require.NoError(t, err)
	_, err = ps.Set(p)
	require.NoError(t, err)

	w, err := sd.Watch(sid, qopts)
	require.NoError(t, err)
	<-w.C()
	s := w.Get().(Service)
	require.Equal(t, 2, len(s.Instances()))
	require.Equal(t, 1, s.Sharding().NumShards())
	require.Equal(t, 2, s.Replication().Replicas())

	p = placement.NewPlacement().
		SetInstances([]placement.Instance{
			placement.NewInstance().
				SetID("i1").
				SetEndpoint("e1").
				SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1).SetState(shard.Initializing)})),
			placement.NewInstance().
				SetID("i2").
				SetEndpoint("e2").
				SetShards(shard.NewShards([]shard.Shard{shard.NewShard(2).SetState(shard.Initializing)})),
		}).
		SetShards([]uint32{1, 2}).
		SetReplicaFactor(1).
		SetIsSharded(true)

	_, err = ps.Set(p)
	require.NoError(t, err)

	<-w.C()
	s = w.Get().(Service)
	require.Equal(t, 2, len(s.Instances()))
	require.Equal(t, 2, s.Sharding().NumShards())
	require.Equal(t, 1, s.Replication().Replicas())

	c := sd.(*client)

	c.RLock()
	kvm, ok := c.kvManagers["zone1"]
	c.RUnlock()
	require.True(t, ok)

	// set a bad value for placement
	v, err := kvm.kv.Set(keyFnWithNamespace(placementPrefix)(sid), &metadatapb.Metadata{Port: 1})
	require.NoError(t, err)
	require.Equal(t, 3, v)

	// make sure the newly set bad value has been propagated to watches
	testWatch, err := kvm.kv.Watch(keyFnWithNamespace(placementPrefix)(sid))
	require.NoError(t, err)
	for range testWatch.C() {
		if testWatch.Get().Version() == 3 {
			break
		}
	}
	testWatch.Close()

	// make sure the bad value has been ignored
	s = w.Get().(Service)
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
		SetInstances([]placement.Instance{
			placement.NewInstance().
				SetID("i1").
				SetEndpoint("e1").
				SetShards(shard.NewShards([]shard.Shard{shard.NewShard(0).SetState(shard.Initializing)})),
		}).
		SetShards([]uint32{0}).
		SetReplicaFactor(1).
		SetIsSharded(true)

	_, err = ps.Set(p)
	require.NoError(t, err)

	// when the next valid placement came through, the watch will be updated
	<-w.C()
	s = w.Get().(Service)
	require.Equal(t, 1, len(s.Instances()))
	require.Equal(t, 1, s.Sharding().NumShards())
	require.Equal(t, 1, s.Replication().Replicas())
	require.Equal(t, true, s.Sharding().IsSharded())

	w.Close()
}

func TestWatchNotIncludeUnhealthy(t *testing.T) {
	opts, m := testSetup()

	sd, err := NewServices(opts)
	require.NoError(t, err)

	qopts := NewQueryOptions()
	sid := NewServiceID().SetName("m3db").SetZone("zone1")

	_, err = sd.Watch(sid, qopts)
	require.Error(t, err)

	sd, err = NewServices(opts.SetInitTimeout(defaultInitTimeout))
	require.NoError(t, err)

	p := placement.NewPlacement().
		SetInstances([]placement.Instance{
			placement.NewInstance().
				SetID("i1").
				SetEndpoint("e1").
				SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1).SetState(shard.Initializing)})),
			placement.NewInstance().
				SetID("i2").
				SetEndpoint("e2").
				SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1).SetState(shard.Initializing)})),
		}).
		SetShards([]uint32{1}).
		SetReplicaFactor(2).
		SetIsSharded(true)

	ps, err := sd.PlacementService(sid, placement.NewOptions())
	require.NoError(t, err)
	_, err = ps.Set(p)
	require.NoError(t, err)

	w, err := sd.Watch(sid, qopts)
	require.NoError(t, err)
	<-w.C()
	s := w.Get().(Service)
	// the heartbeat has nil value, so no filtering
	require.Equal(t, 2, len(s.Instances()))
	require.Equal(t, 1, s.Sharding().NumShards())
	require.Equal(t, 2, s.Replication().Replicas())

	mockHB, ok := m.getMockStore(sid)
	require.True(t, ok)

	hbWatchable, ok := mockHB.getWatchable(serviceKey(sid))
	require.True(t, ok)

	// heartbeat
	hbWatchable.Update([]string{"i1"})
	<-w.C()
	s = w.Get().(Service)
	require.Equal(t, 1, len(s.Instances()))
	require.Equal(t, "i1", s.Instances()[0].InstanceID())
	require.Equal(t, sid, s.Instances()[0].ServiceID())
	require.Equal(t, 1, s.Sharding().NumShards())
	require.Equal(t, 2, s.Replication().Replicas())

	hbWatchable.Update([]string{"i1", "i2"})
	<-w.C()
	s = w.Get().(Service)
	require.Equal(t, 2, len(s.Instances()))
	require.Equal(t, 1, s.Sharding().NumShards())
	require.Equal(t, 2, s.Replication().Replicas())

	hbWatchable.Update([]string{})

	<-w.C()
	s = w.Get().(Service)
	require.Equal(t, 0, len(s.Instances()))
	require.Equal(t, 1, s.Sharding().NumShards())
	require.Equal(t, 2, s.Replication().Replicas())

	hbWatchable.Update([]string{"i2"})

	<-w.C()
	s = w.Get().(Service)
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
	v, err := kvm.kv.Set(keyFnWithNamespace(placementPrefix)(sid), &metadatapb.Metadata{Port: 1})
	require.NoError(t, err)
	require.Equal(t, 2, v)

	// make sure the newly set bad value has been propagated to watches
	testWatch, err := kvm.kv.Watch(keyFnWithNamespace(placementPrefix)(sid))
	require.NoError(t, err)
	for range testWatch.C() {
		if testWatch.Get().Version() == 2 {
			break
		}
	}
	testWatch.Close()

	// make sure the bad value has been ignored
	require.Equal(t, 0, len(w.C()))
	s = w.Get().(Service)
	require.Equal(t, 1, len(s.Instances()))
	require.Equal(t, 1, s.Sharding().NumShards())
	require.True(t, s.Sharding().IsSharded())
	require.Equal(t, 2, s.Replication().Replicas())

	// now receive a update from heartbeat Store
	// will try to merge it with existing valid placement
	hbWatchable.Update([]string{"i1", "i2"})

	<-w.C()
	s = w.Get().(Service)
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
	s = w.Get().(Service)
	require.Equal(t, 2, len(s.Instances()))
	require.Equal(t, 1, s.Sharding().NumShards())
	require.True(t, s.Sharding().IsSharded())
	require.Equal(t, 2, s.Replication().Replicas())

	p = placement.NewPlacement().
		SetInstances([]placement.Instance{
			placement.NewInstance().
				SetID("i1").
				SetEndpoint("e1").
				SetShards(shard.NewShards([]shard.Shard{shard.NewShard(0).SetState(shard.Initializing)})),
		}).
		SetShards([]uint32{0}).
		SetReplicaFactor(1).
		SetIsSharded(true)

	_, err = ps.Set(p)
	require.NoError(t, err)

	// when the next valid placement came through, the watch will be updated
	<-w.C()
	s = w.Get().(Service)
	require.Equal(t, 1, len(s.Instances()))
	require.Equal(t, 1, s.Sharding().NumShards())
	require.Equal(t, 1, s.Replication().Replicas())
	require.Equal(t, true, s.Sharding().IsSharded())
}

func TestMultipleWatches(t *testing.T) {
	opts, _ := testSetup()

	qopts := NewQueryOptions()
	sid := NewServiceID().SetName("m3db").SetZone("zone1")

	p := placement.NewPlacement().
		SetInstances([]placement.Instance{
			placement.NewInstance().
				SetID("i1").
				SetEndpoint("e1").
				SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1).SetState(shard.Initializing)})),
			placement.NewInstance().
				SetID("i2").
				SetEndpoint("e2").
				SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1).SetState(shard.Initializing)})),
		}).
		SetShards([]uint32{1}).
		SetReplicaFactor(2).
		SetIsSharded(true)

	ps, err := newTestPlacementStorage(sid, opts, placement.NewOptions())
	require.NoError(t, err)

	_, err = ps.SetIfNotExist(p)
	require.NoError(t, err)

	sd, err := NewServices(opts)
	require.NoError(t, err)

	_, err = sd.Query(sid, qopts)
	require.NoError(t, err)

	w1, err := sd.Watch(sid, qopts)
	require.NoError(t, err)

	w2, err := sd.Watch(sid, qopts)
	require.NoError(t, err)

	kvm, ok := sd.(*client).kvManagers["zone1"]
	require.True(t, ok)

	require.Equal(t, 1, len(kvm.serviceWatchables))
	for _, w := range kvm.serviceWatchables {
		require.Equal(t, 2, w.watches())
	}
	<-w1.C()
	<-w2.C()

	require.Equal(t, w1.Get(), w2.Get())

	w1.Close()
	w2.Close()
}

func TestWatch_GetAfterTimeout(t *testing.T) {
	sid := NewServiceID().SetName("m3db").SetZone("zone1")
	opts, _ := testSetup()

	sd, err := NewServices(opts)
	require.NoError(t, err)

	qopts := NewQueryOptions().SetIncludeUnhealthy(true)
	_, err = sd.Watch(sid, qopts)
	require.Error(t, err)

	sd, err = NewServices(opts.SetInitTimeout(defaultInitTimeout))
	require.NoError(t, err)

	p := placement.NewPlacement().
		SetInstances([]placement.Instance{
			placement.NewInstance().
				SetID("i1").
				SetEndpoint("e1").
				SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1).SetState(shard.Initializing)})),
			placement.NewInstance().
				SetID("i2").
				SetEndpoint("e2").
				SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1).SetState(shard.Initializing)})),
		}).
		SetShards([]uint32{1}).
		SetReplicaFactor(2).
		SetIsSharded(true)

	ps, err := sd.PlacementService(sid, placement.NewOptions())
	require.NoError(t, err)
	_, err = ps.Set(p)
	require.NoError(t, err)

	sd, err = NewServices(opts)
	require.NoError(t, err)

	w, err := sd.Watch(sid, qopts)
	require.NoError(t, err)
	<-w.C()
	s := w.Get().(Service)
	require.Equal(t, 2, len(s.Instances()))
	require.Equal(t, 1, s.Sharding().NumShards())
	require.Equal(t, 2, s.Replication().Replicas())

	// up the version to 2 and delete it and set a new placement
	// to verify the watch can still receive update on the new placement
	_, err = ps.Set(p)
	require.NoError(t, err)

	err = ps.Delete()
	require.NoError(t, err)

	p = placement.NewPlacement().
		SetInstances([]placement.Instance{
			placement.NewInstance().
				SetID("i1").
				SetEndpoint("e1").
				SetShards(shard.NewShards([]shard.Shard{shard.NewShard(0).SetState(shard.Initializing)})),
		}).
		SetShards([]uint32{0}).
		SetReplicaFactor(1).
		SetIsSharded(true)

	_, err = ps.Set(p)
	require.NoError(t, err)

	for range w.C() {
		s = w.Get().(Service)
		if s.Replication().Replicas() == 1 {
			break
		}
	}
}

func TestWatchInterrupted(t *testing.T) {
	opts, _ := testSetup()
	sd, err := NewServices(opts.SetInitTimeout(0))
	require.NoError(t, err)

	testWatchInterrupted(t, sd)
}

func TestWatchInterruptedWithTimeout(t *testing.T) {
	opts, _ := testSetup()
	sd, err := NewServices(opts.SetInitTimeout(1 * time.Minute))
	require.NoError(t, err)

	testWatchInterrupted(t, sd)
}

func testWatchInterrupted(t *testing.T, s Services) {
	sid := NewServiceID().SetName("m3db").SetZone("zone1")

	interruptedCh := make(chan struct{})
	close(interruptedCh)

	qopts := NewQueryOptions().
		SetIncludeUnhealthy(true).
		SetInterruptedCh(interruptedCh)
	_, err := s.Watch(sid, qopts)
	require.Error(t, err)
	require.True(t, errors.Is(err, xos.ErrInterrupted))
}

func TestHeartbeatService(t *testing.T) {
	opts, _ := testSetup()

	sd, err := NewServices(opts)
	require.NoError(t, err)

	sid := NewServiceID()

	_, err = sd.HeartbeatService(sid)
	assert.Equal(t, errNoServiceName, err)

	sid = sid.SetName("m3db").SetZone("z1")

	hb, err := sd.HeartbeatService(sid)
	assert.NoError(t, err)
	assert.NotNil(t, hb)
}

func TestCacheCollisions_Heartbeat(t *testing.T) {
	opts, _ := testSetup()

	sid := func() ServiceID {
		return NewServiceID().SetName("svc")
	}

	sd, err := NewServices(opts)
	require.NoError(t, err)
	c := sd.(*client)

	for _, id := range []ServiceID{
		sid().SetEnvironment("e1").SetZone("z1"),
		sid().SetEnvironment("e1").SetZone("z2"),
		sid().SetEnvironment("e2").SetZone("z1"),
		sid().SetEnvironment("e2").SetZone("z2"),
	} {
		_, err := sd.HeartbeatService(id)
		assert.NoError(t, err)
	}

	assert.Equal(t, 4, len(c.hbStores), "cached hb stores should have 4 unique entries")
}

func TestCacheCollisions_Watchables(t *testing.T) {
	opts, _ := testSetup()

	sd, err := NewServices(opts.SetInitTimeout(defaultInitTimeout))
	require.NoError(t, err)

	sid := func() ServiceID {
		return NewServiceID().SetName("svc")
	}

	qopts := NewQueryOptions().SetIncludeUnhealthy(true)

	for _, id := range []ServiceID{
		sid().SetEnvironment("e1").SetZone("z1"),
		sid().SetEnvironment("e1").SetZone("z2"),
		sid().SetEnvironment("e2").SetZone("z1"),
		sid().SetEnvironment("e2").SetZone("z2"),
	} {
		_, err := sd.HeartbeatService(id)
		assert.NoError(t, err)

		ps, err := sd.PlacementService(id, placement.NewOptions())
		require.NoError(t, err)

		p := placement.NewPlacement().SetInstances([]placement.Instance{
			placement.NewInstance().SetID("i1").SetEndpoint("i:p"),
		})
		_, err = ps.Set(p)
		assert.NoError(t, err)

		_, err = sd.Watch(id, qopts)
		assert.NoError(t, err)
	}

	for _, z := range []string{"z1", "z2"} {
		kvm, err := sd.(*client).getKVManager(z)
		require.NoError(t, err)
		assert.Equal(t, 2, len(kvm.serviceWatchables), "each zone should have 2 unique watchable entries")
	}
}

func TestLeaderService(t *testing.T) {
	mc := gomock.NewController(t)
	defer mc.Finish()

	ld := newTestLeaderGen(mc)

	opts, _ := testSetup()

	opts = opts.SetLeaderGen(ld)
	cl, err := NewServices(opts)
	require.NoError(t, err)

	sid1 := NewServiceID().SetName("s1")
	sid2 := NewServiceID().SetName("s2")
	eo1 := NewElectionOptions()
	eo2 := NewElectionOptions().SetLeaderTimeout(30 * time.Second)
	eo3 := NewElectionOptions().SetResignTimeout(30 * time.Second)

	for _, sid := range []ServiceID{sid1, sid2} {
		for _, eo := range []ElectionOptions{eo1, eo2, eo3} {
			_, err := cl.LeaderService(sid, eo)
			assert.NoError(t, err)
		}
	}

	assert.Equal(t, 6, len(cl.(*client).ldSvcs),
		"should cache 6 unique client entries")
}

func TestServiceIDEqual(t *testing.T) {
	sid := NewServiceID().SetName("name").SetEnvironment("env").SetZone("zone")
	assert.Equal(t, "name", sid.Name())
	assert.Equal(t, "env", sid.Environment())
	assert.Equal(t, "zone", sid.Zone())

	assert.True(t, sid.Equal(NewServiceID().SetName("name").SetEnvironment("env").SetZone("zone")))
	assert.False(t, sid.Equal(NewServiceID().SetName("name").SetEnvironment("env")))
	assert.False(t, sid.Equal(NewServiceID().SetName("name").SetZone("zone")))
	assert.False(t, sid.Equal(NewServiceID().SetEnvironment("env").SetZone("zone")))
	assert.False(t, sid.Equal(nil))
}

func newTestLeaderGen(mc *gomock.Controller) LeaderGen {
	svc := NewMockLeaderService(mc)
	return func(sid ServiceID, eo ElectionOptions) (LeaderService, error) {
		return svc, nil
	}
}

func testSetup() (Options, *mockHBGen) {
	var (
		lock   sync.Mutex
		stores = make(map[string]kv.Store)
	)
	kvGen := func(zone string) (kv.Store, error) {
		lock.Lock()
		store, ok := stores[zone]
		if ok {
			lock.Unlock()
			return store, nil
		}
		store = mem.NewStore()
		stores[zone] = store
		lock.Unlock()
		return store, nil
	}

	m := &mockHBGen{
		hbs: map[string]*mockHBStore{},
	}
	hbGen := func(sid ServiceID) (HeartbeatService, error) {
		return m.genMockStore(sid)
	}

	return NewOptions().
		SetKVGen(kvGen).
		SetHeartbeatGen(hbGen).
		SetLeaderGen(emptyLdGen).
		SetInitTimeout(100 * time.Millisecond).
		SetInstrumentsOptions(instrument.NewOptions()), m
}

type mockHBGen struct {
	sync.Mutex

	hbs map[string]*mockHBStore
}

func (m *mockHBGen) genMockStore(sid ServiceID) (*mockHBStore, error) {
	k := serviceKey(sid)

	m.Lock()
	defer m.Unlock()

	s, ok := m.hbs[k]
	if ok {
		return s, nil
	}
	s = &mockHBStore{
		hbs:        map[string]map[string]time.Time{},
		watchables: map[string]xwatch.Watchable{},
		sid:        sid,
	}

	m.hbs[k] = s
	return s, nil
}

func (m *mockHBGen) getMockStore(sid ServiceID) (*mockHBStore, bool) {
	m.Lock()
	defer m.Unlock()

	s, ok := m.hbs[serviceKey(sid)]
	return s, ok
}

type mockHBStore struct {
	sync.Mutex

	sid        ServiceID
	hbs        map[string]map[string]time.Time
	watchables map[string]xwatch.Watchable
}

func (hb *mockHBStore) Heartbeat(instance placement.Instance, ttl time.Duration) error {
	hb.Lock()
	defer hb.Unlock()
	hbMap, ok := hb.hbs[serviceKey(hb.sid)]
	if !ok {
		hbMap = map[string]time.Time{}
		hb.hbs[serviceKey(hb.sid)] = hbMap
	}
	hbMap[instance.ID()] = time.Now()
	return nil
}

func (hb *mockHBStore) Get() ([]string, error) {
	hb.Lock()
	defer hb.Unlock()

	var r []string
	hbMap, ok := hb.hbs[serviceKey(hb.sid)]
	if !ok {
		return r, nil
	}

	r = make([]string, 0, len(hbMap))
	for k := range hbMap {
		r = append(r, k)
	}
	return r, nil
}

func (hb *mockHBStore) GetInstances() ([]placement.Instance, error) {
	hb.Lock()
	defer hb.Unlock()

	var r []placement.Instance
	hbMap, ok := hb.hbs[serviceKey(hb.sid)]
	if !ok {
		return r, nil
	}

	r = make([]placement.Instance, 0, len(hbMap))
	for k := range hbMap {
		r = append(r, placement.NewInstance().SetID(k))
	}
	return r, nil
}

func (hb *mockHBStore) Watch() (xwatch.Watch, error) {
	hb.Lock()
	defer hb.Unlock()

	watchable, ok := hb.watchables[serviceKey(hb.sid)]
	if ok {
		_, w, err := watchable.Watch()
		return w, err
	}

	watchable = xwatch.NewWatchable()
	hb.watchables[serviceKey(hb.sid)] = watchable

	_, w, err := watchable.Watch()
	return w, err
}

func (hb *mockHBStore) getWatchable(s string) (xwatch.Watchable, bool) {
	hb.Lock()
	defer hb.Unlock()

	w, ok := hb.watchables[s]
	return w, ok
}

func (hb *mockHBStore) Delete(id string) error {
	hb.Lock()
	defer hb.Unlock()

	hbMap, ok := hb.hbs[serviceKey(hb.sid)]
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

func newTestPlacementStorage(sid ServiceID, opts Options, pOpts placement.Options) (placement.Storage, error) {
	if opts.KVGen() == nil {
		return nil, errNoKVGen
	}
	store, err := opts.KVGen()(sid.Zone())
	if err != nil {
		return nil, err
	}
	return storage.NewPlacementStorage(
		store,
		keyFnWithNamespace(placementNamespace(opts.NamespaceOptions().PlacementNamespace()))(sid),
		pOpts,
	), nil
}
