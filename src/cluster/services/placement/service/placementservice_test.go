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

package service

import (
	"errors"
	"math/rand"
	"sort"
	"sync"
	"testing"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
	"github.com/m3db/m3cluster/shard"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testServiceID() services.ServiceID {
	return services.NewServiceID().SetName("test_service")
}

func TestGoodWorkflow(t *testing.T) {
	p := NewPlacementService(NewMockStorage(), testServiceID(), placement.NewOptions())
	testGoodWorkflow(t, p)

	p = NewPlacementService(NewMockStorage(), testServiceID(), placement.NewOptions().SetLooseRackCheck(true))
	testGoodWorkflow(t, p)
}

func TestManyShards(t *testing.T) {
	p := NewPlacementService(NewMockStorage(), testServiceID(), placement.NewOptions())
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 2)
	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 2)
	i3 := placement.NewEmptyInstance("i3", "r3", "z1", "endpoint", 2)
	i4 := placement.NewEmptyInstance("i4", "r1", "z1", "endpoint", 2)
	i5 := placement.NewEmptyInstance("i5", "r2", "z1", "endpoint", 2)
	i6 := placement.NewEmptyInstance("i6", "r3", "z1", "endpoint", 2)
	_, err := p.BuildInitialPlacement([]services.PlacementInstance{i1, i2, i3, i4, i5, i6}, 8192, 1)

	assert.NoError(t, err)
}

func assertPlacementInstanceEqualExceptShards(
	t *testing.T,
	expected services.PlacementInstance,
	observed services.PlacementInstance,
) {
	assert.Equal(t, expected.ID(), observed.ID())
	assert.Equal(t, expected.Rack(), observed.Rack())
	assert.Equal(t, expected.Zone(), observed.Zone())
	assert.Equal(t, expected.Weight(), observed.Weight())
	assert.Equal(t, expected.Endpoint(), observed.Endpoint())
	assert.True(t, len(observed.Shards().All()) > 0)
}

func testGoodWorkflow(t *testing.T, p services.PlacementService) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 2)
	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 2)
	i3 := placement.NewEmptyInstance("i3", "r3", "z1", "endpoint", 2)
	_, err := p.BuildInitialPlacement([]services.PlacementInstance{i1, i2}, 10, 1)
	assert.NoError(t, err)

	_, err = p.AddReplica()
	assert.NoError(t, err)

	for _, instance := range []services.PlacementInstance{i1, i2} {
		err = p.MarkInstanceAvailable(instance.ID())
		assert.NoError(t, err)
	}
	_, ai, err := p.AddInstances([]services.PlacementInstance{i3})
	assert.NoError(t, err)
	assertPlacementInstanceEqualExceptShards(t, i3, ai[0])

	err = p.MarkInstanceAvailable(i3.ID())
	assert.NoError(t, err)

	_, err = p.RemoveInstances([]string{i1.ID()})
	assert.NoError(t, err)

	markAllInstancesAvailable(t, p)

	var (
		i21 = placement.NewEmptyInstance("i21", "r2", "z1", "endpoint", 1)
		i4  = placement.NewEmptyInstance("i4", "r4", "z1", "endpoint", 1)
	)
	_, usedInstances, err := p.ReplaceInstance(
		i2.ID(),
		[]services.PlacementInstance{i21, i4,
			i3, // already in placement
			placement.NewEmptyInstance("i31", "r3", "z1", "endpoint", 1), // conflict
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(usedInstances))
	assertPlacementInstanceEqualExceptShards(t, i21, usedInstances[0])
	assertPlacementInstanceEqualExceptShards(t, i4, usedInstances[1])

	for _, id := range []string{"i21", "i4"} {
		err = p.MarkInstanceAvailable(id)
		assert.NoError(t, err)
	}

	s, _, err := p.Placement()
	assert.NoError(t, err)
	assert.Equal(t, 3, s.NumInstances())
	_, exist := s.Instance("i21")
	assert.True(t, exist)
	_, exist = s.Instance("i4")
	assert.True(t, exist)

	_, ai, err = p.AddInstances([]services.PlacementInstance{i1})
	assert.NoError(t, err)
	assertPlacementInstanceEqualExceptShards(t, i1, ai[0])

	i24 := placement.NewEmptyInstance("i24", "r2", "z1", "endpoint", 1)
	_, ai, err = p.AddInstances([]services.PlacementInstance{i24})
	assert.NoError(t, err)
	assertPlacementInstanceEqualExceptShards(t, i24, ai[0])

	i34 := placement.NewEmptyInstance("i34", "r3", "z1", "endpoint", 1)
	_, ai, err = p.AddInstances([]services.PlacementInstance{i34})
	assert.NoError(t, err)
	assertPlacementInstanceEqualExceptShards(t, i34, ai[0])

	i35 := placement.NewEmptyInstance("i35", "r3", "z1", "endpoint", 1)
	_, ai, err = p.AddInstances([]services.PlacementInstance{i35})
	assert.NoError(t, err)
	assertPlacementInstanceEqualExceptShards(t, i35, ai[0])

	i41 := placement.NewEmptyInstance("i41", "r4", "z1", "endpoint", 1)
	instances := []services.PlacementInstance{
		placement.NewEmptyInstance("i15", "r1", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i34", "r3", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i35", "r3", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i36", "r3", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i23", "r2", "z1", "endpoint", 1),
		i41,
	}
	_, ai, err = p.AddInstances(instances)
	assert.NoError(t, err)
	assertPlacementInstanceEqualExceptShards(t, i41, ai[0])
	s, _, err = p.Placement()
	assert.NoError(t, err)
	_, exist = s.Instance("i41") // instance added from least weighted rack
	assert.True(t, exist)
}

func TestNonShardedWorkflow(t *testing.T) {
	ps := NewPlacementService(NewMockStorage(), testServiceID(), placement.NewOptions().SetIsSharded(false))

	_, err := ps.BuildInitialPlacement([]services.PlacementInstance{
		placement.NewEmptyInstance("i1", "r1", "z1", "e1", 1),
		placement.NewEmptyInstance("i2", "r1", "z1", "e2", 1),
	}, 10, 1)
	assert.Error(t, err)

	p, err := ps.BuildInitialPlacement([]services.PlacementInstance{
		placement.NewEmptyInstance("i1", "r1", "z1", "e1", 1),
		placement.NewEmptyInstance("i2", "r1", "z1", "e2", 1),
	}, 0, 1)
	assert.NoError(t, err)
	assert.Equal(t, 2, p.NumInstances())
	assert.Equal(t, 0, p.NumShards())
	assert.Equal(t, 1, p.ReplicaFactor())
	assert.False(t, p.IsSharded())

	p, err = ps.AddReplica()
	assert.NoError(t, err)
	assert.Equal(t, 2, p.NumInstances())
	assert.Equal(t, 0, p.NumShards())
	assert.Equal(t, 2, p.ReplicaFactor())
	assert.False(t, p.IsSharded())

	i3 := placement.NewEmptyInstance("i3", "r1", "z1", "e3", 1)
	i4 := placement.NewEmptyInstance("i4", "r1", "z1", "e4", 1)
	p, ai, err := ps.AddInstances([]services.PlacementInstance{i3, i4})
	assert.NoError(t, err)
	assert.Equal(t, i3, ai[0])
	assert.Equal(t, 3, p.NumInstances())
	assert.Equal(t, 0, p.NumShards())
	assert.Equal(t, 2, p.ReplicaFactor())
	assert.False(t, p.IsSharded())

	p, err = ps.RemoveInstances([]string{"i1"})
	assert.NoError(t, err)
	assert.Equal(t, 2, p.NumInstances())
	assert.Equal(t, 0, p.NumShards())
	assert.Equal(t, 2, p.ReplicaFactor())
	assert.False(t, p.IsSharded())

	p, usedInstances, err := ps.ReplaceInstance("i2", []services.PlacementInstance{i3, i4})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(usedInstances))
	assert.Equal(t, i4, usedInstances[0])
	assert.Equal(t, 2, p.NumInstances())
	assert.Equal(t, 0, p.NumShards())
	assert.Equal(t, 2, p.ReplicaFactor())
	assert.False(t, p.IsSharded())

	// nothing happens because i3 has no shards
	err = ps.MarkInstanceAvailable("i3")
	assert.NoError(t, err)
	assert.Equal(t, 2, p.NumInstances())
	assert.Equal(t, 0, p.NumShards())
	assert.Equal(t, 2, p.ReplicaFactor())
	assert.False(t, p.IsSharded())
}

func TestDryrun(t *testing.T) {
	m := NewMockStorage()
	sid := testServiceID()
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 2)
	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 2)
	i3 := placement.NewEmptyInstance("i3", "r3", "z1", "endpoint", 2)
	dryrunPS := NewPlacementService(m, sid, placement.NewOptions().SetDryrun(true))
	ps := NewPlacementService(m, sid, placement.NewOptions())

	_, err := dryrunPS.BuildInitialPlacement([]services.PlacementInstance{i1, i2}, 10, 2)
	assert.NoError(t, err)

	_, _, err = m.Placement(sid)
	assert.Error(t, err)

	_, err = ps.BuildInitialPlacement([]services.PlacementInstance{i1, i2}, 10, 2)
	assert.NoError(t, err)

	_, v, err := m.Placement(sid)
	assert.NoError(t, err)
	assert.Equal(t, 1, v)

	_, ai, err := dryrunPS.AddInstances([]services.PlacementInstance{i3})
	assert.NoError(t, err)
	assertPlacementInstanceEqualExceptShards(t, i3, ai[0])

	_, v, _ = m.Placement(sid)
	assert.Equal(t, 1, v)

	_, ai, err = ps.AddInstances([]services.PlacementInstance{i3})
	assert.NoError(t, err)
	assertPlacementInstanceEqualExceptShards(t, i3, ai[0])

	_, v, _ = m.Placement(sid)
	assert.Equal(t, 2, v)

	_, err = dryrunPS.RemoveInstances([]string{"i3"})
	assert.NoError(t, err)

	_, v, _ = m.Placement(sid)
	assert.Equal(t, 2, v)

	_, err = ps.RemoveInstances([]string{"i3"})
	assert.NoError(t, err)

	_, v, _ = m.Placement(sid)
	assert.Equal(t, 3, v)

	_, usedInstances, err := dryrunPS.ReplaceInstance("i2", []services.PlacementInstance{i3})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(usedInstances))
	assertPlacementInstanceEqualExceptShards(t, i3, usedInstances[0])

	_, v, _ = m.Placement(sid)
	assert.Equal(t, 3, v)

	_, usedInstances, err = ps.ReplaceInstance("i2", []services.PlacementInstance{i3})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(usedInstances))
	assertPlacementInstanceEqualExceptShards(t, i3, usedInstances[0])

	p, v, _ := m.Placement(sid)
	assert.Equal(t, 4, v)

	err = dryrunPS.SetPlacement(p)
	assert.NoError(t, err)

	_, v, _ = m.Placement(sid)
	assert.Equal(t, 4, v)

	err = ps.SetPlacement(p)
	assert.NoError(t, err)

	_, v, _ = m.Placement(sid)
	assert.Equal(t, 5, v)

	err = dryrunPS.Delete()
	assert.NoError(t, err)

	_, v, err = m.Placement(sid)
	assert.NoError(t, err)
	assert.Equal(t, 5, v)

	err = ps.Delete()
	assert.NoError(t, err)

	_, _, err = m.Placement(sid)
	assert.Error(t, err)
}

func TestGetValidCandidates(t *testing.T) {
	i1 := placement.NewInstance().SetID("i1").SetZone("z")
	i1.Shards().Add(shard.NewShard(0).SetState(shard.Available))

	i2 := placement.NewInstance().SetID("i2").SetZone("z")
	i2.Shards().Add(shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i2"))

	i3 := placement.NewInstance().SetID("i3").SetZone("z")
	i3.Shards().Add(shard.NewShard(0).SetState(shard.Leaving))

	p := placement.NewPlacement().
		SetInstances([]services.PlacementInstance{i1, i2, i3}).
		SetIsSharded(true).
		SetReplicaFactor(2).
		SetShards([]uint32{0})

	emptyI3 := placement.NewInstance().SetID("i3").SetZone("z3")
	i4 := placement.NewInstance().SetID("i4").SetZone("z")
	candidates := []services.PlacementInstance{i3, emptyI3, i1, i4}
	res := getValidCandidates(p, candidates, placement.NewOptions())
	assert.Equal(t, []services.PlacementInstance{i3, i3, i4}, res)
}

func TestBadInitialPlacement(t *testing.T) {
	p := NewPlacementService(NewMockStorage(), testServiceID(), placement.NewOptions().SetIsSharded(false))

	// invalid numShards
	_, err := p.BuildInitialPlacement([]services.PlacementInstance{
		placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i2", "r1", "z1", "endpoint", 1),
	}, -1, 1)
	assert.Error(t, err)

	// invalid rf
	_, err = p.BuildInitialPlacement([]services.PlacementInstance{
		placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i2", "r1", "z1", "endpoint", 1),
	}, 10, 0)
	assert.Error(t, err)

	// numshards > 0 && sharded == false
	_, err = p.BuildInitialPlacement([]services.PlacementInstance{
		placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i2", "r1", "z1", "endpoint", 1),
	}, 10, 1)
	assert.Error(t, err)

	p = NewPlacementService(NewMockStorage(), testServiceID(), placement.NewOptions())

	// not enough instances
	_, err = p.BuildInitialPlacement([]services.PlacementInstance{}, 10, 1)
	assert.Error(t, err)

	// err: rf == 0 && sharded == true
	_, err = p.BuildInitialPlacement([]services.PlacementInstance{
		placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i2", "r1", "z1", "endpoint", 1),
	}, 10, 0)
	assert.Error(t, err)

	// not enough racks
	_, err = p.BuildInitialPlacement([]services.PlacementInstance{
		placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i2", "r1", "z1", "endpoint", 1),
	}, 100, 2)
	assert.Error(t, err)

	// too many zones
	_, err = p.BuildInitialPlacement([]services.PlacementInstance{
		placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i2", "r2", "z2", "endpoint", 1),
	}, 100, 2)
	assert.Error(t, err)
	assert.Equal(t, errMultipleZones, err)

	_, err = p.BuildInitialPlacement([]services.PlacementInstance{
		placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1),
	}, 100, 2)
	assert.NoError(t, err)

	// placement already exist
	_, err = p.BuildInitialPlacement([]services.PlacementInstance{
		placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1),
	}, 100, 2)
	assert.Error(t, err)
}

func TestBadAddReplica(t *testing.T) {
	p := NewPlacementService(NewMockStorage(), testServiceID(), placement.NewOptions())

	_, err := p.BuildInitialPlacement(
		[]services.PlacementInstance{placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)},
		10, 1)
	assert.NoError(t, err)

	// not enough racks/instances
	_, err = p.AddReplica()
	assert.Error(t, err)

	// could not find placement for service
	p = NewPlacementService(NewMockStorage(), testServiceID(), placement.NewOptions())
	_, err = p.AddReplica()
	assert.Error(t, err)
}

func TestBadAddInstance(t *testing.T) {
	ms := NewMockStorage()
	p := NewPlacementService(ms, testServiceID(), placement.NewOptions())

	_, err := p.BuildInitialPlacement(
		[]services.PlacementInstance{placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)},
		10, 1)
	assert.NoError(t, err)

	// adding instance already exist
	_, _, err = p.AddInstances([]services.PlacementInstance{placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)})
	assert.Error(t, err)

	// too many zones
	_, _, err = p.AddInstances([]services.PlacementInstance{placement.NewEmptyInstance("i2", "r2", "z2", "endpoint", 1)})
	assert.Error(t, err)
	assert.Equal(t, errNoValidInstance, err)

	p = NewPlacementService(ms, testServiceID(), placement.NewOptions())
	_, _, err = p.AddInstances([]services.PlacementInstance{placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)})
	assert.Error(t, err)

	// could not find placement for service
	p = NewPlacementService(NewMockStorage(), testServiceID(), placement.NewOptions())
	_, _, err = p.AddInstances([]services.PlacementInstance{placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)})
	assert.Error(t, err)
}

func TestBadRemoveInstance(t *testing.T) {
	p := NewPlacementService(NewMockStorage(), testServiceID(), placement.NewOptions())

	_, err := p.BuildInitialPlacement(
		[]services.PlacementInstance{placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)},
		10, 1)
	assert.NoError(t, err)

	// leaving instance not exist
	_, err = p.RemoveInstances([]string{"not_exist"})
	assert.Error(t, err)

	// not enough racks/instances after removal
	_, err = p.RemoveInstances([]string{"i1"})
	assert.Error(t, err)

	// could not find placement for service
	p = NewPlacementService(NewMockStorage(), testServiceID(), placement.NewOptions())
	_, err = p.RemoveInstances([]string{"i1"})
	assert.Error(t, err)
}

func TestBadReplaceInstance(t *testing.T) {
	p := NewPlacementService(NewMockStorage(), testServiceID(), placement.NewOptions())

	_, err := p.BuildInitialPlacement([]services.PlacementInstance{
		placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i4", "r4", "z1", "endpoint", 1),
	}, 10, 1)
	assert.NoError(t, err)

	// leaving instance not exist
	_, _, err = p.ReplaceInstance(
		"not_exist",
		[]services.PlacementInstance{placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)},
	)
	assert.Error(t, err)

	// adding instance already exist
	_, _, err = p.ReplaceInstance(
		"i1",
		[]services.PlacementInstance{placement.NewEmptyInstance("i4", "r4", "z1", "endpoint", 1)},
	)
	assert.Error(t, err)

	// not enough rack after replace
	_, err = p.AddReplica()
	assert.NoError(t, err)
	_, _, err = p.ReplaceInstance(
		"i4",
		[]services.PlacementInstance{placement.NewEmptyInstance("i12", "r1", "z1", "endpoint", 1)},
	)
	assert.Error(t, err)

	// could not find placement for service
	p = NewPlacementService(NewMockStorage(), testServiceID(), placement.NewOptions())
	_, _, err = p.ReplaceInstance(
		"i1",
		[]services.PlacementInstance{placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)},
	)
	assert.Error(t, err)
}

func TestReplaceInstanceWithLooseRackCheck(t *testing.T) {
	p := NewPlacementService(NewMockStorage(), testServiceID(), placement.NewOptions().SetLooseRackCheck(true))

	_, err := p.BuildInitialPlacement(
		[]services.PlacementInstance{
			placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1),
			placement.NewEmptyInstance("i4", "r4", "z1", "endpoint", 1),
		}, 10, 1)
	assert.NoError(t, err)

	// leaving instance not exist
	_, _, err = p.ReplaceInstance(
		"not_exist",
		[]services.PlacementInstance{placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)},
	)
	assert.Error(t, err)

	// adding instance already exist
	_, _, err = p.ReplaceInstance(
		"i1",
		[]services.PlacementInstance{placement.NewEmptyInstance("i4", "r4", "z1", "endpoint", 1)},
	)
	assert.Error(t, err)

	// NO ERROR when not enough rack after replace
	_, err = p.AddReplica()
	assert.NoError(t, err)
	i12 := placement.NewEmptyInstance("i12", "r1", "z1", "endpoint", 1)
	_, usedInstances, err := p.ReplaceInstance("i4", []services.PlacementInstance{i12})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(usedInstances))
	assertPlacementInstanceEqualExceptShards(t, i12, usedInstances[0])

	// could not find placement for service
	p = NewPlacementService(NewMockStorage(), testServiceID(), placement.NewOptions())
	_, _, err = p.ReplaceInstance(
		"i1",
		[]services.PlacementInstance{placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)},
	)
	assert.Error(t, err)
}

func TestMarkShard(t *testing.T) {
	ms := NewMockStorage()
	sid := testServiceID()

	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Leaving))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	i2 := placement.NewEmptyInstance("i2", "r1", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(4).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(5).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(6).SetState(shard.Available))

	i3 := placement.NewEmptyInstance("i3", "r2", "z1", "endpoint", 1)
	i3.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i3.Shards().Add(shard.NewShard(3).SetState(shard.Available))
	i3.Shards().Add(shard.NewShard(5).SetState(shard.Available))

	i4 := placement.NewEmptyInstance("i4", "r2", "z1", "endpoint", 1)
	i4.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i4.Shards().Add(shard.NewShard(4).SetState(shard.Available))
	i4.Shards().Add(shard.NewShard(6).SetState(shard.Available))

	i5 := placement.NewEmptyInstance("i5", "r2", "z1", "endpoint", 1)
	i5.Shards().Add(shard.NewShard(1).SetState(shard.Initializing).SetSourceID("i1"))

	instances := []services.PlacementInstance{i1, i2, i3, i4, i5}
	p := placement.NewPlacement().
		SetInstances(instances).
		SetShards([]uint32{1, 2, 3, 4, 5, 6}).
		SetReplicaFactor(2).
		SetIsSharded(true)
	err := ms.SetIfNotExist(sid, p)
	assert.NoError(t, err)

	ps := NewPlacementService(ms, sid, placement.NewOptions())
	err = ps.MarkShardAvailable("i5", 1)
	assert.NoError(t, err)
	p, _, err = ms.Placement(sid)
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
	for _, instance := range p.Instances() {
		for _, s := range instance.Shards().All() {
			assert.Equal(t, shard.Available, s.State())
		}
	}

	err = ps.MarkShardAvailable("i1", 1)
	assert.Error(t, err)

	err = ps.MarkShardAvailable("i5", 5)
	assert.Error(t, err)
}

func TestMarkInstance(t *testing.T) {
	ms := NewMockStorage()
	sid := testServiceID()

	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Leaving))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	i2 := placement.NewEmptyInstance("i2", "r1", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(4).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(5).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(6).SetState(shard.Available))

	i3 := placement.NewEmptyInstance("i3", "r2", "z1", "endpoint", 1)
	i3.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i3.Shards().Add(shard.NewShard(3).SetState(shard.Available))
	i3.Shards().Add(shard.NewShard(5).SetState(shard.Available))

	i4 := placement.NewEmptyInstance("i4", "r2", "z1", "endpoint", 1)
	i4.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i4.Shards().Add(shard.NewShard(4).SetState(shard.Available))
	i4.Shards().Add(shard.NewShard(6).SetState(shard.Available))

	i5 := placement.NewEmptyInstance("i5", "r2", "z1", "endpoint", 1)
	i5.Shards().Add(shard.NewShard(1).SetState(shard.Initializing).SetSourceID("i1"))
	i5.Shards().Add(shard.NewShard(2).SetState(shard.Initializing).SetSourceID("i1"))

	instances := []services.PlacementInstance{i1, i2, i3, i4, i5}
	p := placement.NewPlacement().
		SetInstances(instances).
		SetShards([]uint32{1, 2, 3, 4, 5, 6}).
		SetReplicaFactor(2).
		SetIsSharded(true)
	err := ms.SetIfNotExist(sid, p)
	assert.NoError(t, err)

	ps := NewPlacementService(ms, sid, placement.NewOptions())
	// marking shard 2 will fail
	err = ps.MarkInstanceAvailable("i5")
	assert.Error(t, err)

	// instance not exist
	err = ps.MarkInstanceAvailable("i6")
	assert.Error(t, err)

	i5.Shards().Remove(2)
	ms = NewMockStorage()
	ms.SetIfNotExist(sid, p)
	ps = NewPlacementService(ms, sid, placement.NewOptions())
	err = ps.MarkInstanceAvailable("i5")
	assert.NoError(t, err)
}

func TestFindReplaceInstance(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r11", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	i10 := placement.NewEmptyInstance("i10", "r11", "z1", "endpoint", 1)
	i10.Shards().Add(shard.NewShard(4).SetState(shard.Available))
	i10.Shards().Add(shard.NewShard(5).SetState(shard.Available))

	i2 := placement.NewEmptyInstance("i2", "r12", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(6).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(7).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(8).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(9).SetState(shard.Available))

	i3 := placement.NewEmptyInstance("i3", "r13", "z1", "endpoint", 3)
	i3.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i3.Shards().Add(shard.NewShard(3).SetState(shard.Available))
	i3.Shards().Add(shard.NewShard(4).SetState(shard.Available))
	i3.Shards().Add(shard.NewShard(5).SetState(shard.Available))
	i3.Shards().Add(shard.NewShard(6).SetState(shard.Available))

	i4 := placement.NewEmptyInstance("i4", "r14", "z1", "endpoint", 1)
	i4.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i4.Shards().Add(shard.NewShard(7).SetState(shard.Available))
	i4.Shards().Add(shard.NewShard(8).SetState(shard.Available))
	i4.Shards().Add(shard.NewShard(9).SetState(shard.Available))

	instances := []services.PlacementInstance{i1, i2, i3, i4, i10}

	ids := []uint32{1, 2, 3, 4, 5, 6, 7, 8}
	s := placement.NewPlacement().SetInstances(instances).SetShards(ids).SetReplicaFactor(2)

	candidates := []services.PlacementInstance{
		placement.NewEmptyInstance("i11", "r11", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i22", "r22", "z2", "endpoint", 1), // bad zone
	}

	p := NewPlacementService(NewMockStorage(), testServiceID(), placement.NewOptions()).(placementService)
	i, err := p.findReplaceInstance(s, candidates, i4)
	assert.Error(t, err)
	assert.Nil(t, i)

	noConflictCandidates := []services.PlacementInstance{
		placement.NewEmptyInstance("i11", "r0", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i22", "r0", "z2", "endpoint", 1),
	}
	i, err = p.findReplaceInstance(s, noConflictCandidates, i3)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "could not find enough instance to replace")
	assert.Nil(t, i)

	p = NewPlacementService(NewMockStorage(), testServiceID(), placement.NewOptions().SetLooseRackCheck(true)).(placementService)
	i, err = p.findReplaceInstance(s, candidates, i4)
	assert.NoError(t, err)
	// gonna prefer r1 because r1 would only conflict shard 2, r2 would conflict 7,8,9
	assert.Equal(t, 1, len(i))
	assert.Equal(t, "r11", i[0].Rack())
}

func TestSetPlacement(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	p := placement.NewPlacement().
		SetInstances([]services.PlacementInstance{i1, i2}).
		SetShards([]uint32{1, 2}).
		SetReplicaFactor(1).
		SetIsSharded(true)

	ps := NewPlacementService(NewMockStorage(), testServiceID(), placement.NewOptions())
	err := ps.SetPlacement(p)
	assert.NoError(t, err)
	pGet, v, err := ps.Placement()
	assert.NoError(t, err)
	assert.Equal(t, 1, v)
	assert.Equal(t, p, pGet)

	// validation error
	err = ps.SetPlacement(placement.NewPlacement().
		SetInstances([]services.PlacementInstance{i1, i2}).
		SetShards([]uint32{1, 2}).
		SetReplicaFactor(2),
	)
	assert.Error(t, err)

	err = ps.SetPlacement(p)
	assert.NoError(t, err)

	p, v, err = ps.Placement()
	assert.NoError(t, err)
	assert.Equal(t, 2, v)
	assert.Equal(t, p, pGet)
}

func TestDeletePlacement(t *testing.T) {
	m := NewMockStorage()

	ps := NewPlacementService(m, testServiceID(), placement.NewOptions())

	err := ps.Delete()
	assert.Error(t, err)

	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	p := placement.NewPlacement().
		SetInstances([]services.PlacementInstance{i1, i2}).
		SetShards([]uint32{1, 2}).
		SetReplicaFactor(1).
		SetIsSharded(true)

	err = ps.SetPlacement(p)
	assert.NoError(t, err)

	_, v, err := ps.Placement()
	assert.NoError(t, err)
	assert.Equal(t, 1, v)

	err = ps.Delete()
	assert.NoError(t, err)

	err = ps.Delete()
	assert.Error(t, err)

	err = ps.SetPlacement(p)
	assert.NoError(t, err)

	_, v, err = ps.Placement()
	assert.NoError(t, err)
	assert.Equal(t, 1, v)
}

func TestGroupInstancesByConflict(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "", "", "endpoint", 1)
	i2 := placement.NewEmptyInstance("i2", "", "", "endpoint", 1)
	i3 := placement.NewEmptyInstance("i3", "", "", "endpoint", 1)
	i4 := placement.NewEmptyInstance("i4", "", "", "endpoint", 2)
	instanceConflicts := []sortableValue{
		sortableValue{value: i1, weight: 1},
		sortableValue{value: i2, weight: 0},
		sortableValue{value: i3, weight: 3},
		sortableValue{value: i4, weight: 2},
	}

	groups := groupInstancesByConflict(instanceConflicts, true)
	assert.Equal(t, 4, len(groups))
	assert.Equal(t, i2, groups[0][0])
	assert.Equal(t, i1, groups[1][0])
	assert.Equal(t, i4, groups[2][0])
	assert.Equal(t, i3, groups[3][0])

	groups = groupInstancesByConflict(instanceConflicts, false)
	assert.Equal(t, 1, len(groups))
	assert.Equal(t, i2, groups[0][0])
}

func TestKnapSack(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "", "", "endpoint", 40000)
	i2 := placement.NewEmptyInstance("i2", "", "", "endpoint", 20000)
	i3 := placement.NewEmptyInstance("i3", "", "", "endpoint", 80000)
	i4 := placement.NewEmptyInstance("i4", "", "", "endpoint", 50000)
	i5 := placement.NewEmptyInstance("i5", "", "", "endpoint", 190000)
	instances := []services.PlacementInstance{i1, i2, i3, i4, i5}

	res, leftWeight := knapsack(instances, 10000)
	assert.Equal(t, -10000, leftWeight)
	assert.Equal(t, []services.PlacementInstance{i2}, res)

	res, leftWeight = knapsack(instances, 20000)
	assert.Equal(t, 0, leftWeight)
	assert.Equal(t, []services.PlacementInstance{i2}, res)

	res, leftWeight = knapsack(instances, 30000)
	assert.Equal(t, -10000, leftWeight)
	assert.Equal(t, []services.PlacementInstance{i1}, res)

	res, leftWeight = knapsack(instances, 60000)
	assert.Equal(t, 0, leftWeight)
	assert.Equal(t, []services.PlacementInstance{i1, i2}, res)

	res, leftWeight = knapsack(instances, 120000)
	assert.Equal(t, 0, leftWeight)
	assert.Equal(t, []services.PlacementInstance{i1, i3}, res)

	res, leftWeight = knapsack(instances, 170000)
	assert.Equal(t, 0, leftWeight)
	assert.Equal(t, []services.PlacementInstance{i1, i3, i4}, res)

	res, leftWeight = knapsack(instances, 190000)
	assert.Equal(t, 0, leftWeight)
	// will prefer i5 than i1+i2+i3+i4
	assert.Equal(t, []services.PlacementInstance{i5}, res)

	res, leftWeight = knapsack(instances, 200000)
	assert.Equal(t, -10000, leftWeight)
	assert.Equal(t, []services.PlacementInstance{i2, i5}, res)

	res, leftWeight = knapsack(instances, 210000)
	assert.Equal(t, 0, leftWeight)
	assert.Equal(t, []services.PlacementInstance{i2, i5}, res)

	res, leftWeight = knapsack(instances, 400000)
	assert.Equal(t, 20000, leftWeight)
	assert.Equal(t, []services.PlacementInstance{i1, i2, i3, i4, i5}, res)
}

func TestFillWeight(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "", "", "endpoint", 4)
	i2 := placement.NewEmptyInstance("i2", "", "", "endpoint", 2)
	i3 := placement.NewEmptyInstance("i3", "", "", "endpoint", 8)
	i4 := placement.NewEmptyInstance("i4", "", "", "endpoint", 5)
	i5 := placement.NewEmptyInstance("i5", "", "", "endpoint", 19)

	i6 := placement.NewEmptyInstance("i6", "", "", "endpoint", 3)
	i7 := placement.NewEmptyInstance("i7", "", "", "endpoint", 7)
	groups := [][]services.PlacementInstance{
		[]services.PlacementInstance{i1, i2, i3, i4, i5},
		[]services.PlacementInstance{i6, i7},
	}

	// When targetWeight is smaller than 38, the first group will satisfy
	res, leftWeight := fillWeight(groups, 1)
	assert.Equal(t, -1, leftWeight)
	assert.Equal(t, []services.PlacementInstance{i2}, res)

	res, leftWeight = fillWeight(groups, 2)
	assert.Equal(t, 0, leftWeight)
	assert.Equal(t, []services.PlacementInstance{i2}, res)

	res, leftWeight = fillWeight(groups, 17)
	assert.Equal(t, 0, leftWeight)
	assert.Equal(t, []services.PlacementInstance{i1, i3, i4}, res)

	res, leftWeight = fillWeight(groups, 20)
	assert.Equal(t, -1, leftWeight)
	assert.Equal(t, []services.PlacementInstance{i2, i5}, res)

	// When targetWeight is bigger than 38, need to get instance from group 2
	res, leftWeight = fillWeight(groups, 40)
	assert.Equal(t, -1, leftWeight)
	assert.Equal(t, []services.PlacementInstance{i1, i2, i3, i4, i5, i6}, res)

	res, leftWeight = fillWeight(groups, 41)
	assert.Equal(t, 0, leftWeight)
	assert.Equal(t, []services.PlacementInstance{i1, i2, i3, i4, i5, i6}, res)

	res, leftWeight = fillWeight(groups, 47)
	assert.Equal(t, -1, leftWeight)
	assert.Equal(t, []services.PlacementInstance{i1, i2, i3, i4, i5, i6, i7}, res)

	res, leftWeight = fillWeight(groups, 48)
	assert.Equal(t, 0, leftWeight)
	assert.Equal(t, []services.PlacementInstance{i1, i2, i3, i4, i5, i6, i7}, res)

	res, leftWeight = fillWeight(groups, 50)
	assert.Equal(t, 2, leftWeight)
	assert.Equal(t, []services.PlacementInstance{i1, i2, i3, i4, i5, i6, i7}, res)
}

func TestFillWeightDeterministic(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "", "", "endpoint", 1)
	i2 := placement.NewEmptyInstance("i2", "", "", "endpoint", 1)
	i3 := placement.NewEmptyInstance("i3", "", "", "endpoint", 1)
	i4 := placement.NewEmptyInstance("i4", "", "", "endpoint", 3)
	i5 := placement.NewEmptyInstance("i5", "", "", "endpoint", 4)

	i6 := placement.NewEmptyInstance("i6", "", "", "endpoint", 1)
	i7 := placement.NewEmptyInstance("i7", "", "", "endpoint", 1)
	i8 := placement.NewEmptyInstance("i8", "", "", "endpoint", 1)
	i9 := placement.NewEmptyInstance("i9", "", "", "endpoint", 2)
	groups := [][]services.PlacementInstance{
		[]services.PlacementInstance{i1, i2, i3, i4, i5},
		[]services.PlacementInstance{i6, i7, i8, i9},
	}

	for i := 1; i < 17; i++ {
		testResultDeterministic(t, groups, i)
	}
}

func testResultDeterministic(t *testing.T, groups [][]services.PlacementInstance, targetWeight int) {
	res, _ := fillWeight(groups, targetWeight)

	// shuffle the order of of each group of instances
	for _, group := range groups {
		for i := range group {
			j := rand.Intn(i + 1)
			group[i], group[j] = group[j], group[i]
		}
	}
	res1, _ := fillWeight(groups, targetWeight)
	assert.Equal(t, res, res1)
}

func TestRackLenSort(t *testing.T) {
	r1 := sortableValue{value: "r1", weight: 1}
	r2 := sortableValue{value: "r2", weight: 2}
	r3 := sortableValue{value: "r3", weight: 3}
	r4 := sortableValue{value: "r4", weight: 2}
	r5 := sortableValue{value: "r5", weight: 1}
	r6 := sortableValue{value: "r6", weight: 2}
	r7 := sortableValue{value: "r7", weight: 3}
	rs := sortableThings{r1, r2, r3, r4, r5, r6, r7}
	sort.Sort(rs)

	seen := 0
	for _, rl := range rs {
		assert.True(t, seen <= rl.weight)
		seen = rl.weight
	}
}

func TestFilterZones(t *testing.T) {
	i1 := placement.NewInstance().SetID("i1").SetZone("z1")
	i2 := placement.NewInstance().SetID("i2").SetZone("z1")
	i3 := placement.NewInstance().SetID("i2").SetZone("z1")
	i4 := placement.NewInstance().SetID("i3").SetZone("z2")

	_, _ = i2, i3

	tests := map[*struct {
		p          services.Placement
		candidates []services.PlacementInstance
		opts       services.PlacementOptions
	}][]services.PlacementInstance{
		{
			p:          placement.NewPlacement().SetInstances([]services.PlacementInstance{i1}),
			candidates: []services.PlacementInstance{},
			opts:       nil,
		}: []services.PlacementInstance{},
		{
			p:          placement.NewPlacement().SetInstances([]services.PlacementInstance{i1}),
			candidates: []services.PlacementInstance{i2},
			opts:       nil,
		}: []services.PlacementInstance{i2},
		{
			p:          placement.NewPlacement().SetInstances([]services.PlacementInstance{i1}),
			candidates: []services.PlacementInstance{i2, i4},
			opts:       nil,
		}: []services.PlacementInstance{i2},
		{
			p:          placement.NewPlacement().SetInstances([]services.PlacementInstance{i1}),
			candidates: []services.PlacementInstance{i2, i3},
			opts:       nil,
		}: []services.PlacementInstance{i2, i3},
		{
			p:          placement.NewPlacement(),
			candidates: []services.PlacementInstance{i2},
			opts:       nil,
		}: []services.PlacementInstance{},
		{
			p:          placement.NewPlacement(),
			candidates: []services.PlacementInstance{i2},
			opts:       placement.NewOptions().SetValidZone("z1"),
		}: []services.PlacementInstance{i2},
	}

	for args, exp := range tests {
		res := filterZones(args.p, args.candidates, args.opts)
		assert.Equal(t, exp, res)
	}
}

// file based placement storage
type mockStorage struct {
	sync.Mutex

	m       map[string]services.Placement
	version int
}

func NewMockStorage() placement.Storage {
	return &mockStorage{m: map[string]services.Placement{}}
}

func (ms *mockStorage) Set(service services.ServiceID, p services.Placement) error {
	ms.Lock()
	defer ms.Unlock()

	ms.m[service.Name()] = p
	ms.version++

	return nil
}

func (ms *mockStorage) CheckAndSet(service services.ServiceID, p services.Placement, v int) error {
	ms.Lock()
	defer ms.Unlock()

	if ms.version == v {
		ms.m[service.Name()] = p
		ms.version++
	} else {
		return errors.New("wrong version")
	}

	return nil
}

func (ms *mockStorage) SetIfNotExist(service services.ServiceID, p services.Placement) error {
	ms.Lock()
	defer ms.Unlock()

	if _, ok := ms.m[service.Name()]; ok {
		return errors.New("placement already exist")
	}
	ms.m[service.Name()] = p
	ms.version = 1
	return nil
}

func (ms *mockStorage) Delete(service services.ServiceID) error {
	ms.Lock()
	defer ms.Unlock()

	if _, exist := ms.m[service.Name()]; !exist {
		return errors.New("not exist")
	}

	delete(ms.m, service.Name())
	ms.version = 0
	return nil
}

func (ms *mockStorage) Placement(service services.ServiceID) (services.Placement, int, error) {
	ms.Lock()
	defer ms.Unlock()

	if p, exist := ms.m[service.Name()]; exist {
		return p, ms.version, nil
	}

	return nil, 0, kv.ErrNotFound
}

func markAllInstancesAvailable(
	t *testing.T,
	ps services.PlacementService,
) {
	p, _, err := ps.Placement()
	require.NoError(t, err)
	for _, i := range p.Instances() {
		if len(i.Shards().ShardsForState(shard.Initializing)) == 0 {
			continue
		}
		err := ps.MarkInstanceAvailable(i.ID())
		require.NoError(t, err)
	}
}
