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
	_, err = p.AddInstance([]services.PlacementInstance{i3})
	assert.NoError(t, err)

	err = p.MarkInstanceAvailable(i3.ID())
	assert.NoError(t, err)

	_, err = p.RemoveInstance(i1.ID())
	assert.NoError(t, err)

	markAllInstancesAvailable(t, p)

	_, err = p.ReplaceInstance(
		i2.ID(),
		[]services.PlacementInstance{
			placement.NewEmptyInstance("i21", "r2", "z1", "endpoint", 1),
			placement.NewEmptyInstance("i4", "r4", "z1", "endpoint", 1),
			i3, // already in placement
			placement.NewEmptyInstance("i31", "r3", "z1", "endpoint", 1), // conflict
		},
	)
	assert.NoError(t, err)

	for _, id := range []string{"i21", "i4"} {
		err = p.MarkInstanceAvailable(id)
		assert.NoError(t, err)
	}

	s, err := p.Placement()
	assert.NoError(t, err)
	assert.Equal(t, 3, s.NumInstances())
	_, exist := s.Instance("i21")
	assert.True(t, exist)
	_, exist = s.Instance("i4")
	assert.True(t, exist)

	_, err = p.AddInstance([]services.PlacementInstance{i1})
	assert.NoError(t, err)

	_, err = p.AddInstance([]services.PlacementInstance{placement.NewEmptyInstance("i24", "r2", "z1", "endpoint", 1)})
	assert.NoError(t, err)

	_, err = p.AddInstance([]services.PlacementInstance{placement.NewEmptyInstance("i34", "r3", "z1", "endpoint", 1)})
	assert.NoError(t, err)
	_, err = p.AddInstance([]services.PlacementInstance{placement.NewEmptyInstance("i35", "r3", "z1", "endpoint", 1)})
	assert.NoError(t, err)

	instances := []services.PlacementInstance{
		placement.NewEmptyInstance("i15", "r1", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i34", "r3", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i35", "r3", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i36", "r3", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i23", "r2", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i41", "r4", "z1", "endpoint", 1),
	}
	_, err = p.AddInstance(instances)
	assert.NoError(t, err)
	s, err = p.Placement()
	assert.NoError(t, err)
	_, exist = s.Instance("i41") // instance added from least weighted rack
	assert.True(t, exist)
}

func TestBadInitialPlacement(t *testing.T) {
	p := NewPlacementService(NewMockStorage(), testServiceID(), placement.NewOptions())

	// not enough instances
	_, err := p.BuildInitialPlacement([]services.PlacementInstance{}, 10, 1)
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
	_, err = p.AddInstance([]services.PlacementInstance{placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)})
	assert.Error(t, err)

	// too many zones
	_, err = p.AddInstance([]services.PlacementInstance{placement.NewEmptyInstance("i2", "r2", "z2", "endpoint", 1)})
	assert.Error(t, err)
	assert.Equal(t, errNoValidInstance, err)

	p = NewPlacementService(ms, testServiceID(), placement.NewOptions())
	_, err = p.AddInstance([]services.PlacementInstance{placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)})
	assert.Error(t, err)

	// could not find placement for service
	p = NewPlacementService(NewMockStorage(), testServiceID(), placement.NewOptions())
	_, err = p.AddInstance([]services.PlacementInstance{placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)})
	assert.Error(t, err)
}

func TestBadRemoveInstance(t *testing.T) {
	p := NewPlacementService(NewMockStorage(), testServiceID(), placement.NewOptions())

	_, err := p.BuildInitialPlacement(
		[]services.PlacementInstance{placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)},
		10, 1)
	assert.NoError(t, err)

	// leaving instance not exist
	_, err = p.RemoveInstance("not_exist")
	assert.Error(t, err)

	// not enough racks/instances after removal
	_, err = p.RemoveInstance("i1")
	assert.Error(t, err)

	// could not find placement for service
	p = NewPlacementService(NewMockStorage(), testServiceID(), placement.NewOptions())
	_, err = p.RemoveInstance("i1")
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
	_, err = p.ReplaceInstance(
		"not_exist",
		[]services.PlacementInstance{placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)},
	)
	assert.Error(t, err)

	// adding instance already exist
	_, err = p.ReplaceInstance(
		"i1",
		[]services.PlacementInstance{placement.NewEmptyInstance("i4", "r4", "z1", "endpoint", 1)},
	)
	assert.Error(t, err)

	// not enough rack after replace
	_, err = p.AddReplica()
	assert.NoError(t, err)
	_, err = p.ReplaceInstance(
		"i4",
		[]services.PlacementInstance{placement.NewEmptyInstance("i12", "r1", "z1", "endpoint", 1)},
	)
	assert.Error(t, err)

	// could not find placement for service
	p = NewPlacementService(NewMockStorage(), testServiceID(), placement.NewOptions())
	_, err = p.ReplaceInstance(
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
	_, err = p.ReplaceInstance(
		"not_exist",
		[]services.PlacementInstance{placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)},
	)
	assert.Error(t, err)

	// adding instance already exist
	_, err = p.ReplaceInstance(
		"i1",
		[]services.PlacementInstance{placement.NewEmptyInstance("i4", "r4", "z1", "endpoint", 1)},
	)
	assert.Error(t, err)

	// NO ERROR when not enough rack after replace
	_, err = p.AddReplica()
	assert.NoError(t, err)
	_, err = p.ReplaceInstance(
		"i4",
		[]services.PlacementInstance{placement.NewEmptyInstance("i12", "r1", "z1", "endpoint", 1)},
	)
	assert.NoError(t, err)

	// could not find placement for service
	p = NewPlacementService(NewMockStorage(), testServiceID(), placement.NewOptions())
	_, err = p.ReplaceInstance(
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
	p := placement.NewPlacement(instances, []uint32{1, 2, 3, 4, 5, 6}, 2)
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
	p := placement.NewPlacement(instances, []uint32{1, 2, 3, 4, 5, 6}, 2)
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
	s := placement.NewPlacement(instances, ids, 2)

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

type errorAlgorithm struct{}

func (errorAlgorithm) InitialPlacement(instances []services.PlacementInstance, ids []uint32) (services.ServicePlacement, error) {
	return nil, errors.New("error in errorAlgorithm")
}

func (errorAlgorithm) AddReplica(p services.ServicePlacement) (services.ServicePlacement, error) {
	return nil, errors.New("error in errorAlgorithm")
}

func (errorAlgorithm) AddInstance(p services.ServicePlacement, h services.PlacementInstance) (services.ServicePlacement, error) {
	return nil, errors.New("error in errorAlgorithm")
}

func (errorAlgorithm) RemoveInstance(p services.ServicePlacement, h services.PlacementInstance) (services.ServicePlacement, error) {
	return nil, errors.New("error in errorAlgorithm")
}

func (errorAlgorithm) ReplaceInstance(p services.ServicePlacement, leavingInstance services.PlacementInstance, addingInstance []services.PlacementInstance) (services.ServicePlacement, error) {
	return nil, errors.New("error in errorAlgorithm")
}

// file based placement storage
type mockStorage struct {
	sync.Mutex

	m       map[string]services.ServicePlacement
	version int
}

func NewMockStorage() placement.Storage {
	return &mockStorage{m: map[string]services.ServicePlacement{}}
}

func (ms *mockStorage) CheckAndSet(service services.ServiceID, p services.ServicePlacement, v int) error {
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

func (ms *mockStorage) SetIfNotExist(service services.ServiceID, p services.ServicePlacement) error {
	ms.Lock()
	defer ms.Unlock()

	if _, ok := ms.m[service.Name()]; ok {
		return errors.New("placement already exist")
	}
	ms.m[service.Name()] = p
	ms.version = 1
	return nil
}

func (ms *mockStorage) Placement(service services.ServiceID) (services.ServicePlacement, int, error) {
	ms.Lock()
	defer ms.Unlock()

	if p, exist := ms.m[service.Name()]; exist {
		return p, ms.version, nil
	}

	return nil, 0, errors.New("placement not exist")
}

func markAllInstancesAvailable(
	t *testing.T,
	ps services.PlacementService,
) {
	p, err := ps.Placement()
	require.NoError(t, err)
	for _, i := range p.Instances() {
		if len(i.Shards().ShardsForState(shard.Initializing)) == 0 {
			continue
		}
		err := ps.MarkInstanceAvailable(i.ID())
		require.NoError(t, err)
	}
}
