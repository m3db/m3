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
	"testing"

	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/placement/algo"
	"github.com/m3db/m3/src/cluster/placement/storage"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGoodWorkflow(t *testing.T) {
	p := NewPlacementService(newMockStorage(),
		WithPlacementOptions(placement.NewOptions().SetValidZone("z1")))
	testGoodWorkflow(t, p)
}

func testGoodWorkflow(t *testing.T, ps placement.Service) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 2)
	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 2)
	i3 := placement.NewEmptyInstance("i3", "r3", "z1", "endpoint", 2)
	_, err := ps.BuildInitialPlacement([]placement.Instance{i1, i2}, 10, 1)
	assert.NoError(t, err)

	_, err = ps.AddReplica()
	assert.NoError(t, err)

	for _, instance := range []placement.Instance{i1, i2} {
		_, err = ps.MarkInstanceAvailable(instance.ID())
		assert.NoError(t, err)
	}
	_, ai, err := ps.AddInstances([]placement.Instance{i3})
	assert.NoError(t, err)
	assertPlacementInstanceEqualExceptShards(t, i3, ai[0])

	_, err = ps.MarkInstanceAvailable(i3.ID())
	assert.NoError(t, err)

	_, err = ps.RemoveInstances([]string{i1.ID()})
	assert.NoError(t, err)

	markAllInstancesAvailable(t, ps)

	var (
		i21 = placement.NewEmptyInstance("i21", "r2", "z1", "endpoint", 1)
		i4  = placement.NewEmptyInstance("i4", "r4", "z1", "endpoint", 1)
	)
	_, usedInstances, err := ps.ReplaceInstances(
		[]string{i2.ID()},
		[]placement.Instance{i21, i4,
			i3, // already in placement
			placement.NewEmptyInstance("i31", "r3", "z1", "endpoint", 1), // conflict
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(usedInstances))
	assertPlacementInstanceEqualExceptShards(t, i21, usedInstances[0])
	assertPlacementInstanceEqualExceptShards(t, i4, usedInstances[1])

	for _, id := range []string{"i21", "i4"} {
		_, err = ps.MarkInstanceAvailable(id)
		assert.NoError(t, err)
	}

	s, err := ps.Placement()
	assert.NoError(t, err)
	assert.Equal(t, 3, s.NumInstances())
	_, exist := s.Instance("i21")
	assert.True(t, exist)
	_, exist = s.Instance("i4")
	assert.True(t, exist)

	_, ai, err = ps.AddInstances([]placement.Instance{i1})
	assert.NoError(t, err)
	assertPlacementInstanceEqualExceptShards(t, i1, ai[0])

	i24 := placement.NewEmptyInstance("i24", "r2", "z1", "endpoint", 1)
	_, ai, err = ps.AddInstances([]placement.Instance{i24})
	assert.NoError(t, err)
	assertPlacementInstanceEqualExceptShards(t, i24, ai[0])

	i34 := placement.NewEmptyInstance("i34", "r3", "z1", "endpoint", 1)
	_, ai, err = ps.AddInstances([]placement.Instance{i34})
	assert.NoError(t, err)
	assertPlacementInstanceEqualExceptShards(t, i34, ai[0])

	i35 := placement.NewEmptyInstance("i35", "r3", "z1", "endpoint", 1)
	_, ai, err = ps.AddInstances([]placement.Instance{i35})
	assert.NoError(t, err)
	assertPlacementInstanceEqualExceptShards(t, i35, ai[0])

	i41 := placement.NewEmptyInstance("i41", "r4", "z1", "endpoint", 1)
	instances := []placement.Instance{
		placement.NewEmptyInstance("i15", "r1", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i34", "r3", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i35", "r3", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i36", "r3", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i23", "r2", "z1", "endpoint", 1),
		i41,
	}
	_, ai, err = ps.AddInstances(instances)
	assert.NoError(t, err)
	assertPlacementInstanceEqualExceptShards(t, i41, ai[0])
	s, err = ps.Placement()
	assert.NoError(t, err)
	// Instance added from least weighted isolation group.
	_, exist = s.Instance("i41")
	assert.True(t, exist)
}

func assertPlacementInstanceEqualExceptShards(
	t *testing.T,
	expected placement.Instance,
	observed placement.Instance,
) {
	assert.Equal(t, expected.ID(), observed.ID())
	assert.Equal(t, expected.IsolationGroup(), observed.IsolationGroup())
	assert.Equal(t, expected.Zone(), observed.Zone())
	assert.Equal(t, expected.Weight(), observed.Weight())
	assert.Equal(t, expected.Endpoint(), observed.Endpoint())
	assert.True(t, len(observed.Shards().All()) > 0)
}

func TestNonShardedWorkflow(t *testing.T) {
	ps := NewPlacementService(newMockStorage(),
		WithPlacementOptions(placement.NewOptions().SetValidZone("z1").SetIsSharded(false)))

	_, err := ps.BuildInitialPlacement([]placement.Instance{
		placement.NewEmptyInstance("i1", "r1", "z1", "e1", 1),
		placement.NewEmptyInstance("i2", "r1", "z1", "e2", 1),
	}, 10, 1)
	assert.Error(t, err)

	p, err := ps.BuildInitialPlacement([]placement.Instance{
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
	p, ai, err := ps.AddInstances([]placement.Instance{i3, i4})
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

	p, usedInstances, err := ps.ReplaceInstances([]string{"i2"}, []placement.Instance{i3, i4})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(usedInstances))
	assert.Equal(t, i4, usedInstances[0])
	assert.Equal(t, 2, p.NumInstances())
	assert.Equal(t, 0, p.NumShards())
	assert.Equal(t, 2, p.ReplicaFactor())
	assert.False(t, p.IsSharded())

	// nothing happens because i3 has no shards
	_, err = ps.MarkInstanceAvailable("i3")
	assert.NoError(t, err)
	assert.Equal(t, 2, p.NumInstances())
	assert.Equal(t, 0, p.NumShards())
	assert.Equal(t, 2, p.ReplicaFactor())
	assert.False(t, p.IsSharded())

	// nothing happens because there are no shards
	_, err = ps.BalanceShards()
	assert.NoError(t, err)
	assert.Equal(t, 2, p.NumInstances())
	assert.Equal(t, 0, p.NumShards())
	assert.Equal(t, 2, p.ReplicaFactor())
	assert.False(t, p.IsSharded())
}

func TestBadInitialPlacement(t *testing.T) {
	p := NewPlacementService(newMockStorage(),
		WithPlacementOptions(placement.NewOptions().SetValidZone("z1").SetIsSharded(false)))

	// invalid numShards
	_, err := p.BuildInitialPlacement([]placement.Instance{
		placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i2", "r1", "z1", "endpoint", 1),
	}, -1, 1)
	assert.Error(t, err)

	// invalid rf
	_, err = p.BuildInitialPlacement([]placement.Instance{
		placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i2", "r1", "z1", "endpoint", 1),
	}, 10, 0)
	assert.Error(t, err)

	// numshards > 0 && sharded == false
	_, err = p.BuildInitialPlacement([]placement.Instance{
		placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i2", "r1", "z1", "endpoint", 1),
	}, 10, 1)
	assert.Error(t, err)

	p = NewPlacementService(newMockStorage(),
		WithPlacementOptions(placement.NewOptions().SetValidZone("z1")))

	// Not enough instances.
	_, err = p.BuildInitialPlacement([]placement.Instance{}, 10, 1)
	assert.Error(t, err)

	// Error: rf == 0 && sharded == true.
	_, err = p.BuildInitialPlacement([]placement.Instance{
		placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i2", "r1", "z1", "endpoint", 1),
	}, 10, 0)
	assert.Error(t, err)

	// Not enough isolation groups.
	_, err = p.BuildInitialPlacement([]placement.Instance{
		placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i2", "r1", "z1", "endpoint", 1),
	}, 100, 2)
	assert.Error(t, err)

	_, err = p.BuildInitialPlacement([]placement.Instance{
		placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1),
	}, 100, 2)
	assert.NoError(t, err)

	// Placement already exist.
	_, err = p.BuildInitialPlacement([]placement.Instance{
		placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1),
	}, 100, 2)
	assert.Error(t, err)
}

func TestBadAddReplica(t *testing.T) {
	p := NewPlacementService(newMockStorage(),
		WithPlacementOptions(placement.NewOptions().SetValidZone("z1")))

	_, err := p.BuildInitialPlacement(
		[]placement.Instance{placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)},
		10, 1)
	assert.NoError(t, err)

	// Not enough isolation groups/instances.
	_, err = p.AddReplica()
	assert.Error(t, err)

	// Could not find placement for service.
	p = NewPlacementService(newMockStorage(),
		WithPlacementOptions(placement.NewOptions().SetValidZone("z1")))
	_, err = p.AddReplica()
	assert.Error(t, err)
}

func TestBadAddInstance(t *testing.T) {
	ms := newMockStorage()
	p := NewPlacementService(ms,
		WithPlacementOptions(placement.NewOptions().SetValidZone("z1")))

	_, err := p.BuildInitialPlacement(
		[]placement.Instance{placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)},
		10, 1)
	assert.NoError(t, err)

	// adding instance already exist
	_, _, err = p.AddInstances([]placement.Instance{placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)})
	assert.Error(t, err)

	// too many zones
	_, _, err = p.AddInstances([]placement.Instance{placement.NewEmptyInstance("i2", "r2", "z2", "endpoint", 1)})
	assert.Error(t, err)

	p = NewPlacementService(ms,
		WithPlacementOptions(placement.NewOptions().SetValidZone("z1")))
	_, _, err = p.AddInstances([]placement.Instance{placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)})
	assert.Error(t, err)

	// could not find placement for service
	p = NewPlacementService(newMockStorage(),
		WithPlacementOptions(placement.NewOptions().SetValidZone("z1")))
	_, _, err = p.AddInstances([]placement.Instance{placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)})
	assert.Error(t, err)
}

func TestBadRemoveInstance(t *testing.T) {
	p := NewPlacementService(newMockStorage(),
		WithPlacementOptions(placement.NewOptions().SetValidZone("z1")))

	_, err := p.BuildInitialPlacement(
		[]placement.Instance{placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)},
		10, 1)
	assert.NoError(t, err)

	// Leaving instance not exist.
	_, err = p.RemoveInstances([]string{"not_exist"})
	assert.Error(t, err)

	// Not enough isolation groups/instances after removal.
	_, err = p.RemoveInstances([]string{"i1"})
	assert.Error(t, err)

	// Could not find placement for service.
	p = NewPlacementService(newMockStorage(),
		WithPlacementOptions(placement.NewOptions().SetValidZone("z1")))
	_, err = p.RemoveInstances([]string{"i1"})
	assert.Error(t, err)
}

func TestBadReplaceInstance(t *testing.T) {
	p := NewPlacementService(newMockStorage(),
		WithPlacementOptions(placement.NewOptions().SetValidZone("z1")))

	_, err := p.BuildInitialPlacement([]placement.Instance{
		placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i4", "r4", "z1", "endpoint", 1),
	}, 10, 1)
	assert.NoError(t, err)

	// Leaving instance not exist.
	_, _, err = p.ReplaceInstances(
		[]string{"not_exist"},
		[]placement.Instance{placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)},
	)
	assert.Error(t, err)

	// Adding instance already exist.
	_, _, err = p.ReplaceInstances(
		[]string{"i1"},
		[]placement.Instance{placement.NewEmptyInstance("i4", "r4", "z1", "endpoint", 1)},
	)
	assert.Error(t, err)

	// Not enough isolation groups after replace.
	_, err = p.AddReplica()
	assert.NoError(t, err)
	_, _, err = p.ReplaceInstances(
		[]string{"i4"},
		[]placement.Instance{placement.NewEmptyInstance("i12", "r1", "z1", "endpoint", 1)},
	)
	assert.Error(t, err)

	// Could not find placement for service.
	p = NewPlacementService(newMockStorage(),
		WithPlacementOptions(placement.NewOptions().SetValidZone("z1")))
	_, _, err = p.ReplaceInstances(
		[]string{"i1"},
		[]placement.Instance{placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)},
	)
	assert.Error(t, err)
}

func TestMarkShard(t *testing.T) {
	ms := newMockStorage()

	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Leaving))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	i2 := placement.NewEmptyInstance("i2", "r1", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(4).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(5).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(0).SetState(shard.Available))

	i3 := placement.NewEmptyInstance("i3", "r2", "z1", "endpoint", 1)
	i3.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i3.Shards().Add(shard.NewShard(3).SetState(shard.Available))
	i3.Shards().Add(shard.NewShard(5).SetState(shard.Available))

	i4 := placement.NewEmptyInstance("i4", "r2", "z1", "endpoint", 1)
	i4.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i4.Shards().Add(shard.NewShard(4).SetState(shard.Available))
	i4.Shards().Add(shard.NewShard(0).SetState(shard.Available))

	i5 := placement.NewEmptyInstance("i5", "r2", "z1", "endpoint", 1)
	i5.Shards().Add(shard.NewShard(1).SetState(shard.Initializing).SetSourceID("i1"))

	instances := []placement.Instance{i1, i2, i3, i4, i5}
	p := placement.NewPlacement().
		SetInstances(instances).
		SetShards([]uint32{0, 1, 2, 3, 4, 5}).
		SetReplicaFactor(2).
		SetIsSharded(true)
	_, err := ms.SetIfNotExist(p)
	assert.NoError(t, err)

	ps := NewPlacementService(ms,
		WithPlacementOptions(placement.NewOptions().SetValidZone("z1")))
	_, err = ps.MarkShardsAvailable("i5", 1)
	assert.NoError(t, err)
	p, err = ms.Placement()
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
	for _, instance := range p.Instances() {
		for _, s := range instance.Shards().All() {
			assert.Equal(t, shard.Available, s.State())
		}
	}

	_, err = ps.MarkShardsAvailable("i1", 1)
	assert.Error(t, err)

	_, err = ps.MarkShardsAvailable("i5", 5)
	assert.Error(t, err)
}

func TestMarkInstance(t *testing.T) {
	ms := newMockStorage()

	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Leaving))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Leaving))
	i1.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	i2 := placement.NewEmptyInstance("i2", "r1", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(4).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(5).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(0).SetState(shard.Available))

	i3 := placement.NewEmptyInstance("i3", "r2", "z1", "endpoint", 1)
	i3.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i3.Shards().Add(shard.NewShard(3).SetState(shard.Available))
	i3.Shards().Add(shard.NewShard(5).SetState(shard.Available))

	i4 := placement.NewEmptyInstance("i4", "r2", "z1", "endpoint", 1)
	i4.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i4.Shards().Add(shard.NewShard(4).SetState(shard.Available))
	i4.Shards().Add(shard.NewShard(0).SetState(shard.Available))

	i5 := placement.NewEmptyInstance("i5", "r2", "z1", "endpoint", 1)
	i5.Shards().Add(shard.NewShard(1).SetState(shard.Initializing).SetSourceID("i1"))
	i5.Shards().Add(shard.NewShard(2).SetState(shard.Initializing).SetSourceID("i1"))

	instances := []placement.Instance{i1, i2, i3, i4, i5}
	p := placement.NewPlacement().
		SetInstances(instances).
		SetShards([]uint32{0, 1, 2, 3, 4, 5}).
		SetReplicaFactor(2).
		SetIsSharded(true)
	_, err := ms.SetIfNotExist(p)
	assert.NoError(t, err)

	ps := NewPlacementService(ms, WithPlacementOptions(placement.NewOptions().SetValidZone("z1")))

	// instance not exist
	_, err = ps.MarkInstanceAvailable("i6")
	assert.Error(t, err)

	_, err = ps.MarkInstanceAvailable("i5")
	assert.NoError(t, err)
	p, err = ps.Placement()
	assert.NoError(t, err)
	for _, instance := range p.Instances() {
		assert.True(t, instance.IsAvailable())
	}
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

	instances := []placement.Instance{i1, i2, i3, i4, i10}

	ids := []uint32{1, 2, 3, 4, 5, 6, 7, 8}
	s := placement.NewPlacement().SetInstances(instances).SetShards(ids).SetReplicaFactor(2)

	candidates := []placement.Instance{
		placement.NewEmptyInstance("i11", "r11", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i22", "r22", "z2", "endpoint", 1),
	}
	noConflictCandidates := []placement.Instance{
		placement.NewEmptyInstance("i11", "r0", "z1", "endpoint", 1),
		placement.NewEmptyInstance("i22", "r0", "z2", "endpoint", 1),
	}

	testCases := []struct {
		opts       placement.Options
		input      []placement.Instance
		replaceIDs []string
		expectRes  []placement.Instance
		expectErr  bool
	}{
		{
			opts:       placement.NewOptions().SetValidZone("z1"),
			input:      candidates,
			replaceIDs: []string{i4.ID()},
			expectRes:  []placement.Instance{candidates[0]},
		},
		{
			opts:       placement.NewOptions().SetValidZone("z1").SetAllowPartialReplace(false),
			input:      candidates,
			replaceIDs: []string{i4.ID()},
			expectErr:  true,
		},
		{
			opts:       placement.NewOptions().SetValidZone("z1"),
			input:      noConflictCandidates,
			replaceIDs: []string{i3.ID()},
			expectRes:  []placement.Instance{noConflictCandidates[0]},
		},
		{
			// Disable AllowPartialReplace, so the weight from the candidate instances must be more than the replacing instance.
			opts:       placement.NewOptions().SetValidZone("z1").SetAllowPartialReplace(false),
			input:      noConflictCandidates,
			replaceIDs: []string{i3.ID()},
			expectErr:  true,
		},
	}
	for _, test := range testCases {
		p := NewPlacementService(nil, WithPlacementOptions(test.opts)).(*placementService)
		res, err := p.selector.SelectReplaceInstances(test.input, test.replaceIDs, s)
		if test.expectErr {
			assert.Error(t, err)
			continue
		}
		assert.Equal(t, test.expectRes, res)
	}
}

func TestMirrorWorkflow(t *testing.T) {
	h1p1 := placement.NewInstance().
		SetID("h1p1").
		SetHostname("h1").
		SetPort(1).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h1p1e").
		SetWeight(1)
	h1p2 := placement.NewInstance().
		SetID("h1p2").
		SetHostname("h1").
		SetPort(2).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h1p2e").
		SetWeight(1)
	h1p3 := placement.NewInstance().
		SetID("h1p3").
		SetHostname("h1").
		SetPort(3).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h1p3e").
		SetWeight(1)
	h2p1 := placement.NewInstance().
		SetID("h2p1").
		SetHostname("h2").
		SetPort(1).
		SetIsolationGroup("r2").
		SetZone("z1").
		SetEndpoint("h2p1e").
		SetWeight(1)
	h2p2 := placement.NewInstance().
		SetID("h2p2").
		SetHostname("h2").
		SetPort(2).
		SetIsolationGroup("r2").
		SetZone("z1").
		SetEndpoint("h2p2e").
		SetWeight(1)
	h2p3 := placement.NewInstance().
		SetID("h2p3").
		SetHostname("h2").
		SetPort(3).
		SetIsolationGroup("r2").
		SetZone("z1").
		SetEndpoint("h2p3e").
		SetWeight(1)
	h3p1 := placement.NewInstance().
		SetID("h3p1").
		SetHostname("h3").
		SetPort(1).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h3p1e").
		SetWeight(2)
	h3p2 := placement.NewInstance().
		SetID("h3p2").
		SetHostname("h3").
		SetPort(2).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h3p2e").
		SetWeight(2)
	h3p3 := placement.NewInstance().
		SetID("h3p3").
		SetHostname("h3").
		SetPort(3).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h3p3e").
		SetWeight(2)
	h4p1 := placement.NewInstance().
		SetID("h4p1").
		SetHostname("h4").
		SetPort(1).
		SetIsolationGroup("r2").
		SetZone("z1").
		SetEndpoint("h4p1e").
		SetWeight(2)
	h4p2 := placement.NewInstance().
		SetID("h4p2").
		SetHostname("h4").
		SetPort(2).
		SetIsolationGroup("r2").
		SetZone("z1").
		SetEndpoint("h4p2e").
		SetWeight(2)
	h4p3 := placement.NewInstance().
		SetID("h4p3").
		SetHostname("h4").
		SetPort(3).
		SetIsolationGroup("r2").
		SetZone("z1").
		SetEndpoint("h4p3e").
		SetWeight(2)

	ps := NewPlacementService(
		newMockStorage(),
		WithPlacementOptions(placement.NewOptions().SetValidZone("z1").SetIsMirrored(true)),
	)

	p, err := ps.BuildInitialPlacement(
		[]placement.Instance{h1p1, h1p2, h1p3, h2p1, h2p2, h2p3, h3p1, h3p2, h3p3, h4p1, h4p2, h4p3},
		20,
		2,
	)
	assert.NoError(t, err)
	assert.Equal(t, h1p1.ShardSetID(), h2p1.ShardSetID())
	assert.Equal(t, h1p2.ShardSetID(), h2p2.ShardSetID())
	assert.Equal(t, h1p3.ShardSetID(), h2p3.ShardSetID())
	assert.Equal(t, h3p1.ShardSetID(), h4p1.ShardSetID())
	assert.Equal(t, h3p2.ShardSetID(), h4p2.ShardSetID())
	assert.Equal(t, h3p3.ShardSetID(), h4p3.ShardSetID())
	assert.Equal(t, 12, p.NumInstances())

	h5p1 := placement.NewInstance().
		SetID("h5p1").
		SetHostname("h5").
		SetPort(1).
		SetIsolationGroup("r1").
		SetZone("z1").
		SetEndpoint("h5p1e").
		SetWeight(2)
	h6p1 := placement.NewInstance().
		SetID("h6p1").
		SetHostname("h6").
		SetPort(1).
		SetIsolationGroup("r2").
		SetZone("z1").
		SetEndpoint("h6p1e").
		SetWeight(2)

	_, addedInstances, err := ps.AddInstances([]placement.Instance{h5p1, h6p1})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(addedInstances))
	assert.Equal(t, addedInstances[0].ShardSetID(), addedInstances[1].ShardSetID())
	assert.Equal(t, uint32(7), addedInstances[0].ShardSetID())

	_, err = ps.RemoveInstances([]string{h5p1.ID(), h6p1.ID()})
	assert.NoError(t, err)

	// Make sure reverting the removed instances reuses the old shard set id.
	_, addedInstances, err = ps.AddInstances([]placement.Instance{h5p1.SetShardSetID(0), h6p1.SetShardSetID(0)})
	for _, instance := range addedInstances {
		assert.Equal(t, uint32(7), instance.ShardSetID())
	}
	assert.NoError(t, err)

	h7p1 := placement.NewInstance().
		SetID("h7p1").
		SetHostname("h7").
		SetPort(1).
		SetIsolationGroup("r2").
		SetZone("z1").
		SetEndpoint("h7p1e").
		SetWeight(2)
	h7p2 := placement.NewInstance().
		SetID("h7p2").
		SetHostname("h7").
		SetPort(2).
		SetIsolationGroup("r2").
		SetZone("z1").
		SetEndpoint("h7p2e").
		SetWeight(2)
	h7p3 := placement.NewInstance().
		SetID("h7p3").
		SetHostname("h7").
		SetPort(3).
		SetIsolationGroup("r2").
		SetZone("z1").
		SetEndpoint("h7p3e").
		SetWeight(2)

	p, addedInstances, err = ps.ReplaceInstances(
		[]string{h4p1.ID(), h4p2.ID(), h4p3.ID()},
		[]placement.Instance{h3p1, h3p2, h3p3, h7p1, h7p2, h7p3},
	)
	require.NoError(t, err)
	h4p1, ok := p.Instance(h4p1.ID())
	assert.True(t, ok)
	h4p2, ok = p.Instance(h4p2.ID())
	assert.True(t, ok)
	h4p3, ok = p.Instance(h4p3.ID())
	assert.True(t, ok)
	assert.Equal(t, h4p1.ShardSetID(), addedInstances[0].ShardSetID())
	assert.Equal(t, h4p1.Shards().AllIDs(), addedInstances[0].Shards().AllIDs())
	assert.Equal(t, h4p2.ShardSetID(), addedInstances[1].ShardSetID())
	assert.Equal(t, h4p2.Shards().AllIDs(), addedInstances[1].Shards().AllIDs())
	assert.Equal(t, h4p3.ShardSetID(), addedInstances[2].ShardSetID())
	assert.Equal(t, h4p3.Shards().AllIDs(), addedInstances[2].Shards().AllIDs())
}

func TestManyShards(t *testing.T) {
	p := NewPlacementService(newMockStorage(),
		WithPlacementOptions(placement.NewOptions().SetValidZone("z1")))
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 2)
	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 2)
	i3 := placement.NewEmptyInstance("i3", "r3", "z1", "endpoint", 2)
	i4 := placement.NewEmptyInstance("i4", "r1", "z1", "endpoint", 2)
	i5 := placement.NewEmptyInstance("i5", "r2", "z1", "endpoint", 2)
	i6 := placement.NewEmptyInstance("i6", "r3", "z1", "endpoint", 2)
	_, err := p.BuildInitialPlacement([]placement.Instance{i1, i2, i3, i4, i5, i6}, 8192, 1)
	assert.NoError(t, err)
}

func TestAddMultipleInstances(t *testing.T) {
	i1 := placement.NewInstance().
		SetID("i1").
		SetIsolationGroup("r1").
		SetEndpoint("i1").
		SetWeight(3)
	i2 := placement.NewInstance().
		SetID("i2").
		SetIsolationGroup("r2").
		SetEndpoint("i2").
		SetWeight(1)
	i3 := placement.NewInstance().
		SetID("i3").
		SetIsolationGroup("r2").
		SetEndpoint("i3").
		SetWeight(1)
	i4 := placement.NewInstance().
		SetID("i4").
		SetIsolationGroup("r4").
		SetEndpoint("i4").
		SetWeight(2)

	tests := []struct {
		name               string
		opts               placement.Options
		initialInstances   []placement.Instance
		candidateInstances []placement.Instance
		expectAdded        []placement.Instance
	}{
		{
			name:               "Add Single Candidate",
			opts:               placement.NewOptions().SetAddAllCandidates(false),
			initialInstances:   []placement.Instance{i1.Clone(), i2.Clone()},
			candidateInstances: []placement.Instance{i3.Clone(), i4.Clone()},
			expectAdded:        []placement.Instance{i4}, // Prefer instance from different isolation group.
		},
		{
			name:               "Add All Candidates",
			opts:               placement.NewOptions().SetAddAllCandidates(true),
			initialInstances:   []placement.Instance{i1.Clone(), i2.Clone()},
			candidateInstances: []placement.Instance{i3.Clone(), i4.Clone()},
			expectAdded:        []placement.Instance{i3, i4},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ps := NewPlacementService(newMockStorage(), WithPlacementOptions(test.opts))
			_, err := ps.BuildInitialPlacement(test.initialInstances, 4, 2)
			require.NoError(t, err)

			_, added, err := ps.AddInstances(test.candidateInstances)
			require.NoError(t, err)
			require.True(t, compareInstances(test.expectAdded, added))
		})
	}
}

func TestReplaceInstances(t *testing.T) {
	i1 := placement.NewInstance().
		SetID("i1").
		SetIsolationGroup("r1").
		SetEndpoint("i1").
		SetWeight(4)
	i2 := placement.NewInstance().
		SetID("i2").
		SetIsolationGroup("r2").
		SetEndpoint("i2").
		SetWeight(2)
	i3 := placement.NewInstance().
		SetID("i3").
		SetIsolationGroup("r2").
		SetEndpoint("i3").
		SetWeight(2)
	i4 := placement.NewInstance().
		SetID("i4").
		SetIsolationGroup("r4").
		SetEndpoint("i4").
		SetWeight(1)

	tests := []struct {
		name               string
		opts               placement.Options
		initialInstances   []placement.Instance
		candidateInstances []placement.Instance
		leavingIDs         []string
		expectErr          bool
		expectAdded        []placement.Instance
	}{
		{
			name:               "Replace With Instance of Same Weight",
			opts:               placement.NewOptions().SetAddAllCandidates(false).SetAllowPartialReplace(false),
			initialInstances:   []placement.Instance{i1.Clone(), i2.Clone()},
			candidateInstances: []placement.Instance{i3.Clone(), i4.Clone()},
			leavingIDs:         []string{"i2"},
			expectErr:          false,
			expectAdded:        []placement.Instance{i3},
		},
		{
			name:               "Add All Candidates",
			opts:               placement.NewOptions().SetAddAllCandidates(true).SetAllowPartialReplace(false),
			initialInstances:   []placement.Instance{i1.Clone(), i2.Clone()},
			candidateInstances: []placement.Instance{i3.Clone(), i4.Clone()},
			leavingIDs:         []string{"i2"},
			expectErr:          false,
			expectAdded:        []placement.Instance{i3, i4},
		},
		{
			name:               "Not Enough Weight With Partial Replace",
			opts:               placement.NewOptions().SetAddAllCandidates(false).SetAllowPartialReplace(true),
			initialInstances:   []placement.Instance{i1.Clone(), i2.Clone()},
			candidateInstances: []placement.Instance{i3.Clone(), i4.Clone()},
			leavingIDs:         []string{"i1"},
			expectErr:          false,
			expectAdded:        []placement.Instance{i3, i4},
		},
		{
			name:               "Not Enough Weight Without Partial Replace",
			opts:               placement.NewOptions().SetAddAllCandidates(false).SetAllowPartialReplace(false),
			initialInstances:   []placement.Instance{i1.Clone(), i2.Clone()},
			candidateInstances: []placement.Instance{i3.Clone(), i4.Clone()},
			leavingIDs:         []string{"i1"},
			expectErr:          true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ps := NewPlacementService(newMockStorage(), WithPlacementOptions(test.opts))
			_, err := ps.BuildInitialPlacement(test.initialInstances, 4, 2)
			require.NoError(t, err)

			_, added, err := ps.ReplaceInstances(test.leavingIDs, test.candidateInstances)
			if test.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.True(t, compareInstances(test.expectAdded, added))
		})
	}
}

func TestValidateFnBeforeUpdate(t *testing.T) {
	p := NewPlacementService(newMockStorage(),
		WithPlacementOptions(placement.NewOptions().SetValidZone("z1"))).(*placementService)

	_, err := p.BuildInitialPlacement(
		[]placement.Instance{placement.NewEmptyInstance("i1", "r1", "z1", "endpoint1", 1)},
		10, 1)
	assert.NoError(t, err)

	expectErr := errors.New("err")
	p.opts = p.opts.SetValidateFnBeforeUpdate(func(placement.Placement) error { return expectErr })
	_, _, err = p.AddInstances([]placement.Instance{placement.NewEmptyInstance("i2", "r2", "z1", "endpoint2", 1)})
	assert.Error(t, err)
	assert.Equal(t, expectErr, err)
}

func TestPlacementServiceImplOptions(t *testing.T) {
	placementOptions := placement.NewOptions().SetValidZone("foozone").SetIsSharded(true)
	al := algo.NewAlgorithm(placementOptions.SetIsSharded(false))

	defaultImpl := newPlacementServiceImpl(nil)
	require.NotNil(t, defaultImpl)
	assert.NotNil(t, defaultImpl.opts)
	assert.NotNil(t, defaultImpl.algo)
	assert.NotEqual(t, placementOptions.ValidZone(), defaultImpl.opts.ValidZone())

	customImpl := newPlacementServiceImpl(nil,
		WithPlacementOptions(placementOptions),
		WithAlgorithm(al))
	assert.Equal(t, placementOptions.ValidZone(), customImpl.opts.ValidZone())
	assert.Equal(t, al, customImpl.algo)
}

func TestBalanceShards(t *testing.T) {
	ms := newMockStorage()

	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(0).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Available))

	i2 := placement.NewEmptyInstance("i2", "r1", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	instances := []placement.Instance{i1, i2}
	p := placement.NewPlacement().
		SetInstances(instances).
		SetShards([]uint32{0, 1, 2, 3}).
		SetReplicaFactor(1).
		SetIsSharded(true)

	_, err := ms.SetIfNotExist(p)
	assert.NoError(t, err)

	ps := NewPlacementService(ms, WithPlacementOptions(placement.NewOptions()))

	p, err = ps.BalanceShards()
	assert.NoError(t, err)

	bi1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	bi1.Shards().Add(shard.NewShard(0).SetState(shard.Leaving))
	bi1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	bi1.Shards().Add(shard.NewShard(2).SetState(shard.Available))

	bi2 := placement.NewEmptyInstance("i2", "r1", "z1", "endpoint", 1)
	bi2.Shards().Add(shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i1"))
	bi2.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	expectedInstances := []placement.Instance{bi1, bi2}
	assert.Equal(t, expectedInstances, p.Instances())
}

func newMockStorage() placement.Storage {
	return storage.NewPlacementStorage(mem.NewStore(), "", nil)
}

func markAllInstancesAvailable(
	t *testing.T,
	ps placement.Service,
) {
	p, err := ps.Placement()
	require.NoError(t, err)
	for _, i := range p.Instances() {
		if len(i.Shards().ShardsForState(shard.Initializing)) == 0 {
			continue
		}
		_, err := ps.MarkInstanceAvailable(i.ID())
		require.NoError(t, err)
	}
}

func compareInstances(left, right []placement.Instance) bool {
	if len(left) != len(right) {
		return false
	}

	ids := make(map[string]struct{}, len(left))
	for _, intce := range left {
		ids[intce.ID()] = struct{}{}
	}

	for _, intce := range right {
		if _, ok := ids[intce.ID()]; !ok {
			return false
		}
	}
	return true
}
