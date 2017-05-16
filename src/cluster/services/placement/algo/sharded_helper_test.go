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

package algo

import (
	"testing"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
	"github.com/m3db/m3cluster/shard"
	"github.com/stretchr/testify/assert"
)

func TestMoveInitializingShard(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(3).SetState(shard.Leaving))

	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Available))

	i3 := placement.NewEmptyInstance("i3", "r3", "z1", "endpoint", 1)
	i3.Shards().Add(shard.NewShard(3).SetState(shard.Initializing).SetSourceID("i1"))

	instances := []services.PlacementInstance{i1, i2, i3}
	p := placement.NewPlacement().SetInstances(instances).SetShards([]uint32{1, 2, 3}).SetReplicaFactor(1)
	ph := newHelper(p, 3, placement.NewOptions()).(*placementHelper)

	// move an Initializing shard
	s3, ok := i3.Shards().Shard(3)
	assert.True(t, ok)
	assert.True(t, ph.moveShard(s3, i3, i2))
	_, ok = i3.Shards().Shard(3)
	assert.False(t, ok)
	// i2 now owns it
	s3, ok = i2.Shards().Shard(3)
	assert.True(t, ok)
	assert.Equal(t, "i1", s3.SourceID())
	assert.Equal(t, shard.Unknown, s3.State())
}

func TestMoveInitializingShardBackToSource(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Leaving))

	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(1).SetState(shard.Initializing).SetSourceID("i1"))

	instances := []services.PlacementInstance{i1, i2}
	p := placement.NewPlacement().SetInstances(instances).SetShards([]uint32{1}).SetReplicaFactor(1)
	ph := newHelper(p, 3, placement.NewOptions()).(*placementHelper)

	s1, ok := i2.Shards().Shard(1)
	assert.True(t, ok)
	assert.True(t, ph.moveShard(s1, i2, i1))
	_, ok = i2.Shards().Shard(1)
	assert.False(t, ok)
	// i1 now owns it
	s1, ok = i1.Shards().Shard(1)
	assert.True(t, ok)
	assert.Equal(t, "", s1.SourceID())
	assert.Equal(t, shard.Available, s1.State())
}

func TestMoveLeavingShard(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(3).SetState(shard.Leaving))

	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Available))

	i3 := placement.NewEmptyInstance("i3", "r3", "z1", "endpoint", 1)
	i3.Shards().Add(shard.NewShard(3).SetState(shard.Initializing).SetSourceID("i1"))

	instances := []services.PlacementInstance{i1, i2, i3}
	p := placement.NewPlacement().SetInstances(instances).SetShards([]uint32{1, 2, 3}).SetReplicaFactor(1)
	ph := newHelper(p, 3, placement.NewOptions()).(*placementHelper)

	// make sure Leaving shard could not be moved
	s3, ok := i1.Shards().Shard(3)
	assert.True(t, ok)
	assert.False(t, ph.moveShard(s3, i1, i2))
}

func TestMoveAvailableShard(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))

	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Available))

	i3 := placement.NewEmptyInstance("i3", "r3", "z1", "endpoint", 1)
	i3.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	instances := []services.PlacementInstance{i1, i2, i3}
	p := placement.NewPlacement().SetInstances(instances).SetShards([]uint32{1, 2, 3}).SetReplicaFactor(1)
	ph := newHelper(p, 3, placement.NewOptions()).(*placementHelper)

	s3, ok := i3.Shards().Shard(3)
	assert.True(t, ok)
	assert.True(t, ph.moveShard(s3, i3, i2))
	assert.Equal(t, shard.Leaving, s3.State())
	assert.Equal(t, "", s3.SourceID())
	s3, ok = i2.Shards().Shard(3)
	assert.True(t, ok)
	assert.Equal(t, shard.Unknown, s3.State())
	assert.Equal(t, "i3", s3.SourceID())
}

func TestAssignShard(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(3).SetState(shard.Leaving))

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

	i5 := placement.NewEmptyInstance("i5", "r3", "z1", "endpoint", 1)
	i5.Shards().Add(shard.NewShard(5).SetState(shard.Available))
	i5.Shards().Add(shard.NewShard(6).SetState(shard.Available))
	i5.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i5.Shards().Add(shard.NewShard(3).SetState(shard.Initializing).SetSourceID("i1"))

	i6 := placement.NewEmptyInstance("i6", "r4", "z1", "endpoint", 1)
	i6.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i6.Shards().Add(shard.NewShard(3).SetState(shard.Available))
	i6.Shards().Add(shard.NewShard(4).SetState(shard.Available))

	instances := []services.PlacementInstance{i1, i2, i3, i4, i5, i6}
	p := placement.NewPlacement().
		SetInstances(instances).
		SetShards([]uint32{1, 2, 3, 4, 5, 6}).
		SetReplicaFactor(3)

	ph := newHelper(p, 3, placement.NewOptions()).(*placementHelper)
	assert.True(t, ph.canAssignInstance(2, i6, i5))
	assert.True(t, ph.canAssignInstance(1, i1, i6))
	assert.False(t, ph.canAssignInstance(2, i6, i1))
	// rack check
	assert.False(t, ph.canAssignInstance(2, i6, i3))
	ph = newHelper(p, 3, placement.NewOptions().SetLooseRackCheck(true)).(*placementHelper)
	assert.True(t, ph.canAssignInstance(2, i6, i3))
}

func TestNonLeavingInstances(t *testing.T) {
	instances := []services.PlacementInstance{
		placement.NewInstance().
			SetID("i1").
			SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1).SetState(shard.Initializing)})),
		placement.NewInstance().
			SetID("i2").
			SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1).SetState(shard.Leaving)})),
		placement.NewInstance().
			SetID("i2"),
	}
	r := nonLeavingInstances(instances)
	assert.Equal(t, 2, len(r))
	assert.Equal(t, "i1", r[0].ID())
	assert.Equal(t, "i2", r[1].ID())
}

func TestMarkShard(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "", "", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Available))

	i2 := placement.NewEmptyInstance("i2", "", "", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(1).SetState(shard.Initializing).SetSourceID("i3"))
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Initializing).SetSourceID("i1"))
	i2.Shards().Add(shard.NewShard(3).SetState(shard.Initializing).SetSourceID("i1"))

	p := placement.NewPlacement().
		SetInstances([]services.PlacementInstance{i1, i2}).
		SetShards([]uint32{1, 2}).
		SetReplicaFactor(2)

	_, err := MarkShardAvailable(p, "i3", 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist in placement")

	_, err = MarkShardAvailable(p, "i1", 3)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist in instance")

	_, err = MarkShardAvailable(p, "i1", 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not in Initializing state")

	_, err = MarkShardAvailable(p, "i2", 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist in placement")

	_, err = MarkShardAvailable(p, "i2", 3)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist in source instance")

	_, err = MarkShardAvailable(p, "i2", 2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not leaving instance")
}

func TestRemoveInstanceFromArray(t *testing.T) {
	instances := []services.PlacementInstance{
		placement.NewEmptyInstance("i1", "", "", "endpoint", 1),
		placement.NewEmptyInstance("i2", "", "", "endpoint", 1),
	}

	assert.Equal(t, instances, removeInstance(instances, "not_exist"))
	assert.Equal(t, []services.PlacementInstance{placement.NewEmptyInstance("i2", "", "", "endpoint", 1)}, removeInstance(instances, "i1"))
}

func TestCopy(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(4).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(5).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(6).SetState(shard.Available))

	instances := []services.PlacementInstance{i1, i2}

	ids := []uint32{1, 2, 3, 4, 5, 6}
	s := placement.NewPlacement().SetInstances(instances).SetShards(ids).SetReplicaFactor(1)
	copy := clonePlacement(s)
	assert.Equal(t, s.NumInstances(), copy.NumInstances())
	assert.Equal(t, s.Shards(), copy.Shards())
	assert.Equal(t, s.ReplicaFactor(), copy.ReplicaFactor())
	for _, i := range s.Instances() {
		copiedInstance, exist := copy.Instance(i.ID())
		assert.True(t, exist)
		assert.Equal(t, copiedInstance, i)
		// make sure they are different objects, updating one won't update the other
		i.Shards().Add(shard.NewShard(100).SetState(shard.Available))
		assert.NotEqual(t, copiedInstance, i)
	}
}

func TestLoadOnInstance(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Initializing))
	assert.Equal(t, 1, loadOnInstance(i1))

	i1.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	assert.Equal(t, 2, loadOnInstance(i1))

	i1.Shards().Add(shard.NewShard(3).SetState(shard.Leaving))
	assert.Equal(t, 2, loadOnInstance(i1))
}

func TestReturnInitShardToSource(t *testing.T) {
	i1 := placement.NewInstance().SetID("i1").SetRack("r1").SetEndpoint("e1").SetWeight(1).SetShards(shard.NewShards(
		[]shard.Shard{
			shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i2"),
			shard.NewShard(1).SetState(shard.Available),
		},
	))
	i2 := placement.NewInstance().SetID("i2").SetRack("r2").SetEndpoint("e2").SetWeight(1).SetShards(shard.NewShards(
		[]shard.Shard{
			shard.NewShard(0).SetState(shard.Leaving),
			shard.NewShard(2).SetState(shard.Available),
		},
	))
	i3 := placement.NewInstance().SetID("i3").SetRack("r3").SetEndpoint("e3").SetWeight(100).SetShards(shard.NewShards(
		[]shard.Shard{},
	))
	ph := NewPlacementHelper(
		placement.NewPlacement().
			SetInstances([]services.PlacementInstance{i1, i2, i3}).
			SetReplicaFactor(1).
			SetShards([]uint32{0, 1, 2}).
			SetIsSharded(true),
		placement.NewOptions(),
	).(*placementHelper)

	ph.returnInitializingShardsToSource(getShardMap(i1.Shards().All()), i1, ph.Instances())

	// Only the Initializing shards are moved
	assert.Equal(t, 1, i1.Shards().NumShards())
	assert.Equal(t, []shard.Shard{shard.NewShard(1).SetState(shard.Available)}, i1.Shards().All())
	assert.Equal(t, 2, i2.Shards().NumShards())
	assert.Equal(t, []shard.Shard{
		shard.NewShard(0).SetState(shard.Available),
		shard.NewShard(2).SetState(shard.Available),
	}, i2.Shards().All())
	assert.Equal(t, 0, i3.Shards().NumShards())
}

func TestReturnInitShardToSource_SourceIsLeaving(t *testing.T) {
	i1 := placement.NewInstance().SetID("i1").SetRack("r1").SetEndpoint("e1").SetWeight(1).SetShards(shard.NewShards(
		[]shard.Shard{shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i2")},
	))
	i2 := placement.NewInstance().SetID("i2").SetRack("r2").SetEndpoint("e2").SetWeight(1).SetShards(shard.NewShards(
		[]shard.Shard{shard.NewShard(0).SetState(shard.Leaving)},
	))
	i3 := placement.NewInstance().SetID("i3").SetRack("r3").SetEndpoint("e3").SetWeight(1).SetShards(shard.NewShards(
		[]shard.Shard{},
	))

	ph := NewPlacementHelper(
		placement.NewPlacement().
			SetInstances([]services.PlacementInstance{i1, i2, i3}).
			SetReplicaFactor(1).
			SetShards([]uint32{0}).
			SetIsSharded(true),
		placement.NewOptions(),
	).(*placementHelper)

	ph.returnInitializingShardsToSource(getShardMap(i1.Shards().All()), i1, ph.Instances())

	assert.Equal(t, 1, i1.Shards().NumShards())
	assert.Equal(t, []shard.Shard{shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i2")}, i1.Shards().All())
	assert.Equal(t, 1, i2.Shards().NumShards())
	assert.Equal(t, []shard.Shard{shard.NewShard(0).SetState(shard.Leaving)}, i2.Shards().All())
	assert.Equal(t, 0, i3.Shards().NumShards())
}

func TestGeneratePlacement(t *testing.T) {
	i1 := placement.NewInstance().SetID("i1").SetRack("r1").SetEndpoint("e1").SetWeight(1).SetShards(shard.NewShards(
		[]shard.Shard{shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i2")},
	))
	i2 := placement.NewInstance().SetID("i2").SetRack("r2").SetEndpoint("e2").SetWeight(1).SetShards(shard.NewShards(
		[]shard.Shard{shard.NewShard(0).SetState(shard.Leaving)},
	))
	i3 := placement.NewInstance().SetID("i3").SetRack("r3").SetEndpoint("e3").SetWeight(1).SetShards(shard.NewShards(
		[]shard.Shard{},
	))

	ph := NewPlacementHelper(
		placement.NewPlacement().
			SetInstances([]services.PlacementInstance{i1, i2, i3}).
			SetReplicaFactor(1).
			SetShards([]uint32{0}).
			SetIsSharded(true),
		placement.NewOptions(),
	)

	p := ph.GeneratePlacement(includeEmpty)
	assert.Equal(t, 3, p.NumInstances())

	p = ph.GeneratePlacement(nonEmptyOnly)
	assert.Equal(t, 2, p.NumInstances())
}

func TestReturnInitShardToSource_RackConflict(t *testing.T) {
	i1 := placement.NewInstance().SetID("i1").SetRack("r1").SetEndpoint("e1").SetWeight(1).SetShards(shard.NewShards(
		[]shard.Shard{shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i2")},
	))
	i2 := placement.NewInstance().SetID("i2").SetRack("r2").SetEndpoint("e2").SetWeight(1).SetShards(shard.NewShards(
		[]shard.Shard{shard.NewShard(0).SetState(shard.Leaving)},
	))
	i3 := placement.NewInstance().SetID("i3").SetRack("r3").SetEndpoint("e3").SetWeight(1).SetShards(shard.NewShards(
		[]shard.Shard{},
	))
	i4 := placement.NewInstance().SetID("i4").SetRack("r2").SetEndpoint("e4").SetWeight(1).SetShards(shard.NewShards(
		[]shard.Shard{shard.NewShard(0).SetState(shard.Available)},
	))

	ph := NewPlacementHelper(
		placement.NewPlacement().
			SetInstances([]services.PlacementInstance{i1, i2, i3, i4}).
			SetReplicaFactor(2).
			SetShards([]uint32{0}).
			SetIsSharded(true),
		placement.NewOptions(),
	).(*placementHelper)

	ph.returnInitializingShardsToSource(getShardMap(i1.Shards().All()), i1, ph.Instances())

	// the Initializing shard will not be returned to i2
	// because another replica of shard 0 is owned by i4, which is also on r2
	assert.Equal(t, 1, i1.Shards().NumShards())
	assert.Equal(t, []shard.Shard{shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i2")}, i1.Shards().All())
	assert.Equal(t, 1, i2.Shards().NumShards())
	assert.Equal(t, []shard.Shard{shard.NewShard(0).SetState(shard.Leaving)}, i2.Shards().All())
	assert.Equal(t, 0, i3.Shards().NumShards())
	assert.Equal(t, 1, i4.Shards().NumShards())
	assert.Equal(t, []shard.Shard{shard.NewShard(0).SetState(shard.Available)}, i4.Shards().All())

	// make sure PlaceShards will handle the unreturned shards
	i1 = placement.NewInstance().SetID("i1").SetRack("r1").SetEndpoint("e1").SetWeight(1).SetShards(shard.NewShards(
		[]shard.Shard{shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i2")},
	))
	i2 = placement.NewInstance().SetID("i2").SetRack("r2").SetEndpoint("e2").SetWeight(1).SetShards(shard.NewShards(
		[]shard.Shard{shard.NewShard(0).SetState(shard.Leaving)},
	))
	i3 = placement.NewInstance().SetID("i3").SetRack("r3").SetEndpoint("e3").SetWeight(1).SetShards(shard.NewShards(
		[]shard.Shard{},
	))
	i4 = placement.NewInstance().SetID("i4").SetRack("r2").SetEndpoint("e4").SetWeight(1).SetShards(shard.NewShards(
		[]shard.Shard{shard.NewShard(0).SetState(shard.Available)},
	))

	ph = NewPlacementHelper(
		placement.NewPlacement().
			SetInstances([]services.PlacementInstance{i1, i2, i3, i4}).
			SetReplicaFactor(2).
			SetShards([]uint32{0}).
			SetIsSharded(true),
		placement.NewOptions(),
	).(*placementHelper)

	err := ph.PlaceShards(i1.Shards().All(), i1, ph.Instances())
	assert.NoError(t, err)
	assert.Equal(t, 0, i1.Shards().NumShards())
	assert.Equal(t, 1, i2.Shards().NumShards())
	assert.Equal(t, []shard.Shard{shard.NewShard(0).SetState(shard.Leaving)}, i2.Shards().All())
	assert.Equal(t, 1, i3.Shards().NumShards())
	assert.Equal(t, []shard.Shard{shard.NewShard(0).SetSourceID("i2")}, i3.Shards().All())
	assert.Equal(t, 1, i4.Shards().NumShards())
	assert.Equal(t, []shard.Shard{shard.NewShard(0).SetState(shard.Available)}, i4.Shards().All())
}
