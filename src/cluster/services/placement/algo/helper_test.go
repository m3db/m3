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

func TestMoveShard(t *testing.T) {
	h1 := placement.NewEmptyInstance("r1h1", "r1", "z1", 1)
	h1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	h1.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	h1.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	h2 := placement.NewEmptyInstance("r1h2", "r1", "z1", 1)
	h2.Shards().Add(shard.NewShard(4).SetState(shard.Available))
	h2.Shards().Add(shard.NewShard(5).SetState(shard.Available))
	h2.Shards().Add(shard.NewShard(6).SetState(shard.Available))

	h3 := placement.NewEmptyInstance("r2h3", "r2", "z1", 1)
	h3.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	h3.Shards().Add(shard.NewShard(3).SetState(shard.Available))
	h3.Shards().Add(shard.NewShard(5).SetState(shard.Available))

	h4 := placement.NewEmptyInstance("r2h4", "r2", "z1", 1)
	h4.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	h4.Shards().Add(shard.NewShard(4).SetState(shard.Available))
	h4.Shards().Add(shard.NewShard(6).SetState(shard.Available))

	h5 := placement.NewEmptyInstance("r3h5", "r3", "z1", 1)
	h5.Shards().Add(shard.NewShard(5).SetState(shard.Available))
	h5.Shards().Add(shard.NewShard(6).SetState(shard.Available))
	h5.Shards().Add(shard.NewShard(1).SetState(shard.Available))

	h6 := placement.NewEmptyInstance("r4h6", "r4", "z1", 1)
	h6.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	h6.Shards().Add(shard.NewShard(3).SetState(shard.Available))
	h6.Shards().Add(shard.NewShard(4).SetState(shard.Available))

	instances := []services.PlacementInstance{h1, h2, h3, h4, h5, h6}

	p := placement.NewPlacement(instances, []uint32{1, 2, 3, 4, 5, 6}, 3)

	ph := newHelper(p, 3, placement.NewOptions()).(*placementHelper)
	assert.True(t, ph.canAssignInstance(2, h6, h5))
	assert.True(t, ph.canAssignInstance(1, h1, h6))
	assert.False(t, ph.canAssignInstance(2, h6, h1))
	// rack check
	assert.False(t, ph.canAssignInstance(2, h6, h3))
	ph = newHelper(p, 3, placement.NewOptions().SetLooseRackCheck(true)).(*placementHelper)
	assert.True(t, ph.canAssignInstance(2, h6, h3))

	s := shard.NewShard(1).SetState(shard.Available)
	ph.assignShardToInstance(s, nil)
	assert.Equal(t, shard.NewShard(1).SetState(shard.Available), s)

	assert.False(t, ph.MoveOneShard(nil, h1))
}

func TestIsInstanceLeaving(t *testing.T) {
	h1 := placement.NewEmptyInstance("r1h1", "r1", "z1", 1)
	h1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	assert.False(t, isInstanceLeaving(h1))

	h1.Shards().Add(shard.NewShard(1).SetState(shard.Initializing))
	assert.False(t, isInstanceLeaving(h1))

	h1.Shards().Add(shard.NewShard(1).SetState(shard.Leaving))
	assert.True(t, isInstanceLeaving(h1))

	h1.SetShards(shard.NewShards(nil))
	assert.False(t, isInstanceLeaving(h1))
}

func TestNonLeavingInstances(t *testing.T) {
	instances := []services.PlacementInstance{
		placement.NewInstance().
			SetID("h1").
			SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1).SetState(shard.Initializing)})),
		placement.NewInstance().
			SetID("h2").
			SetShards(shard.NewShards([]shard.Shard{shard.NewShard(1).SetState(shard.Leaving)})),
		placement.NewInstance().
			SetID("h2"),
	}
	r := nonLeavingInstances(instances)
	assert.Equal(t, 2, len(r))
	assert.Equal(t, "h1", r[0].ID())
	assert.Equal(t, "h2", r[1].ID())
}

func TestMarkShard(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "", "", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Available))

	i2 := placement.NewEmptyInstance("i2", "", "", 1)
	i2.Shards().Add(shard.NewShard(1).SetState(shard.Initializing).SetSourceID("i3"))
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Initializing).SetSourceID("i1"))
	i2.Shards().Add(shard.NewShard(3).SetState(shard.Initializing).SetSourceID("i1"))

	p := placement.NewPlacement([]services.PlacementInstance{i1, i2}, []uint32{1, 2}, 2)

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
		placement.NewEmptyInstance("h1", "", "", 1),
		placement.NewEmptyInstance("h2", "", "", 1),
	}

	assert.Equal(t, instances, removeInstance(instances, "not_exist"))
	assert.Equal(t, []services.PlacementInstance{placement.NewEmptyInstance("h2", "", "", 1)}, removeInstance(instances, "h1"))
}

func TestCopy(t *testing.T) {
	h1 := placement.NewEmptyInstance("r1h1", "r1", "z1", 1)
	h1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	h1.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	h1.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	h2 := placement.NewEmptyInstance("r2h2", "r2", "z1", 1)
	h2.Shards().Add(shard.NewShard(4).SetState(shard.Available))
	h2.Shards().Add(shard.NewShard(5).SetState(shard.Available))
	h2.Shards().Add(shard.NewShard(6).SetState(shard.Available))

	instances := []services.PlacementInstance{h1, h2}

	ids := []uint32{1, 2, 3, 4, 5, 6}
	s := placement.NewPlacement(instances, ids, 1)
	copy := clonePlacement(s)
	assert.Equal(t, s.NumInstances(), copy.NumInstances())
	assert.Equal(t, s.Shards(), copy.Shards())
	assert.Equal(t, s.ReplicaFactor(), copy.ReplicaFactor())
	for _, i := range s.Instances() {
		assert.Equal(t, copy.Instance(i.ID()), i)
		// make sure they are different objects, updating one won't update the other
		i.Shards().Add(shard.NewShard(100).SetState(shard.Available))
		assert.NotEqual(t, copy.Instance(i.ID()), i)
	}
}

func TestLoadOnInstance(t *testing.T) {
	h1 := placement.NewEmptyInstance("r1h1", "r1", "z1", 1)
	h1.Shards().Add(shard.NewShard(1).SetState(shard.Initializing))
	assert.Equal(t, 1, loadOnInstance(h1))

	h1.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	assert.Equal(t, 2, loadOnInstance(h1))

	h1.Shards().Add(shard.NewShard(3).SetState(shard.Leaving))
	assert.Equal(t, 2, loadOnInstance(h1))
}
