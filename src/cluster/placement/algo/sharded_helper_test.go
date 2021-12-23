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
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMoveInitializingShard(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(3).SetState(shard.Leaving))

	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Available))

	i3 := placement.NewEmptyInstance("i3", "r3", "z1", "endpoint", 1)
	i3.Shards().Add(shard.NewShard(3).SetState(shard.Initializing).SetSourceID("i1"))

	instances := []placement.Instance{i1, i2, i3}
	p := placement.NewPlacement().SetInstances(instances).SetShards([]uint32{1, 2, 3}).SetReplicaFactor(1)
	ph := newHelper(p, 3, placement.NewOptions()).(*helper)

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

	instances := []placement.Instance{i1, i2}
	p := placement.NewPlacement().SetInstances(instances).SetShards([]uint32{1}).SetReplicaFactor(1)
	ph := newHelper(p, 3, placement.NewOptions()).(*helper)

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

	instances := []placement.Instance{i1, i2, i3}
	p := placement.NewPlacement().SetInstances(instances).SetShards([]uint32{1, 2, 3}).SetReplicaFactor(1)
	ph := newHelper(p, 3, placement.NewOptions()).(*helper)

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

	instances := []placement.Instance{i1, i2, i3}
	p := placement.NewPlacement().SetInstances(instances).SetShards([]uint32{1, 2, 3}).SetReplicaFactor(1)
	ph := newHelper(p, 3, placement.NewOptions()).(*helper)

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

	instances := []placement.Instance{i1, i2, i3, i4, i5, i6}
	p := placement.NewPlacement().
		SetInstances(instances).
		SetShards([]uint32{1, 2, 3, 4, 5, 6}).
		SetReplicaFactor(3)

	ph := newHelper(p, 3, placement.NewOptions()).(*helper)
	assert.True(t, ph.canAssignInstance(2, i6, i5))
	assert.True(t, ph.canAssignInstance(1, i1, i6))
	assert.False(t, ph.canAssignInstance(2, i6, i1))
	assert.False(t, ph.canAssignInstance(2, i6, i3))
}

func TestNonLeavingInstances(t *testing.T) {
	instances := []placement.Instance{
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
	i1 := placement.NewInstance().
		SetID("i1").
		SetIsolationGroup("r1").
		SetEndpoint("e1").
		SetWeight(1).
		SetShards(shard.NewShards(
			[]shard.Shard{
				shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i2"),
				shard.NewShard(1).SetState(shard.Available),
			},
		))
	i2 := placement.NewInstance().
		SetID("i2").
		SetIsolationGroup("r2").
		SetEndpoint("e2").
		SetWeight(1).
		SetShards(shard.NewShards(
			[]shard.Shard{
				shard.NewShard(0).SetState(shard.Leaving),
				shard.NewShard(2).SetState(shard.Available),
			},
		))
	i3 := placement.NewInstance().
		SetID("i3").
		SetIsolationGroup("r3").
		SetEndpoint("e3").
		SetWeight(100).
		SetShards(shard.NewShards(
			[]shard.Shard{},
		))
	ph := NewPlacementHelper(
		placement.NewPlacement().
			SetInstances([]placement.Instance{i1, i2, i3}).
			SetReplicaFactor(1).
			SetShards([]uint32{0, 1, 2}).
			SetIsSharded(true),
		placement.NewOptions(),
	).(*helper)

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
	i1 := placement.NewInstance().
		SetID("i1").
		SetIsolationGroup("r1").
		SetEndpoint("e1").
		SetWeight(1).
		SetShards(shard.NewShards(
			[]shard.Shard{shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i2")},
		))
	i2 := placement.NewInstance().
		SetID("i2").
		SetIsolationGroup("r2").
		SetEndpoint("e2").
		SetWeight(1).
		SetShards(shard.NewShards(
			[]shard.Shard{shard.NewShard(0).SetState(shard.Leaving)},
		))
	i3 := placement.NewInstance().
		SetID("i3").
		SetIsolationGroup("r3").
		SetEndpoint("e3").
		SetWeight(1).
		SetShards(shard.NewShards(
			[]shard.Shard{},
		))

	ph := NewPlacementHelper(
		placement.NewPlacement().
			SetInstances([]placement.Instance{i1, i2, i3}).
			SetReplicaFactor(1).
			SetShards([]uint32{0}).
			SetIsSharded(true),
		placement.NewOptions(),
	).(*helper)

	ph.returnInitializingShardsToSource(getShardMap(i1.Shards().All()), i1, ph.Instances())

	assert.Equal(t, 1, i1.Shards().NumShards())
	assert.Equal(t, []shard.Shard{shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i2")}, i1.Shards().All())
	assert.Equal(t, 1, i2.Shards().NumShards())
	assert.Equal(t, []shard.Shard{shard.NewShard(0).SetState(shard.Leaving)}, i2.Shards().All())
	assert.Equal(t, 0, i3.Shards().NumShards())
}

func TestGeneratePlacement(t *testing.T) {
	i1 := placement.NewInstance().
		SetID("i1").
		SetIsolationGroup("r1").
		SetEndpoint("e1").
		SetWeight(1).
		SetShards(shard.NewShards(
			[]shard.Shard{shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i2")},
		))
	i2 := placement.NewInstance().
		SetID("i2").
		SetIsolationGroup("r2").
		SetEndpoint("e2").
		SetWeight(1).
		SetShards(shard.NewShards(
			[]shard.Shard{shard.NewShard(0).SetState(shard.Leaving)},
		))
	i3 := placement.NewInstance().
		SetID("i3").
		SetIsolationGroup("r3").
		SetEndpoint("e3").
		SetWeight(1).
		SetShards(shard.NewShards(
			[]shard.Shard{},
		))

	ph := newHelper(
		placement.NewPlacement().
			SetInstances([]placement.Instance{i1, i2, i3}).
			SetReplicaFactor(1).
			SetShards([]uint32{0}).
			SetIsSharded(true),
		1,
		placement.NewOptions(),
	)

	p := ph.generatePlacement()
	assert.Equal(t, 2, p.NumInstances())
}

func TestReturnInitShardToSource_IsolationGroupConflict(t *testing.T) {
	i1 := placement.NewInstance().
		SetID("i1").
		SetIsolationGroup("r1").
		SetEndpoint("e1").
		SetWeight(1).
		SetShards(shard.NewShards(
			[]shard.Shard{shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i2")},
		))
	i2 := placement.NewInstance().
		SetID("i2").
		SetIsolationGroup("r2").
		SetEndpoint("e2").
		SetWeight(1).
		SetShards(shard.NewShards(
			[]shard.Shard{shard.NewShard(0).SetState(shard.Leaving)},
		))
	i3 := placement.NewInstance().
		SetID("i3").
		SetIsolationGroup("r3").
		SetEndpoint("e3").
		SetWeight(1).
		SetShards(shard.NewShards(
			[]shard.Shard{},
		))
	i4 := placement.NewInstance().
		SetID("i4").
		SetIsolationGroup("r2").
		SetEndpoint("e4").
		SetWeight(1).
		SetShards(shard.NewShards(
			[]shard.Shard{shard.NewShard(0).SetState(shard.Available)},
		))

	ph := NewPlacementHelper(
		placement.NewPlacement().
			SetInstances([]placement.Instance{i1, i2, i3, i4}).
			SetReplicaFactor(2).
			SetShards([]uint32{0}).
			SetIsSharded(true),
		placement.NewOptions(),
	).(*helper)

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

	// make sure placeShards will handle the unreturned shards
	i1 = placement.NewInstance().
		SetID("i1").
		SetIsolationGroup("r1").
		SetEndpoint("e1").
		SetWeight(1).
		SetShards(shard.NewShards(
			[]shard.Shard{shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i2")},
		))
	i2 = placement.NewInstance().
		SetID("i2").
		SetIsolationGroup("r2").
		SetEndpoint("e2").
		SetWeight(1).
		SetShards(shard.NewShards(
			[]shard.Shard{shard.NewShard(0).SetState(shard.Leaving)},
		))
	i3 = placement.NewInstance().
		SetID("i3").
		SetIsolationGroup("r3").
		SetEndpoint("e3").
		SetWeight(1).
		SetShards(shard.NewShards(
			[]shard.Shard{},
		))
	i4 = placement.NewInstance().
		SetID("i4").
		SetIsolationGroup("r2").
		SetEndpoint("e4").
		SetWeight(1).
		SetShards(shard.NewShards(
			[]shard.Shard{shard.NewShard(0).SetState(shard.Available)},
		))

	ph = NewPlacementHelper(
		placement.NewPlacement().
			SetInstances([]placement.Instance{i1, i2, i3, i4}).
			SetReplicaFactor(2).
			SetShards([]uint32{0}).
			SetIsSharded(true),
		placement.NewOptions(),
	).(*helper)

	err := ph.placeShards(i1.Shards().All(), i1, ph.Instances())
	assert.NoError(t, err)
	assert.Equal(t, 0, i1.Shards().NumShards())
	assert.Equal(t, 1, i2.Shards().NumShards())
	assert.Equal(t, []shard.Shard{shard.NewShard(0).SetState(shard.Leaving)}, i2.Shards().All())
	assert.Equal(t, 1, i3.Shards().NumShards())
	assert.Equal(t, []shard.Shard{shard.NewShard(0).SetSourceID("i2")}, i3.Shards().All())
	assert.Equal(t, 1, i4.Shards().NumShards())
	assert.Equal(t, []shard.Shard{shard.NewShard(0).SetState(shard.Available)}, i4.Shards().All())
}

func TestMarkShardSuccess(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "", "", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Initializing))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Initializing).SetSourceID("i2"))

	i2 := placement.NewEmptyInstance("i2", "", "", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(1).SetState(shard.Initializing))
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Leaving))

	p := placement.NewPlacement().
		SetInstances([]placement.Instance{i1, i2}).
		SetShards([]uint32{1, 2}).
		SetReplicaFactor(2)

	opts := placement.NewOptions()
	_, err := markShardsAvailable(p, "i1", []uint32{1}, opts)
	assert.NoError(t, err)

	_, err = markShardsAvailable(p, "i1", []uint32{2}, opts)
	assert.NoError(t, err)
}

func TestMarkShardSuccessBulk(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "", "", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Initializing))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Initializing).SetSourceID("i2"))

	i2 := placement.NewEmptyInstance("i2", "", "", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(1).SetState(shard.Initializing))
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Leaving))

	p := placement.NewPlacement().
		SetInstances([]placement.Instance{i1, i2}).
		SetShards([]uint32{1, 2}).
		SetReplicaFactor(2)

	opts := placement.NewOptions()
	modifiedPlacement, err := markShardsAvailable(p, "i1", []uint32{1, 2}, opts)
	assert.NoError(t, err)

	mi1, ok := modifiedPlacement.Instance("i1")
	require.True(t, ok)

	shards := mi1.Shards().All()
	require.Len(t, shards, 2)
	for _, s := range shards {
		require.Equal(t, shard.Available, s.State())
	}
}

func TestMarkShardFailure(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "", "", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Available))

	i2 := placement.NewEmptyInstance("i2", "", "", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(1).SetState(shard.Initializing).SetSourceID("i3"))
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Initializing).SetSourceID("i1"))
	i2.Shards().Add(shard.NewShard(3).SetState(shard.Initializing).SetSourceID("i1"))

	p := placement.NewPlacement().
		SetInstances([]placement.Instance{i1, i2}).
		SetShards([]uint32{1, 2}).
		SetReplicaFactor(2)

	opts := placement.NewOptions().
		SetIsShardCutoverFn(genShardCutoverFn(time.Now())).
		SetIsShardCutoffFn(genShardCutoffFn(time.Now(), time.Minute))
	_, err := markShardsAvailable(p, "i3", []uint32{1}, opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist in placement")

	_, err = markShardsAvailable(p, "i1", []uint32{3}, opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist in instance")

	_, err = markShardsAvailable(p, "i1", []uint32{1}, opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not in Initializing state")

	_, err = markShardsAvailable(p, "i2", []uint32{1}, opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist in placement")

	_, err = markShardsAvailable(p, "i2", []uint32{3}, opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist in source instance")

	_, err = markShardsAvailable(p, "i2", []uint32{2}, opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not leaving instance")
}

func TestMarkShardAsAvailableWithoutValidation(t *testing.T) {
	var (
		cutoverTime      = time.Now().Add(time.Hour)
		cutoverTimeNanos = cutoverTime.UnixNano()
	)
	i1 := placement.NewEmptyInstance("i1", "", "", "e1", 1)
	i1.Shards().Add(shard.NewShard(0).SetState(shard.Leaving).SetCutoffNanos(cutoverTimeNanos))

	i2 := placement.NewEmptyInstance("i2", "", "", "e2", 1)
	i2.Shards().Add(shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i1").SetCutoverNanos(cutoverTimeNanos))

	p := placement.NewPlacement().
		SetInstances([]placement.Instance{i1, i2}).
		SetShards([]uint32{0}).
		SetReplicaFactor(1).
		SetIsSharded(true).
		SetIsMirrored(true)

	opts := placement.NewOptions()
	_, err := markShardsAvailable(p.Clone(), "i2", []uint32{0}, opts)
	assert.NoError(t, err)

	opts = placement.NewOptions().SetIsShardCutoverFn(nil).SetIsShardCutoffFn(nil)
	_, err = markShardsAvailable(p.Clone(), "i2", []uint32{0}, opts)
	assert.NoError(t, err)
}

func TestMarkShardAsAvailableWithValidation(t *testing.T) {
	var (
		cutoverTime           = time.Now()
		cutoverTimeNanos      = cutoverTime.UnixNano()
		maxTimeWindow         = time.Hour
		tenMinutesInThePast   = cutoverTime.Add(1 - 0*time.Minute)
		tenMinutesInTheFuture = cutoverTime.Add(10 * time.Minute)
		oneHourInTheFuture    = cutoverTime.Add(maxTimeWindow)
	)
	i1 := placement.NewEmptyInstance("i1", "", "", "e1", 1)
	i1.Shards().Add(shard.NewShard(0).SetState(shard.Leaving).SetCutoffNanos(cutoverTimeNanos))

	i2 := placement.NewEmptyInstance("i2", "", "", "e2", 1)
	i2.Shards().Add(shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i1").SetCutoverNanos(cutoverTimeNanos))

	p := placement.NewPlacement().
		SetInstances([]placement.Instance{i1, i2}).
		SetShards([]uint32{0}).
		SetReplicaFactor(1).
		SetIsSharded(true).
		SetIsMirrored(true)

	opts := placement.NewOptions().
		SetIsShardCutoverFn(genShardCutoverFn(tenMinutesInThePast)).
		SetIsShardCutoffFn(genShardCutoffFn(tenMinutesInThePast, time.Hour))
	_, err := markShardsAvailable(p.Clone(), "i2", []uint32{0}, opts)
	assert.Error(t, err)

	opts = placement.NewOptions().
		SetIsShardCutoverFn(genShardCutoverFn(tenMinutesInTheFuture)).
		SetIsShardCutoffFn(genShardCutoffFn(tenMinutesInTheFuture, time.Hour))
	_, err = markShardsAvailable(p.Clone(), "i2", []uint32{0}, opts)
	assert.Error(t, err)

	opts = placement.NewOptions().
		SetIsShardCutoverFn(genShardCutoverFn(oneHourInTheFuture)).
		SetIsShardCutoffFn(genShardCutoffFn(oneHourInTheFuture, time.Hour))
	p, err = markShardsAvailable(p.Clone(), "i2", []uint32{0}, opts)
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
}

func TestRemoveInstanceFromArray(t *testing.T) {
	instances := []placement.Instance{
		placement.NewEmptyInstance("i1", "", "", "endpoint", 1),
		placement.NewEmptyInstance("i2", "", "", "endpoint", 1),
	}

	assert.Equal(t, instances, removeInstanceFromList(instances, "not_exist"))
	assert.Equal(t, []placement.Instance{placement.NewEmptyInstance("i2", "", "", "endpoint", 1)}, removeInstanceFromList(instances, "i1"))
}

func TestMarkAllAsAvailable(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "", "", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Initializing))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Initializing))

	i2 := placement.NewEmptyInstance("i2", "", "", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(1).SetState(shard.Initializing))
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Initializing))

	p := placement.NewPlacement().
		SetInstances([]placement.Instance{i1, i2}).
		SetShards([]uint32{1, 2}).
		SetReplicaFactor(2)

	opts := placement.NewOptions()
	_, _, err := markAllShardsAvailable(p, opts)
	assert.NoError(t, err)

	i2.Shards().Add(shard.NewShard(3).SetState(shard.Initializing).SetSourceID("i3"))
	p = placement.NewPlacement().
		SetInstances([]placement.Instance{i1, i2}).
		SetShards([]uint32{1, 2}).
		SetReplicaFactor(2)
	_, _, err = markAllShardsAvailable(p, opts)
	assert.Contains(t, err.Error(), "does not exist in placement")
}

// nolint: dupl
func TestOptimize(t *testing.T) {
	rf := 1
	tests := []struct {
		name            string
		shards          []uint32
		instancesBefore []placement.Instance
		instancesAfter  []placement.Instance
	}{
		{
			name:            "empty",
			instancesBefore: []placement.Instance{},
			instancesAfter:  []placement.Instance{},
		},
		{
			name:   "no optimization when instance is off by one",
			shards: []uint32{1, 2, 3, 4, 5},
			instancesBefore: []placement.Instance{
				placement.NewInstance().SetID("i1").SetWeight(1).SetShards(
					shard.NewShards([]shard.Shard{
						shard.NewShard(1).SetState(shard.Available),
						shard.NewShard(2).SetState(shard.Available),
					})),
				placement.NewInstance().SetID("i2").SetWeight(1).SetShards(
					shard.NewShards([]shard.Shard{
						shard.NewShard(3).SetState(shard.Available),
						shard.NewShard(4).SetState(shard.Available),
					})),
				placement.NewInstance().SetID("i3").SetWeight(1).SetShards(
					shard.NewShards([]shard.Shard{
						shard.NewShard(5).SetState(shard.Available),
					})),
			},
			instancesAfter: []placement.Instance{
				placement.NewInstance().SetID("i1").SetWeight(1).SetShards(
					shard.NewShards([]shard.Shard{
						shard.NewShard(1).SetState(shard.Available),
						shard.NewShard(2).SetState(shard.Available),
					})),
				placement.NewInstance().SetID("i2").SetWeight(1).SetShards(
					shard.NewShards([]shard.Shard{
						shard.NewShard(3).SetState(shard.Available),
						shard.NewShard(4).SetState(shard.Available),
					})),
				placement.NewInstance().SetID("i3").SetWeight(1).SetShards(
					shard.NewShards([]shard.Shard{
						shard.NewShard(5).SetState(shard.Available),
					})),
			},
		},
		{
			name:   "optimizing one imbalanced instance",
			shards: []uint32{1, 2, 3, 4, 5, 6},
			instancesBefore: []placement.Instance{
				placement.NewInstance().SetID("i1").SetWeight(1).SetShards(
					shard.NewShards([]shard.Shard{
						shard.NewShard(1).SetState(shard.Available),
						shard.NewShard(2).SetState(shard.Available),
						shard.NewShard(3).SetState(shard.Available),
					})),
				placement.NewInstance().SetID("i2").SetWeight(1).SetShards(
					shard.NewShards([]shard.Shard{
						shard.NewShard(4).SetState(shard.Available),
						shard.NewShard(5).SetState(shard.Available),
					})),
				placement.NewInstance().SetID("i3").SetWeight(1).SetShards(
					shard.NewShards([]shard.Shard{
						shard.NewShard(6).SetState(shard.Available),
					})),
			},
			instancesAfter: []placement.Instance{
				placement.NewInstance().SetID("i1").SetWeight(1).SetShards(
					shard.NewShards([]shard.Shard{
						shard.NewShard(1).SetState(shard.Leaving),
						shard.NewShard(2).SetState(shard.Available),
						shard.NewShard(3).SetState(shard.Available),
					})),
				placement.NewInstance().SetID("i2").SetWeight(1).SetShards(
					shard.NewShards([]shard.Shard{
						shard.NewShard(4).SetState(shard.Available),
						shard.NewShard(5).SetState(shard.Available),
					})),
				placement.NewInstance().SetID("i3").SetWeight(1).SetShards(
					shard.NewShards([]shard.Shard{
						shard.NewShard(1).SetState(shard.Unknown).SetSourceID("i1"),
						shard.NewShard(6).SetState(shard.Available),
					})),
			},
		},
		{
			name:   "optimizing multiple imbalanced instances",
			shards: []uint32{1, 2, 3, 4, 5, 6, 7, 8},
			instancesBefore: []placement.Instance{
				placement.NewInstance().SetID("i1").SetWeight(1).SetShards(
					shard.NewShards([]shard.Shard{
						shard.NewShard(1).SetState(shard.Available),
						shard.NewShard(2).SetState(shard.Available),
						shard.NewShard(3).SetState(shard.Available),
						shard.NewShard(4).SetState(shard.Available),
					})),
				placement.NewInstance().SetID("i2").SetWeight(1).SetShards(
					shard.NewShards([]shard.Shard{
						shard.NewShard(5).SetState(shard.Available),
						shard.NewShard(6).SetState(shard.Available),
					})),
				placement.NewInstance().SetID("i3").SetWeight(1).SetShards(
					shard.NewShards([]shard.Shard{
						shard.NewShard(7).SetState(shard.Available),
						shard.NewShard(8).SetState(shard.Available),
					})),
			},
			instancesAfter: []placement.Instance{
				placement.NewInstance().SetID("i1").SetWeight(1).SetShards(
					shard.NewShards([]shard.Shard{
						shard.NewShard(1).SetState(shard.Leaving),
						shard.NewShard(2).SetState(shard.Available),
						shard.NewShard(3).SetState(shard.Available),
						shard.NewShard(4).SetState(shard.Available),
					})),
				placement.NewInstance().SetID("i2").SetWeight(1).SetShards(
					shard.NewShards([]shard.Shard{
						shard.NewShard(1).SetState(shard.Unknown).SetSourceID("i1"),
						shard.NewShard(5).SetState(shard.Available),
						shard.NewShard(6).SetState(shard.Available),
					})),
				placement.NewInstance().SetID("i3").SetWeight(1).SetShards(
					shard.NewShards([]shard.Shard{
						shard.NewShard(7).SetState(shard.Available),
						shard.NewShard(8).SetState(shard.Available),
					})),
			},
		},
		{
			name:   "no optimization for balanced instances with different weights",
			shards: []uint32{1, 2, 3, 4, 5, 6, 7, 8},
			instancesBefore: []placement.Instance{
				placement.NewInstance().SetID("i1").SetWeight(2).SetShards(
					shard.NewShards([]shard.Shard{
						shard.NewShard(1).SetState(shard.Available),
						shard.NewShard(2).SetState(shard.Available),
						shard.NewShard(3).SetState(shard.Available),
						shard.NewShard(4).SetState(shard.Available),
					})),
				placement.NewInstance().SetID("i2").SetWeight(1).SetShards(
					shard.NewShards([]shard.Shard{
						shard.NewShard(5).SetState(shard.Available),
						shard.NewShard(6).SetState(shard.Available),
					})),
				placement.NewInstance().SetID("i3").SetWeight(1).SetShards(
					shard.NewShards([]shard.Shard{
						shard.NewShard(7).SetState(shard.Available),
						shard.NewShard(8).SetState(shard.Available),
					})),
			},
			instancesAfter: []placement.Instance{
				placement.NewInstance().SetID("i1").SetWeight(2).SetShards(
					shard.NewShards([]shard.Shard{
						shard.NewShard(1).SetState(shard.Available),
						shard.NewShard(2).SetState(shard.Available),
						shard.NewShard(3).SetState(shard.Available),
						shard.NewShard(4).SetState(shard.Available),
					})),
				placement.NewInstance().SetID("i2").SetWeight(1).SetShards(
					shard.NewShards([]shard.Shard{
						shard.NewShard(5).SetState(shard.Available),
						shard.NewShard(6).SetState(shard.Available),
					})),
				placement.NewInstance().SetID("i3").SetWeight(1).SetShards(
					shard.NewShards([]shard.Shard{
						shard.NewShard(7).SetState(shard.Available),
						shard.NewShard(8).SetState(shard.Available),
					})),
			},
		},
		{
			name:   "optimization for imbalanced instances with different weights",
			shards: []uint32{1, 2, 3, 4, 5, 6, 7, 8},
			instancesBefore: []placement.Instance{
				placement.NewInstance().SetID("i1").SetWeight(2).SetShards(
					shard.NewShards([]shard.Shard{
						shard.NewShard(1).SetState(shard.Available),
						shard.NewShard(2).SetState(shard.Available),
						shard.NewShard(3).SetState(shard.Available),
					})),
				placement.NewInstance().SetID("i2").SetWeight(1).SetShards(
					shard.NewShards([]shard.Shard{
						shard.NewShard(4).SetState(shard.Available),
						shard.NewShard(5).SetState(shard.Available),
						shard.NewShard(6).SetState(shard.Available),
					})),
				placement.NewInstance().SetID("i3").SetWeight(1).SetShards(
					shard.NewShards([]shard.Shard{
						shard.NewShard(7).SetState(shard.Available),
						shard.NewShard(8).SetState(shard.Available),
					})),
			},
			instancesAfter: []placement.Instance{
				placement.NewInstance().SetID("i1").SetWeight(2).SetShards(
					shard.NewShards([]shard.Shard{
						shard.NewShard(1).SetState(shard.Available),
						shard.NewShard(2).SetState(shard.Available),
						shard.NewShard(3).SetState(shard.Available),
						shard.NewShard(4).SetState(shard.Unknown).SetSourceID("i2"),
					})),
				placement.NewInstance().SetID("i2").SetWeight(1).SetShards(
					shard.NewShards([]shard.Shard{
						shard.NewShard(4).SetState(shard.Leaving),
						shard.NewShard(5).SetState(shard.Available),
						shard.NewShard(6).SetState(shard.Available),
					})),
				placement.NewInstance().SetID("i3").SetWeight(1).SetShards(
					shard.NewShards([]shard.Shard{
						shard.NewShard(7).SetState(shard.Available),
						shard.NewShard(8).SetState(shard.Available),
					})),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := placement.NewPlacement().SetShards(tt.shards).SetReplicaFactor(rf).SetInstances(tt.instancesBefore)
			ph := newHelper(p, rf, placement.NewOptions())

			err := ph.optimize(unsafe)
			assert.NoError(t, err)

			instancesAfter := ph.Instances()
			sort.Slice(instancesAfter, func(i, j int) bool {
				return instancesAfter[i].ID() < instancesAfter[j].ID()
			})
			require.Equal(t, len(tt.instancesAfter), len(instancesAfter))
			for i, actual := range instancesAfter {
				assert.Equal(t, tt.instancesAfter[i].String(), actual.String())
			}
		})
	}
}

func genShardCutoverFn(now time.Time) placement.ShardValidateFn {
	return func(s shard.Shard) error {
		switch s.State() {
		case shard.Initializing:
			if s.CutoverNanos() > now.UnixNano() {
				return fmt.Errorf("could only mark shard %d available after %v", s.ID(), time.Unix(0, s.CutoverNanos()))
			}
			return nil
		default:
			return fmt.Errorf("could not mark shard %d available, invalid state %s", s.ID(), s.State().String())
		}
	}
}

func genShardCutoffFn(now time.Time, maxWindowSize time.Duration) placement.ShardValidateFn {
	return func(s shard.Shard) error {
		switch s.State() {
		case shard.Leaving:
			if s.CutoffNanos() > now.UnixNano()-maxWindowSize.Nanoseconds() {
				return fmt.Errorf("could not return shard %d", s.ID())
			}
			return nil
		default:
			return fmt.Errorf("could not mark shard %d available, invalid state %s", s.ID(), s.State().String())
		}
	}
}
