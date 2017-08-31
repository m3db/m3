// Copyright (c) 2017 Uber Technologies, Inc.
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

	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/shard"

	"github.com/stretchr/testify/assert"
)

func TestMirrorWorkflow(t *testing.T) {
	i1 := placement.NewInstance().
		SetID("i1").
		SetRack("r1").
		SetEndpoint("endpoint1").
		SetShardSetID(0).
		SetWeight(1)
	i2 := placement.NewInstance().
		SetID("i2").
		SetRack("r2").
		SetEndpoint("endpoint2").
		SetShardSetID(0).
		SetWeight(1)
	i3 := placement.NewInstance().
		SetID("i3").
		SetRack("r3").
		SetEndpoint("endpoint3").
		SetShardSetID(1).
		SetWeight(2)
	i4 := placement.NewInstance().
		SetID("i4").
		SetRack("r4").
		SetEndpoint("endpoint4").
		SetShardSetID(1).
		SetWeight(2)
	i5 := placement.NewInstance().
		SetID("i5").
		SetRack("r5").
		SetEndpoint("endpoint5").
		SetShardSetID(2).
		SetWeight(3)
	i6 := placement.NewInstance().
		SetID("i6").
		SetRack("r6").
		SetEndpoint("endpoint6").
		SetShardSetID(2).
		SetWeight(3)

	instances := []placement.Instance{i1, i2, i3, i4, i5, i6}

	numShards := 1024
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewAlgorithm(placement.NewOptions().SetIsMirrored(true))
	p, err := a.InitialPlacement(instances, ids, 2)
	assert.NoError(t, err)
	assert.True(t, p.IsMirrored())
	assert.Equal(t, 2, p.ReplicaFactor())

	_, err = a.AddInstances(p, []placement.Instance{placement.NewEmptyInstance("xxx", "rrr", "zzz", "endpoint", 1)})
	assert.Error(t, err)

	p, err = a.AddInstances(p, []placement.Instance{})
	assert.NoError(t, err)

	i7 := placement.NewInstance().
		SetID("i7").
		SetRack("r7").
		SetEndpoint("endpoint7").
		SetShardSetID(3).
		SetWeight(4)
	i8 := placement.NewInstance().
		SetID("i8").
		SetRack("r8").
		SetEndpoint("endpoint8").
		SetShardSetID(3).
		SetWeight(4)
	p, err = a.AddInstances(p, []placement.Instance{i7, i8})
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "")

	newI1, ok := p.Instance("i1")
	assert.True(t, ok)
	assert.Equal(t, i1.SetShards(newI1.Shards()), newI1)
	newI2, ok := p.Instance("i2")
	assert.True(t, ok)
	assert.Equal(t, i2.SetShards(newI2.Shards()), newI2)
	newI3, ok := p.Instance("i3")
	assert.True(t, ok)
	assert.Equal(t, i3.SetShards(newI3.Shards()), newI3)
	newI4, ok := p.Instance("i4")
	assert.True(t, ok)
	assert.Equal(t, i4.SetShards(newI4.Shards()), newI4)
	newI5, ok := p.Instance("i5")
	assert.True(t, ok)
	assert.Equal(t, i5.SetShards(newI5.Shards()), newI5)
	newI6, ok := p.Instance("i6")
	assert.True(t, ok)
	assert.Equal(t, i6.SetShards(newI6.Shards()), newI6)
	newI7, ok := p.Instance("i7")
	assert.True(t, ok)
	assert.Equal(t, i7.SetShards(newI7.Shards()), newI7)
	newI8, ok := p.Instance("i8")
	assert.True(t, ok)
	assert.Equal(t, i8.SetShards(newI8.Shards()), newI8)

	_, err = a.RemoveInstances(p, []string{i1.ID()})
	assert.Error(t, err)

	p, err = a.RemoveInstances(p, []string{i7.ID(), i8.ID()})
	assert.NoError(t, err)

	p, err = a.ReplaceInstances(p, []string{"i6"}, []placement.Instance{
		placement.NewInstance().
			SetID("i16").
			SetRack("r6").
			SetEndpoint("endpoint6").
			SetShardSetID(2).
			SetWeight(3),
	})
	assert.NoError(t, err)

	i9 := placement.NewInstance().
		SetID("i9").
		SetRack("r9").
		SetEndpoint("endpoint9").
		SetShardSetID(4).
		SetWeight(1)
	i10 := placement.NewInstance().
		SetID("i10").
		SetRack("r10").
		SetEndpoint("endpoint10").
		SetShardSetID(4).
		SetWeight(1)
	p, err = a.AddInstances(p, []placement.Instance{i9, i10})
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "")

	p, err = a.ReplaceInstances(p, []string{"i9"}, []placement.Instance{
		placement.NewInstance().
			SetID("i19").
			SetRack("r9").
			SetEndpoint("endpoint19").
			SetShardSetID(4).
			SetWeight(1),
	})
	assert.NoError(t, err)

	_, err = a.RemoveInstances(p, []string{"i19", "i10"})
	assert.NoError(t, err)
}

func TestMirrorInitError(t *testing.T) {
	i1 := placement.NewInstance().
		SetID("i1").
		SetRack("r1").
		SetEndpoint("endpoint1").
		SetShardSetID(0).
		SetWeight(1)
	i2 := placement.NewInstance().
		SetID("i2").
		SetRack("r2").
		SetEndpoint("endpoint2").
		SetShardSetID(0).
		SetWeight(1)
	i3 := placement.NewInstance().
		SetID("i3").
		SetRack("r3").
		SetEndpoint("endpoint3").
		SetShardSetID(1).
		SetWeight(2)

	instances := []placement.Instance{i1, i2, i3}

	numShards := 100
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewAlgorithm(placement.NewOptions().SetIsMirrored(true))
	_, err := a.InitialPlacement(instances, ids, 2)
	assert.Error(t, err)
}

func TestMirrorAddInstancesError(t *testing.T) {
	i1 := placement.NewInstance().
		SetID("i1").
		SetRack("r1").
		SetEndpoint("endpoint1").
		SetShardSetID(0).
		SetWeight(1)
	i2 := placement.NewInstance().
		SetID("i2").
		SetRack("r2").
		SetEndpoint("endpoint2").
		SetShardSetID(0).
		SetWeight(1)
	i3 := placement.NewInstance().
		SetID("i3").
		SetRack("r3").
		SetEndpoint("endpoint3").
		SetShardSetID(1).
		SetWeight(2)
	i4 := placement.NewInstance().
		SetID("i4").
		SetRack("r4").
		SetEndpoint("endpoint4").
		SetShardSetID(1).
		SetWeight(2)
	i5 := placement.NewInstance().
		SetID("i5").
		SetRack("r5").
		SetEndpoint("endpoint5").
		SetShardSetID(2).
		SetWeight(3)
	i6 := placement.NewInstance().
		SetID("i6").
		SetRack("r6").
		SetEndpoint("endpoint6").
		SetShardSetID(2).
		SetWeight(3)

	instances := []placement.Instance{i1, i2, i3, i4}

	numShards := 100
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewAlgorithm(placement.NewOptions().SetIsMirrored(true))
	p, err := a.InitialPlacement(instances, ids, 2)
	assert.NoError(t, err)

	_, err = a.AddInstances(placement.ClonePlacement(p).SetIsMirrored(false), []placement.Instance{i5, i6})
	assert.Error(t, err)

	_, err = a.AddInstances(placement.ClonePlacement(p).SetReplicaFactor(1), []placement.Instance{i5, i6})
	assert.Error(t, err)

	// Allow adding back leaving instances.
	p, err = a.RemoveInstances(p, []string{i3.ID(), i4.ID()})
	assert.NoError(t, err)

	_, err = a.AddInstances(p, []placement.Instance{i3, i4})
	assert.NoError(t, err)

	// Duplicated shardset id.
	_, err = a.AddInstances(p, []placement.Instance{
		placement.NewInstance().
			SetID("i7").
			SetRack("r7").
			SetEndpoint("endpoint7").
			SetShardSetID(0).
			SetWeight(3),
		placement.NewInstance().
			SetID("i7").
			SetRack("r7").
			SetEndpoint("endpoint7").
			SetShardSetID(0).
			SetWeight(3)},
	)
	assert.Error(t, err)
}

func TestMirrorRemoveInstancesError(t *testing.T) {
	i1 := placement.NewInstance().
		SetID("i1").
		SetRack("r1").
		SetEndpoint("endpoint1").
		SetShardSetID(0).
		SetWeight(1)
	i2 := placement.NewInstance().
		SetID("i2").
		SetRack("r2").
		SetEndpoint("endpoint2").
		SetShardSetID(0).
		SetWeight(1)
	i3 := placement.NewInstance().
		SetID("i3").
		SetRack("r3").
		SetEndpoint("endpoint3").
		SetShardSetID(1).
		SetWeight(2)
	i4 := placement.NewInstance().
		SetID("i4").
		SetRack("r4").
		SetEndpoint("endpoint4").
		SetShardSetID(1).
		SetWeight(2)

	instances := []placement.Instance{i1, i2, i3, i4}

	numShards := 100
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewAlgorithm(placement.NewOptions().SetIsMirrored(true))
	p, err := a.InitialPlacement(instances, ids, 2)
	assert.NoError(t, err)

	_, err = a.RemoveInstances(p.SetIsMirrored(false), []string{"i1", "i2"})
	assert.Error(t, err)

	_, err = a.RemoveInstances(p.SetReplicaFactor(1), []string{"i1", "i2"})
	assert.Error(t, err)

	_, err = a.RemoveInstances(p, []string{"i1"})
	assert.Error(t, err)

	_, err = a.RemoveInstances(p, []string{"bad"})
	assert.Error(t, err)
}

func TestMirrorReplaceInstancesError(t *testing.T) {
	i1 := placement.NewInstance().
		SetID("i1").
		SetRack("r1").
		SetEndpoint("endpoint1").
		SetShardSetID(0).
		SetWeight(1)
	i2 := placement.NewInstance().
		SetID("i2").
		SetRack("r2").
		SetEndpoint("endpoint2").
		SetShardSetID(0).
		SetWeight(1)
	i3 := placement.NewInstance().
		SetID("i3").
		SetRack("r3").
		SetEndpoint("endpoint3").
		SetShardSetID(1).
		SetWeight(2)
	i4 := placement.NewInstance().
		SetID("i4").
		SetRack("r4").
		SetEndpoint("endpoint4").
		SetShardSetID(1).
		SetWeight(2)

	instances := []placement.Instance{i1, i2, i3, i4}

	numShards := 100
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewAlgorithm(placement.NewOptions().SetIsMirrored(true))
	p, err := a.InitialPlacement(instances, ids, 2)
	assert.NoError(t, err)

	_, err = a.ReplaceInstances(p.SetIsMirrored(false), []string{"i1"}, []placement.Instance{
		placement.NewInstance().
			SetID("i11").
			SetRack("r1").
			SetEndpoint("endpoint1").
			SetShardSetID(0).
			SetWeight(1),
	})
	assert.Error(t, err)

	_, err = a.ReplaceInstances(p, []string{"i1"}, []placement.Instance{
		placement.NewInstance().
			SetID("i11").
			SetRack("r1").
			SetEndpoint("endpoint1").
			SetShardSetID(0).
			SetWeight(1),
		placement.NewInstance().
			SetID("i12").
			SetRack("r1").
			SetEndpoint("endpoint1").
			SetShardSetID(0).
			SetWeight(1),
	})
	assert.Error(t, err)

	_, err = a.ReplaceInstances(p, []string{"bad"}, []placement.Instance{
		placement.NewInstance().
			SetID("i11").
			SetRack("r1").
			SetEndpoint("endpoint1").
			SetShardSetID(0).
			SetWeight(1),
	})
	assert.Error(t, err)
}

func TestMirrorReplaceWithLeavingShards(t *testing.T) {
	i1 := placement.NewInstance().
		SetID("i1").
		SetRack("r1").
		SetEndpoint("endpoint1").
		SetShardSetID(0).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Leaving),
			shard.NewShard(1).SetState(shard.Available),
		}))
	i2 := placement.NewInstance().
		SetID("i2").
		SetRack("r2").
		SetEndpoint("endpoint2").
		SetShardSetID(0).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Leaving),
			shard.NewShard(1).SetState(shard.Available),
		}))
	i3 := placement.NewInstance().
		SetID("i3").
		SetRack("r3").
		SetEndpoint("endpoint3").
		SetShardSetID(1).
		SetWeight(2).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i1"),
			shard.NewShard(2).SetState(shard.Available),
		}))
	i4 := placement.NewInstance().
		SetID("i4").
		SetRack("r4").
		SetEndpoint("endpoint4").
		SetShardSetID(1).
		SetWeight(2).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i2"),
			shard.NewShard(2).SetState(shard.Available),
		}))
	p := placement.NewPlacement().
		SetReplicaFactor(2).
		SetShards([]uint32{0, 1, 2}).
		SetInstances([]placement.Instance{i1, i2, i3, i4}).
		SetIsMirrored(true).
		SetIsSharded(true)

	a := NewAlgorithm(placement.NewOptions().SetIsMirrored(true))

	replaceI1 := placement.NewInstance().
		SetID("newI1").
		SetRack("r1").
		SetEndpoint("endpoint1").
		SetShardSetID(0).
		SetWeight(1)

	replaceI4 := placement.NewInstance().
		SetID("newI4").
		SetRack("r4").
		SetEndpoint("endpoint4").
		SetShardSetID(1).
		SetWeight(2)

	p2, err := a.ReplaceInstances(p, []string{"i1", "i4"}, []placement.Instance{replaceI1, replaceI4})
	assert.NoError(t, err)

	_, ok := p2.Instance("i1")
	assert.True(t, ok)
	newI1, ok := p2.Instance("newI1")
	assert.True(t, ok)
	assert.Equal(t, replaceI1.SetShards(
		shard.NewShards([]shard.Shard{
			shard.NewShard(1).SetState(shard.Initializing).SetSourceID("i1"),
		}),
	), newI1)
	_, ok = p2.Instance("i4")
	assert.True(t, ok)
	newI4, ok := p2.Instance("newI4")
	assert.True(t, ok)
	assert.Equal(t, replaceI4.SetShards(
		shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i4"),
			shard.NewShard(2).SetState(shard.Initializing).SetSourceID("i4"),
		}),
	), newI4)
	assert.NoError(t, placement.Validate(p2))

	p2, err = placement.MarkAllShardsAsAvailable(p2)
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p2))
}

func TestIncompatibleWithMirroredAlgo(t *testing.T) {
	a := newMirroredAlgorithm(placement.NewOptions())
	p := placement.NewPlacement()

	err := a.IsCompatibleWith(p)
	assert.Error(t, err)
	assert.Equal(t, errIncompatibleWithMirrorAlgo, err)

	err = a.IsCompatibleWith(p.SetIsSharded(true))
	assert.Error(t, err)
	assert.Equal(t, errIncompatibleWithMirrorAlgo, err)

	err = a.IsCompatibleWith(p.SetIsSharded(true).SetIsMirrored(true))
	assert.Nil(t, err)
}

func TestGroupInstanceByShardSetID(t *testing.T) {
	i1 := placement.NewInstance().
		SetID("i1").
		SetRack("r1").
		SetEndpoint("endpoint1").
		SetShardSetID(0).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Available),
		}))
	i2 := placement.NewInstance().
		SetID("i2").
		SetRack("r2").
		SetEndpoint("endpoint2").
		SetShardSetID(0).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Available),
		}))

	res, err := groupInstancesByShardSetID([]placement.Instance{i1, i2}, 2)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(res))
	assert.Equal(t, placement.NewInstance().
		SetID("0").
		SetRack("0").
		SetShardSetID(0).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Available),
		})), res[0])

	_, err = groupInstancesByShardSetID([]placement.Instance{i1, placement.CloneInstance(i2).SetWeight(2)}, 2)
	assert.Error(t, err)

	_, err = groupInstancesByShardSetID([]placement.Instance{i1, placement.CloneInstance(i2).SetRack("r1")}, 2)
	assert.Error(t, err)
}
