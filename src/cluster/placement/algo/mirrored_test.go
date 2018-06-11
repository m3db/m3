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
	"time"

	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/shard"

	"github.com/stretchr/testify/assert"
)

func TestMirrorWorkflow(t *testing.T) {
	i1 := placement.NewInstance().
		SetID("i1").
		SetIsolationGroup("r1").
		SetEndpoint("endpoint1").
		SetShardSetID(1).
		SetWeight(1)
	i2 := placement.NewInstance().
		SetID("i2").
		SetIsolationGroup("r2").
		SetEndpoint("endpoint2").
		SetShardSetID(1).
		SetWeight(1)
	i3 := placement.NewInstance().
		SetID("i3").
		SetIsolationGroup("r3").
		SetEndpoint("endpoint3").
		SetShardSetID(2).
		SetWeight(2)
	i4 := placement.NewInstance().
		SetID("i4").
		SetIsolationGroup("r4").
		SetEndpoint("endpoint4").
		SetShardSetID(2).
		SetWeight(2)
	i5 := placement.NewInstance().
		SetID("i5").
		SetIsolationGroup("r5").
		SetEndpoint("endpoint5").
		SetShardSetID(3).
		SetWeight(3)
	i6 := placement.NewInstance().
		SetID("i6").
		SetIsolationGroup("r6").
		SetEndpoint("endpoint6").
		SetShardSetID(3).
		SetWeight(3)

	instances := []placement.Instance{i1, i2, i3, i4, i5, i6}

	numShards := 1024
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewAlgorithm(placement.NewOptions().SetIsMirrored(true).
		SetPlacementCutoverNanosFn(timeNanosGen(1)).
		SetShardCutoverNanosFn(timeNanosGen(2)).
		SetShardCutoffNanosFn(timeNanosGen(3)))
	p, err := a.InitialPlacement(instances, ids, 2)
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
	assert.True(t, p.IsMirrored())
	assert.Equal(t, 2, p.ReplicaFactor())
	assert.Equal(t, int64(1), p.CutoverNanos())
	assert.Equal(t, uint32(3), p.MaxShardSetID())

	_, err = a.AddInstances(p, []placement.Instance{placement.NewEmptyInstance("xxx", "rrr", "zzz", "endpoint", 1)})
	assert.Error(t, err)

	p, err = a.AddInstances(p, []placement.Instance{})
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))

	i7 := placement.NewInstance().
		SetID("i7").
		SetIsolationGroup("r7").
		SetEndpoint("endpoint7").
		SetShardSetID(4).
		SetWeight(4)
	i8 := placement.NewInstance().
		SetID("i8").
		SetIsolationGroup("r8").
		SetEndpoint("endpoint8").
		SetShardSetID(4).
		SetWeight(4)
	p, err = a.AddInstances(p, []placement.Instance{i7, i8})
	assert.NoError(t, err)
	assert.Equal(t, uint32(4), p.MaxShardSetID())
	validateDistribution(t, p, 1.01)

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
	assert.Equal(t, uint32(4), p.MaxShardSetID())

	p, err = a.RemoveInstances(p, []string{i7.ID(), i8.ID()})
	assert.NoError(t, err)
	assert.Equal(t, uint32(4), p.MaxShardSetID())
	assert.NoError(t, placement.Validate(p))

	p, err = a.ReplaceInstances(p, []string{"i6"}, []placement.Instance{
		placement.NewInstance().
			SetID("i16").
			SetIsolationGroup("r6").
			SetEndpoint("endpoint6").
			SetShardSetID(3).
			SetWeight(3),
	})
	assert.NoError(t, err)
	assert.Equal(t, uint32(4), p.MaxShardSetID())
	assert.NoError(t, placement.Validate(p))

	i9 := placement.NewInstance().
		SetID("i9").
		SetIsolationGroup("r9").
		SetEndpoint("endpoint9").
		SetShardSetID(5).
		SetWeight(1)
	i10 := placement.NewInstance().
		SetID("i10").
		SetIsolationGroup("r10").
		SetEndpoint("endpoint10").
		SetShardSetID(5).
		SetWeight(1)
	p, err = a.AddInstances(p, []placement.Instance{i9, i10})
	assert.NoError(t, err)
	assert.Equal(t, uint32(5), p.MaxShardSetID())
	validateDistribution(t, p, 1.01)

	p, err = a.ReplaceInstances(p, []string{"i9"}, []placement.Instance{
		placement.NewInstance().
			SetID("i19").
			SetIsolationGroup("r9").
			SetEndpoint("endpoint19").
			SetShardSetID(5).
			SetWeight(1),
	})
	assert.NoError(t, err)
	assert.Equal(t, uint32(5), p.MaxShardSetID())
	assert.NoError(t, placement.Validate(p))

	p, err = a.RemoveInstances(p, []string{"i19", "i10"})
	assert.NoError(t, err)
	assert.Equal(t, uint32(5), p.MaxShardSetID())
	assert.NoError(t, placement.Validate(p))
}

func TestMirrorTestAddAndRevertBeforeCutover(t *testing.T) {
	i1 := placement.NewInstance().
		SetID("i1").
		SetIsolationGroup("r1").
		SetEndpoint("endpoint1").
		SetShardSetID(1).
		SetWeight(1)
	i2 := placement.NewInstance().
		SetID("i2").
		SetIsolationGroup("r2").
		SetEndpoint("endpoint2").
		SetShardSetID(1).
		SetWeight(1)
	i3 := placement.NewInstance().
		SetID("i3").
		SetIsolationGroup("r3").
		SetEndpoint("endpoint3").
		SetShardSetID(2).
		SetWeight(2)
	i4 := placement.NewInstance().
		SetID("i4").
		SetIsolationGroup("r4").
		SetEndpoint("endpoint4").
		SetShardSetID(2).
		SetWeight(2)
	i5 := placement.NewInstance().
		SetID("i5").
		SetIsolationGroup("r5").
		SetEndpoint("endpoint5").
		SetShardSetID(3).
		SetWeight(3)
	i6 := placement.NewInstance().
		SetID("i6").
		SetIsolationGroup("r6").
		SetEndpoint("endpoint6").
		SetShardSetID(3).
		SetWeight(3)

	instances := []placement.Instance{i1, i2, i3, i4}

	numShards := 100
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	now := time.Now()
	nowNanos := now.UnixNano()
	shardCutoverTime := now.Add(time.Hour).UnixNano()
	opts := placement.NewOptions().
		SetIsMirrored(true).
		SetShardCutoverNanosFn(func() int64 { return shardCutoverTime }).
		SetShardCutoffNanosFn(func() int64 { return shardCutoverTime })
	a := NewAlgorithm(opts)
	p, err := a.InitialPlacement(instances, ids, 2)
	assert.NoError(t, err)
	assert.Equal(t, uint32(2), p.MaxShardSetID())
	assert.NoError(t, placement.Validate(p))

	p, _, err = a.MarkAllShardsAvailable(p)
	assert.NoError(t, err)

	p1, err := a.AddInstances(p, []placement.Instance{i5, i6})
	assert.NoError(t, err)
	assert.Equal(t, uint32(3), p1.MaxShardSetID())
	assert.NoError(t, placement.Validate(p1))
	assert.True(t, allInitializing(p1, []string{"i5", "i6"}, nowNanos))

	p2, err := a.RemoveInstances(p1, []string{"i5", "i6"})
	assert.NoError(t, err)
	assert.Equal(t, uint32(3), p2.MaxShardSetID())
	assert.NoError(t, placement.Validate(p2))
	_, ok := p2.Instance("i5")
	assert.False(t, ok)
	_, ok = p2.Instance("i6")
	assert.False(t, ok)

	assert.NoError(t, err)
	assert.Equal(t, p.SetMaxShardSetID(3), p2)
}

func TestMirrorTestAddAndRevertAfterCutover(t *testing.T) {
	i1 := placement.NewInstance().
		SetID("i1").
		SetIsolationGroup("r1").
		SetEndpoint("endpoint1").
		SetShardSetID(1).
		SetWeight(1)
	i2 := placement.NewInstance().
		SetID("i2").
		SetIsolationGroup("r2").
		SetEndpoint("endpoint2").
		SetShardSetID(1).
		SetWeight(1)
	i3 := placement.NewInstance().
		SetID("i3").
		SetIsolationGroup("r3").
		SetEndpoint("endpoint3").
		SetShardSetID(2).
		SetWeight(2)
	i4 := placement.NewInstance().
		SetID("i4").
		SetIsolationGroup("r4").
		SetEndpoint("endpoint4").
		SetShardSetID(2).
		SetWeight(2)
	i5 := placement.NewInstance().
		SetID("i5").
		SetIsolationGroup("r5").
		SetEndpoint("endpoint5").
		SetShardSetID(3).
		SetWeight(3)
	i6 := placement.NewInstance().
		SetID("i6").
		SetIsolationGroup("r6").
		SetEndpoint("endpoint6").
		SetShardSetID(3).
		SetWeight(3)

	instances := []placement.Instance{i1, i2, i3, i4}

	numShards := 100
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	now := time.Now()
	nowNanos := now.UnixNano()
	shardCutoverTime := now.Add(-time.Hour).UnixNano()
	a := NewAlgorithm(placement.NewOptions().
		SetIsMirrored(true).
		SetShardCutoverNanosFn(func() int64 { return shardCutoverTime }).
		SetShardCutoffNanosFn(func() int64 { return shardCutoverTime }))
	p, err := a.InitialPlacement(instances, ids, 2)
	assert.Equal(t, uint32(2), p.MaxShardSetID())
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))

	p1, err := a.AddInstances(p, []placement.Instance{i5, i6})
	assert.NoError(t, err)
	assert.Equal(t, uint32(3), p1.MaxShardSetID())
	assert.NoError(t, placement.Validate(p1))
	assert.False(t, allInitializing(p1, []string{"i5", "i6"}, nowNanos))

	p2, err := a.RemoveInstances(p1, []string{"i5", "i6"})
	assert.NoError(t, err)
	assert.Equal(t, uint32(3), p2.MaxShardSetID())
	assert.NoError(t, placement.Validate(p2))
	i5, ok := p2.Instance("i5")
	assert.True(t, ok)
	assert.True(t, i5.IsLeaving())
	i6, ok = p2.Instance("i6")
	assert.True(t, ok)
	assert.True(t, i6.IsLeaving())
}

func TestMirrorTestRemoveAndRevertBeforeCutover(t *testing.T) {
	i1 := placement.NewInstance().
		SetID("i1").
		SetIsolationGroup("r1").
		SetEndpoint("endpoint1").
		SetShardSetID(1).
		SetWeight(1)
	i2 := placement.NewInstance().
		SetID("i2").
		SetIsolationGroup("r2").
		SetEndpoint("endpoint2").
		SetShardSetID(1).
		SetWeight(1)
	i3 := placement.NewInstance().
		SetID("i3").
		SetIsolationGroup("r3").
		SetEndpoint("endpoint3").
		SetShardSetID(2).
		SetWeight(2)
	i4 := placement.NewInstance().
		SetID("i4").
		SetIsolationGroup("r4").
		SetEndpoint("endpoint4").
		SetShardSetID(2).
		SetWeight(2)
	i5 := placement.NewInstance().
		SetID("i5").
		SetIsolationGroup("r5").
		SetEndpoint("endpoint5").
		SetShardSetID(3).
		SetWeight(3)
	i6 := placement.NewInstance().
		SetID("i6").
		SetIsolationGroup("r6").
		SetEndpoint("endpoint6").
		SetShardSetID(3).
		SetWeight(3)

	instances := []placement.Instance{i1, i2, i3, i4, i5, i6}

	numShards := 100
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	now := time.Now()
	nowNanos := now.UnixNano()
	shardCutoverTime := now.Add(time.Hour).UnixNano()
	opts := placement.NewOptions().
		SetIsMirrored(true).
		SetShardCutoverNanosFn(func() int64 { return shardCutoverTime }).
		SetShardCutoffNanosFn(func() int64 { return shardCutoverTime })
	a := NewAlgorithm(opts)
	p, err := a.InitialPlacement(instances, ids, 2)
	assert.NoError(t, err)
	assert.Equal(t, uint32(3), p.MaxShardSetID())
	assert.NoError(t, placement.Validate(p))

	p, _, err = a.MarkAllShardsAvailable(p)
	assert.NoError(t, err)

	p1, err := a.RemoveInstances(p, []string{"i5", "i6"})
	assert.NoError(t, err)
	assert.Equal(t, uint32(3), p1.MaxShardSetID())
	assert.NoError(t, placement.Validate(p1))
	assert.True(t, allLeaving(p1, []placement.Instance{i5, i6}, nowNanos))

	p2, err := a.AddInstances(p1, []placement.Instance{i5, i6})
	assert.NoError(t, err)
	assert.Equal(t, uint32(3), p2.MaxShardSetID())
	assert.NoError(t, placement.Validate(p2))
	i5, ok := p2.Instance("i5")
	assert.True(t, ok)
	assert.Equal(t, i5.Shards().NumShards(), i5.Shards().NumShardsForState(shard.Available))
	i6, ok = p2.Instance("i6")
	assert.True(t, ok)
	assert.Equal(t, i6.Shards().NumShards(), i6.Shards().NumShardsForState(shard.Available))

	assert.NoError(t, err)
	assert.Equal(t, p, p2)
}

func TestMirrorTestRemoveAndRevertAfterCutover(t *testing.T) {
	i1 := placement.NewInstance().
		SetID("i1").
		SetIsolationGroup("r1").
		SetEndpoint("endpoint1").
		SetShardSetID(1).
		SetWeight(1)
	i2 := placement.NewInstance().
		SetID("i2").
		SetIsolationGroup("r2").
		SetEndpoint("endpoint2").
		SetShardSetID(1).
		SetWeight(1)
	i3 := placement.NewInstance().
		SetID("i3").
		SetIsolationGroup("r3").
		SetEndpoint("endpoint3").
		SetShardSetID(2).
		SetWeight(2)
	i4 := placement.NewInstance().
		SetID("i4").
		SetIsolationGroup("r4").
		SetEndpoint("endpoint4").
		SetShardSetID(2).
		SetWeight(2)
	i5 := placement.NewInstance().
		SetID("i5").
		SetIsolationGroup("r5").
		SetEndpoint("endpoint5").
		SetShardSetID(3).
		SetWeight(3)
	i6 := placement.NewInstance().
		SetID("i6").
		SetIsolationGroup("r6").
		SetEndpoint("endpoint6").
		SetShardSetID(3).
		SetWeight(3)

	instances := []placement.Instance{i1, i2, i3, i4, i5, i6}

	numShards := 10
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	now := time.Now()
	nowNanos := now.UnixNano()
	shardCutoverTime := now.Add(-time.Hour).UnixNano()
	a := NewAlgorithm(placement.NewOptions().
		SetIsMirrored(true).
		SetShardCutoverNanosFn(func() int64 { return shardCutoverTime }).
		SetShardCutoffNanosFn(func() int64 { return shardCutoverTime }))
	p, err := a.InitialPlacement(instances, ids, 2)
	assert.NoError(t, err)
	assert.Equal(t, uint32(3), p.MaxShardSetID())
	assert.NoError(t, placement.Validate(p))

	p1, err := a.RemoveInstances(p, []string{"i5", "i6"})
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p1))
	assert.Equal(t, uint32(3), p1.MaxShardSetID())
	assert.False(t, allLeaving(p1, []placement.Instance{i5, i6}, nowNanos))

	p2, err := a.AddInstances(p1, []placement.Instance{i5.SetShards(shard.NewShards(nil)), i6.SetShards(shard.NewShards(nil))})
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p2))
	assert.Equal(t, uint32(3), p2.MaxShardSetID())

	i5, ok := p2.Instance("i5")
	assert.True(t, ok)
	assert.True(t, i5.IsInitializing())
	i6, ok = p2.Instance("i6")
	assert.True(t, ok)
	assert.True(t, i6.IsInitializing())
}

func TestMirrorTestReplaceAndRevertBeforeCutover(t *testing.T) {
	i1 := placement.NewInstance().
		SetID("i1").
		SetIsolationGroup("r1").
		SetEndpoint("endpoint1").
		SetShardSetID(1).
		SetWeight(1)
	i2 := placement.NewInstance().
		SetID("i2").
		SetIsolationGroup("r2").
		SetEndpoint("endpoint2").
		SetShardSetID(1).
		SetWeight(1)
	i3 := placement.NewInstance().
		SetID("i3").
		SetIsolationGroup("r3").
		SetEndpoint("endpoint3").
		SetShardSetID(2).
		SetWeight(2)
	i4 := placement.NewInstance().
		SetID("i4").
		SetIsolationGroup("r4").
		SetEndpoint("endpoint4").
		SetShardSetID(2).
		SetWeight(2)
	i5 := placement.NewInstance().
		SetID("i5").
		SetIsolationGroup("r5").
		SetEndpoint("endpoint5").
		SetShardSetID(2).
		SetWeight(2)

	instances := []placement.Instance{i1, i2, i3, i4}

	numShards := 100
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	now := time.Now()
	nowNanos := now.UnixNano()
	shardCutoverTime := now.Add(time.Hour).UnixNano()
	a := NewAlgorithm(placement.NewOptions().
		SetIsMirrored(true).
		SetShardCutoverNanosFn(func() int64 { return shardCutoverTime }).
		SetShardCutoffNanosFn(func() int64 { return shardCutoverTime }))
	p, err := a.InitialPlacement(instances, ids, 2)
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
	assert.Equal(t, uint32(2), p.MaxShardSetID())

	p1, err := a.ReplaceInstances(p, []string{"i4"}, []placement.Instance{i5})
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p1))
	assert.Equal(t, uint32(2), p1.MaxShardSetID())
	assert.True(t, allLeaving(p1, []placement.Instance{i4}, nowNanos))
	assert.True(t, allInitializing(p1, []string{"i5"}, nowNanos))

	p2, err := a.ReplaceInstances(p1, []string{"i5"}, []placement.Instance{i4})
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p2))
	assert.Equal(t, uint32(2), p2.MaxShardSetID())
	i4, ok := p2.Instance("i4")
	assert.True(t, ok)
	assert.Equal(t, i4.Shards().NumShards(), i4.Shards().NumShardsForState(shard.Available))
	_, ok = p2.Instance("i5")
	assert.False(t, ok)
}

func TestMirrorTestReplaceAndRevertAfterCutover(t *testing.T) {
	i1 := placement.NewInstance().
		SetID("i1").
		SetIsolationGroup("r1").
		SetEndpoint("endpoint1").
		SetShardSetID(1).
		SetWeight(1)
	i2 := placement.NewInstance().
		SetID("i2").
		SetIsolationGroup("r2").
		SetEndpoint("endpoint2").
		SetShardSetID(1).
		SetWeight(1)
	i3 := placement.NewInstance().
		SetID("i3").
		SetIsolationGroup("r3").
		SetEndpoint("endpoint3").
		SetShardSetID(2).
		SetWeight(2)
	i4 := placement.NewInstance().
		SetID("i4").
		SetIsolationGroup("r4").
		SetEndpoint("endpoint4").
		SetShardSetID(2).
		SetWeight(2)
	i5 := placement.NewInstance().
		SetID("i5").
		SetIsolationGroup("r5").
		SetEndpoint("endpoint5").
		SetShardSetID(2).
		SetWeight(2)

	instances := []placement.Instance{i1, i2, i3, i4}

	numShards := 100
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	now := time.Now()
	nowNanos := now.UnixNano()
	shardCutoverTime := now.Add(-time.Hour).UnixNano()
	a := NewAlgorithm(placement.NewOptions().
		SetIsMirrored(true).
		SetShardCutoverNanosFn(func() int64 { return shardCutoverTime }).
		SetShardCutoffNanosFn(func() int64 { return shardCutoverTime }))
	p, err := a.InitialPlacement(instances, ids, 2)
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
	assert.Equal(t, uint32(2), p.MaxShardSetID())

	p1, err := a.ReplaceInstances(p, []string{"i4"}, []placement.Instance{i5})
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p1))
	assert.Equal(t, uint32(2), p1.MaxShardSetID())
	assert.False(t, allLeaving(p1, []placement.Instance{i4}, nowNanos))
	assert.False(t, allInitializing(p1, []string{"i5"}, nowNanos))

	i4, ok := p1.Instance("i4")
	assert.True(t, ok)
	assert.True(t, i4.IsLeaving())
	ssI4 := i4.Shards()
	i5, ok = p1.Instance("i5")
	assert.True(t, ok)
	assert.True(t, i5.IsInitializing())
	ssI5 := i5.Shards()

	p2, err := a.ReplaceInstances(p1, []string{"i5"}, []placement.Instance{i4})
	assert.Equal(t, uint32(2), p2.MaxShardSetID())
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p2))

	i4, ok = p2.Instance("i4")
	assert.True(t, ok)
	assert.True(t, i4.IsInitializing())
	i5, ok = p2.Instance("i5")
	assert.True(t, ok)
	assert.True(t, i5.IsLeaving())

	assert.True(t, ssI4.Equals(i5.Shards()))
	// Can't directly compare shards.Equals because the shards in ssI5 will be having "i4"
	// as sourceID and shards in i4 will be having "i5" as sourceID.
	assert.Equal(t, ssI5.NumShardsForState(shard.Initializing), i4.Shards().NumShardsForState(shard.Initializing))
}

func TestMirrorInitError(t *testing.T) {
	i1 := placement.NewInstance().
		SetID("i1").
		SetIsolationGroup("r1").
		SetEndpoint("endpoint1").
		SetShardSetID(1).
		SetWeight(1)
	i2 := placement.NewInstance().
		SetID("i2").
		SetIsolationGroup("r2").
		SetEndpoint("endpoint2").
		SetShardSetID(1).
		SetWeight(1)
	i3 := placement.NewInstance().
		SetID("i3").
		SetIsolationGroup("r3").
		SetEndpoint("endpoint3").
		SetShardSetID(2).
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
		SetIsolationGroup("r1").
		SetEndpoint("endpoint1").
		SetShardSetID(1).
		SetWeight(1)
	i2 := placement.NewInstance().
		SetID("i2").
		SetIsolationGroup("r2").
		SetEndpoint("endpoint2").
		SetShardSetID(1).
		SetWeight(1)
	i3 := placement.NewInstance().
		SetID("i3").
		SetIsolationGroup("r3").
		SetEndpoint("endpoint3").
		SetShardSetID(2).
		SetWeight(2)
	i4 := placement.NewInstance().
		SetID("i4").
		SetIsolationGroup("r4").
		SetEndpoint("endpoint4").
		SetShardSetID(2).
		SetWeight(2)
	i5 := placement.NewInstance().
		SetID("i5").
		SetIsolationGroup("r5").
		SetEndpoint("endpoint5").
		SetShardSetID(3).
		SetWeight(3)
	i6 := placement.NewInstance().
		SetID("i6").
		SetIsolationGroup("r6").
		SetEndpoint("endpoint6").
		SetShardSetID(3).
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
	assert.NoError(t, placement.Validate(p))
	assert.Equal(t, uint32(2), p.MaxShardSetID())

	_, err = a.AddInstances(p.Clone().SetIsMirrored(false), []placement.Instance{i5, i6})
	assert.Error(t, err)
	assert.Equal(t, uint32(2), p.MaxShardSetID())

	_, err = a.AddInstances(p.Clone().SetReplicaFactor(1), []placement.Instance{i5, i6})
	assert.Error(t, err)
	assert.Equal(t, uint32(2), p.MaxShardSetID())

	// Allow adding back leaving instances.
	p, err = a.RemoveInstances(p, []string{i3.ID(), i4.ID()})
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
	assert.Equal(t, uint32(2), p.MaxShardSetID())

	p, err = a.AddInstances(p, []placement.Instance{
		placement.NewInstance().
			SetID("i3").
			SetIsolationGroup("r3").
			SetEndpoint("endpoint3").
			SetShardSetID(2).
			SetWeight(2),
		placement.NewInstance().
			SetID("i4").
			SetIsolationGroup("r4").
			SetEndpoint("endpoint4").
			SetShardSetID(2).
			SetWeight(2),
	})
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
	assert.Equal(t, uint32(2), p.MaxShardSetID())

	// Duplicated shardset id.
	_, err = a.AddInstances(p, []placement.Instance{
		placement.NewInstance().
			SetID("i7").
			SetIsolationGroup("r7").
			SetEndpoint("endpoint7").
			SetShardSetID(1).
			SetWeight(3),
		placement.NewInstance().
			SetID("i7").
			SetIsolationGroup("r7").
			SetEndpoint("endpoint7").
			SetShardSetID(1).
			SetWeight(3)},
	)
	assert.Error(t, err)
	assert.Equal(t, uint32(2), p.MaxShardSetID())
}

func TestMirrorRemoveInstancesError(t *testing.T) {
	i1 := placement.NewInstance().
		SetID("i1").
		SetIsolationGroup("r1").
		SetEndpoint("endpoint1").
		SetShardSetID(1).
		SetWeight(1)
	i2 := placement.NewInstance().
		SetID("i2").
		SetIsolationGroup("r2").
		SetEndpoint("endpoint2").
		SetShardSetID(1).
		SetWeight(1)
	i3 := placement.NewInstance().
		SetID("i3").
		SetIsolationGroup("r3").
		SetEndpoint("endpoint3").
		SetShardSetID(2).
		SetWeight(2)
	i4 := placement.NewInstance().
		SetID("i4").
		SetIsolationGroup("r4").
		SetEndpoint("endpoint4").
		SetShardSetID(2).
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
	assert.NoError(t, placement.Validate(p))
	assert.Equal(t, uint32(2), p.MaxShardSetID())

	_, err = a.RemoveInstances(p.SetIsMirrored(false), []string{"i1", "i2"})
	assert.Error(t, err)
	assert.Equal(t, uint32(2), p.MaxShardSetID())

	_, err = a.RemoveInstances(p.SetReplicaFactor(1), []string{"i1", "i2"})
	assert.Error(t, err)
	assert.Equal(t, uint32(2), p.MaxShardSetID())

	_, err = a.RemoveInstances(p, []string{"i1"})
	assert.Error(t, err)
	assert.Equal(t, uint32(2), p.MaxShardSetID())

	_, err = a.RemoveInstances(p, []string{"bad"})
	assert.Error(t, err)
	assert.Equal(t, uint32(2), p.MaxShardSetID())
}

func TestMirrorReplaceInstancesError(t *testing.T) {
	i1 := placement.NewInstance().
		SetID("i1").
		SetIsolationGroup("r1").
		SetEndpoint("endpoint1").
		SetShardSetID(1).
		SetWeight(1)
	i2 := placement.NewInstance().
		SetID("i2").
		SetIsolationGroup("r2").
		SetEndpoint("endpoint2").
		SetShardSetID(1).
		SetWeight(1)
	i3 := placement.NewInstance().
		SetID("i3").
		SetIsolationGroup("r3").
		SetEndpoint("endpoint3").
		SetShardSetID(2).
		SetWeight(2)
	i4 := placement.NewInstance().
		SetID("i4").
		SetIsolationGroup("r4").
		SetEndpoint("endpoint4").
		SetShardSetID(2).
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
	assert.NoError(t, placement.Validate(p))
	assert.Equal(t, uint32(2), p.MaxShardSetID())

	_, err = a.ReplaceInstances(p.SetIsMirrored(false), []string{"i1"}, []placement.Instance{
		placement.NewInstance().
			SetID("i11").
			SetIsolationGroup("r1").
			SetEndpoint("endpoint1").
			SetShardSetID(0).
			SetWeight(1),
	})
	assert.Error(t, err)
	assert.Equal(t, uint32(2), p.MaxShardSetID())

	_, err = a.ReplaceInstances(p, []string{"i1"}, []placement.Instance{
		placement.NewInstance().
			SetID("i11").
			SetIsolationGroup("r1").
			SetEndpoint("endpoint1").
			SetShardSetID(0).
			SetWeight(1),
		placement.NewInstance().
			SetID("i12").
			SetIsolationGroup("r1").
			SetEndpoint("endpoint1").
			SetShardSetID(0).
			SetWeight(1),
	})
	assert.Error(t, err)
	assert.Equal(t, uint32(2), p.MaxShardSetID())

	_, err = a.ReplaceInstances(p, []string{"bad"}, []placement.Instance{
		placement.NewInstance().
			SetID("i11").
			SetIsolationGroup("r1").
			SetEndpoint("endpoint1").
			SetShardSetID(0).
			SetWeight(1),
	})
	assert.Error(t, err)
	assert.Equal(t, uint32(2), p.MaxShardSetID())
}

func TestMirrorReplaceWithLeavingShards(t *testing.T) {
	i1 := placement.NewInstance().
		SetID("i1").
		SetIsolationGroup("r1").
		SetEndpoint("endpoint1").
		SetShardSetID(1).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Leaving),
			shard.NewShard(1).SetState(shard.Available),
		}))
	i2 := placement.NewInstance().
		SetID("i2").
		SetIsolationGroup("r2").
		SetEndpoint("endpoint2").
		SetShardSetID(1).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Leaving),
			shard.NewShard(1).SetState(shard.Available),
		}))
	i3 := placement.NewInstance().
		SetID("i3").
		SetIsolationGroup("r3").
		SetEndpoint("endpoint3").
		SetShardSetID(2).
		SetWeight(2).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i1"),
			shard.NewShard(2).SetState(shard.Available),
		}))
	i4 := placement.NewInstance().
		SetID("i4").
		SetIsolationGroup("r4").
		SetEndpoint("endpoint4").
		SetShardSetID(2).
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
		SetIsSharded(true).
		SetMaxShardSetID(2)

	opts := placement.NewOptions().SetIsMirrored(true)
	a := NewAlgorithm(opts)

	replaceI1 := placement.NewInstance().
		SetID("newI1").
		SetIsolationGroup("r1").
		SetEndpoint("endpoint1").
		SetShardSetID(1).
		SetWeight(1)

	replaceI4 := placement.NewInstance().
		SetID("newI4").
		SetIsolationGroup("r4").
		SetEndpoint("endpoint4").
		SetShardSetID(2).
		SetWeight(2)

	p2, err := a.ReplaceInstances(p, []string{"i1", "i4"}, []placement.Instance{replaceI1, replaceI4})
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
	assert.Equal(t, uint32(2), p2.MaxShardSetID())

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
		SetIsolationGroup("r1").
		SetEndpoint("endpoint1").
		SetShardSetID(1).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Available),
		}))
	i2 := placement.NewInstance().
		SetID("i2").
		SetIsolationGroup("r2").
		SetEndpoint("endpoint2").
		SetShardSetID(1).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Available),
		}))

	res, err := groupInstancesByShardSetID([]placement.Instance{i1, i2}, 2)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(res))
	assert.Equal(t, placement.NewInstance().
		SetID("1").
		SetIsolationGroup("1").
		SetShardSetID(1).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Available),
		})), res[0])

	_, err = groupInstancesByShardSetID([]placement.Instance{i1, i2.Clone().SetWeight(2)}, 2)
	assert.Error(t, err)

	_, err = groupInstancesByShardSetID([]placement.Instance{i1, i2.Clone().SetIsolationGroup("r1")}, 2)
	assert.Error(t, err)
}

func TestReturnInitializingShards(t *testing.T) {
	i1 := placement.NewInstance().
		SetID("i1").
		SetIsolationGroup("r1").
		SetEndpoint("endpoint1").
		SetShardSetID(1).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Leaving),
			shard.NewShard(1).SetState(shard.Available),
		}))
	i2 := placement.NewInstance().
		SetID("i2").
		SetIsolationGroup("r2").
		SetEndpoint("endpoint2").
		SetShardSetID(1).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Leaving),
			shard.NewShard(1).SetState(shard.Available),
		}))
	i3 := placement.NewInstance().
		SetID("i3").
		SetIsolationGroup("r3").
		SetEndpoint("endpoint3").
		SetShardSetID(1).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i1"),
		}))
	i4 := placement.NewInstance().
		SetID("i4").
		SetIsolationGroup("r1"). // Same isolation group with i1.
		SetEndpoint("endpoint4").
		SetShardSetID(1).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i2"),
		}))

	p := placement.NewPlacement().SetInstances([]placement.Instance{i1, i2, i3, i4}).SetReplicaFactor(2).SetShards([]uint32{0, 1})

	a := NewAlgorithm(placement.NewOptions().SetIsMirrored(true)).(mirroredAlgorithm)
	p1, err := a.returnInitializingShards(p.Clone(), []string{i3.ID(), i4.ID()})
	assert.NoError(t, err)
	assert.Equal(t, 2, p1.NumInstances())
	assert.NoError(t, placement.Validate(p1))
	assert.Equal(t, uint32(1), p1.MaxShardSetID())
	p2, err := a.returnInitializingShards(p.Clone(), []string{i4.ID(), i3.ID()})
	assert.NoError(t, err)
	assert.Equal(t, 2, p2.NumInstances())
	assert.NoError(t, placement.Validate(p2))
	assert.Equal(t, uint32(1), p2.MaxShardSetID())
}

func TestReclaimLeavingShards(t *testing.T) {
	i1 := placement.NewInstance().
		SetID("i1").
		SetIsolationGroup("r1").
		SetEndpoint("endpoint1").
		SetShardSetID(1).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Leaving),
		}))
	i2 := placement.NewInstance().
		SetID("i2").
		SetIsolationGroup("r2").
		SetEndpoint("endpoint2").
		SetShardSetID(1).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Leaving),
		}))
	i3 := placement.NewInstance().
		SetID("i3").
		SetIsolationGroup("r3").
		SetEndpoint("endpoint3").
		SetShardSetID(1).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i1"),
		}))
	i4 := placement.NewInstance().
		SetID("i4").
		SetIsolationGroup("r1"). // Same isolation group with i1.
		SetEndpoint("endpoint4").
		SetShardSetID(1).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i2"),
		}))

	p := placement.NewPlacement().SetInstances([]placement.Instance{i1, i2, i3, i4}).SetReplicaFactor(2).SetShards([]uint32{0})

	a := NewAlgorithm(placement.NewOptions().SetIsMirrored(true)).(mirroredAlgorithm)
	p1, err := a.reclaimLeavingShards(p.Clone(), []placement.Instance{i2, i1})
	assert.NoError(t, err)
	assert.Equal(t, 2, p1.NumInstances())
	assert.NoError(t, placement.Validate(p1))
	assert.Equal(t, uint32(1), p1.MaxShardSetID())
	p2, err := a.reclaimLeavingShards(p.Clone(), []placement.Instance{i1, i2})
	assert.NoError(t, err)
	assert.Equal(t, 2, p2.NumInstances())
	assert.NoError(t, placement.Validate(p2))
	assert.Equal(t, uint32(1), p2.MaxShardSetID())
}

func TestReclaimLeavingShardsWithAvailable(t *testing.T) {
	i1 := placement.NewInstance().
		SetID("i1").
		SetIsolationGroup("r1").
		SetEndpoint("endpoint1").
		SetShardSetID(1).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Leaving),
		}))
	i2 := placement.NewInstance().
		SetID("i2").
		SetIsolationGroup("r2").
		SetEndpoint("endpoint2").
		SetShardSetID(1).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Leaving),
		}))
	i3 := placement.NewInstance().
		SetID("i3").
		SetIsolationGroup("r3").
		SetEndpoint("endpoint3").
		SetShardSetID(2).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i1"),
			shard.NewShard(1).SetState(shard.Available),
		}))
	i4 := placement.NewInstance().
		SetID("i4").
		SetIsolationGroup("r1"). // Same isolation group with i1.
		SetEndpoint("endpoint4").
		SetShardSetID(2).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i2"),
			shard.NewShard(1).SetState(shard.Available),
		}))

	p := placement.NewPlacement().SetInstances([]placement.Instance{i1, i2, i3, i4}).SetReplicaFactor(2).SetShards([]uint32{0, 1})

	a := NewAlgorithm(placement.NewOptions().SetIsMirrored(true)).(mirroredAlgorithm)
	p1, err := a.reclaimLeavingShards(p.Clone(), []placement.Instance{i2, i1})
	assert.NoError(t, err)
	assert.Equal(t, 4, p1.NumInstances())
	assert.NoError(t, placement.Validate(p1))
	assert.Equal(t, uint32(2), p1.MaxShardSetID())
	p2, err := a.reclaimLeavingShards(p.Clone(), []placement.Instance{i1, i2})
	assert.NoError(t, err)
	assert.Equal(t, 4, p2.NumInstances())
	assert.NoError(t, placement.Validate(p2))
	assert.Equal(t, uint32(2), p2.MaxShardSetID())
}

func TestMarkShardAsAvailableWithMirroredAlgo(t *testing.T) {
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

	a := newMirroredAlgorithm(placement.NewOptions().
		SetIsShardCutoverFn(genShardCutoverFn(tenMinutesInThePast)).
		SetIsShardCutoffFn(genShardCutoffFn(tenMinutesInThePast, time.Hour)))
	_, err := a.MarkShardsAvailable(p, "i2", 0)
	assert.Error(t, err)

	a = newMirroredAlgorithm(placement.NewOptions().
		SetIsShardCutoverFn(genShardCutoverFn(tenMinutesInTheFuture)).
		SetIsShardCutoffFn(genShardCutoffFn(tenMinutesInTheFuture, time.Hour)))
	_, err = a.MarkShardsAvailable(p, "i2", 0)
	assert.Error(t, err)

	a = newMirroredAlgorithm(placement.NewOptions().
		SetIsShardCutoverFn(genShardCutoverFn(oneHourInTheFuture)).
		SetIsShardCutoffFn(genShardCutoffFn(oneHourInTheFuture, time.Hour)))
	p, err = a.MarkShardsAvailable(p, "i2", 0)
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
}

func TestMarkShardAsAvailableBulkWithMirroredAlgo(t *testing.T) {
	var (
		cutoverTime        = time.Now()
		cutoverTimeNanos   = cutoverTime.UnixNano()
		maxTimeWindow      = time.Hour
		oneHourInTheFuture = cutoverTime.Add(maxTimeWindow)
	)
	i1 := placement.NewEmptyInstance("i1", "", "", "e1", 1)
	i1.Shards().Add(shard.NewShard(0).SetState(shard.Leaving).SetCutoffNanos(cutoverTimeNanos))
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Leaving).SetCutoffNanos(cutoverTimeNanos))

	i2 := placement.NewEmptyInstance("i2", "", "", "e2", 1)
	i2.Shards().Add(shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i1").SetCutoverNanos(cutoverTimeNanos))
	i2.Shards().Add(shard.NewShard(1).SetState(shard.Initializing).SetSourceID("i1").SetCutoverNanos(cutoverTimeNanos))

	p := placement.NewPlacement().
		SetInstances([]placement.Instance{i1, i2}).
		SetShards([]uint32{0, 1}).
		SetReplicaFactor(1).
		SetIsSharded(true).
		SetIsMirrored(true)

	a := newMirroredAlgorithm(placement.NewOptions().
		SetIsShardCutoverFn(genShardCutoverFn(oneHourInTheFuture)).
		SetIsShardCutoffFn(genShardCutoffFn(oneHourInTheFuture, time.Hour)))
	p, err := a.MarkShardsAvailable(p, "i2", 0, 1)
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))

	mi2, ok := p.Instance("i2")
	assert.True(t, ok)
	shards := mi2.Shards().All()
	assert.Len(t, shards, 2)
	for _, s := range shards {
		assert.Equal(t, shard.Available, s.State())
	}
}

func TestMirrorAlgoWithSimpleShardStateType(t *testing.T) {
	i1 := placement.NewInstance().
		SetID("i1").
		SetIsolationGroup("r1").
		SetEndpoint("endpoint1").
		SetShardSetID(1).
		SetWeight(1)
	i2 := placement.NewInstance().
		SetID("i2").
		SetIsolationGroup("r2").
		SetEndpoint("endpoint2").
		SetShardSetID(1).
		SetWeight(1)
	i3 := placement.NewInstance().
		SetID("i3").
		SetIsolationGroup("r3").
		SetEndpoint("endpoint3").
		SetShardSetID(2).
		SetWeight(2)
	i4 := placement.NewInstance().
		SetID("i4").
		SetIsolationGroup("r4").
		SetEndpoint("endpoint4").
		SetShardSetID(2).
		SetWeight(2)
	i5 := placement.NewInstance().
		SetID("i5").
		SetIsolationGroup("r5").
		SetEndpoint("endpoint5").
		SetShardSetID(3).
		SetWeight(3)
	i6 := placement.NewInstance().
		SetID("i6").
		SetIsolationGroup("r6").
		SetEndpoint("endpoint6").
		SetShardSetID(3).
		SetWeight(3)

	instances := []placement.Instance{i1, i2, i3, i4, i5, i6}

	numShards := 1024
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := NewAlgorithm(placement.NewOptions().SetIsMirrored(true).
		SetPlacementCutoverNanosFn(timeNanosGen(1)).
		SetShardStateMode(placement.StableShardStateOnly))
	p, err := a.InitialPlacement(instances, ids, 2)
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
	verifyAllShardsInAvailableState(t, p)

	i7 := placement.NewInstance().
		SetID("i7").
		SetIsolationGroup("r7").
		SetEndpoint("endpoint7").
		SetShardSetID(4).
		SetWeight(4)
	i8 := placement.NewInstance().
		SetID("i8").
		SetIsolationGroup("r8").
		SetEndpoint("endpoint8").
		SetShardSetID(4).
		SetWeight(4)
	p, err = a.AddInstances(p, []placement.Instance{i7, i8})
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
	verifyAllShardsInAvailableState(t, p)

	p, err = a.RemoveInstances(p, []string{i7.ID(), i8.ID()})
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
	verifyAllShardsInAvailableState(t, p)

	p, err = a.ReplaceInstances(p, []string{"i6"}, []placement.Instance{
		placement.NewInstance().
			SetID("i16").
			SetIsolationGroup("r6").
			SetEndpoint("endpoint6").
			SetShardSetID(3).
			SetWeight(3),
	})
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
	verifyAllShardsInAvailableState(t, p)

	i9 := placement.NewInstance().
		SetID("i9").
		SetIsolationGroup("r9").
		SetEndpoint("endpoint9").
		SetShardSetID(5).
		SetWeight(1)
	i10 := placement.NewInstance().
		SetID("i10").
		SetIsolationGroup("r10").
		SetEndpoint("endpoint10").
		SetShardSetID(5).
		SetWeight(1)
	p, err = a.AddInstances(p, []placement.Instance{i9, i10})
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
	verifyAllShardsInAvailableState(t, p)

	p, err = a.ReplaceInstances(p, []string{"i9"}, []placement.Instance{
		placement.NewInstance().
			SetID("i19").
			SetIsolationGroup("r9").
			SetEndpoint("endpoint19").
			SetShardSetID(5).
			SetWeight(1),
	})
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
	verifyAllShardsInAvailableState(t, p)

	p, err = a.RemoveInstances(p, []string{"i19", "i10"})
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
	verifyAllShardsInAvailableState(t, p)
}
