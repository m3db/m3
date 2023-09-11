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
	"errors"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMirrorWorkflow(t *testing.T) {
	i1 := newTestInstance("i1").SetShardSetID(1).SetWeight(1)
	i2 := newTestInstance("i2").SetShardSetID(1).SetWeight(1)
	i3 := newTestInstance("i3").SetShardSetID(2).SetWeight(2)
	i4 := newTestInstance("i4").SetShardSetID(2).SetWeight(2)
	i5 := newTestInstance("i5").SetShardSetID(3).SetWeight(3)
	i6 := newTestInstance("i6").SetShardSetID(3).SetWeight(3)
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

	i7 := newTestInstance("i7").SetShardSetID(4).SetWeight(4)
	i8 := newTestInstance("i8").SetShardSetID(4).SetWeight(4)
	p, err = a.AddInstances(p, []placement.Instance{i7, i8})
	assert.NoError(t, err)
	assert.Equal(t, uint32(4), p.MaxShardSetID())
	validateDistribution(t, p, 1.01)

	// validate InstanceMetadata is still set on all instances
	var zero placement.InstanceMetadata
	for _, inst := range p.Instances() {
		assert.NotEqual(t, zero, inst.Metadata())
	}

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

	i16 := newTestInstance("i16").SetShardSetID(3).SetWeight(3)
	p, err = a.ReplaceInstances(p, []string{"i6"}, []placement.Instance{i16})
	assert.NoError(t, err)
	assert.Equal(t, uint32(4), p.MaxShardSetID())
	assert.NoError(t, placement.Validate(p))

	i9 := newTestInstance("i9").SetShardSetID(5).SetWeight(1)
	i10 := newTestInstance("i10").SetShardSetID(5).SetWeight(1)
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

	_, err = a.ReplaceInstances(p, []string{"foo"}, []placement.Instance{i1})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "instance foo does not exist in placement")
}

func TestMirrorAddAndRevertBeforeCutover(t *testing.T) {
	i1 := newTestInstance("i1").SetShardSetID(1).SetWeight(1)
	i2 := newTestInstance("i2").SetShardSetID(1).SetWeight(1)
	i3 := newTestInstance("i3").SetShardSetID(2).SetWeight(2)
	i4 := newTestInstance("i4").SetShardSetID(2).SetWeight(2)
	i5 := newTestInstance("i5").SetShardSetID(3).SetWeight(3)
	i6 := newTestInstance("i6").SetShardSetID(3).SetWeight(3)
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
	assert.NoError(t, placement.Validate(p))

	p1, err := a.AddInstances(p, []placement.Instance{i5, i6})
	assert.NoError(t, err)
	assert.Equal(t, uint32(3), p1.MaxShardSetID())
	assert.NoError(t, placement.Validate(p1))
	assert.True(t, globalChecker.allInitializing(p1, []string{"i5", "i6"}, nowNanos))

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

func TestMirrorAddMultiplePairs(t *testing.T) {
	i1 := newTestInstance("i1").SetShardSetID(1).SetWeight(1)
	i2 := newTestInstance("i2").SetShardSetID(1).SetWeight(1)
	i3 := newTestInstance("i3").SetShardSetID(2).SetWeight(2)
	i4 := newTestInstance("i4").SetShardSetID(2).SetWeight(2)
	i5 := newTestInstance("i5").SetShardSetID(3).SetWeight(3)
	i6 := newTestInstance("i6").SetShardSetID(3).SetWeight(3)
	i7 := newTestInstance("i7").SetShardSetID(4).SetWeight(3)
	i8 := newTestInstance("i8").SetShardSetID(4).SetWeight(3)
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

	p1, err := a.AddInstances(p, []placement.Instance{i5, i6, i7, i8})
	assert.NoError(t, err)
	assert.Equal(t, uint32(4), p1.MaxShardSetID())
	assert.NoError(t, placement.Validate(p1))
	assert.True(t, globalChecker.allInitializing(p1, []string{"i5", "i6", "i7", "i8"}, nowNanos))
	i5, ok := p1.Instance("i5")
	assert.True(t, ok)
	i6, ok = p1.Instance("i6")
	assert.True(t, ok)
	assertInstancesArePeers(t, i5, i6)
	i7, ok = p1.Instance("i7")
	assert.True(t, ok)
	i8, ok = p1.Instance("i8")
	assert.True(t, ok)
	assertInstancesArePeers(t, i7, i8)

	// Removing all initializing nodes will trigger the revert path.
	p2, err := a.RemoveInstances(p1.Clone(), []string{"i5", "i6", "i7", "i8"})
	assert.NoError(t, err)
	assert.Equal(t, uint32(4), p2.MaxShardSetID())
	assert.NoError(t, placement.Validate(p2))
	_, ok = p2.Instance("i5")
	assert.False(t, ok)
	_, ok = p2.Instance("i7")
	assert.False(t, ok)

	assert.NoError(t, err)
	assert.Equal(t, p.SetMaxShardSetID(4), p2)

	// Removing part of the initializing nodes will not trigger the revert path
	// and will only do a normal revert.
	p3, err := a.RemoveInstances(p1.Clone(), []string{"i7", "i8"})
	assert.NoError(t, err)
	assert.Equal(t, uint32(4), p3.MaxShardSetID())
	assert.NoError(t, placement.Validate(p3))
	_, ok = p3.Instance("i5")
	assert.True(t, ok)
	newI7, ok := p3.Instance("i7")
	assert.True(t, ok)
	assert.True(t, newI7.IsLeaving())

	assert.NoError(t, err)
	assert.Equal(t, p.SetMaxShardSetID(4), p2)
}

func TestMirrorAddMultiplePairsAndPartialRevert(t *testing.T) {
	i1 := newTestInstance("i1").SetShardSetID(1).SetWeight(1)
	i2 := newTestInstance("i2").SetShardSetID(1).SetWeight(1)
	i3 := newTestInstance("i3").SetShardSetID(2).SetWeight(2)
	i4 := newTestInstance("i4").SetShardSetID(2).SetWeight(2)
	i5 := newTestInstance("i5").SetShardSetID(3).SetWeight(3)
	i6 := newTestInstance("i6").SetShardSetID(3).SetWeight(3)
	i7 := newTestInstance("i7").SetShardSetID(4).SetWeight(3)
	i8 := newTestInstance("i8").SetShardSetID(4).SetWeight(3)
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
	require.NoError(t, err)
	assert.Equal(t, uint32(2), p.MaxShardSetID())
	assert.NoError(t, placement.Validate(p))

	p, _, err = a.MarkAllShardsAvailable(p)
	require.NoError(t, err)

	p1, err := a.AddInstances(p, []placement.Instance{i5, i6, i7, i8})
	require.NoError(t, err)
	assert.Equal(t, uint32(4), p1.MaxShardSetID())
	require.NoError(t, placement.Validate(p1))
	assert.True(t, globalChecker.allInitializing(p1, []string{"i5", "i6", "i7", "i8"}, nowNanos))
	i5, ok := p1.Instance("i5")
	assert.True(t, ok)
	i6, ok = p1.Instance("i6")
	assert.True(t, ok)
	assertInstancesArePeers(t, i5, i6)
	i7, ok = p1.Instance("i7")
	assert.True(t, ok)
	i8, ok = p1.Instance("i8")
	assert.True(t, ok)
	assertInstancesArePeers(t, i7, i8)

	// Removing instances that are not peers and are pending add must fail.
	_, err = a.RemoveInstances(p1.Clone(), []string{"i5", "i7"})
	assert.Error(t, err)
}

func TestMirrorAddAndRevertAfterCutover(t *testing.T) {
	i1 := newTestInstance("i1").SetShardSetID(1).SetWeight(1)
	i2 := newTestInstance("i2").SetShardSetID(1).SetWeight(1)
	i3 := newTestInstance("i3").SetShardSetID(2).SetWeight(2)
	i4 := newTestInstance("i4").SetShardSetID(2).SetWeight(2)
	i5 := newTestInstance("i5").SetShardSetID(3).SetWeight(3)
	i6 := newTestInstance("i6").SetShardSetID(3).SetWeight(3)
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
	assert.False(t, globalChecker.allInitializing(p1, []string{"i5", "i6"}, nowNanos))

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

func TestMirrorRemoveAndRevertBeforeCutover(t *testing.T) {
	i1 := newTestInstance("i1").SetShardSetID(1).SetWeight(1)
	i2 := newTestInstance("i2").SetShardSetID(1).SetWeight(1)
	i3 := newTestInstance("i3").SetShardSetID(2).SetWeight(2)
	i4 := newTestInstance("i4").SetShardSetID(2).SetWeight(2)
	i5 := newTestInstance("i5").SetShardSetID(3).SetWeight(3)
	i6 := newTestInstance("i6").SetShardSetID(3).SetWeight(3)
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
	assert.True(t, globalChecker.allLeaving(p1, []placement.Instance{i5, i6}, nowNanos))

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

func TestMirrorRemoveAndRevertAfterCutover(t *testing.T) {
	i1 := newTestInstance("i1").SetShardSetID(1).SetWeight(1)
	i2 := newTestInstance("i2").SetShardSetID(1).SetWeight(1)
	i3 := newTestInstance("i3").SetShardSetID(2).SetWeight(2)
	i4 := newTestInstance("i4").SetShardSetID(2).SetWeight(2)
	i5 := newTestInstance("i5").SetShardSetID(3).SetWeight(3)
	i6 := newTestInstance("i6").SetShardSetID(3).SetWeight(3)
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
	assert.False(t, globalChecker.allLeaving(p1, []placement.Instance{i5, i6}, nowNanos))

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

func TestMirrorReplaceAndRevertBeforeCutover(t *testing.T) {
	i1 := newTestInstance("i1").SetShardSetID(1).SetWeight(1)
	i2 := newTestInstance("i2").SetShardSetID(1).SetWeight(1)
	i3 := newTestInstance("i3").SetShardSetID(2).SetWeight(2)
	i4 := newTestInstance("i4").SetShardSetID(2).SetWeight(2)
	i5 := newTestInstance("i5").SetShardSetID(2).SetWeight(2)
	instances := []placement.Instance{i1, i2, i3, i4}

	numShards := 12
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
	assert.True(t, globalChecker.allLeaving(p1, []placement.Instance{i4}, nowNanos))
	assert.True(t, localChecker.allInitializing(p1, []string{"i5"}, nowNanos))

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

func TestMirrorReplaceAndRevertAfterCutover(t *testing.T) {
	i1 := newTestInstance("i1").SetShardSetID(1).SetWeight(1)
	i2 := newTestInstance("i2").SetShardSetID(1).SetWeight(1)
	i3 := newTestInstance("i3").SetShardSetID(2).SetWeight(2)
	i4 := newTestInstance("i4").SetShardSetID(2).SetWeight(2)
	i5 := newTestInstance("i5").SetShardSetID(2).SetWeight(2)
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
	assert.False(t, globalChecker.allLeaving(p1, []placement.Instance{i4}, nowNanos))
	assert.False(t, globalChecker.allInitializing(p1, []string{"i5"}, nowNanos))

	i4, ok := p1.Instance("i4")
	assert.True(t, ok)
	assert.True(t, i4.IsLeaving())
	ssI4 := i4.Shards()
	i5, ok = p1.Instance("i5")
	assert.True(t, ok)
	assert.True(t, i5.IsInitializing())
	ssI5 := i5.Shards()

	p2, err := a.ReplaceInstances(p1, []string{"i5"}, []placement.Instance{i4})
	assert.NoError(t, err)
	assert.Equal(t, uint32(2), p2.MaxShardSetID())
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

func TestMirrorMultipleNonOverlappingReplaces(t *testing.T) {
	i1 := newTestInstance("i1").SetShardSetID(1)
	i2 := newTestInstance("i2").SetShardSetID(1)
	i3 := newTestInstance("i3").SetShardSetID(2)
	i4 := newTestInstance("i4").SetShardSetID(2)
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

	p, _, err = a.MarkAllShardsAvailable(p)
	assert.NoError(t, err)

	// First replace: i1 replaced by i5.
	i5 := newTestInstance("i5").SetShardSetID(1)
	p1, err := a.ReplaceInstances(p, []string{"i1"}, []placement.Instance{i5})
	require.NoError(t, err)
	require.NoError(t, placement.Validate(p1))
	assert.Equal(t, uint32(2), p1.MaxShardSetID())
	assert.True(t, globalChecker.allLeaving(p1, []placement.Instance{i1}, nowNanos))
	assert.True(t, globalChecker.allInitializing(p1, []string{"i5"}, nowNanos))

	// Second replace that does not overlap with first replace: i3 replaced by i6.
	i6 := newTestInstance("i6").SetShardSetID(2)
	p2, err := a.ReplaceInstances(p1, []string{"i3"}, []placement.Instance{i6})
	require.NoError(t, err)
	require.NoError(t, placement.Validate(p2))

	assert.Equal(t, uint32(2), p2.MaxShardSetID())
	assert.True(t, globalChecker.allLeaving(p2, []placement.Instance{i3, i1}, nowNanos))
	assert.True(t, globalChecker.allInitializing(p2, []string{"i6", "i5"}, nowNanos))
}

func TestMirrorReplacesCannotOverlap(t *testing.T) {
	i1 := newTestInstance("i1").SetShardSetID(1)
	i2 := newTestInstance("i2").SetShardSetID(1)
	i3 := newTestInstance("i3").SetShardSetID(2)
	i4 := newTestInstance("i4").SetShardSetID(2)
	instances := []placement.Instance{i1, i2, i3, i4}

	numShards := 100
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	now := time.Now()
	nowNanos := now.UnixNano()
	shardCutoverTime := now.Add(2 * time.Hour).UnixNano()
	a := NewAlgorithm(placement.NewOptions().
		SetIsMirrored(true).
		SetShardCutoverNanosFn(func() int64 { return shardCutoverTime }).
		SetShardCutoffNanosFn(func() int64 { return shardCutoverTime }))
	p, err := a.InitialPlacement(instances, ids, 2)
	require.NoError(t, err)
	require.NoError(t, placement.Validate(p))
	assert.Equal(t, uint32(2), p.MaxShardSetID())

	p, _, err = a.MarkAllShardsAvailable(p)
	require.NoError(t, err)

	a = NewAlgorithm(placement.NewOptions().
		SetIsMirrored(true).
		SetShardCutoverNanosFn(func() int64 { return shardCutoverTime }).
		SetShardCutoffNanosFn(func() int64 { return shardCutoverTime }).
		SetIsShardCutoffFn(func(shard.Shard) error { return errors.New("Not cutoff") }).
		SetIsShardCutoverFn(func(shard.Shard) error { return errors.New("Not cutover") }))

	// First replace: i1 replaced by i5.
	i5 := newTestInstance("i5").SetShardSetID(1)
	p1, err := a.ReplaceInstances(p, []string{"i1"}, []placement.Instance{i5})
	require.NoError(t, err)
	require.NoError(t, placement.Validate(p1))
	assert.Equal(t, uint32(2), p1.MaxShardSetID())
	assert.True(t, globalChecker.allLeaving(p1, []placement.Instance{i1}, nowNanos))
	assert.True(t, globalChecker.allInitializing(p1, []string{"i5"}, nowNanos))

	// Second replace: i5 replaced by i6, is overlapping with first replace
	// because i5 has initializing shards that are not cutover.
	i6 := newTestInstance("i6").SetShardSetID(1)
	p2, err := a.ReplaceInstances(p1, []string{"i5"}, []placement.Instance{i6})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Not cutover")
	assert.Nil(t, p2)

	assert.Equal(t, uint32(2), p1.MaxShardSetID())
	assert.True(t, globalChecker.allLeaving(p1, []placement.Instance{i1}, nowNanos))
	assert.True(t, globalChecker.allInitializing(p1, []string{"i5"}, nowNanos))

	// Third replace: i1 replaced by i6, is overlapping with first replace
	// because i1 has a pending replacement peer i5 with initializing shards that are not cutover.
	p2, err = a.ReplaceInstances(p1, []string{"i1"}, []placement.Instance{i6})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Not cutover")
	assert.Nil(t, p2)

	assert.Equal(t, uint32(2), p1.MaxShardSetID())
	assert.True(t, globalChecker.allLeaving(p1, []placement.Instance{i1}, nowNanos))
	assert.True(t, globalChecker.allInitializing(p1, []string{"i5"}, nowNanos))

	// Fourth replace: i3 replaced by i6, is not overlapping with first replace and must go through.
	i6.SetShardSetID(i3.ShardSetID())
	p2, err = a.ReplaceInstances(p1, []string{"i3"}, []placement.Instance{i6})
	require.NoError(t, err)
	require.NoError(t, placement.Validate(p2))
	assert.Equal(t, uint32(2), p2.MaxShardSetID())
	assert.True(t, globalChecker.allLeaving(p2, []placement.Instance{i1, i3}, nowNanos))
	assert.True(t, globalChecker.allInitializing(p2, []string{"i5", "i6"}, nowNanos))
}

func TestMirrorRevertOfReplaceMustMatch(t *testing.T) {
	i1 := newTestInstance("i1").SetShardSetID(1)
	i2 := newTestInstance("i2").SetShardSetID(1)
	i3 := newTestInstance("i3").SetShardSetID(2)
	i4 := newTestInstance("i4").SetShardSetID(2)
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
	require.NoError(t, err)
	require.NoError(t, placement.Validate(p))
	assert.Equal(t, uint32(2), p.MaxShardSetID())

	p, _, err = a.MarkAllShardsAvailable(p)
	assert.NoError(t, err)

	// First replace: i1 replaced by i1a.
	i1a := newTestInstance("i1a").
		SetShardSetID(i1.ShardSetID()).
		SetWeight(i1.Weight())
	p1, err := a.ReplaceInstances(p, []string{"i1"}, []placement.Instance{i1a})
	require.NoError(t, err)
	require.NoError(t, placement.Validate(p1))
	assert.Equal(t, uint32(2), p1.MaxShardSetID())
	assert.True(t, globalChecker.allLeaving(p1, []placement.Instance{i1}, nowNanos))
	assert.True(t, globalChecker.allInitializing(p1, []string{"i1a"}, nowNanos))

	// Second replace: i3 replaced by i3a.
	i3a := newTestInstance("i3a").
		SetShardSetID(i3.ShardSetID()).
		SetWeight(i3.Weight())
	p2, err := a.ReplaceInstances(p1, []string{"i3"}, []placement.Instance{i3a})
	require.NoError(t, err)
	require.NoError(t, placement.Validate(p2))
	assert.Equal(t, uint32(2), p2.MaxShardSetID())
	assert.True(t, globalChecker.allLeaving(p2, []placement.Instance{i1, i3}, nowNanos))
	assert.True(t, globalChecker.allInitializing(p2, []string{"i1a", "i3a"}, nowNanos))

	t.Run("revert_of_non_matching_replace_must_fail", func(t *testing.T) {
		newI1, ok := p2.Instance("i1")
		assert.True(t, ok)
		_, err = a.ReplaceInstances(p2, []string{"i3a"}, []placement.Instance{newI1})
		assert.Error(t, err)

		newI3, ok := p2.Instance("i3")
		assert.True(t, ok)
		_, err = a.ReplaceInstances(p2, []string{"i1a"}, []placement.Instance{newI3})
		assert.Error(t, err)
	})

	t.Run("revert_of_matching_replace_must_succeed", func(t *testing.T) {
		newI3, ok := p2.Instance("i3")
		assert.True(t, ok)
		p3, err := a.ReplaceInstances(p2, []string{"i3a"}, []placement.Instance{newI3})
		require.NoError(t, err)
		_, ok = p3.Instance("i3a")
		assert.False(t, ok)
		_, ok = p3.Instance("i3")
		assert.True(t, ok)
		assert.True(t, localChecker.allAvailable(p3, []string{"i3"}, nowNanos))

		newI1, ok := p2.Instance("i1")
		assert.True(t, ok)
		p4, err := a.ReplaceInstances(p3, []string{"i1a"}, []placement.Instance{newI1})
		require.NoError(t, err)
		_, ok = p4.Instance("i1a")
		assert.False(t, ok)
		_, ok = p4.Instance("i1")
		assert.True(t, ok)
		assert.True(t, localChecker.allAvailable(p4, []string{"i1"}, nowNanos))
	})
}

func TestMirrorReplaceDuringPendingAddMustFail(t *testing.T) {
	i1 := newTestInstance("i1").SetShardSetID(1)
	i2 := newTestInstance("i2").SetShardSetID(1)
	i3 := newTestInstance("i3").SetShardSetID(2)
	i4 := newTestInstance("i4").SetShardSetID(2)
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
	require.NoError(t, err)
	require.NoError(t, placement.Validate(p))
	assert.Equal(t, uint32(2), p.MaxShardSetID())

	p, _, err = a.MarkAllShardsAvailable(p)
	require.NoError(t, err)

	a = NewAlgorithm(placement.NewOptions().
		SetIsMirrored(true).
		SetShardCutoverNanosFn(func() int64 { return shardCutoverTime }).
		SetShardCutoffNanosFn(func() int64 { return shardCutoverTime }).
		SetIsShardCutoffFn(func(shard.Shard) error { return errors.New("Not cutoff") }).
		SetIsShardCutoverFn(func(shard.Shard) error { return errors.New("Not cutover") }))

	// Add a new pair
	i5 := newTestInstance("i5").SetShardSetID(3)
	i6 := newTestInstance("i6").SetShardSetID(3)
	p1, err := a.AddInstances(p, []placement.Instance{i5, i6})
	require.NoError(t, err)
	require.NoError(t, placement.Validate(p1))

	i1, ok := p1.Instance("i1")
	assert.True(t, ok)
	assert.True(t, i1.Shards().NumShardsForState(shard.Leaving) > 0)
	assert.True(t, i1.Shards().NumShardsForState(shard.Available) > i1.Shards().NumShardsForState(shard.Leaving))

	assert.Equal(t, uint32(3), p1.MaxShardSetID())
	assert.True(t, globalChecker.allInitializing(p1, []string{"i5", "i6"}, nowNanos))
	i5, ok = p1.Instance("i5")
	assert.True(t, ok)
	i6, ok = p1.Instance("i6")
	assert.True(t, ok)
	assertInstancesArePeers(t, i5, i6)

	// Replace of instance that is pending add must fail.
	i5a := newTestInstance("i5a").
		SetShardSetID(i5.ShardSetID()).
		SetWeight(i5.Weight())
	_, err = a.ReplaceInstances(p1, []string{"i5"}, []placement.Instance{i5a})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Not cutover")

	// Replace of instance that has some shards available
	// and some shards leaving to newly added instance must fail.
	i1, ok = p1.Instance("i1")
	assert.True(t, ok)
	i1ShardsAvailableBeforeReplace := i1.Shards().NumShardsForState(shard.Available)
	assert.True(t, i1ShardsAvailableBeforeReplace > 0)

	i1a := newTestInstance("i1a").
		SetShardSetID(i1.ShardSetID()).
		SetWeight(i1.Weight())
	_, err = a.ReplaceInstances(p1, []string{"i1"}, []placement.Instance{i1a})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "replaced instances must have all their shards available")
}

func TestMirrorReplaceDuringPendingRemoveMustFail(t *testing.T) {
	i1 := newTestInstance("i1").SetShardSetID(1)
	i2 := newTestInstance("i2").SetShardSetID(1)
	i3 := newTestInstance("i3").SetShardSetID(2)
	i4 := newTestInstance("i4").SetShardSetID(2)
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
	require.NoError(t, err)
	require.NoError(t, placement.Validate(p))
	assert.Equal(t, uint32(2), p.MaxShardSetID())

	p, _, err = a.MarkAllShardsAvailable(p)
	assert.NoError(t, err)

	a = NewAlgorithm(placement.NewOptions().
		SetIsMirrored(true).
		SetShardCutoverNanosFn(func() int64 { return shardCutoverTime }).
		SetShardCutoffNanosFn(func() int64 { return shardCutoverTime }).
		SetIsShardCutoffFn(func(shard.Shard) error { return errors.New("Not cutoff") }).
		SetIsShardCutoverFn(func(shard.Shard) error { return errors.New("Not cutover") }))

	p1, err := a.RemoveInstances(p, []string{"i3", "i4"})
	require.NoError(t, err)
	require.NoError(t, placement.Validate(p1))
	i3, ok := p1.Instance("i3")
	assert.True(t, ok)
	i4, ok = p1.Instance("i4")
	assert.True(t, ok)
	assert.True(t, globalChecker.allLeaving(p1, []placement.Instance{i3, i4}, nowNanos))
	i1, ok = p1.Instance("i1")
	assert.True(t, ok)
	assert.True(t, i1.Shards().NumShardsForState(shard.Initializing) > 0)
	assert.Equal(t, i1.Shards().NumShardsForState(shard.Available), i1.Shards().NumShardsForState(shard.Initializing))

	// Replace of instance that is pending remove must fail.
	i3a := newTestInstance("i3a").
		SetShardSetID(i3.ShardSetID()).
		SetWeight(i3.Weight())
	_, err = a.ReplaceInstances(p1, []string{"i3"}, []placement.Instance{i3a})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "replaced instances must have all their shards available")

	// Replace of instance that has some shards available
	// and some shards initializing from another isntance pending remove must fail.
	i1, ok = p1.Instance("i1")
	assert.True(t, ok)
	i1ShardsAvailableBeforeReplace := i1.Shards().NumShardsForState(shard.Available)
	assert.True(t, i1ShardsAvailableBeforeReplace > 0)

	i1a := newTestInstance("i1a").
		SetShardSetID(i1.ShardSetID()).
		SetWeight(i1.Weight())
	_, err = a.ReplaceInstances(p1, []string{"i1"}, []placement.Instance{i1a})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Not cutover")
}

func TestMirrorInitError(t *testing.T) {
	i1 := newTestInstance("i1").SetShardSetID(1).SetWeight(1)
	i2 := newTestInstance("i2").SetShardSetID(1).SetWeight(1)
	i3 := newTestInstance("i3").SetShardSetID(2).SetWeight(2)
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
	i1 := newTestInstance("i1").SetShardSetID(1).SetWeight(1)
	i2 := newTestInstance("i2").SetShardSetID(1).SetWeight(1)
	i3 := newTestInstance("i3").SetShardSetID(2).SetWeight(2)
	i4 := newTestInstance("i4").SetShardSetID(2).SetWeight(2)
	i5 := newTestInstance("i5").SetShardSetID(3).SetWeight(3)
	i6 := newTestInstance("i6").SetShardSetID(3).SetWeight(3)
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
		newTestInstance("i3").SetShardSetID(2).SetWeight(2),
		newTestInstance("i4").SetShardSetID(2).SetWeight(2),
	})
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
	assert.Equal(t, uint32(2), p.MaxShardSetID())

	// Duplicated shardset id.
	_, err = a.AddInstances(p, []placement.Instance{
		newTestInstance("i7").SetShardSetID(1).SetWeight(3),
		newTestInstance("i7").SetShardSetID(1).SetWeight(3),
	})
	assert.Error(t, err)
	assert.Equal(t, uint32(2), p.MaxShardSetID())
}

func TestMirrorRemoveInstancesError(t *testing.T) {
	i1 := newTestInstance("i1").SetShardSetID(1).SetWeight(1)
	i2 := newTestInstance("i2").SetShardSetID(1).SetWeight(1)
	i3 := newTestInstance("i3").SetShardSetID(2).SetWeight(2)
	i4 := newTestInstance("i4").SetShardSetID(2).SetWeight(2)
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
	i1 := newTestInstance("i1").SetShardSetID(1).SetWeight(1)
	i2 := newTestInstance("i2").SetShardSetID(1).SetWeight(1)
	i3 := newTestInstance("i3").SetShardSetID(2).SetWeight(2)
	i4 := newTestInstance("i4").SetShardSetID(2).SetWeight(2)
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
		newTestInstance("i11").SetShardSetID(0).SetWeight(1),
	})
	assert.Error(t, err)
	assert.Equal(t, uint32(2), p.MaxShardSetID())

	_, err = a.ReplaceInstances(p, []string{"i1"}, []placement.Instance{
		newTestInstance("i11").SetShardSetID(0).SetWeight(1),
		newTestInstance("i12").SetShardSetID(0).SetWeight(1),
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
	i1 := newTestInstance("i1").
		SetShardSetID(1).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Leaving),
			shard.NewShard(1).SetState(shard.Available),
		}))
	i2 := newTestInstance("i2").
		SetShardSetID(1).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Leaving),
			shard.NewShard(1).SetState(shard.Available),
		}))
	i3 := newTestInstance("i3").
		SetShardSetID(2).
		SetWeight(2).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i1"),
			shard.NewShard(2).SetState(shard.Available),
		}))
	i4 := newTestInstance("i4").
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

	replaceI1 := newTestInstance("newI1").SetShardSetID(1).SetWeight(1)
	replaceI4 := newTestInstance("newI4").SetShardSetID(2).SetWeight(2)
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
	i1 := newTestInstance("i1").SetShardSetID(1).SetWeight(1)
	i2 := newTestInstance("i2").SetShardSetID(1).SetWeight(1)
	i3 := newTestInstance("i3").SetShardSetID(2).SetWeight(2)
	i4 := newTestInstance("i4").SetShardSetID(2).SetWeight(2)
	i5 := newTestInstance("i5").SetShardSetID(3).SetWeight(3)
	i6 := newTestInstance("i6").SetShardSetID(3).SetWeight(3)
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

	i7 := newTestInstance("i7").SetShardSetID(4).SetWeight(4)
	i8 := newTestInstance("i8").SetShardSetID(4).SetWeight(4)
	p, err = a.AddInstances(p, []placement.Instance{i7, i8})
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
	verifyAllShardsInAvailableState(t, p)

	p, err = a.RemoveInstances(p, []string{i7.ID(), i8.ID()})
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
	verifyAllShardsInAvailableState(t, p)

	p, err = a.ReplaceInstances(p, []string{"i6"}, []placement.Instance{
		newTestInstance("i16").SetShardSetID(3).SetWeight(3),
	})
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
	verifyAllShardsInAvailableState(t, p)

	i9 := newTestInstance("i9").SetShardSetID(5).SetWeight(1)
	i10 := newTestInstance("i10").SetShardSetID(5).SetWeight(1)
	p, err = a.AddInstances(p, []placement.Instance{i9, i10})
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
	verifyAllShardsInAvailableState(t, p)

	p, err = a.ReplaceInstances(p, []string{"i9"}, []placement.Instance{
		newTestInstance("i19").SetShardSetID(5).SetWeight(1),
	})
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
	verifyAllShardsInAvailableState(t, p)

	p, err = a.RemoveInstances(p, []string{"i19", "i10"})
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
	verifyAllShardsInAvailableState(t, p)

	p, err = a.BalanceShards(p)
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
	verifyAllShardsInAvailableState(t, p)
}

func TestMarkInstanceAndItsPeersAvailable(t *testing.T) {
	i1 := newTestInstance("i1").SetShardSetID(1)
	i2 := newTestInstance("i2").SetShardSetID(1)
	i3 := newTestInstance("i3").SetShardSetID(2)
	i4 := newTestInstance("i4").SetShardSetID(2)
	instances := []placement.Instance{i1, i2, i3, i4}

	numShards := 12
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
	require.NoError(t, err)
	require.NoError(t, placement.Validate(p))
	require.Equal(t, uint32(2), p.MaxShardSetID())
	require.True(t, globalChecker.allInitializing(p, []string{"i1", "i2", "i3", "i4"}, nowNanos))

	mirroredAlgo, ok := a.(mirroredAlgorithm)
	require.True(t, ok)

	t.Run("empty_input", func(t *testing.T) {
		_, err := mirroredAlgo.markInstanceAndItsPeersAvailable(p, "")
		require.Error(t, err)
	})

	t.Run("invalid_instance", func(t *testing.T) {
		_, err := mirroredAlgo.markInstanceAndItsPeersAvailable(p, "non-existent")
		require.Error(t, err)
	})

	t.Run("noop_when_shards_already_available", func(t *testing.T) {
		p1, _, err := mirroredAlgo.MarkAllShardsAvailable(p)
		require.NoError(t, err)
		require.True(t, globalChecker.allAvailable(p1, []string{"i1", "i2", "i3", "i4"}, nowNanos))

		for i := 1; i < 3; i++ {
			p2, err := mirroredAlgo.markInstanceAndItsPeersAvailable(p1, "i1")
			require.NoError(t, err)
			require.Equal(t, p1, p2)
		}
	})

	t.Run("mark_first_pair", func(t *testing.T) {
		p1, err := mirroredAlgo.markInstanceAndItsPeersAvailable(p.Clone(), "i1")
		require.NoError(t, err)
		require.True(t, globalChecker.allAvailable(p1, []string{"i1", "i2"}, nowNanos))
		require.True(t, globalChecker.allInitializing(p1, []string{"i3", "i4"}, nowNanos))

		// Shouldn't matter which of the peers is marked.
		p2, err := mirroredAlgo.markInstanceAndItsPeersAvailable(p.Clone(), "i2")
		require.NoError(t, err)
		require.Equal(t, p1, p2)
	})

	t.Run("mark_second_pair", func(t *testing.T) {
		p1, err := mirroredAlgo.markInstanceAndItsPeersAvailable(p.Clone(), "i4")
		require.NoError(t, err)
		require.True(t, globalChecker.allAvailable(p1, []string{"i3", "i4"}, nowNanos))
		require.True(t, globalChecker.allInitializing(p1, []string{"i1", "i2"}, nowNanos))

		// Shouldn't matter which of the peers is marked.
		p2, err := mirroredAlgo.markInstanceAndItsPeersAvailable(p.Clone(), "i3")
		require.NoError(t, err)
		require.Equal(t, p1, p2)
	})

	t.Run("mark_added_pair", func(t *testing.T) {
		i5 := newTestInstance("i5").SetShardSetID(3)
		i6 := newTestInstance("i6").SetShardSetID(3)
		p1, err := mirroredAlgo.AddInstances(p, []placement.Instance{i5, i6})
		require.NoError(t, err)
		require.True(t, globalChecker.allInitializing(p1, []string{"i5", "i6"}, nowNanos))
		newI1, exists := p1.Instance("i1")
		require.True(t, exists)
		require.True(t, newI1.Shards().NumShardsForState(shard.Leaving) > 0)
		require.True(t, newI1.Shards().NumShardsForState(shard.Available) > newI1.Shards().NumShardsForState(shard.Leaving))
		newI2, exists := p1.Instance("i2")
		require.True(t, exists)
		require.Equal(t, newI1.Shards().NumShardsForState(shard.Leaving), newI2.Shards().NumShardsForState(shard.Leaving))
		require.Equal(t, newI1.Shards().NumShardsForState(shard.Available), newI2.Shards().NumShardsForState(shard.Available))
		newI3, exists := p1.Instance("i3")
		require.True(t, exists)
		require.True(t, newI3.Shards().NumShardsForState(shard.Leaving) > 0)
		require.True(t, newI3.Shards().NumShardsForState(shard.Available) > newI3.Shards().NumShardsForState(shard.Leaving))
		newI4, exists := p1.Instance("i4")
		require.True(t, exists)
		require.Equal(t, newI3.Shards().NumShardsForState(shard.Leaving), newI4.Shards().NumShardsForState(shard.Leaving))
		require.Equal(t, newI3.Shards().NumShardsForState(shard.Available), newI4.Shards().NumShardsForState(shard.Available))

		p2, err := mirroredAlgo.markInstanceAndItsPeersAvailable(p1, "i5")
		require.NoError(t, err)
		require.True(t, globalChecker.allAvailable(p2, []string{"i1", "i2", "i3", "i4", "i5", "i6"}, nowNanos))
	})
}

func TestBalanceShardsForMirroredWhenBalanced(t *testing.T) {
	i1 := newTestInstance("i1").
		SetShardSetID(1).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Available),
		}))
	i2 := newTestInstance("i2").
		SetShardSetID(1).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Available),
		}))
	i3 := newTestInstance("i3").
		SetShardSetID(2).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(1).SetState(shard.Available),
		}))
	i4 := newTestInstance("i4").
		SetShardSetID(2).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(1).SetState(shard.Available),
		}))
	initialPlacement := placement.NewPlacement().
		SetReplicaFactor(2).
		SetShards([]uint32{0, 1}).
		SetInstances([]placement.Instance{i1, i2, i3, i4}).
		SetIsMirrored(true).
		SetIsSharded(true).
		SetMaxShardSetID(2)

	expectedPlacement := initialPlacement.Clone()

	a := NewAlgorithm(placement.NewOptions().SetIsMirrored(true))

	balancedPlacement, err := a.BalanceShards(initialPlacement)
	assert.NoError(t, err)
	assert.Equal(t, expectedPlacement, balancedPlacement)
}

func TestBalanceShardsForMirroredWhenImbalanced(t *testing.T) {
	i1 := newTestInstance("i1").
		SetShardSetID(1).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Available),
			shard.NewShard(1).SetState(shard.Available),
		}))
	i2 := newTestInstance("i2").
		SetShardSetID(1).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Available),
			shard.NewShard(1).SetState(shard.Available),
		}))
	i3 := newTestInstance("i3").
		SetShardSetID(2).
		SetWeight(2).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(2).SetState(shard.Available),
		}))
	i4 := newTestInstance("i4").
		SetShardSetID(2).
		SetWeight(2).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(2).SetState(shard.Available),
		}))
	p := placement.NewPlacement().
		SetReplicaFactor(2).
		SetShards([]uint32{0, 1, 2, 3}).
		SetInstances([]placement.Instance{i1, i2, i3, i4}).
		SetIsMirrored(true).
		SetIsSharded(true).
		SetMaxShardSetID(2)

	a := NewAlgorithm(placement.NewOptions().SetIsMirrored(true))

	balancedPlacement, err := a.BalanceShards(p)
	assert.NoError(t, err)

	bi1 := newTestInstance("i1").
		SetShardSetID(1).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Leaving),
			shard.NewShard(1).SetState(shard.Available),
		}))
	bi2 := newTestInstance("i2").
		SetShardSetID(1).
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Leaving),
			shard.NewShard(1).SetState(shard.Available),
		}))
	bi3 := newTestInstance("i3").
		SetShardSetID(2).
		SetWeight(2).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i1"),
			shard.NewShard(2).SetState(shard.Available),
		}))
	bi4 := newTestInstance("i4").
		SetShardSetID(2).
		SetWeight(2).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i2"),
			shard.NewShard(2).SetState(shard.Available),
		}))
	expectedInstances := []placement.Instance{bi1, bi2, bi3, bi4}

	assert.Equal(t, expectedInstances, balancedPlacement.Instances())
}

func newTestInstance(id string) placement.Instance {
	return placement.NewInstance().
		SetID(id).
		SetIsolationGroup("rack-" + id).
		SetEndpoint("endpoint-" + id).
		SetMetadata(placement.InstanceMetadata{DebugPort: 80}).
		SetWeight(1)
}

func assertInstancesArePeers(t *testing.T, i1, i2 placement.Instance) {
	assert.Equal(t, i1.Shards().AllIDs(), i1.Shards().AllIDs())
	assert.Equal(t, i2.ShardSetID(), i2.ShardSetID())
}
