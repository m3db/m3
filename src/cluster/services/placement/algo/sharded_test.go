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
	"math"
	"testing"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
	"github.com/m3db/m3cluster/shard"
	"github.com/stretchr/testify/assert"
)

func TestGoodCase(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i2 := placement.NewEmptyInstance("i2", "r1", "z1", "endpoint", 1)
	i3 := placement.NewEmptyInstance("i3", "r2", "z1", "endpoint", 1)
	i4 := placement.NewEmptyInstance("i4", "r2", "z1", "endpoint", 1)
	i5 := placement.NewEmptyInstance("i5", "r3", "z1", "endpoint", 1)
	i6 := placement.NewEmptyInstance("i6", "r4", "z1", "endpoint", 1)
	i7 := placement.NewEmptyInstance("i7", "r5", "z1", "endpoint", 1)
	i8 := placement.NewEmptyInstance("i8", "r6", "z1", "endpoint", 1)
	i9 := placement.NewEmptyInstance("i9", "r7", "z1", "endpoint", 1)

	instances := []services.PlacementInstance{i1, i2, i3, i4, i5, i6, i7, i8, i9}

	numShards := 1024
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids)
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "TestGoodCase replica 1")

	p, err = a.AddInstance(p, placement.NewEmptyInstance("i21", "r6", "z1", "endpoint", 1))
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "TestGoodCase add 1")

	p, err = a.RemoveInstance(p, i1.ID())
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	_, exist := p.Instance(i1.ID())
	assert.False(t, exist)
	validateDistribution(t, p, 1.01, "TestGoodCase remove 1")

	i12 := placement.NewEmptyInstance("i12", "r3", "z1", "endpoint", 1)
	p, err = a.ReplaceInstance(p, i5.ID(), []services.PlacementInstance{i12})
	assert.NoError(t, err)
	_, exist = p.Instance(i5.ID())
	assert.True(t, exist)
	p = markAllShardsAsAvailable(t, p)
	_, exist = p.Instance(i5.ID())
	assert.False(t, exist)
	validateDistribution(t, p, 1.01, "TestGoodCase add 2")

	p, err = a.RemoveInstance(p, i2.ID())
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "TestGoodCase remove 1")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "TestGoodCase replica 2")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "TestGoodCase replica 3")

	i10 := placement.NewEmptyInstance("i10", "r4", "z1", "endpoint", 1)
	p, err = a.AddInstance(p, i10)
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "TestGoodCase add 3")

	i11 := placement.NewEmptyInstance("i11", "r7", "z1", "endpoint", 1)
	p, err = a.AddInstance(p, i11)
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "TestGoodCase add 4")

	i13 := placement.NewEmptyInstance("i13", "r5", "z1", "endpoint", 1)
	p, err = a.ReplaceInstance(p, i3.ID(), []services.PlacementInstance{i13})
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "TestGoodCase replace 1")

	p, err = a.RemoveInstance(p, i4.ID())
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.02, "TestGoodCase remove 2")
}

func TestGoodCaseWithWeight(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 10)
	i2 := placement.NewEmptyInstance("i2", "r1", "z1", "endpoint", 10)
	i3 := placement.NewEmptyInstance("i3", "r2", "z1", "endpoint", 20)
	i4 := placement.NewEmptyInstance("i4", "r3", "z1", "endpoint", 10)
	i5 := placement.NewEmptyInstance("i5", "r4", "z1", "endpoint", 30)
	i6 := placement.NewEmptyInstance("i6", "r6", "z1", "endpoint", 40)
	i7 := placement.NewEmptyInstance("i7", "r7", "z1", "endpoint", 10)
	i8 := placement.NewEmptyInstance("i8", "r8", "z1", "endpoint", 10)
	i9 := placement.NewEmptyInstance("i9", "r9", "z1", "endpoint", 10)

	instances := []services.PlacementInstance{i1, i2, i3, i4, i5, i6, i7, i8, i9}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids)
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "TestGoodCaseWithWeight replica 1")

	p, err = a.AddInstance(p, placement.NewEmptyInstance("h21", "r2", "z1", "endpoint", 10))
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "TestGoodCaseWithWeight add 1")

	p, err = a.RemoveInstance(p, i1.ID())
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "TestGoodCaseWithWeight remove 1")

	p, err = a.ReplaceInstance(p, i3.ID(),
		[]services.PlacementInstance{
			placement.NewEmptyInstance("h31", "r1", "z1", "endpoint", 10),
			placement.NewEmptyInstance("h32", "r1", "z1", "endpoint", 10),
		})
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "TestGoodCaseWithWeight replace 1")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "TestGoodCaseWithWeight replica 2")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "TestGoodCaseWithWeight replica 3")

	h10 := placement.NewEmptyInstance("h10", "r10", "z1", "endpoint", 10)
	p, err = a.AddInstance(p, h10)
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "TestGoodCaseWithWeight add 2")

	h11 := placement.NewEmptyInstance("h11", "r7", "z1", "endpoint", 10)
	p, err = a.AddInstance(p, h11)
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "TestGoodCaseWithWeight add 2")

	h13 := placement.NewEmptyInstance("h13", "r5", "z1", "endpoint", 10)

	p, err = a.ReplaceInstance(p, h11.ID(), []services.PlacementInstance{h13})
	assert.NoError(t, err)
	p = markAllShardsAsAvailable(t, p)
	validateDistribution(t, p, 1.01, "TestGoodCaseWithWeight replace 1")
}

func TestPlacementChangeWithoutStateUpdate(t *testing.T) {
	// similar to test case 1 but not marking any shard as Available after placement change
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i2 := placement.NewEmptyInstance("i2", "r1", "z1", "endpoint", 1)
	i3 := placement.NewEmptyInstance("i3", "r2", "z1", "endpoint", 1)
	i4 := placement.NewEmptyInstance("i4", "r2", "z1", "endpoint", 1)
	i5 := placement.NewEmptyInstance("i5", "r3", "z1", "endpoint", 1)
	i6 := placement.NewEmptyInstance("i6", "r4", "z1", "endpoint", 1)
	i7 := placement.NewEmptyInstance("i7", "r5", "z1", "endpoint", 1)
	i8 := placement.NewEmptyInstance("i8", "r6", "z1", "endpoint", 1)
	i9 := placement.NewEmptyInstance("i9", "r7", "z1", "endpoint", 1)

	instances := []services.PlacementInstance{i1, i2, i3, i4, i5, i6, i7, i8, i9}

	numShards := 100
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids)
	assert.NoError(t, err)
	assert.Equal(t, 1, p.ReplicaFactor())
	assert.Equal(t, numShards, p.NumShards())
	for _, instance := range p.Instances() {
		for _, s := range instance.Shards().All() {
			assert.Equal(t, shard.Initializing, s.State())
			assert.Equal(t, "", s.SourceID())
		}
	}
	validateDistribution(t, p, 1.01, "TestPlacementChangeWithoutStateUpdate replica 1")

	p, err = a.AddInstance(p, placement.NewEmptyInstance("i21", "r6", "z1", "endpoint", 1))
	assert.NoError(t, err)
	instance, exist := p.Instance("i21")
	assert.True(t, exist)
	for _, s := range instance.Shards().All() {
		assert.Equal(t, shard.Initializing, s.State())
		// there should be no source shard because non of the Initializing shards has been marked as available yet
		assert.Equal(t, "", s.SourceID())
	}
	validateDistribution(t, p, 1.01, "TestPlacementChangeWithoutStateUpdate add 1")

	p, err = a.RemoveInstance(p, i1.ID())
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestPlacementChangeWithoutStateUpdate remove 1")

	i12 := placement.NewEmptyInstance("i12", "r3", "z1", "endpoint", 1)
	p, err = a.ReplaceInstance(p, i5.ID(), []services.PlacementInstance{i12})
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestPlacementChangeWithoutStateUpdate add 1")

	p, err = a.RemoveInstance(p, i2.ID())
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestPlacementChangeWithoutStateUpdate remove 1")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestPlacementChangeWithoutStateUpdate replica 2")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestPlacementChangeWithoutStateUpdate replica 3")

	i10 := placement.NewEmptyInstance("i10", "r4", "z1", "endpoint", 1)
	p, err = a.AddInstance(p, i10)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestPlacementChangeWithoutStateUpdate add 1")

	h11 := placement.NewEmptyInstance("i11", "r7", "z1", "endpoint", 1)
	p, err = a.AddInstance(p, h11)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestPlacementChangeWithoutStateUpdate add 2")

	i13 := placement.NewEmptyInstance("i13", "r5", "z1", "endpoint", 1)
	p, err = a.ReplaceInstance(p, i3.ID(), []services.PlacementInstance{i13})
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestPlacementChangeWithoutStateUpdate replace 1")

	p, err = a.RemoveInstance(p, i4.ID())
	assert.NoError(t, err)
	validateDistribution(t, p, 1.02, "TestPlacementChangeWithoutStateUpdate remove 2")
}

func TestOverSizedRack(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i6 := placement.NewEmptyInstance("i6", "r1", "z1", "endpoint", 1)
	i7 := placement.NewEmptyInstance("i7", "r1", "z1", "endpoint", 1)

	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i3 := placement.NewEmptyInstance("i3", "r2", "z1", "endpoint", 1)

	i4 := placement.NewEmptyInstance("i4", "r3", "z1", "endpoint", 1)
	i8 := placement.NewEmptyInstance("i8", "r3", "z1", "endpoint", 1)

	i5 := placement.NewEmptyInstance("i5", "r4", "z1", "endpoint", 1)

	i9 := placement.NewEmptyInstance("i9", "r5", "z1", "endpoint", 1)

	instances := []services.PlacementInstance{i1, i2, i3, i4, i5, i6, i7, i8, i9}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := newShardedAlgorithm(placement.NewOptions().SetAllowPartialReplace(false))
	p, err := a.InitialPlacement(instances, ids)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestOverSizedRack replica 1")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestOverSizedRack replica 2")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestOverSizedRack replica 3")

	i10 := placement.NewEmptyInstance("i10", "r4", "z1", "endpoint", 1)
	_, err = a.ReplaceInstance(p, i8.ID(), []services.PlacementInstance{i10})
	assert.Error(t, err)

	a = newShardedAlgorithm(placement.NewOptions())
	p, err = a.ReplaceInstance(p, i8.ID(), []services.PlacementInstance{i10})
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestOverSizedRack replace 1")

	// At this point, r1 has 4 instances to share a copy of 1024 partitions
	i11 := placement.NewEmptyInstance("i11", "r1", "z1", "endpoint", 1)

	p, err = a.ReplaceInstance(p, i2.ID(), []services.PlacementInstance{i11})
	assert.NoError(t, err)
	validateDistribution(t, p, 1.22, "TestOverSizedRack replace 2")

	// adding a new instance to relieve the load on the hot instances
	i12 := placement.NewEmptyInstance("i12", "r4", "z1", "endpoint", 1)
	p, err = a.AddInstance(p, i12)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.15, "TestOverSizedRack add 1")
}

func TestRemoveInitializingInstance(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "e1", 1)
	i2 := placement.NewEmptyInstance("i2", "r1", "z1", "e2", 1)

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement([]services.PlacementInstance{i1}, []uint32{1, 2})
	assert.NoError(t, err)

	p, err = MarkShardAvailable(p, "i1", 1)
	assert.NoError(t, err)
	p, err = MarkShardAvailable(p, "i1", 2)
	assert.NoError(t, err)

	instance1, ok := p.Instance("i1")
	assert.True(t, ok)
	assert.Equal(t, 2, instance1.Shards().NumShardsForState(shard.Available))

	p, err = a.AddInstance(p, i2)
	assert.NoError(t, err)

	instance1, ok = p.Instance("i1")
	assert.True(t, ok)
	assert.Equal(t, 1, instance1.Shards().NumShardsForState(shard.Available))
	assert.Equal(t, 1, instance1.Shards().NumShardsForState(shard.Leaving))

	instance2, ok := p.Instance("i2")
	assert.True(t, ok)
	assert.Equal(t, 1, instance2.Shards().NumShardsForState(shard.Initializing))
	for _, s := range instance2.Shards().All() {
		assert.Equal(t, "i1", s.SourceID())
	}

	p, err = a.RemoveInstance(p, "i2")
	assert.NoError(t, err)

	instance1, ok = p.Instance("i1")
	assert.True(t, ok)
	for _, s := range instance1.Shards().All() {
		assert.Equal(t, shard.Available, s.State())
		assert.Equal(t, "", s.SourceID())
	}

	_, ok = p.Instance("i2")
	assert.False(t, ok)
}

func TestInitPlacementOnNoInstances(t *testing.T) {
	instances := []services.PlacementInstance{}

	ids := make([]uint32, 128)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids)
	assert.Error(t, err)
	assert.Nil(t, p)
}

func TestOneRack(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i2 := placement.NewEmptyInstance("i2", "r1", "z1", "endpoint", 1)

	instances := []services.PlacementInstance{i1, i2}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestOneRack replica 1")

	i6 := placement.NewEmptyInstance("i6", "r1", "z1", "endpoint", 1)

	p, err = a.AddInstance(p, i6)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestOneRack addinstance 1")
}

func TestRFGreaterThanRackLen(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i6 := placement.NewEmptyInstance("i6", "r1", "z1", "endpoint", 1)

	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i3 := placement.NewEmptyInstance("i3", "r2", "z1", "endpoint", 1)

	instances := []services.PlacementInstance{i1, i2, i3, i6}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestRFGreaterThanRackLen replica 1")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestRFGreaterThanRackLen replica 2")

	p1, err := a.AddReplica(p)
	assert.Error(t, err)
	assert.Nil(t, p1)
	assert.NoError(t, placement.Validate(p))
}

func TestRFGreaterThanRackLenAfterInstanceRemoval(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)

	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)

	instances := []services.PlacementInstance{i1, i2}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestRFGreaterThanRackLenAfterInstanceRemoval replica 1")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestRFGreaterThanRackLenAfterInstanceRemoval replica 2")

	p1, err := a.RemoveInstance(p, i2.ID())
	assert.Error(t, err)
	assert.Nil(t, p1)
	assert.NoError(t, placement.Validate(p))
}

func TestRFGreaterThanRackLenAfterInstanceReplace(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)

	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)

	instances := []services.PlacementInstance{i1, i2}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestRFGreaterThanRackLenAfterInstanceReplace replica 1")

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestRFGreaterThanRackLenAfterInstanceReplace replica 2")

	i3 := placement.NewEmptyInstance("i3", "r1", "z1", "endpoint", 1)
	p1, err := a.ReplaceInstance(p, i2.ID(), []services.PlacementInstance{i3})
	assert.Error(t, err)
	assert.Nil(t, p1)
	assert.NoError(t, placement.Validate(p))
}

func TestLooseRackCheckAlgorithm(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)

	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)

	instances := []services.PlacementInstance{i1, i2}

	ids := make([]uint32, 10)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids)
	assert.NoError(t, err)
	markAllShardsAsAvailable(t, p)
	assert.NoError(t, placement.Validate(p))

	p, err = a.AddReplica(p)
	assert.NoError(t, err)
	markAllShardsAsAvailable(t, p)
	assert.NoError(t, placement.Validate(p))

	p1, err := a.AddReplica(p)
	assert.Equal(t, errNotEnoughRacks, err)
	assert.Nil(t, p1)
	markAllShardsAsAvailable(t, p)
	assert.NoError(t, placement.Validate(p))

	i4 := placement.NewEmptyInstance("i4", "r2", "z1", "endpoint", 1)
	p, err = a.AddInstance(p, i4)
	assert.NoError(t, err)
	markAllShardsAsAvailable(t, p)
	assert.NoError(t, placement.Validate(p))

	p1, err = a.AddReplica(p)
	assert.Equal(t, errNotEnoughRacks, err)
	assert.Nil(t, p1)
	markAllShardsAsAvailable(t, p)
	assert.NoError(t, placement.Validate(p))

	b := newShardedAlgorithm(placement.NewOptions().SetLooseRackCheck(true))
	// different with normal algo, which would return error here
	i3 := placement.NewEmptyInstance("i3", "r1", "z1", "endpoint", 1)
	p, err = b.ReplaceInstance(p, i2.ID(), []services.PlacementInstance{i3})
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))

	p1, err = b.ReplaceInstance(p, i4.ID(), []services.PlacementInstance{i3})
	assert.Equal(t, errAddingInstanceAlreadyExist, err)
	assert.Nil(t, p1)
	assert.NoError(t, placement.Validate(p))

	markAllShardsAsAvailable(t, p)
	p, err = b.RemoveInstance(p, i3.ID())
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))

	markAllShardsAsAvailable(t, p)
	i5 := placement.NewEmptyInstance("i5", "r3", "z1", "endpoint", 1)
	p, err = b.AddInstance(p, i5)
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))

	p, err = b.AddReplica(p)
	assert.NoError(t, err)
	markAllShardsAsAvailable(t, p)
	assert.NoError(t, placement.Validate(p))
}

func TestAddInstancesCouldNotReachTargetLoad(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	p := placement.NewPlacement().
		SetInstances([]services.PlacementInstance{}).
		SetShards(ids).
		SetReplicaFactor(1).
		SetIsSharded(true)

	a := newShardedAlgorithm(placement.NewOptions())

	p1, err := a.AddInstance(p, i1)
	// errCouldNotReachTargetLoad should only happen when trying to add a instance to
	// an invalid placement that does not have enough shards for the rf it thought it has
	assert.Equal(t, errCouldNotReachTargetLoad, err)
	assert.Nil(t, p1)
}

func TestRemoveAbsentInstance(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)

	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)

	instances := []services.PlacementInstance{i1, i2}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestRemoveAbsentInstance replica 1")

	i3 := placement.NewEmptyInstance("i3", "r3", "z1", "endpoint", 1)

	p1, err := a.RemoveInstance(p, i3.ID())
	assert.Error(t, err)
	assert.Nil(t, p1)
	assert.NoError(t, placement.Validate(p))
}

func TestReplaceAbsentInstance(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)

	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)

	instances := []services.PlacementInstance{i1, i2}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestReplaceAbsentInstance replica 1")

	i3 := placement.NewEmptyInstance("i3", "r3", "z1", "endpoint", 1)
	i4 := placement.NewEmptyInstance("i4", "r4", "z1", "endpoint", 1)

	p1, err := a.ReplaceInstance(p, i3.ID(), []services.PlacementInstance{i4})
	assert.Error(t, err)
	assert.Nil(t, p1)
	assert.NoError(t, placement.Validate(p))
}

func TestInit(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "", "e1", 1)
	i2 := placement.NewEmptyInstance("i2", "r2", "", "e2", 1)
	instances := []services.PlacementInstance{i1, i2}

	numShards := 4
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids)
	assert.NoError(t, err)
	assert.Equal(t, 1, p.ReplicaFactor())
	assert.Equal(t, numShards, p.NumShards())
	i1, _ = p.Instance("i1")
	i2, _ = p.Instance("i2")
	assert.Equal(t, 2, loadOnInstance(i1))
	assert.Equal(t, 2, i1.Shards().NumShards())
	assert.Equal(t, "e1", i1.Endpoint())
	assert.Equal(t, 2, loadOnInstance(i2))
	assert.Equal(t, 2, i2.Shards().NumShards())
	assert.Equal(t, "e2", i2.Endpoint())
	for _, instance := range p.Instances() {
		for _, s := range instance.Shards().All() {
			assert.Equal(t, shard.Initializing, s.State())
			assert.Equal(t, "", s.SourceID())
		}
	}
}

func TestAddReplica(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "", "e1", 1)
	i1.Shards().Add(shard.NewShard(0).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))

	i2 := placement.NewEmptyInstance("i2", "r2", "", "e2", 1)
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	instances := []services.PlacementInstance{i1, i2}

	numShards := 4
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}
	p := placement.NewPlacement().
		SetInstances(instances).
		SetShards(ids).
		SetReplicaFactor(1).
		SetIsSharded(true)

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.AddReplica(p)
	assert.NoError(t, err)
	assert.Equal(t, 2, p.ReplicaFactor())
	assert.Equal(t, numShards, p.NumShards())
	i1, _ = p.Instance("i1")
	i2, _ = p.Instance("i2")
	assert.Equal(t, 4, loadOnInstance(i1))
	assert.Equal(t, 4, i1.Shards().NumShards())
	assert.Equal(t, "e1", i1.Endpoint())
	assert.Equal(t, 4, loadOnInstance(i2))
	assert.Equal(t, 4, i2.Shards().NumShards())
	assert.Equal(t, "e2", i2.Endpoint())
	for _, instance := range p.Instances() {
		availableTotal := 0
		initTotal := 0
		for _, s := range instance.Shards().All() {
			assert.NotEqual(t, shard.Leaving, s.State())
			assert.Equal(t, "", s.SourceID())
			if s.State() == shard.Available {
				availableTotal++
			}
			if s.State() == shard.Initializing {
				initTotal++
			}
		}
		assert.Equal(t, 2, availableTotal)
		assert.Equal(t, 2, initTotal)
	}
}

func TestAddInstance(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "", "e1", 1)
	i1.Shards().Add(shard.NewShard(0).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))

	instances := []services.PlacementInstance{i1}

	numShards := 2
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}
	p := placement.NewPlacement().
		SetInstances(instances).
		SetShards(ids).
		SetReplicaFactor(1).
		SetIsSharded(true)

	a := newShardedAlgorithm(placement.NewOptions())
	i2 := placement.NewEmptyInstance("i2", "r2", "", "e2", 1)
	p, err := a.AddInstance(p, i2)
	assert.NoError(t, err)
	assert.Equal(t, 1, p.ReplicaFactor())
	i1, _ = p.Instance("i1")
	i2, _ = p.Instance("i2")
	assert.Equal(t, 1, loadOnInstance(i1))
	assert.Equal(t, 2, i1.Shards().NumShards())
	assert.Equal(t, "e1", i1.Endpoint())
	assert.Equal(t, 1, loadOnInstance(i2))
	assert.Equal(t, 1, i2.Shards().NumShards())
	assert.Equal(t, "e2", i2.Endpoint())
	for _, s := range i2.Shards().All() {
		assert.Equal(t, shard.Initializing, s.State())
		assert.Equal(t, "i1", s.SourceID())
	}
}

func TestAddInstance_ExistNonLeaving(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)

	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)

	instances := []services.PlacementInstance{i1, i2}

	ids := make([]uint32, 10)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestAddExistInstance replica 1")

	p1, err := a.AddInstance(p, i2)
	assert.Error(t, err)
	assert.Nil(t, p1)
	assert.NoError(t, placement.Validate(p))
}

func TestAddInstance_ExistAndLeaving(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)

	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)

	instances := []services.PlacementInstance{i1, i2}

	ids := make([]uint32, 10)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids)
	assert.NoError(t, err)
	validateDistribution(t, p, 1.01, "TestAddInstance_ExistAndLeaving replica 1")
	p = markAllShardsAsAvailable(t, p)

	p, err = a.RemoveInstance(p, "i2")
	assert.NoError(t, err)

	i2, ok := p.Instance("i2")
	assert.True(t, ok)
	assert.Equal(t, i2.Shards().NumShards(), i2.Shards().NumShardsForState(shard.Leaving))

	p, err = a.AddInstance(p, i2)
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
}

func TestAddInstance_ExistAndLeavingRackConflict(t *testing.T) {
	i1 := placement.NewInstance().SetID("i1").SetEndpoint("e1").SetRack("r1").SetZone("z1").SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Leaving),
			shard.NewShard(1).SetState(shard.Leaving),
			shard.NewShard(3).SetState(shard.Leaving),
		}))
	i2 := placement.NewInstance().SetID("i2").SetEndpoint("e2").SetRack("r1").SetZone("z2").SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Available),
			shard.NewShard(1).SetState(shard.Available),
		}))
	i3 := placement.NewInstance().SetID("i3").SetEndpoint("e3").SetRack("r3").SetZone("z3").SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(2).SetState(shard.Available),
			shard.NewShard(3).SetState(shard.Available),
		}))
	i4 := placement.NewInstance().SetID("i4").SetEndpoint("e4").SetRack("r4").SetZone("z4").SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i1"),
			shard.NewShard(1).SetState(shard.Initializing).SetSourceID("i1"),
			shard.NewShard(2).SetState(shard.Initializing).SetSourceID("i1"),
			shard.NewShard(3).SetState(shard.Initializing).SetSourceID("i1"),
		}))

	p := placement.NewPlacement().
		SetInstances([]services.PlacementInstance{i1, i2, i3, i4}).
		SetIsSharded(true).
		SetReplicaFactor(2).
		SetShards([]uint32{0, 1, 2, 3})

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.AddInstance(p, i1)
	assert.NoError(t, err)
	i1, ok := p.Instance("i1")
	assert.True(t, ok)
	assert.Equal(t, 2, loadOnInstance(i1))
}

func TestRemoveInstance(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "", "e1", 1)
	i1.Shards().Add(shard.NewShard(0).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))

	i2 := placement.NewEmptyInstance("i2", "r2", "", "e2", 1)
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	instances := []services.PlacementInstance{i1, i2}

	numShards := 4
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}
	p := placement.NewPlacement().
		SetInstances(instances).
		SetShards(ids).
		SetReplicaFactor(1).
		SetIsSharded(true)

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.RemoveInstance(p, i2.ID())
	assert.NoError(t, err)
	assert.Equal(t, 1, p.ReplicaFactor())
	assert.Equal(t, numShards, p.NumShards())
	i1, _ = p.Instance("i1")
	i2, _ = p.Instance("i2")
	assert.Equal(t, 4, loadOnInstance(i1))
	assert.Equal(t, 4, i1.Shards().NumShards())
	assert.Equal(t, "e1", i1.Endpoint())
	assert.Equal(t, 0, loadOnInstance(i2))
	assert.Equal(t, 2, i2.Shards().NumShards())
	assert.Equal(t, "e2", i2.Endpoint())
	for _, s := range i2.Shards().All() {
		assert.Equal(t, shard.Leaving, s.State())
		assert.Equal(t, "", s.SourceID())
	}
	availableTotal := 0
	initTotal := 0
	for _, s := range i1.Shards().All() {
		if s.State() == shard.Available {
			availableTotal++
		}
		if s.State() == shard.Initializing {
			initTotal++
		}
	}
	assert.Equal(t, 2, availableTotal)
	assert.Equal(t, 2, initTotal)
}

func TestReplaceInstance(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "", "e1", 1)
	i1.Shards().Add(shard.NewShard(0).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))

	i2 := placement.NewEmptyInstance("i2", "r2", "", "e2", 1)
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	instances := []services.PlacementInstance{i1, i2}

	numShards := 4
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}
	p := placement.NewPlacement().
		SetInstances(instances).
		SetShards(ids).
		SetReplicaFactor(1).
		SetIsSharded(true)

	a := newShardedAlgorithm(placement.NewOptions())
	i3 := placement.NewEmptyInstance("i3", "r3", "", "e3", 1)
	p, err := a.ReplaceInstance(p, i2.ID(), []services.PlacementInstance{i3})
	assert.NoError(t, err)
	i1, _ = p.Instance("i1")
	i2, _ = p.Instance("i2")
	i3, _ = p.Instance("i3")
	assert.Equal(t, 2, loadOnInstance(i1))
	assert.Equal(t, 2, i1.Shards().NumShards())
	assert.Equal(t, "e1", i1.Endpoint())
	assert.Equal(t, 0, loadOnInstance(i2))
	assert.Equal(t, 2, i2.Shards().NumShards())
	assert.Equal(t, "e2", i2.Endpoint())
	assert.Equal(t, 2, loadOnInstance(i3))
	assert.Equal(t, 2, i3.Shards().NumShards())
	assert.Equal(t, "e3", i3.Endpoint())
	for _, s := range i1.Shards().All() {
		assert.Equal(t, shard.Available, s.State())
		assert.Equal(t, "", s.SourceID())
	}
	for _, s := range i2.Shards().All() {
		assert.Equal(t, shard.Leaving, s.State())
		assert.Equal(t, "", s.SourceID())
	}
	for _, s := range i3.Shards().All() {
		assert.Equal(t, shard.Initializing, s.State())
		assert.Equal(t, "i2", s.SourceID())
	}
}

func TestReplaceInstance_Backout(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "", "e1", 1)
	i1.Shards().Add(shard.NewShard(0).SetState(shard.Available))

	i2 := placement.NewEmptyInstance("i2", "r2", "", "e2", 1)
	i2.Shards().Add(shard.NewShard(1).SetState(shard.Available))

	instances := []services.PlacementInstance{i1, i2}

	p := placement.NewPlacement().
		SetInstances(instances).
		SetShards([]uint32{0, 1}).
		SetReplicaFactor(1).
		SetIsSharded(true)

	a := newShardedAlgorithm(placement.NewOptions())
	i3 := placement.NewEmptyInstance("i3", "r3", "", "e3", 1)
	p, err := a.ReplaceInstance(p, i2.ID(), []services.PlacementInstance{i3})
	assert.NoError(t, err)
	i1, _ = p.Instance("i1")
	i2, _ = p.Instance("i2")
	i3, _ = p.Instance("i3")
	assert.Equal(t, 1, loadOnInstance(i1))
	assert.Equal(t, 1, i1.Shards().NumShards())
	assert.Equal(t, "e1", i1.Endpoint())
	assert.Equal(t, 0, loadOnInstance(i2))
	assert.Equal(t, 1, i2.Shards().NumShards())
	assert.Equal(t, "e2", i2.Endpoint())
	assert.Equal(t, 1, loadOnInstance(i3))
	assert.Equal(t, 1, i3.Shards().NumShards())
	assert.Equal(t, "e3", i3.Endpoint())
	for _, s := range i1.Shards().All() {
		assert.Equal(t, shard.Available, s.State())
		assert.Equal(t, "", s.SourceID())
	}
	for _, s := range i2.Shards().All() {
		assert.Equal(t, shard.Leaving, s.State())
		assert.Equal(t, "", s.SourceID())
	}
	for _, s := range i3.Shards().All() {
		assert.Equal(t, shard.Initializing, s.State())
		assert.Equal(t, "i2", s.SourceID())
	}

	p, err = a.AddInstance(p, i2)
	assert.NoError(t, err)
	_, ok := p.Instance("i3")
	assert.False(t, ok)

	i1, _ = p.Instance("i1")
	i2, _ = p.Instance("i2")
	assert.Equal(t, 1, loadOnInstance(i1))
	assert.Equal(t, 1, i1.Shards().NumShards())
	assert.Equal(t, 1, i1.Shards().NumShardsForState(shard.Available))
	assert.Equal(t, "e1", i1.Endpoint())
	assert.Equal(t, 1, loadOnInstance(i2))
	assert.Equal(t, 1, i2.Shards().NumShards())
	assert.Equal(t, 1, i2.Shards().NumShardsForState(shard.Available))
	assert.Equal(t, "e2", i2.Endpoint())
}

func TestShardedAlgoOnNonShardedPlacement(t *testing.T) {
	i1 := placement.NewInstance().SetID("i1").SetEndpoint("e1")
	i2 := placement.NewInstance().SetID("i2").SetEndpoint("e2")
	i3 := placement.NewInstance().SetID("i3").SetEndpoint("e3")
	i4 := placement.NewInstance().SetID("i4").SetEndpoint("e4")

	p, err := newNonShardedAlgorithm().InitialPlacement([]services.PlacementInstance{i1, i2}, []uint32{})
	assert.NoError(t, err)

	a := newShardedAlgorithm(placement.NewOptions())
	_, err = a.AddReplica(p)
	assert.Error(t, err)
	assert.Equal(t, errShardedAlgoOnNotShardedPlacement, err)

	_, err = a.AddInstance(p, i3)
	assert.Error(t, err)
	assert.Equal(t, errShardedAlgoOnNotShardedPlacement, err)

	_, err = a.RemoveInstance(p, "i1")
	assert.Error(t, err)
	assert.Equal(t, errShardedAlgoOnNotShardedPlacement, err)

	_, err = a.ReplaceInstance(p, "i1", []services.PlacementInstance{i3, i4})
	assert.Error(t, err)
	assert.Equal(t, errShardedAlgoOnNotShardedPlacement, err)
}

func markAllShardsAsAvailable(t *testing.T, p services.ServicePlacement) services.ServicePlacement {
	var err error
	for _, instance := range p.Instances() {
		for _, s := range instance.Shards().All() {
			if s.State() == shard.Initializing {
				p, err = MarkShardAvailable(p, instance.ID(), s.ID())
				assert.NoError(t, err)
			}
		}
	}
	return p
}

func validateDistribution(t *testing.T, p services.ServicePlacement, expectPeakOverAvg float64, testCase string) {
	assert.NoError(t, placement.Validate(p), "placement validation failed")
	ph := NewPlacementHelper(p, placement.NewOptions()).(*placementHelper)
	total := 0
	for _, i := range p.Instances() {
		load := loadOnInstance(i)
		total += load
		avgLoad := getWeightedLoad(ph, i.Weight())
		instanceOverAvg := float64(load) / float64(avgLoad)
		if math.Abs(float64(load-avgLoad)) > 1 {
			assert.True(t, instanceOverAvg <= expectPeakOverAvg, fmt.Sprintf("Bad distribution in %s, peak/Avg on %s is too high: %v, expecting %v, load on instance: %v, avg load: %v",
				testCase, i.ID(), instanceOverAvg, expectPeakOverAvg, load, avgLoad))
		}

		targetLoad := ph.TargetLoadForInstance(i.ID())
		if targetLoad == 0 {
			continue
		}
		instanceOverTarget := float64(load) / float64(targetLoad)
		if math.Abs(float64(load-targetLoad)) > 1 {
			assert.True(t, instanceOverTarget <= 1.03, fmt.Sprintf("Bad distribution in %s, peak/Target on %s is too high: %v, load on instance: %v, target load: %v",
				testCase, i.ID(), instanceOverTarget, load, targetLoad))
		}
	}
	assert.Equal(t, total, p.ReplicaFactor()*p.NumShards(), fmt.Sprintf("Wrong total shards: expecting %v, but got %v", p.ReplicaFactor()*p.NumShards(), total))
}

func getWeightedLoad(ph *placementHelper, weight uint32) int {
	return ph.rf * len(ph.shardToInstanceMap) * int(weight) / int(ph.totalWeight)
}
