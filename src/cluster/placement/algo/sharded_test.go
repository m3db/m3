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
	"time"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMinorWeightDifference(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint1", 262136)
	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint2", 262144)
	i3 := placement.NewEmptyInstance("i3", "r3", "z1", "endpoint3", 262144)
	i4 := placement.NewEmptyInstance("i4", "r4", "z1", "endpoint4", 262144)
	i5 := placement.NewEmptyInstance("i5", "r5", "z1", "endpoint5", 262144)
	i6 := placement.NewEmptyInstance("i6", "r4", "z1", "endpoint6", 262144)

	numShards := 1024
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement([]placement.Instance{i1, i2, i3, i4, i5, i6}, ids, 1)
	require.NoError(t, err)
	validateDistribution(t, p, 1.01)

	p, err = a.AddReplica(p)
	require.NoError(t, err)
	validateDistribution(t, p, 1.01)

	p, err = a.AddReplica(p)
	require.NoError(t, err)
	validateDistribution(t, p, 1.01)
}

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

	instances := []placement.Instance{i1, i2, i3, i4, i5, i6, i7, i8, i9}

	numShards := 1024
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	opts := placement.NewOptions().
		SetPlacementCutoverNanosFn(timeNanosGen(1)).
		SetShardCutoverNanosFn(timeNanosGen(2)).
		SetShardCutoffNanosFn(timeNanosGen(3))
	a := newShardedAlgorithm(opts)
	p, err := a.InitialPlacement(instances, ids, 1)
	require.NoError(t, err)
	validateCutoverCutoffNanos(t, p, opts)
	validateDistribution(t, p, 1.01)
	p, updated := mustMarkAllShardsAsAvailable(t, p, opts)
	require.True(t, updated)

	opts = placement.NewOptions().
		SetPlacementCutoverNanosFn(timeNanosGen(11)).
		SetShardCutoverNanosFn(timeNanosGen(12)).
		SetShardCutoffNanosFn(timeNanosGen(13))
	a = newShardedAlgorithm(opts)
	p, err = a.AddInstances(p, []placement.Instance{placement.NewEmptyInstance("i21", "r6", "z1", "endpoint", 1)})
	require.NoError(t, err)
	validateCutoverCutoffNanos(t, p, opts)
	p, updated = mustMarkAllShardsAsAvailable(t, p, opts)
	require.True(t, updated)
	validateCutoverCutoffNanos(t, p, opts)
	validateDistribution(t, p, 1.01)

	opts = placement.NewOptions().
		SetPlacementCutoverNanosFn(timeNanosGen(21)).
		SetShardCutoverNanosFn(timeNanosGen(22)).
		SetShardCutoffNanosFn(timeNanosGen(23))
	a = newShardedAlgorithm(opts)
	p, err = a.RemoveInstances(p, []string{i1.ID()})
	require.NoError(t, err)
	validateCutoverCutoffNanos(t, p, opts)
	p, updated = mustMarkAllShardsAsAvailable(t, p, opts)
	require.True(t, updated)
	_, exist := p.Instance(i1.ID())
	assert.False(t, exist)
	validateCutoverCutoffNanos(t, p, opts)
	validateDistribution(t, p, 1.01)

	opts = placement.NewOptions().
		SetPlacementCutoverNanosFn(timeNanosGen(31)).
		SetShardCutoverNanosFn(timeNanosGen(32)).
		SetShardCutoffNanosFn(timeNanosGen(33))
	a = newShardedAlgorithm(opts)
	i12 := placement.NewEmptyInstance("i12", "r3", "z1", "endpoint", 1)
	p, err = a.ReplaceInstances(p, []string{i5.ID()}, []placement.Instance{i12})
	require.NoError(t, err)
	validateCutoverCutoffNanos(t, p, opts)
	_, exist = p.Instance(i5.ID())
	assert.True(t, exist)
	p, updated = mustMarkAllShardsAsAvailable(t, p, opts)
	require.True(t, updated)
	_, exist = p.Instance(i5.ID())
	assert.False(t, exist)
	validateCutoverCutoffNanos(t, p, opts)
	validateDistribution(t, p, 1.01)

	opts = placement.NewOptions().
		SetPlacementCutoverNanosFn(timeNanosGen(41)).
		SetShardCutoverNanosFn(timeNanosGen(42)).
		SetShardCutoffNanosFn(timeNanosGen(43))
	a = newShardedAlgorithm(opts)
	p, err = a.RemoveInstances(p, []string{i2.ID()})
	require.NoError(t, err)
	validateCutoverCutoffNanos(t, p, opts)
	p, updated = mustMarkAllShardsAsAvailable(t, p, opts)
	require.True(t, updated)
	validateCutoverCutoffNanos(t, p, opts)
	validateDistribution(t, p, 1.01)

	opts = placement.NewOptions().
		SetPlacementCutoverNanosFn(timeNanosGen(51)).
		SetShardCutoverNanosFn(timeNanosGen(52)).
		SetShardCutoffNanosFn(timeNanosGen(53))
	a = newShardedAlgorithm(opts)
	p, err = a.AddReplica(p)
	require.NoError(t, err)
	validateCutoverCutoffNanos(t, p, opts)
	p, updated = mustMarkAllShardsAsAvailable(t, p, opts)
	require.True(t, updated)
	validateCutoverCutoffNanos(t, p, opts)
	validateDistribution(t, p, 1.01)

	p, err = a.AddReplica(p)
	require.NoError(t, err)
	validateCutoverCutoffNanos(t, p, opts)
	p, updated = mustMarkAllShardsAsAvailable(t, p, opts)
	require.True(t, updated)
	validateCutoverCutoffNanos(t, p, opts)
	validateDistribution(t, p, 1.01)

	i10 := placement.NewEmptyInstance("i10", "r4", "z1", "endpoint", 1)
	i11 := placement.NewEmptyInstance("i11", "r7", "z1", "endpoint", 1)
	p, err = a.AddInstances(p, []placement.Instance{i10, i11})
	require.NoError(t, err)
	validateCutoverCutoffNanos(t, p, opts)
	p, updated = mustMarkAllShardsAsAvailable(t, p, opts)
	require.True(t, updated)
	validateCutoverCutoffNanos(t, p, opts)
	validateDistribution(t, p, 1.01)

	i13 := placement.NewEmptyInstance("i13", "r5", "z1", "endpoint", 1)
	p, err = a.ReplaceInstances(p, []string{i3.ID()}, []placement.Instance{i13})
	require.NoError(t, err)
	validateCutoverCutoffNanos(t, p, opts)
	p, updated = mustMarkAllShardsAsAvailable(t, p, opts)
	require.True(t, updated)
	validateCutoverCutoffNanos(t, p, opts)
	validateDistribution(t, p, 1.01)

	p, err = a.RemoveInstances(p, []string{i4.ID()})
	require.NoError(t, err)
	validateCutoverCutoffNanos(t, p, opts)
	p, updated = mustMarkAllShardsAsAvailable(t, p, opts)
	require.True(t, updated)
	validateCutoverCutoffNanos(t, p, opts)
	validateDistribution(t, p, 1.02)
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

	instances := []placement.Instance{i1, i2, i3, i4, i5, i6, i7, i8, i9}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	opts := placement.NewOptions()
	a := newShardedAlgorithm(opts)
	p, err := a.InitialPlacement(instances, ids, 1)
	require.NoError(t, err)
	validateDistribution(t, p, 1.01)

	p, err = a.AddInstances(p, []placement.Instance{placement.NewEmptyInstance("h21", "r2", "z1", "endpoint", 10)})
	require.NoError(t, err)
	p, _ = mustMarkAllShardsAsAvailable(t, p, opts)
	validateDistribution(t, p, 1.01)

	p, err = a.RemoveInstances(p, []string{i1.ID()})
	require.NoError(t, err)
	p, _ = mustMarkAllShardsAsAvailable(t, p, opts)
	validateDistribution(t, p, 1.01)

	p, err = a.ReplaceInstances(p, []string{i3.ID()},
		[]placement.Instance{
			placement.NewEmptyInstance("h31", "r1", "z1", "endpoint", 10),
			placement.NewEmptyInstance("h32", "r1", "z1", "endpoint", 10),
		})
	require.NoError(t, err)
	p, _ = mustMarkAllShardsAsAvailable(t, p, opts)
	validateDistribution(t, p, 1.01)

	p, err = a.AddReplica(p)
	require.NoError(t, err)
	p, _ = mustMarkAllShardsAsAvailable(t, p, opts)
	validateDistribution(t, p, 1.01)

	p, err = a.AddReplica(p)
	require.NoError(t, err)
	p, _ = mustMarkAllShardsAsAvailable(t, p, opts)
	validateDistribution(t, p, 1.01)

	h10 := placement.NewEmptyInstance("h10", "r10", "z1", "endpoint", 10)
	h11 := placement.NewEmptyInstance("h11", "r7", "z1", "endpoint", 10)
	p, err = a.AddInstances(p, []placement.Instance{h10, h11})
	require.NoError(t, err)
	p, _ = mustMarkAllShardsAsAvailable(t, p, opts)
	validateDistribution(t, p, 1.01)

	h13 := placement.NewEmptyInstance("h13", "r5", "z1", "endpoint", 10)
	p, err = a.ReplaceInstances(p, []string{h11.ID()}, []placement.Instance{h13})
	require.NoError(t, err)
	p, _ = mustMarkAllShardsAsAvailable(t, p, opts)
	validateDistribution(t, p, 1.01)
}

func TestPlacementChangeWithoutStateUpdate(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i2 := placement.NewEmptyInstance("i2", "r1", "z1", "endpoint", 1)
	i3 := placement.NewEmptyInstance("i3", "r2", "z1", "endpoint", 1)
	i4 := placement.NewEmptyInstance("i4", "r2", "z1", "endpoint", 1)
	i5 := placement.NewEmptyInstance("i5", "r3", "z1", "endpoint", 1)
	i6 := placement.NewEmptyInstance("i6", "r4", "z1", "endpoint", 1)
	i7 := placement.NewEmptyInstance("i7", "r5", "z1", "endpoint", 1)
	i8 := placement.NewEmptyInstance("i8", "r6", "z1", "endpoint", 1)
	i9 := placement.NewEmptyInstance("i9", "r7", "z1", "endpoint", 1)

	instances := []placement.Instance{i1, i2, i3, i4, i5, i6, i7, i8, i9}

	numShards := 100
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids, 1)
	require.NoError(t, err)
	assert.Equal(t, 1, p.ReplicaFactor())
	assert.Equal(t, numShards, p.NumShards())
	for _, instance := range p.Instances() {
		assert.Equal(t, instance.Shards().NumShards(), instance.Shards().NumShardsForState(shard.Initializing))
	}
	validateDistribution(t, p, 1.01)

	p, err = a.AddInstances(p, []placement.Instance{placement.NewEmptyInstance("i21", "r6", "z1", "endpoint", 1)})
	require.NoError(t, err)
	validateDistribution(t, p, 1.01)

	p, err = a.RemoveInstances(p, []string{i1.ID()})
	require.NoError(t, err)
	validateDistribution(t, p, 1.01)

	i12 := placement.NewEmptyInstance("i12", "r3", "z1", "endpoint", 1)
	p, err = a.ReplaceInstances(p, []string{i5.ID()}, []placement.Instance{i12})
	require.NoError(t, err)
	validateDistribution(t, p, 1.01)

	p, err = a.RemoveInstances(p, []string{i2.ID()})
	require.NoError(t, err)
	validateDistribution(t, p, 1.01)

	p, err = a.AddReplica(p)
	require.NoError(t, err)
	validateDistribution(t, p, 1.01)

	p, err = a.AddReplica(p)
	require.NoError(t, err)
	validateDistribution(t, p, 1.01)

	i10 := placement.NewEmptyInstance("i10", "r4", "z1", "endpoint", 1)
	i11 := placement.NewEmptyInstance("i11", "r7", "z1", "endpoint", 1)
	p, err = a.AddInstances(p, []placement.Instance{i10, i11})
	require.NoError(t, err)
	validateDistribution(t, p, 1.01)

	i13 := placement.NewEmptyInstance("i13", "r5", "z1", "endpoint", 1)
	p, err = a.ReplaceInstances(p, []string{i3.ID()}, []placement.Instance{i13})
	require.NoError(t, err)
	validateDistribution(t, p, 1.01)

	p, err = a.RemoveInstances(p, []string{i4.ID()})
	require.NoError(t, err)
	validateDistribution(t, p, 1.02)
}

func TestOverSizedIsolationGroup(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i6 := placement.NewEmptyInstance("i6", "r1", "z1", "endpoint", 1)
	i7 := placement.NewEmptyInstance("i7", "r1", "z1", "endpoint", 1)

	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i3 := placement.NewEmptyInstance("i3", "r2", "z1", "endpoint", 1)

	i4 := placement.NewEmptyInstance("i4", "r3", "z1", "endpoint", 1)
	i8 := placement.NewEmptyInstance("i8", "r3", "z1", "endpoint", 1)

	i5 := placement.NewEmptyInstance("i5", "r4", "z1", "endpoint", 1)

	i9 := placement.NewEmptyInstance("i9", "r5", "z1", "endpoint", 1)

	instances := []placement.Instance{i1, i2, i3, i4, i5, i6, i7, i8, i9}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	opts := placement.NewOptions().SetAllowPartialReplace(false)
	a := newShardedAlgorithm(opts)
	p, err := a.InitialPlacement(instances, ids, 1)
	require.NoError(t, err)
	validateDistribution(t, p, 1.01)

	p, err = a.AddReplica(p)
	require.NoError(t, err)
	validateDistribution(t, p, 1.01)

	p, err = a.AddReplica(p)
	require.NoError(t, err)
	validateDistribution(t, p, 1.01)

	i10 := placement.NewEmptyInstance("i10", "r4", "z1", "endpoint", 1)
	_, err = a.ReplaceInstances(p, []string{i8.ID()}, []placement.Instance{i10})
	assert.Error(t, err)

	p, updated := mustMarkAllShardsAsAvailable(t, p, opts)
	require.True(t, updated)
	a = newShardedAlgorithm(placement.NewOptions())
	i11 := placement.NewEmptyInstance("i11", "r1", "z1", "endpoint", 1)
	p, err = a.ReplaceInstances(p, []string{i8.ID(), i2.ID()}, []placement.Instance{i10, i11})
	require.NoError(t, err)
	i8, ok := p.Instance(i8.ID())
	assert.True(t, ok)
	assert.True(t, i8.IsLeaving())
	i2, ok = p.Instance(i2.ID())
	assert.True(t, ok)
	assert.True(t, i2.IsLeaving())
	i10, ok = p.Instance(i10.ID())
	assert.True(t, ok)
	assert.True(t, i10.IsInitializing())
	i11, ok = p.Instance(i11.ID())
	assert.True(t, ok)
	assert.True(t, i11.IsInitializing())
	validateDistribution(t, p, 1.22)

	// adding a new instance to relieve the load on the hot instances
	i12 := placement.NewEmptyInstance("i12", "r4", "z1", "endpoint", 1)
	p, err = a.AddInstances(p, []placement.Instance{i12})
	require.NoError(t, err)
	validateDistribution(t, p, 1.15)
}

func TestRemoveInitializingInstance(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "e1", 1)
	i2 := placement.NewEmptyInstance("i2", "r1", "z1", "e2", 1)

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement([]placement.Instance{i1}, []uint32{1, 2}, 1)
	require.NoError(t, err)

	instance1, ok := p.Instance("i1")
	assert.True(t, ok)
	assert.Equal(t, 2, instance1.Shards().NumShardsForState(shard.Initializing))

	p, updated := mustMarkAllShardsAsAvailable(t, p, placement.NewOptions())
	require.True(t, updated)

	instance1, ok = p.Instance("i1")
	assert.True(t, ok)
	assert.Equal(t, 2, instance1.Shards().NumShardsForState(shard.Available))

	p, err = a.AddInstances(p, []placement.Instance{i2})
	require.NoError(t, err)

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

	p, err = a.RemoveInstances(p, []string{"i2"})
	require.NoError(t, err)

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
	instances := []placement.Instance{}

	ids := make([]uint32, 128)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids, 1)
	assert.Error(t, err)
	assert.Nil(t, p)
}

func TestOneIsolationGroup(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i2 := placement.NewEmptyInstance("i2", "r1", "z1", "endpoint", 1)

	instances := []placement.Instance{i1, i2}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids, 1)
	require.NoError(t, err)
	validateDistribution(t, p, 1.01)

	i6 := placement.NewEmptyInstance("i6", "r1", "z1", "endpoint", 1)
	p, err = a.AddInstances(p, []placement.Instance{i6})
	require.NoError(t, err)
	validateDistribution(t, p, 1.01)
}

func TestRFGreaterThanNumberOfIsolationGroups(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i6 := placement.NewEmptyInstance("i6", "r1", "z1", "endpoint", 1)

	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i3 := placement.NewEmptyInstance("i3", "r2", "z1", "endpoint", 1)

	instances := []placement.Instance{i1, i2, i3, i6}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids, 1)
	require.NoError(t, err)
	validateDistribution(t, p, 1.01)

	p, err = a.AddReplica(p)
	require.NoError(t, err)
	validateDistribution(t, p, 1.01)

	p1, err := a.AddReplica(p)
	assert.Error(t, err)
	assert.Nil(t, p1)
	require.NoError(t, placement.Validate(p))
}

func TestRFGreaterThanNumberOfIsolationGroupAfterInstanceRemoval(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)

	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)

	instances := []placement.Instance{i1, i2}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids, 1)
	require.NoError(t, err)
	validateDistribution(t, p, 1.01)

	p, err = a.AddReplica(p)
	require.NoError(t, err)
	validateDistribution(t, p, 1.01)

	p1, err := a.RemoveInstances(p, []string{i2.ID()})
	assert.Error(t, err)
	assert.Nil(t, p1)
	require.NoError(t, placement.Validate(p))
}

func TestRFGreaterThanNumberOfIsolationGroupAfterInstanceReplace(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)

	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)

	instances := []placement.Instance{i1, i2}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids, 1)
	require.NoError(t, err)
	validateDistribution(t, p, 1.01)

	p, err = a.AddReplica(p)
	require.NoError(t, err)
	validateDistribution(t, p, 1.01)

	i3 := placement.NewEmptyInstance("i3", "r1", "z1", "endpoint", 1)
	p1, err := a.ReplaceInstances(p, []string{i2.ID()}, []placement.Instance{i3})
	assert.Error(t, err)
	assert.Nil(t, p1)
	require.NoError(t, placement.Validate(p))
}

func TestRemoveMultipleInstances(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 10)
	i2 := placement.NewEmptyInstance("i2", "r1", "z1", "endpoint", 10)
	i3 := placement.NewEmptyInstance("i3", "r2", "z1", "endpoint", 20)
	i4 := placement.NewEmptyInstance("i4", "r3", "z1", "endpoint", 10)
	i5 := placement.NewEmptyInstance("i5", "r4", "z1", "endpoint", 30)
	i6 := placement.NewEmptyInstance("i6", "r6", "z1", "endpoint", 40)
	i7 := placement.NewEmptyInstance("i7", "r7", "z1", "endpoint", 10)
	i8 := placement.NewEmptyInstance("i8", "r8", "z1", "endpoint", 10)
	i9 := placement.NewEmptyInstance("i9", "r9", "z1", "endpoint", 10)

	instances := []placement.Instance{i1, i2, i3, i4, i5, i6, i7, i8, i9}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids, 2)
	require.NoError(t, err)

	p, updated := mustMarkAllShardsAsAvailable(t, p, placement.NewOptions())
	require.True(t, updated)

	p, err = a.RemoveInstances(p, []string{"i1", "i2", "i3"})
	require.NoError(t, err)
	require.NoError(t, placement.Validate(p))

	i1, ok := p.Instance("i1")
	assert.True(t, ok)
	assert.True(t, i1.IsLeaving())
	i2, ok = p.Instance("i2")
	assert.True(t, ok)
	assert.True(t, i2.IsLeaving())
	i3, ok = p.Instance("i3")
	assert.True(t, ok)
	assert.True(t, i3.IsLeaving())
}

func TestRemoveAbsentInstance(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)

	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)

	instances := []placement.Instance{i1, i2}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids, 1)
	require.NoError(t, err)
	validateDistribution(t, p, 1.01)

	i3 := placement.NewEmptyInstance("i3", "r3", "z1", "endpoint", 1)

	p1, err := a.RemoveInstances(p, []string{i3.ID()})
	assert.Error(t, err)
	assert.Nil(t, p1)
	require.NoError(t, placement.Validate(p))
}

func TestReplaceAbsentInstance(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)

	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)

	instances := []placement.Instance{i1, i2}

	ids := make([]uint32, 1024)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids, 1)
	require.NoError(t, err)
	validateDistribution(t, p, 1.01)

	i3 := placement.NewEmptyInstance("i3", "r3", "z1", "endpoint", 1)
	i4 := placement.NewEmptyInstance("i4", "r4", "z1", "endpoint", 1)

	p1, err := a.ReplaceInstances(p, []string{i3.ID()}, []placement.Instance{i4})
	assert.Error(t, err)
	assert.Nil(t, p1)
	require.NoError(t, placement.Validate(p))
}

func TestInitWithTransitionalShardStates(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "", "e1", 1)
	i2 := placement.NewEmptyInstance("i2", "r2", "", "e2", 1)
	instances := []placement.Instance{i1, i2}

	numShards := 4
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids, 1)
	require.NoError(t, err)
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
		assert.Equal(t, instance.Shards().NumShards(), instance.Shards().NumShardsForState(shard.Initializing))
	}
}

func TestInitWithStableShardStates(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "", "e1", 1)
	i2 := placement.NewEmptyInstance("i2", "r2", "", "e2", 1)
	instances := []placement.Instance{i1, i2}

	numShards := 4
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := newShardedAlgorithm(placement.NewOptions().SetShardStateMode(placement.StableShardStateOnly))
	p, err := a.InitialPlacement(instances, ids, 1)
	require.NoError(t, err)
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
		assert.Equal(t, instance.Shards().NumShards(), instance.Shards().NumShardsForState(shard.Available))
	}
}

func TestAddReplica(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "", "e1", 1)
	i1.Shards().Add(shard.NewShard(0).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))

	i2 := placement.NewEmptyInstance("i2", "r2", "", "e2", 1)
	i2.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	instances := []placement.Instance{i1, i2}

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
	require.NoError(t, err)
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

	instances := []placement.Instance{i1}

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
	p, err := a.AddInstances(p, []placement.Instance{i2})
	require.NoError(t, err)
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

	instances := []placement.Instance{i1, i2}

	ids := make([]uint32, 10)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids, 1)
	require.NoError(t, err)

	p1, err := a.AddInstances(p, []placement.Instance{i2})
	assert.Error(t, err)
	assert.Nil(t, p1)
	require.NoError(t, placement.Validate(p))
}

func TestAddInstance_ExistAndLeaving(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)

	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)

	instances := []placement.Instance{i1, i2}

	ids := make([]uint32, 10)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.InitialPlacement(instances, ids, 1)
	require.NoError(t, err)

	p, updated := mustMarkAllShardsAsAvailable(t, p, placement.NewOptions())
	require.True(t, updated)

	p, err = a.RemoveInstances(p, []string{"i2"})
	require.NoError(t, err)

	i2, ok := p.Instance("i2")
	assert.True(t, ok)
	assert.True(t, i2.IsLeaving())

	p, err = a.AddInstances(p, []placement.Instance{i2})
	require.NoError(t, err)
	require.NoError(t, placement.Validate(p))
}

func TestAddInstance_ExistAndLeavingIsolationGroupConflict(t *testing.T) {
	i1 := placement.NewInstance().SetID("i1").SetEndpoint("e1").SetIsolationGroup("r1").SetZone("z1").SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Leaving),
			shard.NewShard(1).SetState(shard.Leaving),
			shard.NewShard(3).SetState(shard.Leaving),
		}))
	i2 := placement.NewInstance().SetID("i2").SetEndpoint("e2").SetIsolationGroup("r1").SetZone("z2").SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Available),
			shard.NewShard(1).SetState(shard.Available),
		}))
	i3 := placement.NewInstance().SetID("i3").SetEndpoint("e3").SetIsolationGroup("r3").SetZone("z3").SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(2).SetState(shard.Available),
			shard.NewShard(3).SetState(shard.Available),
		}))
	i4 := placement.NewInstance().SetID("i4").SetEndpoint("e4").SetIsolationGroup("r4").SetZone("z4").SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i1"),
			shard.NewShard(1).SetState(shard.Initializing).SetSourceID("i1"),
			shard.NewShard(2).SetState(shard.Initializing).SetSourceID("i1"),
			shard.NewShard(3).SetState(shard.Initializing).SetSourceID("i1"),
		}))

	p := placement.NewPlacement().
		SetInstances([]placement.Instance{i1, i2, i3, i4}).
		SetIsSharded(true).
		SetReplicaFactor(2).
		SetShards([]uint32{0, 1, 2, 3})

	a := newShardedAlgorithm(placement.NewOptions())
	p, err := a.AddInstances(p, []placement.Instance{i1})
	require.NoError(t, err)
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

	instances := []placement.Instance{i1, i2}

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
	p, err := a.RemoveInstances(p, []string{i2.ID()})
	require.NoError(t, err)
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

	instances := []placement.Instance{i1, i2}

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
	p, err := a.ReplaceInstances(p, []string{i2.ID()}, []placement.Instance{i3})
	require.NoError(t, err)
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

	instances := []placement.Instance{i1, i2}

	p := placement.NewPlacement().
		SetInstances(instances).
		SetShards([]uint32{0, 1}).
		SetReplicaFactor(1).
		SetIsSharded(true)

	a := newShardedAlgorithm(placement.NewOptions())
	i3 := placement.NewEmptyInstance("i3", "r3", "", "e3", 1)
	p, err := a.ReplaceInstances(p, []string{i2.ID()}, []placement.Instance{i3})
	require.NoError(t, err)
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

	p, err = a.AddInstances(p, []placement.Instance{i2})
	require.NoError(t, err)
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

func TestIncompatibleWithShardedAlgo(t *testing.T) {
	i1 := placement.NewInstance().SetID("i1").SetEndpoint("e1")
	i2 := placement.NewInstance().SetID("i2").SetEndpoint("e2")
	i3 := placement.NewInstance().SetID("i3").SetEndpoint("e3")
	i4 := placement.NewInstance().SetID("i4").SetEndpoint("e4")

	p, err := newNonShardedAlgorithm().InitialPlacement([]placement.Instance{i1, i2}, []uint32{}, 1)
	require.NoError(t, err)

	a := newShardedAlgorithm(placement.NewOptions())
	_, err = a.AddReplica(p)
	assert.Error(t, err)
	assert.Equal(t, errIncompatibleWithShardedAlgo, err)

	_, err = a.AddInstances(p, []placement.Instance{i3})
	assert.Error(t, err)
	assert.Equal(t, errIncompatibleWithShardedAlgo, err)

	_, err = a.RemoveInstances(p, []string{"i1"})
	assert.Error(t, err)
	assert.Equal(t, errIncompatibleWithShardedAlgo, err)

	_, err = a.ReplaceInstances(p, []string{"i1"}, []placement.Instance{i3, i4})
	assert.Error(t, err)
	assert.Equal(t, errIncompatibleWithShardedAlgo, err)

	_, err = a.MarkShardsAvailable(p, "i2", 0)
	assert.Error(t, err)
	assert.Equal(t, errIncompatibleWithShardedAlgo, err)
}

func TestMarkShardAsAvailableWithShardedAlgo(t *testing.T) {
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

	a := newShardedAlgorithm(placement.NewOptions().
		SetIsShardCutoverFn(genShardCutoverFn(tenMinutesInThePast)).
		SetIsShardCutoffFn(genShardCutoffFn(tenMinutesInThePast, time.Hour)))
	_, err := a.MarkShardsAvailable(p, "i2", 0)
	assert.Error(t, err)

	a = newShardedAlgorithm(placement.NewOptions().
		SetIsShardCutoverFn(genShardCutoverFn(tenMinutesInTheFuture)).
		SetIsShardCutoffFn(genShardCutoffFn(tenMinutesInTheFuture, time.Hour)))
	_, err = a.MarkShardsAvailable(p, "i2", 0)
	assert.Error(t, err)

	a = newShardedAlgorithm(placement.NewOptions().
		SetIsShardCutoverFn(genShardCutoverFn(oneHourInTheFuture)).
		SetIsShardCutoffFn(genShardCutoffFn(oneHourInTheFuture, time.Hour)))
	p, err = a.MarkShardsAvailable(p, "i2", 0)
	assert.NoError(t, err)
	assert.NoError(t, placement.Validate(p))
}

func TestMarkShardAsAvailableBulkWithShardedAlgo(t *testing.T) {
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

	a := newShardedAlgorithm(placement.NewOptions().
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

func TestGoodCaseWithSimpleShardStateType(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i2 := placement.NewEmptyInstance("i2", "r1", "z1", "endpoint", 1)
	i3 := placement.NewEmptyInstance("i3", "r2", "z1", "endpoint", 1)
	i4 := placement.NewEmptyInstance("i4", "r2", "z1", "endpoint", 1)
	i5 := placement.NewEmptyInstance("i5", "r3", "z1", "endpoint", 1)
	i6 := placement.NewEmptyInstance("i6", "r4", "z1", "endpoint", 1)
	i7 := placement.NewEmptyInstance("i7", "r5", "z1", "endpoint", 1)
	i8 := placement.NewEmptyInstance("i8", "r6", "z1", "endpoint", 1)
	i9 := placement.NewEmptyInstance("i9", "r7", "z1", "endpoint", 1)

	instances := []placement.Instance{i1, i2, i3, i4, i5, i6, i7, i8, i9}

	numShards := 1024
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	opts := placement.NewOptions().
		SetPlacementCutoverNanosFn(timeNanosGen(1)).
		SetShardCutoverNanosFn(timeNanosGen(2)).
		SetShardCutoffNanosFn(timeNanosGen(3)).
		SetShardStateMode(placement.StableShardStateOnly)
	a := newShardedAlgorithm(opts)
	p, err := a.InitialPlacement(instances, ids, 1)
	require.NoError(t, err)
	verifyAllShardsInAvailableState(t, p)

	p, err = a.AddInstances(p, []placement.Instance{placement.NewEmptyInstance("i21", "r6", "z1", "endpoint", 1)})
	require.NoError(t, err)
	verifyAllShardsInAvailableState(t, p)

	p, err = a.RemoveInstances(p, []string{i1.ID()})
	require.NoError(t, err)
	verifyAllShardsInAvailableState(t, p)

	i12 := placement.NewEmptyInstance("i12", "r3", "z1", "endpoint", 1)
	p, err = a.ReplaceInstances(p, []string{i5.ID()}, []placement.Instance{i12})
	require.NoError(t, err)
	verifyAllShardsInAvailableState(t, p)

	p, err = a.AddReplica(p)
	require.NoError(t, err)
	verifyAllShardsInAvailableState(t, p)

	p, err = a.BalanceShards(p)
	require.NoError(t, err)
	verifyAllShardsInAvailableState(t, p)
}

func TestBalanceShardsForShardedWhenBalanced(t *testing.T) {
	i1 := newTestInstance("i1").
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Available),
		}))
	i2 := newTestInstance("i2").
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Available),
		}))
	i3 := newTestInstance("i3").
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(1).SetState(shard.Available),
		}))
	i4 := newTestInstance("i4").
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(1).SetState(shard.Available),
		}))
	initialPlacement := placement.NewPlacement().
		SetReplicaFactor(2).
		SetShards([]uint32{0, 1}).
		SetInstances([]placement.Instance{i1, i2, i3, i4}).
		SetIsSharded(true)

	expectedPlacement := initialPlacement.Clone()

	a := NewAlgorithm(placement.NewOptions())

	balancedPlacement, err := a.BalanceShards(initialPlacement)
	assert.NoError(t, err)
	assert.Equal(t, expectedPlacement, balancedPlacement)
}

func TestBalanceShardsForShardedWhenImbalanced(t *testing.T) {
	i1 := newTestInstance("i1").
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Available),
			shard.NewShard(1).SetState(shard.Available),
		}))
	i2 := newTestInstance("i2").
		SetWeight(2).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(2).SetState(shard.Available),
		}))
	p := placement.NewPlacement().
		SetReplicaFactor(1).
		SetShards([]uint32{0, 1, 2}).
		SetInstances([]placement.Instance{i1, i2}).
		SetIsSharded(true)

	a := NewAlgorithm(placement.NewOptions())

	balancedPlacement, err := a.BalanceShards(p)
	assert.NoError(t, err)

	bi1 := newTestInstance("i1").
		SetWeight(1).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Leaving),
			shard.NewShard(1).SetState(shard.Available),
		}))
	bi2 := newTestInstance("i2").
		SetWeight(2).
		SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(0).SetState(shard.Initializing).SetSourceID("i1"),
			shard.NewShard(2).SetState(shard.Available),
		}))
	expectedInstances := []placement.Instance{bi1, bi2}

	assert.Equal(t, expectedInstances, balancedPlacement.Instances())
}

func verifyAllShardsInAvailableState(t *testing.T, p placement.Placement) {
	for _, instance := range p.Instances() {
		s := instance.Shards()
		require.Equal(t, len(s.All()), len(s.ShardsForState(shard.Available)))
		require.Empty(t, s.ShardsForState(shard.Initializing))
		require.Empty(t, s.ShardsForState(shard.Leaving))
	}
}

func mustMarkAllShardsAsAvailable(t *testing.T, p placement.Placement, opts placement.Options) (placement.Placement, bool) {
	if opts == nil {
		opts = placement.NewOptions()
	}
	p, updated, err := markAllShardsAvailable(p, opts)
	require.NoError(t, err)
	return p, updated
}

func validateCutoverCutoffNanos(t *testing.T, p placement.Placement, opts placement.Options) {
	for _, i := range p.Instances() {
		for _, s := range i.Shards().All() {
			switch s.State() {
			case shard.Available:
				assert.Equal(t, shard.DefaultShardCutoverNanos, s.CutoverNanos())
				assert.Equal(t, shard.DefaultShardCutoffNanos, s.CutoffNanos())
			case shard.Initializing:
				assert.Equal(t, opts.ShardCutoverNanosFn()(), s.CutoverNanos())
				assert.Equal(t, shard.DefaultShardCutoffNanos, s.CutoffNanos())
			case shard.Leaving:
				assert.Equal(t, shard.DefaultShardCutoverNanos, s.CutoverNanos())
				assert.Equal(t, opts.ShardCutoffNanosFn()(), s.CutoffNanos())
			case shard.Unknown:
				assert.Fail(t, "invalid shard state")
			}
		}
	}
	assert.Equal(t, opts.PlacementCutoverNanosFn()(), p.CutoverNanos())
}

func validateDistribution(t *testing.T, p placement.Placement, expectPeakOverAvg float64) {
	assert.NoError(t, placement.Validate(p), "placement validation failed")
	ph := NewPlacementHelper(p, placement.NewOptions()).(*helper)
	total := 0
	for _, i := range p.Instances() {
		load := loadOnInstance(i)
		total += load
		avgLoad := getWeightedLoad(ph, i.Weight())
		instanceOverAvg := float64(load) / float64(avgLoad)
		if math.Abs(float64(load-avgLoad)) > 1 {
			assert.True(t, instanceOverAvg <= expectPeakOverAvg, fmt.Sprintf("Bad shard distribution, peak/Avg on %s is too high: %v, expecting %v, load on instance: %v, avg load: %v",
				i.ID(), instanceOverAvg, expectPeakOverAvg, load, avgLoad))
		}

		targetLoad := ph.targetLoadForInstance(i.ID())
		if targetLoad == 0 {
			continue
		}
		instanceOverTarget := float64(load) / float64(targetLoad)
		if math.Abs(float64(load-targetLoad)) > 1 {
			assert.True(t, instanceOverTarget <= 1.03, fmt.Sprintf("Bad shard distribution, peak/Target on %s is too high: %v, load on instance: %v, target load: %v",
				i.ID(), instanceOverTarget, load, targetLoad))
		}
	}
	assert.Equal(t, p.ReplicaFactor()*p.NumShards(), total, fmt.Sprintf("Wrong total shards: expecting %v, but got %v", p.ReplicaFactor()*p.NumShards(), total))
}

func getWeightedLoad(ph *helper, weight uint32) int {
	return ph.rf * len(ph.shardToInstanceMap) * int(weight) / int(ph.totalWeight)
}

func timeNanosGen(v int64) placement.TimeNanosFn {
	return func() int64 {
		return v
	}
}
