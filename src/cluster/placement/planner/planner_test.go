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

package planner

import (
	"sort"
	"testing"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"

	"github.com/stretchr/testify/assert"
)

func TestDeployment(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(4).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(5).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(6).SetState(shard.Available))

	i3 := placement.NewEmptyInstance("i3", "r3", "z1", "endpoint", 1)
	i3.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i3.Shards().Add(shard.NewShard(3).SetState(shard.Available))
	i3.Shards().Add(shard.NewShard(5).SetState(shard.Available))

	i4 := placement.NewEmptyInstance("i4", "r4", "z1", "endpoint", 1)
	i4.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i4.Shards().Add(shard.NewShard(4).SetState(shard.Available))
	i4.Shards().Add(shard.NewShard(6).SetState(shard.Available))

	i5 := placement.NewEmptyInstance("i5", "r5", "z1", "endpoint", 1)
	i5.Shards().Add(shard.NewShard(5).SetState(shard.Available))
	i5.Shards().Add(shard.NewShard(6).SetState(shard.Available))
	i5.Shards().Add(shard.NewShard(1).SetState(shard.Available))

	i6 := placement.NewEmptyInstance("i6", "r6", "z1", "endpoint", 1)
	i6.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i6.Shards().Add(shard.NewShard(3).SetState(shard.Available))
	i6.Shards().Add(shard.NewShard(4).SetState(shard.Available))

	instances := []placement.Instance{i1, i2, i3, i4, i5, i6}

	mp := placement.NewPlacement().
		SetInstances(instances).
		SetShards([]uint32{1, 2, 3, 4, 5, 6}).
		SetReplicaFactor(3)

	dp := NewShardAwareDeploymentPlanner(placement.NewDeploymentOptions())
	steps := dp.DeploymentSteps(mp)
	total := 0
	for _, step := range steps {
		total += len(step)
	}
	assert.Equal(t, total, 6)
	assert.True(t, len(steps) == 3)
}

func TestDeterministicSteps(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Available))

	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(3).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(4).SetState(shard.Available))

	i3 := placement.NewEmptyInstance("i3", "r3", "z1", "endpoint", 1)
	i3.Shards().Add(shard.NewShard(5).SetState(shard.Available))
	i3.Shards().Add(shard.NewShard(6).SetState(shard.Available))

	i4 := placement.NewEmptyInstance("i4", "r4", "z1", "endpoint", 1)
	i4.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i4.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	i5 := placement.NewEmptyInstance("i5", "r5", "z1", "endpoint", 1)
	i5.Shards().Add(shard.NewShard(4).SetState(shard.Available))
	i5.Shards().Add(shard.NewShard(6).SetState(shard.Available))

	i6 := placement.NewEmptyInstance("i6", "r6", "z1", "endpoint", 1)
	i6.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i6.Shards().Add(shard.NewShard(5).SetState(shard.Available))

	h7 := placement.NewEmptyInstance("i7", "r7", "z1", "endpoint", 1)
	h7.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	h7.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	h8 := placement.NewEmptyInstance("i8", "r8", "z1", "endpoint", 1)
	h8.Shards().Add(shard.NewShard(4).SetState(shard.Available))
	h8.Shards().Add(shard.NewShard(5).SetState(shard.Available))

	h9 := placement.NewEmptyInstance("i9", "r9", "z1", "endpoint", 1)
	h9.Shards().Add(shard.NewShard(6).SetState(shard.Available))
	h9.Shards().Add(shard.NewShard(1).SetState(shard.Available))

	mp1 := placement.NewPlacement().
		SetInstances([]placement.Instance{i1, i2, i3, i4, i5, i6, h7, h8, h9}).
		SetShards([]uint32{1, 2, 3, 4, 5, 6}).
		SetReplicaFactor(3)

	mp2 := placement.NewPlacement().
		SetInstances([]placement.Instance{i4, i5, i6, h7, h8, h9, i1, i2, i3}).
		SetShards([]uint32{1, 2, 3, 4, 5, 6}).
		SetReplicaFactor(3)

	dp := NewShardAwareDeploymentPlanner(placement.NewDeploymentOptions())
	assert.Equal(t, dp.DeploymentSteps(mp1), dp.DeploymentSteps(mp2))
}

func TestDeploymentWithThreeReplica(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "endpoint", 1)
	i1.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i1.Shards().Add(shard.NewShard(2).SetState(shard.Available))

	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "endpoint", 1)
	i2.Shards().Add(shard.NewShard(3).SetState(shard.Available))
	i2.Shards().Add(shard.NewShard(4).SetState(shard.Available))

	i3 := placement.NewEmptyInstance("i3", "r3", "z1", "endpoint", 1)
	i3.Shards().Add(shard.NewShard(5).SetState(shard.Available))
	i3.Shards().Add(shard.NewShard(6).SetState(shard.Available))

	i4 := placement.NewEmptyInstance("i4", "r4", "z1", "endpoint", 1)
	i4.Shards().Add(shard.NewShard(1).SetState(shard.Available))
	i4.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	i5 := placement.NewEmptyInstance("i5", "r5", "z1", "endpoint", 1)
	i5.Shards().Add(shard.NewShard(4).SetState(shard.Available))
	i5.Shards().Add(shard.NewShard(6).SetState(shard.Available))

	i6 := placement.NewEmptyInstance("i6", "r6", "z1", "endpoint", 1)
	i6.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	i6.Shards().Add(shard.NewShard(5).SetState(shard.Available))

	h7 := placement.NewEmptyInstance("i7", "r7", "z1", "endpoint", 1)
	h7.Shards().Add(shard.NewShard(2).SetState(shard.Available))
	h7.Shards().Add(shard.NewShard(3).SetState(shard.Available))

	h8 := placement.NewEmptyInstance("i8", "r8", "z1", "endpoint", 1)
	h8.Shards().Add(shard.NewShard(4).SetState(shard.Available))
	h8.Shards().Add(shard.NewShard(5).SetState(shard.Available))

	h9 := placement.NewEmptyInstance("i9", "r9", "z1", "endpoint", 1)
	h9.Shards().Add(shard.NewShard(6).SetState(shard.Available))
	h9.Shards().Add(shard.NewShard(1).SetState(shard.Available))

	instances := []placement.Instance{i1, i2, i3, i4, i5, i6, h7, h8, h9}

	mp := placement.NewPlacement().
		SetInstances(instances).
		SetShards([]uint32{1, 2, 3, 4, 5, 6}).
		SetReplicaFactor(3)

	dp := NewShardAwareDeploymentPlanner(placement.NewDeploymentOptions())
	steps := dp.DeploymentSteps(mp)
	total := 0
	for _, step := range steps {
		total += len(step)
	}
	assert.Equal(t, total, 9)
	assert.True(t, len(steps) == 3)

	dp = NewShardAwareDeploymentPlanner(placement.NewDeploymentOptions().SetMaxStepSize(2))
	steps = dp.DeploymentSteps(mp)
	total = 0
	for _, step := range steps {
		total += len(step)
	}
	assert.Equal(t, total, 9)
	assert.True(t, len(steps) == 5)

	dp = NewShardAwareDeploymentPlanner(placement.NewDeploymentOptions().SetMaxStepSize(1))
	steps = dp.DeploymentSteps(mp)
	total = 0
	for _, step := range steps {
		total += len(step)
	}
	assert.Equal(t, total, 9)
	assert.True(t, len(steps) == 9)
}

func TestRemoveInstance(t *testing.T) {
	i1 := placement.NewEmptyInstance("i1", "r1", "z1", "", 1)
	i2 := placement.NewEmptyInstance("i2", "r2", "z1", "", 1)
	i3 := placement.NewEmptyInstance("i3", "r3", "z1", "", 1)
	i4 := placement.NewEmptyInstance("i4", "r4", "z1", "", 1)

	instances := []placement.Instance{i1, i2, i3, i4}

	left := removeInstance(instances, i4)
	assert.Equal(t, 3, len(left))

	left = removeInstance(instances, placement.NewEmptyInstance("i5", "r5", "z1", "", 1))
	assert.Equal(t, 4, len(left))
}

func TestSort(t *testing.T) {
	var steps sortableSteps
	steps = append(steps, []placement.Instance{
		placement.NewEmptyInstance("", "", "", "", 1),
		placement.NewEmptyInstance("", "", "", "", 1)})
	steps = append(steps, []placement.Instance{
		placement.NewEmptyInstance("", "", "", "", 1),
		placement.NewEmptyInstance("", "", "", "", 1),
		placement.NewEmptyInstance("", "", "", "", 1)})
	steps = append(steps, []placement.Instance{
		placement.NewEmptyInstance("", "", "", "", 1)})
	sort.Sort(steps)

	assert.Equal(t, 3, len(steps))
	assert.Equal(t, 3, len(steps[0]))
	assert.Equal(t, 2, len(steps[1]))
	assert.Equal(t, 1, len(steps[2]))
}
