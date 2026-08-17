// Copyright (c) 2018 Uber Technologies, Inc.
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

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
)

// Draining an instance out of a subcluster in batches of shards keeps the rest
// of the subcluster serving: only the shards of the instance being drained are
// marked as leaving, so every other replica of those shards stays available.
// The tests below drive such a drain through the placement algorithm and
// validate the placement after every step, which is what a caller persisting
// each step to the placement store does.

func TestSubclusteredAlgorithm_DrainInstanceInBatches(t *testing.T) {
	tests := []struct {
		name                   string
		instancesPerSubcluster int
		replicaFactor          int
		subClusters            int
		totalShards            int
		batchSize              int
	}{
		{
			// The subcluster cannot keep any of its shards once an instance leaves,
			// so all of them move to the other subcluster one batch at a time.
			name:                   "subcluster has one instance per isolation group",
			instancesPerSubcluster: 3,
			replicaFactor:          3,
			subClusters:            2,
			totalShards:            32,
			batchSize:              5,
		},
		{
			name:                   "subcluster has two instances per isolation group",
			instancesPerSubcluster: 6,
			replicaFactor:          3,
			subClusters:            2,
			totalShards:            64,
			batchSize:              8,
		},
		{
			name:                   "single shard batches",
			instancesPerSubcluster: 3,
			replicaFactor:          3,
			subClusters:            3,
			totalShards:            18,
			batchSize:              1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			algo := newSubclusteredTestAlgorithm(tt.instancesPerSubcluster)
			p := newAvailableSubclusteredPlacement(t, algo, tt.instancesPerSubcluster,
				tt.replicaFactor, tt.subClusters, tt.totalShards)

			drainingID := p.Instances()[0].ID()
			batches := drainInstanceInBatches(t, algo, p, drainingID, tt.batchSize)
			require.NotZero(t, batches)
		})
	}
}

// TestSubclusteredAlgorithm_DrainInstanceFromDrainedSubcluster drains a second
// instance out of a subcluster that has already given a part of its shards to
// another subcluster, which is what happens when more than one instance of a
// subcluster is decommissioned one after the other.
func TestSubclusteredAlgorithm_DrainInstanceFromDrainedSubcluster(t *testing.T) {
	const (
		instancesPerSubcluster = 3
		replicaFactor          = 3
		subClusters            = 2
		totalShards            = 32
		batchSize              = 5
	)

	algo := newSubclusteredTestAlgorithm(instancesPerSubcluster)
	p := newAvailableSubclusteredPlacement(t, algo, instancesPerSubcluster,
		replicaFactor, subClusters, totalShards)

	first := p.Instances()[0]
	subClusterID := first.SubClusterID()
	p = drainInstanceInBatches(t, algo, p, first.ID(), batchSize)

	var second placement.Instance
	for _, instance := range placement.BySubClusterIDThenInstanceID(p.Instances()) {
		if instance.SubClusterID() == subClusterID {
			second = instance
			break
		}
	}
	require.NotNil(t, second)

	p = drainInstanceInBatches(t, algo, p, second.ID(), batchSize)
	require.NoError(t, placement.Validate(p))
}

// TestSubclusteredAlgorithm_DrainInstanceWhileAnotherSubclusterIsPartial verifies
// that draining an instance is rejected while another subcluster is partial. A
// placement can hold at most one partial subcluster, so drains of instances of
// different subclusters have to be done one after the other.
func TestSubclusteredAlgorithm_DrainInstanceWhileAnotherSubclusterIsPartial(t *testing.T) {
	const (
		instancesPerSubcluster = 6
		replicaFactor          = 3
		subClusters            = 3
		totalShards            = 64
		batchSize              = 5
	)

	algo := newSubclusteredTestAlgorithm(instancesPerSubcluster)
	p := newAvailableSubclusteredPlacement(t, algo, instancesPerSubcluster,
		replicaFactor, subClusters, totalShards)

	drained := p.Instances()[0]
	p = drainInstanceInBatches(t, algo, p, drained.ID(), batchSize)

	var other placement.Instance
	for _, instance := range placement.BySubClusterIDThenInstanceID(p.Instances()) {
		if instance.SubClusterID() != drained.SubClusterID() {
			other = instance
			break
		}
	}
	require.NotNil(t, other)

	_, err := algo.RemoveInstances(p.Clone(), []string{other.ID()})
	require.Error(t, err)
	require.Contains(t, err.Error(), "partial subcluster")
}

// TestSubclusteredAlgorithm_DrainInstanceHoldingShardsOfRemovedSubcluster drains
// an instance of a subcluster that took over the shards of a subcluster that was
// removed from the placement, so the instance holds more shards than the
// instances of a balanced placement do.
func TestSubclusteredAlgorithm_DrainInstanceHoldingShardsOfRemovedSubcluster(t *testing.T) {
	const (
		instancesPerSubcluster = 3
		replicaFactor          = 3
		subClusters            = 3
		totalShards            = 36
		batchSize              = 5
	)

	algo := newSubclusteredTestAlgorithm(instancesPerSubcluster)
	p := newAvailableSubclusteredPlacement(t, algo, instancesPerSubcluster,
		replicaFactor, subClusters, totalShards)

	removedSubClusterID := p.Instances()[0].SubClusterID()
	var instanceIDs []string
	for _, instance := range placement.BySubClusterIDThenInstanceID(p.Instances()) {
		if instance.SubClusterID() == removedSubClusterID {
			instanceIDs = append(instanceIDs, instance.ID())
		}
	}

	p, err := algo.RemoveInstances(p, instanceIDs)
	require.NoError(t, err)
	require.NoError(t, placement.Validate(p))
	p, _, err = algo.MarkAllShardsAvailable(p)
	require.NoError(t, err)
	require.NoError(t, placement.Validate(p))

	var holder placement.Instance
	for _, instance := range placement.BySubClusterIDThenInstanceID(p.Instances()) {
		if instance.SubClusterID() != removedSubClusterID {
			holder = instance
			break
		}
	}
	require.NotNil(t, holder)

	p = drainInstanceInBatches(t, algo, p, holder.ID(), batchSize)
	require.NoError(t, placement.Validate(p))
}

// drainInstanceInBatches moves the shards of the given instance to other
// instances in batches of at most batchSize shards, one destination instance per
// batch, until the instance is drained and removed from the placement. Each
// batch is planned by simulating the removal of the instance and replaying only
// the shard movements of the instance being drained. The placement is validated
// after applying a batch and after marking the batch available, and the shards
// of every other instance are checked to never be left in leaving state.
func drainInstanceInBatches(
	t *testing.T,
	algo placement.Algorithm,
	p placement.Placement,
	drainingID string,
	batchSize int,
) placement.Placement {
	t.Helper()

	batches := 0
	for {
		draining, exists := p.Instance(drainingID)
		if !exists {
			break
		}
		require.Less(t, batches, p.NumShards()+1, "the drain does not make progress")

		simulated, err := algo.RemoveInstances(p.Clone(), []string{drainingID})
		require.NoError(t, err)

		destinationID, shardIDs := planShardBatch(t, simulated, drainingID, batchSize)
		require.NotEmpty(t, shardIDs)
		require.LessOrEqual(t, len(shardIDs), batchSize)

		destination, exists := p.Instance(destinationID)
		require.True(t, exists)
		require.Equal(t, draining.IsolationGroup(), destination.IsolationGroup(),
			"a shard can only move to an instance of the same isolation group")

		for _, shardID := range shardIDs {
			leaving, exists := draining.Shards().Shard(shardID)
			require.True(t, exists)
			require.Equal(t, shard.Available, leaving.State())

			leaving.SetState(shard.Leaving).SetCutoffNanos(shard.UnInitializedValue)
			destination.Shards().Add(shard.NewShard(shardID).
				SetState(shard.Initializing).
				SetSourceID(drainingID))
		}

		require.NoError(t, placement.Validate(p), "batch %d in flight", batches)
		requireOnlyInstanceHasLeavingShards(t, p, drainingID)

		p, err = algo.MarkShardsAvailable(p, destinationID, shardIDs...)
		require.NoError(t, err)
		require.NoError(t, placement.Validate(p), "batch %d marked available", batches)
		requireOnlyInstanceHasLeavingShards(t, p, drainingID)

		batches++
	}

	require.NotZero(t, batches)
	// The placement algorithm removes an instance from the placement when its
	// last shard is marked available, so no explicit removal is needed.
	_, exists := p.Instance(drainingID)
	require.False(t, exists)

	return p
}

// planShardBatch returns the shards of a single destination instance that the
// simulated removal assigned away from the instance being drained, capped at
// batchSize shards.
func planShardBatch(
	t *testing.T,
	simulated placement.Placement,
	drainingID string,
	batchSize int,
) (string, []uint32) {
	t.Helper()

	shardsByDestination := make(map[string][]uint32)
	for _, instance := range simulated.Instances() {
		if instance.ID() == drainingID {
			continue
		}
		for _, s := range instance.Shards().All() {
			if s.State() == shard.Initializing && s.SourceID() == drainingID {
				shardsByDestination[instance.ID()] = append(shardsByDestination[instance.ID()], s.ID())
			}
		}
	}
	require.NotEmpty(t, shardsByDestination)

	destinationIDs := make([]string, 0, len(shardsByDestination))
	for destinationID := range shardsByDestination {
		destinationIDs = append(destinationIDs, destinationID)
	}
	sort.Strings(destinationIDs)

	destinationID := destinationIDs[0]
	shardIDs := shardsByDestination[destinationID]
	sort.Slice(shardIDs, func(i, j int) bool { return shardIDs[i] < shardIDs[j] })
	if len(shardIDs) > batchSize {
		shardIDs = shardIDs[:batchSize]
	}

	return destinationID, shardIDs
}

func requireOnlyInstanceHasLeavingShards(t *testing.T, p placement.Placement, instanceID string) {
	t.Helper()

	for _, instance := range p.Instances() {
		if instance.ID() == instanceID {
			continue
		}
		require.Zero(t, instance.Shards().NumShardsForState(shard.Leaving),
			"instance %s should not have leaving shards", instance.ID())
	}
}

func instanceHoldsShardOfSubCluster(
	p placement.Placement,
	instance placement.Instance,
	subClusterID uint32,
) bool {
	for _, s := range instance.Shards().All() {
		for _, other := range p.Instances() {
			if other.SubClusterID() != subClusterID {
				continue
			}
			if _, exists := other.Shards().Shard(s.ID()); exists {
				return true
			}
		}
	}
	return false
}

func newSubclusteredTestAlgorithm(instancesPerSubcluster int) placement.Algorithm {
	return subclusteredPlacementAlgorithm{
		opts: placement.NewOptions().
			SetInstancesPerSubCluster(instancesPerSubcluster).
			SetIsSubclustered(true),
	}
}

func newAvailableSubclusteredPlacement(
	t *testing.T,
	algo placement.Algorithm,
	instancesPerSubcluster, replicaFactor, subClusters, totalShards int,
) placement.Placement {
	t.Helper()

	instances := make([]placement.Instance, 0, instancesPerSubcluster*subClusters)
	for i := 0; i < instancesPerSubcluster*subClusters; i++ {
		instances = append(instances, placement.NewInstance().
			SetID(fmt.Sprintf("I%03d", i)).
			SetIsolationGroup(fmt.Sprintf("R%d", i%replicaFactor)).
			SetWeight(1).
			SetEndpoint(fmt.Sprintf("E%03d", i)).
			SetShards(shard.NewShards(nil)))
	}

	shardIDs := make([]uint32, totalShards)
	for i := range shardIDs {
		shardIDs[i] = uint32(i)
	}

	p, err := algo.InitialPlacement(instances, shardIDs, replicaFactor)
	require.NoError(t, err)

	p, _, err = algo.MarkAllShardsAvailable(p)
	require.NoError(t, err)
	require.NoError(t, placement.Validate(p))

	return p
}
