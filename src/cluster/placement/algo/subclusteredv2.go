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

	"github.com/m3db/m3/src/cluster/placement"
)

// BySubClusterIDInstanceID is a type that implements sort.Interface for a slice of placement.Instance
// It sorts instances first by subcluster ID, then by instance ID within each subcluster
type BySubClusterIDInstanceID []placement.Instance

func (a BySubClusterIDInstanceID) Len() int { return len(a) }

func (a BySubClusterIDInstanceID) Less(i, j int) bool {
	if a[i].SubClusterID() == a[j].SubClusterID() {
		return a[i].ID() < a[j].ID()
	}
	return a[i].SubClusterID() < a[j].SubClusterID()
}

func (a BySubClusterIDInstanceID) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

type subclusteredv2 struct {
	opts placement.Options
}

func newSubclusteredv2(opts placement.Options) placement.Algorithm {
	return subclusteredv2{opts: opts}
}

func (a subclusteredv2) IsCompatibleWith(p placement.Placement) error {
	if !p.IsSharded() {
		return errIncompatibleWithShardedAlgo
	}

	return nil
}

func (a subclusteredv2) InitialPlacement(
	instances []placement.Instance,
	shards []uint32,
	rf int,
) (placement.Placement, error) {
	if err := assignSubClusterID(nil, a.opts, instances); err != nil {
		return nil, err
	}
	ph := newSubclusteredv2InitHelper(placement.Instances(instances).Clone(), shards, a.opts)
	if err := ph.placeShards(newShards(shards), nil, ph.Instances()); err != nil {
		return nil, err
	}

	var (
		p   = ph.generatePlacement()
		err error
	)
	for i := 1; i < rf; i++ {
		p, err = a.AddReplica(p)
		if err != nil {
			return nil, err
		}
	}

	return tryCleanupShardState(p, a.opts)
}

// nolint: dupl
func (a subclusteredv2) AddReplica(p placement.Placement) (placement.Placement, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}

	p = p.Clone()
	ph := newubclusteredv2ReplicaHelper(p, a.opts)
	if err := ph.placeShards(newShards(p.Shards()), nil, nonLeavingInstances(ph.Instances())); err != nil {
		return nil, err
	}

	if err := ph.optimize(safe); err != nil {
		return nil, err
	}

	return tryCleanupShardState(ph.generatePlacement(), a.opts)
}

func (a subclusteredv2) RemoveInstances(
	p placement.Placement,
	instanceIDs []string,
) (placement.Placement, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}

	p = p.Clone()
	for _, instanceID := range instanceIDs {
		ph, leavingInstance, err := newubclusteredv2RemoveInstanceHelper(p, instanceID, a.opts)
		if err != nil {
			return nil, err
		}
		// place the shards from the leaving instance to the rest of the cluster
		if err := ph.placeShards(leavingInstance.Shards().All(), leavingInstance, ph.Instances()); err != nil {
			return nil, err
		}

		if err := ph.optimize(safe); err != nil {
			return nil, err
		}

		if p, _, err = addInstanceToPlacement(ph.generatePlacement(), leavingInstance, withShards); err != nil {
			return nil, err
		}
	}
	return tryCleanupShardState(p, a.opts)
}

func (a subclusteredv2) AddInstances(
	p placement.Placement,
	instances []placement.Instance,
) (placement.Placement, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}

	p = p.Clone()
	for _, instance := range instances {
		ph, addingInstance, err := newubclusteredv2AddInstanceHelper(p, instance, a.opts, withLeavingShardsOnly)
		if err != nil {
			return nil, err
		}

		if err := ph.addInstance(addingInstance); err != nil {
			return nil, err
		}

		p = ph.generatePlacement()
	}

	return tryCleanupShardState(p, a.opts)
}

// nolint: dupl
func (a subclusteredv2) ReplaceInstances(
	p placement.Placement,
	leavingInstanceIDs []string,
	addingInstances []placement.Instance,
) (placement.Placement, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}

	p = p.Clone()
	ph, leavingInstances, addingInstances, err := newubclusteredv2ReplaceInstanceHelper(p, leavingInstanceIDs, addingInstances, a.opts)
	if err != nil {
		return nil, err
	}

	for _, leavingInstance := range leavingInstances {
		err = ph.placeShards(leavingInstance.Shards().All(), leavingInstance, addingInstances)
		// nolint: errorlint
		if err != nil && err != errNotEnoughIsolationGroups {
			// errNotEnoughIsolationGroups means the adding instances do not
			// have enough isolation groups to take all the shards, but the rest
			// instances might have more isolation groups to take all the shards.
			return nil, err
		}
		load := loadOnInstance(leavingInstance)
		if load != 0 && !a.opts.AllowPartialReplace() {
			return nil, fmt.Errorf("could not fully replace all shards from %s, %d shards left unassigned",
				leavingInstance.ID(), load)
		}
	}

	if a.opts.AllowPartialReplace() {
		// Place the shards left on the leaving instance to the rest of the cluster.
		for _, leavingInstance := range leavingInstances {
			if err = ph.placeShards(leavingInstance.Shards().All(), leavingInstance, ph.Instances()); err != nil {
				return nil, err
			}
		}

		// if err := ph.optimize(unsafe); err != nil {
		// 	return nil, err
		// }
	}

	p = ph.generatePlacement()
	for _, leavingInstance := range leavingInstances {
		if p, _, err = addInstanceToPlacement(p, leavingInstance, withShards); err != nil {
			return nil, err
		}
	}
	return tryCleanupShardState(p, a.opts)
}

func (a subclusteredv2) MarkShardsAvailable(
	p placement.Placement,
	instanceID string,
	shardIDs ...uint32,
) (placement.Placement, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}

	return markShardsAvailable(p.Clone(), instanceID, shardIDs, a.opts)
}

func (a subclusteredv2) MarkAllShardsAvailable(
	p placement.Placement,
) (placement.Placement, bool, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, false, err
	}

	return markAllShardsAvailable(p, a.opts)
}

func (a subclusteredv2) BalanceShards(
	p placement.Placement,
) (placement.Placement, error) {
	ph := newHelper(p, p.ReplicaFactor(), a.opts)
	if err := ph.optimize(unsafe); err != nil {
		return nil, fmt.Errorf("shard balance optimization failed: %w", err)
	}

	return tryCleanupShardState(ph.generatePlacement(), a.opts)
}

func assignSubClusterID(currPlacement placement.Placement, opts placement.Options, instances []placement.Instance) error {
	instancesPerSubcluster := opts.InstancesPerSubCluster()
	if instancesPerSubcluster <= 0 {
		return fmt.Errorf("instances per subcluster is not set")
	}

	// If current placement is nil, start assigning from subcluster 1
	nextSubclusterID := uint32(1)
	instancesInCurrentSubcluster := 0
	if currPlacement != nil {
		currInstances := currPlacement.Instances()
		sort.Sort(BySubClusterIDInstanceID(currInstances))

		// Count instances in each subcluster
		subclusterCounts := make(map[uint32]int)
		for _, instance := range currInstances {
			subclusterCounts[instance.SubClusterID()]++
		}

		// Find the first subcluster that isn't full
		for subclusterID := uint32(1); ; subclusterID++ {
			count := subclusterCounts[subclusterID]
			if count < instancesPerSubcluster {
				nextSubclusterID = subclusterID
				instancesInCurrentSubcluster = count
				break
			}
		}
	}

	// Assign subcluster IDs to new instances

	for _, instance := range instances {
		instance.SetSubClusterID(nextSubclusterID)
		instancesInCurrentSubcluster++

		// If we've filled the current subcluster, move to the next one
		if instancesInCurrentSubcluster >= instancesPerSubcluster {
			nextSubclusterID++
			instancesInCurrentSubcluster = 0
		}
	}
	return nil
}
