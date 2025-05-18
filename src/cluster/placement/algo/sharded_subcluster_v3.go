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
	"github.com/m3db/m3/src/cluster/shard"
)

var (
	errNoInstances            = fmt.Errorf("no instances provided")
	errNoShards               = fmt.Errorf("no shards provided")
	errInvalidReplicaFactor   = fmt.Errorf("invalid replica factor")
	errNotEnoughInstances     = fmt.Errorf("not enough instances for replication")
	errInstanceNotFound       = fmt.Errorf("instance not found")
	errInstanceAlreadyLeaving = fmt.Errorf("instance is already leaving")
	errInstanceNotLeaving     = fmt.Errorf("instance is not leaving")
	errShardNotFound          = fmt.Errorf("shard not found")
	errShardNotInitializing   = fmt.Errorf("shard is not initializing")
	errNotSharded             = fmt.Errorf("placement is not sharded")
	errNotMirrored            = fmt.Errorf("placement is not mirrored")
)

// NewSubclusteredv3Algorithm returns a new subclustered placement algorithm.
func NewSubclusteredv3Algorithm(opts placement.Options) placement.Algorithm {
	return &subclusteredv3Algorithm{opts: opts}
}

type subclusteredv3Algorithm struct {
	opts placement.Options
}

func (a *subclusteredv3Algorithm) InitialPlacement(
	instances []placement.Instance,
	shards []uint32,
	rf int,
) (placement.Placement, error) {
	if len(instances) == 0 {
		return nil, errNoInstances
	}

	if len(shards) == 0 {
		return nil, errNoShards
	}

	if rf <= 0 {
		return nil, errInvalidReplicaFactor
	}

	// Assign subcluster IDs based on InstancesPerSubCluster
	instancesPerSubCluster := a.opts.InstancesPerSubCluster()
	if instancesPerSubCluster <= 0 {
		return nil, fmt.Errorf("instances per subcluster must be greater than 0")
	}

	// Sort instances by isolation group to ensure consistent subcluster assignment
	sort.Slice(instances, func(i, j int) bool {
		return instances[i].IsolationGroup() < instances[j].IsolationGroup()
	})

	// Assign subcluster IDs
	currentSubClusterID := uint32(1)
	instancesInCurrentSubCluster := 0
	for _, instance := range instances {
		instance.SetSubClusterID(currentSubClusterID)
		instancesInCurrentSubCluster++
		if instancesInCurrentSubCluster >= instancesPerSubCluster {
			currentSubClusterID++
			instancesInCurrentSubCluster = 0
		}
	}

	// Validate subcluster assignments
	subClusterCount := make(map[uint32]int)
	for _, instance := range instances {
		subClusterCount[instance.SubClusterID()]++
	}

	// Check if each subcluster has enough instances for replication
	for subClusterID, count := range subClusterCount {
		if count < rf {
			return nil, fmt.Errorf("subcluster %d has %d instances but requires %d for replication",
				subClusterID, count, rf)
		}
	}

	ph := newSubclusteredv3InitHelper(instances, shards, a.opts)
	return ph.generatePlacement(), nil
}

func (a *subclusteredv3Algorithm) AddReplica(
	p placement.Placement,
) (placement.Placement, error) {
	if p.ReplicaFactor() >= len(p.Instances()) {
		return nil, errNotEnoughInstances
	}

	ph := newSubclusteredv3Helper(p, p.ReplicaFactor()+1, a.opts)
	if err := ph.optimize(unsafe); err != nil {
		return nil, err
	}

	return ph.generatePlacement(), nil
}

func (a *subclusteredv3Algorithm) RemoveInstance(
	p placement.Placement,
	instanceID string,
) (placement.Placement, error) {
	instance, exist := p.Instance(instanceID)
	if !exist {
		return nil, errInstanceNotFound
	}

	if instance.IsLeaving() {
		return nil, errInstanceAlreadyLeaving
	}

	ph := newSubclusteredv3Helper(p, p.ReplicaFactor(), a.opts)
	ph.returnInitializingShards(instance)

	instance.SetShards(shard.NewShards(nil))
	instance.SetShardSetID(0)

	return ph.generatePlacement(), nil
}

func (a *subclusteredv3Algorithm) ReplaceInstance(
	p placement.Placement,
	leavingInstanceID string,
	addingInstanceID string,
) (placement.Placement, error) {
	leavingInstance, exist := p.Instance(leavingInstanceID)
	if !exist {
		return nil, errInstanceNotFound
	}

	addingInstance, exist := p.Instance(addingInstanceID)
	if !exist {
		return nil, errInstanceNotFound
	}

	if !leavingInstance.IsLeaving() {
		return nil, errInstanceNotLeaving
	}

	if addingInstance.IsLeaving() {
		return nil, errInstanceAlreadyLeaving
	}

	ph := newSubclusteredv3Helper(p, p.ReplicaFactor(), a.opts)
	if err := ph.replaceInstance(leavingInstance, addingInstance); err != nil {
		return nil, err
	}

	return ph.generatePlacement(), nil
}

func (a *subclusteredv3Algorithm) AddInstances(
	p placement.Placement,
	addingInstances []placement.Instance,
) (placement.Placement, error) {
	if len(addingInstances) == 0 {
		return nil, errNoInstances
	}

	// Get current subcluster assignments
	subClusterCount := make(map[uint32]int)
	for _, instance := range p.Instances() {
		subClusterCount[instance.SubClusterID()]++
	}

	// Sort instances by isolation group for consistent assignment
	sort.Slice(addingInstances, func(i, j int) bool {
		return addingInstances[i].IsolationGroup() < addingInstances[j].IsolationGroup()
	})

	// Assign subcluster IDs to new instances
	instancesPerSubCluster := a.opts.InstancesPerSubCluster()
	for _, instance := range addingInstances {
		// Find a subcluster that needs more instances
		var targetSubClusterID uint32
		for subClusterID, count := range subClusterCount {
			if count < instancesPerSubCluster {
				targetSubClusterID = subClusterID
				subClusterCount[subClusterID]++
				break
			}
		}

		// If no existing subcluster needs more instances, create a new one
		if targetSubClusterID == 0 {
			targetSubClusterID = uint32(len(subClusterCount) + 1)
			subClusterCount[targetSubClusterID] = 1
		}

		instance.SetSubClusterID(targetSubClusterID)
	}

	ph := newSubclusteredv3Helper(p, p.ReplicaFactor(), a.opts)

	for _, addingInstance := range addingInstances {
		if err := ph.addInstance(addingInstance); err != nil {
			return nil, err
		}
	}

	return ph.generatePlacement(), nil
}

func (a *subclusteredv3Algorithm) RemoveInstances(
	p placement.Placement,
	instanceIDs []string,
) (placement.Placement, error) {
	if len(instanceIDs) == 0 {
		return nil, errNoInstances
	}

	ph := newSubclusteredv3Helper(p, p.ReplicaFactor(), a.opts)

	for _, instanceID := range instanceIDs {
		instance, exist := p.Instance(instanceID)
		if !exist {
			return nil, errInstanceNotFound
		}

		if instance.IsLeaving() {
			return nil, errInstanceAlreadyLeaving
		}

		ph.returnInitializingShards(instance)
		instance.SetShards(shard.NewShards(nil))
		instance.SetShardSetID(0)
	}

	return ph.generatePlacement(), nil
}

func (a *subclusteredv3Algorithm) MarkShardsAvailable(
	p placement.Placement,
	instanceID string,
	shardIDs ...uint32,
) (placement.Placement, error) {
	instance, exist := p.Instance(instanceID)
	if !exist {
		return nil, errInstanceNotFound
	}

	if instance.IsLeaving() {
		return nil, errInstanceAlreadyLeaving
	}

	ph := newSubclusteredv3Helper(p, p.ReplicaFactor(), a.opts)

	for _, shardID := range shardIDs {
		s, ok := instance.Shards().Shard(shardID)
		if !ok {
			return nil, errShardNotFound
		}

		if s.State() != shard.Initializing {
			return nil, errShardNotInitializing
		}

		s.SetState(shard.Available)
	}

	return ph.generatePlacement(), nil
}

func (a *subclusteredv3Algorithm) MarkAllShardsAvailable(
	p placement.Placement,
) (placement.Placement, bool, error) {
	ph := newSubclusteredv3Helper(p, p.ReplicaFactor(), a.opts)

	anyChanged := false
	for _, instance := range p.Instances() {
		if instance.IsLeaving() {
			continue
		}

		for _, s := range instance.Shards().ShardsForState(shard.Initializing) {
			s.SetState(shard.Available)
			anyChanged = true
		}
	}

	return ph.generatePlacement(), anyChanged, nil
}

func (a *subclusteredv3Algorithm) BalanceShards(
	p placement.Placement,
) (placement.Placement, error) {
	ph := newSubclusteredv3Helper(p, p.ReplicaFactor(), a.opts)
	if err := ph.optimize(safe); err != nil {
		return nil, err
	}

	return ph.generatePlacement(), nil
}

func (a *subclusteredv3Algorithm) IsCompatibleWith(p placement.Placement) error {
	if !p.IsSharded() {
		return errNotSharded
	}

	if p.IsMirrored() != a.opts.IsMirrored() {
		return errNotMirrored
	}

	return nil
}

func (a *subclusteredv3Algorithm) ReplaceInstances(
	p placement.Placement,
	leavingInstanceIDs []string,
	addingInstances []placement.Instance,
) (placement.Placement, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}

	if len(leavingInstanceIDs) == 0 {
		return nil, errNoInstances
	}

	if len(addingInstances) == 0 {
		return nil, errNoInstances
	}

	ph := newSubclusteredv3Helper(p, p.ReplicaFactor(), a.opts)

	for _, instanceID := range leavingInstanceIDs {
		instance, exist := p.Instance(instanceID)
		if !exist {
			return nil, errInstanceNotFound
		}

		if !instance.IsLeaving() {
			return nil, errInstanceNotLeaving
		}

		ph.returnInitializingShards(instance)
		instance.SetShards(shard.NewShards(nil))
		instance.SetShardSetID(0)
	}

	for _, addingInstance := range addingInstances {
		if err := ph.addInstance(addingInstance); err != nil {
			return nil, err
		}
	}

	return ph.generatePlacement(), nil
}
