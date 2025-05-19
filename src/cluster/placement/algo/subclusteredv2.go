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
	"strings"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
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
	ph, err := newSubclusteredv2InitHelper(placement.Instances(instances).Clone(), shards, a.opts)
	if err != nil {
		return nil, err
	}
	if err := ph.placeShards(newShards(shards), nil, ph.Instances()); err != nil {
		return nil, err
	}

	p := ph.generatePlacement()
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

	for i, leavingInstance := range leavingInstances {
		err = ph.placeShards(leavingInstance.Shards().All(), leavingInstance, []placement.Instance{addingInstances[i]})
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
	ph := newubclusteredv2Helper(p, p.ReplicaFactor(), a.opts, 0)
	// fmt.Println("Target load per instance:", ph.targetLoad)
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

// PlacementDiff represents the differences found between two placements
type PlacementDiff struct {
	// InstanceCounts contains the number of instances in each placement
	InstanceCounts struct {
		Before int
		After  int
	}
	// ShardAssignmentDiffs contains shard assignment differences
	ShardAssignmentDiffs map[string]struct {
		Before map[uint32]shard.State
		After  map[uint32]shard.State
	}
	p placement.Placement
}

// ComparePlacements compares two placements and returns their differences
func ComparePlacements(before, after placement.Placement) (*PlacementDiff, error) {
	if before == nil || after == nil {
		return nil, fmt.Errorf("both placements must be non-nil")
	}

	diff := &PlacementDiff{
		ShardAssignmentDiffs: make(map[string]struct {
			Before map[uint32]shard.State
			After  map[uint32]shard.State
		}),
	}

	// Set instance counts
	diff.InstanceCounts.Before = len(before.Instances())
	diff.InstanceCounts.After = len(after.Instances())

	// Create maps for quick lookup
	beforeInstances := make(map[string]placement.Instance)
	afterInstances := make(map[string]placement.Instance)

	// Populate instance maps
	for _, instance := range before.Instances() {
		beforeInstances[instance.ID()] = instance
	}
	for _, instance := range after.Instances() {
		afterInstances[instance.ID()] = instance
	}

	// Compare shard assignments for all instances in both placements
	allInstanceIDs := make(map[string]struct{})
	for id := range beforeInstances {
		allInstanceIDs[id] = struct{}{}
	}
	for id := range afterInstances {
		allInstanceIDs[id] = struct{}{}
	}

	for id := range allInstanceIDs {
		beforeInstance, beforeExists := beforeInstances[id]
		afterInstance, afterExists := afterInstances[id]

		// Get shard assignments for both instances
		beforeShards := make(map[uint32]shard.State)
		afterShards := make(map[uint32]shard.State)

		if beforeExists {
			for _, s := range beforeInstance.Shards().All() {
				beforeShards[s.ID()] = s.State()
			}
		}
		if afterExists {
			for _, s := range afterInstance.Shards().All() {
				afterShards[s.ID()] = s.State()
			}
		}

		// Check if shard assignments are different
		hasDiff := false
		if len(beforeShards) != len(afterShards) {
			hasDiff = true
		} else {
			for shardID, beforeState := range beforeShards {
				if afterState, exists := afterShards[shardID]; !exists || afterState != beforeState {
					hasDiff = true
					break
				}
			}
		}

		if hasDiff {
			diff.ShardAssignmentDiffs[id] = struct {
				Before map[uint32]shard.State
				After  map[uint32]shard.State
			}{
				Before: beforeShards,
				After:  afterShards,
			}
		}
	}

	diff.p = after

	return diff, nil
}

// PrintDiff prints the differences between placements in a readable format
func (d *PlacementDiff) PrintDiff() string {
	var result strings.Builder

	// Print instance count differences
	result.WriteString(fmt.Sprintf("Instance Count Differences:\n"))
	result.WriteString(fmt.Sprintf("  Before: %d instances\n", d.InstanceCounts.Before))
	result.WriteString(fmt.Sprintf("  After:  %d instances\n\n", d.InstanceCounts.After))

	// Print shard assignment differences
	if len(d.ShardAssignmentDiffs) > 0 {
		result.WriteString("Shard Assignment Differences:\n")
		for instanceID, diff := range d.ShardAssignmentDiffs {
			i, _ := d.p.Instance(instanceID)
			result.WriteString(fmt.Sprintf("\nInstance: %s SubclusterID: %d\n", instanceID, i.SubClusterID()))

			// Print before state
			result.WriteString("  Before:\n")
			if len(diff.Before) == 0 {
				result.WriteString("    No shards assigned\n")
			} else {
				for shardID, state := range diff.Before {
					result.WriteString(fmt.Sprintf("    Shard %d: %s\n", shardID, state))
				}
			}

			// Print after state
			result.WriteString("  After:\n")
			if len(diff.After) == 0 {
				result.WriteString("    No shards assigned\n")
			} else {
				for shardID, state := range diff.After {
					result.WriteString(fmt.Sprintf("    Shard %d: %s\n", shardID, state))
				}
			}

			// Print summary of changes
			result.WriteString("  Changes:\n")
			added := make(map[uint32]shard.State)
			removed := make(map[uint32]shard.State)
			modified := make(map[uint32]struct {
				Before shard.State
				After  shard.State
			})

			// Find added and modified shards
			for shardID, afterState := range diff.After {
				if beforeState, exists := diff.Before[shardID]; exists {
					if beforeState != afterState {
						modified[shardID] = struct {
							Before shard.State
							After  shard.State
						}{beforeState, afterState}
					}
				} else {
					added[shardID] = afterState
				}
			}

			// Find removed shards
			for shardID, beforeState := range diff.Before {
				if _, exists := diff.After[shardID]; !exists {
					removed[shardID] = beforeState
				}
			}

			// Print added shards with subcluster info
			if len(added) > 0 {
				result.WriteString("    Added:\n")
				for shardID, state := range added {
					// Find all instances that have this shard in the after state
					var instancesWithShard []string
					for _, inst := range d.p.Instances() {
						if s, ok := inst.Shards().Shard(shardID); ok && s.State() == state {
							instancesWithShard = append(instancesWithShard,
								fmt.Sprintf("%s(subcluster:%d)", inst.ID(), inst.SubClusterID()))
						}
					}
					result.WriteString(fmt.Sprintf("      Shard %d: %s -> Instances: %v\n",
						shardID, state, instancesWithShard))
				}
			}

			// Print removed shards
			if len(removed) > 0 {
				result.WriteString("    Removed:\n")
				for shardID, state := range removed {
					result.WriteString(fmt.Sprintf("      Shard %d: %s\n", shardID, state))
				}
			}

			// Print modified shards with subcluster info
			if len(modified) > 0 {
				result.WriteString("    Modified:\n")
				for shardID, states := range modified {
					// Find all instances that have this shard in the after state
					var instancesWithShard []string
					for _, inst := range d.p.Instances() {
						if s, ok := inst.Shards().Shard(shardID); ok && s.State() == states.After {
							instancesWithShard = append(instancesWithShard,
								fmt.Sprintf("%s(subcluster:%d)", inst.ID(), inst.SubClusterID()))
						}
					}
					result.WriteString(fmt.Sprintf("      Shard %d: %s -> %s -> Instances: %v\n",
						shardID, states.Before, states.After, instancesWithShard))
				}
			}
		}
	} else {
		result.WriteString("No shard assignment differences found.\n")
	}

	return result.String()
}
