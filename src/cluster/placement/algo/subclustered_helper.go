// Copyright (c) 2025 Uber Technologies, Inc.
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
	"fmt"
	"math"
	"sort"

	"go.uber.org/zap"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
)

var (
	// nolint: unused
	errSubclusteredHelperNotImplemented = errors.New("subclustered helper methods not yet implemented")
)

// nolint
type subclusteredHelper struct {
	targetLoad             map[string]int
	shardToInstanceMap     map[uint32]map[placement.Instance]struct{}
	groupToInstancesMap    map[string]map[placement.Instance]struct{}
	groupToWeightMap       map[string]uint32
	subClusters            map[uint32]*subcluster
	rf                     int
	uniqueShards           []uint32
	instances              map[string]placement.Instance
	log                    *zap.Logger
	opts                   placement.Options
	totalWeight            uint32
	instancesPerSubcluster int
}

// subcluster is a subcluster in the placement.
// nolint
type subcluster struct {
	id                  uint32
	targetShardCount    int
	instances           map[string]placement.Instance
	shardMap            map[uint32]int
	instanceShardCounts map[string]int
}

func newSubclusteredInitHelper(
	instances []placement.Instance,
	ids []uint32,
	opts placement.Options,
	rf int,
) (placementHelper, error) {
	err := assignSubClusterIDs(instances, nil, opts.InstancesPerSubCluster())
	if err != nil {
		return nil, err
	}
	emptyPlacement := placement.NewPlacement().
		SetInstances(instances).
		SetShards(ids).
		SetReplicaFactor(rf).
		SetIsSharded(true).
		SetIsSubclustered(true).
		SetInstancesPerSubCluster(opts.InstancesPerSubCluster()).
		SetCutoverNanos(opts.PlacementCutoverNanosFn()())
	ph, err := newSubclusteredHelper(emptyPlacement, opts, 0)
	if err != nil {
		return nil, err
	}
	return ph, nil
}

func newSubclusteredHelper(
	p placement.Placement,
	opts placement.Options,
	subClusterToExclude uint32,
) (placementHelper, error) {
	ph := &subclusteredHelper{
		rf:                     p.ReplicaFactor(),
		instances:              make(map[string]placement.Instance, p.NumInstances()),
		uniqueShards:           p.Shards(),
		log:                    opts.InstrumentOptions().Logger(),
		opts:                   opts,
		instancesPerSubcluster: p.InstancesPerSubCluster(),
	}

	for _, instance := range p.Instances() {
		ph.instances[instance.ID()] = instance
	}

	// We are adding a constraint of all instances have the same weight when we are using subclustered placement.
	err := ph.validateInstanceWeight()
	if err != nil {
		return nil, err
	}

	ph.scanCurrentLoad(subClusterToExclude)

	err = ph.validateSubclusterDistribution()
	if err != nil {
		return nil, err
	}

	ph.buildTargetLoad(subClusterToExclude)
	ph.buildTargetSubclusterLoad(subClusterToExclude)

	return ph, nil
}

// validateInstanceWeight validates that all instances have the same weight.
func (ph *subclusteredHelper) validateInstanceWeight() error {
	if len(ph.instances) == 0 {
		return nil
	}

	// Get the expected weight from the first instance
	firstInstance := true
	expectedWeight := uint32(0)

	// Check that each and every instance has the same weight
	for _, instance := range ph.instances {
		if firstInstance {
			expectedWeight = instance.Weight()
			firstInstance = false
			continue
		}

		if instance.Weight() != expectedWeight {
			return fmt.Errorf("sub-clustered algo currently only supports all instances having equal weights. "+
				"inconsistent instance weights: instance %s has weight %d, expected %d, ",
				instance.ID(), instance.Weight(), expectedWeight)
		}
	}

	return nil
}

// nolint
func (ph *subclusteredHelper) scanCurrentLoad(subClusterToExclude uint32) {
	ph.shardToInstanceMap = make(map[uint32]map[placement.Instance]struct{}, len(ph.uniqueShards))
	ph.groupToInstancesMap = make(map[string]map[placement.Instance]struct{})
	ph.groupToWeightMap = make(map[string]uint32)
	ph.subClusters = make(map[uint32]*subcluster)
	totalWeight := uint32(0)
	for _, instance := range ph.instances {
		if instance.IsLeaving() {
			continue
		}

		ig := instance.IsolationGroup()

		if _, exist := ph.groupToInstancesMap[ig]; !exist {
			ph.groupToInstancesMap[ig] = make(map[placement.Instance]struct{})
		}
		ph.groupToInstancesMap[ig][instance] = struct{}{}

		subClusterID := instance.SubClusterID()
		if _, exist := ph.subClusters[subClusterID]; !exist {
			ph.subClusters[subClusterID] = &subcluster{
				id:                  subClusterID,
				instances:           make(map[string]placement.Instance),
				shardMap:            make(map[uint32]int),
				instanceShardCounts: make(map[string]int),
			}
		}

		// if we are checking that all instance weight is same than we can simplify the calculation by assuming it as 1.
		ph.groupToWeightMap[ig]++
		totalWeight++
		ph.subClusters[subClusterID].instances[instance.ID()] = instance

		for _, s := range instance.Shards().All() {
			if s.State() == shard.Leaving {
				continue
			}
			ph.assignShardToInstance(s, instance)
		}

	}
	ph.totalWeight = totalWeight
}

// buildTargetLoad builds the target load for the instances in the placement.
// The target load is the number of shards that each instance should have.
// The target load is calculated based on the total weight of the instances and the replica factor.
// This method implements a weighted load balancing algorithm that handles both normal and
// over-weighted isolation groups. Over-weighted groups are those that have more instances
// than the replica factor allows, which requires special handling to ensure proper distribution.
// nolint
func (ph *subclusteredHelper) buildTargetLoad(subClusterToExclude uint32) {
	// Step 1: Identify over-weighted isolation groups
	// Over-weighted groups are those where the number of instances exceeds the replica factor.
	// These groups need special handling as they can't follow the standard distribution formula.
	overWeightedGroups := 0
	overWeight := uint32(0)
	for _, weight := range ph.groupToWeightMap {
		if isOverWeighted(weight, ph.totalWeight, ph.rf) {
			overWeightedGroups++
			overWeight += weight
		}
	}

	// Step 2: Calculate target load for each instance
	// The target load determines how many shards each instance should ideally have.
	targetLoad := make(map[string]int, len(ph.instances))
	for _, instance := range ph.instances {
		// Skip instances that are leaving the cluster
		if instance.IsLeaving() {
			continue
		}

		// Get the weight of the instance's isolation group
		igWeight := ph.groupToWeightMap[instance.IsolationGroup()]

		if isOverWeighted(igWeight, ph.totalWeight, ph.rf) {
			// For over-weighted isolation groups:
			// Distribute shards proportionally within the group based on instance weight.
			// Use ceiling to ensure we don't under-allocate shards.
			targetLoad[instance.ID()] = int(math.Ceil(float64(ph.getShardLen()) *
				float64(instance.Weight()) / float64(igWeight)))
		} else {
			// For normal isolation groups:
			// Use the standard formula that accounts for over-weighted groups.
			// The formula ensures that the total shard count across all instances
			// equals totalShards * replicaFactor, while respecting instance weights.
			targetLoad[instance.ID()] = ph.getShardLen() * (ph.rf - overWeightedGroups) *
				int(instance.Weight()) / int(ph.totalWeight-overWeight)
		}
	}
	ph.targetLoad = targetLoad
}

// buildTargetSubclusterLoad builds the target load for the subclusters.
// This method distributes the total number of shards evenly across all active subclusters,
// ensuring that each subcluster gets approximately the same number of shards to manage.
// Any remaining shards (due to integer division) are distributed one by one to subclusters
// in a deterministic order to ensure consistent placement.
func (ph *subclusteredHelper) buildTargetSubclusterLoad(subClusterToExclude uint32) {
	totalShards := len(ph.uniqueShards)
	subClusters := ph.getSubclusterIds(subClusterToExclude)
	sort.Slice(subClusters, func(i, j int) bool { return subClusters[i] <= subClusters[j] })
	totalDivided := 0
	for _, subClusterID := range subClusters {
		ph.subClusters[subClusterID].targetShardCount = int(math.Floor(float64(totalShards) / float64(len(subClusters))))
		totalDivided += ph.subClusters[subClusterID].targetShardCount
	}
	diff := totalShards - totalDivided
	for _, curr := range subClusters {
		if diff == 0 {
			break
		}
		ph.subClusters[curr].targetShardCount++
		diff--
	}
}

// getSubclusterIds gets the subcluster ids slice.
func (ph *subclusteredHelper) getSubclusterIds(subClusterToExclude uint32) []uint32 {
	subClusterIds := make([]uint32, 0, len(ph.subClusters))
	for k := range ph.subClusters {
		if k == subClusterToExclude {
			continue
		}
		subClusterIds = append(subClusterIds, k)
	}
	return subClusterIds
}

// getShardLen gets the shard length.
func (ph *subclusteredHelper) getShardLen() int {
	return len(ph.uniqueShards)
}

// assignShardToInstance assigns a shard to an instance.
// nolint: unused
func (ph *subclusteredHelper) assignShardToInstance(s shard.Shard, to placement.Instance) {
	to.Shards().Add(s)

	if _, exist := ph.shardToInstanceMap[s.ID()]; !exist {
		ph.shardToInstanceMap[s.ID()] = make(map[placement.Instance]struct{})
	}
	ph.shardToInstanceMap[s.ID()][to] = struct{}{}
	ph.subClusters[to.SubClusterID()].shardMap[s.ID()]++
	ph.subClusters[to.SubClusterID()].instanceShardCounts[to.ID()]++
}

// nolint
// Instances returns the list of instances managed by the PlacementHelper.
func (ph *subclusteredHelper) Instances() []placement.Instance {
	// TODO: Implement subclustered instances logic
	return nil
}

// CanMoveShard checks if the shard can be moved from the instance to the target isolation group.
// nolint: unused
func (ph *subclusteredHelper) CanMoveShard(shard uint32, from placement.Instance, toIsolationGroup string) bool {
	// TODO: Implement subclustered shard movement logic
	return false
}

// placeShards distributes shards to the instances in the helper, with aware of where are the shards coming from.
// nolint: unused
func (ph *subclusteredHelper) placeShards(
	shards []shard.Shard,
	from placement.Instance,
	candidates []placement.Instance,
) error {
	// TODO: Implement subclustered shard placement logic
	return fmt.Errorf("subclustered placeShards not yet implemented: %w",
		errSubclusteredHelperNotImplemented)
}

// addInstance adds an instance to the placement.
// nolint: unused
func (ph *subclusteredHelper) addInstance(addingInstance placement.Instance) error {
	// TODO: Implement subclustered add instance logic
	return fmt.Errorf("subclustered addInstance not yet implemented: %w", errSubclusteredHelperNotImplemented)
}

// optimize rebalances the load distribution in the cluster.
// nolint: unused
func (ph *subclusteredHelper) optimize(t optimizeType) error {
	// TODO: Implement subclustered optimization logic
	return fmt.Errorf("subclustered optimize not yet implemented: %w", errSubclusteredHelperNotImplemented)
}

// generatePlacement generates a placement.
// nolint: unused
func (ph *subclusteredHelper) generatePlacement() placement.Placement {
	// TODO: Implement subclustered placement generation logic
	return nil
}

// reclaimLeavingShards reclaims all the leaving shards on the given instance
// by pulling them back from the rest of the cluster.
// nolint: unused
func (ph *subclusteredHelper) reclaimLeavingShards(instance placement.Instance) {
	// TODO: Implement subclustered reclaim leaving shards logic
}

// returnInitializingShards returns all the initializing shards on the given instance
// by returning them back to the original owners.
// nolint: unused
func (ph *subclusteredHelper) returnInitializingShards(instance placement.Instance) {
	// TODO: Implement subclustered return initializing shards logic
}

// validateSubclusterDistribution validates that:
// 1. Number of isolation groups equals replica factor (rf)
// 2. For complete subclusters, nodes per isolation group = instancesPerSubcluster / rf
// nolint: unused
func (ph *subclusteredHelper) validateSubclusterDistribution() error {
	if len(ph.instances) == 0 {
		return nil
	}

	if ph.rf <= 0 {
		return fmt.Errorf("replica factor should be greater than 0")
	}

	if ph.opts.InstancesPerSubCluster() <= 0 {
		return fmt.Errorf("instances per subcluster is not set")
	}

	if len(ph.groupToInstancesMap) != ph.rf {
		return fmt.Errorf("number of isolation groups (%d) does not match replica factor (%d)",
			len(ph.groupToInstancesMap), ph.rf)
	}

	// Validate each subcluster
	for subclusterID, currCluster := range ph.subClusters {
		isolationGroups := make(map[string]int)
		for _, instance := range currCluster.instances {
			isolationGroups[instance.IsolationGroup()]++
		}

		expectedInstancesPerGroup := ph.instancesPerSubcluster / ph.rf
		for isolationGroup, count := range isolationGroups {
			if len(currCluster.instances) == ph.instancesPerSubcluster && count != expectedInstancesPerGroup {
				return fmt.Errorf("subcluster %d isolation group %s has %d instances, expected %d",
					subclusterID, isolationGroup, count, expectedInstancesPerGroup)
			} else if len(currCluster.instances) < ph.instancesPerSubcluster && count > expectedInstancesPerGroup {
				return fmt.Errorf("subcluster %d isolation group %s has %d instances, expected 0",
					subclusterID, isolationGroup, count)
			}
		}
	}

	return nil
}

func assignSubClusterIDs(
	instances []placement.Instance,
	currPlacement placement.Placement,
	instancesPerSubcluster int,
) error {
	if instancesPerSubcluster <= 0 {
		return fmt.Errorf("instances per subcluster is not set")
	}

	// If current placement is nil, start assigning from subcluster 1
	maxSubclusterID := uint32(1)
	maxSubclusterCount := 0
	if currPlacement != nil {
		currInstances := currPlacement.Instances()

		for _, instance := range currInstances {
			if instance.IsLeaving() {
				continue
			}
			if instance.SubClusterID() > maxSubclusterID {
				maxSubclusterID = instance.SubClusterID()
				maxSubclusterCount = 1
			} else if instance.SubClusterID() == maxSubclusterID {
				maxSubclusterCount++
			}
		}
	}

	// Assign subcluster IDs to new instances
	for _, instance := range instances {
		if maxSubclusterCount == instancesPerSubcluster {
			maxSubclusterID++
			maxSubclusterCount = 0
		}
		instance.SetSubClusterID(maxSubclusterID)
		maxSubclusterCount++
	}
	return nil
}
