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
			return fmt.Errorf("inconsistent instance weights: instance %s has weight %d, expected %d",
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
		if _, exist := ph.groupToInstancesMap[instance.IsolationGroup()]; !exist {
			ph.groupToInstancesMap[instance.IsolationGroup()] = make(map[placement.Instance]struct{})
		}
		ph.groupToInstancesMap[instance.IsolationGroup()][instance] = struct{}{}

		if instance.IsLeaving() {
			continue
		}

		subClusterID := instance.SubClusterID()
		if _, exist := ph.subClusters[subClusterID]; !exist {
			ph.subClusters[subClusterID] = &subcluster{
				id:                  subClusterID,
				instances:           make(map[string]placement.Instance),
				shardMap:            make(map[uint32]int),
				instanceShardCounts: make(map[string]int),
			}
		}

		// if we are checking that all instance weight is same than we can simply the calculation by assuming it as 1
		ph.groupToWeightMap[instance.IsolationGroup()]++
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

// buildTargetLoad builds the target load for the placement.
// nolint
func (ph *subclusteredHelper) buildTargetLoad(subClusterToExclude uint32) {
	overWeightedGroups := 0
	overWeight := uint32(0)
	for _, weight := range ph.groupToWeightMap {
		if isOverWeighted(weight, ph.totalWeight, ph.rf) {
			overWeightedGroups++
			overWeight += weight
		}
	}

	targetLoad := make(map[string]int, len(ph.instances))
	for _, instance := range ph.instances {
		if instance.IsLeaving() {
			continue
		}
		igWeight := ph.groupToWeightMap[instance.IsolationGroup()]
		if isOverWeighted(igWeight, ph.totalWeight, ph.rf) {
			targetLoad[instance.ID()] = int(math.Ceil(float64(ph.getShardLen()) *
				float64(instance.Weight()) / float64(igWeight)))
		} else {
			targetLoad[instance.ID()] = ph.getShardLen() * (ph.rf - overWeightedGroups) *
				int(instance.Weight()) / int(ph.totalWeight-overWeight)
		}
	}
	ph.targetLoad = targetLoad
}

// buildTargetSubclusterLoad builds the target load for the subclusters.
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
	return subClusterIds // Returns zero value of K and false if map is empty
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
