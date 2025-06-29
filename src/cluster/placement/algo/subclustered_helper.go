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
	"container/heap"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
)

var (
	// nolint: unused
	errSubclusteredHelperNotImplemented = errors.New("subclustered helper methods not yet implemented")
)

type validationOperation int

const (
	validationOpRemoval validationOperation = iota
	validationOpAddition
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
		SetHasSubClusters(true).
		SetInstancesPerSubCluster(opts.InstancesPerSubCluster()).
		SetCutoverNanos(opts.PlacementCutoverNanosFn()())
	ph, err := newSubclusteredHelper(emptyPlacement, opts, 0)
	if err != nil {
		return nil, err
	}
	return ph, nil
}

func newubclusteredAddInstanceHelper(
	p placement.Placement,
	instance placement.Instance,
	opts placement.Options,
	t instanceType,
) (placementHelper, placement.Instance, error) {
	instanceInPlacement, exist := p.Instance(instance.ID())
	if !exist {
		if err := assignSubClusterIDs([]placement.Instance{instance}, p, opts.InstancesPerSubCluster()); err != nil {
			return nil, nil, err
		}
		ph, err := newSubclusteredHelper(p.SetInstances(append(p.Instances(), instance)), opts, 0)
		if err != nil {
			return nil, nil, err
		}
		err = ph.validatePartialSubclusters(instance.SubClusterID(), validationOpAddition)
		if err != nil {
			return nil, nil, err
		}
		return ph, instance, nil
	}

	switch t {
	case withLeavingShardsOnly:
		if !instanceInPlacement.IsLeaving() {
			return nil, nil, errInstanceContainsNonLeavingShards
		}
	case withAvailableOrLeavingShardsOnly:
		shards := instanceInPlacement.Shards()
		if shards.NumShards() != shards.NumShardsForState(shard.Available)+shards.NumShardsForState(shard.Leaving) {
			return nil, nil, errInstanceContainsInitializingShards
		}
	default:
		return nil, nil, fmt.Errorf("unexpected type %v", t)
	}

	ph, err := newSubclusteredHelper(p, opts, 0)
	if err != nil {
		return nil, nil, err
	}
	return ph, instanceInPlacement, nil
}

func newubclusteredRemoveInstanceHelper(
	p placement.Placement,
	instanceID string,
	opts placement.Options,
) (placementHelper, placement.Instance, error) {
	p, leavingInstance, err := removeInstanceFromPlacement(p, instanceID)
	if err != nil {
		return nil, nil, err
	}
	subclusterInstances := getSubClusterInstances(p.Instances(), leavingInstance.SubClusterID())
	// if the number of instances after removing the leaving instance is still equal to instancesPerSubcluster,
	// we can safely assume that there was a replace operation going on in the cluster.
	// In that case we don't need to exclude the subcluster from the calculation of targetShardCount.
	if len(subclusterInstances) == opts.InstancesPerSubCluster() {
		ph, err := newSubclusteredHelper(p, opts, 0)
		if err != nil {
			return nil, nil, err
		}
		return ph, leavingInstance, nil
	}
	// if the number of instances after removing the leaving instance is not equal to instancesPerSubcluster,
	// we need to exclude the subcluster from the calculation of targetShardCount.
	// Basically we are considering this operation equivalent to removeSubcluster.
	ph, err := newSubclusteredHelper(p, opts, leavingInstance.SubClusterID())
	if err != nil {
		return nil, nil, err
	}
	err = ph.validatePartialSubclusters(leavingInstance.SubClusterID(), validationOpRemoval)
	if err != nil {
		return nil, nil, err
	}
	return ph, leavingInstance, nil
}

func newubclusteredReplaceInstanceHelper(
	p placement.Placement,
	instanceIDs []string,
	addingInstances []placement.Instance,
	opts placement.Options,
) (placementHelper, []placement.Instance, []placement.Instance, error) {
	var (
		leavingInstances = make([]placement.Instance, len(instanceIDs))
		err              error
	)
	for i, instanceID := range instanceIDs {
		p, leavingInstances[i], err = removeInstanceFromPlacement(p, instanceID)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	newAddingInstances := make([]placement.Instance, len(addingInstances))
	for i, instance := range addingInstances {
		p, newAddingInstances[i], err = addInstanceToPlacement(p, instance, anyType)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	if len(newAddingInstances) != len(leavingInstances) {
		return nil, nil, nil, fmt.Errorf("number of adding instances (%d) does not match number of leaving instances (%d)",
			len(newAddingInstances), len(leavingInstances))
	}

	// Match adding instances with leaving instances
	for i, addingInstance := range newAddingInstances {
		addingInstance.SetSubClusterID(leavingInstances[i].SubClusterID())
	}
	ph, err := newSubclusteredHelper(p, opts, 0)
	if err != nil {
		return nil, nil, nil, err
	}
	return ph, leavingInstances, newAddingInstances, nil
}

func newSubclusteredHelper(
	p placement.Placement,
	opts placement.Options,
	subClusterToExclude uint32,
) (*subclusteredHelper, error) {
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

	ph.scanCurrentLoad()

	err = ph.validateSubclusterDistribution()
	if err != nil {
		return nil, err
	}

	ph.buildTargetLoad()
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

func (ph *subclusteredHelper) scanCurrentLoad() {
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
// nolint: dupl
func (ph *subclusteredHelper) buildTargetLoad() {
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
	res := make([]placement.Instance, 0, len(ph.instances))
	for _, instance := range ph.instances {
		res = append(res, instance)
	}
	return res
}

func (ph *subclusteredHelper) moveShard(candidateShard shard.Shard, from, to placement.Instance) bool {
	shardID := candidateShard.ID()
	if !ph.canAssignInstance(shardID, from, to) {
		return false
	}

	if candidateShard.State() == shard.Leaving {
		return false
	}

	newShard := shard.NewShard(shardID)
	if from != nil {
		// nolint: exhaustive
		switch candidateShard.State() {
		case shard.Unknown, shard.Initializing:
			from.Shards().Remove(shardID)
			newShard.SetSourceID(candidateShard.SourceID())
		case shard.Available:
			candidateShard.
				SetState(shard.Leaving).
				SetCutoffNanos(ph.opts.ShardCutoffNanosFn()())
			newShard.SetSourceID(from.ID())
		}
		ph.removeShardFromInstance(shardID, from)
	}
	curShard, ok := to.Shards().Shard(shardID)
	if ok && curShard.State() == shard.Leaving {
		newShard = shard.NewShard(shardID).SetState(shard.Available)
		instances := ph.shardToInstanceMap[shardID]
		for instance := range instances {
			shards := instance.Shards()
			initShard, ok := shards.Shard(shardID)
			if ok && initShard.SourceID() == to.ID() {
				initShard.SetSourceID("")
			}
		}

	}

	ph.assignShardToInstance(newShard, to)
	return true
}

func (ph *subclusteredHelper) removeShardFromInstance(shardID uint32, from placement.Instance) {
	delete(ph.shardToInstanceMap[shardID], from)
	if fromsubcluster, exist := ph.subClusters[from.SubClusterID()]; exist {
		fromsubcluster.shardMap[shardID]--
		if fromsubcluster.shardMap[shardID] == 0 {
			delete(fromsubcluster.shardMap, shardID)
		}
		fromsubcluster.instanceShardCounts[from.ID()]--
		if fromsubcluster.instanceShardCounts[from.ID()] == 0 {
			delete(fromsubcluster.instanceShardCounts, from.ID())
		}
	}
}

func (ph *subclusteredHelper) canAssignInstance(shardID uint32, from, to placement.Instance) bool {
	s, ok := to.Shards().Shard(shardID)
	if ok && s.State() != shard.Leaving {
		return false
	}
	tosubcluster := ph.subClusters[to.SubClusterID()]
	// the targetshardCount is 0 when we are removing the subcluster.
	// In this case we don't want to assign shard to any other instance in the the leaving subcluster.
	// As eventually these shards will be assigned to the new subcluster.
	if tosubcluster.targetShardCount == 0 {
		return false
	}
	// if the subcluster is full, the shard should be already assigned to the subcluster
	// if the shard is not assigned to the subcluster, return false
	if len(tosubcluster.shardMap) == tosubcluster.targetShardCount {
		if _, exists := tosubcluster.shardMap[shardID]; !exists {
			return false
		}
	}

	if from != nil {
		fromSubcluster, exists := ph.subClusters[from.SubClusterID()]
		// In case of removing an instance/subcluster. the targetShardCount will be 0.
		// If it is the last instance in the subcluster, the subcluster should not be present in the helper
		if !exists || fromSubcluster.targetShardCount == 0 {
			// In case of removing a subcluster we only need to check if all the replicas of a shard has been
			// assigned to the same subcluster.
			for instance := range ph.shardToInstanceMap[shardID] {
				if instance.SubClusterID() == from.SubClusterID() {
					continue
				}
				if instance.SubClusterID() != to.SubClusterID() {
					return false
				}
			}
			return ph.CanMoveShard(shardID, from, to.IsolationGroup())
		}
		// Case 1(add-instance): If we are moving the shard within the same subcluster, we just need to check
		// if the if the shard cnn be moved to the to IsolationGroup.
		if from.SubClusterID() == to.SubClusterID() {
			return ph.CanMoveShard(shardID, from, to.IsolationGroup())
		}
		// Case 2(add-instance): If we are moving the shard across subclusters.
		// Case 2.1(add-instance): Check if the from instance's subcluster can give the shards, i.e.
		// if the number of shards in the from instance's subcluster has reached to its targetShatdCount
		// in that case we cannot take any shard from this instance's subcluster.
		if exists && len(fromSubcluster.shardMap) == fromSubcluster.targetShardCount {
			return false
		}
		// Case 2.2(add-instance): If we can take shards from the from instance's subcluster, we need to check
		// if the from subcluster has given all the shards only the replicas of the shards is left
		// in the from subcluster. IF that is the case then we need to make sure the replica is only going
		// to the subcluster which already has one or more replica of the shard. To find this we will
		// take intersection of the shards in froma d to subcluster and if the shard doesn'y exist in
		// intersection and len(intersection) == (len(fromsubcluster.shardMap)-fromsubcluster.targetShardCount)
		// we will return false. (This case will be viable when the targetShardCount of to subcluster hasn't reached but
		// the from subcluster has given all the shards.)
		if exists && len(fromSubcluster.shardMap) > fromSubcluster.targetShardCount {
			intersection := ph.findMapKeyIntersection(tosubcluster.shardMap, fromSubcluster.shardMap)
			if _, exist := intersection[shardID]; !exist &&
				len(intersection) == (len(fromSubcluster.shardMap)-fromSubcluster.targetShardCount) {
				return false
			}
		}
		// Case 2.3(add-instance): If the from subcluster hasn't given all the shards,
		// we just need to check for isolation group movement
	}
	return ph.CanMoveShard(shardID, from, to.IsolationGroup())
}

// findMapKeyIntersection returns a map containing keys that exist in both input maps
func (ph *subclusteredHelper) findMapKeyIntersection(map1, map2 map[uint32]int) map[uint32]struct{} {
	// Create a map to store keys from the first map
	keys := make(map[uint32]struct{})
	for k := range map1 {
		keys[k] = struct{}{}
	}

	// Create result map for intersection
	intersection := make(map[uint32]struct{})

	// Find intersection by checking which keys from map1 exist in map2
	for k := range map2 {
		if _, exists := keys[k]; exists {
			intersection[k] = struct{}{}
		}
	}

	return intersection
}

// CanMoveShard checks if the shard can be moved from the instance to the target isolation group.
// nolint: unused
func (ph *subclusteredHelper) CanMoveShard(shard uint32, from placement.Instance, toIsolationGroup string) bool {
	if from != nil {
		if from.IsolationGroup() == toIsolationGroup {
			return true
		}
	}
	for instance := range ph.shardToInstanceMap[shard] {
		if instance.IsolationGroup() == toIsolationGroup {
			return false
		}
	}
	return true
}

// placeShards distributes shards to the instances in the helper, with aware of where are the shards coming from.
// nolint: dupl
func (ph *subclusteredHelper) placeShards(
	shards []shard.Shard,
	from placement.Instance,
	candidates []placement.Instance,
) error {
	shardSet := getShardMap(shards)
	if from != nil {
		ph.returnInitializingShardsToSource(shardSet, from, candidates)
	}

	instanceHeap, err := ph.buildInstanceHeap(nonLeavingInstances(candidates), true)
	if err != nil {
		return err
	}
	// if there are shards left to be assigned, distribute them evenly
	var triedInstances []placement.Instance
	for _, s := range shardSet {
		if s.State() == shard.Leaving {
			continue
		}
		moved := false
		for instanceHeap.Len() > 0 {
			tryInstance := heap.Pop(instanceHeap).(placement.Instance)
			triedInstances = append(triedInstances, tryInstance)
			if ph.moveShard(s, from, tryInstance) {
				moved = true
				break
			}
		}
		if !moved {
			return errNotEnoughIsolationGroups
		}
		for _, triedInstance := range triedInstances {
			heap.Push(instanceHeap, triedInstance)
		}
		triedInstances = triedInstances[:0]
	}
	return nil
}

// addInstance adds an instance to the placement.
// nolint: unused
func (ph *subclusteredHelper) addInstance(addingInstance placement.Instance) error {
	ph.reclaimLeavingShards(addingInstance)
	return ph.assignLoadToInstanceUnsafe(addingInstance)
}

func (ph *subclusteredHelper) assignLoadToInstanceUnsafe(addingInstance placement.Instance) error {
	return ph.assignTargetLoad(addingInstance, func(from, to placement.Instance) bool {
		return ph.moveOneShard(from, to)
	})
}

func (ph *subclusteredHelper) assignTargetLoad(
	targetInstance placement.Instance,
	moveOneShardFn func(from, to placement.Instance) bool,
) error {

	targetLoad := ph.targetLoadForInstance(targetInstance.ID())
	// First try to move shards from other subclusters
	instanceHeap, err := ph.buildInstanceHeap(ph.removeSubClusterInstances(targetInstance.SubClusterID()), false)
	if err != nil {
		return err
	}
	for targetInstance.Shards().NumShards() < targetLoad && instanceHeap.Len() > 0 {
		fromInstance := heap.Pop(instanceHeap).(placement.Instance)
		if moved := moveOneShardFn(fromInstance, targetInstance); moved {
			heap.Push(instanceHeap, fromInstance)
		}
	}
	// Then try to move shards from the same subcluster
	instanceHeap, err = ph.buildInstanceHeap(ph.getSubClusterInstances(targetInstance.SubClusterID()), false)
	if err != nil {
		return err
	}
	for targetInstance.Shards().NumShards() < targetLoad && instanceHeap.Len() > 0 {
		fromInstance := heap.Pop(instanceHeap).(placement.Instance)
		if moved := moveOneShardFn(fromInstance, targetInstance); moved {
			heap.Push(instanceHeap, fromInstance)
		}
	}
	return nil
}

func (ph *subclusteredHelper) targetLoadForInstance(id string) int {
	return ph.targetLoad[id]
}

func (ph *subclusteredHelper) moveOneShard(from, to placement.Instance) bool {
	return ph.moveOneShardInState(from, to, shard.Unknown) ||
		ph.moveOneShardInState(from, to, shard.Initializing) ||
		ph.moveOneShardInState(from, to, shard.Available)
}

func (ph *subclusteredHelper) moveOneShardInState(from, to placement.Instance, state shard.State) bool {
	shards := from.Shards().ShardsForState(state)
	toSubcluster := ph.subClusters[to.SubClusterID()]
	// we are randomly shuffling the shards to minimize the shard sharing percentage between
	// the replica sets within a subcluster
	if to.SubClusterID() == from.SubClusterID() || len(toSubcluster.shardMap) == toSubcluster.targetShardCount {
		shards = ph.randomShuffle(shards)
	} else {
		// we are greedy shuffling the shards to minimize the skew in the existing subclusters
		// when moving the shards to new subcluster. We sort the shards by the amount of skew it
		// will cause in the subcluster if all the shard replicas will be removed. We then take the
		// shard which will cause the least skew and move it to the new subcluster.
		shards = ph.greedyShuffle(shards, from)
	}
	for _, s := range shards {
		if ph.moveShard(s, from, to) {
			return true
		}
	}
	return false
}

func (ph *subclusteredHelper) randomShuffle(shards []shard.Shard) []shard.Shard {
	if len(shards) <= 1 {
		return shards
	}

	result := make([]shard.Shard, len(shards))
	copy(result, shards)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := len(result) - 1; i > 0; i-- {
		j := rng.Intn(i + 1)
		result[i], result[j] = result[j], result[i]
	}

	return result
}

func (ph *subclusteredHelper) greedyShuffle(shards []shard.Shard, fromInstance placement.Instance) []shard.Shard {
	if len(shards) == 0 {
		return shards
	}
	// Focus optimization specifically on minimizing skew within this subcluster
	return ph.optimizeForSubclusterBalance(shards, fromInstance)
}

// calculateSubclusterSkew computes the skew (max - min shard count) within a subcluster
func (ph *subclusteredHelper) calculateSubclusterSkew(instanceCounts map[string]int) int {
	if len(instanceCounts) == 0 {
		return 0
	}
	minCount := math.MaxInt32
	maxCount := 0
	for _, count := range instanceCounts {
		if count < minCount {
			minCount = count
		}
		if count > maxCount {
			maxCount = count
		}
	}

	return maxCount - minCount
}

// optimizeForSubclusterBalance orders shards to minimize subcluster skew during removal process
// Calculate actual skew after removal of each shard and sort by that for optimal ordering
func (ph *subclusteredHelper) optimizeForSubclusterBalance(
	shards []shard.Shard,
	fromInstance placement.Instance,
) []shard.Shard {
	if len(shards) <= 1 {
		// No optimization needed for single shard
		return shards
	}

	type shardSkewScore struct {
		shard            shard.Shard
		skewAfterRemoval int
	}

	shardScores := make([]shardSkewScore, 0, len(shards))
	fromSubcluster := ph.subClusters[fromInstance.SubClusterID()]
	instanceCounts := fromSubcluster.instanceShardCounts
	countAfterReplicaRemoval := make(map[string]int)
	for id, count := range instanceCounts {
		countAfterReplicaRemoval[id] = count
	}

	// Remove the count for the shards in the from instance whose one or more replicas have
	// already been moved to the new subcluster. These shard counts should not be counted in
	// skew calculation as they will be moved to the new subcluster.
	for s, count := range fromSubcluster.shardMap {
		if count == ph.rf {
			continue
		}
		for instance := range ph.shardToInstanceMap[s] {
			if instance.SubClusterID() == fromInstance.SubClusterID() {
				countAfterReplicaRemoval[instance.ID()]--
				if countAfterReplicaRemoval[instance.ID()] == 0 {
					delete(countAfterReplicaRemoval, instance.ID())
				}
			}
		}
	}
	for _, s := range shards {
		shardID := s.ID()
		tempCounts := make(map[string]int)
		for id, count := range countAfterReplicaRemoval {
			tempCounts[id] = count
		}

		if count, exists := fromSubcluster.shardMap[shardID]; exists && count < ph.rf {
			// if some of the replicas of the shards have been moved then that shard should be moved at last
			// we priopitize moving new shards  to the subcluster first until the target subcluster count
			// hasn't been reached.
			shardScores = append(shardScores, shardSkewScore{
				shard:            s,
				skewAfterRemoval: math.MaxInt32,
			})
			continue
		}

		// Find all instances in the subcluster that currently hold this shard and remove it
		if instancesWithShard, exists := ph.shardToInstanceMap[shardID]; exists {
			for instance := range instancesWithShard {
				// Only consider instances in the same subcluster to remove shard replicas count from.
				if instance.SubClusterID() == fromInstance.SubClusterID() && tempCounts[instance.ID()] > 0 {
					tempCounts[instance.ID()]-- // Remove one replica from this instance
				}
			}
		}

		// Calculate resulting skew within the subcluster after removing all replicas of this shard
		skewAfterRemoval := ph.calculateSubclusterSkew(tempCounts)
		shardScores = append(shardScores, shardSkewScore{
			shard:            s,
			skewAfterRemoval: skewAfterRemoval,
		})
	}

	// Sort by skewAfterRemoval (ascending) - prioritize shards that result in lowest skew when removed
	// For shards with the same skew, randomize their order to avoid deterministic bias
	sort.Slice(shardScores, func(i, j int) bool {
		if shardScores[i].skewAfterRemoval == shardScores[j].skewAfterRemoval {
			// Randomly shuffle equal skew shards for non-deterministic ordering
			return rand.Float64() < 0.5
		}
		return shardScores[i].skewAfterRemoval < shardScores[j].skewAfterRemoval
	})

	// Extract sorted shards
	result := make([]shard.Shard, len(shards))
	for i, score := range shardScores {
		result[i] = score.shard
	}

	return result
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
	var instances = make([]placement.Instance, 0, len(ph.instances))

	for _, instance := range ph.instances {
		if instance.Shards().NumShards() > 0 {
			instances = append(instances, instance)
		}
	}

	for _, instance := range instances {
		shards := instance.Shards()
		for _, s := range shards.ShardsForState(shard.Unknown) {
			shards.Add(shard.NewShard(s.ID()).
				SetSourceID(s.SourceID()).
				SetState(shard.Initializing).
				SetCutoverNanos(ph.opts.ShardCutoverNanosFn()()))
		}
	}

	return placement.NewPlacement().
		SetInstances(instances).
		SetShards(ph.uniqueShards).
		SetReplicaFactor(ph.rf).
		SetIsSharded(true).
		SetHasSubClusters(true).
		SetInstancesPerSubCluster(ph.instancesPerSubcluster).
		SetIsMirrored(ph.opts.IsMirrored()).
		SetCutoverNanos(ph.opts.PlacementCutoverNanosFn()())
}

// reclaimLeavingShards reclaims all the leaving shards on the given instance
// by pulling them back from the rest of the cluster.
// nolint: unused
func (ph *subclusteredHelper) reclaimLeavingShards(instance placement.Instance) {
	if instance.Shards().NumShardsForState(shard.Leaving) == 0 {
		// Shortcut if there is nothing to be reclaimed.
		return
	}
	id := instance.ID()
	for _, i := range ph.instances {
		for _, s := range i.Shards().ShardsForState(shard.Initializing) {
			if s.SourceID() == id {
				// NB(cw) in very rare case, the leaving shards could not be taken back.
				// For example: in a RF=2 case, instance a and b on ig1, instance c on ig2,
				// c took shard1 from instance a, before we tried to assign shard1 back to instance a,
				// b got assigned shard1, now if we try to add instance a back to the topology, a can
				// no longer take shard1 back.
				// But it's fine, the algo will fil up those load with other shards from the cluster
				ph.moveShard(s, i, instance)
			}
		}
	}
}

// returnInitializingShards returns all the initializing shards on the given instance
// by returning them back to the original owners.
// nolint: unused
func (ph *subclusteredHelper) returnInitializingShards(instance placement.Instance) {
	shardSet := getShardMap(instance.Shards().All())
	ph.returnInitializingShardsToSource(shardSet, instance, ph.Instances())
}

// nolint: dupl
func (ph *subclusteredHelper) returnInitializingShardsToSource(
	shardSet map[uint32]shard.Shard,
	from placement.Instance,
	candidates []placement.Instance,
) {
	candidateMap := make(map[string]placement.Instance, len(candidates))
	for _, candidate := range candidates {
		candidateMap[candidate.ID()] = candidate
	}
	for _, s := range shardSet {
		if s.State() != shard.Initializing {
			continue
		}
		sourceID := s.SourceID()
		if sourceID == "" {
			continue
		}
		sourceInstance, ok := candidateMap[sourceID]
		if !ok {
			continue
		}
		if sourceInstance.IsLeaving() {
			continue
		}
		if ph.moveShard(s, from, sourceInstance) {
			delete(shardSet, s.ID())
		}
	}
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

func (ph *subclusteredHelper) buildInstanceHeap(
	instances []placement.Instance,
	availableCapacityAscending bool,
) (heap.Interface, error) {
	return newHeap(instances, availableCapacityAscending, ph.targetLoad, ph.groupToWeightMap, true)
}

// removeSubClusterInstances returns instances that are not in the specified subcluster
func (ph *subclusteredHelper) removeSubClusterInstances(subclusterID uint32) []placement.Instance {
	var instances = nonLeavingInstances(ph.Instances())
	var result = make([]placement.Instance, 0, len(instances))
	for _, instance := range instances {
		if instance.SubClusterID() != subclusterID {
			result = append(result, instance)
		}
	}
	return result
}

// getSubClusterInstances returns instances that are in the specified subcluster
func (ph *subclusteredHelper) getSubClusterInstances(subclusterID uint32) []placement.Instance {
	currSubcluster := ph.subClusters[subclusterID]
	var instances = make([]placement.Instance, 0, len(currSubcluster.instances))
	for _, instance := range currSubcluster.instances {
		if instance.IsLeaving() {
			continue
		}
		instances = append(instances, instance)
	}
	return instances
}

func (ph *subclusteredHelper) validatePartialSubclusters(excludeSubclusterID uint32, op validationOperation) error {
	for subclusterID, subcluster := range ph.subClusters {
		if subclusterID == excludeSubclusterID {
			continue
		}
		if len(subcluster.instances) < ph.instancesPerSubcluster {
			var operation string
			switch op {
			case validationOpRemoval:
				operation = "removed"
			case validationOpAddition:
				operation = "added"
			}
			return fmt.Errorf("partial subcluster %d is present with %d instances, while a subcluster %d is being %s",
				subclusterID, len(subcluster.instances), excludeSubclusterID, operation)
		}
	}
	return nil
}

// getSubClusterInstances returns instances that are in the specified subcluster
func getSubClusterInstances(instances []placement.Instance, subclusterID uint32) []placement.Instance {
	var result []placement.Instance
	for _, instance := range instances {
		if instance.SubClusterID() == subclusterID {
			result = append(result, instance)
		}
	}
	return result
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
