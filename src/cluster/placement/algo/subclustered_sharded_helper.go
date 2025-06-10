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
	"container/heap"
	"fmt"
	"math"
	"sort"

	"go.uber.org/zap"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
)

type subclusteredHelper struct {
	targetLoad          map[string]int
	shardToInstanceMap  map[uint32]map[placement.Instance]struct{}
	groupToInstancesMap map[string]map[placement.Instance]struct{}
	groupToWeightMap    map[string]uint32
	subClusters         map[uint32]*subcluster
	rf                  int
	uniqueShards        []uint32
	instances           map[string]placement.Instance
	log                 *zap.Logger
	opts                placement.Options
	totalWeight         uint32
	maxShardSetID       uint32
}

type subcluster struct {
	id                  uint32
	targetShardCount    int
	instances           map[string]placement.Instance
	shardMap            map[uint32]int
	instanceShardCounts map[string]int
}

func newSubclusteredInitHelper(instances []placement.Instance, ids []uint32, opts placement.Options) (placementHelper, error) {
	emptyPlacement := placement.NewPlacement().
		SetInstances(instances).
		SetShards(ids).
		SetReplicaFactor(0).
		SetIsSharded(true).
		SetCutoverNanos(opts.PlacementCutoverNanosFn()())
	ph, err := newubclusteredHelper(emptyPlacement, emptyPlacement.ReplicaFactor()+1, opts, 0)
	if err != nil {
		return nil, err
	}
	return ph, nil
}

func newubclusteredAddReplicaHelper(p placement.Placement, opts placement.Options) (placementHelper, error) {
	ph, err := newubclusteredHelper(p, p.ReplicaFactor()+1, opts, 0)
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
		if err := assignSubClusterID(p, opts, []placement.Instance{instance}); err != nil {
			return nil, nil, err
		}
		ph, err := newubclusteredHelper(p.SetInstances(append(p.Instances(), instance)), p.ReplicaFactor(), opts, 0)
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

	ph, err := newubclusteredHelper(p, p.ReplicaFactor(), opts, 0)
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
	if len(subclusterInstances) == opts.InstancesPerSubCluster() {
		ph, err := newubclusteredHelper(p, p.ReplicaFactor(), opts, 0)
		if err != nil {
			return nil, nil, err
		}
		return ph, leavingInstance, nil
	}
	ph, err := newubclusteredHelper(p, p.ReplicaFactor(), opts, leavingInstance.SubClusterID())
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

	// Match adding instances with leaving instances
	for i, addingInstance := range newAddingInstances {
		addingInstance.SetSubClusterID(leavingInstances[i].SubClusterID())
	}
	ph, err := newubclusteredHelper(p, p.ReplicaFactor(), opts, 0)
	if err != nil {
		return nil, nil, nil, err
	}
	return ph, leavingInstances, newAddingInstances, nil
}

func newubclusteredHelper(p placement.Placement, targetRF int, opts placement.Options, subClusterToExclude uint32) (placementHelper, error) {
	ph := &subclusteredHelper{
		rf:            targetRF,
		instances:     make(map[string]placement.Instance, p.NumInstances()),
		uniqueShards:  p.Shards(),
		maxShardSetID: p.MaxShardSetID(),
		log:           opts.InstrumentOptions().Logger(),
		opts:          opts,
	}

	for _, instance := range p.Instances() {
		ph.instances[instance.ID()] = instance
	}
	err := ph.validateInstanceWeight(p.Instances())
	if err != nil {
		return nil, err
	}

	ph.scanCurrentLoad()
	ph.buildTargetLoad()
	ph.buildTargetSubclusterLoad(subClusterToExclude)
	return ph, nil
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
			// Leaving instances are not counted as usable capacities in the placement.
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

		ph.groupToWeightMap[instance.IsolationGroup()] = ph.groupToWeightMap[instance.IsolationGroup()] + 1 // if we are checking that all instance weight is same than we can simply the calculation by assuming it as 1
		totalWeight += 1
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
			// We should not set a target load for leaving instances.
			continue
		}
		igWeight := ph.groupToWeightMap[instance.IsolationGroup()]
		if isOverWeighted(igWeight, ph.totalWeight, ph.rf) {
			// If the instance is on a over-sized isolation group, the target load
			// equals (shardLen / capacity of the isolation group).
			targetLoad[instance.ID()] = int(math.Ceil(float64(ph.getShardLen()) * float64(instance.Weight()) / float64(igWeight)))
		} else {
			// If the instance is on a normal isolation group, get the target load
			// with aware of other over-sized isolation group.
			targetLoad[instance.ID()] = ph.getShardLen() * (ph.rf - overWeightedGroups) * int(instance.Weight()) / int(ph.totalWeight-overWeight)
		}
	}
	ph.targetLoad = targetLoad
}
func getKeys[K comparable, V any](m map[K]V, excludeKey K) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		if k == excludeKey {
			continue
		}
		keys = append(keys, k)
	}
	return keys // Returns zero value of K and false if map is empty
}

func (ph *subclusteredHelper) buildTargetSubclusterLoad(subClusterToExclude uint32) {
	totalShards := len(ph.uniqueShards)
	subClusters := getKeys(ph.subClusters, subClusterToExclude)
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

func (ph *subclusteredHelper) Instances() []placement.Instance {
	res := make([]placement.Instance, 0, len(ph.instances))
	for _, instance := range ph.instances {
		res = append(res, instance)
	}
	return res
}

func (ph *subclusteredHelper) getShardLen() int {
	return len(ph.uniqueShards)
}

func (ph *subclusteredHelper) targetLoadForInstance(id string) int {
	return ph.targetLoad[id]
}

func (ph *subclusteredHelper) moveOneShard(from, to placement.Instance) bool {
	// The order matter here:
	// The Unknown shards were just moved, so free to be moved around.
	// The Initializing shards were still being initialized on the instance,
	// so moving them are cheaper than moving those Available shards.
	return ph.moveOneShardInState(from, to, shard.Unknown) ||
		ph.moveOneShardInState(from, to, shard.Initializing) ||
		ph.moveOneShardInState(from, to, shard.Available)
}

// nolint: unparam
func (ph *subclusteredHelper) moveOneShardInState(from, to placement.Instance, state shard.State) bool {
	shards := from.Shards().ShardsForState(state)
	// Use context-aware shuffling to minimize skew when removing shards
	shuffledShards := ph.shuffleShardsWithContext(shards, from)
	for _, s := range shuffledShards {
		if ph.moveShard(s, from, to) {
			return true
		}
	}
	return false
}

func (ph *subclusteredHelper) moveShard(candidateShard shard.Shard, from, to placement.Instance) bool {
	shardID := candidateShard.ID()
	if !ph.canAssignInstance(shardID, from, to) {
		return false
	}

	if candidateShard.State() == shard.Leaving {
		// should not move a Leaving shard,
		// Leaving shard will be removed when the Initializing shard is marked as Available
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

		delete(ph.shardToInstanceMap[shardID], from)
		if fromsubcluster, exist := ph.subClusters[from.SubClusterID()]; exist {
			fromsubcluster.shardMap[shardID] -= 1
			if fromsubcluster.shardMap[shardID] == 0 {
				delete(fromsubcluster.shardMap, shardID)
			}
			fromsubcluster.instanceShardCounts[from.ID()] -= 1
			if fromsubcluster.instanceShardCounts[from.ID()] == 0 {
				delete(fromsubcluster.instanceShardCounts, from.ID())
			}
			for instance := range ph.shardToInstanceMap[shardID] {
				if instance.SubClusterID() == from.SubClusterID() {
					fromsubcluster.instanceShardCounts[instance.ID()] -= 1
					if fromsubcluster.instanceShardCounts[instance.ID()] == 0 {
						delete(fromsubcluster.instanceShardCounts, instance.ID())
					}
				}
			}
		}
	}

	curShard, ok := to.Shards().Shard(shardID)
	if ok && curShard.State() == shard.Leaving {
		// NB(cw): if the instance already owns the shard in Leaving state,
		// simply mark it as Available
		newShard = shard.NewShard(shardID).SetState(shard.Available)
		// NB(cw): Break the link between new owner of this shard with this Leaving instance
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

func (ph *subclusteredHelper) buildInstanceHeap(instances []placement.Instance, availableCapacityAscending bool) (heap.Interface, error) {
	return newHeap(instances, availableCapacityAscending, ph.targetLoad, ph.groupToWeightMap)
}

func (ph *subclusteredHelper) generatePlacement() placement.Placement {
	var instances = make([]placement.Instance, 0, len(ph.instances))

	for _, instance := range ph.instances {
		if instance.Shards().NumShards() > 0 {
			instances = append(instances, instance)
		}
	}

	maxShardSetID := ph.maxShardSetID
	for _, instance := range instances {
		shards := instance.Shards()
		for _, s := range shards.ShardsForState(shard.Unknown) {
			shards.Add(shard.NewShard(s.ID()).
				SetSourceID(s.SourceID()).
				SetState(shard.Initializing).
				SetCutoverNanos(ph.opts.ShardCutoverNanosFn()()))
		}
		if shardSetID := instance.ShardSetID(); shardSetID >= maxShardSetID {
			maxShardSetID = shardSetID
		}
	}

	return placement.NewPlacement().
		SetInstances(instances).
		SetShards(ph.uniqueShards).
		SetReplicaFactor(ph.rf).
		SetIsSharded(true).
		SetIsMirrored(ph.opts.IsMirrored()).
		SetCutoverNanos(ph.opts.PlacementCutoverNanosFn()()).
		SetMaxShardSetID(maxShardSetID)
}

func (ph *subclusteredHelper) placeShards(
	shards []shard.Shard,
	from placement.Instance,
	candidates []placement.Instance,
) error {
	shardSet := getShardMap(shards)
	if from != nil {
		// NB(cw) when removing an adding instance that has not finished bootstrapping its
		// Initializing shards, prefer to return those Initializing shards back to the leaving instance
		// to reduce some bootstrapping work in the cluster.
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
			// This should only happen when RF > number of isolation groups.
			return errNotEnoughIsolationGroups
		}
		for _, triedInstance := range triedInstances {
			heap.Push(instanceHeap, triedInstance)
		}
		triedInstances = triedInstances[:0]
	}
	return nil
}

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
			// NB(cw): This is not an error because the candidates are not
			// necessarily all the instances in the placement.
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

func (ph *subclusteredHelper) mostUnderLoadedInstance() (placement.Instance, bool) {
	var (
		res              placement.Instance
		maxLoadGap       int
		totalLoadSurplus int
	)

	for id, instance := range ph.instances {
		loadGap := ph.targetLoad[id] - loadOnInstance(instance)
		if loadGap > maxLoadGap {
			maxLoadGap = loadGap
			res = instance
		}
		if loadGap == maxLoadGap && res != nil && res.ID() > id {
			res = instance
		}
		if loadGap < 0 {
			totalLoadSurplus -= loadGap
		}
	}
	if maxLoadGap > 0 && totalLoadSurplus != 0 {
		return res, true
	}
	return nil, false
}

func (ph *subclusteredHelper) optimize(t optimizeType) error {
	var fn assignLoadFn
	switch t {
	case safe:
		fn = ph.assignLoadToInstanceSafe
	case unsafe:
		fn = ph.assignLoadToInstanceUnsafe
	}
	uniq := make(map[string]struct{}, len(ph.instances))
	for {
		ins, ok := ph.mostUnderLoadedInstance()
		if !ok {
			return nil
		}
		if _, exist := uniq[ins.ID()]; exist {
			return nil
		}

		uniq[ins.ID()] = struct{}{}
		// fmt.Printf("assigning load to instance %s, target load: %d, current load: %d, subcluster id: %d\n", ins.ID(), ph.targetLoad[ins.ID()], loadOnInstance(ins), ins.SubClusterID())
		if err := fn(ins); err != nil {
			return err
		}
	}
}

func (ph *subclusteredHelper) assignLoadToInstanceSafe(addingInstance placement.Instance) error {
	return ph.assignTargetLoad(addingInstance, func(from, to placement.Instance) bool {
		return ph.moveOneShardInState(from, to, shard.Unknown)
	})
}

func (ph *subclusteredHelper) assignLoadToInstanceUnsafe(addingInstance placement.Instance) error {
	return ph.assignTargetLoad(addingInstance, func(from, to placement.Instance) bool {
		return ph.moveOneShard(from, to)
	})
}

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

func (ph *subclusteredHelper) addInstance(addingInstance placement.Instance) error {
	ph.reclaimLeavingShards(addingInstance)
	return ph.assignLoadToInstanceUnsafe(addingInstance)
}

func (ph *subclusteredHelper) assignTargetLoad(
	targetInstance placement.Instance,
	moveOneShardFn func(from, to placement.Instance) bool,
) error {
	targetLoad := ph.targetLoadForInstance(targetInstance.ID())
	// try to take shards from the most loaded instances until the adding instance reaches target load
	instanceHeap, err := ph.buildInstanceHeap(removeSubClusterInstances(nonLeavingInstances(ph.Instances()), targetInstance.SubClusterID()), false)
	if err != nil {
		return err
	}
	for targetInstance.Shards().NumShards() < targetLoad && instanceHeap.Len() > 0 {
		fromInstance := heap.Pop(instanceHeap).(placement.Instance)
		if moved := moveOneShardFn(fromInstance, targetInstance); moved {
			heap.Push(instanceHeap, fromInstance)
		}
	}
	instanceHeap, err = ph.buildInstanceHeap(nonLeavingInstances(getSubClusterInstances(ph.Instances(), targetInstance.SubClusterID())), false)
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

func (ph *subclusteredHelper) canAssignInstance(shardID uint32, from, to placement.Instance) bool {
	s, ok := to.Shards().Shard(shardID)
	if ok && s.State() != shard.Leaving {
		// NB(cw): a Leaving shard is not counted to the load of the instance
		// so the instance should be able to take the ownership back if needed
		// assuming i1 owns shard 1 as Available, this case can be triggered by:
		// 1: add i2, now shard 1 is "Leaving" on i1 and "Initializing" on i2
		// 2: remove i2, now i2 needs to return shard 1 back to i1
		// and i1 should be able to take it and mark it as "Available"
		return false
	}
	tosubcluster := ph.subClusters[to.SubClusterID()]
	if tosubcluster.targetShardCount == 0 {
		return false
	}
	if from != nil {
		if from.SubClusterID() == to.SubClusterID() {
			return ph.CanMoveShard(shardID, from, to.IsolationGroup())
		}
	}
	if len(tosubcluster.shardMap) == tosubcluster.targetShardCount {
		if _, exists := tosubcluster.shardMap[shardID]; !exists {
			return false
		}
	}
	for instance := range ph.shardToInstanceMap[shardID] {
		if from != nil {
			if instance.SubClusterID() == from.SubClusterID() {
				continue
			}
			intersection := findMapKeyIntersection(tosubcluster.shardMap, ph.subClusters[instance.SubClusterID()].shardMap)
			if _, exist := intersection[shardID]; exist {
				continue
			}
		}
		if instance.SubClusterID() != to.SubClusterID() {
			return false
		}
	}
	if from != nil && from.SubClusterID() != to.SubClusterID() {
		fromsubcluster, exist := ph.subClusters[from.SubClusterID()]
		if exist && len(fromsubcluster.shardMap) == fromsubcluster.targetShardCount {
			return false
		}
		if exist && len(fromsubcluster.shardMap) > fromsubcluster.targetShardCount {
			intersection := findMapKeyIntersection(tosubcluster.shardMap, fromsubcluster.shardMap)
			if _, exist := intersection[shardID]; !exist && len(intersection) == (len(fromsubcluster.shardMap)-fromsubcluster.targetShardCount) {
				return false
			}
		}
	}
	return ph.CanMoveShard(shardID, from, to.IsolationGroup())
}

func (ph *subclusteredHelper) assignShardToInstance(s shard.Shard, to placement.Instance) {
	to.Shards().Add(s)

	if _, exist := ph.shardToInstanceMap[s.ID()]; !exist {
		ph.shardToInstanceMap[s.ID()] = make(map[placement.Instance]struct{})
	}
	ph.shardToInstanceMap[s.ID()][to] = struct{}{}
	ph.subClusters[to.SubClusterID()].shardMap[s.ID()] += 1
	ph.subClusters[to.SubClusterID()].instanceShardCounts[to.ID()] += 1
}

func (ph *subclusteredHelper) validateInstanceWeight(instances []placement.Instance) error {
	if len(instances) == 0 {
		return nil
	}

	// Get the expected weight from the first instance
	expectedWeight := instances[0].Weight()

	// Check that each and every instance has the same weight
	for _, instance := range instances {
		if instance.Weight() != expectedWeight {
			return fmt.Errorf("inconsistent instance weights: instance %s has weight %d, expected %d",
				instance.ID(), instance.Weight(), expectedWeight)
		}
	}

	return nil
}

func removeSubclusterInstancesFromPlacement(p placement.Placement, subclusterID uint32) (placement.Placement, []placement.Instance) {
	instancesRemoved := []placement.Instance{}
	for _, instance := range p.Instances() {
		if instance.SubClusterID() == subclusterID {
			instancesRemoved = append(instancesRemoved, instance)
			p = p.SetInstances(removeInstanceFromList(p.Instances(), instance.ID()))
		}
	}
	return p, instancesRemoved
}

// findMapKeyIntersection returns a map containing keys that exist in both input maps
func findMapKeyIntersection(map1, map2 map[uint32]int) map[uint32]struct{} {
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

// shuffleShards returns shards ordered to minimize skew when removing them from a subcluster.
// Skew is the difference between maximum and minimum shard count across instances in the subcluster.
// The function prioritizes removing shards from instances with higher shard counts first to balance distribution.
func shuffleShards(shards []shard.Shard) []shard.Shard {
	if len(shards) == 0 {
		return shards
	}

	// For now, return shards in original order since we need placement context
	// to determine optimal ordering. This maintains existing behavior while
	// providing a foundation for future enhancement.
	result := make([]shard.Shard, len(shards))
	copy(result, shards)
	return result
}

// shuffleShardsWithContext returns shards ordered to minimize skew when removing them from a subcluster.
// This version uses the helper's subcluster map to determine optimal ordering for shard removal.
// Skew is minimized by focusing specifically on balance within the target subcluster.
func (ph *subclusteredHelper) shuffleShardsWithContext(shards []shard.Shard, fromInstance placement.Instance) []shard.Shard {
	if len(shards) == 0 {
		return shards
	}

	// Get all instances in the same subcluster as fromInstance using the helper's subcluster map
	subclusterID := fromInstance.SubClusterID()
	subcluster, exists := ph.subClusters[subclusterID]
	if !exists {
		// If subcluster doesn't exist, return shards as-is
		result := make([]shard.Shard, len(shards))
		copy(result, shards)
		return result
	}

	// Convert subcluster instances map to slice
	subclusterInstances := make([]placement.Instance, 0, len(subcluster.instances))
	for _, instance := range subcluster.instances {
		subclusterInstances = append(subclusterInstances, instance)
	}

	if len(subclusterInstances) <= 1 {
		// If there's only one instance in the subcluster, skew optimization is not applicable
		result := make([]shard.Shard, len(shards))
		copy(result, shards)
		return result
	}

	// Focus optimization specifically on minimizing skew within this subcluster
	return ph.optimizeShardRemovalOrder(shards, fromInstance)
}

// optimizeShardRemovalOrder determines the best order to remove shards to minimize skew within the subcluster.
// It focuses specifically on subcluster-level skew optimization by considering how each removal affects
// the balance across all instances within the same subcluster.
func (ph *subclusteredHelper) optimizeShardRemovalOrder(shards []shard.Shard, fromInstance placement.Instance) []shard.Shard {
	if len(shards) == 0 {
		return shards
	}

	// Get current shard counts for all instances in this specific subcluster
	subclusterShardCounts := ph.subClusters[fromInstance.SubClusterID()].instanceShardCounts

	// Use a more sophisticated approach: prioritize shards whose removal brings us closer to perfect balance
	return ph.optimizeForSubclusterBalance(shards, subclusterShardCounts)
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

// calculateSubclusterStandardDeviation computes the standard deviation of shard counts within a subcluster
// This provides a more nuanced measure of balance than just skew (max - min)
func (ph *subclusteredHelper) calculateSubclusterStandardDeviation(instanceCounts map[string]int) float64 {
	if len(instanceCounts) == 0 {
		return 0.0
	}

	// Calculate mean
	sum := 0
	for _, count := range instanceCounts {
		sum += count
	}
	mean := float64(sum) / float64(len(instanceCounts))

	// Calculate variance
	variance := 0.0
	for _, count := range instanceCounts {
		diff := float64(count) - mean
		variance += diff * diff
	}
	variance /= float64(len(instanceCounts))

	// Return standard deviation
	return math.Sqrt(variance)
}

// optimizeForSubclusterBalance orders shards to minimize subcluster skew during removal process
// Calculate actual skew after removal of each shard and sort by that for optimal ordering
func (ph *subclusteredHelper) optimizeForSubclusterBalance(shards []shard.Shard, instanceCounts map[string]int) []shard.Shard {
	if len(shards) <= 1 {
		// No optimization needed for single shard
		return shards
	}

	// Calculate skew after removing each shard, then sort by resulting skew
	type shardSkewScore struct {
		shard            shard.Shard
		skewAfterRemoval int
	}

	shardScores := make([]shardSkewScore, 0, len(shards))

	for _, s := range shards {
		shardID := s.ID()

		// Simulate removing ALL replicas of this shard from the subcluster
		tempCounts := make(map[string]int)
		for id, count := range instanceCounts {
			tempCounts[id] = count
		}

		// Find all instances in the subcluster that currently hold this shard and remove it
		if instancesWithShard, exists := ph.shardToInstanceMap[shardID]; exists {
			for instance := range instancesWithShard {
				// Only consider instances in the same subcluster
				if instanceID := instance.ID(); tempCounts[instanceID] > 0 {
					tempCounts[instanceID]-- // Remove one replica from this instance
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
	sort.Slice(shardScores, func(i, j int) bool {
		return shardScores[i].skewAfterRemoval < shardScores[j].skewAfterRemoval
	})

	// Extract sorted shards
	result := make([]shard.Shard, len(shards))
	for i, score := range shardScores {
		result[i] = score.shard
	}

	return result
}

// removeSubClusterInstances returns instances that are not in the specified subcluster
func removeSubClusterInstances(instances []placement.Instance, subclusterID uint32) []placement.Instance {
	var result []placement.Instance
	for _, instance := range instances {
		if instance.SubClusterID() != subclusterID {
			result = append(result, instance)
		}
	}
	return result
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
