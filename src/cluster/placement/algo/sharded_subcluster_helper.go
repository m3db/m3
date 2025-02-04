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
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"

	"go.uber.org/zap"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
)

type balanceWeighType int

const (
	none balanceWeighType = iota
	addInstance
	removeInstance
)

type SubClusterByID []subCluster

func (s SubClusterByID) Len() int { return len(s) }

func (s SubClusterByID) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s SubClusterByID) Less(i, j int) bool {
	return s[i].ID < s[j].ID
}

type subCluster struct {
	ID                  uint32
	instances           map[placement.Instance]struct{}
	groupToInstancesMap map[string][]placement.Instance
	shards              map[uint32]int
	weight              int
}

type subClusterShardedHelper struct {
	targetLoad                 map[string]int
	shardToInstanceMap         map[uint32]map[placement.Instance]struct{}
	groupToInstancesMap        map[string]map[placement.Instance]struct{}
	groupToWeightMap           map[string]uint32
	subClusterMap              map[uint32]subCluster
	currentShardsPerSubCluster map[uint32]int
	targetShardsPerSubCluster  map[uint32]int
	rf                         int
	subClusters                []uint32
	uniqueShards               []uint32
	instances                  map[string]placement.Instance
	log                        *zap.Logger
	opts                       placement.Options
	totalWeight                uint32
	maxShardSetID              uint32
}

// NewSubClusteredPlacementHelper returns a placement helper
func NewSubClusteredPlacementHelper(p placement.Placement, opts placement.Options) PlacementHelper {
	return newHelper(p, p.ReplicaFactor(), opts)
}

func newInitSubClusterHelper(instances []placement.Instance, ids []uint32, opts placement.Options) placementHelper {
	emptyPlacement := placement.NewPlacement().
		SetInstances(instances).
		SetShards(ids).
		SetReplicaFactor(0).
		SetIsSharded(true).
		SetHasSubClusters(true).
		SetCutoverNanos(opts.PlacementCutoverNanosFn()())
	return newSubClusterHelper(emptyPlacement, emptyPlacement.ReplicaFactor()+1, none, opts)
}

func newSubClusteredAddReplicaHelper(p placement.Placement, opts placement.Options) placementHelper {
	return newSubClusterHelper(p, p.ReplicaFactor()+1, none, opts)
}

func newSubClusterAddInstanceHelper(
	p placement.Placement,
	instance placement.Instance,
	opts placement.Options,
	t instanceType,
) (placementHelper, placement.Instance, error) {
	instanceInPlacement, exist := p.Instance(instance.ID())
	if !exist {
		return newSubClusterHelper(p.SetInstances(append(p.Instances(), instance)), p.ReplicaFactor(), addInstance, opts), instance, nil
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

	return newSubClusterHelper(p, p.ReplicaFactor(), addInstance, opts), instanceInPlacement, nil
}

//func newRemoveInstanceHelper(
//	p placement.Placement,
//	instanceID string,
//	opts placement.Options,
//) (placementHelper, placement.Instance, error) {
//	p, leavingInstance, err := removeInstanceFromPlacement(p, instanceID)
//	if err != nil {
//		return nil, nil, err
//	}
//	return newHelper(p, p.ReplicaFactor(), opts), leavingInstance, nil
//}
//
//func newReplaceInstanceHelper(
//	p placement.Placement,
//	instanceIDs []string,
//	addingInstances []placement.Instance,
//	opts placement.Options,
//) (placementHelper, []placement.Instance, []placement.Instance, error) {
//	var (
//		leavingInstances = make([]placement.Instance, len(instanceIDs))
//		err              error
//	)
//	for i, instanceID := range instanceIDs {
//		p, leavingInstances[i], err = removeInstanceFromPlacement(p, instanceID)
//		if err != nil {
//			return nil, nil, nil, err
//		}
//	}
//
//	newAddingInstances := make([]placement.Instance, len(addingInstances))
//	for i, instance := range addingInstances {
//		p, newAddingInstances[i], err = addInstanceToPlacement(p, instance, anyType)
//		if err != nil {
//			return nil, nil, nil, err
//		}
//	}
//	return newHelper(p, p.ReplicaFactor(), opts), leavingInstances, newAddingInstances, nil
//}

func newSubClusterHelper(p placement.Placement, targetRF int, balanceType balanceWeighType, opts placement.Options) placementHelper {
	sph := &subClusterShardedHelper{
		rf:            targetRF,
		instances:     make(map[string]placement.Instance, p.NumInstances()),
		uniqueShards:  p.Shards(),
		maxShardSetID: p.MaxShardSetID(),
		log:           opts.InstrumentOptions().Logger(),
		opts:          opts,
	}

	for _, instance := range p.Instances() {
		sph.instances[instance.ID()] = instance
	}

	sph.scanAndValidateSubClusterLoad()
	sph.buildTargetSubClusterLoad(balanceType)

	sph.scanCurrentLoad()
	sph.buildTargetLoad()
	return sph
}

func (sph *subClusterShardedHelper) scanCurrentLoad() {
	sph.shardToInstanceMap = make(map[uint32]map[placement.Instance]struct{}, len(sph.uniqueShards))
	sph.groupToInstancesMap = make(map[string]map[placement.Instance]struct{})
	sph.groupToWeightMap = make(map[string]uint32)
	totalWeight := uint32(0)
	for _, instance := range sph.instances {
		if _, exist := sph.groupToInstancesMap[instance.IsolationGroup()]; !exist {
			sph.groupToInstancesMap[instance.IsolationGroup()] = make(map[placement.Instance]struct{})
		}
		sph.groupToInstancesMap[instance.IsolationGroup()][instance] = struct{}{}

		if instance.IsLeaving() {
			// Leaving instances are not counted as usable capacities in the placement.
			continue
		}

		sph.groupToWeightMap[instance.IsolationGroup()] = sph.groupToWeightMap[instance.IsolationGroup()] + instance.Weight()
		totalWeight += instance.Weight()

		for _, s := range instance.Shards().All() {
			if s.State() == shard.Leaving {
				continue
			}
			sph.assignShardToInstance(s, instance)
		}
	}
	sph.totalWeight = totalWeight
}

func (sph *subClusterShardedHelper) buildTargetLoad() {
	overWeightedGroups := 0
	overWeight := uint32(0)
	for _, weight := range sph.groupToWeightMap {
		if isOverWeighted(weight, sph.totalWeight, sph.rf) {
			overWeightedGroups++
			overWeight += weight
		}
	}

	targetLoad := make(map[string]int, len(sph.instances))
	for _, instance := range sph.instances {
		if instance.IsLeaving() {
			// We should not set a target load for leaving instances.
			continue
		}
		igWeight := sph.groupToWeightMap[instance.IsolationGroup()]
		if isOverWeighted(igWeight, sph.totalWeight, sph.rf) {
			// If the instance is on a over-sized isolation group, the target load
			// equals (shardLen / capacity of the isolation group).
			targetLoad[instance.ID()] = int(math.Ceil(float64(sph.getShardLen()) * float64(instance.Weight()) / float64(igWeight)))
		} else {
			// If the instance is on a normal isolation group, get the target load
			// with aware of other over-sized isolation group.
			targetLoad[instance.ID()] = sph.getShardLen() * (sph.rf - overWeightedGroups) * int(instance.Weight()) / int(sph.totalWeight-overWeight)
		}
	}
	sph.targetLoad = targetLoad
}

func (sph *subClusterShardedHelper) Instances() []placement.Instance {
	res := make([]placement.Instance, 0, len(sph.instances))
	for _, instance := range sph.instances {
		res = append(res, instance)
	}
	return res
}

func (sph *subClusterShardedHelper) getShardLen() int {
	return len(sph.uniqueShards)
}

func (sph *subClusterShardedHelper) targetLoadForInstance(id string) int {
	return sph.targetLoad[id]
}

func (sph *subClusterShardedHelper) moveOneShard(from, to placement.Instance) bool {
	// The order matter here:
	// The Unknown shards were just moved, so free to be moved around.
	// The Initializing shards were still being initialized on the instance,
	// so moving them are cheaper than moving those Available shards.
	return sph.moveOneShardInState(from, to, shard.Unknown) ||
		sph.moveOneShardInState(from, to, shard.Initializing) ||
		sph.moveOneShardInState(from, to, shard.Available)
}

// nolint: unparam
func (sph *subClusterShardedHelper) moveOneShardInState(from, to placement.Instance, state shard.State) bool {
	shardsToMoveFrom := from.Shards().ShardsForState(state)
	sort.Sort(ShardByID(shardsToMoveFrom))
	deterministicShuffle(shardsToMoveFrom, int64(len(sph.groupToInstancesMap)))
	for _, s := range shardsToMoveFrom {
		if sph.moveShard(s, from, to) {
			sph.updateSubClusterLoad(s, from, to)
			return true
		}
	}
	return false
}

func (sph *subClusterShardedHelper) moveShard(candidateShard shard.Shard, from, to placement.Instance) bool {
	shardID := candidateShard.ID()
	if !sph.canAssignInstance(shardID, from, to) {
		return false
	}

	if candidateShard.State() == shard.Leaving {
		// should not move a Leaving shard,
		// Leaving shard will be removed when the Initializing shard is marked as Available
		return false
	}

	newShard := shard.NewShard(shardID)

	if from != nil {
		switch candidateShard.State() {
		case shard.Unknown, shard.Initializing:
			from.Shards().Remove(shardID)
			newShard.SetSourceID(candidateShard.SourceID())
		case shard.Available:
			candidateShard.
				SetState(shard.Leaving).
				SetCutoffNanos(sph.opts.ShardCutoffNanosFn()())
			newShard.SetSourceID(from.ID())
		}

		delete(sph.shardToInstanceMap[shardID], from)
	}

	curShard, ok := to.Shards().Shard(shardID)
	if ok && curShard.State() == shard.Leaving {
		// NB(cw): if the instance already owns the shard in Leaving state,
		// simply mark it as Available
		newShard = shard.NewShard(shardID).SetState(shard.Available)
		// NB(cw): Break the link between new owner of this shard with this Leaving instance
		instances := sph.shardToInstanceMap[shardID]
		for instance := range instances {
			shards := instance.Shards()
			initShard, ok := shards.Shard(shardID)
			if ok && initShard.SourceID() == to.ID() {
				initShard.SetSourceID("")
			}
		}

	}

	sph.assignShardToInstance(newShard, to)
	return true
}

func (sph *subClusterShardedHelper) CanMoveShard(shard uint32, from placement.Instance, toIsolationGroup string) bool {
	if from != nil {
		if from.IsolationGroup() == toIsolationGroup {
			return true
		}
	}
	for instance := range sph.shardToInstanceMap[shard] {
		if instance.IsolationGroup() == toIsolationGroup {
			return false
		}
	}
	return true
}

func (sph *subClusterShardedHelper) buildInstanceHeap(instances []placement.Instance, availableCapacityAscending bool) (heap.Interface, error) {
	return newHeap(instances, availableCapacityAscending, sph.targetLoad, sph.groupToWeightMap)
}

func (sph *subClusterShardedHelper) generatePlacement() placement.Placement {
	var instances = make([]placement.Instance, 0, len(sph.instances))

	for _, instance := range sph.instances {
		if instance.Shards().NumShards() > 0 {
			instances = append(instances, instance)
		}
	}

	maxShardSetID := sph.maxShardSetID
	for _, instance := range instances {
		shards := instance.Shards()
		for _, s := range shards.ShardsForState(shard.Unknown) {
			shards.Add(shard.NewShard(s.ID()).
				SetSourceID(s.SourceID()).
				SetState(shard.Initializing).
				SetCutoverNanos(sph.opts.ShardCutoverNanosFn()()))
		}
		if shardSetID := instance.ShardSetID(); shardSetID >= maxShardSetID {
			maxShardSetID = shardSetID
		}
	}

	return placement.NewPlacement().
		SetInstances(instances).
		SetShards(sph.uniqueShards).
		SetReplicaFactor(sph.rf).
		SetIsSharded(true).
		SetIsMirrored(sph.opts.IsMirrored()).
		SetCutoverNanos(sph.opts.PlacementCutoverNanosFn()()).
		SetMaxShardSetID(maxShardSetID).SetHasSubClusters(true)
}
func (sph *subClusterShardedHelper) updateSubClusterLoad(shardMoved shard.Shard, fromInstance, toInstance placement.Instance) error {
	var fromInstanceSubClusterID uint32
	if fromInstance != nil {
		fromInstanceSubClusterID = fromInstance.SubClusterID()
	}
	toInstanceSubClusterID := toInstance.SubClusterID()
	if fromInstanceSubClusterID == toInstanceSubClusterID {
		return nil
	}
	toInstanceSubCluster := sph.subClusterMap[toInstanceSubClusterID]
	if _, ok := toInstanceSubCluster.shards[shardMoved.ID()]; !ok {
		sph.currentShardsPerSubCluster[toInstanceSubClusterID]++
	}
	toInstanceSubCluster.shards[shardMoved.ID()]++
	sph.subClusterMap[toInstanceSubClusterID] = toInstanceSubCluster

	// Update from
	if fromInstance != nil {
		fromInstanceSubCluster := sph.subClusterMap[fromInstanceSubClusterID]
		fromInstanceSubCluster.shards[shardMoved.ID()]--
		if fromInstanceSubCluster.shards[shardMoved.ID()] == 0 {
			delete(fromInstanceSubCluster.shards, shardMoved.ID())
			sph.currentShardsPerSubCluster[fromInstanceSubClusterID]--
		}
	}
	return nil
}

func getShardList(shardMap map[uint32]struct{}) []uint32 {
	shardList := make([]uint32, 0, len(shardMap))
	for shardID := range shardMap {
		shardList = append(shardList, shardID)
	}
	return shardList
}

func (sph *subClusterShardedHelper) placeShards(
	shards []shard.Shard,
	from placement.Instance,
	candidates []placement.Instance,
) error {
	shardSet := getShardMap(shards)
	if from != nil {
		// NB(cw) when removing an adding instance that has not finished bootstrapping its
		// Initializing shards, prefer to return those Initializing shards back to the leaving instance
		// to reduce some bootstrapping work in the cluster.
		sph.returnInitializingShardsToSource(shardSet, from, candidates)
	}

	instanceHeap, err := sph.buildInstanceHeap(nonLeavingInstances(candidates), true)
	if err != nil {
		return err
	}
	// if there are shards left to be assigned, distribute them evenly
	var triedInstances []placement.Instance
	sort.Sort(ShardByID(shards))
	sph.deterministicShuffle(shards)
	for _, s := range shards {
		if s.State() == shard.Leaving {
			continue
		}
		moved := false
		for instanceHeap.Len() > 0 {
			tryInstance := heap.Pop(instanceHeap).(placement.Instance)
			triedInstances = append(triedInstances, tryInstance)
			if sph.moveShard(s, from, tryInstance) {
				moved = true
				sph.updateSubClusterLoad(s, from, tryInstance)
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

func (sph *subClusterShardedHelper) returnInitializingShards(instance placement.Instance) {
	shardSet := getShardMap(instance.Shards().All())
	sph.returnInitializingShardsToSource(shardSet, instance, sph.Instances())
}

func (sph *subClusterShardedHelper) returnInitializingShardsToSource(
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
		if sph.moveShard(s, from, sourceInstance) {
			delete(shardSet, s.ID())
		}
	}
}

func (sph *subClusterShardedHelper) mostUnderLoadedInstance() (placement.Instance, bool) {
	var (
		res              placement.Instance
		maxLoadGap       int
		totalLoadSurplus int
	)

	for id, instance := range sph.instances {
		loadGap := sph.targetLoad[id] - loadOnInstance(instance)
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

func (sph *subClusterShardedHelper) optimize(t optimizeType) error {
	var fn assignLoadFn
	switch t {
	case safe:
		fn = sph.assignLoadToInstanceSafe
	case unsafe:
		fn = sph.assignLoadToInstanceUnsafe
	}
	uniq := make(map[string]struct{}, len(sph.instances))
	for {
		ins, ok := sph.mostUnderLoadedInstance()
		if !ok {
			return nil
		}
		if _, exist := uniq[ins.ID()]; exist {
			return nil
		}

		uniq[ins.ID()] = struct{}{}
		if err := fn(ins); err != nil {
			return err
		}
	}
}

func (sph *subClusterShardedHelper) assignLoadToInstanceSafe(addingInstance placement.Instance) error {
	return sph.assignTargetLoad(addingInstance, func(from, to placement.Instance) bool {
		return sph.moveOneShardInState(from, to, shard.Unknown)
	})
}

func (sph *subClusterShardedHelper) assignLoadToInstanceUnsafe(addingInstance placement.Instance) error {
	return sph.assignTargetLoad(addingInstance, func(from, to placement.Instance) bool {
		return sph.moveOneShard(from, to)
	})
}

func (sph *subClusterShardedHelper) reclaimLeavingShards(instance placement.Instance) {
	if instance.Shards().NumShardsForState(shard.Leaving) == 0 {
		// Shortcut if there is nothing to be reclaimed.
		return
	}
	id := instance.ID()
	for _, i := range sph.instances {
		for _, s := range i.Shards().ShardsForState(shard.Initializing) {
			if s.SourceID() == id {
				// NB(cw) in very rare case, the leaving shards could not be taken back.
				// For example: in a RF=2 case, instance a and b on ig1, instance c on ig2,
				// c took shard1 from instance a, before we tried to assign shard1 back to instance a,
				// b got assigned shard1, now if we try to add instance a back to the topology, a can
				// no longer take shard1 back.
				// But it's fine, the algo will fil up those load with other shards from the cluster
				sph.moveShard(s, i, instance)
			}
		}
	}
}

func (sph *subClusterShardedHelper) addInstance(addingInstance placement.Instance) error {
	sph.reclaimLeavingShards(addingInstance)
	return sph.assignLoadToInstanceUnsafe(addingInstance)
}

func (sph *subClusterShardedHelper) addInstances(addingInstances []placement.Instance) error {
	shardsMovedToSubCluster := make(map[uint32][]uint32)
	for _, targetInstance := range addingInstances {
		targetSubClusterID := targetInstance.SubClusterID()
		for subClusterID, currSubCluster := range sph.subClusterMap {
			if sph.currentShardsPerSubCluster[subClusterID] > sph.targetShardsPerSubCluster[subClusterID] { // Shards needs to moved from here
				// try to take shards from the most loaded instances until the adding instance reaches target load
				instanceHeap, err := sph.buildInstanceHeap(nonLeavingInstances(getInstances(sph.subClusterMap[subClusterID].instances)), false)
				if err != nil {
					return err
				}
				for sph.currentShardsPerSubCluster[subClusterID] > sph.targetShardsPerSubCluster[subClusterID] && instanceHeap.Len() > 0 {
					fromInstance := heap.Pop(instanceHeap).(placement.Instance)
					if moved := sph.moveOneShard(fromInstance, targetInstance); moved {
						heap.Push(instanceHeap, fromInstance)
						//delete(currSubCluster.shards, s)
						sph.currentShardsPerSubCluster[subClusterID]--
						sph.currentShardsPerSubCluster[targetSubClusterID]++
						sph.subClusterMap[targetSubClusterID] = currSubCluster
						if _, ok := shardsMovedToSubCluster[targetSubClusterID]; !ok {
							shardsMovedToSubCluster[targetSubClusterID] = make([]uint32, 0)
						}
						//shardsMovedToSubCluster[targetSubClusterID] = append(shardsMovedToSubCluster[targetSubClusterID], s)
						targetSubCluster := sph.subClusterMap[targetSubClusterID]
						//targetSubCluster.shards[s] = struct{}{}
						sph.subClusterMap[targetSubClusterID] = targetSubCluster
					}
				}
				return nil
			}
		}
	}

	for _, targetInstance := range addingInstances {
		shardsMoved := targetInstance.Shards().All()
		deterministicShuffle(shardsMoved, int64(len(sph.groupToInstancesMap)))

	}
	//
	//for subClusterID, shardsMoved := range shardsMovedToSubCluster {
	//	for _, addingInstance := range addingInstances {
	//		if addingInstance.SubClusterID() != subClusterID {
	//			continue
	//		}
	//		for _, shardID := range shardsMoved {
	//			for
	//		}
	//	}
	//}

	return nil
}

func (sph *subClusterShardedHelper) assignTargetLoad(
	targetInstance placement.Instance,
	moveOneShardFn func(from, to placement.Instance) bool,
) error {
	targetLoad := sph.targetLoadForInstance(targetInstance.ID())
	// try to take shards from the most loaded instances until the adding instance reaches target load
	instanceHeap, err := sph.buildInstanceHeap(removeSubClusterInstances(nonLeavingInstances(sph.Instances()), targetInstance.SubClusterID()), false)
	if err != nil {
		return err
	}
	for targetInstance.Shards().NumShards() < targetLoad && instanceHeap.Len() > 0 {
		fromInstance := heap.Pop(instanceHeap).(placement.Instance)
		if moved := moveOneShardFn(fromInstance, targetInstance); moved {
			heap.Push(instanceHeap, fromInstance)
		}
	}
	instanceHeap, err = sph.buildInstanceHeap(nonLeavingInstances(getInstances(sph.subClusterMap[targetInstance.SubClusterID()].instances)), false)
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

func removeShardFromList(id uint32, shards []uint32) []uint32 {
	newShards := make([]uint32, 0, len(shards)-1)
	for _, s := range shards {
		if s == id {
			continue
		}
		newShards = append(newShards, s)
	}
	return newShards
}
func (sph *subClusterShardedHelper) canAssignInstance(shardID uint32, from, to placement.Instance) bool {
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
	toSubClusterID := to.SubClusterID()
	_, ok = sph.subClusterMap[toSubClusterID].shards[shardID]
	if ok && sph.currentShardsPerSubCluster[toSubClusterID] < sph.targetShardsPerSubCluster[toSubClusterID] {
		return false
	}
	if sph.currentShardsPerSubCluster[toSubClusterID] == sph.targetShardsPerSubCluster[toSubClusterID] {
		if !ok && len(sph.subClusterMap[toSubClusterID].instances)%sph.rf == 0 {
			return false
		}
	}
	if from != nil {
		fromSubClusterID := from.SubClusterID()
		if sph.targetShardsPerSubCluster[fromSubClusterID] > sph.currentShardsPerSubCluster[fromSubClusterID] {
			return false
		}
		shardCount, _ := sph.subClusterMap[fromSubClusterID].shards[shardID]
		if (sph.currentShardsPerSubCluster[toSubClusterID] == sph.targetShardsPerSubCluster[toSubClusterID]) && shardCount == sph.rf && toSubClusterID != fromSubClusterID {
			return false
		}

		//if ok && sph.targetShardsPerSubCluster[fromSubClusterID] < sph.currentShardsPerSubCluster[fromSubClusterID] {
		//	return false
		//}
	}

	return sph.CanMoveShard(shardID, from, to.IsolationGroup())
}

func (sph *subClusterShardedHelper) assignShardToInstance(s shard.Shard, to placement.Instance) {
	to.Shards().Add(s)

	if _, exist := sph.shardToInstanceMap[s.ID()]; !exist {
		sph.shardToInstanceMap[s.ID()] = make(map[placement.Instance]struct{})
	}
	sph.shardToInstanceMap[s.ID()][to] = struct{}{}
}

func (sph *subClusterShardedHelper) deterministicShuffle(arr []shard.Shard) {
	r := rand.New(rand.NewSource(int64(len(sph.groupToInstancesMap))))

	for i := len(arr) - 1; i > 0; i-- {
		j := r.Intn(i + 1) // Generate a random index
		arr[i], arr[j] = arr[j], arr[i]
	}
}

func (sph *subClusterShardedHelper) scanAndValidateSubClusterLoad() error {
	err := sph.scanAndValidateInstancesInSubCluster()
	if err != nil {
		return err
	}

	sph.currentShardsPerSubCluster = make(map[uint32]int)
	sph.subClusters = make([]uint32, 0)
	for subClusterID, currSubCluster := range sph.subClusterMap {
		instances := currSubCluster.instances
		sph.subClusters = append(sph.subClusters, subClusterID)
		for instance, _ := range instances {
			if instance.IsLeaving() {
				continue
			}
			for _, s := range instance.Shards().All() {
				if s.State() == shard.Leaving {
					continue
				}
				currSubCluster.shards[s.ID()]++
			}
		}
		sph.currentShardsPerSubCluster[subClusterID] = len(currSubCluster.shards)
		sph.subClusterMap[subClusterID] = currSubCluster
	}
	return nil
}

func (sph *subClusterShardedHelper) buildTargetSubClusterLoad(weighType balanceWeighType) {
	totalWeight := 0
	for subClusterID, currSubCluster := range sph.subClusterMap {
		for instance, _ := range currSubCluster.instances {
			if instance.IsLeaving() {
				continue
			}
			currSubCluster.weight += int(instance.Weight())
		}
		switch weighType {
		case none:
		// Do nothing
		case addInstance:
			if currSubCluster.weight%sph.rf != 0 {
				currSubCluster.weight = sph.rf * (int(math.Ceil(float64(currSubCluster.weight) / float64(sph.rf))))
			}
		case removeInstance:
			if currSubCluster.weight%sph.rf != 0 {
				currSubCluster.weight = sph.rf * (int(math.Floor(float64(currSubCluster.weight) / float64(sph.rf))))
			}
		}

		totalWeight += currSubCluster.weight
		sph.subClusterMap[subClusterID] = currSubCluster
	}
	sph.targetShardsPerSubCluster = make(map[uint32]int)
	totalDivided := 0
	totalShards := len(sph.uniqueShards)
	for subClusterID, currCluster := range sph.subClusterMap {
		sph.targetShardsPerSubCluster[subClusterID] = int(math.Floor((float64(currCluster.weight) / float64(totalWeight)) * float64(totalShards)))
		totalDivided += sph.targetShardsPerSubCluster[subClusterID]
	}
	temp := make([]subCluster, 0, len(sph.subClusters))
	for _, subClusterID := range sph.subClusters {
		temp = append(temp, sph.subClusterMap[subClusterID])
	}
	sort.Sort(subClusterByWeightDesc(temp))
	diff := totalShards - totalDivided
	for _, curr := range temp {
		if diff == 0 {
			break
		}
		sph.targetShardsPerSubCluster[curr.ID]++
		diff--
	}
}

func (sph *subClusterShardedHelper) scanAndValidateInstancesInSubCluster() error {
	sph.subClusterMap = make(map[uint32]subCluster)
	for _, instance := range sph.instances {
		subClusterID := instance.SubClusterID()
		if subClusterID == 0 && sph.opts.HasSubClusters() {
			return errors.New("sub cluster ID cannot be 0")
		}
		if _, ok := sph.subClusterMap[subClusterID]; !ok {
			sph.subClusterMap[subClusterID] = subCluster{
				instances: make(map[placement.Instance]struct{}),
				shards:    make(map[uint32]int),
				ID:        subClusterID,
			}
		}
		currSubCluster := sph.subClusterMap[subClusterID]
		currSubCluster.instances[instance] = struct{}{}
	}
	for subClusterID, currSubCluster := range sph.subClusterMap {
		igToInstanceMap := make(map[string]map[placement.Instance]struct{})
		for instance, _ := range currSubCluster.instances {
			if _, ok := igToInstanceMap[instance.IsolationGroup()]; !ok {
				igToInstanceMap[instance.IsolationGroup()] = make(map[placement.Instance]struct{})
			}
			igToInstanceMap[instance.IsolationGroup()][instance] = struct{}{}
		}
		currSubCluster.groupToInstancesMap = make(map[string][]placement.Instance)
		for ig, instances := range igToInstanceMap {
			currSubCluster.groupToInstancesMap[ig] = getInstances(instances)
			sort.Sort(placement.ByIDAscending(currSubCluster.groupToInstancesMap[ig]))
		}
		sph.subClusterMap[subClusterID] = currSubCluster
	}
	err := sph.validateInstancesPerSubCluster()
	if err != nil {
		return err
	}
	return nil
}

func (sph *subClusterShardedHelper) validateInstancesPerSubCluster() error {
	for _, curr := range sph.subClusterMap { // Add condition of not including leaving instances in valid
		if len(curr.instances) > sph.opts.InstancesPerSubCluster() {
			return errors.New("number of instances per subCluster is greater than the number of allowed instances per subCluster")
		}
		//if subClusterID != 0 && len(instances)%sph.rf != 0 {
		//	return errors.New(fmt.Sprintf("sub %d cluster too small, atleast %d instances are required per subcluster", subClusterID, sph.rf))
		//}
	}
	return nil
}

func (sph *subClusterShardedHelper) placeShardsInSubCluster(ids []uint32) {
	clusterHeap := newSubClusterHeap(sph.subClusters, sph.currentShardsPerSubCluster, sph.targetShardsPerSubCluster)
	sort.Sort(UInts(ids))
	deterministicShuffle(ids, int64(len(sph.groupToInstancesMap)))

	for i := 0; i < len(ids); {
		triedSubClusters := make([]uint32, 0)
		for clusterHeap.Len() > 0 && i < len(ids) {
			subClusterID := clusterHeap.Pop().(uint32)
			triedSubClusters = append(triedSubClusters, subClusterID)
			if sph.currentShardsPerSubCluster[subClusterID] == sph.targetShardsPerSubCluster[subClusterID] {
				continue
			}
			if _, ok := sph.subClusterMap[subClusterID]; !ok {
				sph.subClusterMap[subClusterID] = subCluster{}
			}
			currSubCluster := sph.subClusterMap[subClusterID]
			currSubCluster.shards[ids[i]] = 1
			sph.subClusterMap[subClusterID] = currSubCluster
			sph.currentShardsPerSubCluster[subClusterID]++
			i++
		}
		for _, triedSubCluster := range triedSubClusters {
			clusterHeap.Push(triedSubCluster)
		}
	}
}

//func (sph *subClusterShardedHelper) balanceShardsInSubClusters() (map[uint32]map[uint32]shardMoveInfo, error) {
//	clusterHeap := newSubClusterHeap(sph.subClusters, sph.currentShardsPerSubCluster, sph.targetShardsPerSubCluster)
//	shardsMovedTo := make(map[uint32]map[uint32]shardMoveInfo)
//	for i := 0; i < len(sph.subClusters); {
//		subClusterID := sph.subClusters[i]
//		for sph.targetShardsPerSubCluster[subClusterID] > sph.currentShardsPerSubCluster[subClusterID] && clusterHeap.Len() > 0 && i < len(sph.subClusters) {
//			currCluster := clusterHeap.Pop().(uint32)
//			moved, shardId, err := sph.moveShardToSubCluster(currCluster, subClusterID)
//			if err != nil {
//				return nil, err
//			}
//			if moved {
//				sph.currentShardsPerSubCluster[subClusterID]++
//				clusterHeap.Push(currCluster)
//				if _, ok := shardsMovedTo[subClusterID]; !ok {
//					shardsMovedTo[subClusterID] = make(map[uint32]shardMoveInfo)
//				}
//				if _, ok := shardsMovedTo[subClusterID][currCluster]; !ok {
//					shardsMovedTo[subClusterID][currCluster] = shardMoveInfo{movedFrom: currCluster}
//				}
//
//				moveInfo := shardsMovedTo[subClusterID][currCluster]
//				moveInfo.shardIds = append(moveInfo.shardIds, shardId)
//				shardsMovedTo[subClusterID][currCluster] = moveInfo
//			}
//		}
//		if sph.targetShardsPerSubCluster[subClusterID] <= sph.currentShardsPerSubCluster[subClusterID] {
//			i++
//		}
//	}
//	return shardsMovedTo, nil
//}
//
//func (sph *subClusterShardedHelper) moveShardToSubCluster(from, to uint32) (bool, uint32, error) {
//	if from == to {
//		return false, 0, errors.New("source and target subClusters are the same")
//	}
//	if sph.currentShardsPerSubCluster[from] == sph.targetShardsPerSubCluster[from] {
//		return false, 0, nil
//	}
//	fromSubCluster := sph.subClusterMap[from]
//	toSubCluster := sph.subClusterMap[to]
//	fromShards := fromSubCluster.shards
//	deterministicShuffle(fromShards, int64(len(fromSubCluster.groupToInstancesMap)))
//	shardToMove := fromShards[len(fromShards)-1]
//	toSubCluster.shards = append(toSubCluster.shards, shardToMove)
//	fromSubCluster.shards = fromShards[:len(fromShards)-1]
//	sort.Sort(UInts(toSubCluster.shards))
//	sort.Sort(UInts(fromSubCluster.shards))
//	sph.subClusterMap[from] = fromSubCluster
//	sph.subClusterMap[to] = toSubCluster
//	sph.currentShardsPerSubCluster[from]--
//	return true, shardToMove, nil
//}

func deterministicShuffle[T any](arr []T, seed int64) {
	r := rand.New(rand.NewSource(seed)) // Create a new PRNG with the fixed seed

	for i := len(arr) - 1; i > 0; i-- {
		j := r.Intn(i + 1) // Generate a random index
		arr[i], arr[j] = arr[j], arr[i]
	}
}

func shuffleShards(shards []uint32, isolationGroups int) {
	sort.Sort(UInts(shards))
	deterministicShuffle(shards, int64(isolationGroups))
}

func getInstances(instances map[placement.Instance]struct{}) []placement.Instance {
	instanceArr := make([]placement.Instance, 0, len(instances))
	for id := range instances {
		instanceArr = append(instanceArr, id)
	}
	return instanceArr
}
