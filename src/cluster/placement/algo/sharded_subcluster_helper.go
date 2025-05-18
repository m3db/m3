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
	"math/rand"
	"sort"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
)

// SubClusterByID sorts subClusters by ID
type SubClusterByID []subCluster

func (s SubClusterByID) Len() int { return len(s) }

func (s SubClusterByID) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s SubClusterByID) Less(i, j int) bool {
	return s[i].ID < s[j].ID
}

// ShardByID sorts shards by ID
type ShardByID []shard.Shard

func (s ShardByID) Len() int           { return len(s) }
func (s ShardByID) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s ShardByID) Less(i, j int) bool { return s[i].ID() < s[j].ID() }

type subCluster struct {
	instances           map[placement.Instance]struct{}
	groupToInstancesMap map[string]map[placement.Instance]struct{}
	groupToWeightMap    map[string]uint32
	shardToInstanceMap  map[uint32]map[placement.Instance]struct{}
	ID                  uint32
	weight              uint32
}

type subClusterShardedHelper struct {
	subClusterMap              map[uint32]*subCluster
	shardToSubClusterMap       map[uint32]uint32
	groupToWeightMap           map[string]uint32
	targetLoad                 map[string]int
	shardToInstanceMap         map[uint32]map[placement.Instance]struct{}
	currentShardsPerSubCluster map[uint32]int
	targetShardsPerSubCluster  map[uint32]int
	rf                         int
	uniqueShards               []uint32
	instances                  map[string]placement.Instance
	opts                       placement.Options
	totalWeight                uint32
}

func newInitSubClusterHelper(instances []placement.Instance, ids []uint32, rf int, opts placement.Options) subClusterShardedHelper {
	emptyPlacement := placement.NewPlacement().
		SetInstances(instances).
		SetShards(ids).
		SetReplicaFactor(rf).
		SetIsSharded(true).
		SetHasSubClusters(true).
		SetCutoverNanos(opts.PlacementCutoverNanosFn()())
	return newSubClusterHelper(emptyPlacement, emptyPlacement.ReplicaFactor(), opts)
}

func newSubClusteredAddInstanceHelper(
	p placement.Placement,
	instances []placement.Instance,
	opts placement.Options,
	t instanceType,
) (subClusterShardedHelper, []placement.Instance, error) {
	newInstances := make([]placement.Instance, 0, len(instances))
	instancesToAddShards := make([]placement.Instance, 0, len(instances))
	for _, instance := range instances {
		instanceInPlacement, exist := p.Instance(instance.ID())
		instancesToAddShards = append(instancesToAddShards, instance)
		if !exist {
			newInstances = append(newInstances, instance)
			continue
		}
		switch t {
		case withLeavingShardsOnly:
			if !instanceInPlacement.IsLeaving() {
				return subClusterShardedHelper{}, nil, errInstanceContainsNonLeavingShards
			}
		case withAvailableOrLeavingShardsOnly:
			shards := instanceInPlacement.Shards()
			if shards.NumShards() != shards.NumShardsForState(shard.Available)+shards.NumShardsForState(shard.Leaving) {
				return subClusterShardedHelper{}, nil, errInstanceContainsInitializingShards
			}
		default:
			return subClusterShardedHelper{}, nil, fmt.Errorf("unexpected type %v", t)
		}
	}

	return newSubClusterHelper(p.SetInstances(append(p.Instances(), newInstances...)), p.ReplicaFactor(), opts), instancesToAddShards, nil

}

func newSubClusterRemoveInstancesHelper(
	p placement.Placement,
	instance string,
	opts placement.Options,
) (subClusterShardedHelper, placement.Instance, []placement.Instance, error) {
	temp, leavingInstance, err := removeInstanceFromPlacement(p, instance)
	if err != nil {
		return subClusterShardedHelper{}, nil, nil, err
	}
	p = temp
	temp, removedInstances := removeSubclusterFromPlacement(p, leavingInstance.SubClusterID())
	p = temp

	return newSubClusterHelper(p, p.ReplicaFactor(), opts), leavingInstance, removedInstances, nil
}

func newSubClusterHelper(p placement.Placement, targetRF int, opts placement.Options) subClusterShardedHelper {
	sph := subClusterShardedHelper{
		rf:           targetRF,
		instances:    make(map[string]placement.Instance, p.NumInstances()),
		uniqueShards: p.Shards(),
		opts:         opts,
	}

	for _, instance := range p.Instances() {
		sph.instances[instance.ID()] = instance
	}

	sph.scanCurrentLoad()
	sph.buildTargetLoad()
	return sph
}

func newSubClusterReplaceInstanceHelper(
	p placement.Placement,
	instanceIDs []string,
	addingInstances []placement.Instance,
	opts placement.Options,
) (subClusterShardedHelper, []placement.Instance, []placement.Instance, error) {
	var (
		leavingInstances = make([]placement.Instance, len(instanceIDs))
		err              error
	)
	for i, instanceID := range instanceIDs {
		p, leavingInstances[i], err = removeInstanceFromPlacement(p, instanceID)
		if err != nil {
			return subClusterShardedHelper{}, nil, nil, err
		}
	}

	newAddingInstances := make([]placement.Instance, len(addingInstances))
	for i, instance := range addingInstances {
		p, newAddingInstances[i], err = addInstanceToPlacement(p, instance, anyType)
		if err != nil {
			return subClusterShardedHelper{}, nil, nil, err
		}
	}
	return newSubClusterHelper(p, p.ReplicaFactor(), opts), leavingInstances, newAddingInstances, nil
}

func (sph *subClusterShardedHelper) scanCurrentLoad() {
	sph.subClusterMap = make(map[uint32]*subCluster)
	sph.shardToInstanceMap = make(map[uint32]map[placement.Instance]struct{})
	sph.currentShardsPerSubCluster = make(map[uint32]int)
	sph.targetLoad = make(map[string]int)
	sph.groupToWeightMap = make(map[string]uint32)
	sph.shardToSubClusterMap = make(map[uint32]uint32)
	totalWeight := uint32(0)
	for _, instance := range sph.instances {
		if _, exist := sph.subClusterMap[instance.SubClusterID()]; !exist {
			sph.subClusterMap[instance.SubClusterID()] = &subCluster{
				shardToInstanceMap:  make(map[uint32]map[placement.Instance]struct{}),
				groupToInstancesMap: make(map[string]map[placement.Instance]struct{}),
				ID:                  instance.SubClusterID(),
				instances:           make(map[placement.Instance]struct{}),
				weight:              uint32(0),
				groupToWeightMap:    make(map[string]uint32),
			}
		}
		currSubCluster := sph.subClusterMap[instance.SubClusterID()]
		if _, exist := currSubCluster.groupToInstancesMap[instance.IsolationGroup()]; !exist {
			currSubCluster.groupToInstancesMap[instance.IsolationGroup()] = make(map[placement.Instance]struct{})
		}
		currSubCluster.groupToInstancesMap[instance.IsolationGroup()][instance] = struct{}{}
		currSubCluster.instances[instance] = struct{}{}

		if instance.IsLeaving() {
			// Leaving instances are not counted as usable capacities in the placement.
			continue
		}

		sph.groupToWeightMap[instance.IsolationGroup()] += instance.Weight()
		currSubCluster.groupToWeightMap[instance.IsolationGroup()] += instance.Weight()
		currSubCluster.weight += instance.Weight()
		totalWeight += instance.Weight()

		for _, s := range instance.Shards().All() {
			if s.State() == shard.Leaving {
				continue
			}
			sph.assignShardToInstance(s, instance, currSubCluster)
		}
	}
	sph.totalWeight += totalWeight
}

func (sph *subClusterShardedHelper) buildTargetLoad() {
	sph.buildTargetSubClusterLoad()
	for subClusterID, currSubCluster := range sph.subClusterMap {
		overWeightedGroups := 0
		overWeight := uint32(0)
		for _, weight := range currSubCluster.groupToWeightMap {
			if isOverWeighted(weight, currSubCluster.weight, sph.rf) {
				overWeightedGroups++
				overWeight += weight
			}
		}
		for instance := range currSubCluster.instances {
			if instance.IsLeaving() {
				// We should not set a target load for leaving instances.
				continue
			}
			igWeight := currSubCluster.groupToWeightMap[instance.IsolationGroup()]
			if isOverWeighted(igWeight, currSubCluster.weight, sph.rf) {
				// If the instance is on a over-sized isolation group, the target load
				// equals (shardLen / capacity of the isolation group).
				sph.targetLoad[instance.ID()] = int(math.Ceil(float64(sph.targetShardsPerSubCluster[subClusterID]) * float64(instance.Weight()) / float64(igWeight)))
			} else {
				// If the instance is on a normal isolation group, get the target load
				// with aware of other over-sized isolation group.
				sph.targetLoad[instance.ID()] = sph.targetShardsPerSubCluster[subClusterID] * (sph.rf - overWeightedGroups) * int(instance.Weight()) / int(currSubCluster.weight-overWeight)
			}
		}
	}
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
	sort.Sort(ShardByID(shards))
	sph.deterministicShuffle(shards)

	instanceHeap, err := sph.buildInstanceHeap(nonLeavingInstances(candidates), true)
	if err != nil {
		return err
	}
	var triedInstances []placement.Instance
	for _, s := range shardSet {
		if s.State() == shard.Leaving {
			continue
		}
		moved := false
		for instanceHeap.Len() > 0 {

			tryInstance := heap.Pop(instanceHeap).(placement.Instance)
			triedInstances = append(triedInstances, tryInstance)
			// fmt.Println(fmt.Sprintf("Trying to move shard %d to %s %s from %s %s", s.ID(), tryInstance.ID(), tryInstance.IsolationGroup(), from.ID(), from.IsolationGroup()))
			if sph.moveShard(s, from, tryInstance) {
				moved = true
				break
			}
		}
		if !moved {
			// This should only happen when RF > number of isolation groups.
			return fmt.Errorf("shard  %v not moved", s.ID())
		}
		for _, triedInstance := range triedInstances {
			heap.Push(instanceHeap, triedInstance)
		}
		triedInstances = triedInstances[:0]
	}
	return nil
}

func (sph *subClusterShardedHelper) Instances() []placement.Instance {
	res := make([]placement.Instance, 0, len(sph.instances))
	for _, instance := range sph.instances {
		res = append(res, instance)
	}
	return res
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
	deterministicShuffle(shardsToMoveFrom, int64(sph.rf))
	for _, s := range shardsToMoveFrom {
		if sph.moveShard(s, from, to) {
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
		newShard = sph.removeShardFromInstance(candidateShard, to, from, sph.subClusterMap[from.SubClusterID()])
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

	sph.assignShardToInstance(newShard, to, sph.subClusterMap[to.SubClusterID()])

	return true
}

func (sph *subClusterShardedHelper) CanMoveShard(shard uint32, from placement.Instance, toIsolationGroup string) bool {
	if from != nil {
		if from.IsolationGroup() == toIsolationGroup {
			return true
		} else if _, exist := sph.subClusterMap[from.SubClusterID()]; !exist {
			return false
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

	for _, instance := range instances {
		shards := instance.Shards()
		for _, s := range shards.ShardsForState(shard.Unknown) {
			shards.Add(shard.NewShard(s.ID()).
				SetSourceID(s.SourceID()).
				SetState(shard.Initializing).
				SetCutoverNanos(sph.opts.ShardCutoverNanosFn()()))
		}
	}

	return placement.NewPlacement().
		SetInstances(instances).
		SetShards(sph.uniqueShards).
		SetReplicaFactor(sph.rf).
		SetIsSharded(true).
		SetIsMirrored(sph.opts.IsMirrored()).
		SetCutoverNanos(sph.opts.PlacementCutoverNanosFn()()).
		SetHasSubClusters(true)
}

func (sph *subClusterShardedHelper) placeShardForInitialPlacement(shards []shard.Shard) error {
	// if there are shards left to be assigned, distribute them evenly

	subClusters := sph.getSubClusters()
	for j := 0; j < sph.rf; j++ {
		l := 0
		for _, currSubCluster := range subClusters {
			instanceHeap, err := sph.buildInstanceHeap(nonLeavingInstances(getInstances(currSubCluster.instances)), true)
			if err != nil {
				return err
			}
			shardsAssigned := 0
			var triedInstances []placement.Instance
			for shardsAssigned < sph.targetShardsPerSubCluster[currSubCluster.ID] && l < len(shards) {
				moved := false
				for instanceHeap.Len() > 0 && l < len(shards) {
					s := shards[l]
					tryInstance := heap.Pop(instanceHeap).(placement.Instance)
					triedInstances = append(triedInstances, tryInstance)
					if sph.moveShard(s, nil, tryInstance) {
						moved = true
						l++
						shardsAssigned++
						break
					}
				}
				if !moved && len(currSubCluster.shardToInstanceMap) < sph.targetShardsPerSubCluster[currSubCluster.ID] {
					// This should only happen when RF > number of isolation groups.
					return errNotEnoughIsolationGroups
				}
				for _, triedInstance := range triedInstances {
					heap.Push(instanceHeap, triedInstance)
				}
				triedInstances = triedInstances[:0]
			}
		}
	}

	return nil
}

func (sph *subClusterShardedHelper) getSubClusters() []*subCluster {
	temp := make([]*subCluster, 0, len(sph.subClusterMap))
	for _, currSubCluster := range sph.subClusterMap {
		temp = append(temp, currSubCluster)
	}
	sort.Sort(subClusterByWeightDesc(temp))
	return temp
}

// nolint: dupl
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

// nolint: dupl
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

// nolint: dupl
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

func (sph *subClusterShardedHelper) assignTargetLoad(
	targetInstance placement.Instance,
	moveOneShardFn func(from, to placement.Instance) bool,
) error {
	targetLoad := sph.targetLoadForInstance(targetInstance.ID())
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
	instances, ok := sph.subClusterMap[toSubClusterID].shardToInstanceMap[shardID]
	if ok && len(instances) == sph.rf && sph.currentShardsPerSubCluster[toSubClusterID] < sph.targetShardsPerSubCluster[toSubClusterID] {
		return false
	}
	if !ok && sph.currentShardsPerSubCluster[toSubClusterID] == sph.targetShardsPerSubCluster[toSubClusterID] {
		return false
	}
	if from != nil {
		fromSubClusterID := from.SubClusterID()
		if _, exists := sph.subClusterMap[fromSubClusterID]; !exists {
			if val, ok := sph.shardToSubClusterMap[shardID]; ok && val != to.SubClusterID() {
				return false
			}
			return sph.CanMoveShard(shardID, from, to.IsolationGroup())
		}
		if sph.targetShardsPerSubCluster[fromSubClusterID] > sph.currentShardsPerSubCluster[fromSubClusterID] {
			return false
		}
		fromInstances, ok := sph.subClusterMap[fromSubClusterID].shardToInstanceMap[shardID]
		if sph.targetShardsPerSubCluster[fromSubClusterID] == sph.currentShardsPerSubCluster[fromSubClusterID] && ok && len(fromInstances) == sph.rf && toSubClusterID != fromSubClusterID {
			return false
		}
	}

	return sph.CanMoveShard(shardID, from, to.IsolationGroup())
}

func (sph *subClusterShardedHelper) assignShardToInstance(s shard.Shard, to placement.Instance, cluster *subCluster) {
	to.Shards().Add(s)

	if _, exist := cluster.shardToInstanceMap[s.ID()]; !exist {
		cluster.shardToInstanceMap[s.ID()] = make(map[placement.Instance]struct{})
		sph.currentShardsPerSubCluster[cluster.ID]++
		sph.shardToSubClusterMap[s.ID()] = cluster.ID
	}
	cluster.shardToInstanceMap[s.ID()][to] = struct{}{}
	if _, exist := sph.shardToInstanceMap[s.ID()]; !exist {
		sph.shardToInstanceMap[s.ID()] = make(map[placement.Instance]struct{})
	}
	sph.shardToInstanceMap[s.ID()][to] = struct{}{}
}

func (sph *subClusterShardedHelper) removeShardFromInstance(s shard.Shard, to, from placement.Instance, cluster *subCluster) shard.Shard {
	shardID := s.ID()
	newShard := shard.NewShard(shardID)
	// nolint:exhaustive
	switch s.State() {
	case shard.Unknown, shard.Initializing:
		from.Shards().Remove(shardID)
		newShard.SetSourceID(s.SourceID())
	case shard.Available:
		s.SetState(shard.Leaving).
			SetCutoffNanos(sph.opts.ShardCutoffNanosFn()())
		newShard.SetSourceID(from.ID())
	}
	if cluster == nil {
		return newShard
	}

	if len(cluster.shardToInstanceMap[shardID]) == sph.rf && from.SubClusterID() != to.SubClusterID() {
		sph.currentShardsPerSubCluster[cluster.ID]--
	}
	delete(cluster.shardToInstanceMap[shardID], from)
	if len(cluster.shardToInstanceMap[shardID]) == 0 {
		delete(cluster.shardToInstanceMap, shardID)
	}
	return newShard
}

func (sph *subClusterShardedHelper) deterministicShuffle(arr []shard.Shard) {
	r := rand.New(rand.NewSource(int64(sph.rf)))

	for i := len(arr) - 1; i > 0; i-- {
		j := r.Intn(i + 1) // Generate a random index
		arr[i], arr[j] = arr[j], arr[i]
	}
}

func (sph *subClusterShardedHelper) buildTargetSubClusterLoad() {
	sph.targetShardsPerSubCluster = make(map[uint32]int)
	totalDivided := 0
	totalShards := len(sph.uniqueShards)
	for subClusterID := range sph.subClusterMap {
		sph.targetShardsPerSubCluster[subClusterID] = int(math.Floor((float64(sph.opts.InstancesPerSubCluster()) / float64(sph.opts.InstancesPerSubCluster()*len(sph.subClusterMap))) * float64(totalShards)))
		totalDivided += sph.targetShardsPerSubCluster[subClusterID]
	}
	temp := sph.getSubClusters()
	diff := totalShards - totalDivided
	for _, curr := range temp {
		if diff == 0 {
			break
		}
		sph.targetShardsPerSubCluster[curr.ID]++
		diff--
	}
}

func deterministicShuffle[T any](arr []T, seed int64) {
	r := rand.New(rand.NewSource(seed)) // Create a new PRNG with the fixed seed

	for i := len(arr) - 1; i > 0; i-- {
		j := r.Intn(i + 1) // Generate a random index
		arr[i], arr[j] = arr[j], arr[i]
	}
}

func getInstances(instances map[placement.Instance]struct{}) []placement.Instance {
	instanceArr := make([]placement.Instance, 0, len(instances))
	for id := range instances {
		instanceArr = append(instanceArr, id)
	}
	sort.Sort(placement.ByIDAscending(instanceArr))
	return instanceArr
}

func removeSubClusterInstances(instances []placement.Instance, cluster uint32) []placement.Instance {
	r := make([]placement.Instance, 0, len(instances))
	for _, instance := range instances {
		if instance.SubClusterID() == cluster {
			continue
		}
		r = append(r, instance)
	}

	return r
}

func getSubClusterInstances(instances []placement.Instance, subClusterID uint32) []placement.Instance {
	r := make([]placement.Instance, 0, len(instances))
	for _, instance := range instances {
		if instance.SubClusterID() == subClusterID {
			r = append(r, instance)
		}
	}
	return r
}
