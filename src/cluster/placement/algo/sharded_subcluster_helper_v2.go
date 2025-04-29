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

type subclusteredhelperv2 struct {
	targetLoad           map[string]int
	shardToInstanceMap   map[uint32]map[placement.Instance]struct{}
	groupToInstancesMap  map[string]map[placement.Instance]struct{}
	subClusterToShardMap map[uint32]map[uint32]struct{}
	groupToWeightMap     map[string]uint32
	subClusters          map[uint32]struct{}
	rf                   int
	uniqueShards         []uint32
	instances            map[string]placement.Instance
	log                  *zap.Logger
	opts                 placement.Options
	totalWeight          uint32
	maxShardSetID        uint32
}

func newSubclusteredv2InitHelper(instances []placement.Instance, ids []uint32, opts placement.Options) *subclusteredhelperv2 {
	emptyPlacement := placement.NewPlacement().
		SetInstances(instances).
		SetShards(ids).
		SetReplicaFactor(0).
		SetIsSharded(true).
		SetCutoverNanos(opts.PlacementCutoverNanosFn()())
	ph := newubclusteredv2Helper(emptyPlacement, emptyPlacement.ReplicaFactor()+1, opts)
	targetLoad := ph.getTargetSubClusterLoad(0)
	ph.distributeInitialShards(targetLoad)
	return ph
}

func newubclusteredv2ReplicaHelper(p placement.Placement, opts placement.Options) *subclusteredhelperv2 {
	ph := newubclusteredv2Helper(p, p.ReplicaFactor()+1, opts)
	return ph
}

func newubclusteredv2AddInstanceHelper(
	p placement.Placement,
	instance placement.Instance,
	opts placement.Options,
	t instanceType,
) (*subclusteredhelperv2, placement.Instance, error) {
	instanceInPlacement, exist := p.Instance(instance.ID())
	if !exist {
		ph := newubclusteredv2Helper(p.SetInstances(append(p.Instances(), instance)), p.ReplicaFactor(), opts)
		targetLoad := ph.getTargetSubClusterLoad(0)
		for subClusterID := range ph.subClusters {
			if instance.SubClusterID() == subClusterID {
				continue
			}
			err := ph.moveShardsToSubCluster(subClusterID, instance.SubClusterID(), targetLoad)
			if err != nil {
				return nil, nil, err
			}
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

	ph := newubclusteredv2Helper(p, p.ReplicaFactor(), opts)
	targetLoad := ph.getTargetSubClusterLoad(0)
	for subClusterID := range ph.subClusters {
		if instance.SubClusterID() == subClusterID {
			continue
		}
		err := ph.moveShardsToSubCluster(subClusterID, instance.SubClusterID(), targetLoad)
		if err != nil {
			return nil, nil, err
		}
	}

	return ph, instanceInPlacement, nil
}

func newubclusteredv2RemoveInstanceHelper(
	p placement.Placement,
	instanceID string,
	opts placement.Options,
) (*subclusteredhelperv2, placement.Instance, error) {
	p, leavingInstance, err := removeInstanceFromPlacement(p, instanceID)
	if err != nil {
		return nil, nil, err
	}
	ph := newubclusteredv2Helper(p, p.ReplicaFactor(), opts)
	targetLoad := ph.getTargetSubClusterLoad(leavingInstance.SubClusterID())
	subClusters := getKeys(ph.subClusters)
	sort.Slice(subClusters, func(i, j int) bool { return subClusters[i] < subClusters[j] })
	for _, subClusterID := range subClusters {
		if leavingInstance.SubClusterID() == subClusterID {
			continue
		}
		err := ph.moveShardsFromSubCluster(leavingInstance.SubClusterID(), subClusterID, targetLoad)
		if err != nil {
			return nil, nil, err
		}
	}
	fmt.Println(ph.subClusterToShardMap)

	return ph, leavingInstance, nil
}

func newubclusteredv2ReplaceInstanceHelper(
	p placement.Placement,
	instanceIDs []string,
	addingInstances []placement.Instance,
	opts placement.Options,
) (*subclusteredhelperv2, []placement.Instance, []placement.Instance, error) {
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
	return newubclusteredv2Helper(p, p.ReplicaFactor(), opts), leavingInstances, newAddingInstances, nil
}

func newubclusteredv2Helper(p placement.Placement, targetRF int, opts placement.Options) *subclusteredhelperv2 {
	ph := &subclusteredhelperv2{
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

	ph.scanCurrentLoad()
	ph.buildTargetLoad()
	return ph
}

func (ph *subclusteredhelperv2) distributeInitialShards(targetLoad map[uint32]int) {
	if ph.subClusterToShardMap == nil {
		ph.subClusterToShardMap = make(map[uint32]map[uint32]struct{})
	}

	sort.Slice(ph.uniqueShards, func(i, j int) bool { return ph.uniqueShards[i] < ph.uniqueShards[j] })
	i := 0

	for to, shardCount := range targetLoad {
		if _, ok := ph.subClusterToShardMap[to]; !ok {
			ph.subClusterToShardMap[to] = make(map[uint32]struct{})
		}
		if len(ph.subClusterToShardMap[to]) == shardCount {
			return
		}
		for i < len(ph.uniqueShards) && len(ph.subClusterToShardMap[to]) < shardCount {
			s := ph.uniqueShards[i]
			if _, ok := ph.subClusterToShardMap[to][s]; !ok {
				ph.subClusterToShardMap[to][s] = struct{}{}
				i++
			}
		}
	}
}

func (ph *subclusteredhelperv2) moveShardsFromSubCluster(from uint32, to uint32, targetCount map[uint32]int) error {
	if from == 0 {
		return fmt.Errorf("subClusterID cannot be zero")
	}
	if ph.subClusterToShardMap == nil {
		ph.subClusterToShardMap = make(map[uint32]map[uint32]struct{})
	}
	if _, ok := ph.subClusterToShardMap[to]; !ok {
		ph.subClusterToShardMap[to] = make(map[uint32]struct{})
	}
	for s := range ph.subClusterToShardMap[to] {
		delete(ph.subClusterToShardMap[from], s)
	}
	if len(ph.subClusterToShardMap[from]) == 0 {
		delete(ph.subClusterToShardMap, from)
		return nil
	}
	if len(ph.subClusterToShardMap[to]) == targetCount[to] {
		return nil
	}

	if len(ph.subClusterToShardMap[to]) >= targetCount[to] {
		return nil
	}

	fromShards := ph.getSubClusterShards(from)
	i := 0
	for i < len(fromShards) && len(ph.subClusterToShardMap[to]) < targetCount[to] {
		s := fromShards[i]
		if _, exists := ph.subClusterToShardMap[to][s]; !exists {
			ph.subClusterToShardMap[to][s] = struct{}{}
			delete(ph.subClusterToShardMap[from], s)
			i++
		}
	}
	if len(ph.subClusterToShardMap[from]) == 0 {
		delete(ph.subClusterToShardMap, from)
	}
	return nil
}

func (ph *subclusteredhelperv2) moveShardsToSubCluster(from uint32, to uint32, targetCount map[uint32]int) error {
	if from == 0 {
		return fmt.Errorf("subClusterID cannot be zero")
	}
	if ph.subClusterToShardMap == nil {
		ph.subClusterToShardMap = make(map[uint32]map[uint32]struct{})
	}
	if _, ok := ph.subClusterToShardMap[to]; !ok {
		ph.subClusterToShardMap[to] = make(map[uint32]struct{})
	}
	if len(ph.subClusterToShardMap[to]) == targetCount[to] {
		return nil
	}

	if _, ok := ph.subClusterToShardMap[from]; !ok {
		return nil
	}

	for s := range ph.subClusterToShardMap[to] {
		delete(ph.subClusterToShardMap[from], s)
	}

	if len(ph.subClusterToShardMap[from]) <= targetCount[from] {
		return nil
	}

	fromShards := ph.getSubClusterShards(from)
	i := 0
	for i < len(fromShards) && len(ph.subClusterToShardMap[from]) > targetCount[from] {
		s := fromShards[i]
		if _, exists := ph.subClusterToShardMap[to][s]; !exists {
			ph.subClusterToShardMap[to][s] = struct{}{}
			delete(ph.subClusterToShardMap[from], s)
			i++
		}
	}
	return nil
}

func (ph *subclusteredhelperv2) getSubClusterShards(subClusterID uint32) []uint32 {
	if ph.subClusterToShardMap[subClusterID] == nil {
		ph.subClusterToShardMap[subClusterID] = make(map[uint32]struct{})
	}
	shards := make([]uint32, 0, len(ph.subClusterToShardMap[subClusterID]))
	for shardID := range ph.subClusterToShardMap[subClusterID] {
		shards = append(shards, shardID)
		ph.subClusterToShardMap[subClusterID][shardID] = struct{}{}
	}
	sort.Slice(shards, func(i, j int) bool { return shards[i] < shards[j] })
	return shards
}

func (ph *subclusteredhelperv2) getTargetSubClusterLoad(excludeSubCluster uint32) map[uint32]int {
	totalDivided := 0
	totalShards := len(ph.uniqueShards)
	subClusters := getKeys(ph.subClusters)
	if excludeSubCluster != 0 {
		for i, v := range subClusters {
			if v == excludeSubCluster {
				subClusters = append(subClusters[:i], subClusters[i+1:]...)
				break
			}
		}
	}
	sort.Slice(subClusters, func(i, j int) bool { return subClusters[i] <= subClusters[j] })
	targetShardsPerSubCluster := make(map[uint32]int)
	for _, subClusterID := range subClusters {
		targetShardsPerSubCluster[subClusterID] = int(math.Floor((float64(ph.opts.InstancesPerSubCluster()) / float64(ph.opts.InstancesPerSubCluster()*len(subClusters))) * float64(totalShards)))
		totalDivided += targetShardsPerSubCluster[subClusterID]
	}

	diff := totalShards - totalDivided
	for _, curr := range subClusters {
		if diff == 0 {
			break
		}
		targetShardsPerSubCluster[curr]++
		diff--
	}
	return targetShardsPerSubCluster

}

func (ph *subclusteredhelperv2) scanCurrentLoad() {
	ph.shardToInstanceMap = make(map[uint32]map[placement.Instance]struct{}, len(ph.uniqueShards))
	ph.groupToInstancesMap = make(map[string]map[placement.Instance]struct{})
	ph.groupToWeightMap = make(map[string]uint32)
	ph.subClusterToShardMap = make(map[uint32]map[uint32]struct{})
	ph.subClusters = make(map[uint32]struct{})
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

		ph.subClusters[instance.SubClusterID()] = struct{}{}

		ph.groupToWeightMap[instance.IsolationGroup()] = ph.groupToWeightMap[instance.IsolationGroup()] + instance.Weight()
		totalWeight += instance.Weight()

		for _, s := range instance.Shards().All() {
			if s.State() == shard.Leaving {
				continue
			}
			ph.assignShardToInstance(s, instance)
			if _, exist := ph.subClusterToShardMap[instance.SubClusterID()]; !exist {
				ph.subClusterToShardMap[instance.SubClusterID()] = make(map[uint32]struct{})
			}
			ph.subClusterToShardMap[instance.SubClusterID()][s.ID()] = struct{}{}
		}
	}
	ph.totalWeight = totalWeight
}

// nolint: dupl
func (ph *subclusteredhelperv2) buildTargetLoad() {
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

func getKeys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys // Returns zero value of K and false if map is empty
}

func (ph *subclusteredhelperv2) Instances() []placement.Instance {
	res := make([]placement.Instance, 0, len(ph.instances))
	for _, instance := range ph.instances {
		res = append(res, instance)
	}
	return res
}

func (ph *subclusteredhelperv2) getShardLen() int {
	return len(ph.uniqueShards)
}

func (ph *subclusteredhelperv2) targetLoadForInstance(id string) int {
	return ph.targetLoad[id]
}

func (ph *subclusteredhelperv2) moveOneShard(from, to placement.Instance) bool {
	// The order matter here:
	// The Unknown shards were just moved, so free to be moved around.
	// The Initializing shards were still being initialized on the instance,
	// so moving them are cheaper than moving those Available shards.
	return ph.moveOneShardInState(from, to, shard.Unknown) ||
		ph.moveOneShardInState(from, to, shard.Initializing) ||
		ph.moveOneShardInState(from, to, shard.Available)
}

// nolint: unparam
func (ph *subclusteredhelperv2) moveOneShardInState(from, to placement.Instance, state shard.State) bool {
	for _, s := range from.Shards().ShardsForState(state) {
		if ph.moveShard(s, from, to) {
			return true
		}
	}
	return false
}

func (ph *subclusteredhelperv2) moveShard(candidateShard shard.Shard, from, to placement.Instance) bool {
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

func (ph *subclusteredhelperv2) CanMoveShard(shard uint32, from placement.Instance, toIsolationGroup string) bool {
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

func (ph *subclusteredhelperv2) buildInstanceHeap(instances []placement.Instance, availableCapacityAscending bool) (heap.Interface, error) {
	return newHeap(instances, availableCapacityAscending, ph.targetLoad, ph.groupToWeightMap)
}

func (ph *subclusteredhelperv2) generatePlacement() placement.Placement {
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

func (ph *subclusteredhelperv2) placeShards(
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
			// fmt.Println(fmt.Sprintf("Trying to move shard %d to instance %s", s.ID(), tryInstance.ID()))
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

func (ph *subclusteredhelperv2) returnInitializingShards(instance placement.Instance) {
	shardSet := getShardMap(instance.Shards().All())
	ph.returnInitializingShardsToSource(shardSet, instance, ph.Instances())
}

// nolint: dupl
func (ph *subclusteredhelperv2) returnInitializingShardsToSource(
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

func (ph *subclusteredhelperv2) mostUnderLoadedInstance() (placement.Instance, bool) {
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

func (ph *subclusteredhelperv2) optimize(t optimizeType) error {
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
		if err := fn(ins); err != nil {
			return err
		}
	}
}

func (ph *subclusteredhelperv2) assignLoadToInstanceSafe(addingInstance placement.Instance) error {
	return ph.assignTargetLoad(addingInstance, func(from, to placement.Instance) bool {
		return ph.moveOneShardInState(from, to, shard.Unknown)
	})
}

func (ph *subclusteredhelperv2) assignLoadToInstanceUnsafe(addingInstance placement.Instance) error {
	return ph.assignTargetLoad(addingInstance, func(from, to placement.Instance) bool {
		return ph.moveOneShard(from, to)
	})
}

func (ph *subclusteredhelperv2) reclaimLeavingShards(instance placement.Instance) {
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

func (ph *subclusteredhelperv2) addInstance(addingInstance placement.Instance) error {
	ph.reclaimLeavingShards(addingInstance)
	return ph.assignLoadToInstanceUnsafe(addingInstance)
}

func (ph *subclusteredhelperv2) assignTargetLoad(
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
	instanceHeap, err = ph.buildInstanceHeap(nonLeavingInstances(ph.Instances()), false)
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

func (ph *subclusteredhelperv2) canAssignInstance(shardID uint32, from, to placement.Instance) bool {
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
	if _, exist := ph.subClusterToShardMap[to.SubClusterID()][shardID]; !exist {
		return false
	}
	return ph.CanMoveShard(shardID, from, to.IsolationGroup())
}

func (ph *subclusteredhelperv2) assignShardToInstance(s shard.Shard, to placement.Instance) {
	to.Shards().Add(s)

	if _, exist := ph.shardToInstanceMap[s.ID()]; !exist {
		ph.shardToInstanceMap[s.ID()] = make(map[placement.Instance]struct{})
	}
	ph.shardToInstanceMap[s.ID()][to] = struct{}{}
}
