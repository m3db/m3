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

type subclusteredhelperv3 struct {
	targetLoad            map[string]int
	shardToInstanceMap    map[uint32]map[placement.Instance]struct{}
	groupToInstancesMap   map[string]map[placement.Instance]struct{}
	subClusterToShardMap  map[uint32]map[uint32]struct{}
	groupToWeightMap      map[string]uint32
	subClusters           map[uint32]struct{}
	rf                    int
	uniqueShards          []uint32
	instances             map[string]placement.Instance
	log                   *zap.Logger
	opts                  placement.Options
	totalWeight           uint32
	maxShardSetID         uint32
	subClusterAssignments map[uint32]*subClusterShardAssignment
}

// subClusterShardAssignment tracks the initial shard assignment for each subcluster
type subClusterShardAssignment struct {
	shards map[uint32]struct{}
	// tracks which instances in the subcluster have been added
	addedInstances map[string]struct{}
	// tracks which instances are being replaced
	replacingInstances map[string]struct{}
}

func newSubclusteredv3InitHelper(instances []placement.Instance, ids []uint32, opts placement.Options) *subclusteredhelperv3 {
	emptyPlacement := placement.NewPlacement().
		SetInstances(instances).
		SetShards(ids).
		SetReplicaFactor(0).
		SetIsSharded(true).
		SetCutoverNanos(opts.PlacementCutoverNanosFn()())
	ph := newSubclusteredv3Helper(emptyPlacement, emptyPlacement.ReplicaFactor()+1, opts)
	targetLoad := ph.getTargetSubClusterLoad(0)
	ph.distributeInitialShards(targetLoad)
	return ph
}

func newSubclusteredv3Helper(p placement.Placement, targetRF int, opts placement.Options) *subclusteredhelperv3 {
	ph := &subclusteredhelperv3{
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

func (ph *subclusteredhelperv3) distributeInitialShards(targetLoad map[uint32]int) {
	// Validate instance weights first
	instances := make([]placement.Instance, 0, len(ph.instances))
	for _, instance := range ph.instances {
		instances = append(instances, instance)
	}
	if err := ph.validateInstanceWeight(instances[0], instances[1:]); err != nil {
		ph.log.Error("invalid instance weights during initial placement", zap.Error(err))
		return
	}

	if ph.subClusterToShardMap == nil {
		ph.subClusterToShardMap = make(map[uint32]map[uint32]struct{})
	}

	// Sort shards for deterministic distribution
	sort.Slice(ph.uniqueShards, func(i, j int) bool { return ph.uniqueShards[i] < ph.uniqueShards[j] })

	// First, divide shards among subclusters
	shardsPerSubCluster := make(map[uint32][]uint32)
	shardIndex := 0

	// Calculate total number of subclusters
	totalSubClusters := len(ph.subClusters)
	for _, instance := range ph.instances {
		if instance.SubClusterID() > uint32(totalSubClusters) {
			totalSubClusters = int(instance.SubClusterID())
		}
	}

	// Initialize shard assignments for all subclusters
	for i := 1; i <= totalSubClusters; i++ {
		shardsPerSubCluster[uint32(i)] = make([]uint32, 0)
	}

	// Distribute shards among subclusters
	for shardIndex < len(ph.uniqueShards) {
		for i := 1; i <= totalSubClusters; i++ {
			if shardIndex >= len(ph.uniqueShards) {
				break
			}
			shardsPerSubCluster[uint32(i)] = append(shardsPerSubCluster[uint32(i)], ph.uniqueShards[shardIndex])
			shardIndex++
		}
	}

	// For each subcluster, distribute its shards among instances
	for subClusterID, shards := range shardsPerSubCluster {
		// Get instances in this subcluster
		subClusterInstances := make([]placement.Instance, 0)
		for _, instance := range ph.instances {
			if instance.SubClusterID() == subClusterID {
				subClusterInstances = append(subClusterInstances, instance)
			}
		}

		// Sort instances by isolation group to ensure even distribution
		sort.Slice(subClusterInstances, func(i, j int) bool {
			return subClusterInstances[i].IsolationGroup() < subClusterInstances[j].IsolationGroup()
		})

		// Initialize subcluster shard map
		if _, ok := ph.subClusterToShardMap[subClusterID]; !ok {
			ph.subClusterToShardMap[subClusterID] = make(map[uint32]struct{})
		}

		// Store initial shard assignment for this subcluster
		ph.storeSubClusterShardAssignment(subClusterID, shards, subClusterInstances)

		// Distribute shards among instances in this subcluster
		for _, shardID := range shards {
			// Add shard to subcluster map
			ph.subClusterToShardMap[subClusterID][shardID] = struct{}{}

			// Assign shard to exactly RF instances in this subcluster
			instanceIndex := 0
			for replica := 0; replica < ph.rf; replica++ {
				if len(subClusterInstances) == 0 {
					continue
				}

				// Get next instance in round-robin fashion
				instance := subClusterInstances[instanceIndex]
				newShard := shard.NewShard(shardID).SetState(shard.Initializing)
				ph.assignShardToInstance(newShard, instance)

				// Move to next instance
				instanceIndex = (instanceIndex + 1) % len(subClusterInstances)
			}
		}
	}
}

// storeSubClusterShardAssignment stores the initial shard assignment for a subcluster
func (ph *subclusteredhelperv3) storeSubClusterShardAssignment(
	subClusterID uint32,
	shards []uint32,
	instances []placement.Instance,
) {
	if ph.subClusterAssignments == nil {
		ph.subClusterAssignments = make(map[uint32]*subClusterShardAssignment)
	}

	assignment := &subClusterShardAssignment{
		shards:             make(map[uint32]struct{}),
		addedInstances:     make(map[string]struct{}),
		replacingInstances: make(map[string]struct{}),
	}

	// Store shard assignment
	for _, shardID := range shards {
		assignment.shards[shardID] = struct{}{}
	}

	// Mark existing instances as added
	for _, instance := range instances {
		assignment.addedInstances[instance.ID()] = struct{}{}
	}

	ph.subClusterAssignments[subClusterID] = assignment
}

func (ph *subclusteredhelperv3) getTargetSubClusterLoad(excludeSubCluster uint32) map[uint32]int {
	totalDivided := 0
	totalShards := len(ph.uniqueShards)
	subClusters := getKeys(ph.subClusters, 0)
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

func (ph *subclusteredhelperv3) scanCurrentLoad() {
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

func (ph *subclusteredhelperv3) buildTargetLoad() {
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

func (ph *subclusteredhelperv3) Instances() []placement.Instance {
	res := make([]placement.Instance, 0, len(ph.instances))
	for _, instance := range ph.instances {
		res = append(res, instance)
	}
	return res
}

func (ph *subclusteredhelperv3) getShardLen() int {
	return len(ph.uniqueShards)
}

func (ph *subclusteredhelperv3) targetLoadForInstance(id string) int {
	return ph.targetLoad[id]
}

func (ph *subclusteredhelperv3) moveOneShard(from, to placement.Instance) bool {
	// The order matter here:
	// The Unknown shards were just moved, so free to be moved around.
	// The Initializing shards were still being initialized on the instance,
	// so moving them are cheaper than moving those Available shards.
	return ph.moveOneShardInState(from, to, shard.Unknown) ||
		ph.moveOneShardInState(from, to, shard.Initializing) ||
		ph.moveOneShardInState(from, to, shard.Available)
}

func (ph *subclusteredhelperv3) moveOneShardInState(from, to placement.Instance, state shard.State) bool {
	for _, s := range from.Shards().ShardsForState(state) {
		if ph.moveShard(s, from, to) {
			return true
		}
	}
	return false
}

func (ph *subclusteredhelperv3) moveShard(candidateShard shard.Shard, from, to placement.Instance) bool {
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

func (ph *subclusteredhelperv3) CanMoveShard(shard uint32, from placement.Instance, toIsolationGroup string) bool {
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

func (ph *subclusteredhelperv3) buildInstanceHeap(instances []placement.Instance, availableCapacityAscending bool) (heap.Interface, error) {
	return newHeap(instances, availableCapacityAscending, ph.targetLoad, ph.groupToWeightMap)
}

func (ph *subclusteredhelperv3) generatePlacement() placement.Placement {
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

func (ph *subclusteredhelperv3) placeShards(
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

func (ph *subclusteredhelperv3) returnInitializingShards(instance placement.Instance) {
	shardSet := getShardMap(instance.Shards().All())
	ph.returnInitializingShardsToSource(shardSet, instance, ph.Instances())
}

func (ph *subclusteredhelperv3) returnInitializingShardsToSource(
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

func (ph *subclusteredhelperv3) mostUnderLoadedInstance() (placement.Instance, bool) {
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

func (ph *subclusteredhelperv3) optimize(t optimizeType) error {
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

func (ph *subclusteredhelperv3) assignLoadToInstanceSafe(addingInstance placement.Instance) error {
	return ph.assignTargetLoad(addingInstance, func(from, to placement.Instance) bool {
		return ph.moveOneShardInState(from, to, shard.Unknown)
	})
}

func (ph *subclusteredhelperv3) assignLoadToInstanceUnsafe(addingInstance placement.Instance) error {
	return ph.assignTargetLoad(addingInstance, func(from, to placement.Instance) bool {
		return ph.moveOneShard(from, to)
	})
}

func (ph *subclusteredhelperv3) reclaimLeavingShards(instance placement.Instance) {
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

func (ph *subclusteredhelperv3) addInstance(addingInstance placement.Instance) error {
	// Get instances in the same subcluster
	subClusterInstances := make([]placement.Instance, 0)
	for _, instance := range ph.instances {
		if instance.SubClusterID() == addingInstance.SubClusterID() {
			subClusterInstances = append(subClusterInstances, instance)
		}
	}

	// Validate instance weight
	if err := ph.validateInstanceWeight(addingInstance, subClusterInstances); err != nil {
		return fmt.Errorf("invalid instance weight during add: %v", err)
	}

	// Get shards assigned to this subcluster
	subClusterShards := ph.getSubClusterShards(addingInstance.SubClusterID())

	// For each shard in the subcluster, ensure RF replicas
	for _, shardID := range subClusterShards {
		// Count current replicas for this shard in the subcluster
		replicaCount := 0
		for _, instance := range subClusterInstances {
			if s, ok := instance.Shards().Shard(shardID); ok && s.State() != shard.Leaving {
				replicaCount++
			}
		}

		// If we need more replicas, assign them to the new instance
		if replicaCount < ph.rf {
			newShard := shard.NewShard(shardID).SetState(shard.Initializing)
			ph.assignShardToInstance(newShard, addingInstance)
		}
	}

	return nil
}

func (ph *subclusteredhelperv3) replaceInstance(
	leavingInstance placement.Instance,
	addingInstance placement.Instance,
) error {
	// Get instances in the same subcluster
	subClusterInstances := make([]placement.Instance, 0)
	for _, instance := range ph.instances {
		if instance.SubClusterID() == addingInstance.SubClusterID() && instance.ID() != leavingInstance.ID() {
			subClusterInstances = append(subClusterInstances, instance)
		}
	}

	// Validate instance weight
	if err := ph.validateInstanceWeight(addingInstance, subClusterInstances); err != nil {
		return fmt.Errorf("invalid instance weight during replace: %v", err)
	}

	// Validate subcluster assignment before replacement
	if err := ph.validateSubClusterAssignment(leavingInstance.SubClusterID()); err != nil {
		return fmt.Errorf("invalid subcluster assignment before replacement: %v", err)
	}

	// Get the shard assignment for this subcluster
	assignment, exists := ph.subClusterAssignments[leavingInstance.SubClusterID()]
	if !exists {
		return fmt.Errorf("no shard assignment found for subcluster %d", leavingInstance.SubClusterID())
	}

	// Validate instances
	if leavingInstance.SubClusterID() != addingInstance.SubClusterID() {
		return fmt.Errorf("replacement instances must be in the same subcluster")
	}

	if _, exists := assignment.replacingInstances[leavingInstance.ID()]; exists {
		return fmt.Errorf("instance %s is already being replaced", leavingInstance.ID())
	}

	if _, exists := assignment.replacingInstances[addingInstance.ID()]; exists {
		return fmt.Errorf("instance %s is already being replaced", addingInstance.ID())
	}

	// Mark instances as being replaced
	assignment.replacingInstances[leavingInstance.ID()] = struct{}{}
	assignment.replacingInstances[addingInstance.ID()] = struct{}{}

	// Store original shard assignment for rollback if needed
	originalShards := make(map[uint32]shard.Shard)
	for _, s := range leavingInstance.Shards().All() {
		originalShards[s.ID()] = s
	}

	// Transfer shards from leaving instance to adding instance
	for _, s := range leavingInstance.Shards().All() {
		if s.State() == shard.Leaving {
			continue
		}
		newShard := shard.NewShard(s.ID()).SetState(shard.Initializing)
		ph.assignShardToInstance(newShard, addingInstance)
	}

	// Validate shard assignment after replacement
	if err := ph.validateSubClusterAssignment(leavingInstance.SubClusterID()); err != nil {
		// Rollback changes if validation fails
		for _, s := range addingInstance.Shards().All() {
			addingInstance.Shards().Remove(s.ID())
		}
		for _, s := range originalShards {
			ph.assignShardToInstance(s, leavingInstance)
		}
		return fmt.Errorf("invalid shard assignment after replacement: %v", err)
	}

	// Clean up replacing instances after successful transfer
	delete(assignment.replacingInstances, leavingInstance.ID())
	delete(assignment.replacingInstances, addingInstance.ID())

	return nil
}

// validateSubClusterAssignment validates the shard assignment for a subcluster
func (ph *subclusteredhelperv3) validateSubClusterAssignment(subClusterID uint32) error {
	assignment, exists := ph.subClusterAssignments[subClusterID]
	if !exists {
		return fmt.Errorf("no shard assignment found for subcluster %d", subClusterID)
	}

	// Validate that all shards in the subcluster are properly assigned
	instances := make([]placement.Instance, 0)
	for _, instance := range ph.instances {
		if instance.SubClusterID() == subClusterID {
			instances = append(instances, instance)
		}
	}

	// Check if we have enough instances for replication
	if len(instances) < ph.rf {
		return fmt.Errorf("subcluster %d has %d instances but requires %d for replication",
			subClusterID, len(instances), ph.rf)
	}

	// Validate shard distribution
	shardCounts := make(map[uint32]int)
	for _, instance := range instances {
		for _, s := range instance.Shards().All() {
			if s.State() == shard.Leaving {
				continue
			}
			shardCounts[s.ID()]++
		}
	}

	// Check replication factor for each shard
	for shardID := range assignment.shards {
		if count := shardCounts[shardID]; count != ph.rf {
			return fmt.Errorf("shard %d in subcluster %d has %d replicas, expected %d",
				shardID, subClusterID, count, ph.rf)
		}
	}

	return nil
}

func (ph *subclusteredhelperv3) assignTargetLoad(
	targetInstance placement.Instance,
	moveOneShardFn func(from, to placement.Instance) bool,
) error {
	targetLoad := ph.targetLoadForInstance(targetInstance.ID())

	// Get all non-leaving instances in the same subcluster
	subClusterInstances := make([]placement.Instance, 0)
	for _, instance := range ph.Instances() {
		if !instance.IsLeaving() && instance.SubClusterID() == targetInstance.SubClusterID() {
			subClusterInstances = append(subClusterInstances, instance)
		}
	}

	// Sort instances by number of shards (descending)
	sort.Slice(subClusterInstances, func(i, j int) bool {
		return subClusterInstances[i].Shards().NumShards() > subClusterInstances[j].Shards().NumShards()
	})

	// Try to move shards from most loaded instances until target load is reached
	for targetInstance.Shards().NumShards() < targetLoad {
		moved := false
		for _, fromInstance := range subClusterInstances {
			if fromInstance.ID() == targetInstance.ID() {
				continue
			}
			if moveOneShardFn(fromInstance, targetInstance) {
				moved = true
				break
			}
		}
		if !moved {
			break
		}
	}

	return nil
}

func (ph *subclusteredhelperv3) assignShardToInstance(s shard.Shard, to placement.Instance) {
	to.Shards().Add(s)

	if _, exist := ph.shardToInstanceMap[s.ID()]; !exist {
		ph.shardToInstanceMap[s.ID()] = make(map[placement.Instance]struct{})
	}
	ph.shardToInstanceMap[s.ID()][to] = struct{}{}
}

func (ph *subclusteredhelperv3) canAssignInstance(shardID uint32, from, to placement.Instance) bool {
	// Check if shard is already assigned to target instance
	s, ok := to.Shards().Shard(shardID)
	if ok && s.State() != shard.Leaving {
		return false
	}

	// Check if shard belongs to target subcluster
	if _, exist := ph.subClusterToShardMap[to.SubClusterID()][shardID]; !exist {
		return false
	}

	// Count replicas in each isolation group
	igCount := make(map[string]int)
	for instance := range ph.shardToInstanceMap[shardID] {
		if instance.IsLeaving() {
			continue
		}
		igCount[instance.IsolationGroup()]++
	}

	// If from instance exists and is in same isolation group as target, allow the move
	if from != nil && from.IsolationGroup() == to.IsolationGroup() {
		return true
	}

	// Check if target isolation group already has a replica
	if igCount[to.IsolationGroup()] > 0 {
		return false
	}

	// Count total replicas
	totalReplicas := 0
	for _, count := range igCount {
		totalReplicas += count
	}

	// Allow assignment if we haven't reached RF yet
	return totalReplicas < ph.rf
}

func (ph *subclusteredhelperv3) getSubClusterShards(subClusterID uint32) []uint32 {
	if ph.subClusterToShardMap[subClusterID] == nil {
		return nil
	}
	shards := make([]uint32, 0, len(ph.subClusterToShardMap[subClusterID]))
	for shardID := range ph.subClusterToShardMap[subClusterID] {
		shards = append(shards, shardID)
	}
	sort.Slice(shards, func(i, j int) bool { return shards[i] < shards[j] })
	return shards
}

func (ph *subclusteredhelperv3) validateInstanceWeight(newInstance placement.Instance, existingInstances []placement.Instance) error {
	if len(existingInstances) == 0 {
		return nil
	}

	// Get the weight of existing instances
	expectedWeight := existingInstances[0].Weight()
	for _, instance := range existingInstances {
		if instance.Weight() != expectedWeight {
			return fmt.Errorf("inconsistent instance weights: instance %s has weight %d, expected %d",
				instance.ID(), instance.Weight(), expectedWeight)
		}
	}

	// Check if new instance has the same weight
	if newInstance.Weight() != expectedWeight {
		return fmt.Errorf("new instance %s has weight %d, expected %d",
			newInstance.ID(), newInstance.Weight(), expectedWeight)
	}

	return nil
}
