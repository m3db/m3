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

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
	"github.com/m3db/m3cluster/shard"
)

type includeInstanceType int

const (
	includeEmpty includeInstanceType = iota
	nonEmptyOnly
)

type optimizeType int

const (
	// safe optimizes the load distribution without violating
	// minimal shard movemoment.
	safe optimizeType = iota
	// unsafe optimizes the load distribution with the potential of violating
	// minimal shard movement in order to reach best shard distribution
	unsafe
)

type assignLoadFn func(instance services.PlacementInstance)

// PlacementHelper helps the algorithm to place shards
type PlacementHelper interface {
	// Instances returns the list of instances managed by the PlacementHelper
	Instances() []services.PlacementInstance

	// HasRackConflict checks if the rack constraint is violated when moving the shard to the target rack
	HasRackConflict(shard uint32, from services.PlacementInstance, toRack string) bool

	// PlaceShards distributes shards to the instances in the helper, with aware of where are the shards coming from
	PlaceShards(shards []shard.Shard, from services.PlacementInstance, candidates []services.PlacementInstance) error

	// AddInstance adds an instance to the placement
	AddInstance(addingInstance services.PlacementInstance)

	// Optimize rebalances the load distribution in the cluster
	Optimize(t optimizeType)

	// GeneratePlacement generates a placement
	GeneratePlacement() services.Placement
}

type placementHelper struct {
	targetLoad         map[string]int
	shardToInstanceMap map[uint32]map[services.PlacementInstance]struct{}
	rackToInstancesMap map[string]map[services.PlacementInstance]struct{}
	rackToWeightMap    map[string]uint32
	totalWeight        uint32
	rf                 int
	uniqueShards       []uint32
	instances          map[string]services.PlacementInstance
	opts               services.PlacementOptions
}

// NewPlacementHelper returns a placement helper
func NewPlacementHelper(p services.Placement, opts services.PlacementOptions) PlacementHelper {
	return newHelper(p, p.ReplicaFactor(), opts)
}

func newInitHelper(instances []services.PlacementInstance, ids []uint32, opts services.PlacementOptions) PlacementHelper {
	emptyPlacement := placement.NewPlacement().
		SetInstances(instances).
		SetShards(ids).
		SetReplicaFactor(0).
		SetIsSharded(true)
	return newHelper(emptyPlacement, emptyPlacement.ReplicaFactor()+1, opts)
}

func newAddReplicaHelper(p services.Placement, opts services.PlacementOptions) PlacementHelper {
	return newHelper(p, p.ReplicaFactor()+1, opts)
}

func newAddInstanceHelper(p services.Placement, i services.PlacementInstance, opts services.PlacementOptions) PlacementHelper {
	p = placement.NewPlacement().
		SetInstances(append(p.Instances(), i)).
		SetShards(p.Shards()).
		SetReplicaFactor(p.ReplicaFactor()).
		SetIsSharded(p.IsSharded())
	return newHelper(p, p.ReplicaFactor(), opts)
}

func newRemoveInstanceHelper(
	p services.Placement,
	instanceID string,
	opts services.PlacementOptions,
) (PlacementHelper, services.PlacementInstance, error) {
	p, leavingInstance, err := removeInstanceFromPlacement(p, instanceID)
	if err != nil {
		return nil, nil, err
	}
	return newHelper(p, p.ReplicaFactor(), opts), leavingInstance, nil
}

func newReplaceInstanceHelper(
	p services.Placement,
	instanceID string,
	addingInstances []services.PlacementInstance,
	opts services.PlacementOptions,
) (PlacementHelper, services.PlacementInstance, []services.PlacementInstance, error) {
	p, leavingInstance, err := removeInstanceFromPlacement(p, instanceID)
	if err != nil {
		return nil, nil, nil, err
	}

	newAddingInstances := make([]services.PlacementInstance, len(addingInstances))
	for i, instance := range addingInstances {
		p, newAddingInstances[i], err = addInstanceToPlacement(p, instance, includeEmpty)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	return newHelper(p, p.ReplicaFactor(), opts), leavingInstance, newAddingInstances, nil
}

func newHelper(p services.Placement, targetRF int, opts services.PlacementOptions) PlacementHelper {
	ph := &placementHelper{
		rf:           targetRF,
		instances:    make(map[string]services.PlacementInstance, p.NumInstances()),
		uniqueShards: p.Shards(),
		opts:         opts,
	}

	for _, instance := range p.Instances() {
		ph.instances[instance.ID()] = instance
	}

	ph.scanCurrentLoad()
	ph.buildTargetLoad()
	return ph
}

func (ph *placementHelper) scanCurrentLoad() {
	ph.shardToInstanceMap = make(map[uint32]map[services.PlacementInstance]struct{}, len(ph.uniqueShards))
	ph.rackToInstancesMap = make(map[string]map[services.PlacementInstance]struct{})
	ph.rackToWeightMap = make(map[string]uint32)
	totalWeight := uint32(0)
	for _, instance := range ph.instances {
		if _, exist := ph.rackToInstancesMap[instance.Rack()]; !exist {
			ph.rackToInstancesMap[instance.Rack()] = make(map[services.PlacementInstance]struct{})
		}
		ph.rackToInstancesMap[instance.Rack()][instance] = struct{}{}

		ph.rackToWeightMap[instance.Rack()] = ph.rackToWeightMap[instance.Rack()] + instance.Weight()
		totalWeight += instance.Weight()

		for _, s := range instance.Shards().All() {
			if s.State() == shard.Leaving {
				continue
			}
			ph.assignShardToInstance(s, instance)
		}
	}
	ph.totalWeight = totalWeight
}

func (ph *placementHelper) buildTargetLoad() {
	overWeightedRack := 0
	overWeight := uint32(0)
	for _, weight := range ph.rackToWeightMap {
		if isRackOverWeight(weight, ph.totalWeight, ph.rf) {
			overWeightedRack++
			overWeight += weight
		}
	}

	targetLoad := make(map[string]int)
	for _, instance := range ph.instances {
		rackWeight := ph.rackToWeightMap[instance.Rack()]
		if isRackOverWeight(rackWeight, ph.totalWeight, ph.rf) {
			// if the instance is on a over-sized rack, the target load is topped at shardLen / rackSize
			targetLoad[instance.ID()] = int(math.Ceil(float64(ph.getShardLen()) * float64(instance.Weight()) / float64(rackWeight)))
		} else {
			// if the instance is on a normal rack, get the target load with aware of other over-sized rack
			targetLoad[instance.ID()] = ph.getShardLen() * (ph.rf - overWeightedRack) * int(instance.Weight()) / int(ph.totalWeight-overWeight)
		}
	}
	ph.targetLoad = targetLoad
}

func (ph *placementHelper) Instances() []services.PlacementInstance {
	res := make([]services.PlacementInstance, 0, len(ph.instances))
	for _, instance := range ph.instances {
		res = append(res, instance)
	}
	return res
}

func (ph *placementHelper) getShardLen() int {
	return len(ph.uniqueShards)
}

func (ph *placementHelper) targetLoadForInstance(id string) int {
	return ph.targetLoad[id]
}

func (ph *placementHelper) moveOneShard(from, to services.PlacementInstance) bool {
	for _, s := range from.Shards().All() {
		if s.State() != shard.Leaving && ph.moveShard(s, from, to) {
			return true
		}
	}
	return false
}

// nolint: unparam
func (ph *placementHelper) moveOneShardInState(from, to services.PlacementInstance, state shard.State) bool {
	for _, s := range from.Shards().ShardsForState(state) {
		if ph.moveShard(s, from, to) {
			return true
		}
	}
	return false
}

func (ph *placementHelper) moveShard(candidateShard shard.Shard, from, to services.PlacementInstance) bool {
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
			candidateShard.SetState(shard.Leaving)
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

func (ph *placementHelper) HasRackConflict(shard uint32, from services.PlacementInstance, toRack string) bool {
	if from != nil {
		if from.Rack() == toRack {
			return false
		}
	}
	for instance := range ph.shardToInstanceMap[shard] {
		if instance.Rack() == toRack {
			return true
		}
	}
	return false
}

func (ph *placementHelper) buildInstanceHeap(instances []services.PlacementInstance, availableCapacityAscending bool) (heap.Interface, error) {
	return newHeap(instances, availableCapacityAscending, ph.targetLoad, ph.rackToWeightMap)
}

func (ph *placementHelper) GeneratePlacement() services.Placement {
	var instances = make([]services.PlacementInstance, 0, len(ph.instances))

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
				SetState(shard.Initializing))
		}
	}

	return placement.NewPlacement().
		SetInstances(instances).
		SetShards(ph.uniqueShards).
		SetReplicaFactor(ph.rf).
		SetIsSharded(true).
		SetIsMirrored(ph.opts.IsMirrored())
}

func (ph *placementHelper) PlaceShards(
	shards []shard.Shard,
	from services.PlacementInstance,
	candidates []services.PlacementInstance,
) error {
	shardSet := getShardMap(shards)
	if from != nil {
		// NB(cw) when removing an adding instance that has not finished bootstrapping its
		// Initializing shards, prefer to return those Initializing shards back to the leaving instance
		// to reduce some bootstrapping work in the cluster.
		if err := ph.returnInitializingShardsToSource(shardSet, from, candidates); err != nil {
			return err
		}
		// prefer to distribute "some" of the load to other racks first
		// because the load from a leaving instance can always get assigned to a instance on the same rack
		if err := ph.placeToRacksOtherThanOrigin(shardSet, from, candidates); err != nil {
			return err
		}
	}

	instanceHeap, err := ph.buildInstanceHeap(candidates, true)
	if err != nil {
		return err
	}
	// if there are shards left to be assigned, distribute them evenly
	var triedInstances []services.PlacementInstance
	for _, s := range shardSet {
		if s.State() == shard.Leaving {
			continue
		}
		moved := false
		for instanceHeap.Len() > 0 {
			tryInstance := heap.Pop(instanceHeap).(services.PlacementInstance)
			triedInstances = append(triedInstances, tryInstance)
			if ph.moveShard(s, from, tryInstance) {
				moved = true
				break
			}
		}
		if !moved {
			// this should only happen when RF > number of racks
			return errNotEnoughRacks
		}
		for _, triedInstance := range triedInstances {
			heap.Push(instanceHeap, triedInstance)
		}
		triedInstances = triedInstances[:0]
	}
	return nil
}

func (ph *placementHelper) returnInitializingShardsToSource(
	shardSet map[uint32]shard.Shard,
	from services.PlacementInstance,
	candidates []services.PlacementInstance,
) error {
	candidateMap := make(map[string]services.PlacementInstance, len(candidates))
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
			return fmt.Errorf("could not find sourceID %s in the placement", sourceID)
		}
		if placement.IsInstanceLeaving(sourceInstance) {
			continue
		}
		if ph.moveShard(s, from, sourceInstance) {
			delete(shardSet, s.ID())
		}
	}
	return nil
}

// placeToRacksOtherThanOrigin move shards from a instance to the rest of the cluster
// the goal of this function is to assign "some" of the shards to the instances in other racks
func (ph *placementHelper) placeToRacksOtherThanOrigin(
	shardsSet map[uint32]shard.Shard,
	from services.PlacementInstance,
	candidates []services.PlacementInstance,
) error {
	otherRack := make([]services.PlacementInstance, 0, len(candidates))
	rack := from.Rack()
	for _, instance := range candidates {
		if instance.Rack() == rack {
			continue
		}
		otherRack = append(otherRack, instance)
	}

	instanceHeap, err := ph.buildInstanceHeap(nonLeavingInstances(otherRack), true)
	if err != nil {
		return err
	}
	var triedInstances []services.PlacementInstance
	for shardID, s := range shardsSet {
		for instanceHeap.Len() > 0 {
			tryInstance := heap.Pop(instanceHeap).(services.PlacementInstance)
			if ph.targetLoadForInstance(tryInstance.ID())-tryInstance.Shards().NumShards() <= 0 {
				// this is where "some" is, at this point the best instance option in the cluster
				// from a different rack has reached its target load, time to break out of the loop
				return nil
			}
			triedInstances = append(triedInstances, tryInstance)
			if ph.moveShard(s, from, tryInstance) {
				delete(shardsSet, shardID)
				break
			}
		}

		for _, triedInstance := range triedInstances {
			heap.Push(instanceHeap, triedInstance)
		}
		triedInstances = triedInstances[:0]
	}
	return nil
}

func (ph *placementHelper) mostUnderLoadedInstance() (services.PlacementInstance, bool) {
	var (
		res        services.PlacementInstance
		maxLoadGap int
	)

	for id, instance := range ph.instances {
		loadGap := ph.targetLoad[id] - loadOnInstance(instance)
		if loadGap > maxLoadGap {
			maxLoadGap = loadGap
			res = instance
		}
	}
	if maxLoadGap > 0 {
		return res, true
	}

	return nil, false
}

func (ph *placementHelper) Optimize(t optimizeType) {
	var fn assignLoadFn
	switch t {
	case safe:
		fn = ph.assignLoadToInstanceSafe
	case unsafe:
		fn = ph.assignLoadToInstanceUnsafe
	}
	ph.optimize(fn)
}

func (ph *placementHelper) optimize(fn assignLoadFn) {
	uniq := make(map[string]struct{}, len(ph.instances))
	for {
		ins, ok := ph.mostUnderLoadedInstance()
		if !ok {
			return
		}
		if _, exist := uniq[ins.ID()]; exist {
			return
		}

		uniq[ins.ID()] = struct{}{}
		fn(ins)
	}
}

func (ph *placementHelper) assignLoadToInstanceSafe(addingInstance services.PlacementInstance) {
	ph.assignTargetLoad(addingInstance, func(from, to services.PlacementInstance) bool {
		return ph.moveOneShardInState(from, to, shard.Unknown)
	})
}

func (ph *placementHelper) assignLoadToInstanceUnsafe(addingInstance services.PlacementInstance) {
	ph.assignTargetLoad(addingInstance, func(from, to services.PlacementInstance) bool {
		return ph.moveOneShard(from, to)
	})
}

func (ph *placementHelper) AddInstance(addingInstance services.PlacementInstance) {
	id := addingInstance.ID()

	for _, instance := range ph.instances {
		for _, s := range instance.Shards().ShardsForState(shard.Initializing) {
			if s.SourceID() == id {
				// NB(cw) in very rare case, the leaving shards could not be taken back.
				// For example: in a RF=2 case, instance a and b on rack1, instance c on rack2,
				// c took shard1 from instance a, before we tried to assign shard1 back to instance a,
				// b got assigned shard1, now if we try to add instance a back to the topology, a can
				// no longer take shard1 back.
				// But it's fine, the algo will fil up those load with other shards from the cluster
				ph.moveShard(s, instance, addingInstance)
			}
		}
	}

	ph.assignLoadToInstanceUnsafe(addingInstance)
}

func (ph *placementHelper) assignTargetLoad(
	addingInstance services.PlacementInstance,
	fn func(from, to services.PlacementInstance) bool,
) error {
	targetLoad := ph.targetLoadForInstance(addingInstance.ID())
	// try to take shards from the most loaded instances until the adding instance reaches target load
	instanceHeap, err := ph.buildInstanceHeap(nonLeavingInstances(ph.Instances()), false)
	if err != nil {
		return err
	}
	for addingInstance.Shards().NumShards() < targetLoad && instanceHeap.Len() > 0 {
		tryInstance := heap.Pop(instanceHeap).(services.PlacementInstance)
		if moved := fn(tryInstance, addingInstance); moved {
			heap.Push(instanceHeap, tryInstance)
		}
	}
	return nil
}

func (ph *placementHelper) canAssignInstance(shardID uint32, from, to services.PlacementInstance) bool {
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
	return ph.opts.LooseRackCheck() || !ph.HasRackConflict(shardID, from, to.Rack())
}

func (ph *placementHelper) assignShardToInstance(s shard.Shard, to services.PlacementInstance) {
	to.Shards().Add(s)

	if _, exist := ph.shardToInstanceMap[s.ID()]; !exist {
		ph.shardToInstanceMap[s.ID()] = make(map[services.PlacementInstance]struct{})
	}
	ph.shardToInstanceMap[s.ID()][to] = struct{}{}
}

// instanceHeap provides an easy way to get best candidate instance to assign/steal a shard
type instanceHeap struct {
	instances         []services.PlacementInstance
	rackToWeightMap   map[string]uint32
	targetLoad        map[string]int
	capacityAscending bool
}

func newHeap(
	instances []services.PlacementInstance,
	capacityAscending bool,
	targetLoad map[string]int,
	rackToWeightMap map[string]uint32,
) (*instanceHeap, error) {
	for _, instance := range instances {
		id := instance.ID()
		if _, ok := targetLoad[id]; !ok {
			return nil, fmt.Errorf("could not build instance heap for instance: %s", id)
		}
	}
	h := &instanceHeap{
		capacityAscending: capacityAscending,
		instances:         instances,
		targetLoad:        targetLoad,
		rackToWeightMap:   rackToWeightMap,
	}
	heap.Init(h)
	return h, nil
}

func (h *instanceHeap) targetLoadForInstance(id string) int {
	return h.targetLoad[id]
}

func (h *instanceHeap) Len() int {
	return len(h.instances)
}

func (h *instanceHeap) Less(i, j int) bool {
	instanceI := h.instances[i]
	instanceJ := h.instances[j]
	leftLoadOnI := h.targetLoadForInstance(instanceI.ID()) - loadOnInstance(instanceI)
	leftLoadOnJ := h.targetLoadForInstance(instanceJ.ID()) - loadOnInstance(instanceJ)
	// if both instance has tokens to be filled, prefer the one on a bigger rack
	// since it tends to be more picky in accepting shards
	if leftLoadOnI > 0 && leftLoadOnJ > 0 {
		if instanceI.Rack() != instanceJ.Rack() {
			return h.rackToWeightMap[instanceI.Rack()] > h.rackToWeightMap[instanceJ.Rack()]
		}
	}
	// compare left capacity on both instances
	if h.capacityAscending {
		return leftLoadOnI > leftLoadOnJ
	}
	return leftLoadOnI < leftLoadOnJ
}

func (h instanceHeap) Swap(i, j int) {
	h.instances[i], h.instances[j] = h.instances[j], h.instances[i]
}

func (h *instanceHeap) Push(i interface{}) {
	instance := i.(services.PlacementInstance)
	h.instances = append(h.instances, instance)
}

func (h *instanceHeap) Pop() interface{} {
	n := len(h.instances)
	instance := h.instances[n-1]
	h.instances = h.instances[0 : n-1]
	return instance
}

func isRackOverWeight(rackWeight, totalWeight uint32, rf int) bool {
	return float64(rackWeight)/float64(totalWeight) >= 1.0/float64(rf)
}

func addInstanceToPlacement(p services.Placement, i services.PlacementInstance, includeInstance includeInstanceType) (services.Placement, services.PlacementInstance, error) {
	if _, exist := p.Instance(i.ID()); exist {
		return nil, nil, errAddingInstanceAlreadyExist
	}
	instance := placement.CloneInstance(i)

	if includeInstance == includeEmpty || instance.Shards().NumShards() > 0 {
		p = placement.NewPlacement().
			SetInstances(append(p.Instances(), instance)).
			SetShards(p.Shards()).
			SetReplicaFactor(p.ReplicaFactor()).
			SetIsSharded(p.IsSharded()).
			SetIsMirrored(p.IsMirrored())
	}
	return p, instance, nil
}

func removeInstanceFromPlacement(p services.Placement, id string) (services.Placement, services.PlacementInstance, error) {
	leavingInstance, exist := p.Instance(id)
	if !exist {
		return nil, nil, fmt.Errorf("instance %s does not exist in placement", id)
	}

	return placement.NewPlacement().
		SetInstances(placement.RemoveInstanceFromList(p.Instances(), id)).
		SetShards(p.Shards()).
		SetReplicaFactor(p.ReplicaFactor()).
		SetIsSharded(p.IsSharded()), leavingInstance, nil
}

func getShardMap(shards []shard.Shard) map[uint32]shard.Shard {
	r := make(map[uint32]shard.Shard, len(shards))

	for _, s := range shards {
		r[s.ID()] = s
	}
	return r
}

func loadOnInstance(instance services.PlacementInstance) int {
	return instance.Shards().NumShards() - instance.Shards().NumShardsForState(shard.Leaving)
}

func nonLeavingInstances(instances []services.PlacementInstance) []services.PlacementInstance {
	r := make([]services.PlacementInstance, 0, len(instances))
	for _, instance := range instances {
		if placement.IsInstanceLeaving(instance) {
			continue
		}
		r = append(r, instance)
	}

	return r
}

func newShards(shardIDs []uint32) []shard.Shard {
	r := make([]shard.Shard, len(shardIDs))
	for i, id := range shardIDs {
		r[i] = shard.NewShard(id)
	}
	return r
}
