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

// PlacementHelper helps the algorithm to place shards
type PlacementHelper interface {
	// PlaceShards distributes shards to the instances in the helper, with aware of where are the shards coming from
	PlaceShards(shards []shard.Shard, from services.PlacementInstance) error

	// GeneratePlacement generates a placement
	GeneratePlacement() services.ServicePlacement

	// TargetLoadForInstance returns the targe load for a instance
	TargetLoadForInstance(id string) int

	// MoveOneShard moves one shard between 2 instances, returns true if any shard is moved
	MoveOneShard(from, to services.PlacementInstance) bool

	// MoveShard moves a particular shard between 2 instances, returns true if the shard is moved
	MoveShard(s shard.Shard, from, to services.PlacementInstance) bool

	// HasNoRackConflict checks if the rack constraint is violated if the given shard is moved to the target rack
	HasNoRackConflict(shard uint32, from services.PlacementInstance, toRack string) bool

	// BuildInstanceHeap returns heap of instances sorted by available capacity
	BuildInstanceHeap(instances []services.PlacementInstance, availableCapacityAscending bool) heap.Interface
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

func (ph *placementHelper) TargetLoadForInstance(id string) int {
	return ph.targetLoad[id]
}

func (ph *placementHelper) MoveOneShard(from, to services.PlacementInstance) bool {
	for _, s := range from.Shards().All() {
		if s.State() != shard.Leaving && ph.MoveShard(s, from, to) {
			return true
		}
	}
	return false
}

func (ph *placementHelper) MoveShard(candidateShard shard.Shard, from, to services.PlacementInstance) bool {
	if !ph.canAssignInstance(candidateShard.ID(), from, to) {
		return false
	}

	if candidateShard.State() == shard.Leaving {
		// should not move a Leaving shard,
		// Leaving shard will be removed when the Initializing shard is marked as Available
		return false
	}

	newShard := shard.NewShard(candidateShard.ID())
	newShard.SetState(shard.Initializing)

	if from != nil {
		if candidateShard.State() == shard.Initializing {
			from.Shards().Remove(candidateShard.ID())
			newShard.SetSourceID(candidateShard.SourceID())
		} else if candidateShard.State() == shard.Available {
			candidateShard.SetState(shard.Leaving)
			newShard.SetSourceID(from.ID())
		}

		delete(ph.shardToInstanceMap[candidateShard.ID()], from)
	}

	curShard, ok := to.Shards().Shard(candidateShard.ID())
	if ok && curShard.State() == shard.Leaving {
		// NB(cw): if the instance already owns the shard in Leaving state,
		// simply mark it as Available
		newShard = shard.NewShard(newShard.ID()).SetState(shard.Available)
	}

	ph.assignShardToInstance(newShard, to)
	return true
}

func (ph *placementHelper) HasNoRackConflict(shard uint32, from services.PlacementInstance, toRack string) bool {
	if from != nil {
		if from.Rack() == toRack {
			return true
		}
	}
	for instance := range ph.shardToInstanceMap[shard] {
		if instance.Rack() == toRack {
			return false
		}
	}
	return true
}

func (ph *placementHelper) BuildInstanceHeap(instances []services.PlacementInstance, availableCapacityAscending bool) heap.Interface {
	return newHeap(instances, availableCapacityAscending, ph.targetLoad, ph.rackToWeightMap)
}

func (ph *placementHelper) GeneratePlacement() services.ServicePlacement {
	instancesWithLoad := make([]services.PlacementInstance, 0, len(ph.instances))
	for _, instance := range ph.instances {
		if loadOnInstance(instance) > 0 {
			instancesWithLoad = append(instancesWithLoad, instance)
		}
	}
	return placement.NewPlacement().
		SetInstances(ph.instanceList()).
		SetShards(ph.uniqueShards).
		SetReplicaFactor(ph.rf).
		SetIsSharded(true)
}

func (ph *placementHelper) PlaceShards(shards []shard.Shard, from services.PlacementInstance) error {
	shardSet := getShardMap(shards)
	if from != nil {
		// NB(cw) when removing an adding instance that has not finished bootstrapping its
		// Initializing shards, prefer to return those Initializing shards back to the leaving instance
		// to reduce some bootstrapping work in the cluster.
		if err := ph.returnInitializingShardsToSource(shardSet, from); err != nil {
			return err
		}
		// prefer to distribute "some" of the load to other racks first
		// because the load from a leaving instance can always get assigned to a instance on the same rack
		ph.placeToRacksOtherThanOrigin(shardSet, from)
	}

	instanceHeap := ph.BuildInstanceHeap(nonLeavingInstances(ph.instanceList()), true)
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
			if ph.MoveShard(s, from, tryInstance) {
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

func (ph *placementHelper) returnInitializingShardsToSource(shardSet map[uint32]shard.Shard, from services.PlacementInstance) error {
	for _, s := range shardSet {
		if s.State() != shard.Initializing {
			continue
		}
		sourceID := s.SourceID()
		if sourceID == "" {
			continue
		}
		sourceInstance, ok := ph.instances[sourceID]
		if !ok {
			return fmt.Errorf("could not find sourceID %s in the placement", sourceID)
		}
		if placement.IsInstanceLeaving(sourceInstance) {
			continue
		}
		if ph.MoveShard(s, from, sourceInstance) {
			delete(shardSet, s.ID())
		}
	}
	return nil
}

// placeToRacksOtherThanOrigin move shards from a instance to the rest of the cluster
// the goal of this function is to assign "some" of the shards to the instances in other racks
func (ph *placementHelper) placeToRacksOtherThanOrigin(shardsSet map[uint32]shard.Shard, from services.PlacementInstance) {
	otherRack := make([]services.PlacementInstance, 0, len(ph.instances))
	for rack, instances := range ph.rackToInstancesMap {
		if rack == from.Rack() {
			continue
		}
		for instance := range instances {
			otherRack = append(otherRack, instance)
		}
	}

	instanceHeap := ph.BuildInstanceHeap(nonLeavingInstances(otherRack), true)

	var triedInstances []services.PlacementInstance
	for shardID, s := range shardsSet {
		for instanceHeap.Len() > 0 {
			tryInstance := heap.Pop(instanceHeap).(services.PlacementInstance)
			if ph.TargetLoadForInstance(tryInstance.ID())-tryInstance.Shards().NumShards() <= 0 {
				// this is where "some" is, at this point the best instance option in the cluster
				// from a different rack has reached its target load, time to break out of the loop
				return
			}
			triedInstances = append(triedInstances, tryInstance)
			if ph.MoveShard(s, from, tryInstance) {
				delete(shardsSet, shardID)
				break
			}
		}

		for _, triedInstance := range triedInstances {
			heap.Push(instanceHeap, triedInstance)
		}
		triedInstances = triedInstances[:0]
	}
}

// NewPlacementHelper returns a placement helper
func NewPlacementHelper(p services.ServicePlacement, opts services.PlacementOptions) PlacementHelper {
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

func newAddReplicaHelper(p services.ServicePlacement, opts services.PlacementOptions) PlacementHelper {
	return newHelper(p, p.ReplicaFactor()+1, opts)
}

func newAddInstanceHelper(p services.ServicePlacement, i services.PlacementInstance, opts services.PlacementOptions) PlacementHelper {
	p = placement.NewPlacement().
		SetInstances(append(p.Instances(), i)).
		SetShards(p.Shards()).
		SetReplicaFactor(p.ReplicaFactor()).
		SetIsSharded(p.IsSharded())
	return newHelper(p, p.ReplicaFactor(), opts)
}

func newRemoveInstanceHelper(
	p services.ServicePlacement,
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
	p services.ServicePlacement,
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
		p, newAddingInstances[i], err = addInstanceToPlacement(p, instance, true)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	return newHelper(p, p.ReplicaFactor(), opts), leavingInstance, newAddingInstances, nil
}

func newHelper(p services.ServicePlacement, targetRF int, opts services.PlacementOptions) PlacementHelper {
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

func isRackOverWeight(rackWeight, totalWeight uint32, rf int) bool {
	return float64(rackWeight)/float64(totalWeight) >= 1.0/float64(rf)
}

func (ph *placementHelper) instanceList() []services.PlacementInstance {
	res := make([]services.PlacementInstance, 0, len(ph.instances))
	for _, instance := range ph.instances {
		res = append(res, instance)
	}
	return res
}

func (ph *placementHelper) getShardLen() int {
	return len(ph.uniqueShards)
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
	return ph.opts.LooseRackCheck() || ph.HasNoRackConflict(shardID, from, to.Rack())
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
) *instanceHeap {
	h := &instanceHeap{
		capacityAscending: capacityAscending,
		instances:         instances,
		targetLoad:        targetLoad,
		rackToWeightMap:   rackToWeightMap,
	}
	heap.Init(h)
	return h
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

// MarkShardAvailable marks the state of a shard to available
func MarkShardAvailable(p services.ServicePlacement, instanceID string, shardID uint32) (services.ServicePlacement, error) {
	p = clonePlacement(p)
	instance, exist := p.Instance(instanceID)
	if !exist {
		return nil, fmt.Errorf("instance %s does not exist in placement", instanceID)
	}

	s, exist := instance.Shards().Shard(shardID)
	if !exist {
		return nil, fmt.Errorf("shard %d does not exist in instance %s", shardID, instanceID)
	}

	if s.State() != shard.Initializing {
		return nil, fmt.Errorf("could not mark shard %d as available, it's not in Initializing state", s.ID())
	}

	sourceID := s.SourceID()
	s.SetState(shard.Available).SetSourceID("")

	// there could be no source for cases like initial placement
	if sourceID == "" {
		return p, nil
	}

	sourceInstance, exist := p.Instance(sourceID)
	if !exist {
		return nil, fmt.Errorf("source instance %s for shard %d does not exist in placement", sourceID, shardID)
	}

	sourceShard, exist := sourceInstance.Shards().Shard(shardID)
	if !exist {
		return nil, fmt.Errorf("shard %d does not exist in source instance %s", shardID, sourceID)
	}

	if sourceShard.State() != shard.Leaving {
		return nil, fmt.Errorf("shard %d is not leaving instance %s", shardID, sourceID)
	}

	sourceInstance.Shards().Remove(shardID)
	if sourceInstance.Shards().NumShards() == 0 {
		return placement.NewPlacement().
			SetInstances(removeInstance(p.Instances(), sourceInstance.ID())).
			SetShards(p.Shards()).
			SetReplicaFactor(p.ReplicaFactor()).
			SetIsSharded(p.IsSharded()), nil
	}
	return p, nil
}

func removeInstance(list []services.PlacementInstance, instanceID string) []services.PlacementInstance {
	for i, instance := range list {
		if instance.ID() == instanceID {
			return append(list[:i], list[i+1:]...)
		}
	}
	return list
}

func addInstanceToPlacement(p services.ServicePlacement, i services.PlacementInstance, allowEmpty bool) (services.ServicePlacement, services.PlacementInstance, error) {
	if _, exist := p.Instance(i.ID()); exist {
		return nil, nil, errAddingInstanceAlreadyExist
	}
	instance := cloneInstance(i)

	if allowEmpty || instance.Shards().NumShards() > 0 {
		p = placement.NewPlacement().
			SetInstances(append(p.Instances(), instance)).
			SetShards(p.Shards()).
			SetReplicaFactor(p.ReplicaFactor()).
			SetIsSharded(p.IsSharded())
	}
	return p, instance, nil
}

func removeInstanceFromPlacement(p services.ServicePlacement, id string) (services.ServicePlacement, services.PlacementInstance, error) {
	leavingInstance, exist := p.Instance(id)
	if !exist {
		return nil, nil, fmt.Errorf("instance %s does not exist in placement", id)
	}

	return placement.NewPlacement().
		SetInstances(removeInstance(p.Instances(), id)).
		SetShards(p.Shards()).
		SetReplicaFactor(p.ReplicaFactor()).
		SetIsSharded(p.IsSharded()), leavingInstance, nil
}

func clonePlacement(p services.ServicePlacement) services.ServicePlacement {
	return placement.NewPlacement().
		SetInstances(cloneInstances(p.Instances())).
		SetShards(p.Shards()).
		SetReplicaFactor(p.ReplicaFactor()).
		SetIsSharded(p.IsSharded())
}

func cloneInstances(instances []services.PlacementInstance) []services.PlacementInstance {
	copied := make([]services.PlacementInstance, len(instances))
	for i, instance := range instances {
		copied[i] = cloneInstance(instance)
	}
	return copied
}

func cloneInstance(instance services.PlacementInstance) services.PlacementInstance {
	return placement.NewInstance().
		SetID(instance.ID()).
		SetRack(instance.Rack()).
		SetZone(instance.Zone()).
		SetWeight(instance.Weight()).
		SetEndpoint(instance.Endpoint()).
		SetShards(cloneShards(instance.Shards()))
}

func cloneShards(shards shard.Shards) shard.Shards {
	newShards := make([]shard.Shard, shards.NumShards())
	for i, s := range shards.All() {
		newShards[i] = shard.NewShard(s.ID()).SetState(s.State()).SetSourceID(s.SourceID())
	}

	return shard.NewShards(newShards)
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
		r[i] = shard.NewShard(id).SetState(shard.Initializing)
	}
	return r
}
