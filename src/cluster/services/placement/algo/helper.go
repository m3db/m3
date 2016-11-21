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
	"math"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
	"github.com/m3db/m3cluster/shard"
)

// PlacementHelper helps the algorithm to place shards
type PlacementHelper interface {
	// PlaceShards distributes shards to the instances in the helper, with aware of where are the shards coming from
	PlaceShards(shards []uint32, from services.PlacementInstance) error
	// GeneratePlacement generates a placement
	GeneratePlacement() services.ServicePlacement
	// TargetLoadForInstance returns the targe load for a instance
	TargetLoadForInstance(id string) int
	// MoveOneShard moves one shard between 2 instances
	MoveOneShard(from, to services.PlacementInstance) bool
	// MoveShard moves a particular shard between 2 instances
	MoveShard(shard uint32, from, to services.PlacementInstance) bool
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
	instances          []services.PlacementInstance
	opts               services.PlacementOptions
}

func (ph *placementHelper) TargetLoadForInstance(id string) int {
	return ph.targetLoad[id]
}

func (ph *placementHelper) MoveOneShard(from, to services.PlacementInstance) bool {
	if from == nil {
		return false
	}
	for _, shard := range from.Shards().ShardIDs() {
		if ph.MoveShard(shard, from, to) {
			return true
		}
	}
	return false
}

func (ph *placementHelper) MoveShard(shard uint32, from, to services.PlacementInstance) bool {
	if ph.canAssignInstance(shard, from, to) {
		ph.assignShardToInstance(shard, to)
		ph.removeShardFromInstance(shard, from)
		return true
	}
	return false
}

func (ph placementHelper) HasNoRackConflict(shard uint32, from services.PlacementInstance, toRack string) bool {
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
	return placement.NewPlacement(ph.instances, ph.uniqueShards, ph.rf)
}

func (ph placementHelper) PlaceShards(shards []uint32, from services.PlacementInstance) error {
	shardSet := placement.ConvertShardSliceToMap(shards)
	if from != nil {
		// prefer to distribute "some" of the load to other racks first
		// because the load from a leaving instance can always get assigned to a instance on the same rack
		ph.placeToRacksOtherThanOrigin(shardSet, from)
	}

	instanceHeap := ph.BuildInstanceHeap(ph.instances, true)
	// if there are shards left to be assigned, distribute them evenly
	var triedInstances []services.PlacementInstance
	for shard := range shardSet {
		moved := false
		for instanceHeap.Len() > 0 {
			tryInstance := heap.Pop(instanceHeap).(services.PlacementInstance)
			triedInstances = append(triedInstances, tryInstance)
			if ph.MoveShard(shard, from, tryInstance) {
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

// placeToRacksOtherThanOrigin move shards from a instance to the rest of the cluster
// the goal of this function is to assign "some" of the shards to the instances in other racks
func (ph placementHelper) placeToRacksOtherThanOrigin(shardsSet map[uint32]int, from services.PlacementInstance) {
	var otherRack []services.PlacementInstance
	for rack, instances := range ph.rackToInstancesMap {
		if rack == from.Rack() {
			continue
		}
		for instance := range instances {
			otherRack = append(otherRack, instance)
		}
	}

	instanceHeap := ph.BuildInstanceHeap(otherRack, true)

	var triedInstances []services.PlacementInstance
	for shard := range shardsSet {
		for instanceHeap.Len() > 0 {
			tryInstance := heap.Pop(instanceHeap).(services.PlacementInstance)
			if ph.TargetLoadForInstance(tryInstance.ID())-tryInstance.Shards().NumShards() <= 0 {
				// this is where "some" is, at this point the best instance option in the cluster
				// from a different rack has reached its target load, time to break out of the loop
				return
			}
			triedInstances = append(triedInstances, tryInstance)
			if ph.MoveShard(shard, from, tryInstance) {
				delete(shardsSet, shard)
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
	emptyPlacement := placement.NewPlacement(instances, ids, 0)
	return newHelper(emptyPlacement, emptyPlacement.ReplicaFactor()+1, opts)
}

func newAddReplicaHelper(p services.ServicePlacement, opts services.PlacementOptions) PlacementHelper {
	return newHelper(p, p.ReplicaFactor()+1, opts)
}

func newAddInstanceHelper(p services.ServicePlacement, i services.PlacementInstance, opts services.PlacementOptions) PlacementHelper {
	p = placement.NewPlacement(append(p.Instances(), i), p.Shards(), p.ReplicaFactor())
	return newHelper(p, p.ReplicaFactor(), opts)
}

func newRemoveInstanceHelper(
	p services.ServicePlacement,
	i services.PlacementInstance,
	opts services.PlacementOptions,
) (PlacementHelper, services.PlacementInstance, error) {
	p, leavingInstance, err := removeInstanceFromPlacement(p, i)
	if err != nil {
		return nil, nil, err
	}
	return newHelper(p, p.ReplicaFactor(), opts), leavingInstance, nil
}

func newReplaceInstanceHelper(
	p services.ServicePlacement,
	leavingInstance services.PlacementInstance,
	addingInstances []services.PlacementInstance,
	opts services.PlacementOptions,
) (PlacementHelper, services.PlacementInstance, []services.PlacementInstance, error) {
	p, leavingInstance, err := removeInstanceFromPlacement(p, leavingInstance)
	if err != nil {
		return nil, nil, nil, err
	}

	newAddingInstances := make([]services.PlacementInstance, len(addingInstances))
	for i, instance := range addingInstances {
		p, newAddingInstances[i], err = addInstanceToPlacement(p, instance)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	return newHelper(p, p.ReplicaFactor(), opts), leavingInstance, newAddingInstances, nil
}

func newHelper(p services.ServicePlacement, targetRF int, opts services.PlacementOptions) PlacementHelper {
	ph := &placementHelper{
		rf:           targetRF,
		instances:    p.Instances(),
		uniqueShards: p.Shards(),
		opts:         opts,
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
	for _, h := range ph.instances {
		if _, exist := ph.rackToInstancesMap[h.Rack()]; !exist {
			ph.rackToInstancesMap[h.Rack()] = make(map[services.PlacementInstance]struct{})
		}
		ph.rackToInstancesMap[h.Rack()][h] = struct{}{}

		ph.rackToWeightMap[h.Rack()] = ph.rackToWeightMap[h.Rack()] + h.Weight()
		totalWeight += h.Weight()

		for _, shard := range h.Shards().ShardIDs() {
			ph.assignShardToInstance(shard, h)
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

func (ph placementHelper) getShardLen() int {
	return len(ph.uniqueShards)
}

func (ph placementHelper) canAssignInstance(shard uint32, from, to services.PlacementInstance) bool {
	if to.Shards().ContainsShard(shard) {
		return false
	}
	return ph.opts.LooseRackCheck() || ph.HasNoRackConflict(shard, from, to.Rack())
}

func (ph placementHelper) assignShardToInstance(shard uint32, to services.PlacementInstance) {
	if to == nil {
		return
	}

	to.Shards().AddShard(shard)

	if _, exist := ph.shardToInstanceMap[shard]; !exist {
		ph.shardToInstanceMap[shard] = make(map[services.PlacementInstance]struct{})
	}
	ph.shardToInstanceMap[shard][to] = struct{}{}
}

func (ph placementHelper) removeShardFromInstance(shard uint32, from services.PlacementInstance) {
	if from == nil {
		return
	}

	from.Shards().RemoveShard(shard)
	delete(ph.shardToInstanceMap[shard], from)
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
	hHeap := &instanceHeap{capacityAscending: capacityAscending, instances: instances, targetLoad: targetLoad, rackToWeightMap: rackToWeightMap}
	heap.Init(hHeap)
	return hHeap
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
	leftLoadOnI := h.targetLoadForInstance(instanceI.ID()) - instanceI.Shards().NumShards()
	leftLoadOnJ := h.targetLoadForInstance(instanceJ.ID()) - instanceJ.Shards().NumShards()
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

func addInstanceToPlacement(p services.ServicePlacement, i services.PlacementInstance) (services.ServicePlacement, services.PlacementInstance, error) {
	if p.Instance(i.ID()) != nil {
		return nil, nil, errAddingInstanceAlreadyExist
	}
	instances := placement.NewEmptyInstance(i.ID(), i.Rack(), i.Zone(), i.Weight())
	return placement.NewPlacement(append(p.Instances(), instances), p.Shards(), p.ReplicaFactor()), instances, nil
}

func removeInstanceFromPlacement(p services.ServicePlacement, leavingInstance services.PlacementInstance) (services.ServicePlacement, services.PlacementInstance, error) {
	leavingInstance = p.Instance(leavingInstance.ID())
	if leavingInstance == nil {
		return nil, nil, errInstanceAbsent
	}

	var instances []services.PlacementInstance
	for i, instance := range p.Instances() {
		if instance.ID() == leavingInstance.ID() {
			instances = append(p.Instances()[:i], p.Instances()[i+1:]...)
			break
		}
	}
	return placement.NewPlacement(instances, p.Shards(), p.ReplicaFactor()), leavingInstance, nil
}

func copyPlacement(p services.ServicePlacement) services.ServicePlacement {
	return placement.NewPlacement(copyInstances(p.Instances()), p.Shards(), p.ReplicaFactor())
}

func copyInstances(instances []services.PlacementInstance) []services.PlacementInstance {
	copied := make([]services.PlacementInstance, len(instances))
	for i, instance := range instances {
		copied[i] = placement.NewInstance().
			SetID(instance.ID()).
			SetRack(instance.Rack()).
			SetZone(instance.Zone()).
			SetWeight(instance.Weight()).
			SetEndpoint(instance.Endpoint()).
			SetShards(shard.NewShardsWithIDs(instance.Shards().ShardIDs()))
	}
	return copied
}
