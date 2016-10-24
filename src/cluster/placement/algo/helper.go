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

	"github.com/m3db/m3cluster/placement"
)

// PlacementHelper helps the algorithm to place shards
type PlacementHelper interface {
	// PlaceShards distributes shards to the hosts in the helper, with aware of where are the shards coming from
	PlaceShards(shards []uint32, from placement.HostShards) error
	// GeneratePlacement generates a placement snapshot
	GenerateSnapshot() placement.Snapshot
	// GetTargetLoadForHost returns the targe load for a host
	GetTargetLoadForHost(hostID string) int
	// MoveOneShard moves one shard between 2 hosts
	MoveOneShard(from, to placement.HostShards) bool
	// MoveShard moves a particular shard between 2 hosts
	MoveShard(shard uint32, from, to placement.HostShards) bool
	// HasNoRackConflict checks if the rack constraint is violated if the given shard is moved to the target rack
	HasNoRackConflict(shard uint32, from placement.HostShards, toRack string) bool
	// BuildHostHeap returns heap of HostShards sorted by available capacity
	BuildHostHeap(hostShards []placement.HostShards, availableCapacityAscending bool) heap.Interface
}

type placementHelper struct {
	targetLoad      map[string]int
	shardToHostMap  map[uint32]map[placement.HostShards]struct{}
	rackToHostsMap  map[string]map[placement.HostShards]struct{}
	rackToWeightMap map[string]uint32
	totalWeight     uint32
	rf              int
	uniqueShards    []uint32
	hostShards      []placement.HostShards
	options         placement.Options
}

func (ph *placementHelper) GetTargetLoadForHost(hostID string) int {
	return ph.targetLoad[hostID]
}

func (ph *placementHelper) MoveOneShard(from, to placement.HostShards) bool {
	if from == nil {
		return false
	}
	for _, shard := range from.Shards() {
		if ph.MoveShard(shard, from, to) {
			return true
		}
	}
	return false
}

func (ph *placementHelper) MoveShard(shard uint32, from, to placement.HostShards) bool {
	if ph.canAssignHost(shard, from, to) {
		ph.assignShardToHost(shard, to)
		ph.removeShardFromHost(shard, from)
		return true
	}
	return false
}

func (ph placementHelper) HasNoRackConflict(shard uint32, from placement.HostShards, toRack string) bool {
	if from != nil {
		if from.Host().Rack() == toRack {
			return true
		}
	}
	for host := range ph.shardToHostMap[shard] {
		if host.Host().Rack() == toRack {
			return false
		}
	}
	return true
}

func (ph *placementHelper) BuildHostHeap(hostShards []placement.HostShards, availableCapacityAscending bool) heap.Interface {
	return newHostHeap(hostShards, availableCapacityAscending, ph.targetLoad, ph.rackToWeightMap)
}

func (ph *placementHelper) GenerateSnapshot() placement.Snapshot {
	return placement.NewPlacementSnapshot(ph.hostShards, ph.uniqueShards, ph.rf)
}

func (ph placementHelper) PlaceShards(shards []uint32, from placement.HostShards) error {
	shardSet := placement.ConvertShardSliceToMap(shards)
	if from != nil {
		// prefer to distribute "some" of the load to other racks first
		// because the load from a leaving host can always get assigned to a host on the same rack
		ph.placeToRacksOtherThanOrigin(shardSet, from)
	}

	hostHeap := ph.BuildHostHeap(ph.hostShards, true)
	// if there are shards left to be assigned, distribute them evenly
	var triedHosts []placement.HostShards
	for shard := range shardSet {
		moved := false
		for hostHeap.Len() > 0 {
			tryHost := heap.Pop(hostHeap).(placement.HostShards)
			triedHosts = append(triedHosts, tryHost)
			if ph.MoveShard(shard, from, tryHost) {
				moved = true
				break
			}
		}
		if !moved {
			// this should only happen when RF > number of racks
			return errNotEnoughRacks
		}
		for _, triedHost := range triedHosts {
			heap.Push(hostHeap, triedHost)
		}
		triedHosts = triedHosts[:0]
	}
	return nil
}

// placeToRacksOtherThanOrigin move shards from a host to the rest of the cluster
// the goal of this function is to assign "some" of the shards to the hosts in other racks
func (ph placementHelper) placeToRacksOtherThanOrigin(shards map[uint32]int, from placement.HostShards) {
	var otherRack []placement.HostShards
	for rack, hostShards := range ph.rackToHostsMap {
		if rack == from.Host().Rack() {
			continue
		}
		for hostShard := range hostShards {
			otherRack = append(otherRack, hostShard)
		}
	}

	hostHeap := ph.BuildHostHeap(otherRack, true)

	var triedHosts []placement.HostShards
	for shard := range shards {
		for hostHeap.Len() > 0 {
			tryHost := heap.Pop(hostHeap).(placement.HostShards)
			if ph.GetTargetLoadForHost(tryHost.Host().ID())-tryHost.ShardsLen() <= 0 {
				// this is where "some" is, at this point the best host option in the cluster
				// from a different rack has reached its target load, time to break out of the loop
				return
			}
			triedHosts = append(triedHosts, tryHost)
			if ph.MoveShard(shard, from, tryHost) {
				delete(shards, shard)
				break
			}
		}

		for _, triedHost := range triedHosts {
			heap.Push(hostHeap, triedHost)
		}
		triedHosts = triedHosts[:0]
	}
}

// NewPlacementHelper returns a placement helper
func NewPlacementHelper(ps placement.Snapshot, opt placement.Options) PlacementHelper {
	return newHelper(ps, ps.Replicas(), opt)
}

func newInitHelper(hosts []placement.Host, ids []uint32, opt placement.Options) PlacementHelper {
	emptyPlacement := placement.NewEmptyPlacementSnapshot(hosts, ids)
	return newHelper(emptyPlacement, emptyPlacement.Replicas()+1, opt)
}

func newAddReplicaHelper(ps placement.Snapshot, opt placement.Options) PlacementHelper {
	return newHelper(ps, ps.Replicas()+1, opt)
}

func newAddHostShardsHelper(ps placement.Snapshot, hostShards placement.HostShards, opt placement.Options) PlacementHelper {
	ps = placement.NewPlacementSnapshot(append(ps.HostShards(), hostShards), ps.Shards(), ps.Replicas())
	return newHelper(ps, ps.Replicas(), opt)
}

func newRemoveHostHelper(ps placement.Snapshot, leavingHost placement.Host, opt placement.Options) (PlacementHelper, placement.HostShards, error) {
	ps, leavingHostShards, err := removeHostFromPlacement(ps, leavingHost)
	if err != nil {
		return nil, nil, err
	}
	return newHelper(ps, ps.Replicas(), opt), leavingHostShards, nil
}

func newReplaceHostHelper(
	ps placement.Snapshot,
	leavingHost placement.Host,
	addingHosts []placement.Host,
	opt placement.Options,
) (PlacementHelper, placement.HostShards, []placement.HostShards, error) {
	ps, leavingHostShards, err := removeHostFromPlacement(ps, leavingHost)
	if err != nil {
		return nil, nil, nil, err
	}

	addingHostShards := make([]placement.HostShards, len(addingHosts))
	for i, host := range addingHosts {
		ps, addingHostShards[i], err = addHostToPlacement(ps, host)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	return newHelper(ps, ps.Replicas(), opt), leavingHostShards, addingHostShards, nil
}

func newHelper(ps placement.Snapshot, targetRF int, opt placement.Options) PlacementHelper {
	ph := &placementHelper{
		rf:           targetRF,
		hostShards:   ps.HostShards(),
		uniqueShards: ps.Shards(),
		options:      opt,
	}

	ph.scanCurrentLoad()
	ph.buildTargetLoad()
	return ph
}

func (ph *placementHelper) scanCurrentLoad() {
	ph.shardToHostMap = make(map[uint32]map[placement.HostShards]struct{}, len(ph.uniqueShards))
	ph.rackToHostsMap = make(map[string]map[placement.HostShards]struct{})
	ph.rackToWeightMap = make(map[string]uint32)
	totalWeight := uint32(0)
	for _, h := range ph.hostShards {
		if _, exist := ph.rackToHostsMap[h.Host().Rack()]; !exist {
			ph.rackToHostsMap[h.Host().Rack()] = make(map[placement.HostShards]struct{})
		}
		ph.rackToHostsMap[h.Host().Rack()][h] = struct{}{}

		ph.rackToWeightMap[h.Host().Rack()] = ph.rackToWeightMap[h.Host().Rack()] + h.Host().Weight()
		totalWeight += h.Host().Weight()

		for _, shard := range h.Shards() {
			ph.assignShardToHost(shard, h)
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
	for _, host := range ph.hostShards {
		rackWeight := ph.rackToWeightMap[host.Host().Rack()]
		if isRackOverWeight(rackWeight, ph.totalWeight, ph.rf) {
			// if the host is on a over-sized rack, the target load is topped at shardLen / rackSize
			targetLoad[host.Host().ID()] = int(math.Ceil(float64(ph.getShardLen()) * float64(host.Host().Weight()) / float64(rackWeight)))
		} else {
			// if the host is on a normal rack, get the target load with aware of other over-sized rack
			targetLoad[host.Host().ID()] = ph.getShardLen() * (ph.rf - overWeightedRack) * int(host.Host().Weight()) / int(ph.totalWeight-overWeight)
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

func (ph placementHelper) canAssignHost(shard uint32, from, to placement.HostShards) bool {
	if to.ContainsShard(shard) {
		return false
	}
	return ph.options.LooseRackCheck() || ph.HasNoRackConflict(shard, from, to.Host().Rack())
}

func (ph placementHelper) assignShardToHost(shard uint32, to placement.HostShards) {
	if to == nil {
		return
	}

	to.AddShard(shard)

	if _, exist := ph.shardToHostMap[shard]; !exist {
		ph.shardToHostMap[shard] = make(map[placement.HostShards]struct{})
	}
	ph.shardToHostMap[shard][to] = struct{}{}
}

func (ph placementHelper) removeShardFromHost(shard uint32, from placement.HostShards) {
	if from == nil {
		return
	}

	from.RemoveShard(shard)
	delete(ph.shardToHostMap[shard], from)
}

// hostHeap provides an easy way to get best candidate host to assign/steal a shard
type hostHeap struct {
	hosts                 []placement.HostShards
	rackToWeightMap       map[string]uint32
	targetLoad            map[string]int
	hostCapacityAscending bool
}

func newHostHeap(hosts []placement.HostShards, hostCapacityAscending bool, targetLoad map[string]int, rackToWeightMap map[string]uint32) *hostHeap {
	hHeap := &hostHeap{hostCapacityAscending: hostCapacityAscending, hosts: hosts, targetLoad: targetLoad, rackToWeightMap: rackToWeightMap}
	heap.Init(hHeap)
	return hHeap
}

func (hh hostHeap) getTargetLoadForHost(hostID string) int {
	return hh.targetLoad[hostID]
}
func (hh hostHeap) Len() int {
	return len(hh.hosts)
}

func (hh hostHeap) Less(i, j int) bool {
	hostI := hh.hosts[i]
	hostJ := hh.hosts[j]
	leftLoadOnI := hh.getTargetLoadForHost(hostI.Host().ID()) - hostI.ShardsLen()
	leftLoadOnJ := hh.getTargetLoadForHost(hostJ.Host().ID()) - hostJ.ShardsLen()
	// if both host has tokens to be filled, prefer the one on a bigger rack
	// since it tends to be more picky in accepting shards
	if leftLoadOnI > 0 && leftLoadOnJ > 0 {
		if hostI.Host().Rack() != hostJ.Host().Rack() {
			return hh.rackToWeightMap[hostI.Host().Rack()] > hh.rackToWeightMap[hostJ.Host().Rack()]
		}
	}
	// compare left capacity on both hosts
	if hh.hostCapacityAscending {
		return leftLoadOnI > leftLoadOnJ
	}
	return leftLoadOnI < leftLoadOnJ
}

func (hh hostHeap) Swap(i, j int) {
	hh.hosts[i], hh.hosts[j] = hh.hosts[j], hh.hosts[i]
}

func (hh *hostHeap) Push(h interface{}) {
	host := h.(placement.HostShards)
	hh.hosts = append(hh.hosts, host)
}

func (hh *hostHeap) Pop() interface{} {
	n := len(hh.hosts)
	host := hh.hosts[n-1]
	hh.hosts = hh.hosts[0 : n-1]
	return host
}

func addHostToPlacement(ps placement.Snapshot, addingHost placement.Host) (placement.Snapshot, placement.HostShards, error) {
	if ps.HostShard(addingHost.ID()) != nil {
		return nil, nil, errAddingHostAlreadyExist
	}
	hss := placement.NewHostShards(addingHost)
	return placement.NewPlacementSnapshot(append(ps.HostShards(), hss), ps.Shards(), ps.Replicas()), hss, nil
}

func removeHostFromPlacement(ps placement.Snapshot, leavingHost placement.Host) (placement.Snapshot, placement.HostShards, error) {
	leavingHostShards := ps.HostShard(leavingHost.ID())
	if leavingHostShards == nil {
		return nil, nil, errHostAbsent
	}

	var hsArr []placement.HostShards
	for i, phs := range ps.HostShards() {
		if phs.Host().ID() == leavingHost.ID() {
			hsArr = append(ps.HostShards()[:i], ps.HostShards()[i+1:]...)
			break
		}
	}
	return placement.NewPlacementSnapshot(hsArr, ps.Shards(), ps.Replicas()), leavingHostShards, nil
}
