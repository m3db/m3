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
	// GetHostHeap returns a host heap that sort the hosts based on their capacity
	GetHostHeap() heap.Interface
}

type placementHelper struct {
	hostHeap       heap.Interface
	targetLoad     map[string]int
	shardToHostMap map[uint32]map[placement.HostShards]struct{}
	rackToHostsMap map[string]map[placement.HostShards]struct{}
	rf             int
	uniqueShards   []uint32
	hostShards     []placement.HostShards
}

func (ph *placementHelper) GetTargetLoadForHost(hostID string) int {
	return ph.targetLoad[hostID]
}

func (ph *placementHelper) MoveOneShard(from, to placement.HostShards) bool {
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

func (ph placementHelper) PlaceShards(shards []uint32, from placement.HostShards) error {
	shardSet := placement.ConvertShardSliceToSet(shards)
	var tried []placement.HostShards

	if from != nil {
		// prefer to distribute "some" of the load to other racks first
		// because the load from a leaving host can always get assigned to a host on the same rack
		ph.placeToRacksOtherThanOrigin(shardSet, from)
	}

	// if there are shards left to be assigned, distribute them evenly
	for shard := range shardSet {
		tried = tried[:0]
		for ph.hostHeap.Len() > 0 {
			tryHost := heap.Pop(ph.hostHeap).(placement.HostShards)
			tried = append(tried, tryHost)
			if ph.canAssignHost(shard, from, tryHost) {
				ph.assignShardToHost(shard, tryHost)
				for _, triedHost := range tried {
					heap.Push(ph.hostHeap, triedHost)
				}
				break
			}
		}
		if ph.hostHeap.Len() == 0 {
			// this should only happen when RF > number of racks
			return errNotEnoughRacks
		}
	}
	return nil
}

func (ph *placementHelper) GetHostHeap() heap.Interface {
	return ph.hostHeap
}

func (ph *placementHelper) GenerateSnapshot() placement.Snapshot {
	return placement.NewPlacementSnapshot(ph.hostShards, ph.uniqueShards, ph.rf)
}

func newInitPlacementHelper(hosts []placement.Host, ids []uint32) PlacementHelper {
	emptyPlacement := placement.NewEmptyPlacementSnapshot(hosts, ids)
	return newPlaceShardingHelper(emptyPlacement, emptyPlacement.Replicas()+1, true)
}

func newReplicaPlacementHelper(s placement.Snapshot, targetRF int) PlacementHelper {
	return newPlaceShardingHelper(s, targetRF, true)
}

func newAddHostShardsPlacementHelper(s placement.Snapshot, hs placement.HostShards) (PlacementHelper, error) {
	var hss []placement.HostShards

	if s.HostShard(hs.Host().ID()) != nil {
		return nil, errHostAlreadyExist
	}

	for _, phs := range s.HostShards() {
		hss = append(hss, phs)
	}

	hss = append(hss, hs)

	ps := placement.NewPlacementSnapshot(hss, s.Shards(), s.Replicas())
	return newPlaceShardingHelper(ps, s.Replicas(), false), nil
}

func newRemoveHostPlacementHelper(s placement.Snapshot, leavingHost placement.Host) (PlacementHelper, placement.HostShards, error) {
	if s.HostShard(leavingHost.ID()) == nil {
		return nil, nil, errHostAbsent
	}
	var leavingHostShards placement.HostShards
	var hosts []placement.HostShards
	for _, phs := range s.HostShards() {
		if phs.Host().ID() == leavingHost.ID() {
			leavingHostShards = phs
			continue
		}
		hosts = append(hosts, phs)
	}
	ps := placement.NewPlacementSnapshot(hosts, s.Shards(), s.Replicas())
	return newPlaceShardingHelper(ps, s.Replicas(), true), leavingHostShards, nil
}

func newPlaceShardingHelper(ps placement.Snapshot, targetRF int, hostCapacityAscending bool) PlacementHelper {
	ph := &placementHelper{
		shardToHostMap: make(map[uint32]map[placement.HostShards]struct{}),
		rackToHostsMap: make(map[string]map[placement.HostShards]struct{}),
		rf:             targetRF,
		hostShards:     ps.HostShards(),
		uniqueShards:   ps.Shards(),
	}

	// build rackToHost map
	ph.buildRackToHostMap()

	ph.buildHostHeap(hostCapacityAscending)
	return ph
}

func (ph *placementHelper) buildRackToHostMap() {
	for _, h := range ph.hostShards {
		if _, exist := ph.rackToHostsMap[h.Host().Rack()]; !exist {
			ph.rackToHostsMap[h.Host().Rack()] = make(map[placement.HostShards]struct{})
		}
		ph.rackToHostsMap[h.Host().Rack()][h] = struct{}{}
		for _, shard := range h.Shards() {
			ph.assignShardToHost(shard, h)
		}
	}
}

func (ph *placementHelper) buildHostHeap(hostCapacityAscending bool) {
	overSizedRack := 0
	overSizedHosts := 0
	rackSizeMap := make(map[string]int)
	for rack := range ph.rackToHostsMap {
		rackHostNumber := len(ph.rackToHostsMap[rack])
		if float64(rackHostNumber)/float64(ph.getHostLen()) >= 1.0/float64(ph.rf) {
			overSizedRack++
			overSizedHosts += rackHostNumber
		}
		rackSizeMap[rack] = rackHostNumber
	}

	targetLoadMap := ph.buildTargetLoadMap(rackSizeMap, overSizedRack, overSizedHosts)
	ph.targetLoad = targetLoadMap
	ph.hostHeap = newHostHeap(ph.hostShards, hostCapacityAscending, targetLoadMap, ph.rackToHostsMap)
}

func (ph *placementHelper) buildTargetLoadMap(rackSizeMap map[string]int, overSizedRackLen, overSizedHostLen int) map[string]int {
	targetLoad := make(map[string]int)
	for _, host := range ph.hostShards {
		rackSize := rackSizeMap[host.Host().Rack()]
		if float64(rackSize)/float64(ph.getHostLen()) >= 1.0/float64(ph.rf) {
			// if the host is on a over-sized rack, the target load is topped at shardLen / rackSize
			targetLoad[host.Host().ID()] = int(math.Ceil(float64(ph.getShardLen()) / float64(rackSize)))
		} else {
			// if the host is on a normal rack, get the target load with aware of other over-sized rack
			targetLoad[host.Host().ID()] = ph.getShardLen() * (ph.rf - overSizedRackLen) / (ph.getHostLen() - overSizedHostLen)
		}
	}
	return targetLoad
}

func (ph placementHelper) getHostLen() int {
	return len(ph.hostShards)
}

func (ph placementHelper) getShardLen() int {
	return len(ph.uniqueShards)
}

func (ph placementHelper) canAssignRack(shard uint32, from placement.HostShards, toRack string) bool {
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

func (ph placementHelper) canAssignHost(shard uint32, from, to placement.HostShards) bool {
	if to.ContainsShard(shard) {
		return false
	}
	return ph.canAssignRack(shard, from, to.Host().Rack())
}

func (ph placementHelper) assignShardToHost(shard uint32, to placement.HostShards) {
	to.AddShard(shard)

	if _, exist := ph.shardToHostMap[shard]; !exist {
		ph.shardToHostMap[shard] = make(map[placement.HostShards]struct{})
	}
	ph.shardToHostMap[shard][to] = struct{}{}
}

func (ph placementHelper) removeShardFromHost(shard uint32, from placement.HostShards) {
	from.RemoveShard(shard)

	delete(ph.shardToHostMap[shard], from)
}

// placeToRacksOtherThanOrigin move shards from a host to the rest of the cluster
// the goal of this function is to assign "some" of the shards to the hosts in other racks
func (ph placementHelper) placeToRacksOtherThanOrigin(shards map[uint32]struct{}, from placement.HostShards) {
	var sameRack []placement.HostShards
	var triedHosts []placement.HostShards
	for ph.hostHeap.Len() > 0 {
		tryHost := heap.Pop(ph.hostHeap).(placement.HostShards)
		if from != nil && tryHost.Host().Rack() == from.Host().Rack() {
			// do not place to same rack for now
			sameRack = append(sameRack, tryHost)
		} else {
			triedHosts = append(triedHosts, tryHost)
		}
	}
	for _, h := range triedHosts {
		heap.Push(ph.hostHeap, h)
	}

	triedHosts = triedHosts[:0]

outer:
	for shard := range shards {
		for ph.hostHeap.Len() > 0 {
			tryHost := heap.Pop(ph.hostHeap).(placement.HostShards)
			triedHosts = append(triedHosts, tryHost)
			if ph.GetTargetLoadForHost(tryHost.Host().ID())-tryHost.ShardsLen() <= 0 {
				// this is where "some" is, at this point the best host option in the cluster
				// from a different rack has reached its target load, time to break out of the loop
				break outer
			}
			if ph.canAssignHost(shard, from, tryHost) {
				ph.assignShardToHost(shard, tryHost)
				delete(shards, shard)
				break
			}
		}

		for _, triedHost := range triedHosts {
			heap.Push(ph.hostHeap, triedHost)
		}
		triedHosts = triedHosts[:0]
	}

	for _, host := range sameRack {
		heap.Push(ph.hostHeap, host)
	}
	for _, triedHost := range triedHosts {
		heap.Push(ph.hostHeap, triedHost)
	}
}

// hostHeap provides an easy way to get best candidate host to assign/steal a shard
type hostHeap struct {
	hosts                 []placement.HostShards
	rackToHostsMap        map[string]map[placement.HostShards]struct{}
	targetLoad            map[string]int
	hostCapacityAscending bool
}

func newHostHeap(hosts []placement.HostShards, hostCapacityAscending bool, targetLoad map[string]int, rackToHostMap map[string]map[placement.HostShards]struct{}) *hostHeap {
	hHeap := &hostHeap{hostCapacityAscending: hostCapacityAscending, hosts: hosts, targetLoad: targetLoad, rackToHostsMap: rackToHostMap}
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
			return len(hh.rackToHostsMap[hostI.Host().Rack()]) > len(hh.rackToHostsMap[hostJ.Host().Rack()])
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
