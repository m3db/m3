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

	"github.com/m3db/m3cluster/placement"
)

var (
	errNotEnoughRacks          = errors.New("not enough racks to take shards, please make sure RF <= number of racks")
	errHostAbsent              = errors.New("could not remove or replace a host that does not exist")
	errHostAlreadyExist        = errors.New("the adding host is already in the placement")
	errCouldNotReachTargetLoad = errors.New("new host could not reach target load")
)

type rackAwarePlacementAlgorithm struct {
}

// NewRackAwarePlacementAlgorithm returns a rack aware placement algorithm
func NewRackAwarePlacementAlgorithm() placement.Algorithm {
	return rackAwarePlacementAlgorithm{}
}

func (a rackAwarePlacementAlgorithm) BuildInitialPlacement(hosts []placement.Host, shards []uint32) (placement.Snapshot, error) {
	ph := newInitPlacementHelper(hosts, shards)

	if err := ph.PlaceShards(shards, nil); err != nil {
		return nil, err
	}
	return ph.GenerateSnapshot(), nil
}

func (a rackAwarePlacementAlgorithm) AddReplica(ps placement.Snapshot) (placement.Snapshot, error) {
	ph := newReplicaPlacementHelper(ps, ps.Replicas()+1)
	if err := ph.PlaceShards(ps.Shards(), nil); err != nil {
		return nil, err
	}
	return ph.GenerateSnapshot(), nil
}

func (a rackAwarePlacementAlgorithm) RemoveHost(ps placement.Snapshot, leavingHost placement.Host) (placement.Snapshot, error) {
	var ph PlacementHelper
	var leavingHostShards placement.HostShards
	var err error
	if ph, leavingHostShards, err = newRemoveHostPlacementHelper(ps, leavingHost); err != nil {
		return nil, err
	}
	// place the shards from the leaving host to the rest of the cluster
	if err := ph.PlaceShards(leavingHostShards.Shards(), leavingHostShards); err != nil {
		return nil, err
	}
	return ph.GenerateSnapshot(), nil
}

func (a rackAwarePlacementAlgorithm) AddHost(ps placement.Snapshot, addingHost placement.Host) (placement.Snapshot, error) {
	addingHostShards := placement.NewEmptyHostShardsFromHost(addingHost)
	return a.addHostShards(ps, addingHostShards)
}

func (a rackAwarePlacementAlgorithm) ReplaceHost(ps placement.Snapshot, leavingHost, addingHost placement.Host) (placement.Snapshot, error) {
	var ph PlacementHelper
	var leavingHostShards placement.HostShards
	var err error
	if ph, leavingHostShards, err = newRemoveHostPlacementHelper(ps, leavingHost); err != nil {
		return nil, err
	}
	addingHostShards := placement.NewEmptyHostShardsFromHost(addingHost)
	var shardsUnassigned []uint32
	// move shards from leaving host to adding host
	for _, shard := range leavingHostShards.Shards() {
		if moved := ph.MoveShard(shard, leavingHostShards, addingHostShards); !moved {
			shardsUnassigned = append(shardsUnassigned, shard)
		}
	}

	// if there are shards that can not be moved to adding host
	// distribute them to the cluster
	if err := ph.PlaceShards(shardsUnassigned, leavingHostShards); err != nil {
		return nil, err
	}

	// add the adding host to the cluster and bring its load up to target load
	cl := ph.GenerateSnapshot()

	return a.addHostShards(cl, addingHostShards)
}

func (a rackAwarePlacementAlgorithm) addHostShards(ps placement.Snapshot, addingHostShard placement.HostShards) (placement.Snapshot, error) {
	var ph PlacementHelper
	var err error
	if ph, err = newAddHostShardsPlacementHelper(ps, addingHostShard); err != nil {
		return nil, err
	}
	targetLoad := ph.GetTargetLoadForHost(addingHostShard.Host().ID())
	// try to steal shards from the most loaded hosts until the adding host reaches target load
	hh := ph.GetHostHeap()
	for addingHostShard.ShardsLen() < targetLoad {
		if hh.Len() == 0 {
			return nil, errCouldNotReachTargetLoad
		}
		tryHost := heap.Pop(hh).(placement.HostShards)
		if moved := ph.MoveOneShard(tryHost, addingHostShard); moved {
			heap.Push(hh, tryHost)
		}
	}

	return ph.GenerateSnapshot(), nil
}
