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

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
)

var (
	errNotEnoughRacks             = errors.New("not enough racks to take shards, please make sure RF is less than number of racks")
	errInstanceAbsent             = errors.New("could not remove or replace a instance that does not exist")
	errAddingInstanceAlreadyExist = errors.New("the adding instance is already in the placement")
	errCouldNotReachTargetLoad    = errors.New("new instance could not reach target load")
)

type rackAwarePlacementAlgorithm struct {
	opts services.PlacementOptions
}

// NewRackAwarePlacementAlgorithm returns a rack aware placement algorithm
func NewRackAwarePlacementAlgorithm(opts services.PlacementOptions) placement.Algorithm {
	return rackAwarePlacementAlgorithm{opts: opts}
}

func (a rackAwarePlacementAlgorithm) InitialPlacement(instances []services.PlacementInstance, shards []uint32) (services.ServicePlacement, error) {
	ph := newInitHelper(instances, shards, a.opts)

	if err := ph.PlaceShards(shards, nil); err != nil {
		return nil, err
	}
	return ph.GeneratePlacement(), nil
}

func (a rackAwarePlacementAlgorithm) AddReplica(p services.ServicePlacement) (services.ServicePlacement, error) {
	p = copyPlacement(p)
	ph := newAddReplicaHelper(p, a.opts)
	if err := ph.PlaceShards(p.Shards(), nil); err != nil {
		return nil, err
	}
	return ph.GeneratePlacement(), nil
}

func (a rackAwarePlacementAlgorithm) RemoveInstance(p services.ServicePlacement, i services.PlacementInstance) (services.ServicePlacement, error) {
	p = copyPlacement(p)
	ph, leavingInstance, err := newRemoveInstanceHelper(p, i, a.opts)
	if err != nil {
		return nil, err
	}
	// place the shards from the leaving instance to the rest of the cluster
	if err := ph.PlaceShards(leavingInstance.Shards().ShardIDs(), leavingInstance); err != nil {
		return nil, err
	}
	return ph.GeneratePlacement(), nil
}

func (a rackAwarePlacementAlgorithm) AddInstance(p services.ServicePlacement, i services.PlacementInstance) (services.ServicePlacement, error) {
	p = copyPlacement(p)
	return a.addInstance(p, placement.NewEmptyInstance(i.ID(), i.Rack(), i.Zone(), i.Weight()))
}

func (a rackAwarePlacementAlgorithm) ReplaceInstance(p services.ServicePlacement, leavingInstance services.PlacementInstance, addingInstances []services.PlacementInstance) (services.ServicePlacement, error) {
	p = copyPlacement(p)
	ph, leavingInstance, addingInstances, err := newReplaceInstanceHelper(p, leavingInstance, addingInstances, a.opts)
	if err != nil {
		return nil, err
	}

	// move shards from leaving instance to adding instance
	instanceHeap := ph.BuildInstanceHeap(addingInstances, true)
	for leavingInstance.Shards().NumShards() > 0 {
		if instanceHeap.Len() == 0 {
			break
		}
		tryInstance := heap.Pop(instanceHeap).(services.PlacementInstance)
		if moved := ph.MoveOneShard(leavingInstance, tryInstance); moved {
			heap.Push(instanceHeap, tryInstance)
		}
	}

	if !a.opts.AllowPartialReplace() {
		if leavingInstance.Shards().NumShards() > 0 {
			return nil, fmt.Errorf("could not fully replace all shards from %s, %v shards left unassigned", leavingInstance.ID(), leavingInstance.Shards().NumShards())
		}
		return ph.GeneratePlacement(), nil
	}

	if leavingInstance.Shards().NumShards() == 0 {
		return ph.GeneratePlacement(), nil
	}
	// place the shards from the leaving instance to the rest of the cluster
	if err := ph.PlaceShards(leavingInstance.Shards().ShardIDs(), leavingInstance); err != nil {
		return nil, err
	}
	// fill up to the target load for added instances if have not already done so
	newP := ph.GeneratePlacement()
	for _, addingInstance := range addingInstances {
		newP, leavingInstance, err = removeInstanceFromPlacement(newP, addingInstance)
		if err != nil {
			return nil, err
		}
		newP, err = a.addInstance(newP, leavingInstance)
		if err != nil {
			return nil, err
		}
	}

	return newP, nil
}

func (a rackAwarePlacementAlgorithm) addInstance(p services.ServicePlacement, addingInstance services.PlacementInstance) (services.ServicePlacement, error) {
	if p.Instance(addingInstance.ID()) != nil {
		return nil, errAddingInstanceAlreadyExist
	}
	ph := newAddInstanceHelper(p, addingInstance, a.opts)
	targetLoad := ph.TargetLoadForInstance(addingInstance.ID())
	// try to take shards from the most loaded instances until the adding instance reaches target load
	hh := ph.BuildInstanceHeap(p.Instances(), false)
	for addingInstance.Shards().NumShards() < targetLoad {
		if hh.Len() == 0 {
			return nil, errCouldNotReachTargetLoad
		}
		tryInstance := heap.Pop(hh).(services.PlacementInstance)
		if moved := ph.MoveOneShard(tryInstance, addingInstance); moved {
			heap.Push(hh, tryInstance)
		}
	}

	return ph.GeneratePlacement(), nil
}
