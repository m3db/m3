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
	"errors"
	"fmt"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
)

var (
	errNotEnoughRacks                   = errors.New("not enough racks to take shards, please make sure RF is less than number of racks")
	errAddingInstanceAlreadyExist       = errors.New("the adding instance is already in the placement")
	errCouldNotReachTargetLoad          = errors.New("new instance could not reach target load")
	errShardedAlgoOnNotShardedPlacement = errors.New("could not apply sharded algo on non-sharded placement")
)

type rackAwarePlacementAlgorithm struct {
	opts services.PlacementOptions
}

func newShardedAlgorithm(opts services.PlacementOptions) placement.Algorithm {
	return rackAwarePlacementAlgorithm{opts: opts}
}

func (a rackAwarePlacementAlgorithm) InitialPlacement(instances []services.PlacementInstance, shards []uint32) (services.ServicePlacement, error) {
	ph := newInitHelper(cloneInstances(instances), shards, a.opts)

	if err := ph.PlaceShards(newShards(shards), nil, ph.Instances()); err != nil {
		return nil, err
	}
	return ph.GeneratePlacement(nonEmptyOnly), nil
}

func (a rackAwarePlacementAlgorithm) AddReplica(p services.ServicePlacement) (services.ServicePlacement, error) {
	if !p.IsSharded() {
		return nil, errShardedAlgoOnNotShardedPlacement
	}

	p = clonePlacement(p)
	ph := newAddReplicaHelper(p, a.opts)
	if err := ph.PlaceShards(newShards(p.Shards()), nil, ph.Instances()); err != nil {
		return nil, err
	}

	ph.Optimize(safe)

	return ph.GeneratePlacement(nonEmptyOnly), nil
}

func (a rackAwarePlacementAlgorithm) RemoveInstance(p services.ServicePlacement, instanceID string) (services.ServicePlacement, error) {
	if !p.IsSharded() {
		return nil, errShardedAlgoOnNotShardedPlacement
	}

	p = clonePlacement(p)
	ph, leavingInstance, err := newRemoveInstanceHelper(p, instanceID, a.opts)
	if err != nil {
		return nil, err
	}
	// place the shards from the leaving instance to the rest of the cluster
	if err := ph.PlaceShards(leavingInstance.Shards().All(), leavingInstance, ph.Instances()); err != nil {
		return nil, err
	}

	result, _, err := addInstanceToPlacement(ph.GeneratePlacement(nonEmptyOnly), leavingInstance, false)
	return result, err
}

func (a rackAwarePlacementAlgorithm) AddInstance(
	p services.ServicePlacement,
	addingInstance services.PlacementInstance,
) (services.ServicePlacement, error) {
	if !p.IsSharded() {
		return nil, errShardedAlgoOnNotShardedPlacement
	}

	p, addingInstance = clonePlacement(p), cloneInstance(addingInstance)
	instance, exist := p.Instance(addingInstance.ID())
	if exist {
		if !placement.IsInstanceLeaving(instance) {
			return nil, errAddingInstanceAlreadyExist
		}
		addingInstance = instance
	}

	ph := newAddInstanceHelper(p, addingInstance, a.opts)

	ph.AddInstance(addingInstance)

	return ph.GeneratePlacement(nonEmptyOnly), nil
}

func (a rackAwarePlacementAlgorithm) ReplaceInstance(
	p services.ServicePlacement,
	instanceID string,
	addingInstances []services.PlacementInstance,
) (services.ServicePlacement, error) {
	if !p.IsSharded() {
		return nil, errShardedAlgoOnNotShardedPlacement
	}

	p = clonePlacement(p)
	ph, leavingInstance, addingInstances, err := newReplaceInstanceHelper(p, instanceID, addingInstances, a.opts)
	if err != nil {
		return nil, err
	}

	err = ph.PlaceShards(leavingInstance.Shards().All(), leavingInstance, addingInstances)
	if err != nil && err != errNotEnoughRacks {
		return nil, err
	}

	if loadOnInstance(leavingInstance) == 0 {
		result, _, err := addInstanceToPlacement(ph.GeneratePlacement(nonEmptyOnly), leavingInstance, false)
		return result, err
	}

	if !a.opts.AllowPartialReplace() {
		return nil, fmt.Errorf("could not fully replace all shards from %s, %v shards left unassigned", leavingInstance.ID(), leavingInstance.Shards().NumShards())
	}

	// place the shards from the leaving instance to the rest of the cluster
	if err := ph.PlaceShards(
		leavingInstance.Shards().All(),
		leavingInstance,
		ph.Instances(),
	); err != nil {
		return nil, err
	}

	ph.Optimize(unsafe)

	result, _, err := addInstanceToPlacement(ph.GeneratePlacement(includeEmpty), leavingInstance, false)
	return result, err
}
