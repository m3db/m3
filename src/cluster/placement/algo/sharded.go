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

	"github.com/m3db/m3cluster/placement"
)

var (
	errNotEnoughRacks              = errors.New("not enough racks to take shards, please make sure RF is less than number of racks")
	errAddingInstanceAlreadyExist  = errors.New("the adding instance is already in the placement")
	errIncompatibleWithShardedAlgo = errors.New("could not apply sharded algo on the placement")
)

type rackAwarePlacementAlgorithm struct {
	opts placement.Options
}

func newShardedAlgorithm(opts placement.Options) placement.Algorithm {
	return rackAwarePlacementAlgorithm{opts: opts}
}

func (a rackAwarePlacementAlgorithm) IsCompatibleWith(p placement.Placement) error {
	if !p.IsSharded() {
		return errIncompatibleWithShardedAlgo
	}

	return nil
}

func (a rackAwarePlacementAlgorithm) InitialPlacement(
	instances []placement.Instance,
	shards []uint32,
	rf int,
) (placement.Placement, error) {
	ph := newInitHelper(placement.CloneInstances(instances), shards, a.opts)

	if err := ph.PlaceShards(newShards(shards), nil, ph.Instances()); err != nil {
		return nil, err
	}
	p := ph.GeneratePlacement()
	for i := 1; i < rf; i++ {
		ph := newAddReplicaHelper(p, a.opts)
		if err := ph.PlaceShards(newShards(p.Shards()), nil, ph.Instances()); err != nil {
			return nil, err
		}
		if err := ph.Optimize(safe); err != nil {
			return nil, err
		}
		p = ph.GeneratePlacement()
	}
	return p, nil
}

func (a rackAwarePlacementAlgorithm) AddReplica(p placement.Placement) (placement.Placement, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}

	p = placement.ClonePlacement(p)
	ph := newAddReplicaHelper(p, a.opts)
	if err := ph.PlaceShards(newShards(p.Shards()), nil, nonLeavingInstances(ph.Instances())); err != nil {
		return nil, err
	}

	if err := ph.Optimize(safe); err != nil {
		return nil, err
	}

	return ph.GeneratePlacement(), nil
}

func (a rackAwarePlacementAlgorithm) RemoveInstances(
	p placement.Placement,
	instanceIDs []string,
) (placement.Placement, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}

	p = placement.ClonePlacement(p)
	for _, instanceID := range instanceIDs {
		ph, leavingInstance, err := newRemoveInstanceHelper(p, instanceID, a.opts)
		if err != nil {
			return nil, err
		}
		// place the shards from the leaving instance to the rest of the cluster
		if err := ph.PlaceShards(leavingInstance.Shards().All(), leavingInstance, ph.Instances()); err != nil {
			return nil, err
		}

		if err := ph.Optimize(safe); err != nil {
			return nil, err
		}

		if p, _, err = addInstanceToPlacement(ph.GeneratePlacement(), leavingInstance, nonEmptyOnly); err != nil {
			return nil, err
		}
	}
	return p, nil
}

func (a rackAwarePlacementAlgorithm) AddInstances(
	p placement.Placement,
	instances []placement.Instance,
) (placement.Placement, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}

	p = placement.ClonePlacement(p)
	for _, instance := range instances {
		ph, addingInstance, err := newAddInstanceHelper(p, instance, a.opts)
		if err != nil {
			return nil, err
		}

		if err := ph.AddInstance(addingInstance); err != nil {
			return nil, err
		}

		p = ph.GeneratePlacement()
	}

	return p, nil
}

func (a rackAwarePlacementAlgorithm) ReplaceInstances(
	p placement.Placement,
	leavingInstanceIDs []string,
	addingInstances []placement.Instance,
) (placement.Placement, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}

	p = placement.ClonePlacement(p)
	ph, leavingInstances, addingInstances, err := newReplaceInstanceHelper(p, leavingInstanceIDs, addingInstances, a.opts)
	if err != nil {
		return nil, err
	}

	for _, leavingInstance := range leavingInstances {
		err = ph.PlaceShards(leavingInstance.Shards().All(), leavingInstance, addingInstances)
		if err != nil && err != errNotEnoughRacks {
			// errNotEnoughRacks means the adding instances do not have enough racks to take all the shards,
			// but the rest instances might have more racks to take all the shards.
			return nil, err
		}
		load := loadOnInstance(leavingInstance)
		if load == 0 {
			result, _, err := addInstanceToPlacement(ph.GeneratePlacement(), leavingInstance, nonEmptyOnly)
			return result, err
		}
		if !a.opts.AllowPartialReplace() {
			return nil, fmt.Errorf("could not fully replace all shards from %s, %d shards left unassigned",
				leavingInstance.ID(), load)
		}
	}

	if a.opts.AllowPartialReplace() {
		// Place the shards left on the leaving instance to the rest of the cluster.
		for _, leavingInstance := range leavingInstances {
			if err = ph.PlaceShards(leavingInstance.Shards().All(), leavingInstance, ph.Instances()); err != nil {
				return nil, err
			}
		}

		if err := ph.Optimize(unsafe); err != nil {
			return nil, err
		}
	}

	for _, leavingInstance := range leavingInstances {
		p, _, err = addInstanceToPlacement(ph.GeneratePlacement(), leavingInstance, nonEmptyOnly)
		if err != nil {
			return nil, err
		}
	}
	return p, nil
}
