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
	errShardsOnNonShardedAlgo         = errors.New("could not apply shards in non-sharded placement")
	errInCompatibleWithNonShardedAlgo = errors.New("could not apply non-sharded algo on the placement")
)

type nonShardedAlgorithm struct{}

func newNonShardedAlgorithm() placement.Algorithm {
	return nonShardedAlgorithm{}
}

func (a nonShardedAlgorithm) IsCompatibleWith(p services.Placement) error {
	if p.IsSharded() {
		return errInCompatibleWithNonShardedAlgo
	}

	if p.IsMirrored() {
		return errInCompatibleWithNonShardedAlgo
	}

	return nil
}

func (a nonShardedAlgorithm) InitialPlacement(
	instances []services.PlacementInstance,
	shards []uint32,
	rf int,
) (services.Placement, error) {
	if len(shards) > 0 {
		return nil, errShardsOnNonShardedAlgo
	}

	return placement.NewPlacement().
		SetInstances(placement.CloneInstances(instances)).
		SetShards(shards).
		SetReplicaFactor(rf).
		SetIsSharded(false), nil
}

func (a nonShardedAlgorithm) AddReplica(p services.Placement) (services.Placement, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}

	return placement.ClonePlacement(p).SetReplicaFactor(p.ReplicaFactor() + 1), nil
}

func (a nonShardedAlgorithm) RemoveInstances(
	p services.Placement,
	instanceIDs []string,
) (services.Placement, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}

	removingInstances := make([]services.PlacementInstance, len(instanceIDs))
	for i, id := range instanceIDs {
		instance, ok := p.Instance(id)
		if !ok {
			return nil, fmt.Errorf("instance %s not found in placement", id)
		}
		removingInstances[i] = instance
	}

	p = placement.ClonePlacement(p)
	instances := p.Instances()
	for _, instance := range removingInstances {
		instances = placement.RemoveInstanceFromList(instances, instance.ID())
	}
	return p.SetInstances(instances), nil
}

func (a nonShardedAlgorithm) AddInstances(
	p services.Placement,
	addingInstances []services.PlacementInstance,
) (services.Placement, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}

	p = placement.ClonePlacement(p)
	instances := p.Instances()
	for _, instance := range addingInstances {
		if _, ok := p.Instance(instance.ID()); ok {
			return nil, fmt.Errorf("instance %s already exist in placement", instance.ID())
		}
		instances = append(instances, instance)
	}

	return p.SetInstances(instances), nil
}

func (a nonShardedAlgorithm) ReplaceInstances(
	p services.Placement,
	leavingInstanceIDs []string,
	addingInstances []services.PlacementInstance,
) (services.Placement, error) {
	if err := a.IsCompatibleWith(p); err != nil {
		return nil, err
	}

	p, err := a.AddInstances(p, addingInstances)
	if err != nil {
		return nil, err
	}

	return a.RemoveInstances(p, leavingInstanceIDs)
}
