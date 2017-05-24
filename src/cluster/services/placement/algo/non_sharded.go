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
	errShardsOnNonShardedAlgo           = errors.New("could not apply shards in non-sharded placement")
	errNonShardedAlgoOnShardedPlacement = errors.New("could not apply non-sharded algo on sharded placement")
)

type nonShardedAlgorithm struct{}

func newNonShardedAlgorithm() placement.Algorithm {
	return nonShardedAlgorithm{}
}

func (a nonShardedAlgorithm) InitialPlacement(
	instances []services.PlacementInstance,
	shards []uint32,
) (services.Placement, error) {
	if len(shards) > 0 {
		return nil, errShardsOnNonShardedAlgo
	}

	return placement.NewPlacement().
		SetInstances(placement.CloneInstances(instances)).
		SetShards(shards).
		SetReplicaFactor(1).
		SetIsSharded(false), nil
}

func (a nonShardedAlgorithm) AddReplica(p services.Placement) (services.Placement, error) {
	if p.IsSharded() {
		return nil, errNonShardedAlgoOnShardedPlacement
	}
	p = placement.ClonePlacement(p)
	return placement.NewPlacement().
		SetInstances(p.Instances()).
		SetShards(p.Shards()).
		SetReplicaFactor(p.ReplicaFactor() + 1).
		SetIsSharded(p.IsSharded()), nil
}

func (a nonShardedAlgorithm) RemoveInstance(p services.Placement, instanceID string) (services.Placement, error) {
	if p.IsSharded() {
		return nil, errNonShardedAlgoOnShardedPlacement
	}
	p = placement.ClonePlacement(p)
	instances := p.Instances()
	for i, instance := range instances {
		if instance.ID() == instanceID {
			last := len(instances) - 1
			instances[i], instances[last] = instances[last], instances[i]
			return placement.NewPlacement().
				SetInstances(instances[:last]).
				SetShards(p.Shards()).
				SetReplicaFactor(p.ReplicaFactor()).
				SetIsSharded(p.IsSharded()), nil
		}
	}
	return nil, fmt.Errorf("instance %s does not exist in placement", instanceID)
}

func (a nonShardedAlgorithm) AddInstance(
	p services.Placement,
	i services.PlacementInstance,
) (services.Placement, error) {
	if p.IsSharded() {
		return nil, errNonShardedAlgoOnShardedPlacement
	}
	p = placement.ClonePlacement(p)
	instances := p.Instances()
	for _, instance := range instances {
		if instance.ID() == i.ID() {
			return nil, fmt.Errorf("instance %s already exist in placement", i.ID())
		}
	}
	return placement.NewPlacement().
		SetInstances(append(instances, i)).
		SetShards(p.Shards()).
		SetReplicaFactor(p.ReplicaFactor()).
		SetIsSharded(p.IsSharded()), nil
}

func (a nonShardedAlgorithm) ReplaceInstance(
	p services.Placement,
	instanceID string,
	addingInstances []services.PlacementInstance,
) (services.Placement, error) {
	var err error
	for _, instance := range addingInstances {
		p, err = a.AddInstance(p, instance)
		if err != nil {
			return nil, err
		}
	}

	return a.RemoveInstance(p, instanceID)
}
