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

package service

import (
	"fmt"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/placement/algo"
	"github.com/m3db/m3/src/cluster/placement/selector"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3x/log"
)

type placementService struct {
	placement.Storage

	opts     placement.Options
	algo     placement.Algorithm
	selector placement.InstanceSelector
	logger   log.Logger
}

// NewPlacementService returns an instance of placement service.
func NewPlacementService(s placement.Storage, opts placement.Options) placement.Service {
	if opts == nil {
		opts = placement.NewOptions()
	}
	return &placementService{
		Storage:  s,
		opts:     opts,
		algo:     algo.NewAlgorithm(opts),
		selector: selector.NewInstanceSelector(opts),
		logger:   opts.InstrumentOptions().Logger(),
	}
}

func (ps *placementService) BuildInitialPlacement(
	candidates []placement.Instance,
	numShards int,
	rf int,
) (placement.Placement, error) {
	if numShards < 0 {
		return nil, fmt.Errorf("could not build initial placement, invalid numShards %d", numShards)
	}

	if rf <= 0 {
		return nil, fmt.Errorf("could not build initial placement, invalid replica factor %d", rf)
	}

	instances, err := ps.selector.SelectInitialInstances(candidates, rf)
	if err != nil {
		return nil, err
	}

	ids := make([]uint32, numShards)
	for i := 0; i < numShards; i++ {
		ids[i] = uint32(i)
	}

	tempPlacement, err := ps.algo.InitialPlacement(instances, ids, rf)
	if err != nil {
		return nil, err
	}

	if err := placement.Validate(tempPlacement); err != nil {
		return nil, err
	}

	return ps.SetIfNotExist(tempPlacement)
}

func (ps *placementService) AddReplica() (placement.Placement, error) {
	curPlacement, err := ps.Placement()
	if err != nil {
		return nil, err
	}

	tempPlacement, err := ps.algo.AddReplica(curPlacement)
	if err != nil {
		return nil, err
	}

	if err := placement.Validate(tempPlacement); err != nil {
		return nil, err
	}

	return ps.CheckAndSet(tempPlacement, curPlacement.GetVersion())
}

func (ps *placementService) AddInstances(
	candidates []placement.Instance,
) (placement.Placement, []placement.Instance, error) {
	curPlacement, err := ps.Placement()
	if err != nil {
		return nil, nil, err
	}

	addingInstances, err := ps.selector.SelectAddingInstances(candidates, curPlacement)
	if err != nil {
		return nil, nil, err
	}

	tempPlacement, err := ps.algo.AddInstances(curPlacement, addingInstances)
	if err != nil {
		return nil, nil, err
	}

	if err := placement.Validate(tempPlacement); err != nil {
		return nil, nil, err
	}

	for i, instance := range addingInstances {
		addingInstance, ok := tempPlacement.Instance(instance.ID())
		if !ok {
			return nil, nil, fmt.Errorf("unable to find added instance [%s] in new placement", instance.ID())
		}
		addingInstances[i] = addingInstance
	}

	newPlacement, err := ps.CheckAndSet(tempPlacement, curPlacement.GetVersion())
	if err != nil {
		return nil, nil, err
	}
	return newPlacement, addingInstances, nil
}

func (ps *placementService) RemoveInstances(instanceIDs []string) (placement.Placement, error) {
	curPlacement, err := ps.Placement()
	if err != nil {
		return nil, err
	}

	tempPlacement, err := ps.algo.RemoveInstances(curPlacement, instanceIDs)
	if err != nil {
		return nil, err
	}

	if err := placement.Validate(tempPlacement); err != nil {
		return nil, err
	}

	return ps.CheckAndSet(tempPlacement, curPlacement.GetVersion())
}

func (ps *placementService) ReplaceInstances(
	leavingInstanceIDs []string,
	candidates []placement.Instance,
) (placement.Placement, []placement.Instance, error) {
	curPlacement, err := ps.Placement()
	if err != nil {
		return nil, nil, err
	}

	addingInstances, err := ps.selector.SelectReplaceInstances(candidates, leavingInstanceIDs, curPlacement)
	if err != nil {
		return nil, nil, err
	}

	tempPlacement, err := ps.algo.ReplaceInstances(curPlacement, leavingInstanceIDs, addingInstances)
	if err != nil {
		return nil, nil, err
	}

	if err := placement.Validate(tempPlacement); err != nil {
		return nil, nil, err
	}

	addedInstances := make([]placement.Instance, 0, len(addingInstances))
	for _, inst := range addingInstances {
		addedInstance, ok := tempPlacement.Instance(inst.ID())
		if !ok {
			return nil, nil, fmt.Errorf("unable to find added instance [%+v] in new placement [%+v]", inst, curPlacement)
		}
		addedInstances = append(addedInstances, addedInstance)
	}

	newPlacement, err := ps.CheckAndSet(tempPlacement, curPlacement.GetVersion())
	if err != nil {
		return nil, nil, err
	}
	return newPlacement, addedInstances, nil
}

func (ps *placementService) MarkShardsAvailable(instanceID string, shardIDs ...uint32) (placement.Placement, error) {
	curPlacement, err := ps.Placement()
	if err != nil {
		return nil, err
	}

	tempPlacement, err := ps.algo.MarkShardsAvailable(curPlacement, instanceID, shardIDs...)
	if err != nil {
		return nil, err
	}

	if err := placement.Validate(tempPlacement); err != nil {
		return nil, err
	}

	return ps.CheckAndSet(tempPlacement, curPlacement.GetVersion())
}

func (ps *placementService) MarkInstanceAvailable(instanceID string) (placement.Placement, error) {
	curPlacement, err := ps.Placement()
	if err != nil {
		return nil, err
	}

	instance, exist := curPlacement.Instance(instanceID)
	if !exist {
		return nil, fmt.Errorf("could not find instance %s in placement", instanceID)
	}

	shards := instance.Shards().ShardsForState(shard.Initializing)
	shardIDs := make([]uint32, len(shards))
	for i, s := range shards {
		shardIDs[i] = s.ID()
	}

	tempPlacement, err := ps.algo.MarkShardsAvailable(curPlacement, instanceID, shardIDs...)
	if err != nil {
		return nil, err
	}

	if err := placement.Validate(tempPlacement); err != nil {
		return nil, err
	}

	return ps.CheckAndSet(tempPlacement, curPlacement.GetVersion())
}

func (ps *placementService) MarkAllShardsAvailable() (placement.Placement, error) {
	curPlacement, err := ps.Placement()
	if err != nil {
		return nil, err
	}

	tempPlacement, updated, err := ps.algo.MarkAllShardsAvailable(curPlacement)
	if err != nil {
		return nil, err
	}
	if !updated {
		return curPlacement, nil
	}

	if err := placement.Validate(tempPlacement); err != nil {
		return nil, err
	}

	return ps.CheckAndSet(tempPlacement, curPlacement.GetVersion())
}
