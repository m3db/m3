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

	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/placement/algo"
	"github.com/m3db/m3cluster/placement/selector"
	"github.com/m3db/m3cluster/shard"
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

	p, err := ps.algo.InitialPlacement(instances, ids, rf)
	if err != nil {
		return nil, err
	}

	if err := placement.Validate(p); err != nil {
		return nil, err
	}

	return p, ps.SetIfNotExist(p)
}

func (ps *placementService) AddReplica() (placement.Placement, error) {
	p, v, err := ps.Placement()
	if err != nil {
		return nil, err
	}

	if p, err = ps.algo.AddReplica(p); err != nil {
		return nil, err
	}

	if err := placement.Validate(p); err != nil {
		return nil, err
	}

	return p, ps.CheckAndSet(p, v)
}

func (ps *placementService) AddInstances(
	candidates []placement.Instance,
) (placement.Placement, []placement.Instance, error) {
	p, v, err := ps.Placement()
	if err != nil {
		return nil, nil, err
	}

	addingInstances, err := ps.selector.SelectAddingInstances(candidates, p)
	if err != nil {
		return nil, nil, err
	}

	if p, err = ps.algo.AddInstances(p, addingInstances); err != nil {
		return nil, nil, err
	}

	if err := placement.Validate(p); err != nil {
		return nil, nil, err
	}

	for i, instance := range addingInstances {
		addingInstance, ok := p.Instance(instance.ID())
		if !ok {
			return nil, nil, fmt.Errorf("unable to find added instance [%s] in new placement", instance.ID())
		}
		addingInstances[i] = addingInstance
	}

	return p, addingInstances, ps.CheckAndSet(p, v)
}

func (ps *placementService) RemoveInstances(instanceIDs []string) (placement.Placement, error) {
	p, v, err := ps.Placement()
	if err != nil {
		return nil, err
	}

	if p, err = ps.algo.RemoveInstances(p, instanceIDs); err != nil {
		return nil, err
	}

	if err := placement.Validate(p); err != nil {
		return nil, err
	}

	return p, ps.CheckAndSet(p, v)
}

func (ps *placementService) ReplaceInstances(
	leavingInstanceIDs []string,
	candidates []placement.Instance,
) (placement.Placement, []placement.Instance, error) {
	p, v, err := ps.Placement()
	if err != nil {
		return nil, nil, err
	}

	addingInstances, err := ps.selector.SelectReplaceInstances(candidates, leavingInstanceIDs, p)
	if err != nil {
		return nil, nil, err
	}

	if p, err = ps.algo.ReplaceInstances(p, leavingInstanceIDs, addingInstances); err != nil {
		return nil, nil, err
	}

	if err := placement.Validate(p); err != nil {
		return nil, nil, err
	}

	addedInstances := make([]placement.Instance, 0, len(addingInstances))
	for _, inst := range addingInstances {
		addedInstance, ok := p.Instance(inst.ID())
		if !ok {
			return nil, nil, fmt.Errorf("unable to find added instance [%+v] in new placement [%+v]", inst, p)
		}
		addedInstances = append(addedInstances, addedInstance)
	}

	return p, addedInstances, ps.CheckAndSet(p, v)
}

func (ps *placementService) MarkShardAvailable(instanceID string, shardID uint32) error {
	p, v, err := ps.Placement()
	if err != nil {
		return err
	}

	if p, err = ps.algo.MarkShardAvailable(p, instanceID, shardID); err != nil {
		return err
	}

	if err := placement.Validate(p); err != nil {
		return err
	}

	return ps.CheckAndSet(p, v)
}

func (ps *placementService) MarkAllShardsAvailable() (placement.Placement, error) {
	p, v, err := ps.Placement()
	if err != nil {
		return nil, err
	}

	p, updated, err := ps.algo.MarkAllShardsAvailable(p)
	if err != nil {
		return nil, err
	}
	if !updated {
		return p, nil
	}

	if err := placement.Validate(p); err != nil {
		return nil, err
	}

	return p, ps.CheckAndSet(p, v)
}

func (ps *placementService) MarkInstanceAvailable(instanceID string) error {
	p, v, err := ps.Placement()
	if err != nil {
		return err
	}

	instance, exist := p.Instance(instanceID)
	if !exist {
		return fmt.Errorf("could not find instance %s in placement", instanceID)
	}

	for _, s := range instance.Shards().All() {
		if s.State() != shard.Initializing {
			continue
		}
		p, err = ps.algo.MarkShardAvailable(p, instanceID, s.ID())
		if err != nil {
			return err
		}
	}

	if err := placement.Validate(p); err != nil {
		return err
	}

	return ps.CheckAndSet(p, v)
}
