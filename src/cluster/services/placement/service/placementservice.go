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

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
	"github.com/m3db/m3cluster/services/placement/algo"
	"github.com/m3db/m3cluster/services/placement/selector"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3x/log"
)

type placementService struct {
	s        placement.Storage
	sid      services.ServiceID
	opts     services.PlacementOptions
	algo     placement.Algorithm
	selector placement.InstanceSelector
	logger   xlog.Logger
}

// NewPlacementService returns an instance of placement service.
func NewPlacementService(s placement.Storage, sid services.ServiceID, opts services.PlacementOptions) services.PlacementService {
	return placementService{
		s:        s,
		sid:      sid,
		opts:     opts,
		algo:     algo.NewAlgorithm(opts),
		selector: selector.NewInstanceSelector(opts),
		logger:   opts.InstrumentOptions().Logger(),
	}
}

func (ps placementService) BuildInitialPlacement(
	candidates []services.PlacementInstance,
	numShards int,
	rf int,
) (services.Placement, error) {
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

	// NB(r): All new placements should appear as available for
	// proper client semantics when calculating consistency results
	p, err = placement.MarkAllShardsAsAvailable(p)
	if err != nil {
		return nil, err
	}

	if ps.opts.Dryrun() {
		ps.logger.Info("this is a dryrun, the operation is not persisted")
		return p, err
	}

	return p, ps.s.SetIfNotExist(ps.sid, p)
}

func (ps placementService) AddReplica() (services.Placement, error) {
	p, v, err := ps.s.Placement(ps.sid)
	if err != nil {
		return nil, err
	}

	if p, err = ps.algo.AddReplica(p); err != nil {
		return nil, err
	}

	if err := placement.Validate(p); err != nil {
		return nil, err
	}

	if ps.opts.Dryrun() {
		ps.logger.Info("this is a dryrun, the operation is not persisted")
		return p, err
	}

	return p, ps.s.CheckAndSet(ps.sid, p, v)
}

func (ps placementService) AddInstances(
	candidates []services.PlacementInstance,
) (services.Placement, []services.PlacementInstance, error) {
	p, v, err := ps.s.Placement(ps.sid)
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
			return nil, nil, fmt.Errorf("unable to find added instance [%s] in new placement", addingInstance.ID())
		}
		addingInstances[i] = addingInstance
	}

	if ps.opts.Dryrun() {
		ps.logger.Info("this is a dryrun, the operation is not persisted")
		return p, addingInstances, err
	}

	return p, addingInstances, ps.s.CheckAndSet(ps.sid, p, v)
}

func (ps placementService) RemoveInstances(instanceIDs []string) (services.Placement, error) {
	p, v, err := ps.s.Placement(ps.sid)
	if err != nil {
		return nil, err
	}

	if p, err = ps.algo.RemoveInstances(p, instanceIDs); err != nil {
		return nil, err
	}

	if err := placement.Validate(p); err != nil {
		return nil, err
	}

	if ps.opts.Dryrun() {
		ps.logger.Info("this is a dryrun, the operation is not persisted")
		return p, err
	}

	return p, ps.s.CheckAndSet(ps.sid, p, v)
}

func (ps placementService) ReplaceInstances(
	leavingInstanceIDs []string,
	candidates []services.PlacementInstance,
) (services.Placement, []services.PlacementInstance, error) {
	p, v, err := ps.s.Placement(ps.sid)
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

	addedInstances := make([]services.PlacementInstance, 0, len(addingInstances))
	for _, inst := range addingInstances {
		addedInstance, ok := p.Instance(inst.ID())
		if !ok {
			return nil, nil, fmt.Errorf("unable to find added instance [%+v] in new placement [%+v]", inst, p)
		}
		addedInstances = append(addedInstances, addedInstance)
	}

	if ps.opts.Dryrun() {
		ps.logger.Info("this is a dryrun, the operation is not persisted")
		return p, addedInstances, err
	}

	return p, addedInstances, ps.s.CheckAndSet(ps.sid, p, v)
}

func (ps placementService) MarkShardAvailable(instanceID string, shardID uint32) error {
	p, v, err := ps.s.Placement(ps.sid)
	if err != nil {
		return err
	}

	p, err = placement.MarkShardAvailable(p, instanceID, shardID)
	if err != nil {
		return err
	}

	if err := placement.Validate(p); err != nil {
		return err
	}

	return ps.s.CheckAndSet(ps.sid, p, v)
}

func (ps placementService) MarkInstanceAvailable(instanceID string) error {
	p, v, err := ps.s.Placement(ps.sid)
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
		p, err = placement.MarkShardAvailable(p, instanceID, s.ID())
		if err != nil {
			return err
		}
	}

	if err := placement.Validate(p); err != nil {
		return err
	}

	return ps.s.CheckAndSet(ps.sid, p, v)
}

func (ps placementService) Placement() (services.Placement, int, error) {
	return ps.s.Placement(ps.sid)
}

func (ps placementService) SetPlacement(p services.Placement) error {
	if err := placement.Validate(p); err != nil {
		return err
	}

	if ps.opts.Dryrun() {
		ps.logger.Info("this is a dryrun, the operation is not persisted")
		return nil
	}

	return ps.s.Set(ps.sid, p)
}

func (ps placementService) Delete() error {
	if ps.opts.Dryrun() {
		ps.logger.Info("this is a dryrun, the placement is not deleted")
		return nil
	}

	return ps.s.Delete(ps.sid)
}
