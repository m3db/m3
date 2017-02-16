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
	"errors"
	"fmt"
	"sort"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
	"github.com/m3db/m3cluster/services/placement/algo"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3x/log"
)

var (
	errInstanceAbsent        = errors.New("could not remove or replace a instance that does not exist")
	errNoValidInstance       = errors.New("no valid instance in the candidate list")
	errMultipleZones         = errors.New("could not init placement on instances from multiple zones")
	errPlacementAlreadyExist = errors.New("could not init placement for service, there is already a placement")
)

type placementService struct {
	ss      placement.Storage
	service services.ServiceID
	opts    services.PlacementOptions
	algo    placement.Algorithm
	logger  xlog.Logger
}

// NewPlacementService returns an instance of placement service
func NewPlacementService(ss placement.Storage, service services.ServiceID, opts services.PlacementOptions) services.PlacementService {
	return placementService{
		ss:      ss,
		service: service,
		opts:    opts,
		algo:    algo.NewAlgorithm(opts),
		logger:  opts.InstrumentOptions().Logger(),
	}
}

func (ps placementService) BuildInitialPlacement(
	instances []services.PlacementInstance,
	numShards int,
	rf int,
) (services.ServicePlacement, error) {
	if numShards < 0 {
		return nil, fmt.Errorf("could not build initial placement, invalid numShards %d", numShards)
	}

	if rf <= 0 {
		return nil, fmt.Errorf("could not build initial placement, invalid replica factor %d", rf)
	}
	var err error
	if err = ps.validateInitInstances(instances); err != nil {
		return nil, err
	}

	ids := make([]uint32, numShards)
	for i := 0; i < numShards; i++ {
		ids[i] = uint32(i)
	}

	p, err := ps.initPlacement(instances, ids, rf)
	if err != nil {
		return nil, err
	}

	if err := placement.Validate(p); err != nil {
		return nil, err
	}

	// NB(r): All new placements should appear as available for
	// proper client semantics when calculating consistency results
	p, err = markAllShardsAsAvailable(p)
	if err != nil {
		return nil, err
	}

	if ps.opts.Dryrun() {
		ps.logger.Info("this is a dryrun, the operation is not persisted")
		return p, err
	}

	return p, ps.ss.SetIfNotExist(ps.service, p)
}

func (ps placementService) initPlacement(
	instances []services.PlacementInstance,
	ids []uint32,
	rf int,
) (services.ServicePlacement, error) {
	p, err := ps.algo.InitialPlacement(instances, ids)
	if err != nil {
		return nil, err
	}

	for i := 1; i < rf; i++ {
		if p, err = ps.algo.AddReplica(p); err != nil {
			return nil, err
		}
	}

	return p, nil
}

func (ps placementService) AddReplica() (services.ServicePlacement, error) {
	p, v, err := ps.ss.Placement(ps.service)
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

	return p, ps.ss.CheckAndSet(ps.service, p, v)
}

func (ps placementService) AddInstance(
	candidates []services.PlacementInstance,
) (services.ServicePlacement, error) {
	p, v, err := ps.ss.Placement(ps.service)
	if err != nil {
		return nil, err
	}
	var addingInstance services.PlacementInstance
	if addingInstance, err = ps.findAddingInstance(p, candidates, ps.opts); err != nil {
		return nil, err
	}

	if p, err = ps.algo.AddInstance(p, addingInstance); err != nil {
		return nil, err
	}

	if err := placement.Validate(p); err != nil {
		return nil, err
	}

	if ps.opts.Dryrun() {
		ps.logger.Info("this is a dryrun, the operation is not persisted")
		return p, err
	}

	return p, ps.ss.CheckAndSet(ps.service, p, v)
}

func (ps placementService) RemoveInstance(instanceID string) (services.ServicePlacement, error) {
	p, v, err := ps.ss.Placement(ps.service)
	if err != nil {
		return nil, err
	}

	if _, exist := p.Instance(instanceID); !exist {
		return nil, errInstanceAbsent
	}

	if p, err = ps.algo.RemoveInstance(p, instanceID); err != nil {
		return nil, err
	}

	if err := placement.Validate(p); err != nil {
		return nil, err
	}

	if ps.opts.Dryrun() {
		ps.logger.Info("this is a dryrun, the operation is not persisted")
		return p, err
	}

	return p, ps.ss.CheckAndSet(ps.service, p, v)
}

func (ps placementService) ReplaceInstance(
	leavingInstanceID string,
	candidates []services.PlacementInstance,
) (services.ServicePlacement, error) {
	p, v, err := ps.ss.Placement(ps.service)
	if err != nil {
		return nil, err
	}

	leavingInstance, exist := p.Instance(leavingInstanceID)
	if !exist {
		return nil, errInstanceAbsent
	}

	addingInstances, err := ps.findReplaceInstance(p, candidates, leavingInstance)
	if err != nil {
		return nil, err
	}

	if p, err = ps.algo.ReplaceInstance(p, leavingInstanceID, addingInstances); err != nil {
		return nil, err
	}

	if err := placement.Validate(p); err != nil {
		return nil, err
	}

	if ps.opts.Dryrun() {
		ps.logger.Info("this is a dryrun, the operation is not persisted")
		return p, err
	}

	return p, ps.ss.CheckAndSet(ps.service, p, v)
}

func (ps placementService) MarkShardAvailable(instanceID string, shardID uint32) error {
	p, v, err := ps.ss.Placement(ps.service)
	if err != nil {
		return err
	}

	p, err = algo.MarkShardAvailable(p, instanceID, shardID)
	if err != nil {
		return err
	}

	if err := placement.Validate(p); err != nil {
		return err
	}

	return ps.ss.CheckAndSet(ps.service, p, v)
}

func (ps placementService) MarkInstanceAvailable(instanceID string) error {
	p, v, err := ps.ss.Placement(ps.service)
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
		p, err = algo.MarkShardAvailable(p, instanceID, s.ID())
		if err != nil {
			return err
		}
	}

	if err := placement.Validate(p); err != nil {
		return err
	}

	return ps.ss.CheckAndSet(ps.service, p, v)
}

func (ps placementService) Placement() (services.ServicePlacement, int, error) {
	return ps.ss.Placement(ps.service)
}

func (ps placementService) SetPlacement(p services.ServicePlacement) error {
	if err := placement.Validate(p); err != nil {
		return err
	}

	if ps.opts.Dryrun() {
		ps.logger.Info("this is a dryrun, the operation is not persisted")
		return nil
	}

	_, v, err := ps.ss.Placement(ps.service)
	if err == kv.ErrNotFound {
		return ps.ss.SetIfNotExist(ps.service, p)
	}

	if err != nil {
		return err
	}

	return ps.ss.CheckAndSet(ps.service, p, v)
}

func (ps placementService) Delete() error {
	if ps.opts.Dryrun() {
		ps.logger.Info("this is a dryrun, the placement is not deleted")
		return nil
	}

	return ps.ss.Delete(ps.service)
}

func (ps placementService) validateInitInstances(instances []services.PlacementInstance) error {
	var zone string
	for _, instance := range instances {
		if zone == "" {
			zone = instance.Zone()
			continue
		}
		if zone != instance.Zone() {
			return errMultipleZones
		}
	}
	return nil
}

func (ps placementService) findAddingInstance(
	p services.ServicePlacement,
	candidates []services.PlacementInstance,
	opts services.PlacementOptions,
) (services.PlacementInstance, error) {
	// filter out already existing instances
	candidates = ps.getNewInstancesToPlacement(p, candidates, opts)

	// build rack-instance map for candidate instances
	candidateRackMap := buildRackMap(candidates)

	// build rack-instance map for current placement
	placementRackMap := buildRackMap(p.Instances())

	// if there is a rack not in the current placement, prefer that rack
	for r, instances := range candidateRackMap {
		if _, exist := placementRackMap[r]; !exist {
			return instances[0], nil
		}
	}

	// otherwise sort the racks in the current placement by capacity and find a instance from least sized rack
	racks := make(sortableThings, 0, len(placementRackMap))
	for rack, instances := range placementRackMap {
		weight := 0
		for _, i := range instances {
			weight += int(i.Weight())
		}
		racks = append(racks, sortableValue{value: rack, weight: weight})
	}
	sort.Sort(racks)

	for _, rackLen := range racks {
		if i, exist := candidateRackMap[rackLen.value.(string)]; exist {
			for _, instance := range i {
				return instance, nil
			}
		}
	}
	// no instance in the candidate instances can be added to the placement
	return nil, errNoValidInstance
}

func (ps placementService) findReplaceInstance(
	p services.ServicePlacement,
	candidates []services.PlacementInstance,
	leaving services.PlacementInstance,
) ([]services.PlacementInstance, error) {
	// filter out already existing instances
	candidates = ps.getNewInstancesToPlacement(p, candidates, ps.opts)

	if len(candidates) == 0 {
		return nil, errNoValidInstance
	}
	// map rack to instances
	rackMap := buildRackMap(candidates)

	// otherwise sort the candidate instances by the number of conflicts
	ph := algo.NewPlacementHelper(p, ps.opts)
	instances := make([]sortableValue, 0, len(rackMap))
	for rack, instancesInRack := range rackMap {
		conflicts := 0
		for _, s := range leaving.Shards().All() {
			if !ph.HasNoRackConflict(s.ID(), leaving, rack) {
				conflicts++
			}
		}
		for _, instance := range instancesInRack {
			instances = append(instances, sortableValue{value: instance, weight: conflicts})
		}
	}

	groups := groupInstancesByConflict(instances, ps.opts.LooseRackCheck())
	if len(groups) == 0 {
		return nil, errNoValidInstance
	}

	result, leftWeight := fillWeight(groups, int(leaving.Weight()))

	if leftWeight > 0 {
		return nil, fmt.Errorf("could not find enough instance to replace %s, %v weight could not be replaced",
			leaving.String(), leftWeight)
	}
	return result, nil
}

func (ps placementService) getNewInstancesToPlacement(
	p services.ServicePlacement,
	candidates []services.PlacementInstance,
	opts services.PlacementOptions,
) []services.PlacementInstance {
	var instances []services.PlacementInstance
	for _, h := range candidates {
		if _, exist := p.Instance(h.ID()); !exist {
			instances = append(instances, h)
		}
	}
	return filterZones(p, instances, opts)
}

func filterZones(
	p services.ServicePlacement,
	candidates []services.PlacementInstance,
	opts services.PlacementOptions,
) []services.PlacementInstance {
	var validZone string
	for _, instance := range p.Instances() {
		if validZone == "" {
			validZone = instance.Zone()
			break
		}
	}

	validInstances := make([]services.PlacementInstance, 0, len(candidates))
	for _, instance := range candidates {
		if validZone == instance.Zone() {
			validInstances = append(validInstances, instance)
		}
	}
	return validInstances
}

func groupInstancesByConflict(instancesSortedByConflicts []sortableValue, allowConflict bool) [][]services.PlacementInstance {
	sort.Sort(sortableThings(instancesSortedByConflicts))
	var groups [][]services.PlacementInstance
	lastSeenConflict := -1
	for _, instance := range instancesSortedByConflicts {
		if !allowConflict && instance.weight > 0 {
			break
		}
		if instance.weight > lastSeenConflict {
			lastSeenConflict = instance.weight
			groups = append(groups, []services.PlacementInstance{})
		}
		if lastSeenConflict == instance.weight {
			groups[len(groups)-1] = append(groups[len(groups)-1], instance.value.(services.PlacementInstance))
		}
	}
	return groups
}

func fillWeight(groups [][]services.PlacementInstance, targetWeight int) ([]services.PlacementInstance, int) {
	var (
		result           []services.PlacementInstance
		instancesInGroup []services.PlacementInstance
	)
	for _, group := range groups {
		sort.Sort(placement.ByIDAscending(group))
		instancesInGroup, targetWeight = knapsack(group, targetWeight)
		result = append(result, instancesInGroup...)
		if targetWeight <= 0 {
			break
		}
	}
	return result, targetWeight
}

func knapsack(instances []services.PlacementInstance, targetWeight int) ([]services.PlacementInstance, int) {
	totalWeight := 0
	for _, instance := range instances {
		totalWeight += int(instance.Weight())
	}
	if totalWeight <= targetWeight {
		return instances[:], targetWeight - totalWeight
	}
	// totalWeight > targetWeight, there is a combination of instances to meet targetWeight for sure
	// we do dp until totalWeight rather than targetWeight here because we need to cover the targetWeight
	// which is a little bit different than the knapsack problem that goes
	weights := make([]int, totalWeight+1)
	combination := make([][]services.PlacementInstance, totalWeight+1)

	// dp: weights[i][j] = max(weights[i-1][j], weights[i-1][j-instance.Weight] + instance.Weight)
	// when there are multiple combination to reach a target weight, we prefer the one with less instances
	for i := range instances {
		// this loop needs to go from len to 1 because updating weights[] is being updated in place
		for j := totalWeight; j >= 1; j-- {
			weight := int(instances[i].Weight())
			if j-weight < 0 {
				continue
			}
			newWeight := weights[j-weight] + weight
			if newWeight > weights[j] {
				weights[j] = weights[j-weight] + weight
				combination[j] = append(combination[j-weight], instances[i])
			} else if newWeight == weights[j] {
				// if can reach same weight, find a combination with less instances
				if len(combination[j-weight])+1 < len(combination[j]) {
					combination[j] = append(combination[j-weight], instances[i])
				}
			}
		}
	}
	for i := targetWeight; i <= totalWeight; i++ {
		if weights[i] >= targetWeight {
			return combination[i], targetWeight - weights[i]
		}
	}

	panic("should never reach here")
}

func buildRackMap(candidates []services.PlacementInstance) map[string][]services.PlacementInstance {
	result := make(map[string][]services.PlacementInstance, len(candidates))
	for _, instance := range candidates {
		if _, exist := result[instance.Rack()]; !exist {
			result[instance.Rack()] = make([]services.PlacementInstance, 0)
		}
		result[instance.Rack()] = append(result[instance.Rack()], instance)
	}
	return result
}

type sortableValue struct {
	value  interface{}
	weight int
}

type sortableThings []sortableValue

func (things sortableThings) Len() int {
	return len(things)
}

func (things sortableThings) Less(i, j int) bool {
	return things[i].weight < things[j].weight
}

func (things sortableThings) Swap(i, j int) {
	things[i], things[j] = things[j], things[i]
}

func markAllShardsAsAvailable(p services.ServicePlacement) (services.ServicePlacement, error) {
	var err error
	for _, instance := range p.Instances() {
		for _, s := range instance.Shards().All() {
			if s.State() == shard.Initializing {
				p, err = algo.MarkShardAvailable(p, instance.ID(), s.ID())
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return p, nil
}
