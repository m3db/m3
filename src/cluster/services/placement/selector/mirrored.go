// Copyright (c) 2017 Uber Technologies, Inc.
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

package selector

import (
	"container/heap"
	"errors"
	"fmt"
	"math"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
	"github.com/m3db/m3x/log"
)

var (
	errNoValidMirrorInstance = errors.New("no valid instance for mirror placement in the candidate list")
)

type mirroredFilter struct {
	opts   services.PlacementOptions
	logger xlog.Logger
}

func newMirroredSelector(opts services.PlacementOptions) placement.InstanceSelector {
	return &mirroredFilter{
		opts:   opts,
		logger: opts.InstrumentOptions().Logger(),
	}
}

// SelectInitialInstances tries to make as many groups as possible from
// the candidate instances to make the initial placement.
func (f *mirroredFilter) SelectInitialInstances(
	candidates []services.PlacementInstance,
	rf int,
) ([]services.PlacementInstance, error) {
	candidates, err := getValidCandidates(placement.NewPlacement(), candidates, f.opts)
	if err != nil {
		return nil, err
	}

	weightToHostMap, err := groupHostsByWeight(candidates)
	if err != nil {
		return nil, err
	}

	var groups = make([][]services.PlacementInstance, 0, len(candidates))
	for _, hosts := range weightToHostMap {
		groupedHosts, ungrouped := groupHostsWithRackCheck(hosts, rf)
		if len(ungrouped) != 0 {
			for _, host := range ungrouped {
				f.logger.Warnf("could not group host %s, rack %s, weight %d", host.name, host.rack, host.weight)
			}
		}
		if len(groupedHosts) == 0 {
			continue
		}

		groupedInstances, err := groupInstancesByHostPort(groupedHosts)
		if err != nil {
			return nil, err
		}

		groups = append(groups, groupedInstances...)
	}

	if len(groups) == 0 {
		return nil, errNoValidMirrorInstance
	}

	return assignShardsToGroupedInstances(groups, placement.NewPlacement()), nil
}

// SelectAddingInstances tries to make just one group of hosts from
// the candidate instances to be added to the placement.
func (f *mirroredFilter) SelectAddingInstances(
	candidates []services.PlacementInstance,
	p services.Placement,
) ([]services.PlacementInstance, error) {
	candidates, err := getValidCandidates(p, candidates, f.opts)
	if err != nil {
		return nil, err
	}

	weightToHostMap, err := groupHostsByWeight(candidates)
	if err != nil {
		return nil, err
	}

	var groups = make([][]services.PlacementInstance, 0, len(candidates))
	for _, hosts := range weightToHostMap {
		groupedHosts, _ := groupHostsWithRackCheck(hosts, p.ReplicaFactor())
		if len(groupedHosts) == 0 {
			continue
		}

		groups, err = groupInstancesByHostPort(groupedHosts[:1])
		if err != nil {
			return nil, err
		}

		break
	}

	if len(groups) == 0 {
		return nil, errNoValidMirrorInstance
	}

	return assignShardsToGroupedInstances(groups, p), nil
}

// FindReplaceInstances for mirror supports replacing multiple instances from one host.
// Two main use cases:
// 1, find a new host from a pool of hosts to replace a host in the placement.
// 2, back out of a replacement, both leaving and adding host are still in the placement.
func (f *mirroredFilter) SelectReplaceInstances(
	candidates []services.PlacementInstance,
	leavingInstanceIDs []string,
	p services.Placement,
) ([]services.PlacementInstance, error) {
	candidates, err := getValidCandidates(p, candidates, f.opts)
	if err != nil {
		return nil, err
	}

	leavingInstances := make([]services.PlacementInstance, 0, len(leavingInstanceIDs))
	for _, id := range leavingInstanceIDs {
		leavingInstance, exist := p.Instance(id)
		if !exist {
			return nil, errInstanceAbsent
		}
		leavingInstances = append(leavingInstances, leavingInstance)
	}

	// Validate leaving instances.
	var (
		h     host
		ssIDs = make(map[uint32]struct{}, len(leavingInstances))
	)
	for _, instance := range leavingInstances {
		if h.name == "" {
			h = newHost(instance.Hostname(), instance.Rack(), instance.Weight())
		}

		err := h.addInstance(instance.Port(), instance)
		if err != nil {
			return nil, err
		}
		ssIDs[instance.ShardSetID()] = struct{}{}
	}

	weightToHostMap, err := groupHostsByWeight(candidates)
	if err != nil {
		return nil, err
	}

	hosts, ok := weightToHostMap[h.weight]
	if !ok {
		return nil, fmt.Errorf("could not find instances with weight %d in the candidate list", h.weight)
	}

	// Find out the racks that are already in the same shard set id with the leaving instances.
	var conflictRacks = make(map[string]struct{})
	for _, instance := range p.Instances() {
		if _, ok := ssIDs[instance.ShardSetID()]; !ok {
			continue
		}
		if instance.Hostname() == h.name {
			continue
		}

		conflictRacks[instance.Rack()] = struct{}{}
	}

	var groups [][]services.PlacementInstance
	for _, candidateHost := range hosts {
		if candidateHost.name == h.name {
			continue
		}

		if _, ok := conflictRacks[candidateHost.rack]; ok {
			continue
		}

		groups, err = groupInstancesByHostPort([][]host{[]host{h, candidateHost}})
		if err != nil {
			f.logger.Warnf("could not match up candidate host %s with target host %s: %v", candidateHost.name, h.name, err)
			continue
		}

		// Successfully grouped candidate with the host in placement.
		break
	}

	if len(groups) == 0 {
		return nil, errNoValidMirrorInstance
	}

	// The groups returned from the groupInstances() might not be the same order as
	// the instances in leavingInstanceIDs. We need to reorder them to the same order
	// as leavingInstanceIDs.
	var res = make([]services.PlacementInstance, len(groups))
	for _, group := range groups {
		if len(group) != 2 {
			return nil, fmt.Errorf("unexpected length of instance group for replacement: %d", len(group))
		}

		idx := findIndex(leavingInstanceIDs, group[0].ID())
		if idx == -1 {
			return nil, fmt.Errorf("could not find instance id: '%s' in leaving instances", group[0].ID())
		}

		res[idx] = group[1].SetShardSetID(group[0].ShardSetID())
	}

	return res, nil
}

func findIndex(ids []string, id string) int {
	for i := range ids {
		if ids[i] == id {
			return i
		}
	}
	// Unexpected.
	return -1
}

func groupHostsByWeight(candidates []services.PlacementInstance) (map[uint32][]host, error) {
	var (
		uniqueHosts      = make(map[string]host, len(candidates))
		weightToHostsMap = make(map[uint32][]host, len(candidates))
	)
	for _, instance := range candidates {
		hostname := instance.Hostname()
		weight := instance.Weight()
		h, ok := uniqueHosts[hostname]
		if !ok {
			h = newHost(hostname, instance.Rack(), weight)
			uniqueHosts[hostname] = h
			weightToHostsMap[weight] = append(weightToHostsMap[weight], h)
		}
		err := h.addInstance(instance.Port(), instance)
		if err != nil {
			return nil, err
		}
	}
	return weightToHostsMap, nil
}

// groupHostsWithRackCheck looks at the racks of the given hosts
// and try to make as many groups as possible. The hosts in each group
// must come from different racks.
func groupHostsWithRackCheck(hosts []host, rf int) ([][]host, []host) {
	if len(hosts) < rf {
		// When the number of hosts is less than rf, no groups can be made.
		return nil, hosts
	}

	var (
		uniqRacks = make(map[string]*rack, len(hosts))
		rh        = racksByNumHost(make([]*rack, 0, len(hosts)))
	)
	for _, h := range hosts {
		r, ok := uniqRacks[h.rack]
		if !ok {
			r = &rack{
				rack:  h.rack,
				hosts: make([]host, 0, rf),
			}

			uniqRacks[h.rack] = r
			rh = append(rh, r)
		}
		r.hosts = append(r.hosts, h)
	}

	heap.Init(&rh)

	// For each group, always prefer to find one host from the largest rack
	// in the heap. After a group is filled, push all the checked racks back
	// to the heap so they can be used for the next group.
	res := make([][]host, 0, int(math.Ceil(float64(len(hosts))/float64(rf))))
	for rh.Len() >= rf {
		// When there are more than rf racks available, try to make a group.
		seenRacks := make(map[string]*rack, rf)
		groups := make([]host, 0, rf)
		for i := 0; i < rf; i++ {
			r := heap.Pop(&rh).(*rack)
			// Move the host from the rack to the group. The racks in the heap
			// always have at least one host.
			groups = append(groups, r.hosts[len(r.hosts)-1])
			r.hosts = r.hosts[:len(r.hosts)-1]
			seenRacks[r.rack] = r
		}
		if len(groups) == rf {
			res = append(res, groups)
		}
		for _, r := range seenRacks {
			if len(r.hosts) > 0 {
				heap.Push(&rh, r)
			}
		}
	}

	ungrouped := make([]host, 0, rh.Len())
	for _, r := range rh {
		ungrouped = append(ungrouped, r.hosts...)
	}
	return res, ungrouped
}

func groupInstancesByHostPort(hostGroups [][]host) ([][]services.PlacementInstance, error) {
	var instanceGroups = make([][]services.PlacementInstance, 0, len(hostGroups))
	for _, hostGroup := range hostGroups {
		for port, instance := range hostGroup[0].portToInstance {
			instanceGroup := make([]services.PlacementInstance, 0, len(hostGroup))
			instanceGroup = append(instanceGroup, instance)
			for _, otherHost := range hostGroup[1:] {
				otherInstance, ok := otherHost.portToInstance[port]
				if !ok {
					return nil, fmt.Errorf("could not find port %d on host %s", port, otherHost.name)
				}
				instanceGroup = append(instanceGroup, otherInstance)
			}
			instanceGroups = append(instanceGroups, instanceGroup)
		}
	}
	return instanceGroups, nil
}

func assignShardsToGroupedInstances(
	groups [][]services.PlacementInstance,
	p services.Placement,
) []services.PlacementInstance {
	var (
		instances   = make([]services.PlacementInstance, 0, p.ReplicaFactor()*len(groups))
		shardSetIDs = nextNShardSetIDs(p, len(groups))
	)
	for i, group := range groups {
		ssID := shardSetIDs[i]
		for _, instance := range group {
			instances = append(instances, instance.SetShardSetID(ssID))
		}
	}

	return instances
}

// nextNShardSetIDs finds the next n smallest integers that are not used
// as shard set ids in the placement.
func nextNShardSetIDs(p services.Placement, n int) []uint32 {
	var (
		instances          = p.Instances()
		currentShardSetIDs = make(map[uint32]struct{}, len(instances))
	)
	for _, instance := range instances {
		currentShardSetIDs[instance.ShardSetID()] = struct{}{}
	}

	var (
		result   = make([]uint32, 0, n)
		curIndex = -1
	)
	// Find the next n smallest integers to be used as shard set ids.
	for len(result) < n {
		curIndex++
		if _, ok := currentShardSetIDs[uint32(curIndex)]; ok {
			continue
		}
		result = append(result, uint32(curIndex))
	}

	return result
}

type host struct {
	name           string
	rack           string
	weight         uint32
	portToInstance map[uint32]services.PlacementInstance
}

func newHost(name, rack string, weight uint32) host {
	return host{
		name:           name,
		rack:           rack,
		weight:         weight,
		portToInstance: make(map[uint32]services.PlacementInstance),
	}
}

func (h host) addInstance(port uint32, instance services.PlacementInstance) error {
	if h.weight != instance.Weight() {
		return fmt.Errorf("could not add instance %s to host %s, weight mismatch: %d and %d",
			instance.ID(), h.name, instance.Weight(), h.weight)
	}
	if h.rack != instance.Rack() {
		return fmt.Errorf("could not add instance %s to host %s, rack mismatch: %s and %s",
			instance.ID(), h.name, instance.Rack(), h.rack)
	}
	h.portToInstance[port] = instance
	return nil
}

type rack struct {
	rack  string
	hosts []host
}

type racksByNumHost []*rack

func (h racksByNumHost) Len() int {
	return len(h)
}

func (h racksByNumHost) Less(i, j int) bool {
	return len(h[i].hosts) > len(h[j].hosts)
}

func (h racksByNumHost) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *racksByNumHost) Push(i interface{}) {
	r := i.(*rack)
	*h = append(*h, r)
}

func (h *racksByNumHost) Pop() interface{} {
	old := *h
	n := len(old)
	rack := old[n-1]
	*h = old[0 : n-1]
	return rack
}
