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

	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/placement/algo"
)

var (
	errInvalidShardLen    = errors.New("shardLen should be greater than zero")
	errHostAbsent         = errors.New("could not remove or replace a host that does not exist")
	errNoValidHost        = errors.New("no valid host in the candidate list")
	errDisableAcrossZones = errors.New("could not disable across zones on a placement that contains multi zones")
	errHostsAcrossZones   = errors.New("could not init placement on hosts across zones with acrossZones disabled")
)

type placementService struct {
	algo    placement.Algorithm
	ss      placement.SnapshotStorage
	options placement.Options
}

// NewPlacementService returns an instance of placement service
// set looseRackCheck to true means rack check will be loosen during host replacement
func NewPlacementService(options placement.Options, ss placement.SnapshotStorage) placement.Service {
	return placementService{algo: algo.NewRackAwarePlacementAlgorithm(options), ss: ss, options: options}
}

func (ps placementService) BuildInitialPlacement(service string, hosts []placement.Host, shardLen int, rf int) (placement.Snapshot, error) {
	if shardLen <= 0 {
		return nil, errInvalidShardLen
	}

	var err error
	if err = ps.validateInitHosts(hosts); err != nil {
		return nil, err
	}

	ids := make([]uint32, shardLen)
	for i := 0; i < shardLen; i++ {
		ids[i] = uint32(i)
	}

	var s placement.Snapshot
	for i := 0; i < rf; i++ {
		if i == 0 {
			s, err = ps.algo.BuildInitialPlacement(hosts, ids)
		} else {
			s, err = ps.algo.AddReplica(s)
		}
		if err != nil {
			return nil, err
		}
	}

	if err = ps.ss.SaveSnapshotForService(service, s); err != nil {
		return nil, err
	}

	return s, nil
}

func (ps placementService) AddReplica(service string) (placement.Snapshot, error) {
	var s placement.Snapshot
	var err error
	if s, err = ps.Snapshot(service); err != nil {
		return nil, err
	}

	if s, err = ps.algo.AddReplica(s); err != nil {
		return nil, err
	}

	if err = ps.ss.SaveSnapshotForService(service, s); err != nil {
		return nil, err
	}

	return s, nil
}

func (ps placementService) AddHost(service string, candidateHosts []placement.Host) (placement.Snapshot, error) {
	var s placement.Snapshot
	var err error
	if s, err = ps.Snapshot(service); err != nil {
		return nil, err
	}
	var addingHost placement.Host
	if addingHost, err = ps.findAddingHost(s, candidateHosts); err != nil {
		return nil, err
	}

	if s, err = ps.algo.AddHost(s, addingHost); err != nil {
		return nil, err
	}

	if err = ps.ss.SaveSnapshotForService(service, s); err != nil {
		return nil, err
	}

	return s, nil
}

func (ps placementService) RemoveHost(service string, host placement.Host) (placement.Snapshot, error) {
	var s placement.Snapshot
	var err error
	if s, err = ps.Snapshot(service); err != nil {
		return nil, err
	}

	if s.HostShard(host.ID()) == nil {
		return nil, errHostAbsent
	}

	if s, err = ps.algo.RemoveHost(s, host); err != nil {
		return nil, err
	}

	if err = ps.ss.SaveSnapshotForService(service, s); err != nil {
		return nil, err
	}

	return s, nil
}

func (ps placementService) ReplaceHost(service string, leavingHost placement.Host, candidateHosts []placement.Host) (placement.Snapshot, error) {
	var s placement.Snapshot
	var err error
	if s, err = ps.Snapshot(service); err != nil {
		return nil, err
	}

	leavingHostShard := s.HostShard(leavingHost.ID())
	if leavingHostShard == nil {
		return nil, errHostAbsent
	}

	addingHosts, err := ps.findReplaceHost(s, candidateHosts, leavingHostShard)
	if err != nil {
		return nil, err
	}

	if s, err = ps.algo.ReplaceHost(s, leavingHost, addingHosts); err != nil {
		return nil, err
	}

	if err = ps.ss.SaveSnapshotForService(service, s); err != nil {
		return nil, err
	}

	return s, nil
}

func (ps placementService) Snapshot(service string) (placement.Snapshot, error) {
	return ps.ss.ReadSnapshotForService(service)
}

func getNewHostsToPlacement(s placement.Snapshot, hosts []placement.Host) []placement.Host {
	var hs []placement.Host
	for _, h := range hosts {
		if s.HostShard(h.ID()) == nil {
			hs = append(hs, h)
		}
	}
	return hs
}

func (ps placementService) findAddingHost(s placement.Snapshot, candidateHosts []placement.Host) (placement.Host, error) {
	// filter out already existing hosts
	candidateHosts = getNewHostsToPlacement(s, candidateHosts)

	candidateHosts, err := filterZones(s, ps.options, candidateHosts)
	if err != nil {
		return nil, err
	}
	// build rack-host map for candidate hosts
	candidateRackHostMap := buildRackHostMap(candidateHosts)

	// build rack map for current placement
	placementRackHostMap := buildRackHostMapFromHostShards(s.HostShards())

	// if there is a rack not in the current placement, prefer that rack
	for r, hosts := range candidateRackHostMap {
		if _, exist := placementRackHostMap[r]; !exist {
			return hosts[0], nil
		}
	}

	// otherwise sort the racks in the current placement by capacity and find a host from least sized rack
	racks := make(sortableThings, 0, len(placementRackHostMap))
	for rack, hss := range placementRackHostMap {
		weight := 0
		for _, hs := range hss {
			weight += int(hs.Weight())
		}
		racks = append(racks, sortableValue{value: rack, weight: weight})
	}
	sort.Sort(racks)

	for _, rackLen := range racks {
		if hs, exist := candidateRackHostMap[rackLen.value.(string)]; exist {
			for _, host := range hs {
				return host, nil
			}
		}
	}
	// no host in the candidate hosts can be added to the placement
	return nil, errNoValidHost
}

func (ps placementService) findReplaceHost(
	s placement.Snapshot,
	candidateHosts []placement.Host,
	leaving placement.HostShards,
) ([]placement.Host, error) {
	// filter out already existing hosts
	candidateHosts = getNewHostsToPlacement(s, candidateHosts)
	candidateHosts, err := filterZones(s, ps.options, candidateHosts)
	if err != nil {
		return nil, err
	}

	if len(candidateHosts) == 0 {
		return nil, errNoValidHost
	}
	// build rackHostMap from candidate hosts
	rackHostMap := buildRackHostMap(candidateHosts)

	// otherwise sort the candidate hosts by the number of conflicts
	ph := algo.NewPlacementHelper(s, ps.options)
	hosts := make([]sortableValue, 0, len(rackHostMap))
	for rack, hostsInRack := range rackHostMap {
		conflicts := 0
		for _, shard := range leaving.Shards() {
			if !ph.HasNoRackConflict(shard, leaving, rack) {
				conflicts++
			}
		}
		for _, host := range hostsInRack {
			hosts = append(hosts, sortableValue{value: host, weight: conflicts})
		}
	}

	groups := groupHostsByConflict(hosts, ps.options.LooseRackCheck())
	if len(groups) == 0 {
		return nil, errNoValidHost
	}

	result, leftWeight := fillWeight(groups, int(leaving.Host().Weight()))

	if leftWeight > 0 {
		return nil, fmt.Errorf("could not find enough host to replace %s, %v weight could not be replaced",
			leaving.Host().String(), leftWeight)
	}
	return result, nil
}

func groupHostsByConflict(hostsSortedByConflicts []sortableValue, allowConflict bool) [][]placement.Host {
	sort.Sort(sortableThings(hostsSortedByConflicts))
	var groups [][]placement.Host
	lastSeenConflict := -1
	for _, host := range hostsSortedByConflicts {
		if !allowConflict && host.weight > 0 {
			break
		}
		if host.weight > lastSeenConflict {
			lastSeenConflict = host.weight
			groups = append(groups, []placement.Host{})
		}
		if lastSeenConflict == host.weight {
			groups[len(groups)-1] = append(groups[len(groups)-1], host.value.(placement.Host))
		}
	}
	return groups
}

func fillWeight(groups [][]placement.Host, targetWeight int) ([]placement.Host, int) {
	var (
		result       []placement.Host
		hostsInGroup []placement.Host
	)
	for _, group := range groups {
		sort.Sort(placement.ByIDAscending(group))
		hostsInGroup, targetWeight = knapsack(group, targetWeight)
		result = append(result, hostsInGroup...)
		if targetWeight <= 0 {
			break
		}
	}
	return result, targetWeight
}

func knapsack(hosts []placement.Host, targetWeight int) ([]placement.Host, int) {
	totalWeight := 0
	for _, host := range hosts {
		totalWeight += int(host.Weight())
	}
	if totalWeight <= targetWeight {
		return hosts[:], targetWeight - totalWeight
	}
	// totalWeight > targetWeight, there is a combination of hosts to meet targetWeight for sure
	// we do dp until totalWeight rather than targetWeight here because we need to cover the targetWeight
	// which is a little bit different than the knapsack problem that goes
	weights := make([]int, totalWeight+1)
	combination := make([][]placement.Host, totalWeight+1)

	// dp: weights[i][j] = max(weights[i-1][j], weights[i-1][j-host.Weight] + host.Weight)
	// when there are multiple combination to reach a target weight, we prefer the one with less hosts
	for i := range hosts {
		// this loop needs to go from len to 1 because updating weights[] is being updated in place
		for j := totalWeight; j >= 1; j-- {
			weight := int(hosts[i].Weight())
			if j-weight < 0 {
				continue
			}
			newWeight := weights[j-weight] + weight
			if newWeight > weights[j] {
				weights[j] = weights[j-weight] + weight
				combination[j] = append(combination[j-weight], hosts[i])
			} else if newWeight == weights[j] {
				// if can reach same weight, find a combination with less hosts
				if len(combination[j-weight])+1 < len(combination[j]) {
					combination[j] = append(combination[j-weight], hosts[i])
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

func filterZones(p placement.Snapshot, opts placement.Options, candidateHosts []placement.Host) ([]placement.Host, error) {
	if opts.AcrossZones() {
		return candidateHosts, nil
	}

	var validZone string
	for _, hostShards := range p.HostShards() {
		if validZone == "" {
			validZone = hostShards.Host().Zone()
			continue
		}
		if validZone != hostShards.Host().Zone() {
			return nil, errDisableAcrossZones
		}
	}

	validHosts := make([]placement.Host, 0, len(candidateHosts))
	for _, host := range candidateHosts {
		if validZone == host.Zone() {
			validHosts = append(validHosts, host)
		}
	}
	return validHosts, nil
}

func buildRackHostMap(candidateHosts []placement.Host) map[string][]placement.Host {
	result := make(map[string][]placement.Host, len(candidateHosts))
	for _, host := range candidateHosts {
		if _, exist := result[host.Rack()]; !exist {
			result[host.Rack()] = make([]placement.Host, 0)
		}
		result[host.Rack()] = append(result[host.Rack()], host)
	}
	return result
}

func buildRackHostMapFromHostShards(hosts []placement.HostShards) map[string][]placement.Host {
	hs := make([]placement.Host, len(hosts))
	for i, host := range hosts {
		hs[i] = host.Host()
	}
	return buildRackHostMap(hs)
}

func (ps placementService) validateInitHosts(hosts []placement.Host) error {
	if ps.options.AcrossZones() {
		return nil
	}

	var zone string
	for _, hostShards := range hosts {
		if zone == "" {
			zone = hostShards.Zone()
			continue
		}
		if zone != hostShards.Zone() {
			return errHostsAcrossZones
		}
	}
	return nil
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
