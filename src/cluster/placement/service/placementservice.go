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
	"sort"

	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/placement/algo"
)

var (
	errInvalidShardLen = errors.New("shardLen should be greater than zero")
	errHostAbsent      = errors.New("could not remove or replace a host that does not exist")
	errNoValidHost     = errors.New("no valid host in the candidate list")
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

func (ps placementService) BuildInitialPlacement(service string, hosts []placement.Host, shardLen int, rf int) error {
	if shardLen <= 0 {
		return errInvalidShardLen
	}

	ids := make([]uint32, shardLen)
	for i := 0; i < shardLen; i++ {
		ids[i] = uint32(i)
	}

	var s placement.Snapshot
	var err error
	for i := 0; i < rf; i++ {
		if i == 0 {
			s, err = ps.algo.BuildInitialPlacement(hosts, ids)
		} else {
			s, err = ps.algo.AddReplica(s)
		}
		if err != nil {
			return err
		}
	}
	return ps.ss.SaveSnapshotForService(service, s)
}

func (ps placementService) AddReplica(service string) error {
	var s placement.Snapshot
	var err error
	if s, err = ps.Snapshot(service); err != nil {
		return err
	}

	if s, err = ps.algo.AddReplica(s); err != nil {
		return err
	}
	return ps.ss.SaveSnapshotForService(service, s)
}

func (ps placementService) AddHost(service string, candidateHosts []placement.Host) error {
	var s placement.Snapshot
	var err error
	if s, err = ps.Snapshot(service); err != nil {
		return err
	}
	var addingHost placement.Host
	if addingHost, err = findAddingHost(s, candidateHosts); err != nil {
		return err
	}

	if s, err = ps.algo.AddHost(s, addingHost); err != nil {
		return err
	}
	return ps.ss.SaveSnapshotForService(service, s)
}

func (ps placementService) RemoveHost(service string, host placement.Host) error {
	var s placement.Snapshot
	var err error
	if s, err = ps.Snapshot(service); err != nil {
		return err
	}

	if s.HostShard(host.ID()) == nil {
		return errHostAbsent
	}

	if s, err = ps.algo.RemoveHost(s, host); err != nil {
		return err
	}
	return ps.ss.SaveSnapshotForService(service, s)
}

func (ps placementService) ReplaceHost(service string, leavingHost placement.Host, candidateHosts []placement.Host) error {
	var s placement.Snapshot
	var err error
	if s, err = ps.Snapshot(service); err != nil {
		return err
	}

	leavingHostShard := s.HostShard(leavingHost.ID())
	if leavingHostShard == nil {
		return errHostAbsent
	}

	var addingHost placement.Host
	if addingHost, err = ps.findReplaceHost(s, candidateHosts, leavingHostShard); err != nil {
		return err
	}

	if s, err = ps.algo.ReplaceHost(s, leavingHost, addingHost); err != nil {
		return err
	}
	return ps.ss.SaveSnapshotForService(service, s)
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

type rackLen struct {
	rack string
	len  int
}

type rackLens []rackLen

func (rls rackLens) Len() int {
	return len(rls)
}

func (rls rackLens) Less(i, j int) bool {
	return rls[i].len < rls[j].len
}

func (rls rackLens) Swap(i, j int) {
	rls[i], rls[j] = rls[j], rls[i]
}

func findAddingHost(s placement.Snapshot, candidateHosts []placement.Host) (placement.Host, error) {
	// filter out already existing hosts
	candidateHosts = getNewHostsToPlacement(s, candidateHosts)

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
	rackLens := make(rackLens, 0, len(placementRackHostMap))
	for rack, hss := range placementRackHostMap {
		rackLens = append(rackLens, rackLen{rack: rack, len: len(hss)})
	}
	sort.Sort(rackLens)

	for _, rackLen := range rackLens {
		if hs, exist := candidateRackHostMap[rackLen.rack]; exist {
			for _, host := range hs {
				return host, nil
			}
		}
	}
	// no host in the candidate hosts can be added to the placement
	return nil, errNoValidHost
}

func (ps placementService) findReplaceHost(s placement.Snapshot, candidateHosts []placement.Host, leaving placement.HostShards) (placement.Host, error) {
	// filter out already existing hosts
	candidateHosts = getNewHostsToPlacement(s, candidateHosts)

	if len(candidateHosts) == 0 {
		return nil, errNoValidHost
	}
	// build rackHostMap from candidate hosts
	candidateRackHostMap := buildRackHostMap(candidateHosts)

	// if there is a host from the same rack can be added, return it.
	if hs, exist := candidateRackHostMap[leaving.Host().Rack()]; exist {
		return hs[0], nil
	}

	return ps.findHostWithRackConflictCheck(s, candidateRackHostMap, leaving)
}

func (ps placementService) findHostWithRackConflictCheck(p placement.Snapshot, rackHostMap map[string][]placement.Host, leaving placement.HostShards) (placement.Host, error) {
	// otherwise sort the candidate hosts by the number of conflicts
	ph := algo.NewPlacementHelper(ps.options, p)

	rackLens := make(rackLens, 0, len(rackHostMap))
	for rack := range rackHostMap {
		rackConflicts := 0
		for _, shard := range leaving.Shards() {
			if !ph.HasNoRackConflict(shard, leaving, rack) {
				rackConflicts++
			}
		}

		rackLens = append(rackLens, rackLen{rack: rack, len: rackConflicts})
	}
	sort.Sort(rackLens)
	if !ps.options.LooseRackCheck() {
		rackLens = getNoConflictRacks(rackLens)
	}
	if len(rackLens) > 0 {
		return rackHostMap[rackLens[0].rack][0], nil
	}
	return nil, errNoValidHost
}

func getNoConflictRacks(rls rackLens) rackLens {
	for i, r := range rls {
		if r.len > 0 {
			return rls[:i]
		}
	}
	return rls
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
