// Copyright (c) 2018 Uber Technologies, Inc.
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

package testutil

import (
	"fmt"

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/topology"
)

const (
	// SelfID is the string used to represent the ID of the origin node.
	SelfID = "self"
)

// MustNewTopologyMap returns a new topology.Map with provided parameters.
// It's a utility method to make tests easier to write.
func MustNewTopologyMap(
	replicas int,
	assignment map[string][]shard.Shard,
) topology.Map {
	v := NewTopologyView(replicas, assignment)
	m, err := v.Map()
	if err != nil {
		panic(err.Error())
	}
	return m
}

// NewTopologyView returns a new TopologyView with provided parameters.
// It's a utility method to make tests easier to write.
func NewTopologyView(
	replicas int,
	assignment map[string][]shard.Shard,
) TopologyView {
	total := 0
	for _, shards := range assignment {
		total += len(shards)
	}

	return TopologyView{
		HashFn:     sharding.DefaultHashFn(total / replicas),
		Replicas:   replicas,
		Assignment: assignment,
	}
}

// TopologyView represents a snaphshot view of a topology.Map.
type TopologyView struct {
	HashFn     sharding.HashFn
	Replicas   int
	Assignment map[string][]shard.Shard
}

// Map returns the topology.Map corresponding to a TopologyView.
func (v TopologyView) Map() (topology.Map, error) {
	var (
		hostShardSets []topology.HostShardSet
		allShards     []shard.Shard
		unique        = make(map[uint32]struct{})
	)

	for hostID, assignedShards := range v.Assignment {
		shardSet, _ := sharding.NewShardSet(assignedShards, v.HashFn)
		host := topology.NewHost(hostID, fmt.Sprintf("%s:9000", hostID))
		hostShardSet := topology.NewHostShardSet(host, shardSet)
		hostShardSets = append(hostShardSets, hostShardSet)
		for _, s := range assignedShards {
			if _, ok := unique[s.ID()]; !ok {
				unique[s.ID()] = struct{}{}
				uniqueShard := shard.NewShard(s.ID()).SetState(shard.Available)
				fmt.Println("marking shard available 3")
				allShards = append(allShards, uniqueShard)
			}
		}
	}

	shardSet, err := sharding.NewShardSet(allShards, v.HashFn)
	if err != nil {
		return nil, err
	}

	opts := topology.NewStaticOptions().
		SetHostShardSets(hostShardSets).
		SetReplicas(v.Replicas).
		SetShardSet(shardSet)

	return topology.NewStaticMap(opts)
}

// HostShardStates is a human-readable way of describing an initial state topology
// on a host-by-host basis.
type HostShardStates map[string][]shard.Shard

// NewStateSnapshot creates a new initial topology state snapshot using HostShardStates
// as input.
func NewStateSnapshot(numMajorityReplicas int, hostShardStates HostShardStates) *topology.StateSnapshot {
	topoState := &topology.StateSnapshot{
		Origin:           topology.NewHost(SelfID, "127.0.0.1"),
		MajorityReplicas: numMajorityReplicas,
		ShardStates:      make(map[topology.ShardID]map[topology.HostID]topology.HostShardState),
	}

	for host, shards := range hostShardStates {
		for _, shard := range shards {
			hostShardStates, ok := topoState.ShardStates[topology.ShardID(shard.ID())]
			if !ok {
				hostShardStates = make(map[topology.HostID]topology.HostShardState)
			}

			hostShardStates[topology.HostID(host)] = topology.HostShardState{
				Host:       topology.NewHost(host, host+"address"),
				ShardState: shard.State(),
			}
			topoState.ShardStates[topology.ShardID(shard.ID())] = hostShardStates
		}
	}

	return topoState
}
