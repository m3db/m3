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

package topology

import (
	"errors"

	"github.com/m3db/m3db/sharding"
)

var (
	errUnownedShard = errors.New("unowned shard")
)

type staticTopologyType struct {
	opts TopologyTypeOptions
}

// NewStaticTopologyType creates a new static topology type
func NewStaticTopologyType(opts TopologyTypeOptions) TopologyType {
	return &staticTopologyType{opts}
}

func (t *staticTopologyType) Create() (Topology, error) {
	if err := t.opts.Validate(); err != nil {
		return nil, err
	}
	return newStaticTopology(t.opts), nil
}

func (t *staticTopologyType) Options() TopologyTypeOptions {
	return t.opts
}

type staticTopology struct {
	topologyMap staticTopologyMap
}

func newStaticTopology(opts TopologyTypeOptions) Topology {
	return &staticTopology{topologyMap: newStaticTopologyMap(opts)}
}

func (t *staticTopology) Get() TopologyMap {
	return &t.topologyMap
}

func (t *staticTopology) GetAndSubscribe(ch chan<- TopologyMap) TopologyMap {
	// Topology is static, ignore the subscription channel
	return &t.topologyMap
}

func (t *staticTopology) Close() error {
	return nil
}

type staticTopologyMap struct {
	shardScheme         sharding.ShardScheme
	orderedHosts        []Host
	hostsByShard        [][]Host
	orderedHostsByShard [][]orderedHost
	replicas            int
	majority            int
}

func newStaticTopologyMap(opts TopologyTypeOptions) staticTopologyMap {
	totalShards := len(opts.GetShardScheme().All().Shards())
	hostShardSets := opts.GetHostShardSets()
	topoMap := staticTopologyMap{
		shardScheme:         opts.GetShardScheme(),
		orderedHosts:        make([]Host, 0, len(hostShardSets)),
		hostsByShard:        make([][]Host, totalShards),
		orderedHostsByShard: make([][]orderedHost, totalShards),
		replicas:            opts.GetReplicas(),
		majority:            majority(opts.GetReplicas()),
	}

	for idx, hostShardSet := range hostShardSets {
		host := hostShardSet.Host()
		topoMap.orderedHosts = append(topoMap.orderedHosts, host)
		for _, shard := range hostShardSet.ShardSet().Shards() {
			topoMap.hostsByShard[shard] = append(topoMap.hostsByShard[shard], host)
			topoMap.orderedHostsByShard[shard] = append(topoMap.orderedHostsByShard[shard], orderedHost{
				idx:  idx,
				host: host,
			})
		}
	}

	return topoMap
}

type orderedHost struct {
	idx  int
	host Host
}

func (t *staticTopologyMap) Hosts() []Host {
	return t.orderedHosts
}

func (t *staticTopologyMap) HostsLen() int {
	return len(t.orderedHosts)
}

func (t *staticTopologyMap) ShardScheme() sharding.ShardScheme {
	return t.shardScheme
}

func (t *staticTopologyMap) Route(id string) (uint32, []Host, error) {
	shard := t.shardScheme.Shard(id)
	if int(shard) >= len(t.hostsByShard) {
		return shard, nil, errUnownedShard
	}
	return shard, t.hostsByShard[shard], nil
}

func (t *staticTopologyMap) RouteForEach(id string, forEachFn RouteForEachFn) error {
	return t.RouteShardForEach(t.shardScheme.Shard(id), forEachFn)
}

func (t *staticTopologyMap) RouteShard(shard uint32) ([]Host, error) {
	if int(shard) >= len(t.hostsByShard) {
		return nil, errUnownedShard
	}
	return t.hostsByShard[shard], nil
}

func (t *staticTopologyMap) RouteShardForEach(shard uint32, forEachFn RouteForEachFn) error {
	if int(shard) >= len(t.orderedHostsByShard) {
		return errUnownedShard
	}
	orderedHosts := t.orderedHostsByShard[shard]
	for i := range orderedHosts {
		forEachFn(orderedHosts[i].idx, orderedHosts[i].host)
	}
	return nil
}

func (t *staticTopologyMap) Replicas() int {
	return t.replicas
}

func (t *staticTopologyMap) MajorityReplicas() int {
	return t.majority
}
