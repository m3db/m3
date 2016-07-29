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

	"github.com/m3db/m3db/interfaces/m3db"
)

var (
	errUnownedShard = errors.New("unowned shard")
)

type staticTopologyType struct {
	opts m3db.StaticTopologyTypeOptions
}

// NewStaticTopologyType creates a new static topology type
func NewStaticTopologyType(opts m3db.StaticTopologyTypeOptions) m3db.TopologyType {
	return &staticTopologyType{opts}
}

func (t *staticTopologyType) Create() (m3db.Topology, error) {
	if err := t.opts.Validate(); err != nil {
		return nil, err
	}
	return newStaticTopology(t.opts), nil
}

func (t *staticTopologyType) Options() m3db.TopologyTypeOptions {
	return t.opts
}

type staticTopology struct {
	topologyMap staticTopologyMap
}

func newStaticTopology(opts m3db.StaticTopologyTypeOptions) m3db.Topology {
	return &staticTopology{topologyMap: newStaticTopologyMap(opts)}
}

func (t *staticTopology) Get() m3db.TopologyMap {
	return &t.topologyMap
}

func (t *staticTopology) GetAndSubscribe(subscriber m3db.TopologySubscriber) m3db.TopologyMap {
	// Topology is static, ignore the subscription channel
	return &t.topologyMap
}

func (t *staticTopology) PostUpdate(update m3db.TopologyUpdate) {
	// Topology is static, ignore the update
}

func (t *staticTopology) Close() error {
	return nil
}

type staticTopologyMap struct {
	shardScheme         m3db.ShardScheme
	orderedHosts        []m3db.Host
	hostsByShard        [][]m3db.Host
	orderedHostsByShard [][]orderedHost
	replicas            int
	majority            int
}

func newStaticTopologyMap(opts m3db.StaticTopologyTypeOptions) staticTopologyMap {
	totalShards := len(opts.GetShardScheme().All().Shards())
	hostShardSets := opts.GetHostShardSets()
	topoMap := staticTopologyMap{
		shardScheme:         opts.GetShardScheme(),
		orderedHosts:        make([]m3db.Host, 0, len(hostShardSets)),
		hostsByShard:        make([][]m3db.Host, totalShards),
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
	host m3db.Host
}

func (t *staticTopologyMap) Hosts() []m3db.Host {
	return t.orderedHosts
}

func (t *staticTopologyMap) HostsLen() int {
	return len(t.orderedHosts)
}

func (t *staticTopologyMap) ShardScheme() m3db.ShardScheme {
	return t.shardScheme
}

func (t *staticTopologyMap) ShardAssignments() m3db.ShardAssignments {
	// TODO(xichen): construct the static shard assignments here.
	return nil
}

func (t *staticTopologyMap) Route(id string) (uint32, []m3db.Host, error) {
	shard := t.shardScheme.Shard(id)
	if int(shard) >= len(t.hostsByShard) {
		return shard, nil, errUnownedShard
	}
	return shard, t.hostsByShard[shard], nil
}

func (t *staticTopologyMap) RouteForEach(id string, forEachFn m3db.RouteForEachFn) error {
	return t.RouteShardForEach(t.shardScheme.Shard(id), forEachFn)
}

func (t *staticTopologyMap) RouteShard(shard uint32) ([]m3db.Host, error) {
	if int(shard) >= len(t.hostsByShard) {
		return nil, errUnownedShard
	}
	return t.hostsByShard[shard], nil
}

func (t *staticTopologyMap) RouteShardForEach(shard uint32, forEachFn m3db.RouteForEachFn) error {
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
