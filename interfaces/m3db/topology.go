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

package m3db

// Host is a container of a host in a topology
type Host interface {
	// Address returns the address of the host
	Address() string
}

// TopologyType is a type of topology that can create new instances of it's type
type TopologyType interface {
	// Create will return a new topology of the topology type
	Create() (Topology, error)
}

// Topology is a container of a topology map and disseminates topology map changes
type Topology interface {
	// Get the topology map
	Get() TopologyMap

	// Get and subscribe to updates for the topology map
	GetAndSubscribe(ch chan<- TopologyMap) TopologyMap

	// Close will close the topology
	Close() error
}

// TopologyMap describes a topology
type TopologyMap interface {
	// Hosts returns all hosts in the map
	Hosts() []Host

	// HostsLen returns the length of all hosts in the map
	HostsLen() int

	// ShardScheme returns the shard scheme for the topology
	ShardScheme() ShardScheme

	// Route will route a given ID to a shard and a set of hosts
	Route(id string) (uint32, []Host, error)

	// RouteForEach will route a given ID to a shard, execute a
	// route function then execute a function for each host
	// in the set of routed hosts
	RouteForEach(id string, shardFn RouteShardFn, forEachFn RouteForEachFn) error

	// RouteShard will route a given shard to a set of hosts
	RouteShard(shard uint32) ([]Host, error)

	// RouteShardForEach will route a given shard and execute
	// a function for each host in the set of routed hosts
	RouteShardForEach(shard uint32, forEachFn RouteForEachFn) error

	// Replicas returns the number of replicas in the topology
	Replicas() int
}

// RouteShardFn is a function to execute to notify of shard routed to
type RouteShardFn func(shard uint32)

// RouteForEachFn is a function to execute for each routed to host
type RouteForEachFn func(idx int, host Host)
