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
	"github.com/m3db/m3db/sharding"
)

// Host is a container of a host in a topology
type Host interface {
	// Address returns the address of the host
	Address() string
}

// HostShardSet is a container for a host and corresponding shard set
type HostShardSet interface {
	// Host returns the host
	Host() Host

	// ShardSet returns the shard set owned by the host
	ShardSet() sharding.ShardSet
}

// Type is a type of topology that can create new instances of it's type
type Type interface {
	// Create will return a new topology of the topology type
	Create() (Topology, error)

	// Options will return the topology options associated with the topology type
	Options() TypeOptions
}

// Topology is a container of a topology map and disseminates topology map changes
type Topology interface {
	// Get the topology map
	Get() Map

	// Get and subscribe to updates for the topology map
	GetAndSubscribe(ch chan<- Map) Map

	// Close will close the topology
	Close() error
}

// Map describes a topology
type Map interface {
	// Hosts returns all hosts in the map
	Hosts() []Host

	// HostsLen returns the length of all hosts in the map
	HostsLen() int

	// ShardScheme returns the shard scheme for the topology
	ShardScheme() sharding.ShardScheme

	// Route will route a given ID to a shard and a set of hosts
	Route(id string) (uint32, []Host, error)

	// RouteForEach will route a given ID to a shard then execute a
	// function for each host in the set of routed hosts
	RouteForEach(id string, forEachFn RouteForEachFn) error

	// RouteShard will route a given shard to a set of hosts
	RouteShard(shard uint32) ([]Host, error)

	// RouteShardForEach will route a given shard and execute
	// a function for each host in the set of routed hosts
	RouteShardForEach(shard uint32, forEachFn RouteForEachFn) error

	// Replicas returns the number of replicas in the topology
	Replicas() int

	// MajorityReplicas returns the number of replicas to establish majority in the topology
	MajorityReplicas() int
}

// RouteForEachFn is a function to execute for each routed to host
type RouteForEachFn func(idx int, host Host)

// ConsistencyLevel is the consistency level for cluster operations
type ConsistencyLevel int

const (
	consistencyLevelNone ConsistencyLevel = iota

	// ConsistencyLevelOne corresponds to a single node participating
	// for an operation to succeed
	ConsistencyLevelOne

	// ConsistencyLevelMajority corresponds to the majority of nodes participating
	// for an operation to succeed
	ConsistencyLevelMajority

	// ConsistencyLevelAll corresponds to all nodes participating
	// for an operation to succeed
	ConsistencyLevelAll
)

// String returns the consistency level as a string
func (l ConsistencyLevel) String() string {
	switch l {
	case ConsistencyLevelOne:
		return "ConsistencyLevelOne"
	case ConsistencyLevelMajority:
		return "ConsistencyLevelMajority"
	case ConsistencyLevelAll:
		return "ConsistencyLevelAll"
	}
	return "ConsistencyLevelNone"
}

// TypeOptions is a set of static topology type options
type TypeOptions interface {
	// Validate validates the options
	Validate() error

	// ShardScheme sets the shardScheme
	ShardScheme(value sharding.ShardScheme) TypeOptions

	// GetShardScheme returns the shardScheme
	GetShardScheme() sharding.ShardScheme

	// Replicas sets the replicas
	Replicas(value int) TypeOptions

	// GetReplicas returns the replicas
	GetReplicas() int

	// HostShardSets sets the hostShardSets
	HostShardSets(value []HostShardSet) TypeOptions

	// GetHostShardSets returns the hostShardSets
	GetHostShardSets() []HostShardSet
}
