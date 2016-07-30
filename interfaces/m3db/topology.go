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
	// ID returns the unique id of the host
	ID() string

	// Address returns the address of the host
	Address() string
}

// HostShardSet is a container for a host and corresponding shard set
type HostShardSet interface {
	// Host returns the host
	Host() Host

	// ShardSet returns the shard set owned by the host
	ShardSet() ShardSet
}

// TopologyType is a type of topology that can create new instances of it's type
type TopologyType interface {
	// Create will return a new topology of the topology type
	Create() (Topology, error)

	// Options will return the topology options associated with the topology type
	Options() TopologyTypeOptions
}

// TopologySubscriber is a receive channel for topology updates
type TopologySubscriber chan<- TopologyMap

// Topology is a container of a topology map and disseminates topology map changes
type Topology interface {
	// Get and subscribe to updates for the topology map
	GetAndSubscribe(subscriber TopologySubscriber) TopologyMap

	// PostUpdate posts a topology update
	PostUpdate(update TopologyUpdate)

	// Close will close the topology
	Close() error
}

type TopologyUpdateType int

const (
	ShardAssignmentUpdate TopologyUpdateType = iota
)

// TopologyUpdate encapsulates information about a topology update.
type TopologyUpdate interface {
	// Type returns the type of the topology update
	Type() TopologyUpdateType

	// Data returns the data related to the update
	Data() interface{}
}

// TopologyClient is the client interface that supports subscribing to topology changes.
type TopologyClient interface {

	// AddSubscriber adds a subscriber to topology change notifications produced
	// by an external source (e.g., the cluster configuration service).
	AddSubscriber(subscriber TopologySubscriber) TopologyMap

	// RemoveSubscriber removes a subscriber from the list of registered subscribers.
	RemoveSubscriber(subscriber TopologySubscriber)

	// PostUpdate posts an topology update.
	PostUpdate(update TopologyUpdate)

	// Close stops the background goroutine that polls the external source for topology updates.
	Close() error
}

type NewTopologyClientFn func() TopologyClient

type ShardState int

const (
	ShardUnassigned ShardState = iota
	ShardInitializing
	ShardAvailable
)

// ShardAssignment represents a shard assignment.
type ShardAssignment interface {
	// ShardID returns the shard id
	ShardID() uint32

	// ShardState returns the shard state.
	ShardState() ShardState
}

// ShardAssignments encapsulates shard assignment information across hosts.
type ShardAssignments interface {
	// GetAssignmentsFor returns the shard assignments for a given host.
	GetAssignmentsFor(host Host) []ShardAssignment

	// AddAssignmentsFor adds the shard assignments for a given host.
	AddAssignmentsFor(host Host, assignments []ShardAssignment)
}

// TopologyMap describes a topology
type TopologyMap interface {
	// Hosts returns all hosts in the map
	Hosts() []Host

	// HostsLen returns the length of all hosts in the map
	HostsLen() int

	// ShardScheme returns the shard scheme for the topology
	ShardScheme() ShardScheme

	// ShardAssignments returns the shard assignments across hosts
	ShardAssignments() ShardAssignments

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
