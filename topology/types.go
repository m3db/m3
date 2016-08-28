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
	"fmt"
	"time"

	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3db/instrument"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3x/close"
	"github.com/m3db/m3x/retry"
	"github.com/m3db/m3x/watch"
)

// Host is a container of a host in a topology
type Host interface {
	fmt.Stringer

	// ID is the identifier of the host
	ID() string

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

// Initializer can init new instances of Topology
type Initializer interface {
	// Init will return a new topology
	Init() (Topology, error)
}

// Topology is a container of a topology map and disseminates topology map changes
type Topology interface {
	xclose.SimpleCloser

	// Get the topology map
	Get() Map
	// Get and subscribe to updates for the topology map
	GetAndSubscribe() (Map, xwatch.Watch, error)
}

// Map describes a topology
type Map interface {
	// Hosts returns all hosts in the map
	Hosts() []Host

	// HostsLen returns the length of all hosts in the map
	HostsLen() int

	// ShardSet returns the shard set for the topology
	ShardSet() sharding.ShardSet

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

// StaticOptions is a set of options for static topology
type StaticOptions interface {
	// Validate validates the options
	Validate() error

	// ShardSet sets the ShardSet
	ShardSet(value sharding.ShardSet) StaticOptions

	// GetShardSet returns the ShardSet
	GetShardSet() sharding.ShardSet

	// Replicas sets the replicas
	Replicas(value int) StaticOptions

	// GetReplicas returns the replicas
	GetReplicas() int

	// HostShardSets sets the hostShardSets
	HostShardSets(value []HostShardSet) StaticOptions

	// GetHostShardSets returns the hostShardSets
	GetHostShardSets() []HostShardSet
}

// DynamicOptions is a set of options for dynamic topology
type DynamicOptions interface {
	// Validate validates the options
	Validate() error

	// ConfigServiceClient sets the client of ConfigService
	ConfigServiceClient(c client.Client) DynamicOptions

	// GetConfigServiceClient returns the client of ConfigService
	GetConfigServiceClient() client.Client

	// Service returns the service name to query ConfigService
	Service(s string) DynamicOptions

	// GetService returns the service name to query ConfigService
	GetService() string

	// QueryOptions returns the ConfigService query options
	QueryOptions(value services.QueryOptions) DynamicOptions

	// GetQueryOptions returns the ConfigService query options
	GetQueryOptions() services.QueryOptions

	// RetryOptions sets the retry options
	RetryOptions(value xretry.Options) DynamicOptions

	// GetRetryOptions returns the retry options
	GetRetryOptions() xretry.Options

	// InstrumentOptions sets the instrumentation options
	InstrumentOptions(value instrument.Options) DynamicOptions

	// GetInstrumentOptions returns the instrumentation options
	GetInstrumentOptions() instrument.Options

	// InitTimeout sets the waiting time for dynamic topology to be initialized
	InitTimeout(value time.Duration) DynamicOptions

	// GetInitTimeout returns the waiting time for dynamic topology to be initialized
	GetInitTimeout() time.Duration

	// HashGen sets the HashGen function
	HashGen(h sharding.HashGen) DynamicOptions

	// GetHashGen returns HashGen function
	GetHashGen() sharding.HashGen
}
