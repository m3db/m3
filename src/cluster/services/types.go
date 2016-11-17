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

package services

import (
	"github.com/m3db/m3cluster/shard"
	xwatch "github.com/m3db/m3x/watch"
)

// Service describes the metadata and instances of a service
type Service interface {
	// Instance returns the service instance with the instance id
	Instance(instanceID string) (ServiceInstance, error)

	// Instances returns the service instances
	Instances() []ServiceInstance

	// SetInstances sets the service instances
	SetInstances(insts []ServiceInstance) Service

	// Replication returns the service replication description or nil if none
	Replication() ServiceReplication

	// SetReplication sets the service replication description or nil if none
	SetReplication(r ServiceReplication) Service

	// Sharding returns the service sharding description or nil if none
	Sharding() ServiceSharding

	// SetSharding sets the service sharding description or nil if none
	SetSharding(s ServiceSharding) Service
}

// ServiceReplication describes the replication of a service
type ServiceReplication interface {
	// Replicas is the count of replicas
	Replicas() int

	// SetReplicas sets the count of replicas
	SetReplicas(r int) ServiceReplication
}

// ServiceSharding describes the sharding of a service
type ServiceSharding interface {
	// NumShards is the number of shards to use for sharding
	NumShards() int

	// SetNumShards sets the number of shards to use for sharding
	SetNumShards(n int) ServiceSharding
}

// ServiceInstance is a single instance of a service
type ServiceInstance interface {
	Service() string                          // the service implemented by the instance
	SetService(s string) ServiceInstance      // sets the service implemented by the instance
	ID() string                               // ID of the instance
	SetID(id string) ServiceInstance          // sets the ID of the instance
	Zone() string                             // Zone in which the instance resides
	SetZone(z string) ServiceInstance         // sets the zone in which the instance resides
	Endpoint() string                         // Endpoint address for contacting the instance
	SetEndpoint(e string) ServiceInstance     // sets the endpoint address for the instance
	Shards() shard.Shards                     // Shards owned by the instance
	SetShards(s shard.Shards) ServiceInstance // sets the Shards assigned to the instance
}

// Advertisement advertises the availability of a given instance of a service
type Advertisement interface {
	ID() string                                  // the ID of the instance being advertised
	SetID(id string) Advertisement               // sets the ID being advertised
	Service() string                             // the service being advertised
	SetService(service string) Advertisement     // sets the service being advertised
	Health() func() error                        // optional health function.  return an error to indicate unhealthy
	SetHealth(health func() error) Advertisement // sets the health function for the advertised instance
	Endpoint() string                            // endpoint exposed by the service
	SetEndpoint(e string) Advertisement          // sets the endpoint exposed by the service
}

// QueryOptions are options to service discovery queries
type QueryOptions interface {
	Zones() []string                         // list of zones to consult. if empty only the local zone will be queried
	SetZones(zones []string) QueryOptions    // sets the list of zones to consult
	IncludeUnhealthy() bool                  // if true, will return unhealthy instances
	SetIncludeUnhealthy(h bool) QueryOptions // sets whether to include unhealthy instances
}

// Services provides access to the service topology
type Services interface {
	// Advertise advertises the availability of an instance of a service
	Advertise(ad Advertisement) error

	// Unadvertise indicates a given instance is no longer available
	Unadvertise(service, id string) error

	// Query returns metadata and a list of available instances for a given service
	Query(service string, opts QueryOptions) (Service, error)

	// Watch returns a watch on metadata and a list of available instances for a given service
	Watch(service string, opts QueryOptions) (xwatch.Watch, error)
}
