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
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/m3db/m3cluster/shard"
)

var errInstanceNotFound = errors.New("instance not found")

// NewService creates a new Service
func NewService() Service { return new(service) }

type service struct {
	instances   []ServiceInstance
	replication ServiceReplication
	sharding    ServiceSharding
}

func (s *service) Instance(instanceID string) (ServiceInstance, error) {
	for _, instance := range s.instances {
		if instance.InstanceID() == instanceID {
			return instance, nil
		}
	}
	return nil, errInstanceNotFound
}
func (s *service) Instances() []ServiceInstance                 { return s.instances }
func (s *service) Replication() ServiceReplication              { return s.replication }
func (s *service) Sharding() ServiceSharding                    { return s.sharding }
func (s *service) SetInstances(insts []ServiceInstance) Service { s.instances = insts; return s }
func (s *service) SetReplication(r ServiceReplication) Service  { s.replication = r; return s }
func (s *service) SetSharding(ss ServiceSharding) Service       { s.sharding = ss; return s }

// NewServiceReplication creates a new ServiceReplication
func NewServiceReplication() ServiceReplication { return new(serviceReplication) }

type serviceReplication struct {
	replicas int
}

func (r *serviceReplication) Replicas() int                          { return r.replicas }
func (r *serviceReplication) SetReplicas(rep int) ServiceReplication { r.replicas = rep; return r }

// NewServiceSharding creates a new ServiceSharding
func NewServiceSharding() ServiceSharding { return new(serviceSharding) }

type serviceSharding struct {
	numShards int
}

func (s *serviceSharding) NumShards() int                     { return s.numShards }
func (s *serviceSharding) SetNumShards(n int) ServiceSharding { s.numShards = n; return s }

// NewServiceInstance creates a new ServiceInstance
func NewServiceInstance() ServiceInstance { return new(serviceInstance) }

type serviceInstance struct {
	service  ServiceID
	id       string
	endpoint string
	shards   shard.Shards
}

func (i *serviceInstance) InstanceID() string                       { return i.id }
func (i *serviceInstance) Endpoint() string                         { return i.endpoint }
func (i *serviceInstance) Shards() shard.Shards                     { return i.shards }
func (i *serviceInstance) ServiceID() ServiceID                     { return i.service }
func (i *serviceInstance) SetInstanceID(id string) ServiceInstance  { i.id = id; return i }
func (i *serviceInstance) SetEndpoint(e string) ServiceInstance     { i.endpoint = e; return i }
func (i *serviceInstance) SetShards(s shard.Shards) ServiceInstance { i.shards = s; return i }

func (i *serviceInstance) SetServiceID(service ServiceID) ServiceInstance {
	i.service = service
	return i
}

// NewAdvertisement creates a new Advertisement
func NewAdvertisement() Advertisement { return new(advertisement) }

type advertisement struct {
	id       string
	service  ServiceID
	endpoint string
	health   func() error
}

func (a *advertisement) InstanceID() string                     { return a.id }
func (a *advertisement) ServiceID() ServiceID                   { return a.service }
func (a *advertisement) Endpoint() string                       { return a.endpoint }
func (a *advertisement) Health() func() error                   { return a.health }
func (a *advertisement) SetInstanceID(id string) Advertisement  { a.id = id; return a }
func (a *advertisement) SetServiceID(s ServiceID) Advertisement { a.service = s; return a }
func (a *advertisement) SetEndpoint(e string) Advertisement     { a.endpoint = e; return a }
func (a *advertisement) SetHealth(h func() error) Advertisement { a.health = h; return a }

// NewServiceID creates new ServiceID
func NewServiceID() ServiceID { return new(serviceID) }

type serviceID struct {
	name string
	env  string
	zone string
}

func (sid *serviceID) Name() string                      { return sid.name }
func (sid *serviceID) Environment() string               { return sid.env }
func (sid *serviceID) Zone() string                      { return sid.zone }
func (sid *serviceID) SetName(n string) ServiceID        { sid.name = n; return sid }
func (sid *serviceID) SetEnvironment(e string) ServiceID { sid.env = e; return sid }
func (sid *serviceID) SetZone(z string) ServiceID        { sid.zone = z; return sid }
func (sid *serviceID) String() string {
	return fmt.Sprintf("[name: %s, env: %s, zone: %s]", sid.name, sid.env, sid.zone)
}

// NewQueryOptions creates new QueryOptions
func NewQueryOptions() QueryOptions { return new(queryOptions) }

type queryOptions struct {
	includeUnhealthy bool
}

func (qo *queryOptions) IncludeUnhealthy() bool                  { return qo.includeUnhealthy }
func (qo *queryOptions) SetIncludeUnhealthy(h bool) QueryOptions { qo.includeUnhealthy = h; return qo }

// NewMetadata creates new Metadata
func NewMetadata() Metadata { return new(metadata) }

type metadata struct {
	port              uint32
	livenessInterval  time.Duration
	heartbeatInterval time.Duration
}

func (m *metadata) Port() uint32                     { return m.port }
func (m *metadata) LivenessInterval() time.Duration  { return m.livenessInterval }
func (m *metadata) HeartbeatInterval() time.Duration { return m.heartbeatInterval }
func (m *metadata) SetPort(p uint32) Metadata        { m.port = p; return m }
func (m *metadata) SetLivenessInterval(l time.Duration) Metadata {
	m.livenessInterval = l
	return m
}
func (m *metadata) SetHeartbeatInterval(l time.Duration) Metadata {
	m.heartbeatInterval = l
	return m
}
func (m *metadata) String() string {
	return fmt.Sprintf("[port: %d, livenessInterval: %v, heartbeatInterval: %v]",
		m.port,
		m.livenessInterval,
		m.heartbeatInterval,
	)
}

// PlacementInstances is a slice of instances that can produce a debug string
type PlacementInstances []PlacementInstance

func (i PlacementInstances) String() string {
	if len(i) == 0 {
		return "[]"
	}
	var strs []string
	strs = append(strs, "[\n")
	for _, elem := range i {
		strs = append(strs, "\t"+elem.String()+",\n")
	}
	strs = append(strs, "]")
	return strings.Join(strs, "")
}
