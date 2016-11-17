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
		if instance.ID() == instanceID {
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
	id       string
	service  string
	zone     string
	endpoint string
	shards   shard.Shards
}

func (i *serviceInstance) Service() string                          { return i.service }
func (i *serviceInstance) ID() string                               { return i.id }
func (i *serviceInstance) Zone() string                             { return i.zone }
func (i *serviceInstance) Endpoint() string                         { return i.endpoint }
func (i *serviceInstance) Shards() shard.Shards                     { return i.shards }
func (i *serviceInstance) SetService(s string) ServiceInstance      { i.service = s; return i }
func (i *serviceInstance) SetID(id string) ServiceInstance          { i.id = id; return i }
func (i *serviceInstance) SetZone(z string) ServiceInstance         { i.zone = z; return i }
func (i *serviceInstance) SetEndpoint(e string) ServiceInstance     { i.endpoint = e; return i }
func (i *serviceInstance) SetShards(s shard.Shards) ServiceInstance { i.shards = s; return i }

// NewAdvertisement creates a new Advertisement
func NewAdvertisement() Advertisement { return new(advertisement) }

type advertisement struct {
	id       string
	service  string
	endpoint string
	health   func() error
}

func (a *advertisement) ID() string                             { return a.id }
func (a *advertisement) Service() string                        { return a.service }
func (a *advertisement) Endpoint() string                       { return a.endpoint }
func (a *advertisement) Health() func() error                   { return a.health }
func (a *advertisement) SetID(id string) Advertisement          { a.id = id; return a }
func (a *advertisement) SetService(s string) Advertisement      { a.service = s; return a }
func (a *advertisement) SetEndpoint(e string) Advertisement     { a.endpoint = e; return a }
func (a *advertisement) SetHealth(h func() error) Advertisement { a.health = h; return a }

// NewQueryOptions creates new QueryOptions
func NewQueryOptions() QueryOptions { return new(queryOptions) }

type queryOptions struct {
	zones            []string
	includeUnhealthy bool
}

func (qo *queryOptions) Zones() []string                         { return qo.zones }
func (qo *queryOptions) IncludeUnhealthy() bool                  { return qo.includeUnhealthy }
func (qo *queryOptions) SetZones(z []string) QueryOptions        { qo.zones = z; return qo }
func (qo *queryOptions) SetIncludeUnhealthy(h bool) QueryOptions { qo.includeUnhealthy = h; return qo }
