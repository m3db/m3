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

package cluster

import (
	"github.com/m3db/m3cluster/kv"
)

// A ServiceInstance is a single instance of a service
type ServiceInstance interface {
	Service() string                      // the service implemented by the instance
	SetService(s string) ServiceInstance  // sets the service implemented by the instance
	ID() string                           // ID of the instance
	SetID(id string) ServiceInstance      // sets the ID of the instance
	Zone() string                         // Zone in which the instance resides
	SetZone(z string) ServiceInstance     // sets the zone in which the instance resides
	Endpoint() string                     // Endpoint address for contacting the instance
	SetEndpoint(e string) ServiceInstance // sets the endpoint address for the instance
}

// NewServiceInstance creates a new ServiceInstance
func NewServiceInstance() ServiceInstance { return new(serviceInstance) }

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

// NewAdvertisement creates a new Advertisement
func NewAdvertisement() Advertisement { return new(advertisement) }

// QueryOptions are options to service discovery queries
type QueryOptions interface {
	Zones() []string                         // list of zones to consult. if empty only the local zone will be queried
	SetZones(zones []string) QueryOptions    // sets the list of zones to consult
	IncludeUnhealthy() bool                  // if true, will return unhealthy instances
	SetIncludeUnhealthy(h bool) QueryOptions // sets whether to include unhealthy instances
}

// NewQueryOptions creates new QueryOptions
func NewQueryOptions() QueryOptions { return new(queryOptions) }

// Services provides access to the service topology
type Services interface {
	// Advertise advertises the availability of an instance of a service
	Advertise(ad Advertisement) error

	// Unadvertise indicates a given instance is no longer available
	Unadvertise(service, id string) error

	// QueryInstances returns the list of available instances for a given service
	QueryInstances(service string, opts QueryOptions) ([]ServiceInstance, error)
}

// Client is the base interface into the cluster management system, providing
// access to cluster services
type Client interface {
	// Services returns access to the set of services
	Services() Services

	// KV returns access to the distributed configuration store
	KV() kv.Store
}

type serviceInstance struct {
	id       string
	service  string
	zone     string
	endpoint string
}

func (i *serviceInstance) Service() string                      { return i.service }
func (i *serviceInstance) ID() string                           { return i.id }
func (i *serviceInstance) Zone() string                         { return i.zone }
func (i *serviceInstance) Endpoint() string                     { return i.endpoint }
func (i *serviceInstance) SetService(s string) ServiceInstance  { i.service = s; return i }
func (i *serviceInstance) SetID(id string) ServiceInstance      { i.id = id; return i }
func (i *serviceInstance) SetZone(z string) ServiceInstance     { i.zone = z; return i }
func (i *serviceInstance) SetEndpoint(e string) ServiceInstance { i.endpoint = e; return i }

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

type queryOptions struct {
	zones            []string
	includeUnhealthy bool
}

func (qo *queryOptions) Zones() []string                         { return qo.zones }
func (qo *queryOptions) IncludeUnhealthy() bool                  { return qo.includeUnhealthy }
func (qo *queryOptions) SetZones(z []string) QueryOptions        { qo.zones = z; return qo }
func (qo *queryOptions) SetIncludeUnhealthy(h bool) QueryOptions { qo.includeUnhealthy = h; return qo }
