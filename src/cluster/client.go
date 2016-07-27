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

// KVStore provides access to the configuration store
type KVStore interface {
	// Get retrieves the value for the given key, marshalling it into the provided value
	Get(key string, v interface{}) error

	// Put stores the value for the given key
	Put(key string, v interface{}) error
}

// A ServiceInstance is a single instance of a service
type ServiceInstance struct {
	Service  string // the service implemented by the instance
	ID       string // ID of the instance
	Zone     string // Zone in which the instance resides
	Endpoint string // Endpoint address for contacting the instance
}

// Advertisement advertises the availability of a given instance of a service
type Advertisement struct {
	ID       string       // the ID of the instance being advertised
	Service  string       // the service being advertised
	Health   func() error // optional health function.  return an error to indicate unhealthy
	Endpoint string       // endpoint exposed by the service
}

// QueryOptions are options to service discovery queries
type QueryOptions struct {
	Zones            []string // list of zones to consult. if empty only the local zone will be queried
	IncludeUnhealthy bool     // if true, will return unhealthy instances
}

// Services provides access to the service topology
type Services interface {
	// Advertise advertises the availability of an instance of a service
	Advertise(ad Advertisement) error

	// Unadvertise indicates a given instance is no longer available
	Unadvertise(service, id string) error

	// QueryInstances returns the list of available instances for a given service
	QueryInstances(service string, opts *QueryOptions) ([]*ServiceInstance, error)

	// QueryEndpoints returns a list of available endpoints for a given service
	QueryEndpoints(service string, opts *QueryOptions) ([]string, error)
}

// Client is the base interface into the cluster management system, providing
// access to cluster services
type Client interface {
	// Services returns access to the set of services
	Services() Services

	// KV returns access to the distributed configuration store
	KV() KVStore
}
