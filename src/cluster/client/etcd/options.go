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

package etcd

import (
	"errors"
	"time"

	"github.com/m3db/m3x/instrument"
)

// Options is the Options to create a config service client
type Options interface {
	Env() string
	SetEnv(e string) Options

	Zone() string
	SetZone(z string) Options

	Service() string
	SetService(id string) Options

	CacheDir() string
	SetCacheDir(dir string) Options

	ServiceInitTimeout() time.Duration
	SetServiceInitTimeout(timeout time.Duration) Options

	Clusters() []Cluster
	SetClusters(clusters []Cluster) Options
	ClusterForZone(z string) (Cluster, bool)

	InstrumentOptions() instrument.Options
	SetInstrumentOptions(iopts instrument.Options) Options

	Validate() error
}

// Cluster defines the configuration for a etcd cluster
type Cluster interface {
	Zone() string
	SetZone(z string) Cluster

	Endpoints() []string
	SetEndpoints(endpoints []string) Cluster
}

// NewOptions creates a set of Options
func NewOptions() Options {
	return options{iopts: instrument.NewOptions()}
}

type options struct {
	env                string
	zone               string
	service            string
	cacheDir           string
	serviceInitTimeout time.Duration
	clusters           map[string]Cluster
	iopts              instrument.Options
}

func (o options) Validate() error {
	if o.service == "" {
		return errors.New("invalid options, no service name set")
	}

	if len(o.clusters) == 0 {
		return errors.New("invalid options, no etcd clusters set")
	}

	if o.iopts == nil {
		return errors.New("invalid options, no instrument options set")
	}

	return nil
}

func (o options) Env() string {
	return o.env
}

func (o options) SetEnv(e string) Options {
	o.env = e
	return o
}

func (o options) Zone() string {
	return o.zone
}

func (o options) SetZone(z string) Options {
	o.zone = z
	return o
}

func (o options) ServiceInitTimeout() time.Duration {
	return o.serviceInitTimeout
}

func (o options) SetServiceInitTimeout(timeout time.Duration) Options {
	o.serviceInitTimeout = timeout
	return o
}

func (o options) CacheDir() string {
	return o.cacheDir
}

func (o options) SetCacheDir(dir string) Options {
	o.cacheDir = dir
	return o
}

func (o options) Service() string {
	return o.service
}

func (o options) SetService(id string) Options {
	o.service = id
	return o
}

func (o options) Clusters() []Cluster {
	res := make([]Cluster, 0, len(o.clusters))
	for _, c := range o.clusters {
		res = append(res, c)
	}
	return res
}

func (o options) SetClusters(clusters []Cluster) Options {
	o.clusters = make(map[string]Cluster, len(clusters))
	for _, c := range clusters {
		o.clusters[c.Zone()] = c
	}
	return o
}

func (o options) ClusterForZone(z string) (Cluster, bool) {
	c, ok := o.clusters[z]
	return c, ok
}

func (o options) InstrumentOptions() instrument.Options {
	return o.iopts
}

func (o options) SetInstrumentOptions(iopts instrument.Options) Options {
	o.iopts = iopts
	return o
}

type cluster struct {
	zone      string
	endpoints []string
}

// NewCluster creates a Cluster
func NewCluster() Cluster {
	return cluster{}
}

func (c cluster) Zone() string {
	return c.zone
}

func (c cluster) SetZone(z string) Cluster {
	c.zone = z
	return c
}

func (c cluster) Endpoints() []string {
	return c.endpoints
}

func (c cluster) SetEndpoints(endpoints []string) Cluster {
	c.endpoints = endpoints
	return c
}
