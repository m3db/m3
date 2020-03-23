// Copyright (c) 2020 Uber Technologies, Inc.
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

package memcluster

import (
	"errors"
	"sync"

	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/cluster/services"
)

const (
	_kvPrefix = "_kv"
)

var (
	// assert the interface matches.
	_ client.Client = (*Client)(nil)
)

// Client provides a cluster/client.Client backed by kv/mem transaction store,
// which stores data in memory instead of in etcd.
type Client struct {
	mu          sync.Mutex
	serviceOpts kv.OverrideOptions
	cache       map[cacheKey]kv.TxnStore
}

// New instantiates a client which defaults its stores to the given zone/env/namespace.
func New(serviceOpts kv.OverrideOptions) *Client {
	return &Client{
		serviceOpts: serviceOpts,
		cache:       make(map[cacheKey]kv.TxnStore),
	}
}

// Services constructs a gateway to all cluster services, backed by a mem store.
func (c *Client) Services(opts services.OverrideOptions) (services.Services, error) {
	if opts == nil {
		opts = services.NewOverrideOptions()
	}

	errUnsupported := errors.New("currently unsupported for inMemoryClusterClient")

	kvGen := func(zone string) (kv.Store, error) {
		return c.Store(kv.NewOverrideOptions().SetZone(zone))
	}

	heartbeatGen := func(sid services.ServiceID) (services.HeartbeatService, error) {
		return nil, errUnsupported
	}

	leaderGen := func(sid services.ServiceID, opts services.ElectionOptions) (services.LeaderService, error) {
		return nil, errUnsupported
	}

	return services.NewServices(
		services.NewOptions().
			SetKVGen(kvGen).
			SetHeartbeatGen(heartbeatGen).
			SetLeaderGen(leaderGen).
			SetNamespaceOptions(opts.NamespaceOptions()),
	)
}

// KV returns/constructs a mem backed kv.Store for the default zone/env/namespace.
func (c *Client) KV() (kv.Store, error) {
	return c.TxnStore(kv.NewOverrideOptions())
}

// Txn returns/constructs a mem backed kv.TxnStore for the default zone/env/namespace.
func (c *Client) Txn() (kv.TxnStore, error) {
	return c.TxnStore(kv.NewOverrideOptions())
}

// Store returns/constructs a mem backed kv.Store for the given env/zone/namespace.
func (c *Client) Store(opts kv.OverrideOptions) (kv.Store, error) {
	return c.TxnStore(opts)
}

// TxnStore returns/constructs a mem backed kv.TxnStore for the given env/zone/namespace.
func (c *Client) TxnStore(opts kv.OverrideOptions) (kv.TxnStore, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	opts = mergeOpts(c.serviceOpts, opts)
	key := cacheKey{
		Env:       opts.Environment(),
		Zone:      opts.Zone(),
		Namespace: opts.Namespace(),
	}
	if s, ok := c.cache[key]; ok {
		return s, nil
	}

	store := mem.NewStore()
	c.cache[key] = store
	return store, nil
}

type cacheKey struct {
	Env       string
	Zone      string
	Namespace string
}

func mergeOpts(defaults kv.OverrideOptions, opts kv.OverrideOptions) kv.OverrideOptions {
	if opts.Zone() == "" {
		opts = opts.SetZone(defaults.Zone())
	}

	if opts.Environment() == "" {
		opts = opts.SetEnvironment(defaults.Environment())
	}

	if opts.Namespace() == "" {
		opts = opts.SetNamespace(_kvPrefix)
	}

	return opts
}
