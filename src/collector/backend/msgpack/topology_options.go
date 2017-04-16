// Copyright (c) 2017 Uber Technologies, Inc.
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

package msgpack

import (
	"time"

	"github.com/m3db/m3cluster/kv"

	"github.com/spaolacci/murmur3"
)

const (
	defaultTopologyKey      = "/topology"
	defaultInitWatchTimeout = 10 * time.Second
)

// HashGenFn generates hash functions based on the number of shards.
type HashGenFn func(numShards int) HashFn

// HashFn hashes a metric id to a shard.
type HashFn func(id []byte) uint32

// TopologyOptions provide a set of topology options.
type TopologyOptions interface {
	// SetTopologyKey sets the kv key to watch for topology changes.
	SetTopologyKey(value string) TopologyOptions

	// TopologyKey returns the kv key to watch for topology changes.
	TopologyKey() string

	// SetInitWatchTimeout sets the initial watch timeout.
	SetInitWatchTimeout(value time.Duration) TopologyOptions

	// InitWatchTimeout returns the initial watch timeout.
	InitWatchTimeout() time.Duration

	// SetKVStore sets the kv store.
	SetKVStore(store kv.Store) TopologyOptions

	// KVStore returns the kv store.
	KVStore() kv.Store

	// SetHashGenFn sets the hash generating function.
	SetHashGenFn(value HashGenFn) TopologyOptions

	// HashGenFn returns the hash generating function.
	HashGenFn() HashGenFn
}

type topologyOptions struct {
	topologyKey      string
	initWatchTimeout time.Duration
	kvStore          kv.Store
	hashGenFn        HashGenFn
}

// NewTopologyOptions create a new set of topology options.
func NewTopologyOptions() TopologyOptions {
	return &topologyOptions{
		topologyKey:      defaultTopologyKey,
		initWatchTimeout: defaultInitWatchTimeout,
		hashGenFn:        defaultHashGen,
	}
}

func (o *topologyOptions) SetTopologyKey(value string) TopologyOptions {
	opts := *o
	opts.topologyKey = value
	return &opts
}

func (o *topologyOptions) TopologyKey() string {
	return o.topologyKey
}

func (o *topologyOptions) SetInitWatchTimeout(value time.Duration) TopologyOptions {
	opts := *o
	opts.initWatchTimeout = value
	return &opts
}

func (o *topologyOptions) InitWatchTimeout() time.Duration {
	return o.initWatchTimeout
}

func (o *topologyOptions) SetKVStore(value kv.Store) TopologyOptions {
	opts := *o
	opts.kvStore = value
	return &opts
}

func (o *topologyOptions) KVStore() kv.Store {
	return o.kvStore
}

func (o *topologyOptions) SetHashGenFn(value HashGenFn) TopologyOptions {
	opts := *o
	opts.hashGenFn = value
	return &opts
}

func (o *topologyOptions) HashGenFn() HashGenFn {
	return o.hashGenFn
}

func defaultHashGen(numShards int) HashFn {
	return func(id []byte) uint32 {
		return murmur3.Sum32(id) % uint32(numShards)
	}
}
