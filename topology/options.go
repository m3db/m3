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
	"errors"
	"fmt"

	"github.com/m3db/m3db/sharding"
)

var (
	errInvalidReplicas                = errors.New("replicas must be equal to or greater than 1")
	errHostShardSetSchemeDoesNotMatch = errors.New("host shard set's shard scheme is not the specified shard scheme")
)

type staticTopologyTypeOptions struct {
	shardScheme   sharding.ShardScheme
	replicas      int
	hostShardSets []HostShardSet
}

// NewStaticTopologyTypeOptions creates a new set of static topology type options
func NewStaticTopologyTypeOptions() TopologyTypeOptions {
	return &staticTopologyTypeOptions{}
}

func (o *staticTopologyTypeOptions) Validate() error {
	if o.replicas < 1 {
		return errInvalidReplicas
	}

	// Make a mapping of each shard to a set of hosts and check each
	// shard has at least the required replicas mapped to
	// NB(r): We allow greater than the required replicas in case
	// node is streaming in and needs to take writes
	totalShards := len(o.shardScheme.All().Shards())
	hostAddressesByShard := make([]map[string]struct{}, totalShards)
	for i := range hostAddressesByShard {
		hostAddressesByShard[i] = make(map[string]struct{}, o.replicas)
	}
	for _, hostShardSet := range o.hostShardSets {
		if hostShardSet.ShardSet().Scheme() != o.shardScheme {
			return errHostShardSetSchemeDoesNotMatch
		}
		hostAddress := hostShardSet.Host().Address()
		for _, shard := range hostShardSet.ShardSet().Shards() {
			hostAddressesByShard[shard][hostAddress] = struct{}{}
		}
	}

	for shard, hosts := range hostAddressesByShard {
		if len(hosts) < o.replicas {
			errorFmt := "shard %d has %d replicas, less than the required %d replicas"
			return fmt.Errorf(errorFmt, shard, len(hosts), o.replicas)
		}
	}

	return nil
}

func (o *staticTopologyTypeOptions) ShardScheme(value sharding.ShardScheme) TopologyTypeOptions {
	opts := *o
	opts.shardScheme = value
	return &opts
}

func (o *staticTopologyTypeOptions) GetShardScheme() sharding.ShardScheme {
	return o.shardScheme
}

func (o *staticTopologyTypeOptions) Replicas(value int) TopologyTypeOptions {
	opts := *o
	opts.replicas = value
	return &opts
}

func (o *staticTopologyTypeOptions) GetReplicas() int {
	return o.replicas
}

func (o *staticTopologyTypeOptions) HostShardSets(value []HostShardSet) TopologyTypeOptions {
	opts := *o
	opts.hostShardSets = value
	return &opts
}

func (o *staticTopologyTypeOptions) GetHostShardSets() []HostShardSet {
	return o.hostShardSets
}
