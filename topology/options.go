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
	"time"

	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3db/instrument"
	"github.com/m3db/m3db/sharding"
)

const (
	defaultServiceName = "m3db"
	defaultInitTimeout = 10 * time.Second
)

var (
	errNoConfigServiceClient   = errors.New("no config service client")
	errNoServiceDiscoverClient = errors.New("no service discover client")
	errNoHashGen               = errors.New("no hash gen function defined")
	errInvalidReplicas         = errors.New("replicas must be equal to or greater than 1")
)

type staticOptions struct {
	shardSet      sharding.ShardSet
	replicas      int
	hostShardSets []HostShardSet
}

// NewStaticOptions creates a new set of static topology options
func NewStaticOptions() StaticOptions {
	return &staticOptions{}
}

func (o *staticOptions) Validate() error {
	if o.replicas < 1 {
		return errInvalidReplicas
	}

	// Make a mapping of each shard to a set of hosts and check each
	// shard has at least the required replicas mapped to
	// NB(r): We allow greater than the required replicas in case
	// node is streaming in and needs to take writes
	totalShards := len(o.shardSet.Shards())
	hostAddressesByShard := make([]map[string]struct{}, totalShards)
	for i := range hostAddressesByShard {
		hostAddressesByShard[i] = make(map[string]struct{}, o.replicas)
	}
	for _, hostShardSet := range o.hostShardSets {
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

func (o *staticOptions) ShardSet(value sharding.ShardSet) StaticOptions {
	opts := *o
	opts.shardSet = value
	return &opts
}

func (o *staticOptions) GetShardSet() sharding.ShardSet {
	return o.shardSet
}

func (o *staticOptions) Replicas(value int) StaticOptions {
	opts := *o
	opts.replicas = value
	return &opts
}

func (o *staticOptions) GetReplicas() int {
	return o.replicas
}

func (o *staticOptions) HostShardSets(value []HostShardSet) StaticOptions {
	opts := *o
	opts.hostShardSets = value
	return &opts
}

func (o *staticOptions) GetHostShardSets() []HostShardSet {
	return o.hostShardSets
}

type dynamicOptions struct {
	configServiceClient client.Client
	service             string
	queryOptions        services.QueryOptions
	instrumentOptions   instrument.Options
	initTimeout         time.Duration
	hashGen             sharding.HashGen
}

// NewDynamicOptions creates a new set of dynamic topology options
func NewDynamicOptions() DynamicOptions {
	return &dynamicOptions{
		service:           defaultServiceName,
		queryOptions:      services.NewQueryOptions(),
		instrumentOptions: instrument.NewOptions(),
		initTimeout:       defaultInitTimeout,
		hashGen:           sharding.DefaultHashGen,
	}
}

func (o *dynamicOptions) Validate() error {
	if o.GetConfigServiceClient() == nil {
		return errNoConfigServiceClient
	}
	if o.GetConfigServiceClient().Services() == nil {
		return errNoServiceDiscoverClient
	}
	if o.GetHashGen() == nil {
		return errNoHashGen
	}
	return nil
}

func (o *dynamicOptions) ConfigServiceClient(c client.Client) DynamicOptions {
	o.configServiceClient = c
	return o
}

func (o *dynamicOptions) GetConfigServiceClient() client.Client {
	return o.configServiceClient
}

func (o *dynamicOptions) Service(s string) DynamicOptions {
	o.service = s
	return o
}

func (o *dynamicOptions) GetService() string {
	return o.service
}

func (o *dynamicOptions) QueryOptions(qo services.QueryOptions) DynamicOptions {
	o.queryOptions = qo
	return o
}

func (o *dynamicOptions) GetQueryOptions() services.QueryOptions {
	return o.queryOptions
}

func (o *dynamicOptions) InstrumentOptions(io instrument.Options) DynamicOptions {
	o.instrumentOptions = io
	return o
}

func (o *dynamicOptions) GetInstrumentOptions() instrument.Options {
	return o.instrumentOptions
}

func (o *dynamicOptions) InitTimeout(value time.Duration) DynamicOptions {
	o.initTimeout = value
	return o
}

func (o *dynamicOptions) GetInitTimeout() time.Duration {
	return o.initTimeout
}

func (o *dynamicOptions) HashGen(h sharding.HashGen) DynamicOptions {
	o.hashGen = h
	return o
}

func (o *dynamicOptions) GetHashGen() sharding.HashGen {
	return o.hashGen
}
