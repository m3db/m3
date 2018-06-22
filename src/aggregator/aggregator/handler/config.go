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

package handler

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/m3db/m3aggregator/aggregator/handler/common"
	"github.com/m3db/m3aggregator/aggregator/handler/filter"
	"github.com/m3db/m3aggregator/aggregator/handler/router"
	"github.com/m3db/m3aggregator/aggregator/handler/writer"
	"github.com/m3db/m3aggregator/sharding"
	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3metrics/encoding/msgpack"
	"github.com/m3db/m3msg/producer"
	"github.com/m3db/m3msg/producer/config"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/retry"

	"github.com/uber-go/tally"
)

const (
	initialBufferSizeGrowthFactor = 2
)

var (
	errNoHandlerConfiguration                   = errors.New("no handler configuration")
	errNoWriterConfiguration                    = errors.New("no writer configuration")
	errNoDynamicOrStaticBackendConfiguration    = errors.New("neither dynamic nor static backend was configured")
	errBothDynamicAndStaticBackendConfiguration = errors.New("both dynamic and static backend were configured")
)

// FlushHandlerConfiguration configures flush handlers.
type FlushHandlerConfiguration struct {
	Handlers []flushHandlerConfiguration `yaml:"handlers" validate:"nonzero"`
	Writer   *writerConfiguration        `yaml:"writer"`
}

// NewHandler creates a new flush handler based on the configuration.
func (c FlushHandlerConfiguration) NewHandler(
	cs client.Client,
	instrumentOpts instrument.Options,
) (Handler, error) {
	if len(c.Handlers) == 0 {
		return nil, errNoHandlerConfiguration
	}
	var (
		handlers       = make([]Handler, 0, len(c.Handlers))
		sharderRouters = make([]SharderRouter, 0, len(c.Handlers))
	)
	for _, hc := range c.Handlers {
		if err := hc.Validate(); err != nil {
			return nil, err
		}
		if hc.DynamicBackend != nil {
			sharderRouter, err := hc.DynamicBackend.NewSharderRouter(
				cs,
				instrumentOpts,
			)
			if err != nil {
				return nil, err
			}
			sharderRouters = append(sharderRouters, sharderRouter)
			continue
		}
		switch hc.StaticBackend.Type {
		case blackholeType:
			handlers = append(handlers, NewBlackholeHandler())
		case loggingType:
			handlers = append(handlers, NewLoggingHandler(instrumentOpts.Logger()))
		case forwardType:
			sharderRouter, err := hc.StaticBackend.NewSharderRouter(instrumentOpts)
			if err != nil {
				return nil, err
			}
			sharderRouters = append(sharderRouters, sharderRouter)
		default:
			return nil, fmt.Errorf("unknown backend type %v", hc.StaticBackend.Type)
		}
	}
	if len(sharderRouters) > 0 {
		if c.Writer == nil {
			return nil, errNoWriterConfiguration
		}
		writerOpts := c.Writer.NewWriterOptions(instrumentOpts)
		shardedHandler := NewShardedHandler(sharderRouters, writerOpts)
		handlers = append(handlers, shardedHandler)
	}
	if len(handlers) == 1 {
		return handlers[0], nil
	}
	return NewBroadcastHandler(handlers), nil
}

type writerConfiguration struct {
	// Maximum Buffer size in bytes before a buffer is flushed to backend.
	MaxBufferSize int `yaml:"maxBufferSize"`

	// Pool of buffered encoders.
	BufferedEncoderPool pool.ObjectPoolConfiguration `yaml:"bufferedEncoderPool"`

	// How frequent is the encoding time sampled and included in the payload.
	EncodingTimeSamplingRate float64 `yaml:"encodingTimeSamplingRate" validate:"min=0.0,max=1.0"`
}

func (c *writerConfiguration) NewWriterOptions(
	instrumentOpts instrument.Options,
) writer.Options {
	opts := writer.NewOptions().
		SetInstrumentOptions(instrumentOpts).
		SetEncodingTimeSamplingRate(c.EncodingTimeSamplingRate)
	if c.MaxBufferSize > 0 {
		opts = opts.SetMaxBufferSize(c.MaxBufferSize)
	}

	// Set buffered encoder pool.
	// NB(xichen): we preallocate a bit over the maximum buffer size as a safety measure
	// because we might write past the max buffer size and rewind it during writing.
	scope := instrumentOpts.MetricsScope()
	iOpts := instrumentOpts.SetMetricsScope(scope.SubScope("buffered-encoder-pool"))
	bufferedEncoderPoolOpts := msgpack.NewBufferedEncoderPoolOptions().
		SetObjectPoolOptions(c.BufferedEncoderPool.NewObjectPoolOptions(iOpts))
	bufferedEncoderPool := msgpack.NewBufferedEncoderPool(bufferedEncoderPoolOpts)
	opts = opts.SetBufferedEncoderPool(bufferedEncoderPool)
	initialBufferSize := c.MaxBufferSize * initialBufferSizeGrowthFactor
	bufferedEncoderPool.Init(func() msgpack.BufferedEncoder {
		return msgpack.NewPooledBufferedEncoderSize(bufferedEncoderPool, initialBufferSize)
	})
	return opts
}

type flushHandlerConfiguration struct {
	// StaticBackend configures the backend.
	StaticBackend *staticBackendConfiguration `yaml:"staticBackend"`

	// DynamicBackend configures the dynamic backend.
	DynamicBackend *dynamicBackendConfiguration `yaml:"dynamicBackend"`
}

func (c flushHandlerConfiguration) Validate() error {
	if c.StaticBackend == nil && c.DynamicBackend == nil {
		return errNoDynamicOrStaticBackendConfiguration
	}
	if c.StaticBackend != nil && c.DynamicBackend != nil {
		return errBothDynamicAndStaticBackendConfiguration
	}
	return nil
}

type dynamicBackendConfiguration struct {
	// Name of the backend.
	Name string `yaml:"name"`

	// Hashing function type.
	HashType sharding.HashType `yaml:"hashType"`

	// Total number of shards.
	TotalShards int `yaml:"totalShards" validate:"nonzero"`

	// Producer configs the m3msg producer.
	Producer config.ProducerConfiguration `yaml:"producer"`

	// Filters configs the filter for consumer services.
	Filters []consumerServiceFilterConfiguration `yaml:"filters"`
}

func (c *dynamicBackendConfiguration) NewSharderRouter(
	cs client.Client,
	instrumentOpts instrument.Options,
) (SharderRouter, error) {
	scope := instrumentOpts.MetricsScope().Tagged(map[string]string{
		"backend":   c.Name,
		"component": "producer",
	})
	p, err := c.Producer.NewProducer(cs, instrumentOpts.SetMetricsScope(scope))
	if err != nil {
		return SharderRouter{}, err
	}
	if err := p.Init(); err != nil {
		return SharderRouter{}, err
	}
	logger := instrumentOpts.Logger()
	for _, filter := range c.Filters {
		sid, f := filter.NewConsumerServiceFilter()
		p.RegisterFilter(sid, f)
		logger.Infof("registered filter for consumer service: %s", sid.String())
	}
	return SharderRouter{
		SharderID: sharding.NewSharderID(c.HashType, c.TotalShards),
		Router:    router.NewWithAckRouter(p),
	}, nil
}

type consumerServiceFilterConfiguration struct {
	ServiceID services.ServiceIDConfiguration `yaml:"serviceID" validate:"nonzero"`
	ShardSet  sharding.ShardSet               `yaml:"shardSet" validate:"nonzero"`
}

func (c consumerServiceFilterConfiguration) NewConsumerServiceFilter() (services.ServiceID, producer.FilterFunc) {
	return c.ServiceID.NewServiceID(), filter.NewShardSetFilter(c.ShardSet)
}

type staticBackendConfiguration struct {
	// Static backend type.
	Type Type `yaml:"type"`

	// Name of the backend.
	Name string `yaml:"name"`

	// Servers for non-sharded backend.
	Servers []string `yaml:"servers"`

	// Configuration for sharded backend.
	Sharded *shardedConfiguration `yaml:"sharded"`

	// Queue size reserved for the backend.
	QueueSize int `yaml:"queueSize"`

	// Connection configuration.
	Connection connectionConfiguration `yaml:"connection"`

	// Disable validation (dangerous but useful for testing and staging environments).
	DisableValidation bool `yaml:"disableValidation"`
}

func (c *staticBackendConfiguration) Validate() error {
	if c.DisableValidation {
		return nil
	}
	hasServers := len(c.Servers) > 0
	hasShards := c.Sharded != nil
	if hasServers && hasShards {
		return fmt.Errorf("backend %s configuration has both servers and shards", c.Name)
	}
	if !hasServers && !hasShards {
		return fmt.Errorf("backend %s configuration has neither servers no shards", c.Name)
	}
	if hasServers {
		return c.validateNonSharded()
	}
	return c.Sharded.Validate()
}

func (c *staticBackendConfiguration) validateNonSharded() error {
	// Make sure we haven't declared the same server more than once.
	serversAssigned := make(map[string]struct{}, len(c.Servers))
	for _, server := range c.Servers {
		if _, alreadyAssigned := serversAssigned[server]; alreadyAssigned {
			return fmt.Errorf("server %s specified more than once", server)
		}
		serversAssigned[server] = struct{}{}
	}
	return nil
}

func (c *staticBackendConfiguration) NewSharderRouter(
	instrumentOpts instrument.Options,
) (SharderRouter, error) {
	if err := c.Validate(); err != nil {
		return SharderRouter{}, err
	}
	backendScope := instrumentOpts.MetricsScope().SubScope(c.Name)

	// Set up queue options.
	queueScope := backendScope.SubScope("queue")
	connectionOpts := c.Connection.NewConnectionOptions(queueScope)
	queueOpts := common.NewQueueOptions().
		SetInstrumentOptions(instrumentOpts.SetMetricsScope(queueScope)).
		SetConnectionOptions(connectionOpts)
	if c.QueueSize > 0 {
		queueOpts = queueOpts.SetQueueSize(c.QueueSize)
	}

	if len(c.Servers) > 0 {
		// This is a non-sharded backend.
		queue, err := common.NewQueue(c.Servers, queueOpts)
		if err != nil {
			return SharderRouter{}, err
		}
		router := router.NewAllowAllRouter(queue)
		return SharderRouter{SharderID: sharding.NoShardingSharderID, Router: router}, nil
	}

	// Sharded backend.
	routerScope := backendScope.SubScope("router")
	return c.Sharded.NewSharderRouter(routerScope, queueOpts)
}

type shardedConfiguration struct {
	// Hashing function type.
	HashType sharding.HashType `yaml:"hashType"`

	// Total number of shards.
	TotalShards int `yaml:"totalShards" validate:"nonzero"`

	// Backend server shard sets.
	Shards []backendServerShardSet `yaml:"shards" validate:"nonzero"`
}

func (c *shardedConfiguration) Validate() error {
	var (
		serversAssigned = make(map[string]struct{}, len(c.Shards))
		shardsAssigned  = make(map[int]struct{}, c.TotalShards)
	)
	for _, shards := range c.Shards {
		// Make sure we have a deterministic ordering.
		sortedShards := make([]int, 0, len(shards.ShardSet))
		for shard := range shards.ShardSet {
			sortedShards = append(sortedShards, int(shard))
		}
		sort.Ints(sortedShards)

		for _, shard := range sortedShards {
			if shard >= c.TotalShards {
				return fmt.Errorf("shard %d exceeds total available shards %d", shard, c.TotalShards)
			}
			if _, shardAlreadyAssigned := shardsAssigned[shard]; shardAlreadyAssigned {
				return fmt.Errorf("shard %d is present in multiple ranges", shard)
			}
			shardsAssigned[shard] = struct{}{}
		}

		for _, server := range shards.Servers {
			if _, serverAlreadyAssigned := serversAssigned[server]; serverAlreadyAssigned {
				return fmt.Errorf("server %s is present in multiple ranges", server)
			}
			serversAssigned[server] = struct{}{}
		}
	}
	if len(shardsAssigned) != c.TotalShards {
		return fmt.Errorf("missing shards; expected %d total received %d",
			c.TotalShards, len(shardsAssigned))
	}
	return nil
}

func (c *shardedConfiguration) NewSharderRouter(
	routerScope tally.Scope,
	queueOpts common.QueueOptions,
) (SharderRouter, error) {
	shardQueueSize := queueOpts.QueueSize() / len(c.Shards)
	shardQueueOpts := queueOpts.SetQueueSize(shardQueueSize)
	sharderID := sharding.NewSharderID(c.HashType, c.TotalShards)
	shardedQueues := make([]router.ShardedQueue, 0, len(c.Shards))
	for _, shard := range c.Shards {
		sq, err := shard.NewShardedQueue(shardQueueOpts)
		if err != nil {
			return SharderRouter{}, err
		}
		shardedQueues = append(shardedQueues, sq)
	}
	router := router.NewShardedRouter(shardedQueues, c.TotalShards, routerScope)
	return SharderRouter{SharderID: sharderID, Router: router}, nil
}

type backendServerShardSet struct {
	Name     string            `yaml:"name"`
	ShardSet sharding.ShardSet `yaml:"shardSet" validate:"nonzero"`
	Servers  []string          `yaml:"servers" validate:"nonzero"`
}

func (s *backendServerShardSet) NewShardedQueue(
	queueOpts common.QueueOptions,
) (router.ShardedQueue, error) {
	instrumentOpts := queueOpts.InstrumentOptions()
	connectionOpts := queueOpts.ConnectionOptions()
	queueScope := instrumentOpts.MetricsScope().Tagged(map[string]string{"shard-set": s.Name})
	reconnectRetryOpts := connectionOpts.ReconnectRetryOptions().SetMetricsScope(queueScope)
	queueOpts = queueOpts.
		SetInstrumentOptions(instrumentOpts.SetMetricsScope(queueScope)).
		SetConnectionOptions(connectionOpts.SetReconnectRetryOptions(reconnectRetryOpts))
	queue, err := common.NewQueue(s.Servers, queueOpts)
	if err != nil {
		return router.ShardedQueue{}, err
	}
	return router.ShardedQueue{ShardSet: s.ShardSet, Queue: queue}, nil
}

type connectionConfiguration struct {
	// Connection timeout.
	ConnectTimeout time.Duration `yaml:"connectTimeout"`

	// Connection keep alive.
	ConnectionKeepAlive *bool `yaml:"connectionKeepAlive"`

	// Connection write timeout.
	ConnectionWriteTimeout time.Duration `yaml:"connectionWriteTimeout"`

	// Reconnect retry options.
	ReconnectRetrier retry.Configuration `yaml:"reconnect"`
}

func (c *connectionConfiguration) NewConnectionOptions(scope tally.Scope) common.ConnectionOptions {
	opts := common.NewConnectionOptions()
	if c.ConnectTimeout != 0 {
		opts = opts.SetConnectTimeout(c.ConnectTimeout)
	}
	if c.ConnectionKeepAlive != nil {
		opts = opts.SetConnectionKeepAlive(*c.ConnectionKeepAlive)
	}
	if c.ConnectionWriteTimeout != 0 {
		opts = opts.SetConnectionWriteTimeout(c.ConnectionWriteTimeout)
	}
	reconnectScope := scope.SubScope("reconnect")
	retryOpts := c.ReconnectRetrier.NewOptions(reconnectScope)
	opts = opts.SetReconnectRetryOptions(retryOpts)
	return opts
}
