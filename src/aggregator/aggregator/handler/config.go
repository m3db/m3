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

	"github.com/m3db/m3/src/aggregator/aggregator/handler/filter"
	"github.com/m3db/m3/src/aggregator/aggregator/handler/writer"
	"github.com/m3db/m3/src/aggregator/sharding"
	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/msg/producer"
	"github.com/m3db/m3/src/msg/producer/config"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"

	"go.uber.org/zap"
)

var (
	errNoHandlerConfiguration                   = errors.New("no handler configuration")
	errNoDynamicOrStaticBackendConfiguration    = errors.New("neither dynamic nor static backend was configured")
	errBothDynamicAndStaticBackendConfiguration = errors.New("both dynamic and static backend were configured")
)

// FlushHandlerConfiguration configures flush handlers.
type FlushHandlerConfiguration struct {
	Handlers []flushHandlerConfiguration `yaml:"handlers" validate:"nonzero"`
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
		handlers = make([]Handler, 0, len(c.Handlers))
	)
	for _, hc := range c.Handlers {
		handler, err := hc.newHandler(cs, instrumentOpts)
		if err != nil {
			return nil, err
		}
		handlers = append(handlers, handler)
	}
	if len(handlers) == 1 {
		return handlers[0], nil
	}
	return NewBroadcastHandler(handlers), nil
}

type writerConfiguration struct {
	// Pool of buffered bytes.
	BytesPool *pool.BucketizedPoolConfiguration `yaml:"bytesPool"`

	// How frequent is the encoding time sampled and included in the payload.
	EncodingTimeSamplingRate float64 `yaml:"encodingTimeSamplingRate" validate:"min=0.0,max=1.0"`
}

func (c writerConfiguration) NewWriterOptions(
	instrumentOpts instrument.Options,
) writer.Options {
	opts := writer.NewOptions().
		SetInstrumentOptions(instrumentOpts).
		SetEncodingTimeSamplingRate(c.EncodingTimeSamplingRate)

	scope := instrumentOpts.MetricsScope()
	iOpts := instrumentOpts.SetMetricsScope(scope.SubScope("buffered-encoder-pool"))
	if c.BytesPool != nil {
		iOpts := iOpts.SetMetricsScope(scope.Tagged(map[string]string{"pool": "buffered-bytes-pool"}))
		bytesPool := pool.NewBytesPool(c.BytesPool.NewBuckets(), c.BytesPool.NewObjectPoolOptions(iOpts))
		bytesPool.Init()
		opts = opts.SetBytesPool(bytesPool)
	}
	return opts
}

type flushHandlerConfiguration struct {
	// StaticBackend configures the backend.
	StaticBackend *staticBackendConfiguration `yaml:"staticBackend"`

	// DynamicBackend configures the dynamic backend.
	DynamicBackend *dynamicBackendConfiguration `yaml:"dynamicBackend"`
}

func (c flushHandlerConfiguration) newHandler(
	cs client.Client,
	instrumentOpts instrument.Options,
) (Handler, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if c.DynamicBackend != nil {
		return c.DynamicBackend.newProtobufHandler(
			cs,
			instrumentOpts,
		)
	}
	switch c.StaticBackend.Type {
	case blackholeType:
		return NewBlackholeHandler(), nil
	case loggingType:
		return NewLoggingHandler(instrumentOpts.Logger()), nil
	default:
		return nil, fmt.Errorf("unknown backend type %v", c.StaticBackend.Type)
	}
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

	// Producer configs the m3msg producer.
	Producer config.ProducerConfiguration `yaml:"producer"`

	// Filters configs the filter for consumer services.
	Filters []consumerServiceFilterConfiguration `yaml:"filters"`

	// Filters configs the filter for consumer services.
	StoragePolicyFilters []storagePolicyFilterConfiguration `yaml:"storagePolicyFilters"`

	// Writer configs the writer options.
	Writer writerConfiguration `yaml:"writer"`
}

func (c *dynamicBackendConfiguration) newProtobufHandler(
	cs client.Client,
	instrumentOpts instrument.Options,
) (Handler, error) {
	scope := instrumentOpts.MetricsScope().Tagged(map[string]string{
		"backend":   c.Name,
		"component": "producer",
	})
	instrumentOpts = instrumentOpts.SetMetricsScope(scope)
	p, err := c.Producer.NewProducer(cs, instrumentOpts)
	if err != nil {
		return nil, err
	}
	if err := p.Init(); err != nil {
		return nil, err
	}
	logger := instrumentOpts.Logger()
	for _, filter := range c.Filters {
		sid, f := filter.NewConsumerServiceFilter()
		p.RegisterFilter(sid, f)
		logger.Info("registered filter for consumer service", zap.Stringer("service", sid))
	}
	for _, filter := range c.StoragePolicyFilters {
		sid, f := filter.NewConsumerServiceFilter()
		p.RegisterFilter(sid, f)
		logger.Info("registered storage policy filter for consumer service",
			zap.Any("policies", filter.StoragePolicies),
			zap.Stringer("service", sid))
	}
	wOpts := c.Writer.NewWriterOptions(instrumentOpts)
	instrumentOpts.Logger().Info("created flush handler with protobuf encoding", zap.String("name", c.Name))
	return NewProtobufHandler(p, c.HashType, wOpts), nil
}

type storagePolicyFilterConfiguration struct {
	ServiceID       services.ServiceIDConfiguration `yaml:"serviceID" validate:"nonzero"`
	StoragePolicies []policy.StoragePolicy          `yaml:"storagePolicies" validate:"nonzero"`
}

func (c storagePolicyFilterConfiguration) NewConsumerServiceFilter() (services.ServiceID, producer.FilterFunc) {
	return c.ServiceID.NewServiceID(), writer.NewStoragePolicyFilter(c.StoragePolicies)
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
}
