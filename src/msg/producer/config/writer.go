// Copyright (c) 2018 Uber Technologies, Inc.
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

package config

import (
	"errors"
	"time"

	"github.com/uber-go/tally"

	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/msg/producer/writer"
	"github.com/m3db/m3/src/msg/protocol/proto"
	"github.com/m3db/m3/src/msg/topic"
	"github.com/m3db/m3/src/x/instrument"
	xio "github.com/m3db/m3/src/x/io"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/retry"
)

// ConnectionConfiguration configs the connection options.
type ConnectionConfiguration struct {
	NumConnections  *int                 `yaml:"numConnections"`
	DialTimeout     *time.Duration       `yaml:"dialTimeout"`
	WriteTimeout    *time.Duration       `yaml:"writeTimeout"`
	KeepAlivePeriod *time.Duration       `yaml:"keepAlivePeriod"`
	ResetDelay      *time.Duration       `yaml:"resetDelay"`
	Retry           *retry.Configuration `yaml:"retry"`
	FlushInterval   *time.Duration       `yaml:"flushInterval"`
	WriteBufferSize *int                 `yaml:"writeBufferSize"`
	ReadBufferSize  *int                 `yaml:"readBufferSize"`
}

// NewOptions creates connection options.
func (c *ConnectionConfiguration) NewOptions(iOpts instrument.Options) writer.ConnectionOptions {
	opts := writer.NewConnectionOptions()
	if c.NumConnections != nil {
		opts = opts.SetNumConnections(*c.NumConnections)
	}
	if c.DialTimeout != nil {
		opts = opts.SetDialTimeout(*c.DialTimeout)
	}
	if c.WriteTimeout != nil {
		opts = opts.SetWriteTimeout(*c.WriteTimeout)
	}
	if c.KeepAlivePeriod != nil {
		opts = opts.SetKeepAlivePeriod(*c.KeepAlivePeriod)
	}
	if c.ResetDelay != nil {
		opts = opts.SetResetDelay(*c.ResetDelay)
	}
	if c.Retry != nil {
		opts = opts.SetRetryOptions(c.Retry.NewOptions(iOpts.MetricsScope()))
	}
	if c.FlushInterval != nil {
		opts = opts.SetFlushInterval(*c.FlushInterval)
	}
	if c.WriteBufferSize != nil {
		opts = opts.SetWriteBufferSize(*c.WriteBufferSize)
	}
	if c.ReadBufferSize != nil {
		opts = opts.SetReadBufferSize(*c.ReadBufferSize)
	}
	return opts.SetInstrumentOptions(iOpts)
}

// WriterConfiguration configs the writer options.
type WriterConfiguration struct {
	TopicName                         string                         `yaml:"topicName" validate:"nonzero"`
	TopicServiceOverride              kv.OverrideConfiguration       `yaml:"topicServiceOverride"`
	TopicWatchInitTimeout             *time.Duration                 `yaml:"topicWatchInitTimeout"`
	PlacementOptions                  placement.Configuration        `yaml:"placement"`
	PlacementServiceOverride          services.OverrideConfiguration `yaml:"placementServiceOverride"`
	PlacementWatchInitTimeout         *time.Duration                 `yaml:"placementWatchInitTimeout"`
	MessagePool                       *pool.ObjectPoolConfiguration  `yaml:"messagePool"`
	MessageQueueNewWritesScanInterval *time.Duration                 `yaml:"messageQueueNewWritesScanInterval"`
	MessageQueueFullScanInterval      *time.Duration                 `yaml:"messageQueueFullScanInterval"`
	MessageQueueScanBatchSize         *int                           `yaml:"messageQueueScanBatchSize"`
	InitialAckMapSize                 *int                           `yaml:"initialAckMapSize"`
	CloseCheckInterval                *time.Duration                 `yaml:"closeCheckInterval"`
	AckErrorRetry                     *retry.Configuration           `yaml:"ackErrorRetry"`
	Encoder                           *proto.Configuration           `yaml:"encoder"`
	Decoder                           *proto.Configuration           `yaml:"decoder"`
	Connection                        *ConnectionConfiguration       `yaml:"connection"`

	// StaticMessageRetry configs a static message retry policy.
	StaticMessageRetry *StaticMessageRetryConfiguration `yaml:"staticMessageRetry"`
	// MessageRetry configs a algorithmic retry policy.
	// Only one of the retry configuration should be used.
	MessageRetry *retry.Configuration `yaml:"messageRetry"`

	// IgnoreCutoffCutover allows producing writes ignoring cutoff/cutover timestamp.
	// Must be in sync with AggregatorConfiguration.WritesIgnoreCutoffCutover.
	IgnoreCutoffCutover bool `yaml:"ignoreCutoffCutover"`
	// WithoutConsumerScope drops the consumer tag from the metrics. For large m3msg deployments the consumer tag can
	// add a lot of cardinality to the metrics.
	WithoutConsumerScope bool `yaml:"withoutConsumerScope"`
}

// StaticMessageRetryConfiguration configs the static message retry policy.
// When messageRetry config exists, messageRetry will override the static config.
type StaticMessageRetryConfiguration struct {
	Backoff []time.Duration `yaml:"backoff"`
}

// NewOptions creates writer options.
func (c *WriterConfiguration) NewOptions(
	cs client.Client,
	iOpts instrument.Options,
	rwOptions xio.Options,
) (writer.Options, error) {
	opts := writer.NewOptions().
		SetTopicName(c.TopicName).
		SetPlacementOptions(c.PlacementOptions.NewOptions()).
		SetInstrumentOptions(iOpts).
		SetWithoutConsumerScope(c.WithoutConsumerScope)

	kvOpts, err := c.TopicServiceOverride.NewOverrideOptions()
	if err != nil {
		return nil, err
	}

	topicServiceOpts := topic.NewServiceOptions().
		SetConfigService(cs).
		SetKVOverrideOptions(kvOpts)
	ts, err := topic.NewService(topicServiceOpts)
	if err != nil {
		return nil, err
	}

	opts = opts.SetTopicService(ts)

	if c.TopicWatchInitTimeout != nil {
		opts = opts.SetTopicWatchInitTimeout(*c.TopicWatchInitTimeout)
	}
	sd, err := cs.Services(c.PlacementServiceOverride.NewOptions())
	if err != nil {
		return nil, err
	}

	opts = opts.SetServiceDiscovery(sd)

	if c.PlacementWatchInitTimeout != nil {
		opts = opts.SetPlacementWatchInitTimeout(*c.PlacementWatchInitTimeout)
	}
	if c.MessagePool != nil {
		opts = opts.SetMessagePoolOptions(c.MessagePool.NewObjectPoolOptions(iOpts))
	}
	opts, err = c.setRetryOptions(opts, iOpts)
	if err != nil {
		return nil, err
	}
	if c.MessageQueueNewWritesScanInterval != nil {
		opts = opts.SetMessageQueueNewWritesScanInterval(*c.MessageQueueNewWritesScanInterval)
	}
	if c.MessageQueueFullScanInterval != nil {
		opts = opts.SetMessageQueueFullScanInterval(*c.MessageQueueFullScanInterval)
	}
	if c.MessageQueueScanBatchSize != nil {
		opts = opts.SetMessageQueueScanBatchSize(*c.MessageQueueScanBatchSize)
	}
	if c.InitialAckMapSize != nil {
		opts = opts.SetInitialAckMapSize(*c.InitialAckMapSize)
	}
	if c.CloseCheckInterval != nil {
		opts = opts.SetCloseCheckInterval(*c.CloseCheckInterval)
	}
	if c.AckErrorRetry != nil {
		opts = opts.SetAckErrorRetryOptions(c.AckErrorRetry.NewOptions(tally.NoopScope))
	}
	if c.Encoder != nil {
		opts = opts.SetEncoderOptions(c.Encoder.NewOptions(iOpts))
	}
	if c.Decoder != nil {
		opts = opts.SetDecoderOptions(c.Decoder.NewOptions(iOpts))
	}
	if c.Connection != nil {
		opts = opts.SetConnectionOptions(c.Connection.NewOptions(iOpts))
	}

	opts = opts.SetIgnoreCutoffCutover(c.IgnoreCutoffCutover)

	opts = opts.SetDecoderOptions(opts.DecoderOptions().SetRWOptions(rwOptions))
	return opts, nil
}

func (c *WriterConfiguration) setRetryOptions(
	opts writer.Options,
	iOpts instrument.Options,
) (writer.Options, error) {
	if c.StaticMessageRetry != nil && c.MessageRetry != nil {
		return nil, errors.New("invalid writer config with both static and algorithmic retry config set")
	}
	if c.MessageRetry != nil {
		return opts.SetMessageRetryNanosFn(
			writer.NextRetryNanosFn(c.MessageRetry.NewOptions(iOpts.MetricsScope())),
		), nil
	}
	if c.StaticMessageRetry != nil {
		fn, err := writer.StaticRetryNanosFn(c.StaticMessageRetry.Backoff)
		if err != nil {
			return nil, err
		}
		return opts.SetMessageRetryNanosFn(fn), nil
	}
	return opts, nil
}
