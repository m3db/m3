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

package client

import (
	"time"

	"github.com/m3db/m3/src/aggregator/sharding"
	m3clusterclient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3metrics/encoding/protobuf"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/retry"

	"github.com/uber-go/tally"
)

// Configuration contains client configuration.
type Configuration struct {
	PlacementKV                kv.OverrideConfiguration       `yaml:"placementKV" validate:"nonzero"`
	PlacementWatcher           placement.WatcherConfiguration `yaml:"placementWatcher"`
	HashType                   *sharding.HashType             `yaml:"hashType"`
	ShardCutoverWarmupDuration *time.Duration                 `yaml:"shardCutoverWarmupDuration"`
	ShardCutoffLingerDuration  *time.Duration                 `yaml:"shardCutoffLingerDuration"`
	Encoder                    EncoderConfiguration           `yaml:"encoder"`
	FlushSize                  int                            `yaml:"flushSize"`
	MaxTimerBatchSize          int                            `yaml:"maxTimerBatchSize"`
	QueueSize                  int                            `yaml:"queueSize"`
	QueueDropType              *DropType                      `yaml:"queueDropType"`
	Connection                 ConnectionConfiguration        `yaml:"connection"`
}

// NewAdminClient creates a new admin client.
func (c *Configuration) NewAdminClient(
	kvClient m3clusterclient.Client,
	clockOpts clock.Options,
	instrumentOpts instrument.Options,
) (AdminClient, error) {
	client, err := c.NewClient(kvClient, clockOpts, instrumentOpts)
	if err != nil {
		return nil, err
	}
	return client.(AdminClient), nil
}

// NewClient creates a new client.
func (c *Configuration) NewClient(
	kvClient m3clusterclient.Client,
	clockOpts clock.Options,
	instrumentOpts instrument.Options,
) (Client, error) {
	opts, err := c.newClientOptions(kvClient, clockOpts, instrumentOpts)
	if err != nil {
		return nil, err
	}
	return NewClient(opts), nil
}

func (c *Configuration) newClientOptions(
	kvClient m3clusterclient.Client,
	clockOpts clock.Options,
	instrumentOpts instrument.Options,
) (Options, error) {
	scope := instrumentOpts.MetricsScope()
	connectionOpts := c.Connection.NewConnectionOptions(scope.SubScope("connection"))
	kvOpts, err := c.PlacementKV.NewOverrideOptions()
	if err != nil {
		return nil, err
	}

	placementStore, err := kvClient.Store(kvOpts)
	if err != nil {
		return nil, err
	}

	iOpts := instrumentOpts.SetMetricsScope(scope.SubScope("encoder"))
	encoderOpts := c.Encoder.NewEncoderOptions(iOpts)

	iOpts = instrumentOpts.SetMetricsScope(scope.SubScope("placement-watcher"))
	watcherOpts := c.PlacementWatcher.NewOptions(placementStore, iOpts)

	// Get the shard fn.
	hashType := sharding.DefaultHash
	if c.HashType != nil {
		hashType = *c.HashType
	}
	shardFn, err := hashType.ShardFn()
	if err != nil {
		return nil, err
	}

	opts := NewOptions().
		SetClockOptions(clockOpts).
		SetInstrumentOptions(instrumentOpts).
		SetStagedPlacementWatcherOptions(watcherOpts).
		SetShardFn(shardFn).
		SetEncoderOptions(encoderOpts).
		SetConnectionOptions(connectionOpts)

	if c.ShardCutoverWarmupDuration != nil {
		opts = opts.SetShardCutoverWarmupDuration(*c.ShardCutoverWarmupDuration)
	}
	if c.ShardCutoffLingerDuration != nil {
		opts = opts.SetShardCutoffLingerDuration(*c.ShardCutoffLingerDuration)
	}
	if c.FlushSize != 0 {
		opts = opts.SetFlushSize(c.FlushSize)
	}
	if c.MaxTimerBatchSize != 0 {
		opts = opts.SetMaxTimerBatchSize(c.MaxTimerBatchSize)
	}
	if c.QueueSize != 0 {
		opts = opts.SetInstanceQueueSize(c.QueueSize)
	}
	if c.QueueDropType != nil {
		opts = opts.SetQueueDropType(*c.QueueDropType)
	}
	return opts, nil
}

// ConnectionConfiguration contains the connection configuration.
type ConnectionConfiguration struct {
	ConnectionTimeout            time.Duration        `yaml:"connectionTimeout"`
	ConnectionKeepAlive          *bool                `yaml:"connectionKeepAlive"`
	WriteTimeout                 time.Duration        `yaml:"writeTimeout"`
	InitReconnectThreshold       int                  `yaml:"initReconnectThreshold"`
	MaxReconnectThreshold        int                  `yaml:"maxReconnectThreshold"`
	ReconnectThresholdMultiplier int                  `yaml:"reconnectThresholdMultiplier"`
	MaxReconnectDuration         *time.Duration       `yaml:"maxReconnectDuration"`
	WriteRetries                 *retry.Configuration `yaml:"writeRetries"`
}

// NewConnectionOptions creates new connection options.
func (c *ConnectionConfiguration) NewConnectionOptions(scope tally.Scope) ConnectionOptions {
	opts := NewConnectionOptions()
	if c.ConnectionTimeout != 0 {
		opts = opts.SetConnectionTimeout(c.ConnectionTimeout)
	}
	if c.ConnectionKeepAlive != nil {
		opts = opts.SetConnectionKeepAlive(*c.ConnectionKeepAlive)
	}
	if c.WriteTimeout != 0 {
		opts = opts.SetWriteTimeout(c.WriteTimeout)
	}
	if c.InitReconnectThreshold != 0 {
		opts = opts.SetInitReconnectThreshold(c.InitReconnectThreshold)
	}
	if c.MaxReconnectThreshold != 0 {
		opts = opts.SetMaxReconnectThreshold(c.MaxReconnectThreshold)
	}
	if c.ReconnectThresholdMultiplier != 0 {
		opts = opts.SetReconnectThresholdMultiplier(c.ReconnectThresholdMultiplier)
	}
	if c.MaxReconnectDuration != nil {
		opts = opts.SetMaxReconnectDuration(*c.MaxReconnectDuration)
	}
	if c.WriteRetries != nil {
		retryOpts := c.WriteRetries.NewOptions(scope)
		opts = opts.SetWriteRetryOptions(retryOpts)
	}
	return opts
}

// EncoderConfiguration configures the encoder.
type EncoderConfiguration struct {
	InitBufferSize *int                              `yaml:"initBufferSize"`
	MaxMessageSize *int                              `yaml:"maxMessageSize"`
	BytesPool      *pool.BucketizedPoolConfiguration `yaml:"bytesPool"`
}

// NewEncoderOptions create a new set of encoder options.
func (c *EncoderConfiguration) NewEncoderOptions(
	instrumentOpts instrument.Options,
) protobuf.UnaggregatedOptions {
	opts := protobuf.NewUnaggregatedOptions()
	if c.InitBufferSize != nil {
		opts = opts.SetInitBufferSize(*c.InitBufferSize)
	}
	if c.MaxMessageSize != nil {
		opts = opts.SetMaxMessageSize(*c.MaxMessageSize)
	}
	if c.BytesPool != nil {
		sizeBuckets := c.BytesPool.NewBuckets()
		objectPoolOpts := c.BytesPool.NewObjectPoolOptions(instrumentOpts)
		bytesPool := pool.NewBytesPool(sizeBuckets, objectPoolOpts)
		opts = opts.SetBytesPool(bytesPool)
		bytesPool.Init()
	}
	return opts
}
