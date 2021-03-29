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
	"errors"
	"fmt"
	"time"

	"github.com/m3db/m3/src/aggregator/sharding"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"
	xio "github.com/m3db/m3/src/x/io"
)

// AggregatorClientType determines the aggregator client type.
type AggregatorClientType int

const (
	// LegacyAggregatorClient is an alias for TCPAggregatorClient
	LegacyAggregatorClient AggregatorClientType = iota
	// M3MsgAggregatorClient is the M3Msg aggregator client type that uses M3Msg to
	// handle publishing to a M3Msg topic the aggregator consumes from.
	M3MsgAggregatorClient
	// TCPAggregatorClient is the TCP aggregator client type and uses it's own
	// TCP negotiation, load balancing and data transmission protocol.
	TCPAggregatorClient

	defaultAggregatorClient = LegacyAggregatorClient

	defaultFlushSize = 1440

	// defaultMaxTimerBatchSize is the default maximum timer batch size.
	// By default there is no limit on the timer batch size.
	defaultMaxTimerBatchSize = 0

	// defaultInstanceQueueSize determines how many metrics can be buffered
	// before it must wait for an existing batch to be flushed to an instance.
	defaultInstanceQueueSize = 2 << 15 // ~65k

	// By default traffic is cut over to shards 10 minutes before the designated
	// cutover time in case there are issues with the instances owning the shards.
	defaultShardCutoverWarmupDuration = 10 * time.Minute

	// By default traffic doesn't stop until one hour after the designated cutoff
	// time in case there are issues with the instances taking over the shards
	// and as such we need to switch the traffic back to the previous owner of the shards.
	defaultShardCutoffLingerDuration = time.Hour

	// By default the oldest metrics in the queue are dropped when it is full.
	defaultDropType = DropOldest

	// By default set maximum batch size to 8mb.
	defaultMaxBatchSize = 2 << 22

	// By default write at least every 100ms.
	defaultBatchFlushDeadline = 100 * time.Millisecond
)

var (
	validAggregatorClientTypes = []AggregatorClientType{
		LegacyAggregatorClient,
		M3MsgAggregatorClient,
		TCPAggregatorClient,
	}

	errTCPClientNoWatcherOptions = errors.New("legacy client: no watcher options set")
	errM3MsgClientNoOptions      = errors.New("m3msg aggregator client: no m3msg options set")
	errNoRWOpts                  = errors.New("no rw opts set for aggregator")
)

func (t AggregatorClientType) String() string {
	switch t {
	case LegacyAggregatorClient:
		return "legacy"
	case M3MsgAggregatorClient:
		return "m3msg"
	case TCPAggregatorClient:
		return "tcp"
	}
	return "unknown"
}

// UnmarshalYAML unmarshals a AggregatorClientType into a valid type from string.
func (t *AggregatorClientType) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}
	if str == "" {
		*t = defaultAggregatorClient
		return nil
	}
	for _, valid := range validAggregatorClientTypes {
		if str == valid.String() {
			*t = valid
			return nil
		}
	}
	return fmt.Errorf("invalid AggregatorClientType: value=%s, valid=%v",
		str, validAggregatorClientTypes)
}

// Options provide a set of client options.
type Options interface {
	// Validate validates the client options.
	Validate() error

	// SetAggregatorClientType sets the client type.
	SetAggregatorClientType(value AggregatorClientType) Options

	// AggregatorClientType returns the client type.
	AggregatorClientType() AggregatorClientType

	// SetM3MsgOptions sets the M3Msg aggregator client options.
	SetM3MsgOptions(value M3MsgOptions) Options

	// M3MsgOptions returns the M3Msg aggregator client options.
	M3MsgOptions() M3MsgOptions

	// SetClockOptions sets the clock options.
	SetClockOptions(value clock.Options) Options

	// ClockOptions returns the clock options.
	ClockOptions() clock.Options

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetEncoderOptions sets the encoder options.
	SetEncoderOptions(value protobuf.UnaggregatedOptions) Options

	// EncoderOptions returns the encoder options.
	EncoderOptions() protobuf.UnaggregatedOptions

	// SetShardFn sets the sharding function.
	SetShardFn(value sharding.ShardFn) Options

	// ShardFn returns the sharding function.
	ShardFn() sharding.ShardFn

	// SetStagedPlacementWatcherOptions sets the staged placement watcher options.
	SetStagedPlacementWatcherOptions(value placement.StagedPlacementWatcherOptions) Options

	// StagedPlacementWatcherOptions returns the staged placement watcher options.
	StagedPlacementWatcherOptions() placement.StagedPlacementWatcherOptions

	// SetShardCutoverWarmupDuration sets the warm up duration for traffic cut over to a shard.
	SetShardCutoverWarmupDuration(value time.Duration) Options

	// ShardCutoverWarmupDuration returns the warm up duration for traffic cut over to a shard.
	ShardCutoverWarmupDuration() time.Duration

	// SetShardCutoffLingerDuration sets the linger duration for traffic cut off from a shard.
	SetShardCutoffLingerDuration(value time.Duration) Options

	// ShardCutoffLingerDuration returns the linger duration for traffic cut off from a shard.
	ShardCutoffLingerDuration() time.Duration

	// SetConnectionOptions sets the connection options.
	SetConnectionOptions(value ConnectionOptions) Options

	// ConnectionOptions returns the connection options.
	ConnectionOptions() ConnectionOptions

	// SetFlushSize sets the buffer size to trigger a flush.
	SetFlushSize(value int) Options

	// FlushSize returns the buffer size to trigger a flush.
	FlushSize() int

	// SetMaxTimerBatchSize sets the maximum timer batch size.
	SetMaxTimerBatchSize(value int) Options

	// MaxTimerBatchSize returns the maximum timer batch size.
	MaxTimerBatchSize() int

	// SetInstanceQueueSize sets the instance queue size.
	SetInstanceQueueSize(value int) Options

	// InstanceQueueSize returns the instance queue size.
	InstanceQueueSize() int

	// SetQueueDropType sets the strategy for which metrics should metrics should be dropped when
	// the queue is full.
	SetQueueDropType(value DropType) Options

	// QueueDropType returns sets the strategy for which metrics should metrics should be dropped
	// when the queue is full.
	QueueDropType() DropType

	// SetMaxBatchSize sets the buffer limit that triggers a write of queued buffers.
	SetMaxBatchSize(value int) Options

	// MaxBatchSize returns the maximum buffer size that triggers a queue drain.
	MaxBatchSize() int

	// SetBatchFlushDeadline sets the deadline that triggers a write of queued buffers.
	SetBatchFlushDeadline(value time.Duration) Options

	// BatchFlushDeadline returns the deadline that triggers a write of queued buffers.
	BatchFlushDeadline() time.Duration

	// SetRWOptions sets RW options.
	SetRWOptions(value xio.Options) Options

	// RWOptions returns the RW options.
	RWOptions() xio.Options
}

type options struct {
	aggregatorClientType       AggregatorClientType
	clockOpts                  clock.Options
	instrumentOpts             instrument.Options
	encoderOpts                protobuf.UnaggregatedOptions
	shardFn                    sharding.ShardFn
	shardCutoverWarmupDuration time.Duration
	shardCutoffLingerDuration  time.Duration
	watcherOpts                placement.StagedPlacementWatcherOptions
	connOpts                   ConnectionOptions
	flushSize                  int
	maxTimerBatchSize          int
	instanceQueueSize          int
	dropType                   DropType
	maxBatchSize               int
	batchFlushDeadline         time.Duration
	m3msgOptions               M3MsgOptions
	rwOpts                     xio.Options
}

// NewOptions creates a new set of client options.
func NewOptions() Options {
	return &options{
		clockOpts:                  clock.NewOptions(),
		instrumentOpts:             instrument.NewOptions(),
		encoderOpts:                protobuf.NewUnaggregatedOptions(),
		shardFn:                    sharding.DefaultHash.MustShardFn(),
		shardCutoverWarmupDuration: defaultShardCutoverWarmupDuration,
		shardCutoffLingerDuration:  defaultShardCutoffLingerDuration,
		watcherOpts:                placement.NewStagedPlacementWatcherOptions(),
		connOpts:                   NewConnectionOptions(),
		flushSize:                  defaultFlushSize,
		maxTimerBatchSize:          defaultMaxTimerBatchSize,
		instanceQueueSize:          defaultInstanceQueueSize,
		dropType:                   defaultDropType,
		maxBatchSize:               defaultMaxBatchSize,
		batchFlushDeadline:         defaultBatchFlushDeadline,
		rwOpts:                     xio.NewOptions(),
	}
}

func (o *options) Validate() error {
	if o.rwOpts == nil {
		return errNoRWOpts
	}
	switch o.aggregatorClientType {
	case M3MsgAggregatorClient:
		opts := o.m3msgOptions
		if opts == nil {
			return errM3MsgClientNoOptions
		}
		return opts.Validate()
	case LegacyAggregatorClient:
		fallthrough // intentional, LegacyAggregatorClient is an alias
	case TCPAggregatorClient:
		if o.watcherOpts == nil {
			return errTCPClientNoWatcherOptions
		}
		return nil
	default:
		return fmt.Errorf("unknown client type: %v", o.aggregatorClientType)
	}
}

func (o *options) SetAggregatorClientType(value AggregatorClientType) Options {
	opts := *o
	opts.aggregatorClientType = value
	return &opts
}

func (o *options) AggregatorClientType() AggregatorClientType {
	return o.aggregatorClientType
}

func (o *options) SetM3MsgOptions(value M3MsgOptions) Options {
	opts := *o
	opts.m3msgOptions = value
	return &opts
}

func (o *options) M3MsgOptions() M3MsgOptions {
	return o.m3msgOptions
}

func (o *options) SetClockOptions(value clock.Options) Options {
	opts := *o
	opts.clockOpts = value
	return &opts
}

func (o *options) ClockOptions() clock.Options {
	return o.clockOpts
}

func (o *options) SetInstrumentOptions(value instrument.Options) Options {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *options) SetEncoderOptions(value protobuf.UnaggregatedOptions) Options {
	opts := *o
	opts.encoderOpts = value
	return &opts
}

func (o *options) EncoderOptions() protobuf.UnaggregatedOptions {
	return o.encoderOpts
}

func (o *options) SetShardFn(value sharding.ShardFn) Options {
	opts := *o
	opts.shardFn = value
	return &opts
}

func (o *options) ShardFn() sharding.ShardFn {
	return o.shardFn
}

func (o *options) SetShardCutoverWarmupDuration(value time.Duration) Options {
	opts := *o
	opts.shardCutoverWarmupDuration = value
	return &opts
}

func (o *options) ShardCutoverWarmupDuration() time.Duration {
	return o.shardCutoverWarmupDuration
}

func (o *options) SetShardCutoffLingerDuration(value time.Duration) Options {
	opts := *o
	opts.shardCutoffLingerDuration = value
	return &opts
}

func (o *options) ShardCutoffLingerDuration() time.Duration {
	return o.shardCutoffLingerDuration
}

func (o *options) SetStagedPlacementWatcherOptions(value placement.StagedPlacementWatcherOptions) Options {
	opts := *o
	opts.watcherOpts = value
	return &opts
}

func (o *options) StagedPlacementWatcherOptions() placement.StagedPlacementWatcherOptions {
	return o.watcherOpts
}

func (o *options) SetConnectionOptions(value ConnectionOptions) Options {
	opts := *o
	opts.connOpts = value
	return &opts
}

func (o *options) ConnectionOptions() ConnectionOptions {
	return o.connOpts
}

func (o *options) SetFlushSize(value int) Options {
	opts := *o
	opts.flushSize = value
	return &opts
}

func (o *options) FlushSize() int {
	return o.flushSize
}

func (o *options) SetMaxTimerBatchSize(value int) Options {
	opts := *o
	opts.maxTimerBatchSize = value
	return &opts
}

func (o *options) MaxTimerBatchSize() int {
	return o.maxTimerBatchSize
}

func (o *options) SetInstanceQueueSize(value int) Options {
	opts := *o
	opts.instanceQueueSize = value
	return &opts
}

func (o *options) InstanceQueueSize() int {
	return o.instanceQueueSize
}

func (o *options) SetQueueDropType(value DropType) Options {
	opts := *o
	opts.dropType = value
	return &opts
}

func (o *options) QueueDropType() DropType {
	return o.dropType
}

func (o *options) SetMaxBatchSize(value int) Options {
	opts := *o
	if value < 0 {
		value = defaultMaxBatchSize
	}
	opts.maxBatchSize = value

	return &opts
}

func (o *options) MaxBatchSize() int {
	return o.maxBatchSize
}

func (o *options) SetBatchFlushDeadline(value time.Duration) Options {
	opts := *o
	if value < 0 {
		value = defaultBatchFlushDeadline
	}
	opts.batchFlushDeadline = value
	return &opts
}

func (o *options) BatchFlushDeadline() time.Duration {
	return o.batchFlushDeadline
}

func (o *options) SetRWOptions(value xio.Options) Options {
	opts := *o
	opts.rwOpts = value
	return &opts
}

func (o *options) RWOptions() xio.Options {
	return o.rwOpts
}
