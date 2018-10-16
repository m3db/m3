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
	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
)

const (
	defaultFlushSize = 1440

	// defaultMaxTimerBatchSize is the default maximum timer batch size.
	// By default there is no limit on the timer batch size.
	defaultMaxTimerBatchSize = 0

	defaultInstanceQueueSize = 4096

	// By default traffic is cut over to shards 10 minutes before the designated
	// cutover time in case there are issues with the instances owning the shards.
	defaultShardCutoverWarmupDuration = 10 * time.Minute

	// By default traffic doesn't stop until one hour after the designated cutoff
	// time in case there are issues with the instances taking over the shards
	// and as such we need to switch the traffic back to the previous owner of the shards.
	defaultShardCutoffLingerDuration = time.Hour

	// By default the oldest metrics in the queue are dropped when it is full.
	defaultDropType = DropOldest
)

// Options provide a set of client options.
type Options interface {
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
}

type options struct {
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
	}
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
