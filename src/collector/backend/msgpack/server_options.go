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

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"

	"github.com/spaolacci/murmur3"
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
)

// ShardFn maps a id to a shard given the total number of shards.
type ShardFn func(id []byte, numShards int) uint32

// ServerOptions provide a set of server options.
type ServerOptions interface {
	// SetClockOptions sets the clock options.
	SetClockOptions(value clock.Options) ServerOptions

	// ClockOptions returns the clock options.
	ClockOptions() clock.Options

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) ServerOptions

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetShardFn sets the sharding function.
	SetShardFn(value ShardFn) ServerOptions

	// ShardFn returns the sharding function.
	ShardFn() ShardFn

	// SetStagedPlacementWatcherOptions sets the staged placement watcher options.
	SetStagedPlacementWatcherOptions(value services.StagedPlacementWatcherOptions) ServerOptions

	// StagedPlacementWatcherOptions returns the staged placement watcher options.
	StagedPlacementWatcherOptions() services.StagedPlacementWatcherOptions

	// SetShardCutoverWarmupDuration sets the warm up duration for traffic cut over to a shard.
	SetShardCutoverWarmupDuration(value time.Duration) ServerOptions

	// ShardCutoverWarmupDuration returns the warm up duration for traffic cut over to a shard.
	ShardCutoverWarmupDuration() time.Duration

	// SetShardCutoffLingerDuration sets the linger duration for traffic cut off from a shard.
	SetShardCutoffLingerDuration(value time.Duration) ServerOptions

	// ShardCutoffLingerDuration returns the linger duration for traffic cut off from a shard.
	ShardCutoffLingerDuration() time.Duration

	// SetConnectionOptions sets the connection options.
	SetConnectionOptions(value ConnectionOptions) ServerOptions

	// ConnectionOptions returns the connection options.
	ConnectionOptions() ConnectionOptions

	// SetFlushSize sets the buffer size to trigger a flush.
	SetFlushSize(value int) ServerOptions

	// FlushSize returns the buffer size to trigger a flush.
	FlushSize() int

	// SetMaxTimerBatchSize sets the maximum timer batch size.
	SetMaxTimerBatchSize(value int) ServerOptions

	// MaxTimerBatchSize returns the maximum timer batch size.
	MaxTimerBatchSize() int

	// SetInstanceQueueSize sets the instance queue size.
	SetInstanceQueueSize(value int) ServerOptions

	// InstanceQueueSize returns the instance queue size.
	InstanceQueueSize() int

	// SetBufferedEncoderPool sets the buffered encoder pool.
	SetBufferedEncoderPool(value msgpack.BufferedEncoderPool) ServerOptions

	// BufferedEncoderPool returns the buffered encoder pool.
	BufferedEncoderPool() msgpack.BufferedEncoderPool
}

type serverOptions struct {
	clockOpts                  clock.Options
	instrumentOpts             instrument.Options
	shardFn                    ShardFn
	shardCutoverWarmupDuration time.Duration
	shardCutoffLingerDuration  time.Duration
	watcherOpts                services.StagedPlacementWatcherOptions
	connOpts                   ConnectionOptions
	flushSize                  int
	maxTimerBatchSize          int
	instanceQueueSize          int
	encoderPool                msgpack.BufferedEncoderPool
}

// NewServerOptions create a new set of server options.
func NewServerOptions() ServerOptions {
	encoderPool := msgpack.NewBufferedEncoderPool(nil)
	encoderPool.Init(func() msgpack.BufferedEncoder {
		return msgpack.NewPooledBufferedEncoder(encoderPool)
	})
	return &serverOptions{
		clockOpts:                  clock.NewOptions(),
		instrumentOpts:             instrument.NewOptions(),
		shardFn:                    defaultShardFn,
		shardCutoverWarmupDuration: defaultShardCutoverWarmupDuration,
		shardCutoffLingerDuration:  defaultShardCutoffLingerDuration,
		watcherOpts:                placement.NewStagedPlacementWatcherOptions(),
		connOpts:                   NewConnectionOptions(),
		flushSize:                  defaultFlushSize,
		maxTimerBatchSize:          defaultMaxTimerBatchSize,
		instanceQueueSize:          defaultInstanceQueueSize,
		encoderPool:                encoderPool,
	}
}

func (o *serverOptions) SetClockOptions(value clock.Options) ServerOptions {
	opts := *o
	opts.clockOpts = value
	return &opts
}

func (o *serverOptions) ClockOptions() clock.Options {
	return o.clockOpts
}

func (o *serverOptions) SetInstrumentOptions(value instrument.Options) ServerOptions {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *serverOptions) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *serverOptions) SetShardFn(value ShardFn) ServerOptions {
	opts := *o
	opts.shardFn = value
	return &opts
}

func (o *serverOptions) ShardFn() ShardFn {
	return o.shardFn
}

func (o *serverOptions) SetShardCutoverWarmupDuration(value time.Duration) ServerOptions {
	opts := *o
	opts.shardCutoverWarmupDuration = value
	return &opts
}

func (o *serverOptions) ShardCutoverWarmupDuration() time.Duration {
	return o.shardCutoverWarmupDuration
}

func (o *serverOptions) SetShardCutoffLingerDuration(value time.Duration) ServerOptions {
	opts := *o
	opts.shardCutoffLingerDuration = value
	return &opts
}

func (o *serverOptions) ShardCutoffLingerDuration() time.Duration {
	return o.shardCutoffLingerDuration
}

func (o *serverOptions) SetStagedPlacementWatcherOptions(value services.StagedPlacementWatcherOptions) ServerOptions {
	opts := *o
	opts.watcherOpts = value
	return &opts
}

func (o *serverOptions) StagedPlacementWatcherOptions() services.StagedPlacementWatcherOptions {
	return o.watcherOpts
}

func (o *serverOptions) SetConnectionOptions(value ConnectionOptions) ServerOptions {
	opts := *o
	opts.connOpts = value
	return &opts
}

func (o *serverOptions) ConnectionOptions() ConnectionOptions {
	return o.connOpts
}

func (o *serverOptions) SetFlushSize(value int) ServerOptions {
	opts := *o
	opts.flushSize = value
	return &opts
}

func (o *serverOptions) FlushSize() int {
	return o.flushSize
}

func (o *serverOptions) SetMaxTimerBatchSize(value int) ServerOptions {
	opts := *o
	opts.maxTimerBatchSize = value
	return &opts
}

func (o *serverOptions) MaxTimerBatchSize() int {
	return o.maxTimerBatchSize
}

func (o *serverOptions) SetInstanceQueueSize(value int) ServerOptions {
	opts := *o
	opts.instanceQueueSize = value
	return &opts
}

func (o *serverOptions) InstanceQueueSize() int {
	return o.instanceQueueSize
}

func (o *serverOptions) SetBufferedEncoderPool(value msgpack.BufferedEncoderPool) ServerOptions {
	opts := *o
	opts.encoderPool = value
	return &opts
}

func (o *serverOptions) BufferedEncoderPool() msgpack.BufferedEncoderPool {
	return o.encoderPool
}

func defaultShardFn(id []byte, numShards int) uint32 {
	return murmur3.Sum32(id) % uint32(numShards)
}
