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

package integration

import (
	"time"

	"github.com/m3db/m3/src/aggregator/aggregator"
	aggclient "github.com/m3db/m3/src/aggregator/client"
	"github.com/m3db/m3/src/aggregator/sharding"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/mem"
	m3msgconfig "github.com/m3db/m3/src/cmd/services/m3coordinator/server/m3msg"
	producerconfig "github.com/m3db/m3/src/msg/producer/config"

	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"

	yaml "gopkg.in/yaml.v2"
)

const (
	defaultRawTCPAddr                 = "localhost:6000"
	defaultHTTPAddr                   = "localhost:6001"
	defaultM3MsgAddr                  = ""
	defaultServerStateChangeTimeout   = 5 * time.Second
	defaultClientBatchSize            = 1440
	defaultWorkerPoolSize             = 4
	defaultInstanceID                 = "localhost"
	defaultPlacementKVKey             = "/placement"
	defaultElectionKeyFmt             = "/shardset/%d/lock"
	defaultFlushTimesKeyFmt           = "/shardset/%d/flush"
	defaultShardSetID                 = 0
	defaultElectionStateChangeTimeout = 10 * time.Second
	defaultEntryCheckInterval         = time.Second
	defaultJitterEnabled              = true
	defaultDiscardNaNAggregatedValues = true
	m3msgConsumerConfigStr            = `
server:
  listenAddress: "localhost:6009"
  keepAliveEnabled: true
  keepAlivePeriod: 5s
handler:
  protobufDecoderPool:
    size: 100
    watermark:
      low: 0.001
      high: 0.002
consumer:
  messagePool:
    size: 5
    maxBufferReuseSize: 65536
  ackFlushInterval: 100ms
  ackBufferSize: 100
  connectionWriteBufferSize: 200
  connectionReadBufferSize: 300
  encoder:
    maxMessageSize: 10485760
    bytesPool:
      watermark:
        low: 0.001
  decoder:
    maxMessageSize: 10485760
    bytesPool:
      watermark:
        high: 0.002
`
	m3msgProducerConfigStr = `
buffer:
  closeCheckInterval: 200ms
  cleanupRetry:
    initialBackoff: 100ms
    maxBackoff: 200ms
writer:
  topicName: passthroughTopic
  topicWatchInitTimeout: 100ms
  placementWatchInitTimeout: 100ms
  messagePool:
    size: 100
  messageRetry:
    initialBackoff: 100ms
    maxBackoff: 500ms
  messageQueueNewWritesScanInterval: 10ms
  messageQueueFullScanInterval: 50ms
  closeCheckInterval: 200ms
  ackErrorRetry:
    initialBackoff: 100ms
    maxBackoff: 500ms
  connection:
    dialTimeout: 500ms
    writeTimeout: 500ms
    keepAlivePeriod: 2s
    retry:
      initialBackoff: 50ms
      maxBackoff: 200ms
    flushInterval: 50ms
    writeBufferSize: 4096
    resetDelay: 50ms
`
)

type testServerOptions interface {
	// SetClockOptions sets the clock options.
	SetClockOptions(value clock.Options) testServerOptions

	// ClockOptions returns the clock options.
	ClockOptions() clock.Options

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) testServerOptions

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetAggregationTypesOptions sets the aggregation types options.
	SetAggregationTypesOptions(value aggregation.TypesOptions) testServerOptions

	// AggregationTypesOptions returns the aggregation types options.
	AggregationTypesOptions() aggregation.TypesOptions

	// SetRawTCPAddr sets the raw TCP server address.
	SetRawTCPAddr(value string) testServerOptions

	// RawTCPAddr returns the raw TCP server address.
	RawTCPAddr() string

	// SetHTTPAddr sets the http server address.
	SetHTTPAddr(value string) testServerOptions

	// HTTPAddr returns the http server address.
	HTTPAddr() string

	// SetM3MsgAddr sets the m3msg server address.
	SetM3MsgAddr(value string) testServerOptions

	// M3MsgAddr returns the m3msg server address.
	M3MsgAddr() string

	// SetInstanceID sets the instance id.
	SetInstanceID(value string) testServerOptions

	// InstanceID returns the instance id.
	InstanceID() string

	// SetElectionKeyFmt sets the election key format.
	SetElectionKeyFmt(value string) testServerOptions

	// ElectionKeyFmt returns the election key format.
	ElectionKeyFmt() string

	// SetElectionCluster sets the test KV cluster for election.
	SetElectionCluster(value *testCluster) testServerOptions

	// ElectionCluster returns the test KV cluster for election.
	ElectionCluster() *testCluster

	// SetShardSetID sets the shard set id.
	SetShardSetID(value uint32) testServerOptions

	// ShardSetID returns the shard set id.
	ShardSetID() uint32

	// SetShardFn sets the sharding function.
	SetShardFn(value sharding.ShardFn) testServerOptions

	// ShardFn returns the sharding function.
	ShardFn() sharding.ShardFn

	// SetPlacementKVKey sets the placement kv key.
	SetPlacementKVKey(value string) testServerOptions

	// PlacementKVKey returns the placement kv key.
	PlacementKVKey() string

	// SetFlushTimesKeyFmt sets the flush times key format.
	SetFlushTimesKeyFmt(value string) testServerOptions

	// FlushTimesKeyFmt returns the flush times key format.
	FlushTimesKeyFmt() string

	// SetKVStore sets the key value store.
	SetKVStore(value kv.Store) testServerOptions

	// KVStore returns the key value store.
	KVStore() kv.Store

	// SetClientBatchSize sets the client-side batch size.
	SetClientBatchSize(value int) testServerOptions

	// ClientBatchSize returns the client-side batch size.
	ClientBatchSize() int

	// SetClientConnectionOptions sets the client-side connection options.
	SetClientConnectionOptions(value aggclient.ConnectionOptions) testServerOptions

	// ClientConnectionOptions returns the client-side connection options.
	ClientConnectionOptions() aggclient.ConnectionOptions

	// SetServerStateChangeTimeout sets the client connect timeout.
	SetServerStateChangeTimeout(value time.Duration) testServerOptions

	// ServerStateChangeTimeout returns the client connect timeout.
	ServerStateChangeTimeout() time.Duration

	// SetElectionStateChangeTimeout sets the election state change timeout.
	SetElectionStateChangeTimeout(value time.Duration) testServerOptions

	// ElectionStateChangeTimeout returns the election state change timeout.
	ElectionStateChangeTimeout() time.Duration

	// SetWorkerPoolSize sets the number of workers in the worker pool.
	SetWorkerPoolSize(value int) testServerOptions

	// WorkerPoolSize returns the number of workers in the worker pool.
	WorkerPoolSize() int

	// SetEntryCheckInterval sets the entry check interval.
	SetEntryCheckInterval(value time.Duration) testServerOptions

	// EntryCheckInterval returns the entry check interval.
	EntryCheckInterval() time.Duration

	// SetJitterEnabled sets whether jittering is enabled.
	SetJitterEnabled(value bool) testServerOptions

	// JitterEnabled returns whether jittering is enabled.
	JitterEnabled() bool

	// SetMaxJitterFn sets the max flush jittering function.
	SetMaxJitterFn(value aggregator.FlushJitterFn) testServerOptions

	// MaxJitterFn returns the max flush jittering function.
	MaxJitterFn() aggregator.FlushJitterFn

	// SetMaxAllowedForwardingDelayFn sets the maximum allowed forwarding delay function.
	SetMaxAllowedForwardingDelayFn(value aggregator.MaxAllowedForwardingDelayFn) testServerOptions

	// MaxAllowedForwardingDelayFn returns the maximum allowed forwarding delay function.
	MaxAllowedForwardingDelayFn() aggregator.MaxAllowedForwardingDelayFn

	// SetDiscardNaNAggregatedValues determines whether NaN aggregated values are discarded.
	SetDiscardNaNAggregatedValues(value bool) testServerOptions

	// DiscardNaNAggregatedValues determines whether NaN aggregated values are discarded.
	DiscardNaNAggregatedValues() bool
}

// nolint: maligned
type serverOptions struct {
	clockOpts                   clock.Options
	instrumentOpts              instrument.Options
	aggTypesOpts                aggregation.TypesOptions
	rawTCPAddr                  string
	httpAddr                    string
	m3msgAddr                   string
	instanceID                  string
	electionKeyFmt              string
	electionCluster             *testCluster
	shardSetID                  uint32
	shardFn                     sharding.ShardFn
	placementKVKey              string
	flushTimesKeyFmt            string
	kvStore                     kv.Store
	serverStateChangeTimeout    time.Duration
	workerPoolSize              int
	clientBatchSize             int
	clientConnectionOpts        aggclient.ConnectionOptions
	electionStateChangeTimeout  time.Duration
	entryCheckInterval          time.Duration
	jitterEnabled               bool
	maxJitterFn                 aggregator.FlushJitterFn
	maxAllowedForwardingDelayFn aggregator.MaxAllowedForwardingDelayFn
	discardNaNAggregatedValues  bool
}

func newTestServerOptions() testServerOptions {
	aggTypesOpts := aggregation.NewTypesOptions().
		SetCounterTypeStringTransformFn(aggregation.EmptyTransform).
		SetTimerTypeStringTransformFn(aggregation.SuffixTransform).
		SetGaugeTypeStringTransformFn(aggregation.EmptyTransform)
	return &serverOptions{
		rawTCPAddr:                  defaultRawTCPAddr,
		httpAddr:                    defaultHTTPAddr,
		m3msgAddr:                   defaultM3MsgAddr,
		clockOpts:                   clock.NewOptions(),
		instrumentOpts:              instrument.NewOptions(),
		aggTypesOpts:                aggTypesOpts,
		instanceID:                  defaultInstanceID,
		electionKeyFmt:              defaultElectionKeyFmt,
		shardSetID:                  defaultShardSetID,
		shardFn:                     sharding.Murmur32Hash.MustShardFn(),
		placementKVKey:              defaultPlacementKVKey,
		flushTimesKeyFmt:            defaultFlushTimesKeyFmt,
		kvStore:                     mem.NewStore(),
		serverStateChangeTimeout:    defaultServerStateChangeTimeout,
		workerPoolSize:              defaultWorkerPoolSize,
		clientBatchSize:             defaultClientBatchSize,
		clientConnectionOpts:        aggclient.NewConnectionOptions(),
		electionStateChangeTimeout:  defaultElectionStateChangeTimeout,
		jitterEnabled:               defaultJitterEnabled,
		entryCheckInterval:          defaultEntryCheckInterval,
		maxJitterFn:                 defaultMaxJitterFn,
		maxAllowedForwardingDelayFn: defaultMaxAllowedForwardingDelayFn,
		discardNaNAggregatedValues:  defaultDiscardNaNAggregatedValues,
	}
}

func (o *serverOptions) SetClockOptions(value clock.Options) testServerOptions {
	opts := *o
	opts.clockOpts = value
	return &opts
}

func (o *serverOptions) ClockOptions() clock.Options {
	return o.clockOpts
}

func (o *serverOptions) SetInstrumentOptions(value instrument.Options) testServerOptions {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *serverOptions) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *serverOptions) SetAggregationTypesOptions(value aggregation.TypesOptions) testServerOptions {
	opts := *o
	opts.aggTypesOpts = value
	return &opts
}

func (o *serverOptions) AggregationTypesOptions() aggregation.TypesOptions {
	return o.aggTypesOpts
}

func (o *serverOptions) SetRawTCPAddr(value string) testServerOptions {
	opts := *o
	opts.rawTCPAddr = value
	return &opts
}

func (o *serverOptions) RawTCPAddr() string {
	return o.rawTCPAddr
}

func (o *serverOptions) SetHTTPAddr(value string) testServerOptions {
	opts := *o
	opts.httpAddr = value
	return &opts
}

func (o *serverOptions) HTTPAddr() string {
	return o.httpAddr
}

func (o *serverOptions) SetM3MsgAddr(value string) testServerOptions {
	opts := *o
	opts.m3msgAddr = value
	return &opts
}

func (o *serverOptions) M3MsgAddr() string {
	return o.m3msgAddr
}

func (o *serverOptions) SetInstanceID(value string) testServerOptions {
	opts := *o
	opts.instanceID = value
	return &opts
}

func (o *serverOptions) InstanceID() string {
	return o.instanceID
}

func (o *serverOptions) SetElectionKeyFmt(value string) testServerOptions {
	opts := *o
	opts.electionKeyFmt = value
	return &opts
}

func (o *serverOptions) ElectionKeyFmt() string {
	return o.electionKeyFmt
}

func (o *serverOptions) SetElectionCluster(value *testCluster) testServerOptions {
	opts := *o
	opts.electionCluster = value
	return &opts
}

func (o *serverOptions) ElectionCluster() *testCluster {
	return o.electionCluster
}

func (o *serverOptions) SetShardSetID(value uint32) testServerOptions {
	opts := *o
	opts.shardSetID = value
	return &opts
}

func (o *serverOptions) ShardSetID() uint32 {
	return o.shardSetID
}

func (o *serverOptions) SetShardFn(value sharding.ShardFn) testServerOptions {
	opts := *o
	opts.shardFn = value
	return &opts
}

func (o *serverOptions) ShardFn() sharding.ShardFn {
	return o.shardFn
}

func (o *serverOptions) SetPlacementKVKey(value string) testServerOptions {
	opts := *o
	opts.placementKVKey = value
	return &opts
}

func (o *serverOptions) PlacementKVKey() string {
	return o.placementKVKey
}

func (o *serverOptions) SetFlushTimesKeyFmt(value string) testServerOptions {
	opts := *o
	opts.flushTimesKeyFmt = value
	return &opts
}

func (o *serverOptions) FlushTimesKeyFmt() string {
	return o.flushTimesKeyFmt
}

func (o *serverOptions) SetKVStore(value kv.Store) testServerOptions {
	opts := *o
	opts.kvStore = value
	return &opts
}

func (o *serverOptions) KVStore() kv.Store {
	return o.kvStore
}

func (o *serverOptions) SetClientBatchSize(value int) testServerOptions {
	opts := *o
	opts.clientBatchSize = value
	return &opts
}

func (o *serverOptions) ClientBatchSize() int {
	return o.clientBatchSize
}

func (o *serverOptions) SetClientConnectionOptions(value aggclient.ConnectionOptions) testServerOptions {
	opts := *o
	opts.clientConnectionOpts = value
	return &opts
}

func (o *serverOptions) ClientConnectionOptions() aggclient.ConnectionOptions {
	return o.clientConnectionOpts
}

func (o *serverOptions) SetServerStateChangeTimeout(value time.Duration) testServerOptions {
	opts := *o
	opts.serverStateChangeTimeout = value
	return &opts
}

func (o *serverOptions) ServerStateChangeTimeout() time.Duration {
	return o.serverStateChangeTimeout
}

func (o *serverOptions) SetElectionStateChangeTimeout(value time.Duration) testServerOptions {
	opts := *o
	opts.electionStateChangeTimeout = value
	return &opts
}

func (o *serverOptions) ElectionStateChangeTimeout() time.Duration {
	return o.electionStateChangeTimeout
}

func (o *serverOptions) SetWorkerPoolSize(value int) testServerOptions {
	opts := *o
	opts.workerPoolSize = value
	return &opts
}

func (o *serverOptions) WorkerPoolSize() int {
	return o.workerPoolSize
}

func (o *serverOptions) SetEntryCheckInterval(value time.Duration) testServerOptions {
	opts := *o
	opts.entryCheckInterval = value
	return &opts
}

func (o *serverOptions) EntryCheckInterval() time.Duration {
	return o.entryCheckInterval
}

func (o *serverOptions) SetJitterEnabled(value bool) testServerOptions {
	opts := *o
	opts.jitterEnabled = value
	return &opts
}

func (o *serverOptions) JitterEnabled() bool {
	return o.jitterEnabled
}

func (o *serverOptions) SetMaxJitterFn(value aggregator.FlushJitterFn) testServerOptions {
	opts := *o
	opts.maxJitterFn = value
	return &opts
}

func (o *serverOptions) MaxJitterFn() aggregator.FlushJitterFn {
	return o.maxJitterFn
}

func (o *serverOptions) SetMaxAllowedForwardingDelayFn(value aggregator.MaxAllowedForwardingDelayFn) testServerOptions {
	opts := *o
	opts.maxAllowedForwardingDelayFn = value
	return &opts
}

func (o *serverOptions) MaxAllowedForwardingDelayFn() aggregator.MaxAllowedForwardingDelayFn {
	return o.maxAllowedForwardingDelayFn
}

func (o *serverOptions) SetDiscardNaNAggregatedValues(value bool) testServerOptions {
	opts := *o
	opts.discardNaNAggregatedValues = value
	return &opts
}

func (o *serverOptions) DiscardNaNAggregatedValues() bool {
	return o.discardNaNAggregatedValues
}

func defaultMaxJitterFn(interval time.Duration) time.Duration {
	return time.Duration(0.75 * float64(interval))
}

func defaultMaxAllowedForwardingDelayFn(
	resolution time.Duration,
	numForwardedTimes int,
) time.Duration {
	return resolution + time.Second*time.Duration(numForwardedTimes)
}

func mustNewM3MsgConsumerConfig() *m3msgconfig.Configuration {
	var m3msgServerConfig m3msgconfig.Configuration
	err := yaml.Unmarshal([]byte(m3msgConsumerConfigStr), &m3msgServerConfig)
	if err != nil {
		panic(err.Error())
	}
	return &m3msgServerConfig
}

func mustNewM3MsgProducerConfig() *producerconfig.ProducerConfiguration {
	var conf producerconfig.ProducerConfiguration
	err := yaml.Unmarshal([]byte(m3msgProducerConfigStr), &conf)
	if err != nil {
		panic(err.Error())
	}
	return &conf
}
