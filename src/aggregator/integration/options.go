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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/aggregator/aggregator"
	aggclient "github.com/m3db/m3/src/aggregator/client"
	"github.com/m3db/m3/src/aggregator/sharding"
	cluster "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
	memcluster "github.com/m3db/m3/src/cluster/mem"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/msg/topic"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"
)

const (
	defaultRawTCPAddr                 = "localhost:6000"
	defaultHTTPAddr                   = "localhost:6001"
	defaultM3MsgAddr                  = "localhost:6002"
	defaultServerStateChangeTimeout   = 5 * time.Second
	defaultClientBatchSize            = 1440
	defaultWorkerPoolSize             = 4
	defaultServiceName                = "m3aggregator"
	defaultInstanceID                 = "localhost"
	defaultPlacementKVKey             = "/placement"
	defaultElectionKeyFmt             = "/shardset/%d/lock"
	defaultFlushTimesKeyFmt           = "/shardset/%d/flush"
	defaultTopicName                  = "aggregator_ingest"
	defaultShardSetID                 = 0
	defaultElectionStateChangeTimeout = 10 * time.Second
	defaultEntryCheckInterval         = time.Second
	defaultJitterEnabled              = true
	defaultDiscardNaNAggregatedValues = true
	defaultEntryTTL                   = time.Hour
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

	// SetM3MsgAddr sets the M3msg server address.
	SetM3MsgAddr(value string) testServerOptions

	// M3MsgAddr returns the M3msg server address.
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

	// SetClusterClient sets the cluster client.
	SetClusterClient(value cluster.Client) testServerOptions

	// ClusterClient returns the cluster client.
	ClusterClient() cluster.Client

	// SetTopicService sets the topic service.
	SetTopicService(value topic.Service) testServerOptions

	// TopicService returns the topic service.
	TopicService() topic.Service

	// SetTopicName sets the topic name.
	SetTopicName(value string) testServerOptions

	// TopicName return the topic name.
	TopicName() string

	// SetAggregatorClientType sets the aggregator client type.
	SetAggregatorClientType(value aggclient.AggregatorClientType) testServerOptions

	// AggregatorClientType returns the agggregator client type.
	AggregatorClientType() aggclient.AggregatorClientType

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

	// SetBufferForPastTimedMetric sets the BufferForPastTimedMetric.
	SetBufferForPastTimedMetric(value time.Duration) testServerOptions

	// BufferForPastTimedMetric is how long to wait for timed metrics to arrive.
	BufferForPastTimedMetric() time.Duration

	// SetEntryTTL sets the EntryTTL.
	SetEntryTTL(value time.Duration) testServerOptions

	// EntryTTL is how long to wait before expiring the aggregation when it's inactive.
	EntryTTL() time.Duration
}

type serverOptions struct {
	clockOpts                     clock.Options
	instrumentOpts                instrument.Options
	aggTypesOpts                  aggregation.TypesOptions
	rawTCPAddr                    string
	httpAddr                      string
	m3MsgAddr                     string
	instanceID                    string
	electionKeyFmt                string
	electionCluster               *testCluster
	shardSetID                    uint32
	shardFn                       sharding.ShardFn
	placementKVKey                string
	flushTimesKeyFmt              string
	clusterClient                 cluster.Client
	topicService                  topic.Service
	topicName                     string
	serverStateChangeTimeout      time.Duration
	workerPoolSize                int
	clientType                    aggclient.AggregatorClientType
	clientBatchSize               int
	clientConnectionOpts          aggclient.ConnectionOptions
	electionStateChangeTimeout    time.Duration
	entryCheckInterval            time.Duration
	jitterEnabled                 bool
	maxJitterFn                   aggregator.FlushJitterFn
	maxAllowedForwardingDelayFn   aggregator.MaxAllowedForwardingDelayFn
	discardNaNAggregatedValues    bool
	resendBufferForPastTimeMetric time.Duration
	entryTTL                      time.Duration
}

func newTestServerOptions(t *testing.T) testServerOptions {
	clientType, err := getAggregatorClientTypeFromEnv()
	require.NoError(t, err)
	instanceID := defaultRawTCPAddr
	if clientType == aggclient.M3MsgAggregatorClient {
		instanceID = defaultM3MsgAddr
	}

	aggTypesOpts := aggregation.NewTypesOptions().
		SetCounterTypeStringTransformFn(aggregation.EmptyTransform).
		SetTimerTypeStringTransformFn(aggregation.SuffixTransform).
		SetGaugeTypeStringTransformFn(aggregation.EmptyTransform)
	connOpts := aggclient.NewConnectionOptions().SetWriteTimeout(time.Second)
	return &serverOptions{
		rawTCPAddr:                  defaultRawTCPAddr,
		httpAddr:                    defaultHTTPAddr,
		m3MsgAddr:                   defaultM3MsgAddr,
		clockOpts:                   clock.NewOptions(),
		instrumentOpts:              instrument.NewOptions(),
		aggTypesOpts:                aggTypesOpts,
		instanceID:                  instanceID,
		electionKeyFmt:              defaultElectionKeyFmt,
		shardSetID:                  defaultShardSetID,
		shardFn:                     sharding.Murmur32Hash.MustShardFn(),
		placementKVKey:              defaultPlacementKVKey,
		flushTimesKeyFmt:            defaultFlushTimesKeyFmt,
		clusterClient:               memcluster.New(kv.NewOverrideOptions()),
		topicService:                nil,
		topicName:                   defaultTopicName,
		serverStateChangeTimeout:    defaultServerStateChangeTimeout,
		workerPoolSize:              defaultWorkerPoolSize,
		clientType:                  clientType,
		clientBatchSize:             defaultClientBatchSize,
		clientConnectionOpts:        connOpts,
		electionStateChangeTimeout:  defaultElectionStateChangeTimeout,
		jitterEnabled:               defaultJitterEnabled,
		entryCheckInterval:          defaultEntryCheckInterval,
		maxJitterFn:                 defaultMaxJitterFn,
		maxAllowedForwardingDelayFn: defaultMaxAllowedForwardingDelayFn,
		discardNaNAggregatedValues:  defaultDiscardNaNAggregatedValues,
		entryTTL:                    defaultEntryTTL,
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

func (o *serverOptions) M3MsgAddr() string {
	return o.m3MsgAddr
}

func (o *serverOptions) SetM3MsgAddr(value string) testServerOptions {
	opts := *o
	opts.m3MsgAddr = value
	return &opts
}

func (o *serverOptions) HTTPAddr() string {
	return o.httpAddr
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

func (o *serverOptions) SetClusterClient(value cluster.Client) testServerOptions {
	opts := *o
	opts.clusterClient = value
	return &opts
}

func (o *serverOptions) ClusterClient() cluster.Client {
	return o.clusterClient
}

func (o *serverOptions) SetTopicService(value topic.Service) testServerOptions {
	opts := *o
	opts.topicService = value
	return &opts
}

func (o *serverOptions) TopicService() topic.Service {
	return o.topicService
}

func (o *serverOptions) SetTopicName(value string) testServerOptions {
	opts := *o
	opts.topicName = value
	return &opts
}

func (o *serverOptions) TopicName() string {
	return o.topicName
}

func (o *serverOptions) SetAggregatorClientType(value aggclient.AggregatorClientType) testServerOptions {
	opts := *o
	opts.clientType = value
	return &opts
}

func (o *serverOptions) AggregatorClientType() aggclient.AggregatorClientType {
	return o.clientType
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

func (o *serverOptions) SetBufferForPastTimedMetric(value time.Duration) testServerOptions {
	opts := *o
	opts.resendBufferForPastTimeMetric = value
	return &opts
}

func (o *serverOptions) BufferForPastTimedMetric() time.Duration {
	return o.resendBufferForPastTimeMetric
}

func (o *serverOptions) SetDiscardNaNAggregatedValues(value bool) testServerOptions {
	opts := *o
	opts.discardNaNAggregatedValues = value
	return &opts
}

func (o *serverOptions) DiscardNaNAggregatedValues() bool {
	return o.discardNaNAggregatedValues
}

func (o *serverOptions) SetEntryTTL(value time.Duration) testServerOptions {
	opts := *o
	opts.entryTTL = value
	return &opts
}

func (o *serverOptions) EntryTTL() time.Duration {
	return o.entryTTL
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
