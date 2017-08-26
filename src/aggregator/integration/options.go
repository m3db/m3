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

	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/mem"
)

// nolint: megacheck
const (
	defaultServerStateChangeTimeout   = 5 * time.Second
	defaultClientBatchSize            = 1440
	defaultClientConnectTimeout       = time.Second
	defaultWorkerPoolSize             = 4
	defaultInstanceID                 = "localhost:6000"
	defaultNumShards                  = 1024
	defaultPlacementKVKey             = "/placement"
	defaultElectionKeyFmt             = "/shardset/%d/lock"
	defaultFlushTimesKeyFmt           = "/shardset/%d/flush"
	defaultShardSetID                 = 0
	defaultElectionStateChangeTimeout = time.Second
	defaultJitterEnabled              = true
)

// nolint: megacheck
type testOptions interface {
	// SetMsgpackAddr sets the msgpack server address.
	SetMsgpackAddr(value string) testOptions

	// MsgpackAddr returns the msgpack server address.
	MsgpackAddr() string

	// SetHTTPAddr sets the http server address.
	SetHTTPAddr(value string) testOptions

	// HTTPAddr returns the http server address.
	HTTPAddr() string

	// SetInstanceID sets the instance id.
	SetInstanceID(value string) testOptions

	// InstanceID returns the instance id.
	InstanceID() string

	// SetNumShards sets the number of shards.
	SetNumShards(value int) testOptions

	// NumShards returns the number of shards.
	NumShards() int

	// SetPlacementKVKey sets the placement kv key.
	SetPlacementKVKey(value string) testOptions

	// PlacementKVKey returns the placement kv key.
	PlacementKVKey() string

	// SetElectionKeyFmt sets the election key format.
	SetElectionKeyFmt(value string) testOptions

	// ElectionKeyFmt returns the election key format.
	ElectionKeyFmt() string

	// SetShardSetID sets the shard set id.
	SetShardSetID(value uint32) testOptions

	// ShardSetID returns the shard set id.
	ShardSetID() uint32

	// SetFlushTimesKeyFmt sets the flush times key format.
	SetFlushTimesKeyFmt(value string) testOptions

	// FlushTimesKeyFmt returns the flush times key format.
	FlushTimesKeyFmt() string

	// SetKVStore sets the key value store.
	SetKVStore(value kv.Store) testOptions

	// KVStore returns the key value store.
	KVStore() kv.Store

	// SetClientBatchSize sets the client-side batch size.
	SetClientBatchSize(value int) testOptions

	// ClientBatchSize returns the client-side batch size.
	ClientBatchSize() int

	// SetClientConnectTimeout sets the client-side connect timeout.
	SetClientConnectTimeout(value time.Duration) testOptions

	// ClientConnectTimeout returns the client-side connect timeout.
	ClientConnectTimeout() time.Duration

	// SetServerStateChangeTimeout sets the client connect timeout.
	SetServerStateChangeTimeout(value time.Duration) testOptions

	// ServerStateChangeTimeout returns the client connect timeout.
	ServerStateChangeTimeout() time.Duration

	// SetElectionStateChangeTimeout sets the election state change timeout.
	SetElectionStateChangeTimeout(value time.Duration) testOptions

	// ElectionStateChangeTimeout returns the election state change timeout.
	ElectionStateChangeTimeout() time.Duration

	// SetWorkerPoolSize sets the number of workers in the worker pool.
	SetWorkerPoolSize(value int) testOptions

	// WorkerPoolSize returns the number of workers in the worker pool.
	WorkerPoolSize() int

	// SetJitterEnabled sets whether jittering is enabled.
	SetJitterEnabled(value bool) testOptions

	// JitterEnabled returns whether jittering is enabled.
	JitterEnabled() bool

	// SetMaxJitterFn sets the max flush jittering function.
	SetMaxJitterFn(value aggregator.FlushJitterFn) testOptions

	// MaxJitterFn returns the max flush jittering function.
	MaxJitterFn() aggregator.FlushJitterFn
}

// nolint: megacheck
type options struct {
	msgpackAddr                string
	httpAddr                   string
	instanceID                 string
	numShards                  int
	placementKVKey             string
	electionKeyFmt             string
	shardSetID                 uint32
	flushTimesKeyFmt           string
	kvStore                    kv.Store
	serverStateChangeTimeout   time.Duration
	workerPoolSize             int
	clientBatchSize            int
	clientConnectTimeout       time.Duration
	electionStateChangeTimeout time.Duration
	jitterEnabled              bool
	maxJitterFn                aggregator.FlushJitterFn
}

// nolint: megacheck
func newTestOptions() testOptions {
	return &options{
		instanceID:                 defaultInstanceID,
		numShards:                  defaultNumShards,
		placementKVKey:             defaultPlacementKVKey,
		electionKeyFmt:             defaultElectionKeyFmt,
		shardSetID:                 defaultShardSetID,
		flushTimesKeyFmt:           defaultFlushTimesKeyFmt,
		kvStore:                    mem.NewStore(),
		serverStateChangeTimeout:   defaultServerStateChangeTimeout,
		workerPoolSize:             defaultWorkerPoolSize,
		clientBatchSize:            defaultClientBatchSize,
		clientConnectTimeout:       defaultClientConnectTimeout,
		electionStateChangeTimeout: defaultElectionStateChangeTimeout,
		jitterEnabled:              defaultJitterEnabled,
		maxJitterFn:                defaultMaxJitterFn,
	}
}

func (o *options) SetMsgpackAddr(value string) testOptions {
	opts := *o
	opts.msgpackAddr = value
	return &opts
}

func (o *options) MsgpackAddr() string {
	return o.msgpackAddr
}

func (o *options) SetHTTPAddr(value string) testOptions {
	opts := *o
	opts.httpAddr = value
	return &opts
}

func (o *options) HTTPAddr() string {
	return o.httpAddr
}

func (o *options) SetInstanceID(value string) testOptions {
	opts := *o
	opts.instanceID = value
	return &opts
}

func (o *options) InstanceID() string {
	return o.instanceID
}

func (o *options) SetNumShards(value int) testOptions {
	opts := *o
	opts.numShards = value
	return &opts
}

func (o *options) NumShards() int {
	return o.numShards
}

func (o *options) SetPlacementKVKey(value string) testOptions {
	opts := *o
	opts.placementKVKey = value
	return &opts
}

func (o *options) PlacementKVKey() string {
	return o.placementKVKey
}

func (o *options) SetElectionKeyFmt(value string) testOptions {
	opts := *o
	opts.electionKeyFmt = value
	return &opts
}

func (o *options) ElectionKeyFmt() string {
	return o.electionKeyFmt
}

func (o *options) SetShardSetID(value uint32) testOptions {
	opts := *o
	opts.shardSetID = value
	return &opts
}

func (o *options) ShardSetID() uint32 {
	return o.shardSetID
}

func (o *options) SetFlushTimesKeyFmt(value string) testOptions {
	opts := *o
	opts.flushTimesKeyFmt = value
	return &opts
}

func (o *options) FlushTimesKeyFmt() string {
	return o.flushTimesKeyFmt
}

func (o *options) SetKVStore(value kv.Store) testOptions {
	opts := *o
	opts.kvStore = value
	return &opts
}

func (o *options) KVStore() kv.Store {
	return o.kvStore
}

func (o *options) SetClientBatchSize(value int) testOptions {
	opts := *o
	opts.clientBatchSize = value
	return &opts
}

func (o *options) ClientBatchSize() int {
	return o.clientBatchSize
}

func (o *options) SetClientConnectTimeout(value time.Duration) testOptions {
	opts := *o
	opts.clientConnectTimeout = value
	return &opts
}

func (o *options) ClientConnectTimeout() time.Duration {
	return o.clientConnectTimeout
}

func (o *options) SetServerStateChangeTimeout(value time.Duration) testOptions {
	opts := *o
	opts.serverStateChangeTimeout = value
	return &opts
}

func (o *options) ServerStateChangeTimeout() time.Duration {
	return o.serverStateChangeTimeout
}

func (o *options) SetElectionStateChangeTimeout(value time.Duration) testOptions {
	opts := *o
	opts.electionStateChangeTimeout = value
	return &opts
}

func (o *options) ElectionStateChangeTimeout() time.Duration {
	return o.electionStateChangeTimeout
}

func (o *options) SetWorkerPoolSize(value int) testOptions {
	opts := *o
	opts.workerPoolSize = value
	return &opts
}

func (o *options) WorkerPoolSize() int {
	return o.workerPoolSize
}

func (o *options) SetJitterEnabled(value bool) testOptions {
	opts := *o
	opts.jitterEnabled = value
	return &opts
}

func (o *options) JitterEnabled() bool {
	return o.jitterEnabled
}

func (o *options) SetMaxJitterFn(value aggregator.FlushJitterFn) testOptions {
	opts := *o
	opts.maxJitterFn = value
	return &opts
}

func (o *options) MaxJitterFn() aggregator.FlushJitterFn {
	return o.maxJitterFn
}

// nolint: megacheck
func defaultMaxJitterFn(interval time.Duration) time.Duration {
	return time.Duration(0.75 * float64(interval))
}
