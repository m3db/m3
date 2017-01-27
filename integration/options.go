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

	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/topology"
)

const (
	// defaultID is the default node ID
	defaultID = "testhost"

	// defaultServerStateChangeTimeout is the default time we wait for a server to change its state.
	defaultServerStateChangeTimeout = 5 * time.Minute

	// defaultClusterConnectionTimeout is the default time we wait for cluster connections to be established.
	defaultClusterConnectionTimeout = 2 * time.Second

	// defaultReadRequestTimeout is the default read request timeout.
	defaultReadRequestTimeout = 2 * time.Second

	// defaultWriteRequestTimeout is the default write request timeout.
	defaultWriteRequestTimeout = 2 * time.Second

	// defaultTruncateRequestTimeout is the default truncate request timeout.
	defaultTruncateRequestTimeout = 2 * time.Second

	// defaultWorkerPoolSize is the default number of workers in the worker pool.
	defaultWorkerPoolSize = 10

	// defaultUseTChannelClientForReading determines whether we use the tchannel client for reading by default.
	defaultUseTChannelClientForReading = true

	// defaultUseTChannelClientForWriting determines whether we use the tchannel client for writing by default.
	defaultUseTChannelClientForWriting = false

	// defaultUseTChannelClientForTruncation determines whether we use the tchannel client for truncation by default.
	defaultUseTChannelClientForTruncation = true

	// defaultWriteConsistencyLevel is the default write consistency level. This
	// should match the default in client/options.
	defaultWriteConsistencyLevel = topology.ConsistencyLevelMajority
)

type testOptions interface {
	// SetNamespaces sets the namespaces.
	SetNamespaces(value []namespace.Metadata) testOptions

	// Namespaces returns the namespaces.
	Namespaces() []namespace.Metadata

	// SetID sets the node ID.
	SetID(value string) testOptions

	// ID returns the node ID.
	ID() string

	// SetHTTPClusterAddr sets the http cluster address.
	SetHTTPClusterAddr(value string) testOptions

	// HTTPClusterAddr returns the http cluster address.
	HTTPClusterAddr() string

	// SetTChannelClusterAddr sets the tchannel cluster address.
	SetTChannelClusterAddr(value string) testOptions

	// TChannelClusterAddr returns the tchannel cluster address.
	TChannelClusterAddr() string

	// SetHTTPNodeAddr sets the http node address.
	SetHTTPNodeAddr(value string) testOptions

	// HTTPNodeAddr returns the http node address.
	HTTPNodeAddr() string

	// SetTChannelNodeAddr sets the tchannel node address.
	SetTChannelNodeAddr(value string) testOptions

	// TChannelNodeAddr returns the tchannel node address.
	TChannelNodeAddr() string

	// SetServerStateChangeTimeout sets the server state change timeout.
	SetServerStateChangeTimeout(value time.Duration) testOptions

	// ServerStateChangeTimeout returns the server state change timeout.
	ServerStateChangeTimeout() time.Duration

	// SetClusterConnectionTimeout sets the cluster connection timeout.
	SetClusterConnectionTimeout(value time.Duration) testOptions

	// ClusterConnectionTimeout returns the cluster connection timeout.
	ClusterConnectionTimeout() time.Duration

	// SetReadRequestTimeout sets the read request timeout.
	SetReadRequestTimeout(value time.Duration) testOptions

	// ReadRequestTimeout returns the read request timeout.
	ReadRequestTimeout() time.Duration

	// SetWriteRequestTimeout sets the write request timeout.
	SetWriteRequestTimeout(value time.Duration) testOptions

	// WriteRequestTimeout returns the write request timeout.
	WriteRequestTimeout() time.Duration

	// SetTruncateRequestTimeout sets the truncate request timeout.
	SetTruncateRequestTimeout(value time.Duration) testOptions

	// TruncateRequestTimeout returns the truncate request timeout.
	TruncateRequestTimeout() time.Duration

	// SetWorkerPoolSize sets the number of workers in the worker pool.
	SetWorkerPoolSize(value int) testOptions

	// WorkerPoolSize returns the number of workers in the worker pool.
	WorkerPoolSize() int

	// SetClusterDatabaseTopologyInitializer sets the topology initializer that
	// is used when creating a cluster database
	SetClusterDatabaseTopologyInitializer(value topology.Initializer) testOptions

	// ClusterDatabaseTopologyInitializer returns the topology initializer that
	// is used when creating a cluster database
	ClusterDatabaseTopologyInitializer() topology.Initializer

	// SetUseTChannelClientForReading sets whether we use the tchannel client for reading.
	SetUseTChannelClientForReading(value bool) testOptions

	// UseTChannelClientForReading returns whether we use the tchannel client for reading.
	UseTChannelClientForReading() bool

	// SetUseTChannelClientForWriting sets whether we use the tchannel client for writing.
	SetUseTChannelClientForWriting(value bool) testOptions

	// UseTChannelClientForWriting returns whether we use the tchannel client for writing.
	UseTChannelClientForWriting() bool

	// SetUseTChannelClientForTruncation sets whether we use the tchannel client for truncation.
	SetUseTChannelClientForTruncation(value bool) testOptions

	// UseTChannelClientForTruncation returns whether we use the tchannel client for truncation.
	UseTChannelClientForTruncation() bool

	// SetDatabaseBlockRetrieverManager sets the block retriever manager to
	// use when bootstrapping retrievable blocks instead of blocks
	// containing data.
	// If you don't wish to bootstrap retrievable blocks instead of
	// blocks containing data then do not set this manager.
	// You can opt into which namespace you wish to have this enabled for
	// by returning nil instead of a result when creating a new block retriever
	// for a namespace from the manager.
	SetDatabaseBlockRetrieverManager(
		value block.DatabaseBlockRetrieverManager,
	) testOptions

	// NewBlockRetrieverFn returns the new block retriever constructor to
	// use when bootstrapping retrievable blocks instead of blocks
	// containing data.
	DatabaseBlockRetrieverManager() block.DatabaseBlockRetrieverManager

	// SetVerifySeriesDebugFilePathPrefix sets the file path prefix for writing a debug file of series comparisons.
	SetVerifySeriesDebugFilePathPrefix(value string) testOptions

	// VerifySeriesDebugFilePathPrefix returns the file path prefix for writing a debug file of series comparisons.
	VerifySeriesDebugFilePathPrefix() string

	// WriteConsistencyLevel returns the consistency level for writing with the m3db client.
	WriteConsistencyLevel() topology.ConsistencyLevel

	// SetWriteConsistencyLevel sets the consistency level for writing with the m3db client.
	SetWriteConsistencyLevel(topology.ConsistencyLevel) testOptions
}

type options struct {
	namespaces                         []namespace.Metadata
	id                                 string
	httpClusterAddr                    string
	tchannelClusterAddr                string
	httpNodeAddr                       string
	tchannelNodeAddr                   string
	serverStateChangeTimeout           time.Duration
	clusterConnectionTimeout           time.Duration
	readRequestTimeout                 time.Duration
	writeRequestTimeout                time.Duration
	truncateRequestTimeout             time.Duration
	workerPoolSize                     int
	clusterDatabaseTopologyInitializer topology.Initializer
	useTChannelClientForReading        bool
	useTChannelClientForWriting        bool
	useTChannelClientForTruncation     bool
	blockRetrieverManager              block.DatabaseBlockRetrieverManager
	verifySeriesDebugFilePathPrefix    string
	writeConsistencyLevel              topology.ConsistencyLevel
}

func newTestOptions() testOptions {
	var namespaces []namespace.Metadata
	for _, ns := range testNamespaces {
		namespaces = append(namespaces, namespace.NewMetadata(ns, namespace.NewOptions()))
	}
	return &options{
		namespaces: namespaces,
		id:         defaultID,
		serverStateChangeTimeout:       defaultServerStateChangeTimeout,
		clusterConnectionTimeout:       defaultClusterConnectionTimeout,
		readRequestTimeout:             defaultReadRequestTimeout,
		writeRequestTimeout:            defaultWriteRequestTimeout,
		truncateRequestTimeout:         defaultTruncateRequestTimeout,
		workerPoolSize:                 defaultWorkerPoolSize,
		useTChannelClientForReading:    defaultUseTChannelClientForReading,
		useTChannelClientForWriting:    defaultUseTChannelClientForWriting,
		useTChannelClientForTruncation: defaultUseTChannelClientForTruncation,
		writeConsistencyLevel:          defaultWriteConsistencyLevel,
	}
}

func (o *options) SetNamespaces(value []namespace.Metadata) testOptions {
	opts := *o
	opts.namespaces = value
	return &opts
}

func (o *options) Namespaces() []namespace.Metadata {
	return o.namespaces
}

func (o *options) SetID(value string) testOptions {
	opts := *o
	opts.id = value
	return &opts
}

func (o *options) ID() string {
	return o.id
}

func (o *options) SetHTTPClusterAddr(value string) testOptions {
	opts := *o
	opts.httpClusterAddr = value
	return &opts
}

func (o *options) HTTPClusterAddr() string {
	return o.httpClusterAddr
}

func (o *options) SetTChannelClusterAddr(value string) testOptions {
	opts := *o
	opts.tchannelClusterAddr = value
	return &opts
}

func (o *options) TChannelClusterAddr() string {
	return o.tchannelClusterAddr
}

func (o *options) SetHTTPNodeAddr(value string) testOptions {
	opts := *o
	opts.httpNodeAddr = value
	return &opts
}

func (o *options) HTTPNodeAddr() string {
	return o.httpNodeAddr
}

func (o *options) SetTChannelNodeAddr(value string) testOptions {
	opts := *o
	opts.tchannelNodeAddr = value
	return &opts
}

func (o *options) TChannelNodeAddr() string {
	return o.tchannelNodeAddr
}

func (o *options) SetServerStateChangeTimeout(value time.Duration) testOptions {
	opts := *o
	opts.serverStateChangeTimeout = value
	return &opts
}

func (o *options) ServerStateChangeTimeout() time.Duration {
	return o.serverStateChangeTimeout
}

func (o *options) SetClusterConnectionTimeout(value time.Duration) testOptions {
	opts := *o
	opts.clusterConnectionTimeout = value
	return &opts
}

func (o *options) ClusterConnectionTimeout() time.Duration {
	return o.clusterConnectionTimeout
}

func (o *options) SetReadRequestTimeout(value time.Duration) testOptions {
	opts := *o
	opts.readRequestTimeout = value
	return &opts
}

func (o *options) ReadRequestTimeout() time.Duration {
	return o.readRequestTimeout
}

func (o *options) SetWriteRequestTimeout(value time.Duration) testOptions {
	opts := *o
	opts.writeRequestTimeout = value
	return &opts
}

func (o *options) WriteRequestTimeout() time.Duration {
	return o.writeRequestTimeout
}

func (o *options) SetTruncateRequestTimeout(value time.Duration) testOptions {
	opts := *o
	opts.truncateRequestTimeout = value
	return &opts
}

func (o *options) TruncateRequestTimeout() time.Duration {
	return o.truncateRequestTimeout
}

func (o *options) SetWorkerPoolSize(value int) testOptions {
	opts := *o
	opts.workerPoolSize = value
	return &opts
}

func (o *options) WorkerPoolSize() int {
	return o.workerPoolSize
}

func (o *options) SetClusterDatabaseTopologyInitializer(value topology.Initializer) testOptions {
	opts := *o
	opts.clusterDatabaseTopologyInitializer = value
	return &opts
}

func (o *options) ClusterDatabaseTopologyInitializer() topology.Initializer {
	return o.clusterDatabaseTopologyInitializer
}

func (o *options) SetUseTChannelClientForReading(value bool) testOptions {
	opts := *o
	opts.useTChannelClientForReading = value
	return &opts
}

func (o *options) UseTChannelClientForReading() bool {
	return o.useTChannelClientForReading
}

func (o *options) SetUseTChannelClientForWriting(value bool) testOptions {
	opts := *o
	opts.useTChannelClientForWriting = value
	return &opts
}

func (o *options) UseTChannelClientForWriting() bool {
	return o.useTChannelClientForWriting
}

func (o *options) SetUseTChannelClientForTruncation(value bool) testOptions {
	opts := *o
	opts.useTChannelClientForTruncation = value
	return &opts
}

func (o *options) UseTChannelClientForTruncation() bool {
	return o.useTChannelClientForTruncation
}

func (o *options) SetDatabaseBlockRetrieverManager(
	value block.DatabaseBlockRetrieverManager,
) testOptions {
	opts := *o
	opts.blockRetrieverManager = value
	return &opts
}

func (o *options) DatabaseBlockRetrieverManager() block.DatabaseBlockRetrieverManager {
	return o.blockRetrieverManager
}

func (o *options) SetVerifySeriesDebugFilePathPrefix(value string) testOptions {
	opts := *o
	opts.verifySeriesDebugFilePathPrefix = value
	return &opts
}

func (o *options) VerifySeriesDebugFilePathPrefix() string {
	return o.verifySeriesDebugFilePathPrefix
}

func (o *options) WriteConsistencyLevel() topology.ConsistencyLevel {
	return o.writeConsistencyLevel
}

func (o *options) SetWriteConsistencyLevel(cLevel topology.ConsistencyLevel) testOptions {
	opts := *o
	opts.writeConsistencyLevel = cLevel
	return &opts
}
