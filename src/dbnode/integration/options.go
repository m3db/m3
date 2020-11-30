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

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/topology"

	"github.com/stretchr/testify/require"
)

const (
	// defaultID is the default node ID
	defaultID = "testhost"

	// defaultServerStateChangeTimeout is the default time we wait for a server to change its state.
	defaultServerStateChangeTimeout = 10 * time.Minute

	// defaultClusterConnectionTimeout is the default time we wait for cluster connections to be established.
	defaultClusterConnectionTimeout = 2 * time.Second

	// defaultReadRequestTimeout is the default read request timeout.
	defaultReadRequestTimeout = 2 * time.Second

	// defaultWriteRequestTimeout is the default write request timeout.
	defaultWriteRequestTimeout = 2 * time.Second

	// defaultTruncateRequestTimeout is the default truncate request timeout.
	defaultTruncateRequestTimeout = 2 * time.Second

	// defaultFetchRequestTimeout is the default fetch request timeout
	defaultFetchRequestTimeout = 15 * time.Second

	// defaultWorkerPoolSize is the default number of workers in the worker pool.
	defaultWorkerPoolSize = 10

	// defaultTickMinimumInterval is the default minimum tick interval.
	defaultTickMinimumInterval = 1 * time.Second

	// defaultUseTChannelClientForReading determines whether we use the tchannel client for reading by default.
	defaultUseTChannelClientForReading = false

	// defaultUseTChannelClientForWriting determines whether we use the tchannel client for writing by default.
	defaultUseTChannelClientForWriting = false

	// defaultUseTChannelClientForTruncation determines whether we use the tchannel client for truncation by default.
	defaultUseTChannelClientForTruncation = true

	// defaultWriteConsistencyLevel is the default write consistency level. This
	// should match the default in client/options.
	defaultWriteConsistencyLevel = topology.ConsistencyLevelMajority

	// defaultNumShards is the default number of shards to use.
	defaultNumShards = 12

	// defaultMaxWiredBlocks is the default max number of wired blocks to keep in memory at once
	defaultMaxWiredBlocks = 10

	// defaultWriteNewSeriesAsync inserts, and index series' synchronously by default.
	defaultWriteNewSeriesAsync = false

	// defaultReportInterval is the default time interval of reporting metrics within the system.
	defaultReportInterval = time.Second
)

var (
	// DefaultIntegrationTestRetentionOpts are default integration test retention options.
	DefaultIntegrationTestRetentionOpts = retention.NewOptions().SetRetentionPeriod(6 * time.Hour)
)

// TestOptions contains integration test options.
type TestOptions interface {
	// SetNamespaces sets the namespaces.
	SetNamespaces(value []namespace.Metadata) TestOptions

	// Namespaces returns the namespaces.
	Namespaces() []namespace.Metadata

	// SetNamespaceInitializer sets the namespace initializer,
	// if this is set, it superseeds Namespaces()
	SetNamespaceInitializer(value namespace.Initializer) TestOptions

	// NamespaceInitializer returns the namespace initializer
	NamespaceInitializer() namespace.Initializer

	// SetID sets the node ID.
	SetID(value string) TestOptions

	// ID returns the node ID.
	ID() string

	// SetTickMinimumInterval sets the tick interval.
	SetTickMinimumInterval(value time.Duration) TestOptions

	// TickMinimumInterval returns the tick interval.
	TickMinimumInterval() time.Duration

	// SetHTTPClusterAddr sets the http cluster address.
	SetHTTPClusterAddr(value string) TestOptions

	// HTTPClusterAddr returns the http cluster address.
	HTTPClusterAddr() string

	// SetTChannelClusterAddr sets the tchannel cluster address.
	SetTChannelClusterAddr(value string) TestOptions

	// TChannelClusterAddr returns the tchannel cluster address.
	TChannelClusterAddr() string

	// SetHTTPNodeAddr sets the http node address.
	SetHTTPNodeAddr(value string) TestOptions

	// HTTPNodeAddr returns the http node address.
	HTTPNodeAddr() string

	// SetTChannelNodeAddr sets the tchannel node address.
	SetTChannelNodeAddr(value string) TestOptions

	// TChannelNodeAddr returns the tchannel node address.
	TChannelNodeAddr() string

	// SetHTTPDebugAddr sets the http debug address.
	SetHTTPDebugAddr(value string) TestOptions

	// HTTPDebugAddr returns the http debug address.
	HTTPDebugAddr() string

	// SetServerStateChangeTimeout sets the server state change timeout.
	SetServerStateChangeTimeout(value time.Duration) TestOptions

	// ServerStateChangeTimeout returns the server state change timeout.
	ServerStateChangeTimeout() time.Duration

	// SetClusterConnectionTimeout sets the cluster connection timeout.
	SetClusterConnectionTimeout(value time.Duration) TestOptions

	// ClusterConnectionTimeout returns the cluster connection timeout.
	ClusterConnectionTimeout() time.Duration

	// SetReadRequestTimeout sets the read request timeout.
	SetReadRequestTimeout(value time.Duration) TestOptions

	// ReadRequestTimeout returns the read request timeout.
	ReadRequestTimeout() time.Duration

	// SetWriteRequestTimeout sets the write request timeout.
	SetWriteRequestTimeout(value time.Duration) TestOptions

	// WriteRequestTimeout returns the write request timeout.
	WriteRequestTimeout() time.Duration

	// SetTruncateRequestTimeout sets the truncate request timeout.
	SetTruncateRequestTimeout(value time.Duration) TestOptions

	// TruncateRequestTimeout returns the truncate request timeout.
	TruncateRequestTimeout() time.Duration

	// SetFetchRequestTimeout sets the fetch request timeout.
	SetFetchRequestTimeout(value time.Duration) TestOptions

	// FetchRequestTimeout returns the fetch request timeout.
	FetchRequestTimeout() time.Duration

	// SetWorkerPoolSize sets the number of workers in the worker pool.
	SetWorkerPoolSize(value int) TestOptions

	// WorkerPoolSize returns the number of workers in the worker pool.
	WorkerPoolSize() int

	// SetClusterDatabaseTopologyInitializer sets the topology initializer that
	// is used when creating a cluster database
	SetClusterDatabaseTopologyInitializer(value topology.Initializer) TestOptions

	// ClusterDatabaseTopologyInitializer returns the topology initializer that
	// is used when creating a cluster database
	ClusterDatabaseTopologyInitializer() topology.Initializer

	// SetUseTChannelClientForReading sets whether we use the tchannel client for reading.
	SetUseTChannelClientForReading(value bool) TestOptions

	// UseTChannelClientForReading returns whether we use the tchannel client for reading.
	UseTChannelClientForReading() bool

	// SetUseTChannelClientForWriting sets whether we use the tchannel client for writing.
	SetUseTChannelClientForWriting(value bool) TestOptions

	// UseTChannelClientForWriting returns whether we use the tchannel client for writing.
	UseTChannelClientForWriting() bool

	// SetUseTChannelClientForTruncation sets whether we use the tchannel client for truncation.
	SetUseTChannelClientForTruncation(value bool) TestOptions

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
	) TestOptions

	// NewBlockRetrieverFn returns the new block retriever constructor to
	// use when bootstrapping retrievable blocks instead of blocks
	// containing data.
	DatabaseBlockRetrieverManager() block.DatabaseBlockRetrieverManager

	// SetVerifySeriesDebugFilePathPrefix sets the file path prefix for writing a debug file of series comparisons.
	SetVerifySeriesDebugFilePathPrefix(value string) TestOptions

	// VerifySeriesDebugFilePathPrefix returns the file path prefix for writing a debug file of series comparisons.
	VerifySeriesDebugFilePathPrefix() string

	// WriteConsistencyLevel returns the consistency level for writing with the m3db client.
	WriteConsistencyLevel() topology.ConsistencyLevel

	// SetWriteConsistencyLevel sets the consistency level for writing with the m3db client.
	SetWriteConsistencyLevel(value topology.ConsistencyLevel) TestOptions

	// NumShards returns the number of shards to use.
	NumShards() int

	// SetNumShards sets the number of shards to use.
	SetNumShards(value int) TestOptions

	// MaxWiredBlocks returns the maximum number of wired blocks to keep in memory using the LRU cache.
	MaxWiredBlocks() uint

	// SetMaxWiredBlocks sets the maximum number of wired blocks to keep in memory using the LRU cache.
	SetMaxWiredBlocks(value uint) TestOptions

	// SetWriteNewSeriesAsync sets whether we insert/index asynchronously.
	SetWriteNewSeriesAsync(bool) TestOptions

	// WriteNewSeriesAsync returns whether we insert/index asynchronously.
	WriteNewSeriesAsync() bool

	// SetFilePathPrefix sets the file path prefix.
	SetFilePathPrefix(value string) TestOptions

	// FilePathPrefix returns the file path prefix.
	FilePathPrefix() string

	// SetProtoEncoding turns on proto encoder.
	SetProtoEncoding(value bool) TestOptions

	// ProtoEncoding returns whether proto encoder is turned on.
	ProtoEncoding() bool

	// SetAssertTestDataEqual sets a comparator to compare two byte arrays,
	// useful for proto-encoded annotations.
	SetAssertTestDataEqual(value assertTestDataEqual) TestOptions

	// AssertTestDataEqual returns a comparator to compare two byte arrays.
	AssertTestDataEqual() assertTestDataEqual

	// SetNowFn will set the now fn.
	SetNowFn(value func() time.Time) TestOptions

	// NowFn returns the now fn.
	NowFn() func() time.Time

	// SetReportInterval sets the time between reporting metrics within the system.
	SetReportInterval(value time.Duration) TestOptions

	// ReportInterval returns the time between reporting metrics within the system.
	ReportInterval() time.Duration
}

type options struct {
	namespaces                         []namespace.Metadata
	nsInitializer                      namespace.Initializer
	id                                 string
	tickMinimumInterval                time.Duration
	httpClusterAddr                    string
	tchannelClusterAddr                string
	httpNodeAddr                       string
	tchannelNodeAddr                   string
	httpDebugAddr                      string
	filePathPrefix                     string
	serverStateChangeTimeout           time.Duration
	clusterConnectionTimeout           time.Duration
	readRequestTimeout                 time.Duration
	writeRequestTimeout                time.Duration
	truncateRequestTimeout             time.Duration
	fetchRequestTimeout                time.Duration
	workerPoolSize                     int
	clusterDatabaseTopologyInitializer topology.Initializer
	blockRetrieverManager              block.DatabaseBlockRetrieverManager
	verifySeriesDebugFilePathPrefix    string
	writeConsistencyLevel              topology.ConsistencyLevel
	numShards                          int
	maxWiredBlocks                     uint
	useTChannelClientForReading        bool
	useTChannelClientForWriting        bool
	useTChannelClientForTruncation     bool
	writeNewSeriesAsync                bool
	protoEncoding                      bool
	assertEqual                        assertTestDataEqual
	nowFn                              func() time.Time
	reportInterval                     time.Duration
}

// NewTestOptions returns a new set of integration test options.
func NewTestOptions(t *testing.T) TestOptions {
	var namespaces []namespace.Metadata
	nsOpts := namespace.NewOptions().
		SetRepairEnabled(false).
		SetRetentionOptions(DefaultIntegrationTestRetentionOpts)

	for _, ns := range testNamespaces {
		md, err := namespace.NewMetadata(ns, nsOpts)
		require.NoError(t, err)
		namespaces = append(namespaces, md)
	}

	return &options{
		namespaces:                     namespaces,
		id:                             defaultID,
		tickMinimumInterval:            defaultTickMinimumInterval,
		serverStateChangeTimeout:       defaultServerStateChangeTimeout,
		clusterConnectionTimeout:       defaultClusterConnectionTimeout,
		readRequestTimeout:             defaultReadRequestTimeout,
		writeRequestTimeout:            defaultWriteRequestTimeout,
		truncateRequestTimeout:         defaultTruncateRequestTimeout,
		fetchRequestTimeout:            defaultFetchRequestTimeout,
		workerPoolSize:                 defaultWorkerPoolSize,
		writeConsistencyLevel:          defaultWriteConsistencyLevel,
		numShards:                      defaultNumShards,
		maxWiredBlocks:                 defaultMaxWiredBlocks,
		useTChannelClientForReading:    defaultUseTChannelClientForReading,
		useTChannelClientForWriting:    defaultUseTChannelClientForWriting,
		useTChannelClientForTruncation: defaultUseTChannelClientForTruncation,
		writeNewSeriesAsync:            defaultWriteNewSeriesAsync,
		reportInterval:                 defaultReportInterval,
	}
}

func (o *options) SetNamespaces(value []namespace.Metadata) TestOptions {
	opts := *o
	opts.namespaces = opts.namespaces[:0]
	opts.namespaces = value
	return &opts
}

func (o *options) Namespaces() []namespace.Metadata {
	return o.namespaces
}

func (o *options) SetNamespaceInitializer(value namespace.Initializer) TestOptions {
	opts := *o
	opts.nsInitializer = value
	return &opts
}

func (o *options) NamespaceInitializer() namespace.Initializer {
	return o.nsInitializer
}

func (o *options) SetID(value string) TestOptions {
	opts := *o
	opts.id = value
	return &opts
}

func (o *options) ID() string {
	return o.id
}

func (o *options) SetTickMinimumInterval(value time.Duration) TestOptions {
	opts := *o
	opts.tickMinimumInterval = value
	return &opts
}

func (o *options) TickMinimumInterval() time.Duration {
	return o.tickMinimumInterval
}

func (o *options) SetHTTPClusterAddr(value string) TestOptions {
	opts := *o
	opts.httpClusterAddr = value
	return &opts
}

func (o *options) HTTPClusterAddr() string {
	return o.httpClusterAddr
}

func (o *options) SetTChannelClusterAddr(value string) TestOptions {
	opts := *o
	opts.tchannelClusterAddr = value
	return &opts
}

func (o *options) TChannelClusterAddr() string {
	return o.tchannelClusterAddr
}

func (o *options) SetHTTPNodeAddr(value string) TestOptions {
	opts := *o
	opts.httpNodeAddr = value
	return &opts
}

func (o *options) HTTPNodeAddr() string {
	return o.httpNodeAddr
}

func (o *options) SetTChannelNodeAddr(value string) TestOptions {
	opts := *o
	opts.tchannelNodeAddr = value
	return &opts
}

func (o *options) TChannelNodeAddr() string {
	return o.tchannelNodeAddr
}

func (o *options) SetHTTPDebugAddr(value string) TestOptions {
	opts := *o
	opts.httpDebugAddr = value
	return &opts
}

func (o *options) HTTPDebugAddr() string {
	return o.httpDebugAddr
}

func (o *options) SetServerStateChangeTimeout(value time.Duration) TestOptions {
	opts := *o
	opts.serverStateChangeTimeout = value
	return &opts
}

func (o *options) ServerStateChangeTimeout() time.Duration {
	return o.serverStateChangeTimeout
}

func (o *options) SetClusterConnectionTimeout(value time.Duration) TestOptions {
	opts := *o
	opts.clusterConnectionTimeout = value
	return &opts
}

func (o *options) ClusterConnectionTimeout() time.Duration {
	return o.clusterConnectionTimeout
}

func (o *options) SetReadRequestTimeout(value time.Duration) TestOptions {
	opts := *o
	opts.readRequestTimeout = value
	return &opts
}

func (o *options) ReadRequestTimeout() time.Duration {
	return o.readRequestTimeout
}

func (o *options) SetWriteRequestTimeout(value time.Duration) TestOptions {
	opts := *o
	opts.writeRequestTimeout = value
	return &opts
}

func (o *options) WriteRequestTimeout() time.Duration {
	return o.writeRequestTimeout
}

func (o *options) SetTruncateRequestTimeout(value time.Duration) TestOptions {
	opts := *o
	opts.truncateRequestTimeout = value
	return &opts
}

func (o *options) TruncateRequestTimeout() time.Duration {
	return o.truncateRequestTimeout
}

func (o *options) SetFetchRequestTimeout(value time.Duration) TestOptions {
	opts := *o
	opts.fetchRequestTimeout = value
	return &opts
}

func (o *options) FetchRequestTimeout() time.Duration {
	return o.fetchRequestTimeout
}

func (o *options) SetWorkerPoolSize(value int) TestOptions {
	opts := *o
	opts.workerPoolSize = value
	return &opts
}

func (o *options) WorkerPoolSize() int {
	return o.workerPoolSize
}

func (o *options) SetClusterDatabaseTopologyInitializer(value topology.Initializer) TestOptions {
	opts := *o
	opts.clusterDatabaseTopologyInitializer = value
	return &opts
}

func (o *options) ClusterDatabaseTopologyInitializer() topology.Initializer {
	return o.clusterDatabaseTopologyInitializer
}

func (o *options) SetUseTChannelClientForReading(value bool) TestOptions {
	opts := *o
	opts.useTChannelClientForReading = value
	return &opts
}

func (o *options) UseTChannelClientForReading() bool {
	return o.useTChannelClientForReading
}

func (o *options) SetUseTChannelClientForWriting(value bool) TestOptions {
	opts := *o
	opts.useTChannelClientForWriting = value
	return &opts
}

func (o *options) UseTChannelClientForWriting() bool {
	return o.useTChannelClientForWriting
}

func (o *options) SetUseTChannelClientForTruncation(value bool) TestOptions {
	opts := *o
	opts.useTChannelClientForTruncation = value
	return &opts
}

func (o *options) UseTChannelClientForTruncation() bool {
	return o.useTChannelClientForTruncation
}

func (o *options) SetDatabaseBlockRetrieverManager(
	value block.DatabaseBlockRetrieverManager,
) TestOptions {
	opts := *o
	opts.blockRetrieverManager = value
	return &opts
}

func (o *options) DatabaseBlockRetrieverManager() block.DatabaseBlockRetrieverManager {
	return o.blockRetrieverManager
}

func (o *options) SetVerifySeriesDebugFilePathPrefix(value string) TestOptions {
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

func (o *options) SetWriteConsistencyLevel(cLevel topology.ConsistencyLevel) TestOptions {
	opts := *o
	opts.writeConsistencyLevel = cLevel
	return &opts
}

func (o *options) NumShards() int {
	return o.numShards
}

func (o *options) SetNumShards(value int) TestOptions {
	opts := *o
	opts.numShards = value
	return &opts
}

func (o *options) MaxWiredBlocks() uint {
	return o.maxWiredBlocks
}

func (o *options) SetMaxWiredBlocks(value uint) TestOptions {
	opts := *o
	opts.maxWiredBlocks = value
	return &opts
}

func (o *options) SetWriteNewSeriesAsync(value bool) TestOptions {
	opts := *o
	opts.writeNewSeriesAsync = value
	return &opts
}

func (o *options) WriteNewSeriesAsync() bool {
	return o.writeNewSeriesAsync
}

func (o *options) SetFilePathPrefix(value string) TestOptions {
	opts := *o
	opts.filePathPrefix = value
	return &opts
}

func (o *options) FilePathPrefix() string {
	return o.filePathPrefix
}

func (o *options) SetProtoEncoding(value bool) TestOptions {
	opts := *o
	opts.protoEncoding = value
	return &opts
}

func (o *options) ProtoEncoding() bool {
	return o.protoEncoding
}

func (o *options) SetAssertTestDataEqual(value assertTestDataEqual) TestOptions {
	opts := *o
	opts.assertEqual = value
	return &opts
}

func (o *options) AssertTestDataEqual() assertTestDataEqual {
	return o.assertEqual
}

func (o *options) SetNowFn(value func() time.Time) TestOptions {
	opts := *o
	opts.nowFn = value
	return &opts
}

func (o *options) NowFn() func() time.Time {
	return o.nowFn
}

func (o *options) SetReportInterval(value time.Duration) TestOptions {
	opts := *o
	opts.reportInterval = value
	return &opts
}

func (o *options) ReportInterval() time.Duration {
	return o.reportInterval
}
