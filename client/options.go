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

package client

import (
	"errors"
	"io"
	"math"
	"runtime"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/encoding/m3tsz"
	"github.com/m3db/m3db/instrument"
	"github.com/m3db/m3db/pool"
	"github.com/m3db/m3db/topology"

	"github.com/uber/tchannel-go"
)

const (
	// defaultConsistencyLevel is the default consistency level
	defaultConsistencyLevel = topology.ConsistencyLevelMajority

	// defaultMaxConnectionCount is the default max connection count
	defaultMaxConnectionCount = 32

	// defaultMinConnectionCount is the default min connection count
	defaultMinConnectionCount = 2

	// defaultHostConnectTimeout is the default host connection timeout
	defaultHostConnectTimeout = 5 * time.Second

	// defaultClusterConnectTimeout is the default cluster connect timeout
	defaultClusterConnectTimeout = 30 * time.Second

	// defaultClusterConnectConsistencyLevel is the default cluster connect consistency level
	defaultClusterConnectConsistencyLevel = ConnectConsistencyLevelMajority

	// defaultWriteRequestTimeout is the default write request timeout
	defaultWriteRequestTimeout = 5 * time.Second

	// defaultFetchRequestTimeout is the default fetch request timeout
	defaultFetchRequestTimeout = 15 * time.Second

	// defaultTruncateRequestTimeout is the default truncate request timeout
	defaultTruncateRequestTimeout = 60 * time.Second

	// defaultWriteOpPoolSize is the default write op pool size
	defaultWriteOpPoolSize = 1000000

	// defaultFetchBatchOpPoolSize is the default fetch op pool size
	defaultFetchBatchOpPoolSize = 8192

	// defaultWriteBatchSize is the default write batch size
	defaultWriteBatchSize = 128

	// defaultFetchBatchSize is the default fetch batch size
	defaultFetchBatchSize = 128

	// defaultHostQueueOpsFlushSize is the default host queue ops flush size
	defaultHostQueueOpsFlushSize = 128

	// defaultHostQueueOpsFlushInterval is the default host queue flush interval
	defaultHostQueueOpsFlushInterval = 5 * time.Millisecond

	// defaultHostQueueOpsArrayPoolSize is the default host queue ops array pool size
	defaultHostQueueOpsArrayPoolSize = 8

	// defaultBackgroundConnectInterval is the default background connect interval
	defaultBackgroundConnectInterval = 4 * time.Second

	// defaultBackgroundConnectStutter is the default background connect stutter
	defaultBackgroundConnectStutter = 1 * time.Second

	// defaultBackgroundHealthCheckInterval is the default background health check interval
	defaultBackgroundHealthCheckInterval = 1 * time.Second

	// defaultBackgroundHealthCheckStutter is the default background health check stutter
	defaultBackgroundHealthCheckStutter = 1 * time.Second

	// defaultSeriesIteratorPoolSize is the default size of the series iterator pools
	defaultSeriesIteratorPoolSize = 100000

	// defaultFetchSeriesBlocksBatchSize is the default fetch series blocks batch size
	defaultFetchSeriesBlocksBatchSize = 4096

	// defaultFetchSeriesBlocksMetadataBatchTimeout is the default series blocks metadata fetch timeout
	defaultFetchSeriesBlocksMetadataBatchTimeout = 5 * time.Second

	// defaultFetchSeriesBlocksMetadataBatchTimeout is the default series blocks contents fetch timeout
	defaultFetchSeriesBlocksBatchTimeout = 30 * time.Second

	// defaultFetchSeriesBlocksBatchConcurrency is the default fetch series blocks in batch parallel concurrency limit
	defaultFetchSeriesBlocksBatchConcurrency = 1024
)

var (
	// defaultSeriesIteratorArrayPoolBuckets is the default pool buckets for the series iterator array pool
	defaultSeriesIteratorArrayPoolBuckets = []pool.Bucket{}

	// defaultFetchSeriesBlocksResultsProcessors is the default concurrency for processing results when fetching series blocks
	defaultFetchSeriesBlocksResultsProcessors = int(math.Max(float64(1), float64(runtime.NumCPU())*0.5))

	errNoTopologyInitializerSet    = errors.New("no topology initializer set")
	errNoReaderIteratorAllocateSet = errors.New("no reader iterator allocator set, encoding not set")
)

type options struct {
	clockOpts                             clock.Options
	instrumentOpts                        instrument.Options
	topologyInitializer                   topology.Initializer
	consistencyLevel                      topology.ConsistencyLevel
	channelOptions                        *tchannel.ChannelOptions
	maxConnectionCount                    int
	minConnectionCount                    int
	hostConnectTimeout                    time.Duration
	clusterConnectTimeout                 time.Duration
	clusterConnectConsistencyLevel        ConnectConsistencyLevel
	writeRequestTimeout                   time.Duration
	fetchRequestTimeout                   time.Duration
	truncateRequestTimeout                time.Duration
	backgroundConnectInterval             time.Duration
	backgroundConnectStutter              time.Duration
	backgroundHealthCheckInterval         time.Duration
	backgroundHealthCheckStutter          time.Duration
	readerIteratorAllocate                encoding.ReaderIteratorAllocate
	writeOpPoolSize                       int
	fetchBatchOpPoolSize                  int
	writeBatchSize                        int
	fetchBatchSize                        int
	hostQueueOpsFlushSize                 int
	hostQueueOpsFlushInterval             time.Duration
	hostQueueOpsArrayPoolSize             int
	seriesIteratorPoolSize                int
	seriesIteratorArrayPoolBuckets        []pool.Bucket
	contextPool                           context.Pool
	origin                                topology.Host
	fetchSeriesBlocksBatchSize            int
	fetchSeriesBlocksMetadataBatchTimeout time.Duration
	fetchSeriesBlocksBatchTimeout         time.Duration
	fetchSeriesBlocksBatchConcurrency     int
	fetchSeriesBlocksResultsProcessors    int
}

// NewOptions creates a new set of client options with defaults
func NewOptions() Options {
	return newOptions()
}

// NewAdminOptions creates a new set of administration client options with defaults
func NewAdminOptions() AdminOptions {
	return newOptions()
}

func newOptions() *options {
	opts := &options{
		clockOpts:                             clock.NewOptions(),
		instrumentOpts:                        instrument.NewOptions(),
		consistencyLevel:                      defaultConsistencyLevel,
		maxConnectionCount:                    defaultMaxConnectionCount,
		minConnectionCount:                    defaultMinConnectionCount,
		hostConnectTimeout:                    defaultHostConnectTimeout,
		clusterConnectTimeout:                 defaultClusterConnectTimeout,
		clusterConnectConsistencyLevel:        defaultClusterConnectConsistencyLevel,
		writeRequestTimeout:                   defaultWriteRequestTimeout,
		fetchRequestTimeout:                   defaultFetchRequestTimeout,
		truncateRequestTimeout:                defaultTruncateRequestTimeout,
		backgroundConnectInterval:             defaultBackgroundConnectInterval,
		backgroundConnectStutter:              defaultBackgroundConnectStutter,
		backgroundHealthCheckInterval:         defaultBackgroundHealthCheckInterval,
		backgroundHealthCheckStutter:          defaultBackgroundHealthCheckStutter,
		writeOpPoolSize:                       defaultWriteOpPoolSize,
		fetchBatchOpPoolSize:                  defaultFetchBatchOpPoolSize,
		writeBatchSize:                        defaultWriteBatchSize,
		fetchBatchSize:                        defaultFetchBatchSize,
		hostQueueOpsFlushSize:                 defaultHostQueueOpsFlushSize,
		hostQueueOpsFlushInterval:             defaultHostQueueOpsFlushInterval,
		hostQueueOpsArrayPoolSize:             defaultHostQueueOpsArrayPoolSize,
		seriesIteratorPoolSize:                defaultSeriesIteratorPoolSize,
		seriesIteratorArrayPoolBuckets:        defaultSeriesIteratorArrayPoolBuckets,
		contextPool:                           context.NewPool(nil, nil),
		fetchSeriesBlocksBatchSize:            defaultFetchSeriesBlocksBatchSize,
		fetchSeriesBlocksMetadataBatchTimeout: defaultFetchSeriesBlocksMetadataBatchTimeout,
		fetchSeriesBlocksBatchTimeout:         defaultFetchSeriesBlocksBatchTimeout,
		fetchSeriesBlocksBatchConcurrency:     defaultFetchSeriesBlocksBatchConcurrency,
		fetchSeriesBlocksResultsProcessors:    defaultFetchSeriesBlocksResultsProcessors,
	}
	return opts.SetEncodingM3TSZ().(*options)
}

func (o *options) Validate() error {
	if o.topologyInitializer == nil {
		return errNoTopologyInitializerSet
	}
	if o.readerIteratorAllocate == nil {
		return errNoReaderIteratorAllocateSet
	}
	return nil
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

func (o *options) SetEncodingM3TSZ() Options {
	opts := *o
	opts.readerIteratorAllocate = func(r io.Reader) encoding.ReaderIterator {
		return m3tsz.NewReaderIterator(r, m3tsz.DefaultIntOptimizationEnabled, encoding.NewOptions())
	}
	return &opts
}

func (o *options) SetTopologyInitializer(value topology.Initializer) Options {
	opts := *o
	opts.topologyInitializer = value
	return &opts
}

func (o *options) TopologyInitializer() topology.Initializer {
	return o.topologyInitializer
}

func (o *options) SetConsistencyLevel(value topology.ConsistencyLevel) Options {
	opts := *o
	opts.consistencyLevel = value
	return &opts
}

func (o *options) ConsistencyLevel() topology.ConsistencyLevel {
	return o.consistencyLevel
}

func (o *options) SetChannelOptions(value *tchannel.ChannelOptions) Options {
	opts := *o
	opts.channelOptions = value
	return &opts
}

func (o *options) ChannelOptions() *tchannel.ChannelOptions {
	return o.channelOptions
}

func (o *options) SetMaxConnectionCount(value int) Options {
	opts := *o
	opts.maxConnectionCount = value
	return &opts
}

func (o *options) MaxConnectionCount() int {
	return o.maxConnectionCount
}

func (o *options) SetMinConnectionCount(value int) Options {
	opts := *o
	opts.minConnectionCount = value
	return &opts
}

func (o *options) MinConnectionCount() int {
	return o.minConnectionCount
}

func (o *options) SetHostConnectTimeout(value time.Duration) Options {
	opts := *o
	opts.hostConnectTimeout = value
	return &opts
}

func (o *options) HostConnectTimeout() time.Duration {
	return o.hostConnectTimeout
}

func (o *options) SetClusterConnectTimeout(value time.Duration) Options {
	opts := *o
	opts.clusterConnectTimeout = value
	return &opts
}

func (o *options) ClusterConnectTimeout() time.Duration {
	return o.clusterConnectTimeout
}

func (o *options) SetClusterConnectConsistencyLevel(value ConnectConsistencyLevel) Options {
	opts := *o
	opts.clusterConnectConsistencyLevel = value
	return &opts
}

func (o *options) ClusterConnectConsistencyLevel() ConnectConsistencyLevel {
	return o.clusterConnectConsistencyLevel
}

func (o *options) SetWriteRequestTimeout(value time.Duration) Options {
	opts := *o
	opts.writeRequestTimeout = value
	return &opts
}

func (o *options) WriteRequestTimeout() time.Duration {
	return o.writeRequestTimeout
}

func (o *options) SetFetchRequestTimeout(value time.Duration) Options {
	opts := *o
	opts.fetchRequestTimeout = value
	return &opts
}

func (o *options) FetchRequestTimeout() time.Duration {
	return o.fetchRequestTimeout
}

func (o *options) SetTruncateRequestTimeout(value time.Duration) Options {
	opts := *o
	opts.truncateRequestTimeout = value
	return &opts
}

func (o *options) TruncateRequestTimeout() time.Duration {
	return o.truncateRequestTimeout
}

func (o *options) SetBackgroundConnectInterval(value time.Duration) Options {
	opts := *o
	opts.backgroundConnectInterval = value
	return &opts
}

func (o *options) BackgroundConnectInterval() time.Duration {
	return o.writeRequestTimeout
}

func (o *options) SetBackgroundConnectStutter(value time.Duration) Options {
	opts := *o
	opts.backgroundConnectStutter = value
	return &opts
}

func (o *options) BackgroundConnectStutter() time.Duration {
	return o.backgroundConnectStutter
}

func (o *options) SetBackgroundHealthCheckInterval(value time.Duration) Options {
	opts := *o
	opts.backgroundHealthCheckInterval = value
	return &opts
}

func (o *options) BackgroundHealthCheckInterval() time.Duration {
	return o.backgroundHealthCheckInterval
}

func (o *options) SetBackgroundHealthCheckStutter(value time.Duration) Options {
	opts := *o
	opts.backgroundHealthCheckStutter = value
	return &opts
}

func (o *options) BackgroundHealthCheckStutter() time.Duration {
	return o.backgroundHealthCheckStutter
}

func (o *options) SetWriteOpPoolSize(value int) Options {
	opts := *o
	opts.writeOpPoolSize = value
	return &opts
}

func (o *options) WriteOpPoolSize() int {
	return o.writeOpPoolSize
}

func (o *options) SetFetchBatchOpPoolSize(value int) Options {
	opts := *o
	opts.fetchBatchOpPoolSize = value
	return &opts
}

func (o *options) FetchBatchOpPoolSize() int {
	return o.fetchBatchOpPoolSize
}

func (o *options) SetContextPool(value context.Pool) Options {
	opts := *o
	opts.contextPool = value
	return &opts
}

func (o *options) ContextPool() context.Pool {
	return o.contextPool
}

func (o *options) SetWriteBatchSize(value int) Options {
	opts := *o
	opts.writeBatchSize = value
	return &opts
}

func (o *options) WriteBatchSize() int {
	return o.writeBatchSize
}

func (o *options) SetFetchBatchSize(value int) Options {
	opts := *o
	opts.fetchBatchSize = value
	return &opts
}

func (o *options) FetchBatchSize() int {
	return o.fetchBatchSize
}

func (o *options) SetHostQueueOpsFlushSize(value int) Options {
	opts := *o
	opts.hostQueueOpsFlushSize = value
	return &opts
}

func (o *options) HostQueueOpsFlushSize() int {
	return o.hostQueueOpsFlushSize
}

func (o *options) SetHostQueueOpsFlushInterval(value time.Duration) Options {
	opts := *o
	opts.hostQueueOpsFlushInterval = value
	return &opts
}

func (o *options) HostQueueOpsFlushInterval() time.Duration {
	return o.hostQueueOpsFlushInterval
}

func (o *options) SetHostQueueOpsArrayPoolSize(value int) Options {
	opts := *o
	opts.hostQueueOpsArrayPoolSize = value
	return &opts
}

func (o *options) HostQueueOpsArrayPoolSize() int {
	return o.hostQueueOpsArrayPoolSize
}

func (o *options) SetSeriesIteratorPoolSize(value int) Options {
	opts := *o
	opts.seriesIteratorPoolSize = value
	return &opts
}

func (o *options) SeriesIteratorPoolSize() int {
	return o.seriesIteratorPoolSize
}

func (o *options) SetSeriesIteratorArrayPoolBuckets(value []pool.Bucket) Options {
	opts := *o
	opts.seriesIteratorArrayPoolBuckets = value
	return &opts
}

func (o *options) SeriesIteratorArrayPoolBuckets() []pool.Bucket {
	return o.seriesIteratorArrayPoolBuckets
}

func (o *options) SetReaderIteratorAllocate(value encoding.ReaderIteratorAllocate) Options {
	opts := *o
	opts.readerIteratorAllocate = value
	return &opts
}

func (o *options) ReaderIteratorAllocate() encoding.ReaderIteratorAllocate {
	return o.readerIteratorAllocate
}

func (o *options) SetOrigin(value topology.Host) AdminOptions {
	opts := *o
	opts.origin = value
	return &opts
}

func (o *options) Origin() topology.Host {
	return o.origin
}

func (o *options) SetFetchSeriesBlocksBatchSize(value int) AdminOptions {
	opts := *o
	opts.fetchSeriesBlocksBatchSize = value
	return &opts
}

func (o *options) FetchSeriesBlocksBatchSize() int {
	return o.fetchSeriesBlocksBatchSize
}

func (o *options) SetFetchSeriesBlocksMetadataBatchTimeout(value time.Duration) AdminOptions {
	opts := *o
	opts.fetchSeriesBlocksMetadataBatchTimeout = value
	return &opts
}

func (o *options) FetchSeriesBlocksMetadataBatchTimeout() time.Duration {
	return o.fetchSeriesBlocksMetadataBatchTimeout
}

func (o *options) SetFetchSeriesBlocksBatchTimeout(value time.Duration) AdminOptions {
	opts := *o
	opts.fetchSeriesBlocksBatchTimeout = value
	return &opts
}

func (o *options) FetchSeriesBlocksBatchTimeout() time.Duration {
	return o.fetchSeriesBlocksBatchTimeout
}

func (o *options) SetFetchSeriesBlocksBatchConcurrency(value int) AdminOptions {
	opts := *o
	opts.fetchSeriesBlocksBatchConcurrency = value
	return &opts
}

func (o *options) FetchSeriesBlocksBatchConcurrency() int {
	return o.fetchSeriesBlocksBatchConcurrency
}

func (o *options) SetFetchSeriesBlocksResultsProcessors(value int) AdminOptions {
	opts := *o
	opts.fetchSeriesBlocksResultsProcessors = value
	return &opts
}

func (o *options) FetchSeriesBlocksResultsProcessors() int {
	return o.fetchSeriesBlocksResultsProcessors
}
