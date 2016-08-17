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
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/instrument"
	"github.com/m3db/m3db/pool"
	"github.com/m3db/m3db/topology"
	xtime "github.com/m3db/m3x/time"

	tchannel "github.com/uber/tchannel-go"
)

// Client can create sessions to write and read to a cluster
type Client interface {
	// NewSession creates a new session
	NewSession() (Session, error)
}

// Session can write and read to a cluster
type Session interface {
	// Write value to the database for an ID
	Write(id string, t time.Time, value float64, unit xtime.Unit, annotation []byte) error

	// Fetch values from the database for an ID
	Fetch(id string, startInclusive, endExclusive time.Time) (encoding.SeriesIterator, error)

	// FetchAll values from the database for a set of IDs
	FetchAll(ids []string, startInclusive, endExclusive time.Time) (encoding.SeriesIterators, error)

	// Close the session
	Close() error
}

type clientSession interface {
	Session

	// Open the client session
	Open() error
}

type hostQueue interface {
	// Open the host queue
	Open()

	// Len returns the length of the queue
	Len() int

	// Enqueue an operation
	Enqueue(op op) error

	// GetConnectionCount gets the current open connection count
	GetConnectionCount() int

	// Close the host queue, will flush any operations still pending
	Close()
}

type connectionPool interface {
	// Open starts the connection pool connecting and health checking
	Open()

	// GetConnectionCount gets the current open connection count
	GetConnectionCount() int

	// NextClient gets the next client for use by the connection pool
	NextClient() (rpc.TChanNode, error)

	// Close the connection pool
	Close()
}

type state int

const (
	stateNotOpen state = iota
	stateOpen
	stateClosed
)

type op interface {
	// Size returns the effective size of inner operations
	Size() int

	// GetCompletionFn gets the completion function for the operation
	GetCompletionFn() completionFn
}

// Options is a set of client options
type Options interface {
	// Validate validates the options
	Validate() error

	// ClockOptions sets the clock options
	ClockOptions(value clock.Options) Options

	// GetClockOptions returns the clock options
	GetClockOptions() clock.Options

	// InstrumentOptions sets the instrumentation options
	InstrumentOptions(value instrument.Options) Options

	// GetInstrumentOptions returns the instrumentation options
	GetInstrumentOptions() instrument.Options

	// EncodingM3TSZ sets m3tsz encoding
	EncodingM3TSZ() Options

	// TopologyType sets the topologyType
	TopologyType(value topology.Type) Options

	// GetTopologyType returns the topologyType
	GetTopologyType() topology.Type

	// ConsistencyLevel sets the consistencyLevel
	ConsistencyLevel(value topology.ConsistencyLevel) Options

	// GetConsistencyLevel returns the consistencyLevel
	GetConsistencyLevel() topology.ConsistencyLevel

	// ChannelOptions sets the channelOptions
	ChannelOptions(value *tchannel.ChannelOptions) Options

	// GetChannelOptions returns the channelOptions
	GetChannelOptions() *tchannel.ChannelOptions

	// MaxConnectionCount sets the maxConnectionCount
	MaxConnectionCount(value int) Options

	// GetMaxConnectionCount returns the maxConnectionCount
	GetMaxConnectionCount() int

	// MinConnectionCount sets the minConnectionCount
	MinConnectionCount(value int) Options

	// GetMinConnectionCount returns the minConnectionCount
	GetMinConnectionCount() int

	// HostConnectTimeout sets the hostConnectTimeout
	HostConnectTimeout(value time.Duration) Options

	// GetHostConnectTimeout returns the hostConnectTimeout
	GetHostConnectTimeout() time.Duration

	// ClusterConnectTimeout sets the clusterConnectTimeout
	ClusterConnectTimeout(value time.Duration) Options

	// GetClusterConnectTimeout returns the clusterConnectTimeout
	GetClusterConnectTimeout() time.Duration

	// ClusterConnectConsistencyLevel sets the clusterConnectConsistencyLevel
	ClusterConnectConsistencyLevel(value topology.ConsistencyLevel) Options

	// GetClusterConnectConsistencyLevel returns the clusterConnectConsistencyLevel
	GetClusterConnectConsistencyLevel() topology.ConsistencyLevel

	// WriteRequestTimeout sets the writeRequestTimeout
	WriteRequestTimeout(value time.Duration) Options

	// GetWriteRequestTimeout returns the writeRequestTimeout
	GetWriteRequestTimeout() time.Duration

	// FetchRequestTimeout sets the fetchRequestTimeout
	FetchRequestTimeout(value time.Duration) Options

	// GetFetchRequestTimeout returns the fetchRequestTimeout
	GetFetchRequestTimeout() time.Duration

	// BackgroundConnectInterval sets the backgroundConnectInterval
	BackgroundConnectInterval(value time.Duration) Options

	// GetBackgroundConnectInterval returns the backgroundConnectInterval
	GetBackgroundConnectInterval() time.Duration

	// BackgroundConnectStutter sets the backgroundConnectStutter
	BackgroundConnectStutter(value time.Duration) Options

	// GetBackgroundConnectStutter returns the backgroundConnectStutter
	GetBackgroundConnectStutter() time.Duration

	// BackgroundHealthCheckInterval sets the backgroundHealthCheckInterval
	BackgroundHealthCheckInterval(value time.Duration) Options

	// GetBackgroundHealthCheckInterval returns the backgroundHealthCheckInterval
	GetBackgroundHealthCheckInterval() time.Duration

	// BackgroundHealthCheckStutter sets the backgroundHealthCheckStutter
	BackgroundHealthCheckStutter(value time.Duration) Options

	// GetBackgroundHealthCheckStutter returns the backgroundHealthCheckStutter
	GetBackgroundHealthCheckStutter() time.Duration

	// WriteOpPoolSize sets the writeOpPoolSize
	WriteOpPoolSize(value int) Options

	// GetWriteOpPoolSize returns the writeOpPoolSize
	GetWriteOpPoolSize() int

	// FetchBatchOpPoolSize sets the fetchBatchOpPoolSize
	FetchBatchOpPoolSize(value int) Options

	// GetFetchBatchOpPoolSize returns the fetchBatchOpPoolSize
	GetFetchBatchOpPoolSize() int

	// WriteBatchSize sets the writeBatchSize
	// NB(r): for a write only application load this should match the host
	// queue ops flush size so that each time a host queue is flushed it can
	// fit the entire flushed write ops into a single batch.
	WriteBatchSize(value int) Options

	// GetWriteBatchSize returns the writeBatchSize
	GetWriteBatchSize() int

	// FetchBatchSize sets the fetchBatchSize
	// NB(r): for a fetch only application load this should match the host
	// queue ops flush size so that each time a host queue is flushed it can
	// fit the entire flushed fetch ops into a single batch.
	FetchBatchSize(value int) Options

	// GetFetchBatchSize returns the fetchBatchSize
	GetFetchBatchSize() int

	// HostQueueOpsFlushSize sets the hostQueueOpsFlushSize
	HostQueueOpsFlushSize(value int) Options

	// GetHostQueueOpsFlushSize returns the hostQueueOpsFlushSize
	GetHostQueueOpsFlushSize() int

	// HostQueueOpsFlushInterval sets the hostQueueOpsFlushInterval
	HostQueueOpsFlushInterval(value time.Duration) Options

	// GetHostQueueOpsFlushInterval returns the hostQueueOpsFlushInterval
	GetHostQueueOpsFlushInterval() time.Duration

	// HostQueueOpsArrayPoolSize sets the hostQueueOpsArrayPoolSize
	HostQueueOpsArrayPoolSize(value int) Options

	// GetHostQueueOpsArrayPoolSize returns the hostQueueOpsArrayPoolSize
	GetHostQueueOpsArrayPoolSize() int

	// SeriesIteratorPoolSize sets the seriesIteratorPoolSize
	SeriesIteratorPoolSize(value int) Options

	// GetSeriesIteratorPoolSize returns the seriesIteratorPoolSize
	GetSeriesIteratorPoolSize() int

	// SeriesIteratorArrayPoolBuckets sets the seriesIteratorArrayPoolBuckets
	SeriesIteratorArrayPoolBuckets(value []pool.Bucket) Options

	// GetSeriesIteratorArrayPoolBuckets returns the seriesIteratorArrayPoolBuckets
	GetSeriesIteratorArrayPoolBuckets() []pool.Bucket

	// ReaderIteratorAllocate sets the readerIteratorAllocate
	ReaderIteratorAllocate(value encoding.ReaderIteratorAllocate) Options

	// GetReaderIteratorAllocate returns the readerIteratorAllocate
	GetReaderIteratorAllocate() encoding.ReaderIteratorAllocate
}
