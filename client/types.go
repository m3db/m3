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
	"fmt"
	"strings"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/storage/index"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/topology"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	xretry "github.com/m3db/m3x/retry"
	xtime "github.com/m3db/m3x/time"

	tchannel "github.com/uber/tchannel-go"
)

// unknown string constant, required to fix lint complaining about
// multiple occurrences of same literal string...
const unknown = "unknown"

// ConnectConsistencyLevel is the consistency level for connecting to a cluster
type ConnectConsistencyLevel int

const (
	// ConnectConsistencyLevelAny corresponds to connecting to any number of nodes for a given shard
	// set, this strategy will attempt to connect to all, then the majority, then one and then none.
	ConnectConsistencyLevelAny ConnectConsistencyLevel = iota

	// ConnectConsistencyLevelNone corresponds to connecting to no nodes for a given shard set
	ConnectConsistencyLevelNone

	// ConnectConsistencyLevelOne corresponds to connecting to a single node for a given shard set
	ConnectConsistencyLevelOne

	// ConnectConsistencyLevelMajority corresponds to connecting to the majority of nodes for a given shard set
	ConnectConsistencyLevelMajority

	// ConnectConsistencyLevelAll corresponds to connecting to all of the nodes for a given shard set
	ConnectConsistencyLevelAll
)

// String returns the consistency level as a string
func (l ConnectConsistencyLevel) String() string {
	switch l {
	case ConnectConsistencyLevelAny:
		return "any"
	case ConnectConsistencyLevelNone:
		return "none"
	case ConnectConsistencyLevelOne:
		return "one"
	case ConnectConsistencyLevelMajority:
		return "majority"
	case ConnectConsistencyLevelAll:
		return "all"
	}
	return unknown
}

var validConnectConsistencyLevels = []ConnectConsistencyLevel{
	ConnectConsistencyLevelAny,
	ConnectConsistencyLevelNone,
	ConnectConsistencyLevelOne,
	ConnectConsistencyLevelMajority,
	ConnectConsistencyLevelAll,
}

var errClusterConnectConsistencyLevelUnspecified = errors.New("cluster connect consistency level not specified")

// UnmarshalYAML unmarshals an ConnectConsistencyLevel into a valid type from string.
func (l *ConnectConsistencyLevel) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}
	if str == "" {
		return errClusterConnectConsistencyLevelUnspecified
	}
	strs := make([]string, len(validConnectConsistencyLevels))
	for _, valid := range validConnectConsistencyLevels {
		if str == valid.String() {
			*l = valid
			return nil
		}
		strs = append(strs, "'"+valid.String()+"'")
	}
	return fmt.Errorf("invalid ConnectConsistencyLevel '%s' valid types are: %s",
		str, strings.Join(strs, ", "))
}

// ReadConsistencyLevel is the consistency level for reading from a cluster
type ReadConsistencyLevel int

const (
	// ReadConsistencyLevelOne corresponds to reading from a single node
	ReadConsistencyLevelOne ReadConsistencyLevel = iota

	// ReadConsistencyLevelUnstrictMajority corresponds to reading from the majority of nodes
	// but relaxing the constraint when it cannot be met, falling back to returning success when
	// reading from at least a single node after attempting reading from the majority of nodes
	ReadConsistencyLevelUnstrictMajority

	// ReadConsistencyLevelMajority corresponds to reading from the majority of nodes
	ReadConsistencyLevelMajority

	// ReadConsistencyLevelAll corresponds to reading from all of the nodes
	ReadConsistencyLevelAll
)

// String returns the consistency level as a string
func (l ReadConsistencyLevel) String() string {
	switch l {
	case ReadConsistencyLevelOne:
		return "one"
	case ReadConsistencyLevelUnstrictMajority:
		return "unstrict_majority"
	case ReadConsistencyLevelMajority:
		return "majority"
	case ReadConsistencyLevelAll:
		return "all"
	}
	return unknown
}

var validReadConsistencyLevels = []ReadConsistencyLevel{
	ReadConsistencyLevelOne,
	ReadConsistencyLevelUnstrictMajority,
	ReadConsistencyLevelMajority,
	ReadConsistencyLevelAll,
}

var errReadConsistencyLevelUnspecified = errors.New("read consistency level not specified")

// UnmarshalYAML unmarshals an ConnectConsistencyLevel into a valid type from string.
func (l *ReadConsistencyLevel) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}
	if str == "" {
		return errReadConsistencyLevelUnspecified
	}
	strs := make([]string, len(validReadConsistencyLevels))
	for _, valid := range validReadConsistencyLevels {
		if str == valid.String() {
			*l = valid
			return nil
		}
		strs = append(strs, "'"+valid.String()+"'")
	}
	return fmt.Errorf("invalid ReadConsistencyLevel '%s' valid types are: %s",
		str, strings.Join(strs, ", "))
}

// Client can create sessions to write and read to a cluster
type Client interface {
	// Options returns the Client Options.
	Options() Options

	// NewSession creates a new session
	NewSession() (Session, error)

	// DefaultSession creates a default session that gets reused
	DefaultSession() (Session, error)

	// DefaultSessionActive returns whether the default session is active
	DefaultSessionActive() bool
}

// Session can write and read to a cluster
type Session interface {
	// Write value to the database for an ID
	Write(namespace, id ident.ID, t time.Time, value float64, unit xtime.Unit, annotation []byte) error

	// WriteTagged value to the database for an ID and given tags.
	WriteTagged(namespace, id ident.ID, tags ident.TagIterator, t time.Time, value float64, unit xtime.Unit, annotation []byte) error

	// Fetch values from the database for an ID
	Fetch(namespace, id ident.ID, startInclusive, endExclusive time.Time) (encoding.SeriesIterator, error)

	// FetchIDs values from the database for a set of IDs
	FetchIDs(namespace ident.ID, ids ident.Iterator, startInclusive, endExclusive time.Time) (encoding.SeriesIterators, error)

	// FetchTagged resolves the provided query to known IDs, and fetches the data for them.
	FetchTagged(index.Query, index.QueryOptions) (encoding.SeriesIterators, error)

	// FetchTaggedIDs resolves the provided query to known IDs.
	FetchTaggedIDs(index.Query, index.QueryOptions) (index.QueryResults, error)

	// ShardID returns the given shard for an ID for callers
	// to easily discern what shard is failing when operations
	// for given IDs begin failing
	ShardID(id ident.ID) (uint32, error)

	// Close the session
	Close() error
}

// AdminClient can create administration sessions
type AdminClient interface {
	Client

	// NewSession creates a new session
	NewAdminSession() (AdminSession, error)

	// DefaultAdminSession creates a default admin session that gets reused
	DefaultAdminSession() (AdminSession, error)
}

// PeerBlocksMetadataIter iterates over a collection of
// blocks metadata from peers
type PeerBlocksMetadataIter interface {
	// Next returns whether there are more items in the collection
	Next() bool

	// Current returns the host and blocks metadata, which remain
	// valid until Next() is called again.
	Current() (topology.Host, block.BlocksMetadata)

	// Err returns any error encountered
	Err() error
}

// PeerBlocksIter iterates over a collection of blocks from peers
type PeerBlocksIter interface {
	// Next returns whether there are more items in the collection
	Next() bool

	// Current returns the metadata, and block data for a single block replica.
	// These remain valid until Next() is called again.
	Current() (topology.Host, ident.ID, block.DatabaseBlock)

	// Err returns any error encountered
	Err() error
}

// AdminSession can perform administrative and node-to-node operations
type AdminSession interface {
	Session

	// Origin returns the host that initiated the session
	Origin() topology.Host

	// Replicas returns the replication factor
	Replicas() int

	// Truncate will truncate the namespace for a given shard
	Truncate(namespace ident.ID) (int64, error)

	// FetchBlocksMetadataFromPeers will fetch the blocks metadata from
	// available peers
	FetchBlocksMetadataFromPeers(
		namespace ident.ID,
		shard uint32,
		start, end time.Time,
		version FetchBlocksMetadataEndpointVersion,
	) (PeerBlocksMetadataIter, error)

	// FetchBootstrapBlocksFromPeers will fetch the most fulfilled block
	// for each series in a best effort method from available peers
	FetchBootstrapBlocksFromPeers(
		namespace namespace.Metadata,
		shard uint32,
		start, end time.Time,
		opts result.Options,
		version FetchBlocksMetadataEndpointVersion,
	) (result.ShardResult, error)

	// FetchBlocksFromPeers will fetch the required blocks from the
	// peers specified
	FetchBlocksFromPeers(
		namespace namespace.Metadata,
		shard uint32,
		metadatas []block.ReplicaMetadata,
		opts result.Options,
	) (PeerBlocksIter, error)
}

type clientSession interface {
	AdminSession

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

	// Host gets the host
	Host() topology.Host

	// ConnectionCount gets the current open connection count
	ConnectionCount() int

	// ConnectionPool gets the connection pool
	ConnectionPool() connectionPool

	// BorrowConnection will borrow a connection and execute a user function
	BorrowConnection(fn withConnectionFn) error

	// Close the host queue, will flush any operations still pending
	Close()
}

type withConnectionFn func(c rpc.TChanNode)

type connectionPool interface {
	// Open starts the connection pool connecting and health checking
	Open()

	// ConnectionCount gets the current open connection count
	ConnectionCount() int

	// NextClient gets the next client for use by the connection pool
	NextClient() (rpc.TChanNode, error)

	// Close the connection pool
	Close()
}

type peerSource interface {
	// BorrowConnection will borrow a connection and execute a user function
	BorrowConnection(hostID string, fn withConnectionFn) error
}

type peer interface {
	// Host gets the host
	Host() topology.Host

	// BorrowConnection will borrow a connection and execute a user function
	BorrowConnection(fn withConnectionFn) error
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

	// CompletionFn gets the completion function for the operation
	CompletionFn() completionFn
}

// Options is a set of client options
type Options interface {
	// Validate validates the options
	Validate() error

	// SetEncodingM3TSZ sets m3tsz encoding
	SetEncodingM3TSZ() Options

	// SetClockOptions sets the clock options
	SetClockOptions(value clock.Options) Options

	// ClockOptions returns the clock options
	ClockOptions() clock.Options

	// SetInstrumentOptions sets the instrumentation options
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrumentation options
	InstrumentOptions() instrument.Options

	// SetTopologyInitializer sets the TopologyInitializer
	SetTopologyInitializer(value topology.Initializer) Options

	// TopologyInitializer returns the TopologyInitializer
	TopologyInitializer() topology.Initializer

	// SetWriteConsistencyLevel sets the write consistency level
	SetWriteConsistencyLevel(value topology.ConsistencyLevel) Options

	// WriteConsistencyLevel returns the write consistency level
	WriteConsistencyLevel() topology.ConsistencyLevel

	// SetReadConsistencyLevel sets the read consistency level
	SetReadConsistencyLevel(value ReadConsistencyLevel) Options

	// ReadConsistencyLevel returns the read consistency level
	ReadConsistencyLevel() ReadConsistencyLevel

	// SetChannelOptions sets the channelOptions
	SetChannelOptions(value *tchannel.ChannelOptions) Options

	// ChannelOptions returns the channelOptions
	ChannelOptions() *tchannel.ChannelOptions

	// SetMaxConnectionCount sets the maxConnectionCount
	SetMaxConnectionCount(value int) Options

	// MaxConnectionCount returns the maxConnectionCount
	MaxConnectionCount() int

	// SetMinConnectionCount sets the minConnectionCount
	SetMinConnectionCount(value int) Options

	// MinConnectionCount returns the minConnectionCount
	MinConnectionCount() int

	// SetHostConnectTimeout sets the hostConnectTimeout
	SetHostConnectTimeout(value time.Duration) Options

	// HostConnectTimeout returns the hostConnectTimeout
	HostConnectTimeout() time.Duration

	// SetClusterConnectTimeout sets the clusterConnectTimeout
	SetClusterConnectTimeout(value time.Duration) Options

	// ClusterConnectTimeout returns the clusterConnectTimeout
	ClusterConnectTimeout() time.Duration

	// SetClusterConnectConsistencyLevel sets the clusterConnectConsistencyLevel
	SetClusterConnectConsistencyLevel(value ConnectConsistencyLevel) Options

	// ClusterConnectConsistencyLevel returns the clusterConnectConsistencyLevel
	ClusterConnectConsistencyLevel() ConnectConsistencyLevel

	// SetWriteRequestTimeout sets the writeRequestTimeout
	SetWriteRequestTimeout(value time.Duration) Options

	// WriteRequestTimeout returns the writeRequestTimeout
	WriteRequestTimeout() time.Duration

	// SetFetchRequestTimeout sets the fetchRequestTimeout
	SetFetchRequestTimeout(value time.Duration) Options

	// FetchRequestTimeout returns the fetchRequestTimeout
	FetchRequestTimeout() time.Duration

	// SetTruncateRequestTimeout sets the truncateRequestTimeout
	SetTruncateRequestTimeout(value time.Duration) Options

	// TruncateRequestTimeout returns the truncateRequestTimeout
	TruncateRequestTimeout() time.Duration

	// SetBackgroundConnectInterval sets the backgroundConnectInterval
	SetBackgroundConnectInterval(value time.Duration) Options

	// BackgroundConnectInterval returns the backgroundConnectInterval
	BackgroundConnectInterval() time.Duration

	// SetBackgroundConnectStutter sets the backgroundConnectStutter
	SetBackgroundConnectStutter(value time.Duration) Options

	// BackgroundConnectStutter returns the backgroundConnectStutter
	BackgroundConnectStutter() time.Duration

	// SetBackgroundHealthCheckInterval sets the background health check interval
	SetBackgroundHealthCheckInterval(value time.Duration) Options

	// BackgroundHealthCheckInterval returns the background health check interval
	BackgroundHealthCheckInterval() time.Duration

	// SetBackgroundHealthCheckStutter sets the background health check stutter
	SetBackgroundHealthCheckStutter(value time.Duration) Options

	// BackgroundHealthCheckStutter returns the background health check stutter
	BackgroundHealthCheckStutter() time.Duration

	// SetBackgroundHealthCheckFailLimit sets the background health failure
	// limit before connection is deemed unhealth
	SetBackgroundHealthCheckFailLimit(value int) Options

	// BackgroundHealthCheckFailLimit returns the background health failure
	// limit before connection is deemed unhealth
	BackgroundHealthCheckFailLimit() int

	// SetBackgroundHealthCheckFailThrottleFactor sets the throttle factor to
	// apply when calculating how long to wait between a failed health check and
	// a retry attempt. It is applied by multiplying against the host connect
	// timeout to produce a throttle sleep value.
	SetBackgroundHealthCheckFailThrottleFactor(value float64) Options

	// BackgroundHealthCheckFailThrottleFactor returns the throttle factor to
	// apply when calculating how long to wait between a failed health check and
	// a retry attempt. It is applied by multiplying against the host connect
	// timeout to produce a throttle sleep value.
	BackgroundHealthCheckFailThrottleFactor() float64

	// SetWriteRetrier sets the write retrier when performing a write for
	// a write operation. Only retryable errors are retried.
	SetWriteRetrier(value xretry.Retrier) Options

	// WriteRetrier returns the write retrier when perform a write for
	// a write operation. Only retryable errors are retried.
	WriteRetrier() xretry.Retrier

	// SetFetchRetrier sets the fetch retrier when performing a write for
	// a fetch operation. Only retryable errors are retried.
	SetFetchRetrier(value xretry.Retrier) Options

	// FetchRetrier returns the fetch retrier when performing a fetch for
	// a fetch operation. Only retryable errors are retried.
	FetchRetrier() xretry.Retrier

	// SetWriteBatchSize sets the writeBatchSize
	// NB(r): for a write only application load this should match the host
	// queue ops flush size so that each time a host queue is flushed it can
	// fit the entire flushed write ops into a single batch.
	SetWriteBatchSize(value int) Options

	// WriteBatchSize returns the writeBatchSize
	WriteBatchSize() int

	// SetFetchBatchSize sets the fetchBatchSize
	// NB(r): for a fetch only application load this should match the host
	// queue ops flush size so that each time a host queue is flushed it can
	// fit the entire flushed fetch ops into a single batch.
	SetFetchBatchSize(value int) Options

	// FetchBatchSize returns the fetchBatchSize
	FetchBatchSize() int

	// SetWriteOpPoolSize sets the writeOpPoolSize
	SetWriteOpPoolSize(value int) Options

	// WriteOpPoolSize returns the writeOpPoolSize
	WriteOpPoolSize() int

	// SetFetchBatchOpPoolSize sets the fetchBatchOpPoolSize
	SetFetchBatchOpPoolSize(value int) Options

	// FetchBatchOpPoolSize returns the fetchBatchOpPoolSize
	FetchBatchOpPoolSize() int

	// SetHostQueueOpsFlushSize sets the hostQueueOpsFlushSize
	SetHostQueueOpsFlushSize(value int) Options

	// HostQueueOpsFlushSize returns the hostQueueOpsFlushSize
	HostQueueOpsFlushSize() int

	// SetHostQueueOpsFlushInterval sets the hostQueueOpsFlushInterval
	SetHostQueueOpsFlushInterval(value time.Duration) Options

	// HostQueueOpsFlushInterval returns the hostQueueOpsFlushInterval
	HostQueueOpsFlushInterval() time.Duration

	// SetContextPool sets the contextPool
	SetContextPool(value context.Pool) Options

	// ContextPool returns the contextPool
	ContextPool() context.Pool

	// SetIdentifierPool sets the identifier pool
	SetIdentifierPool(value ident.Pool) Options

	// IdentifierPool returns the identifier pool
	IdentifierPool() ident.Pool

	// HostQueueOpsArrayPoolSize sets the hostQueueOpsArrayPoolSize
	SetHostQueueOpsArrayPoolSize(value int) Options

	// HostQueueOpsArrayPoolSize returns the hostQueueOpsArrayPoolSize
	HostQueueOpsArrayPoolSize() int

	// SetSeriesIteratorPoolSize sets the seriesIteratorPoolSize
	SetSeriesIteratorPoolSize(value int) Options

	// SeriesIteratorPoolSize returns the seriesIteratorPoolSize
	SeriesIteratorPoolSize() int

	// SetSeriesIteratorArrayPoolBuckets sets the seriesIteratorArrayPoolBuckets
	SetSeriesIteratorArrayPoolBuckets(value []pool.Bucket) Options

	// SeriesIteratorArrayPoolBuckets returns the seriesIteratorArrayPoolBuckets
	SeriesIteratorArrayPoolBuckets() []pool.Bucket

	// SetReaderIteratorAllocate sets the readerIteratorAllocate
	SetReaderIteratorAllocate(value encoding.ReaderIteratorAllocate) Options

	// ReaderIteratorAllocate returns the readerIteratorAllocate
	ReaderIteratorAllocate() encoding.ReaderIteratorAllocate
}

// AdminOptions is a set of administration client options
type AdminOptions interface {
	Options

	// SetOrigin sets the current host originating requests from
	SetOrigin(value topology.Host) AdminOptions

	// Origin gets the current host originating requests from
	Origin() topology.Host

	// SetFetchSeriesBlocksMaxBlockRetries sets the max retries for fetching series blocks
	SetFetchSeriesBlocksMaxBlockRetries(value int) AdminOptions

	// FetchSeriesBlocksMaxBlockRetries gets the max retries for fetching series blocks
	FetchSeriesBlocksMaxBlockRetries() int

	// SetFetchSeriesBlocksBatchSize sets the batch size for fetching series blocks in batch
	SetFetchSeriesBlocksBatchSize(value int) AdminOptions

	// FetchSeriesBlocksBatchSize gets the batch size for fetching series blocks in batch
	FetchSeriesBlocksBatchSize() int

	// SetFetchSeriesBlocksMetadataBatchTimeout sets the timeout for fetching series blocks metadata in batch
	SetFetchSeriesBlocksMetadataBatchTimeout(value time.Duration) AdminOptions

	// FetchSeriesBlocksMetadataBatchTimeout gets the timeout for fetching series blocks metadata in batch
	FetchSeriesBlocksMetadataBatchTimeout() time.Duration

	// SetFetchSeriesBlocksBatchTimeout sets the timeout for fetching series blocks in batch
	SetFetchSeriesBlocksBatchTimeout(value time.Duration) AdminOptions

	// FetchSeriesBlocksBatchTimeout gets the timeout for fetching series blocks in batch
	FetchSeriesBlocksBatchTimeout() time.Duration

	// SetFetchSeriesBlocksBatchConcurrency sets the concurrency for fetching series blocks in batch
	SetFetchSeriesBlocksBatchConcurrency(value int) AdminOptions

	// FetchSeriesBlocksBatchConcurrency gets the concurrency for fetching series blocks in batch
	FetchSeriesBlocksBatchConcurrency() int

	// SetStreamBlocksRetrier sets the retrier for streaming blocks
	SetStreamBlocksRetrier(value xretry.Retrier) AdminOptions

	// StreamBlocksRetrier returns the retrier for streaming blocks
	StreamBlocksRetrier() xretry.Retrier
}
