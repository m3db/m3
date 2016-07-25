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

package m3db

import (
	"time"

	"github.com/m3db/m3db/x/logging"
	"github.com/m3db/m3db/x/metrics"

	tchannel "github.com/uber/tchannel-go"
)

// NowFn is the function supplied to determine "now"
type NowFn func() time.Time

// DatabaseOptions is a set of database options
type DatabaseOptions interface {
	// EncodingTszPooled sets tsz encoding with pooling and returns a new DatabaseOptions
	EncodingTszPooled(bufferBucketAllocSize, databaseBlockAllocSize int) DatabaseOptions

	// EncodingTsz sets tsz encoding and returns a new DatabaseOptions
	EncodingTsz() DatabaseOptions

	// Logger sets the logger and returns a new DatabaseOptions
	Logger(value logging.Logger) DatabaseOptions

	// GetLogger returns the logger
	GetLogger() logging.Logger

	// MetricsScope sets the metricsScope and returns a new DatabaseOptions
	MetricsScope(value metrics.Scope) DatabaseOptions

	// GetMetricsScope returns the metricsScope
	GetMetricsScope() metrics.Scope

	// BlockSize sets the blockSize and returns a new DatabaseOptions
	BlockSize(value time.Duration) DatabaseOptions

	// GetBlockSize returns the blockSize
	GetBlockSize() time.Duration

	// NewEncoderFn sets the newEncoderFn and returns a new DatabaseOptions
	// TODO(r): now that we have an encoder pool consider removing newencoderfn being required
	NewEncoderFn(value NewEncoderFn) DatabaseOptions

	// GetNewEncoderFn returns the newEncoderFn
	GetNewEncoderFn() NewEncoderFn

	// NewDecoderFn sets the newDecoderFn and returns a new DatabaseOptions
	NewDecoderFn(value NewDecoderFn) DatabaseOptions

	// GetNewDecoderFn returns the newDecoderFn
	GetNewDecoderFn() NewDecoderFn

	// NowFn sets the nowFn and returns a new DatabaseOptions
	NowFn(value NowFn) DatabaseOptions

	// GetNowFn returns the nowFn
	GetNowFn() NowFn

	// BufferFuture sets the bufferFuture and returns a new DatabaseOptions
	BufferFuture(value time.Duration) DatabaseOptions

	// GetBufferFuture returns the bufferFuture
	GetBufferFuture() time.Duration

	// BufferPast sets the bufferPast and returns a new DatabaseOptions
	BufferPast(value time.Duration) DatabaseOptions

	// GetBufferPast returns the bufferPast
	GetBufferPast() time.Duration

	// BufferDrain sets the bufferDrain and returns a new DatabaseOptions
	BufferDrain(value time.Duration) DatabaseOptions

	// GetBufferDrain returns the bufferDrain
	GetBufferDrain() time.Duration

	// BufferBucketAllocSize sets the bufferBucketAllocSize and returns a new DatabaseOptions
	BufferBucketAllocSize(value int) DatabaseOptions

	// GetBufferBucketAllocSize returns the bufferBucketAllocSize
	GetBufferBucketAllocSize() int

	// DatabaseBlockAllocSize sets the databaseBlockAllocSize and returns a new DatabaseOptions
	DatabaseBlockAllocSize(value int) DatabaseOptions

	// GetDatabaseBlockAllocSize returns the databaseBlockAllocSize
	GetDatabaseBlockAllocSize() int

	// RetentionPeriod sets how long we intend to keep data in memory.
	RetentionPeriod(value time.Duration) DatabaseOptions

	// RetentionPeriod is how long we intend to keep data in memory.
	GetRetentionPeriod() time.Duration

	// NewBootstrapFn sets the newBootstrapFn and returns a new DatabaseOptions
	NewBootstrapFn(value NewBootstrapFn) DatabaseOptions

	// GetBootstrapFn returns the newBootstrapFn
	GetBootstrapFn() NewBootstrapFn

	// BytesPool sets the bytesPool and returns a new DatabaseOptions
	BytesPool(value BytesPool) DatabaseOptions

	// GetBytesPool returns the bytesPool
	GetBytesPool() BytesPool

	// ContextPool sets the contextPool and returns a new DatabaseOptions
	ContextPool(value ContextPool) DatabaseOptions

	// GetContextPool returns the contextPool
	GetContextPool() ContextPool

	// DatabaseBlockPool sets the databaseBlockPool and returns a new DatabaseOptions
	DatabaseBlockPool(value DatabaseBlockPool) DatabaseOptions

	// GetDatabaseBlockPool returns the databaseBlockPool
	GetDatabaseBlockPool() DatabaseBlockPool

	// EncoderPool sets the encoderPool and returns a new DatabaseOptions
	EncoderPool(value EncoderPool) DatabaseOptions

	// GetEncoderPool returns the encoderPool
	GetEncoderPool() EncoderPool

	// SegmentReaderPool sets the segment reader pool.
	SegmentReaderPool(value SegmentReaderPool) DatabaseOptions

	// GetSegmentReaderPool returns the segment reader pool.
	GetSegmentReaderPool() SegmentReaderPool

	// ReaderIteratorPool sets the readerIteratorPool and returns a new DatabaseOptions
	ReaderIteratorPool(value ReaderIteratorPool) DatabaseOptions

	// GetReaderIteratorPool returns the readerIteratorPool
	GetReaderIteratorPool() ReaderIteratorPool

	// MultiReaderIteratorPool sets the multiReaderIteratorPool and returns a new DatabaseOptions
	MultiReaderIteratorPool(value MultiReaderIteratorPool) DatabaseOptions

	// GetMultiReaderIteratorPool returns the multiReaderIteratorPool
	GetMultiReaderIteratorPool() MultiReaderIteratorPool

	// MaxFlushRetries sets the maximum number of retries when data flushing fails.
	MaxFlushRetries(value int) DatabaseOptions

	// GetMaxFlushRetries returns the maximum number of retries when data flushing fails.
	GetMaxFlushRetries() int

	// FilePathPrefix sets the file path prefix for sharded TSDB files.
	FilePathPrefix(value string) DatabaseOptions

	// GetFilePathPrefix returns the file path prefix for sharded TSDB files.
	GetFilePathPrefix() string

	// NewFileSetReaderFn sets the function for creating a new fileset reader.
	NewFileSetReaderFn(value NewFileSetReaderFn) DatabaseOptions

	// GetNewFileSetReaderFn returns the function for creating a new fileset reader.
	GetNewFileSetReaderFn() NewFileSetReaderFn

	// NewFileSetWriterFn sets the function for creating a new fileset writer.
	NewFileSetWriterFn(value NewFileSetWriterFn) DatabaseOptions

	// GetNewFileSetWriterFn returns the function for creating a new fileset writer.
	GetNewFileSetWriterFn() NewFileSetWriterFn

	// NewPersistenceManagerFn sets the function for creating a new persistence manager.
	NewPersistenceManagerFn(value NewPersistenceManagerFn) DatabaseOptions

	// GetNewPersistenceManagerFn returns the function for creating a new persistence manager.
	GetNewPersistenceManagerFn() NewPersistenceManagerFn
}

// ClientOptions is a set of client options
type ClientOptions interface {
	// Validate validates the options
	Validate() error

	// EncodingTsz sets tsz encoding and returns a new ClientOptions
	EncodingTsz() ClientOptions

	// Logger sets the logger and returns a new ClientOptions
	Logger(value logging.Logger) ClientOptions

	// GetLogger returns the logger
	GetLogger() logging.Logger

	// MetricsScope sets the metricsScope and returns a new ClientOptions
	MetricsScope(value metrics.Scope) ClientOptions

	// GetMetricsScope returns the metricsScope
	GetMetricsScope() metrics.Scope

	// TopologyType sets the topologyType and returns a new ClientOptions
	TopologyType(value TopologyType) ClientOptions

	// GetTopologyType returns the topologyType
	GetTopologyType() TopologyType

	// ConsistencyLevel sets the consistencyLevel and returns a new ClientOptions
	ConsistencyLevel(value ConsistencyLevel) ClientOptions

	// GetConsistencyLevel returns the consistencyLevel
	GetConsistencyLevel() ConsistencyLevel

	// ChannelOptions sets the channelOptions and returns a new ClientOptions
	ChannelOptions(value *tchannel.ChannelOptions) ClientOptions

	// GetChannelOptions returns the channelOptions
	GetChannelOptions() *tchannel.ChannelOptions

	// NowFn sets the nowFn and returns a new ClientOptions
	NowFn(value NowFn) ClientOptions

	// GetNowFn returns the nowFn
	GetNowFn() NowFn

	// MaxConnectionCount sets the maxConnectionCount and returns a new ClientOptions
	MaxConnectionCount(value int) ClientOptions

	// GetMaxConnectionCount returns the maxConnectionCount
	GetMaxConnectionCount() int

	// MinConnectionCount sets the minConnectionCount and returns a new ClientOptions
	MinConnectionCount(value int) ClientOptions

	// GetMinConnectionCount returns the minConnectionCount
	GetMinConnectionCount() int

	// HostConnectTimeout sets the hostConnectTimeout and returns a new ClientOptions
	HostConnectTimeout(value time.Duration) ClientOptions

	// GetHostConnectTimeout returns the hostConnectTimeout
	GetHostConnectTimeout() time.Duration

	// ClusterConnectTimeout sets the clusterConnectTimeout and returns a new ClientOptions
	ClusterConnectTimeout(value time.Duration) ClientOptions

	// GetClusterConnectTimeout returns the clusterConnectTimeout
	GetClusterConnectTimeout() time.Duration

	// WriteRequestTimeout sets the writeRequestTimeout and returns a new ClientOptions
	WriteRequestTimeout(value time.Duration) ClientOptions

	// GetWriteRequestTimeout returns the writeRequestTimeout
	GetWriteRequestTimeout() time.Duration

	// FetchRequestTimeout sets the fetchRequestTimeout and returns a new ClientOptions
	FetchRequestTimeout(value time.Duration) ClientOptions

	// GetFetchRequestTimeout returns the fetchRequestTimeout
	GetFetchRequestTimeout() time.Duration

	// BackgroundConnectInterval sets the backgroundConnectInterval and returns a new ClientOptions
	BackgroundConnectInterval(value time.Duration) ClientOptions

	// GetBackgroundConnectInterval returns the backgroundConnectInterval
	GetBackgroundConnectInterval() time.Duration

	// BackgroundConnectStutter sets the backgroundConnectStutter and returns a new ClientOptions
	BackgroundConnectStutter(value time.Duration) ClientOptions

	// GetBackgroundConnectStutter returns the backgroundConnectStutter
	GetBackgroundConnectStutter() time.Duration

	// BackgroundHealthCheckInterval sets the backgroundHealthCheckInterval and returns a new ClientOptions
	BackgroundHealthCheckInterval(value time.Duration) ClientOptions

	// GetBackgroundHealthCheckInterval returns the backgroundHealthCheckInterval
	GetBackgroundHealthCheckInterval() time.Duration

	// BackgroundHealthCheckStutter sets the backgroundHealthCheckStutter and returns a new ClientOptions
	BackgroundHealthCheckStutter(value time.Duration) ClientOptions

	// GetBackgroundHealthCheckStutter returns the backgroundHealthCheckStutter
	GetBackgroundHealthCheckStutter() time.Duration

	// WriteOpPoolSize sets the writeOpPoolSize and returns a new ClientOptions
	WriteOpPoolSize(value int) ClientOptions

	// GetWriteOpPoolSize returns the writeOpPoolSize
	GetWriteOpPoolSize() int

	// FetchBatchOpPoolSize sets the fetchBatchOpPoolSize and returns a new ClientOptions
	FetchBatchOpPoolSize(value int) ClientOptions

	// GetFetchBatchOpPoolSize returns the fetchBatchOpPoolSize
	GetFetchBatchOpPoolSize() int

	// WriteBatchSize sets the writeBatchSize and returns a new ClientOptions
	// NB(r): for a write only application load this should match the host
	// queue ops flush size so that each time a host queue is flushed it can
	// fit the entire flushed write ops into a single batch.
	WriteBatchSize(value int) ClientOptions

	// GetWriteBatchSize returns the writeBatchSize
	GetWriteBatchSize() int

	// FetchBatchSize sets the fetchBatchSize and returns a new ClientOptions
	// NB(r): for a fetch only application load this should match the host
	// queue ops flush size so that each time a host queue is flushed it can
	// fit the entire flushed fetch ops into a single batch.
	FetchBatchSize(value int) ClientOptions

	// GetFetchBatchSize returns the fetchBatchSize
	GetFetchBatchSize() int

	// HostQueueOpsFlushSize sets the hostQueueOpsFlushSize and returns a new ClientOptions
	HostQueueOpsFlushSize(value int) ClientOptions

	// GetHostQueueOpsFlushSize returns the hostQueueOpsFlushSize
	GetHostQueueOpsFlushSize() int

	// HostQueueOpsFlushInterval sets the hostQueueOpsFlushInterval and returns a new ClientOptions
	HostQueueOpsFlushInterval(value time.Duration) ClientOptions

	// GetHostQueueOpsFlushInterval returns the hostQueueOpsFlushInterval
	GetHostQueueOpsFlushInterval() time.Duration

	// HostQueueOpsArrayPoolSize sets the hostQueueOpsArrayPoolSize and returns a new ClientOptions
	HostQueueOpsArrayPoolSize(value int) ClientOptions

	// GetHostQueueOpsArrayPoolSize returns the hostQueueOpsArrayPoolSize
	GetHostQueueOpsArrayPoolSize() int

	// SeriesIteratorPoolSize sets the seriesIteratorPoolSize and returns a new ClientOptions
	SeriesIteratorPoolSize(value int) ClientOptions

	// GetSeriesIteratorPoolSize returns the seriesIteratorPoolSize
	GetSeriesIteratorPoolSize() int

	// SeriesIteratorArrayPoolBuckets sets the seriesIteratorArrayPoolBuckets and returns a new ClientOptions
	SeriesIteratorArrayPoolBuckets(value []PoolBucket) ClientOptions

	// GetSeriesIteratorArrayPoolBuckets returns the seriesIteratorArrayPoolBuckets
	GetSeriesIteratorArrayPoolBuckets() []PoolBucket

	// ReaderIteratorAllocate sets the readerIteratorAllocate and returns a new ClientOptions
	ReaderIteratorAllocate(value ReaderIteratorAllocate) ClientOptions

	// GetReaderIteratorAllocate returns the readerIteratorAllocate
	GetReaderIteratorAllocate() ReaderIteratorAllocate
}

// TopologyTypeOptions is a set of static topology type options
type TopologyTypeOptions interface {
	// Validate validates the options
	Validate() error

	// ShardScheme sets the shardScheme and returns a new TopologyTypeOptions
	ShardScheme(value ShardScheme) TopologyTypeOptions

	// GetShardScheme returns the shardScheme
	GetShardScheme() ShardScheme

	// Replicas sets the replicas and returns a new TopologyTypeOptions
	Replicas(value int) TopologyTypeOptions

	// GetReplicas returns the replicas
	GetReplicas() int

	// HostShardSets sets the hostShardSets and returns a new TopologyTypeOptions
	HostShardSets(value []HostShardSet) TopologyTypeOptions

	// GetHostShardSets returns the hostShardSets
	GetHostShardSets() []HostShardSet
}
