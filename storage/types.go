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

package storage

import (
	"bytes"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/persist/fs/commitlog"
	"github.com/m3db/m3db/ratelimit"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/storage/repair"
	"github.com/m3db/m3db/storage/series"
	"github.com/m3db/m3db/ts"
	xcounter "github.com/m3db/m3db/x/counter"
	xio "github.com/m3db/m3db/x/io"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"
)

// Database is a time series database
type Database interface {
	// Options returns the database options
	Options() Options

	// AssignShardSet sets the shard set assignment and returns immediately
	AssignShardSet(shardSet sharding.ShardSet)

	// Namespaces returns the namespaces
	Namespaces() []Namespace

	// Open will open the database for writing and reading
	Open() error

	// Close will close the database for writing and reading
	Close() error

	// Write value to the database for an ID
	Write(
		ctx context.Context,
		namespace ts.ID,
		id ts.ID,
		timestamp time.Time,
		value float64,
		unit xtime.Unit,
		annotation []byte,
	) error

	// ReadEncoded retrieves encoded segments for an ID
	ReadEncoded(
		ctx context.Context,
		namespace ts.ID,
		id ts.ID,
		start, end time.Time,
	) ([][]xio.SegmentReader, error)

	// FetchBlocks retrieves data blocks for a given id and a list of block start times.
	FetchBlocks(
		ctx context.Context,
		namespace ts.ID,
		shard uint32,
		id ts.ID,
		starts []time.Time,
	) ([]block.FetchBlockResult, error)

	// FetchBlocksMetadata retrieves blocks metadata for a given shard, returns the
	// fetched block metadata results, the next page token, and any error encountered.
	// If we have fetched all the block metadata, we return nil as the next page token.
	FetchBlocksMetadata(
		ctx context.Context,
		namespace ts.ID,
		shard uint32,
		start, end time.Time,
		limit int64,
		pageToken int64,
		includeSizes bool,
		includeChecksums bool,
	) (block.FetchBlocksMetadataResults, *int64, error)

	// Bootstrap bootstraps the database.
	Bootstrap() error

	// IsBootstrapped determines whether the database is bootstrapped.
	IsBootstrapped() bool

	// IsOverloaded determines whether the database is overloaded
	IsOverloaded() bool

	// Repair will issue a repair and return nil on success or error on error.
	Repair() error

	// Truncate truncates data for the given namespace
	Truncate(namespace ts.ID) (int64, error)
}

// database is the internal database interface
type database interface {
	Database

	// getOwnedNamespaces returns the namespaces this database owns.
	getOwnedNamespaces() []databaseNamespace
}

// Namespace is a time series database namespace
type Namespace interface {
	// ID returns the ID of the namespace
	ID() ts.ID

	// NumSeries returns the number of series in the namespace
	NumSeries() int64

	// Shards returns the shards
	Shards() []Shard
}

// NamespacesByID is a sortable slice of namespaces by ID
type NamespacesByID []Namespace

func (n NamespacesByID) Len() int      { return len(n) }
func (n NamespacesByID) Swap(i, j int) { n[i], n[j] = n[j], n[i] }
func (n NamespacesByID) Less(i, j int) bool {
	return bytes.Compare(n[i].ID().Data().Get(), n[j].ID().Data().Get()) < 0
}

type databaseNamespace interface {
	Namespace

	// AssignShardSet sets the shard set assignment and returns immediately
	AssignShardSet(shardSet sharding.ShardSet)

	// Tick performs any regular maintenance operations
	Tick(c context.Cancellable, softDeadline time.Duration)

	// Write writes a data point
	Write(
		ctx context.Context,
		id ts.ID,
		timestamp time.Time,
		value float64,
		unit xtime.Unit,
		annotation []byte,
	) error

	// ReadEncoded reads data for given id within [start, end)
	ReadEncoded(
		ctx context.Context,
		id ts.ID,
		start, end time.Time,
	) ([][]xio.SegmentReader, error)

	// FetchBlocks retrieves data blocks for a given id and a list of block start times.
	FetchBlocks(
		ctx context.Context,
		shardID uint32,
		id ts.ID,
		starts []time.Time,
	) ([]block.FetchBlockResult, error)

	// FetchBlocksMetadata retrieves the blocks metadata.
	FetchBlocksMetadata(
		ctx context.Context,
		shardID uint32,
		start, end time.Time,
		limit int64,
		pageToken int64,
		includeSizes bool,
		includeChecksums bool,
	) (block.FetchBlocksMetadataResults, *int64, error)

	// Bootstrap performs bootstrapping
	Bootstrap(
		process bootstrap.Process,
		targetRanges []bootstrap.TargetRange,
	) error

	// Flush flushes in-memory data
	Flush(blockStart time.Time, flush persist.Flush) error

	// NeedsFlush returns true if the namespace needs a flush for a block start.
	NeedsFlush(blockStart time.Time) bool

	// CleanupFileset cleans up fileset files
	CleanupFileset(earliestToRetain time.Time) error

	// Truncate truncates the in-memory data for this namespace
	Truncate() (int64, error)

	// Repair repairs the namespace data for a given time range
	Repair(repairer databaseShardRepairer, tr xtime.Range) error
}

// Shard is a time series database shard
type Shard interface {
	// ID returns the ID of the shard
	ID() uint32

	// NumSeries returns the number of series in the shard
	NumSeries() int64

	// IsBootstrapped returns whether the shard is already bootstrapped
	IsBootstrapped() bool
}

type databaseShard interface {
	Shard

	// Close will release the shard resources and close the shard
	Close() error

	// Tick performs any updates to ensure series drain their buffers and blocks are flushed, etc
	Tick(c context.Cancellable, softDeadline time.Duration) tickResult

	Write(
		ctx context.Context,
		id ts.ID,
		timestamp time.Time,
		value float64,
		unit xtime.Unit,
		annotation []byte,
	) error

	ReadEncoded(
		ctx context.Context,
		id ts.ID,
		start, end time.Time,
	) ([][]xio.SegmentReader, error)

	// FetchBlocks retrieves data blocks for a given id and a list of block start times.
	FetchBlocks(
		ctx context.Context,
		id ts.ID,
		starts []time.Time,
	) ([]block.FetchBlockResult, error)

	// FetchBlocksMetadata retrieves the blocks metadata.
	FetchBlocksMetadata(
		ctx context.Context,
		start, end time.Time,
		limit int64,
		pageToken int64,
		includeSizes bool,
		includeChecksums bool,
	) (block.FetchBlocksMetadataResults, *int64)

	Bootstrap(
		bootstrappedSeries map[ts.Hash]result.DatabaseSeriesBlocks,
	) error

	// Flush flushes the series in this shard.
	Flush(
		namespace ts.ID,
		blockStart time.Time,
		flush persist.Flush,
	) error

	// FlushState returns the flush state for this shard at block start.
	FlushState(blockStart time.Time) fileOpState

	// CleanupFileset cleans up fileset files
	CleanupFileset(namespace ts.ID, earliestToRetain time.Time) error

	// Repair repairs the shard data for a given time
	Repair(
		ctx context.Context,
		namespace ts.ID,
		tr xtime.Range,
		repairer databaseShardRepairer,
	) (repair.MetadataComparisonResult, error)
}

// databaseBootstrapManager manages the bootstrap process.
type databaseBootstrapManager interface {
	// IsBootstrapped returns whether the database is already bootstrapped.
	IsBootstrapped() bool

	// Bootstrap performs bootstrapping for all namespaces and shards owned.
	Bootstrap() error

	// Report reports runtime information
	Report()
}

// databaseFlushManager manages flushing in-memory data to persistent storage.
type databaseFlushManager interface {
	// NeedsFlush returns true if the data for a given time have been flushed.
	NeedsFlush(t time.Time) bool

	// FlushTimeStart is the earliest flushable time.
	FlushTimeStart(t time.Time) time.Time

	// FlushTimeEnd is the latest flushable time.
	FlushTimeEnd(t time.Time) time.Time

	// Flush flushes in-memory data to persistent storage.
	Flush(t time.Time) error

	// SetRateLimitOptions sets the rate limit options
	SetRateLimitOptions(value ratelimit.Options)

	// RateLimitOptions returns the rate limit options
	RateLimitOptions() ratelimit.Options

	// Report reports runtime information
	Report()
}

// databaseCleanupManager manages cleaning up persistent storage space.
type databaseCleanupManager interface {
	// Cleanup cleans up data not needed in the persistent storage.
	Cleanup(t time.Time) error

	// Report reports runtime information
	Report()
}

// FileOpOptions control the database file operations behavior
type FileOpOptions interface {
	// SetRetentionOptions sets the retention options
	SetRetentionOptions(value retention.Options) FileOpOptions

	// RetentionOptions returns the retention options
	RetentionOptions() retention.Options

	// SetJitter sets the jitter for database file operations
	SetJitter(value time.Duration) FileOpOptions

	// Jitter returns the jitter for database file operations
	Jitter() time.Duration

	// Validate validates the options
	Validate() error
}

// databaseFileSystemManager manages the database related filesystem activities.
type databaseFileSystemManager interface {
	// Cleanup cleans up data not needed in the persistent storage.
	Cleanup(t time.Time) error

	// Flush flushes in-memory data to persistent storage.
	Flush(t time.Time) error

	// Disable disables the filesystem manager and prevents it from
	// performing file operations, returns the current file operation status
	Disable() fileOpStatus

	// Enable enables the filesystem manager to perform file operations
	Enable() fileOpStatus

	// Status returns the file operation status
	Status() fileOpStatus

	// Run attempts to perform all filesystem-related operations,
	// returning true if those operations are performed, and false otherwise
	Run(t time.Time, runType runType, forceType forceType) bool

	// Report reports runtime information
	Report()
}

// databaseShardRepairer repairs in-memory data for a shard
type databaseShardRepairer interface {
	// Options returns the repair options
	Options() repair.Options

	// Repair repairs the data for a given namespace and shard
	Repair(
		ctx context.Context,
		namespace ts.ID,
		tr xtime.Range,
		shard databaseShard,
	) (repair.MetadataComparisonResult, error)
}

// databaseRepairer repairs in-memory database data
type databaseRepairer interface {
	// Start starts the repair process
	Start()

	// Stop stops the repair process
	Stop()

	// Repair repairs in-memory data
	Repair() error

	// Report reports runtime information
	Report()
}

// databaseTickManager performs periodic ticking
type databaseTickManager interface {
	// Tick performs maintenance operations, restarting the current
	// tick if force is true. It returns nil if a new tick has
	// completed successfully, and an error otherwise.
	Tick(softDeadline time.Duration, forceType forceType) error
}

// databaseMediator mediates actions among various database managers
type databaseMediator interface {
	// Open opens the mediator
	Open() error

	// IsBootstrapped returns whether the database is bootstrapped
	IsBootstrapped() bool

	// Bootstrap bootstraps the database with file operations performed at the end
	Bootstrap() error

	// DisableFileOps disables file operations
	DisableFileOps()

	// EnableFileOps enables file operations
	EnableFileOps()

	// Tick performs a tick
	Tick(softDeadline time.Duration, runType runType, forceType forceType) error

	// Repair repairs the database
	Repair() error

	// Close closes the mediator
	Close() error

	// Report reports runtime information
	Report()
}

// Options represents the options for storage
type Options interface {
	// SetEncodingM3TSZPooled sets m3tsz encoding with pooling
	SetEncodingM3TSZPooled() Options

	// SetClockOptions sets the clock options
	SetClockOptions(value clock.Options) Options

	// ClockOptions returns the clock options
	ClockOptions() clock.Options

	// SetInstrumentOptions sets the instrumentation options
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrumentation options
	InstrumentOptions() instrument.Options

	// SetRetentionOptions sets the retention options
	SetRetentionOptions(value retention.Options) Options

	// RetentionOptions returns the retention options
	RetentionOptions() retention.Options

	// SetDatabaseBlockOptions sets the database block options
	SetDatabaseBlockOptions(value block.Options) Options

	// DatabaseBlockOptions returns the database block options
	DatabaseBlockOptions() block.Options

	// SetCommitLogOptions sets the commit log options
	SetCommitLogOptions(value commitlog.Options) Options

	// CommitLogOptions returns the commit log options
	CommitLogOptions() commitlog.Options

	// SetErrorCounterOptions sets the error counter options
	SetErrorCounterOptions(value xcounter.Options) Options

	// ErrorCounterOptions returns the error counter options
	ErrorCounterOptions() xcounter.Options

	// SetErrorWindowForLoad sets the error window for load
	SetErrorWindowForLoad(value time.Duration) Options

	// ErrorWindowForLoad returns the error window for load
	ErrorWindowForLoad() time.Duration

	// SetErrorThresholdForLoad sets the error threshold for load
	SetErrorThresholdForLoad(value int64) Options

	// ErrorThresholdForLoad returns the error threshold for load
	ErrorThresholdForLoad() int64

	// SetRepairEnabled sets whether or not to enable the repair
	SetRepairEnabled(b bool) Options

	// RepairEnabled returns whether the repair is enabled
	RepairEnabled() bool

	// SetRepairOptions sets the repair options
	SetRepairOptions(value repair.Options) Options

	// RepairOptions returns the repair options
	RepairOptions() repair.Options

	// SetFileOpOptions sets the file op options
	SetFileOpOptions(value FileOpOptions) Options

	// FileOpOptions returns the repair options
	FileOpOptions() FileOpOptions

	// SetBootstrapProcess sets the bootstrap process for the database
	SetBootstrapProcess(value bootstrap.Process) Options

	// BootstrapProcess returns the bootstrap process for the database
	BootstrapProcess() bootstrap.Process

	// SetPersistManager sets the persistence manager
	SetPersistManager(value persist.Manager) Options

	// PersistManager returns the persistence manager
	PersistManager() persist.Manager

	// SetMaxFlushRetries sets the maximum number of retries when data flushing fails
	SetMaxFlushRetries(value int) Options

	// MaxFlushRetries returns the maximum number of retries when data flushing fails
	MaxFlushRetries() int

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
	) Options

	// NewBlockRetrieverFn returns the new block retriever constructor to
	// use when bootstrapping retrievable blocks instead of blocks
	// containing data.
	DatabaseBlockRetrieverManager() block.DatabaseBlockRetrieverManager

	// SetContextPool sets the contextPool
	SetContextPool(value context.Pool) Options

	// ContextPool returns the contextPool
	ContextPool() context.Pool

	// SetDatabaseSeriesPool sets the database series pool
	SetDatabaseSeriesPool(value series.DatabaseSeriesPool) Options

	// DatabaseSeriesPool returns the database series pool
	DatabaseSeriesPool() series.DatabaseSeriesPool

	// SetBytesPool sets the bytesPool
	SetBytesPool(value pool.CheckedBytesPool) Options

	// BytesPool returns the bytesPool
	BytesPool() pool.CheckedBytesPool

	// SetEncoderPool sets the contextPool
	SetEncoderPool(value encoding.EncoderPool) Options

	// EncoderPool returns the contextPool
	EncoderPool() encoding.EncoderPool

	// SetSegmentReaderPool sets the contextPool
	SetSegmentReaderPool(value xio.SegmentReaderPool) Options

	// SegmentReaderPool returns the contextPool
	SegmentReaderPool() xio.SegmentReaderPool

	// SetReaderIteratorPool sets the readerIteratorPool
	SetReaderIteratorPool(value encoding.ReaderIteratorPool) Options

	// ReaderIteratorPool returns the readerIteratorPool
	ReaderIteratorPool() encoding.ReaderIteratorPool

	// SetMultiReaderIteratorPool sets the multiReaderIteratorPool
	SetMultiReaderIteratorPool(value encoding.MultiReaderIteratorPool) Options

	// MultiReaderIteratorPool returns the multiReaderIteratorPool
	MultiReaderIteratorPool() encoding.MultiReaderIteratorPool

	// SetIDPool sets the ID pool.
	SetIdentifierPool(value ts.IdentifierPool) Options

	// IDPool returns the ID pool.
	IdentifierPool() ts.IdentifierPool

	// SetFetchBlockMetadataResultsPool sets the fetchBlockMetadataResultsPool
	SetFetchBlockMetadataResultsPool(value block.FetchBlockMetadataResultsPool) Options

	// FetchBlockMetadataResultsPool returns the fetchBlockMetadataResultsPool
	FetchBlockMetadataResultsPool() block.FetchBlockMetadataResultsPool

	// SetFetchBlocksMetadataResultsPool sets the fetchBlocksMetadataResultsPool
	SetFetchBlocksMetadataResultsPool(value block.FetchBlocksMetadataResultsPool) Options

	// FetchBlocksMetadataResultsPool returns the fetchBlocksMetadataResultsPool
	FetchBlocksMetadataResultsPool() block.FetchBlocksMetadataResultsPool
}
