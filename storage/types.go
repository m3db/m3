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
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/instrument"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/persist/fs/commitlog"
	"github.com/m3db/m3db/pool"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/repair"
	xio "github.com/m3db/m3db/x/io"
	xtime "github.com/m3db/m3x/time"
)

// Database is a time series database
type Database interface {
	// Options returns the database options
	Options() Options

	// Open will open the database for writing and reading
	Open() error

	// Close will close the database for writing and reading
	Close() error

	// Write value to the database for an ID
	Write(
		ctx context.Context,
		namespace string,
		id string,
		timestamp time.Time,
		value float64,
		unit xtime.Unit,
		annotation []byte,
	) error

	// ReadEncoded retrieves encoded segments for an ID
	ReadEncoded(
		ctx context.Context,
		namespace string,
		id string,
		start, end time.Time,
	) ([][]xio.SegmentReader, error)

	// FetchBlocks retrieves data blocks for a given id and a list of block start times.
	FetchBlocks(
		ctx context.Context,
		namespace string,
		shard uint32,
		id string,
		starts []time.Time,
	) ([]block.FetchBlockResult, error)

	// FetchBlocksMetadata retrieves blocks metadata for a given shard, returns the
	// fetched block metadata results, the next page token, and any error encountered.
	// If we have fetched all the block metadata, we return nil as the next page token.
	FetchBlocksMetadata(
		ctx context.Context,
		namespace string,
		shard uint32,
		limit int64,
		pageToken int64,
		includeSizes bool,
		includeChecksums bool,
	) ([]block.FetchBlocksMetadataResult, *int64, error)

	// Bootstrap bootstraps the database.
	Bootstrap() error

	// IsBootstrapped determines whether the database is bootstrapped.
	IsBootstrapped() bool

	// Truncate truncates data for the given namespace
	Truncate(namespace string) (int64, error)
}

type databaseNamespace interface {
	// Name returns the name of the namespace
	Name() string

	// NumSeries returns the number of series in the namespace
	NumSeries() int64

	// Tick performs any regular maintenance operations
	Tick(softDeadline time.Duration)

	// Write writes a data point
	Write(
		ctx context.Context,
		id string,
		timestamp time.Time,
		value float64,
		unit xtime.Unit,
		annotation []byte,
	) error

	// ReadEncoded reads data for given id within [start, end)
	ReadEncoded(
		ctx context.Context,
		id string,
		start, end time.Time,
	) ([][]xio.SegmentReader, error)

	// FetchBlocks retrieves data blocks for a given id and a list of block start times.
	FetchBlocks(
		ctx context.Context,
		shardID uint32,
		id string,
		starts []time.Time,
	) ([]block.FetchBlockResult, error)

	// FetchBlocksMetadata retrieves the blocks metadata.
	FetchBlocksMetadata(
		ctx context.Context,
		shardID uint32,
		limit int64,
		pageToken int64,
		includeSizes bool,
		includeChecksums bool,
	) ([]block.FetchBlocksMetadataResult, *int64, error)

	// Bootstrap performs bootstrapping
	Bootstrap(
		bs bootstrap.Bootstrap,
		targetRanges xtime.Ranges,
		writeStart time.Time,
	) error

	// Flush flushes in-memory data
	Flush(blockStart time.Time, pm persist.Manager) error

	// CleanupFileset cleans up fileset files
	CleanupFileset(earliestToRetain time.Time) error

	// Truncate truncates the in-memory data for this namespace
	Truncate() (int64, error)

	// Repair repairs the namespace data
	Repair(repairer databaseShardRepairer) error
}

type databaseShard interface {
	ID() uint32

	NumSeries() int64

	// Tick performs any updates to ensure series drain their buffers and blocks are flushed, etc
	Tick(softDeadline time.Duration)

	Write(
		ctx context.Context,
		id string,
		timestamp time.Time,
		value float64,
		unit xtime.Unit,
		annotation []byte,
	) error

	ReadEncoded(
		ctx context.Context,
		id string,
		start, end time.Time,
	) ([][]xio.SegmentReader, error)

	// FetchBlocks retrieves data blocks for a given id and a list of block start times.
	FetchBlocks(
		ctx context.Context,
		id string,
		starts []time.Time,
	) []block.FetchBlockResult

	// FetchBlocksMetadata retrieves the blocks metadata.
	FetchBlocksMetadata(
		ctx context.Context,
		limit int64,
		pageToken int64,
		includeSizes bool,
		includeChecksums bool,
	) ([]block.FetchBlocksMetadataResult, *int64)

	Bootstrap(
		bootstrappedSeries map[string]block.DatabaseSeriesBlocks,
		writeStart time.Time,
	) error

	// Flush flushes the series in this shard.
	Flush(
		namespace string,
		blockStart time.Time,
		pm persist.Manager,
	) error

	// CleanupFileset cleans up fileset files
	CleanupFileset(namespace string, earliestToRetain time.Time) error

	// Repair repairs the shard data
	Repair(namespace string, repairer databaseShardRepairer) (repair.MetadataComparisonResult, error)
}

type databaseSeries interface {
	ID() string

	// Tick performs any updates to ensure buffer drains, blocks are flushed, etc
	Tick() error

	Write(
		ctx context.Context,
		timestamp time.Time,
		value float64,
		unit xtime.Unit,
		annotation []byte,
	) error

	ReadEncoded(
		ctx context.Context,
		start, end time.Time,
	) ([][]xio.SegmentReader, error)

	// FetchBlocks retrieves data blocks given a list of block start times.
	FetchBlocks(ctx context.Context, starts []time.Time) []block.FetchBlockResult

	// FetchBlocksMetadata retrieves the blocks metadata.
	FetchBlocksMetadata(
		ctx context.Context,
		includeSizes bool,
		includeChecksums bool,
	) block.FetchBlocksMetadataResult

	// IsEmpty returns whether series is empty
	IsEmpty() bool

	// IsBootstrapped returns whether the series is bootstrapped or not
	IsBootstrapped() bool

	// Bootstrap merges the raw series bootstrapped along with the buffered data.
	Bootstrap(rs block.DatabaseSeriesBlocks) error

	// Flush flushes the data blocks of this series for a given start time.
	Flush(ctx context.Context, blockStart time.Time, persistFn persist.Fn) error
}

type databaseBuffer interface {
	Write(
		ctx context.Context,
		timestamp time.Time,
		value float64,
		unit xtime.Unit,
		annotation []byte,
	) error

	// ReadEncoded will return the full buffer's data as encoded segments
	// if start and end intersects the buffer at all, nil otherwise
	ReadEncoded(
		ctx context.Context,
		start, end time.Time,
	) [][]xio.SegmentReader

	// FetchBlocks retrieves data blocks given a list of block start times.
	FetchBlocks(ctx context.Context, starts []time.Time) []block.FetchBlockResult

	// FetchBlocksMetadata retrieves the blocks metadata.
	FetchBlocksMetadata(
		ctx context.Context,
		includeSizes bool,
		includeChecksums bool,
	) []block.FetchBlockMetadataResult

	IsEmpty() bool

	NeedsDrain() bool

	DrainAndReset(forced bool)
}

// databaseBootstrapManager manages the bootstrap process.
type databaseBootstrapManager interface {
	// IsBootstrapped returns whether the database is already bootstrapped.
	IsBootstrapped() bool

	// Bootstrap performs bootstrapping for all shards owned by db. It returns an error
	// if the server is currently being bootstrapped, and nil otherwise.
	Bootstrap() error
}

// databaseFlushManager manages flushing in-memory data to persistent storage.
type databaseFlushManager interface {
	// HasFlushed returns true if the data for a given time have been flushed.
	HasFlushed(t time.Time) bool

	// FlushTimeStart is the earliest flushable time.
	FlushTimeStart(t time.Time) time.Time

	// FlushTimeEnd is the latest flushable time.
	FlushTimeEnd(t time.Time) time.Time

	// Flush flushes in-memory data to persistent storage.
	Flush(t time.Time) error
}

// databaseCleanupManager manages cleaning up persistent storage space.
type databaseCleanupManager interface {
	// Cleanup cleans up data not needed in the persistent storage.
	Cleanup(t time.Time) error
}

// databaseFileSystemManager manages the database related filesystem activities.
type databaseFileSystemManager interface {
	databaseFlushManager
	databaseCleanupManager

	// ShouldRun determines if any file operations are needed for time t
	ShouldRun(t time.Time) bool

	// Run performs all filesystem-related operations
	Run(t time.Time, async bool)
}

// databaseShardRepairer repairs in-memory data for a shard
type databaseShardRepairer interface {
	// Options returns the repair options
	Options() repair.Options

	// Repair repairs the data for a given namespace and shard
	Repair(namespace string, shard databaseShard) (repair.MetadataComparisonResult, error)
}

// databaseRepairer repairs in-memory database data
type databaseRepairer interface {
	// Start starts the repair process
	Start()

	// Stop stops the repair process
	Stop()

	// Repair repairs in-memory data
	Repair() error
}

// NewBootstrapFn creates a new bootstrap
type NewBootstrapFn func() bootstrap.Bootstrap

// NewPersistManagerFn creates a new persist manager
type NewPersistManagerFn func() persist.Manager

// Options represents the options for storage
type Options interface {
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

	// SetRepairOptions sets the repair options
	SetRepairOptions(value repair.Options) Options

	// RepairOptions returns the repair options
	RepairOptions() repair.Options

	// SetEncodingM3TSZPooled sets m3tsz encoding with pooling
	SetEncodingM3TSZPooled() Options

	// SetEncodingM3TSZ sets m3tsz encoding
	SetEncodingM3TSZ() Options

	// SetNewEncoderFn sets the newEncoderFn
	SetNewEncoderFn(value encoding.NewEncoderFn) Options

	// NewEncoderFn returns the newEncoderFn
	NewEncoderFn() encoding.NewEncoderFn

	// SetNewDecoderFn sets the newDecoderFn
	SetNewDecoderFn(value encoding.NewDecoderFn) Options

	// NewDecoderFn returns the newDecoderFn
	NewDecoderFn() encoding.NewDecoderFn

	// SetNewBootstrapFn sets the newBootstrapFn
	SetNewBootstrapFn(value NewBootstrapFn) Options

	// NewBootstrapFn returns the newBootstrapFn
	NewBootstrapFn() NewBootstrapFn

	// SetNewPersistManagerFn sets the function for creating a new persistence manager
	SetNewPersistManagerFn(value NewPersistManagerFn) Options

	// NewPersistManagerFn returns the function for creating a new persistence manager
	NewPersistManagerFn() NewPersistManagerFn

	// SetMaxFlushRetries sets the maximum number of retries when data flushing fails
	SetMaxFlushRetries(value int) Options

	// MaxFlushRetries returns the maximum number of retries when data flushing fails
	MaxFlushRetries() int

	// SetContextPool sets the contextPool
	SetContextPool(value context.Pool) Options

	// ContextPool returns the contextPool
	ContextPool() context.Pool

	// SetBytesPool sets the bytesPool
	SetBytesPool(value pool.BytesPool) Options

	// BytesPool returns the bytesPool
	BytesPool() pool.BytesPool

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
}
