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
	xio "github.com/m3db/m3db/x/io"
	xtime "github.com/m3db/m3x/time"
)

// FetchBlockResult captures the block start time, the readers for the underlying streams, and any errors encountered.
type FetchBlockResult interface {
	// Start returns the start time of an encoded block
	Start() time.Time

	// Readers returns the readers for the underlying streams.
	Readers() []xio.SegmentReader

	// Err returns the error encountered when fetching the block.
	Err() error
}

// FetchBlocksMetadataResult captures the fetch results for multiple database blocks.
type FetchBlocksMetadataResult interface {
	// ID returns id of the series containing the blocks
	ID() string

	// Blocks returns the metadata of series blocks
	Blocks() []FetchBlockMetadataResult
}

// FetchBlockMetadataResult captures the block start time, the block size, and any errors encountered
type FetchBlockMetadataResult interface {
	// Start return the start time of a database block
	Start() time.Time

	// Size returns the size of the block, or nil if not available.
	Size() *int64

	// Err returns the error encountered if any
	Err() error
}

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
		id string,
		timestamp time.Time,
		value float64,
		unit xtime.Unit,
		annotation []byte,
	) error

	// ReadEncoded retrieves encoded segments for an ID
	ReadEncoded(
		ctx context.Context,
		id string,
		start, end time.Time,
	) ([][]xio.SegmentReader, error)

	// FetchBlocks retrieves data blocks for a given id and a list of block start times.
	FetchBlocks(
		ctx context.Context,
		shard uint32,
		id string,
		starts []time.Time,
	) ([]FetchBlockResult, error)

	// FetchBlocksMetadata retrieves blocks metadata for a given shard, returns the
	// fetched block metadata results, the next page token, and any error encountered.
	// If we have fetched all the block metadata, we return nil as the next page token.
	FetchBlocksMetadata(
		ctx context.Context,
		shardID uint32,
		limit int64,
		pageToken int64,
		includeSizes bool,
	) ([]FetchBlocksMetadataResult, *int64, error)

	// Bootstrap bootstraps the database.
	Bootstrap() error

	// IsBootstrapped determines whether the database is bootstrapped.
	IsBootstrapped() bool
}

type databaseShard interface {
	ShardNum() uint32

	// Tick performs any updates to ensure series drain their buffers and blocks are flushed, etc
	Tick()

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
	) []FetchBlockResult

	// FetchBlocksMetadata retrieves the blocks metadata.
	FetchBlocksMetadata(
		ctx context.Context,
		limit int64,
		pageToken int64,
		includeSizes bool,
	) ([]FetchBlocksMetadataResult, *int64)

	Bootstrap(bs bootstrap.Bootstrap, writeStart time.Time, cutover time.Time) error

	// Flush flushes the series in this shard.
	Flush(ctx context.Context, blockStart time.Time, pm persist.Manager) error
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
	FetchBlocks(ctx context.Context, starts []time.Time) []FetchBlockResult

	// FetchBlocksMetadata retrieves the blocks metadata.
	FetchBlocksMetadata(ctx context.Context, includeSizes bool) FetchBlocksMetadataResult

	Empty() bool

	// Bootstrap merges the raw series bootstrapped along with the buffered data.
	Bootstrap(rs block.DatabaseSeriesBlocks, cutover time.Time) error

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
	FetchBlocks(ctx context.Context, starts []time.Time) []FetchBlockResult

	// FetchBlocksMetadata retrieves the blocks metadata.
	FetchBlocksMetadata(ctx context.Context, includeSizes bool) []FetchBlockMetadataResult

	Empty() bool

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

// databaseFlushManager manages the data flushing process.
type databaseFlushManager interface {
	// NeedsFlush determines whether we need to flush in-memory data blocks given a timestamp.
	NeedsFlush(t time.Time) bool

	// Flush flushes the in-memory data blocks.
	Flush(t time.Time, async bool)
}

// NewBootstrapFn creates a new bootstrap
type NewBootstrapFn func() bootstrap.Bootstrap

// NewPersistManagerFn creates a new persist manager
type NewPersistManagerFn func() persist.Manager

// Options represents the options for storage
type Options interface {
	// ClockOptions sets the clock options
	ClockOptions(value clock.Options) Options

	// GetClockOptions returns the clock options
	GetClockOptions() clock.Options

	// InstrumentOptions sets the instrumentation options
	InstrumentOptions(value instrument.Options) Options

	// GetInstrumentOptions returns the instrumentation options
	GetInstrumentOptions() instrument.Options

	// RetentionOptions sets the retention options
	RetentionOptions(value retention.Options) Options

	// GetRetentionOptions returns the retention options
	GetRetentionOptions() retention.Options

	// DatabaseBlockOptions sets the database block options
	DatabaseBlockOptions(value block.Options) Options

	// GetDatabaseBlockOptions returns the database block options
	GetDatabaseBlockOptions() block.Options

	// CommitLogOptions sets the commit log options
	CommitLogOptions(value commitlog.Options) Options

	// GetCommitLogOptions returns the commit log options
	GetCommitLogOptions() commitlog.Options

	// EncodingM3TSZPooled sets m3tsz encoding with pooling
	EncodingM3TSZPooled() Options

	// EncodingM3TSZ sets m3tsz encoding
	EncodingM3TSZ() Options

	// NewEncoderFn sets the newEncoderFn
	NewEncoderFn(value encoding.NewEncoderFn) Options

	// GetNewEncoderFn returns the newEncoderFn
	GetNewEncoderFn() encoding.NewEncoderFn

	// NewDecoderFn sets the newDecoderFn
	NewDecoderFn(value encoding.NewDecoderFn) Options

	// GetNewDecoderFn returns the newDecoderFn
	GetNewDecoderFn() encoding.NewDecoderFn

	// NewBootstrapFn sets the newBootstrapFn
	NewBootstrapFn(value NewBootstrapFn) Options

	// GetNewBootstrapFn returns the newBootstrapFn
	GetNewBootstrapFn() NewBootstrapFn

	// NewPersistManagerFn sets the function for creating a new persistence manager
	NewPersistManagerFn(value NewPersistManagerFn) Options

	// GetNewPersistManagerFn returns the function for creating a new persistence manager
	GetNewPersistManagerFn() NewPersistManagerFn

	// MaxFlushRetries sets the maximum number of retries when data flushing fails
	MaxFlushRetries(value int) Options

	// GetMaxFlushRetries returns the maximum number of retries when data flushing fails
	GetMaxFlushRetries() int

	// ContextPool sets the contextPool
	ContextPool(value context.Pool) Options

	// GetContextPool returns the contextPool
	GetContextPool() context.Pool

	// BytesPool sets the bytesPool
	BytesPool(value pool.BytesPool) Options

	// GetBytesPool returns the bytesPool
	GetBytesPool() pool.BytesPool

	// EncoderPool sets the contextPool
	EncoderPool(value encoding.EncoderPool) Options

	// GetEncoderPool returns the contextPool
	GetEncoderPool() encoding.EncoderPool

	// SegmentReaderPool sets the contextPool
	SegmentReaderPool(value xio.SegmentReaderPool) Options

	// GetSegmentReaderPool returns the contextPool
	GetSegmentReaderPool() xio.SegmentReaderPool

	// ReaderIteratorPool sets the readerIteratorPool
	ReaderIteratorPool(value encoding.ReaderIteratorPool) Options

	// GetReaderIteratorPool returns the readerIteratorPool
	GetReaderIteratorPool() encoding.ReaderIteratorPool

	// MultiReaderIteratorPool sets the multiReaderIteratorPool
	MultiReaderIteratorPool(value encoding.MultiReaderIteratorPool) Options

	// GetMultiReaderIteratorPool returns the multiReaderIteratorPool
	GetMultiReaderIteratorPool() encoding.MultiReaderIteratorPool
}
