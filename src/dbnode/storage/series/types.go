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

package series

import (
	"time"

	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	xtime "github.com/m3db/m3x/time"

	"github.com/uber-go/tally"
)

// DatabaseSeries is a series in the database
type DatabaseSeries interface {
	block.OnRetrieveBlock
	block.OnEvictedFromWiredList

	// ID returns the ID of the series
	ID() ident.ID

	// Tags return the tags of the series
	Tags() ident.Tags

	// Tick executes async updates
	Tick() (TickResult, error)

	// Write writes a new value
	Write(
		ctx context.Context,
		timestamp time.Time,
		value float64,
		unit xtime.Unit,
		annotation []byte,
		wopts WriteOptions,
	) error

	// ReadEncoded reads encoded blocks
	ReadEncoded(
		ctx context.Context,
		start, end time.Time,
	) ([][]xio.BlockReader, error)

	// FetchBlocks returns data blocks given a list of block start times
	FetchBlocks(
		ctx context.Context,
		starts []time.Time,
	) ([]block.FetchBlockResult, error)

	// FetchBlocksMetadata returns the blocks metadata
	FetchBlocksMetadata(
		ctx context.Context,
		start, end time.Time,
		opts FetchBlocksMetadataOptions,
	) (block.FetchBlocksMetadataResult, error)

	// IsEmpty returns whether series is empty
	IsEmpty() bool

	// NumActiveBlocks returns the number of active blocks the series currently holds
	NumActiveBlocks() int

	// IsBootstrapped returns whether the series is bootstrapped or not
	IsBootstrapped() bool

	// Bootstrap merges the raw series bootstrapped along with any buffered data
	Bootstrap(blocks block.DatabaseSeriesBlocks) (BootstrapResult, error)

	// Flush flushes the data blocks of this series for a given start time
	Flush(
		ctx context.Context,
		blockStart time.Time,
		persistFn persist.DataFn,
		version int,
	) (FlushOutcome, error)

	// Snapshot snapshots the buffer buckets of this series for any data that has
	// not been rotated into a block yet
	Snapshot(
		ctx context.Context,
		blockStart time.Time,
		persistFn persist.DataFn,
	) error

	// Close will close the series and if pooled returned to the pool
	Close()

	// Reset resets the series for reuse
	Reset(
		id ident.ID,
		tags ident.Tags,
		blockRetriever QueryableBlockRetriever,
		onRetrieveBlock block.OnRetrieveBlock,
		onEvictedFromWiredList block.OnEvictedFromWiredList,
		opts Options,
	)
}

// FetchBlocksMetadataOptions encapsulates block fetch metadata options
// and specifies a few series specific options too.
type FetchBlocksMetadataOptions struct {
	block.FetchBlocksMetadataOptions

	// IncludeCachedBlocks specifies whether to also include cached blocks
	// when returning series metadata.
	IncludeCachedBlocks bool
}

// QueryableBlockRetriever is a block retriever that can tell if a block
// is retrievable or not for a given start time.
type QueryableBlockRetriever interface {
	block.DatabaseShardBlockRetriever

	// IsBlockRetrievable returns whether a block is retrievable
	// for a given block start time
	IsBlockRetrievable(blockStart time.Time) bool

	// RetrievableBlockVersion returns the last time a block was marked success
	RetrievableBlockVersion(blockStart time.Time) int
}

// TickStatus is the status of a series for a given tick
type TickStatus struct {
	// ActiveBlocks is the number of total active blocks
	ActiveBlocks int
	// WiredBlocks is the number of blocks wired in memory (all data kept)
	WiredBlocks int
	// UnwiredBlocks is the number of blocks unwired (data kept on disk)
	UnwiredBlocks int
	// PendingMergeBlocks is the number of blocks pending merges
	PendingMergeBlocks int
}

// TickResult is a set of results from a tick
type TickResult struct {
	TickStatus
	// MadeExpiredBlocks is count of blocks just expired
	MadeExpiredBlocks int
	// MadeUnwiredBlocks is count of blocks just unwired from memory
	MadeUnwiredBlocks int
	// MergedOutOfOrderBlocks is count of blocks merged from out of order streams
	MergedOutOfOrderBlocks int
}

// DatabaseSeriesAllocate allocates a database series for a pool
type DatabaseSeriesAllocate func() DatabaseSeries

// DatabaseSeriesPool provides a pool for database series
type DatabaseSeriesPool interface {
	// Get provides a database series from the pool
	Get() DatabaseSeries

	// Put returns a database series to the pool
	Put(block DatabaseSeries)
}

// FlushOutcome is an enum that provides more context about the outcome
// of series.Flush() to the caller.
type FlushOutcome int

const (
	// FlushOutcomeErr is just a default value that can be returned when we're also returning an error
	FlushOutcomeErr FlushOutcome = iota
	// FlushOutcomeBlockDoesNotExist indicates that the series did not have a block for the specified flush blockStart.
	FlushOutcomeBlockDoesNotExist
	// FlushOutcomeFlushedToDisk indicates that a block existed and was flushed to disk successfully.
	FlushOutcomeFlushedToDisk
)

// BootstrapResult contains information about the result of bootstrapping a series.
// It is returned from the series Bootstrap method primarily so the caller can aggregate
// and emit metrics instead of the series itself having to store additional fields (which
// would be costly because we have millions of them.)
type BootstrapResult struct {
	NumBlocksMovedToBuffer int64
	NumBlocksMerged        int64
}

// Options represents the options for series
type Options interface {
	// Validate validates the options
	Validate() error

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

	// SetCachePolicy sets the series cache policy
	SetCachePolicy(value CachePolicy) Options

	// CachePolicy returns the series cache policy
	CachePolicy() CachePolicy

	// SetContextPool sets the contextPool
	SetContextPool(value context.Pool) Options

	// ContextPool returns the contextPool
	ContextPool() context.Pool

	// SetEncoderPool sets the contextPool
	SetEncoderPool(value encoding.EncoderPool) Options

	// EncoderPool returns the contextPool
	EncoderPool() encoding.EncoderPool

	// SetMultiReaderIteratorPool sets the multiReaderIteratorPool
	SetMultiReaderIteratorPool(value encoding.MultiReaderIteratorPool) Options

	// MultiReaderIteratorPool returns the multiReaderIteratorPool
	MultiReaderIteratorPool() encoding.MultiReaderIteratorPool

	// SetFetchBlockMetadataResultsPool sets the fetchBlockMetadataResultsPool
	SetFetchBlockMetadataResultsPool(value block.FetchBlockMetadataResultsPool) Options

	// FetchBlockMetadataResultsPool returns the fetchBlockMetadataResultsPool
	FetchBlockMetadataResultsPool() block.FetchBlockMetadataResultsPool

	// SetIdentifierPool sets the identifierPool
	SetIdentifierPool(value ident.Pool) Options

	// IdentifierPool returns the identifierPool
	IdentifierPool() ident.Pool

	// SetStats sets the configured Stats.
	SetStats(value Stats) Options

	// Stats returns the configured Stats.
	Stats() Stats
}

// Stats is passed down from namespace/shard to avoid allocations per series.
type Stats struct {
	encoderCreated tally.Counter
}

// NewStats returns a new Stats for the provided scope.
func NewStats(scope tally.Scope) Stats {
	subScope := scope.SubScope("series")
	return Stats{
		encoderCreated: subScope.Counter("encoder-created"),
	}
}

// IncCreatedEncoders incs the EncoderCreated stat.
func (s Stats) IncCreatedEncoders() {
	s.encoderCreated.Inc(1)
}

// WriteType is an enum for warm/cold write types
type WriteType int

const (
	// WarmWrite represents warm writes (within the buffer past/future window)
	WarmWrite WriteType = iota

	// ColdWrite represents cold writes (outside the buffer past/future window)
	ColdWrite
)

// WriteOptions define different options for a write
type WriteOptions struct {
	WriteTime time.Time
}
