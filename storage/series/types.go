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

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/ts"
	xio "github.com/m3db/m3db/x/io"
	"github.com/m3db/m3x/instrument"
	xtime "github.com/m3db/m3x/time"
)

// DatabaseSeries is a series in the database
type DatabaseSeries interface {
	// ID returns the ID of the series
	ID() ts.ID

	// Tick executes any updates to ensure buffer drains, blocks are flushed, etc
	Tick() (TickResult, error)

	// Write writes a new value
	Write(
		ctx context.Context,
		timestamp time.Time,
		value float64,
		unit xtime.Unit,
		annotation []byte,
	) error

	// ReadEncoded reads encoded blocks
	ReadEncoded(
		ctx context.Context,
		start, end time.Time,
	) ([][]xio.SegmentReader, error)

	// FetchBlocks returns data blocks given a list of block start times
	FetchBlocks(ctx context.Context, starts []time.Time) []block.FetchBlockResult

	// FetchBlocksMetadata returns the blocks metadata
	FetchBlocksMetadata(
		ctx context.Context,
		start, end time.Time,
		includeSizes bool,
		includeChecksums bool,
	) block.FetchBlocksMetadataResult

	// IsEmpty returns whether series is empty
	IsEmpty() bool

	// IsBootstrapped returns whether the series is bootstrapped or not
	IsBootstrapped() bool

	// Bootstrap merges the raw series bootstrapped along with any buffered data
	Bootstrap(blocks block.DatabaseSeriesBlocks) error

	// Flush flushes the data blocks of this series for a given start time
	Flush(ctx context.Context, blockStart time.Time, persistFn persist.Fn) error

	// Close will close the series and if pooled returned to the pool
	Close()

	// Reset resets the series for reuse
	Reset(
		id ts.ID,
		bootstrapped bool,
		blockRetriever QueryableBlockRetriever,
	)
}

// QueryableBlockRetriever is a block retriever that can tell if a block
// is retrievable or not for a given start time.
type QueryableBlockRetriever interface {
	block.DatabaseShardBlockRetriever

	// IsBlockRetrievable returns whether a block is retrievable
	// for a given block start time
	IsBlockRetrievable(blockStart time.Time) bool
}

// TickResult is a set of results from a tick
type TickResult struct {
	ActiveBlocks           int
	ExpiredBlocks          int
	ResetRetrievableBlocks int
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

// Options represents the options for series
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
	SetIdentifierPool(value ts.IdentifierPool) Options

	// IdentifierPool returns the identifierPool
	IdentifierPool() ts.IdentifierPool
}
