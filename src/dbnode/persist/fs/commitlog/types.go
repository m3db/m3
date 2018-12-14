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

package commitlog

import (
	"time"

	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"
)

// Strategy describes the commit log writing strategy
type Strategy int

const (
	// StrategyWriteWait describes the strategy that waits
	// for the buffered commit log chunk that contains a write to flush
	// before acknowledging a write
	StrategyWriteWait Strategy = iota

	// StrategyWriteBehind describes the strategy that does not wait
	// for the buffered commit log chunk that contains a write to flush
	// before acknowledging a write
	StrategyWriteBehind
)

// CommitLog provides a synchronized commit log
type CommitLog interface {
	// Open the commit log
	Open() error

	// Write will write an entry in the commit log for a given series
	Write(
		ctx context.Context,
		series ts.Series,
		datapoint ts.Datapoint,
		unit xtime.Unit,
		annotation ts.Annotation,
	) error

	// WriteBatch is the same as Write, but in batch.
	WriteBatch(
		ctx context.Context,
		writes ts.WriteBatch,
	) error

	// Close the commit log
	Close() error

	// ActiveLogs returns a slice of the active commitlogs.
	ActiveLogs() ([]persist.CommitlogFile, error)

	// RotateLogs rotates the commitlog and returns the File that represents
	// the new commitlog file.
	RotateLogs() (persist.CommitlogFile, error)
}

// Iterator provides an iterator for commit logs
type Iterator interface {
	// Next returns whether the iterator has the next value
	Next() bool

	// Current returns the current commit log entry
	Current() (ts.Series, ts.Datapoint, xtime.Unit, ts.Annotation)

	// Err returns an error if an error occurred
	Err() error

	// Close the iterator
	Close()
}

// IteratorOpts is a struct that contains coptions for the Iterator
type IteratorOpts struct {
	CommitLogOptions      Options
	FileFilterPredicate   FileFilterPredicate
	SeriesFilterPredicate SeriesFilterPredicate
}

// Options represents the options for the commit log.
type Options interface {
	// Validate validates the Options.
	Validate() error

	// SetClockOptions sets the clock options.
	SetClockOptions(value clock.Options) Options

	// ClockOptions returns the clock options.
	ClockOptions() clock.Options

	// SetInstrumentOptions sets the instrumentation options.
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrumentation options,
	InstrumentOptions() instrument.Options

	// SetBlockSize sets the block size.
	SetBlockSize(value time.Duration) Options

	// BlockSize returns the block size.
	BlockSize() time.Duration

	// SetFilesystemOptions sets the filesystem options.
	SetFilesystemOptions(value fs.Options) Options

	// FilesystemOptions returns the filesystem options.
	FilesystemOptions() fs.Options

	// SetFlushSize sets the flush size.
	SetFlushSize(value int) Options

	// FlushSize returns the flush size.
	FlushSize() int

	// SetStrategy sets the strategy.
	SetStrategy(value Strategy) Options

	// Strategy returns the strategy.
	Strategy() Strategy

	// SetFlushInterval sets the flush interval.
	SetFlushInterval(value time.Duration) Options

	// FlushInterval returns the flush interval.
	FlushInterval() time.Duration

	// SetBacklogQueueSize sets the backlog queue size.
	SetBacklogQueueSize(value int) Options

	// BacklogQueueSize returns the backlog queue size.
	BacklogQueueSize() int

	// SetBacklogQueueChannelSize sets the size of the Golang channel
	// that backs the queue.
	SetBacklogQueueChannelSize(value int) Options

	// BacklogQueueChannelSize returns the size of the Golang channel
	// that backs the queue.
	BacklogQueueChannelSize() int

	// SetBytesPool sets the checked bytes pool.
	SetBytesPool(value pool.CheckedBytesPool) Options

	// BytesPool returns the checked bytes pool.
	BytesPool() pool.CheckedBytesPool

	// SetReadConcurrency sets the concurrency of the reader.
	SetReadConcurrency(concurrency int) Options

	// ReadConcurrency returns the concurrency of the reader.
	ReadConcurrency() int

	// SetIdentifierPool sets the IdentifierPool to use for pooling identifiers.
	SetIdentifierPool(value ident.Pool) Options

	// IdentifierPool returns the IdentifierPool to use for pooling identifiers.
	IdentifierPool() ident.Pool
}

// FileFilterPredicate is a predicate that allows the caller to determine
// which commitlogs the iterator should read from.
type FileFilterPredicate func(f persist.CommitlogFile) bool

// SeriesFilterPredicate is a predicate that determines whether datapoints for a given series
// should be returned from the Commit log reader. The predicate is pushed down to the
// reader level to prevent having to run the same function for every datapoint for a
// given series.
type SeriesFilterPredicate func(id ident.ID, namespace ident.ID) bool
