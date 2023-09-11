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

	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/ts/writes"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
	xtime "github.com/m3db/m3/src/x/time"
)

// Strategy describes the commit log writing strategy
type Strategy int

// FailureStrategy describes the commit log failure strategy
type FailureStrategy int

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

const (
	// FailureStrategyPanic describes the failure strategy that causes
	// the commit log to panic on I/O errors. This provides the only correct
	// behavior for M3DB nodes.
	FailureStrategyPanic FailureStrategy = iota

	// FailureStrategyCallback describes the failure strategy that calls the configured
	// FailureCallback. The return value of that callback determines whether to panic
	// or ignore the passed error.
	FailureStrategyCallback

	// FailureStrategyIgnore describes the failure strategy that causes the
	// commit log to swallow I/O errors.
	FailureStrategyIgnore
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
		writes writes.WriteBatch,
	) error

	// Close the commit log
	Close() error

	// ActiveLogs returns a slice of the active commitlogs.
	ActiveLogs() (persist.CommitLogFiles, error)

	// RotateLogs rotates the commitlog and returns the File that represents
	// the new commitlog file.
	RotateLogs() (persist.CommitLogFile, error)

	// QueueLength returns the number of writes that are currently in the commitlog
	// queue.
	QueueLength() int64
}

// LogEntry is a commit log entry being read.
type LogEntry struct {
	Series    ts.Series
	Datapoint ts.Datapoint
	Unit      xtime.Unit
	// Annotation gets invalidates on every read.
	Annotation ts.Annotation
	Metadata   LogEntryMetadata
}

// LogEntryMetadata is a set of metadata about a commit log entry being read.
type LogEntryMetadata struct {
	// FileReadID is a unique index for the current commit log
	// file that is being read (only unique per-process).
	FileReadID uint64
	// SeriesUniqueIndex is the series unique index relative to the
	// current commit log file being read.
	SeriesUniqueIndex uint64
}

// Iterator provides an iterator for commit logs.
type Iterator interface {
	// Next returns whether the iterator has the next value.
	Next() bool

	// Current returns the current commit log entry.
	Current() LogEntry

	// Err returns an error if an error occurred.
	Err() error

	// Close the iterator.
	Close()
}

// IteratorOpts is a struct that contains coptions for the Iterator.
type IteratorOpts struct {
	CommitLogOptions    Options
	FileFilterPredicate FileFilterPredicate
	// ReturnMetadataAsRef will return all series metadata such as ID,
	// tags and namespace as a reference instead of returning pooled
	// or allocated byte/string/ID references.
	// Useful if caller does not hold onto the result between calls to
	// the next read log entry and wants to avoid allocations and pool
	// contention.
	// Note: Series metadata will only be set on the result of a log
	// entry read if the series is read for the first time for the
	// combined tuple of FileReadID and SeriesUniqueIndex returned by
	// the LogEntryMetadata. EncodedTags will also be returned
	// instead of Tags on the series metadata.
	ReturnMetadataAsRef bool
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

	// SetFailureStrategy sets the strategy.
	SetFailureStrategy(value FailureStrategy) Options

	// FailureStrategy returns the strategy.
	FailureStrategy() FailureStrategy

	// SetFailureCallback sets the strategy.
	SetFailureCallback(value FailureCallback) Options

	// FailureCallback returns the strategy.
	FailureCallback() FailureCallback
}

// FileFilterInfo contains information about a commitog file that can be used to
// determine whether the iterator should filter it out or not.
type FileFilterInfo struct {
	// If isCorrupt is true then File will contain a valid CommitLogFile, otherwise
	// ErrorWithPath will contain an error and the path of the corrupt file.
	File      persist.CommitLogFile
	Err       ErrorWithPath
	IsCorrupt bool
}

// FileFilterPredicate is a predicate that allows the caller to determine
// which commitlogs the iterator should read from.
type FileFilterPredicate func(f FileFilterInfo) bool
