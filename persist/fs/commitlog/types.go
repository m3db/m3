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

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/instrument"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/ts"
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
		series Series,
		datapoint ts.Datapoint,
		unit xtime.Unit,
		annotation ts.Annotation,
	) error

	// WriteBehind will write an entry in the commit log for a given series without waiting for completion
	WriteBehind(
		series Series,
		datapoint ts.Datapoint,
		unit xtime.Unit,
		annotation ts.Annotation,
	) error

	// Iter returns an iterator for accessing commit logs
	Iter() (Iterator, error)

	// Close the commit log
	Close() error
}

// Iterator provides an iterator for commit logs
type Iterator interface {
	// Next returns whether the iterator has the next value
	Next() bool

	// Current returns the current commit log entry
	Current() (Series, ts.Datapoint, xtime.Unit, ts.Annotation)

	// Err returns an error if an error occurred
	Err() error

	// Close the iterator
	Close()
}

// Series describes a series in the commit log
type Series struct {
	// UniqueIndex is the unique index assigned to this series
	UniqueIndex uint64

	// ID is the series identifier
	ID string

	// Namespace is the namespace the series belongs to
	Namespace string

	// Shard is the shard the series belongs to
	Shard uint32
}

// Options represents the options for the commit log
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

	// FilesystemOptions sets the filesystem options
	FilesystemOptions(value fs.Options) Options

	// GetFilesystemOptions returns the filesystem options
	GetFilesystemOptions() fs.Options

	// FlushSize sets the flush size
	FlushSize(value int) Options

	// GetFlushSize returns the flush size
	GetFlushSize() int

	// Strategy sets the strategy
	Strategy(value Strategy) Options

	// GetStrategy returns the strategy
	GetStrategy() Strategy

	// FlushInterval sets the flush interval
	FlushInterval(value time.Duration) Options

	// GetFlushInterval returns the flush interval
	GetFlushInterval() time.Duration

	// BacklogQueueSize sets the backlog queue size
	BacklogQueueSize(value int) Options

	// GetBacklogQueueSize returns the backlog queue size
	GetBacklogQueueSize() int
}
