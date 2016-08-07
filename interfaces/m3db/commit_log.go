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
	"io"

	"github.com/m3db/m3x/time"
)

// CommitLogStrategy describes the comit log writing strategy
type CommitLogStrategy int

const (
	// CommitLogStrategyWriteWait describes the strategy that waits
	// for the buffered commit log chunk that contains a write to flush
	// before acknowledging a write
	CommitLogStrategyWriteWait CommitLogStrategy = iota

	// CommitLogStrategyWriteBehind describes the strategy that does not wait
	// for the buffered commit log chunk that contains a write to flush
	// before acknowledging a write
	CommitLogStrategyWriteBehind
)

// CommitLog provides a synchronized commit log
type CommitLog interface {
	io.Closer

	// Open the commit log
	Open() error

	// Write will write an entry in the commit log for a given series
	Write(
		series CommitLogSeries,
		datapoint Datapoint,
		unit xtime.Unit,
		annotation Annotation,
	) error

	// WriteBehind will write an entry in the commit log for a given series without waiting for completion
	WriteBehind(
		series CommitLogSeries,
		datapoint Datapoint,
		unit xtime.Unit,
		annotation Annotation,
	) error

	// Iter returns an iterator for accessing commit logs
	Iter() (CommitLogIterator, error)
}

// CommitLogIterator provides an iterator for commit logs
type CommitLogIterator interface {
	// Next returns whether the iterator has the next value
	Next() bool

	// Current returns the current commit log entry
	Current() (CommitLogSeries, Datapoint, xtime.Unit, Annotation)

	// Err returns an error if an error occurred
	Err() error

	// Close the iterator
	Close()
}

// CommitLogSeries describes a series in the commit log
type CommitLogSeries struct {
	// UniqueIndex is the unique index assigned to this series
	UniqueIndex uint64

	// ID is the series identifier
	ID string

	// Shard is the shard the series belongs to
	Shard uint32
}
