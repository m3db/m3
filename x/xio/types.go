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

package xio

import (
	"io"
	"time"

	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/resource"
)

// Reader is an IO reader that can clone itself to reset the position
// in the stream so another consumer can can begin again from the start
type Reader interface {
	io.Reader

	// Clone returns a clone of the underlying data reset
	// to the start of the reader
	Clone() Reader
}

// BlockReader implements the io reader interface backed by a segment with start and end times
type BlockReader interface {
	SegmentReader

	Start() time.Time

	End() time.Time

	// ResetWindowed resets the reader to read a new segment, and updates block start and end times
	ResetWindowed(segment ts.Segment, start, end time.Time)

	// SegmentReader exposes inner segment reader
	SegmentReader() (SegmentReader, error)
}

// SegmentReader implements the io reader interface backed by a segment
type SegmentReader interface {
	Reader
	resource.Finalizer

	// Segment gets the segment read by this reader
	Segment() (ts.Segment, error)

	// Reset resets the reader to read a new segment
	Reset(segment ts.Segment)
}

// SegmentReaderPool provides a pool for segment readers
type SegmentReaderPool interface {
	// Init will initialize the pool
	Init()

	// Get provides a segment reader from the pool
	Get() SegmentReader

	// Put returns a segment reader to the pool
	Put(sr SegmentReader)
}

// ReaderSliceOfSlicesIterator is an iterator that iterates through an array of reader arrays
type ReaderSliceOfSlicesIterator interface {
	// Next moves to the next item
	Next() bool

	CurrentStart() time.Time

	CurrentEnd() time.Time

	// CurrentLen returns the current slice of readers
	CurrentLen() int

	// CurrentAt returns the current reader in the slice of readers at an index
	CurrentAt(idx int) Reader

	// Close closes the iterator
	Close()
}

// ReaderSliceOfSlicesFromBlockReadersIterator is an iterator that iterates through an array of reader arrays
type ReaderSliceOfSlicesFromBlockReadersIterator interface {
	ReaderSliceOfSlicesIterator

	// Reset resets the iterator with a new array of block readers arrays
	Reset(blocks [][]BlockReader)
}
