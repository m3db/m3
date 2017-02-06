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

	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/ts"
)

// ReaderSliceReader implements the io reader interface backed by a slice of readers
type ReaderSliceReader interface {
	io.Reader

	Readers() []io.Reader
}

// SegmentReader implements the io reader interface backed by a segment
type SegmentReader interface {
	io.Reader
	context.Finalizer

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

	// CurrentLen returns the current slice of readers
	CurrentLen() int

	// CurrentAt returns the current reader in the slice of readers at an index
	CurrentAt(idx int) io.Reader

	// Close closes the iterator
	Close()
}

// ReaderSliceOfSlicesFromSegmentReadersIterator is an iterator that iterates through an array of reader arrays
type ReaderSliceOfSlicesFromSegmentReadersIterator interface {
	ReaderSliceOfSlicesIterator

	// Reset resets the iterator with a new array of segment readers arrays
	Reset(segments [][]SegmentReader)
}
