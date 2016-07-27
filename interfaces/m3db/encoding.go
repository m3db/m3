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
	"time"

	"github.com/m3db/m3x/time"
)

// A Datapoint is a single data value reported at a given time
type Datapoint struct {
	Timestamp time.Time
	Value     float64
}

// Annotation represents information used to annotate datapoints.
type Annotation []byte

// Encoder is the generic interface for different types of encoders.
type Encoder interface {
	// Encode encodes a datapoint and optionally an annotation.
	Encode(dp Datapoint, timeUnit xtime.Unit, annotation Annotation) error

	// Stream is the streaming interface for reading encoded bytes in the encoder.
	Stream() SegmentReader

	// Seal will append any end of stream marker and ensure the encoder cannot be written to anymore.
	Seal()

	// Reset resets the start time of the encoder and the internal state.
	Reset(t time.Time, capacity int)

	// Reset resets the start time of the encoder and the internal state with some preset data.
	ResetSetData(t time.Time, data []byte, writable bool)

	// Close closes the encoder and if pooled will return to the pool.
	Close()
}

// NewEncoderFn creates a new encoder
type NewEncoderFn func(start time.Time, bytes []byte) Encoder

// Iterator is the generic interface for iterating over encoded data.
type Iterator interface {
	// Next moves to the next item
	Next() bool

	// Current returns the value as well as the annotation associated with the current datapoint.
	// Users should not hold on to the returned Annotation object as it may get invalidated when
	// the iterator calls Next().
	Current() (Datapoint, xtime.Unit, Annotation)

	// Err returns the error encountered
	Err() error

	// Close closes the iterator and if pooled will return to the pool.
	Close()
}

// ReaderIterator is the interface for a single-reader iterator.
type ReaderIterator interface {
	Iterator

	// Reset resets the iterator to read from a new reader.
	Reset(reader io.Reader)
}

// MultiReaderIterator is an iterator that iterates in order over a list of sets of
// internally ordered but not collectively in order readers, it also deduplicates datapoints.
type MultiReaderIterator interface {
	Iterator

	// Reset resets the iterator to read from a slice of readers.
	Reset(readers []io.Reader)

	// Reset resets the iterator to read from a slice of slice readers.
	ResetSliceOfSlices(readers ReaderSliceOfSlicesIterator)
}

// ReaderSliceOfSlicesIterator is an iterator that iterates through an array of reader arrays
type ReaderSliceOfSlicesIterator interface {
	// Next moves to the next item
	Next() bool

	// CurrentLen returns the current slice of readers
	CurrentLen() int

	// CurrentAt returns the current reader in the slice of readers at an index
	CurrentAt(idx int) io.Reader

	// Close closes the iterator and if pooled will return to the pool
	Close()
}

// ReaderSliceOfSlicesFromSegmentReadersIterator is an iterator that iterates through an array of reader arrays
type ReaderSliceOfSlicesFromSegmentReadersIterator interface {
	ReaderSliceOfSlicesIterator

	// Reset resets the iterator with a new array of segment readers arrays
	Reset(segments [][]SegmentReader)
}

// SeriesIterator is an iterator that iterates over a set of iterators from different replicas
// and de-dupes & merges results from the replicas for a given series while also applying a time
// filter on top of the values in case replicas returned values out of range on either end
type SeriesIterator interface {
	Iterator

	// ID gets the ID of the series
	ID() string

	// Start returns the start time filter specified for the iterator
	Start() time.Time

	// End returns the end time filter specified for the iterator
	End() time.Time

	// Reset resets the iterator to read from a set of iterators from different replicas, one
	// must note that this can be an array with nil entries if some replicas did not return successfully
	Reset(id string, startInclusive, endExclusive time.Time, replicas []Iterator)
}

// SeriesIterators is a collection of SeriesIterator that can close all iterators
type SeriesIterators interface {
	// Iters returns the array of series iterators
	Iters() []SeriesIterator

	// Len returns the length of the iters
	Len() int

	// Close closes all iterators contained
	Close()
}

// MutableSeriesIterators is a mutable SeriesIterators
type MutableSeriesIterators interface {
	SeriesIterators

	// Reset the iters collection to a size for reuse
	Reset(size int)

	// Cap returns the capacity of the iters
	Cap() int

	// SetAt an index a SeriesIterator
	SetAt(idx int, iter SeriesIterator)
}

// Decoder is the generic interface for different types of decoders.
type Decoder interface {
	// Decode decodes the encoded data in the reader.
	Decode(reader io.Reader) ReaderIterator
}

// NewDecoderFn creates a new decoder
type NewDecoderFn func() Decoder
