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

	xtime "github.com/m3db/m3db/x/time"
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
	// Done will append any end of stream marker and ensure the encoder cannot be written to anymore.
	Done()
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

type SingleReaderIterator interface {
	Iterator

	// Reset resets the iterator to read from a new reader.
	Reset(reader io.Reader)
}

type MultiReaderIterator interface {
	Iterator

	// Reset resets the iterator to read from a slice of readers.
	Reset(readers []io.Reader)
}

// Decoder is the generic interface for different types of decoders.
type Decoder interface {
	// DecodeSingle decodes the encoded data in the reader.
	DecodeSingle(readers io.Reader) SingleReaderIterator

	// DecodeMulti decodes the encoded data in the readers.
	DecodeMulti(readers []io.Reader) MultiReaderIterator
}

// NewDecoderFn creates a new decoder
type NewDecoderFn func() Decoder
