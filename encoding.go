package memtsdb

import (
	"io"
	"time"

	xtime "code.uber.internal/infra/memtsdb/x/time"
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
	// Reset resets the iterator to read from a new reader.
	Reset(reader io.Reader)
	// Close closes the iterator and if pooled will return to the pool.
	Close()
}

// Decoder is the generic interface for different types of decoders.
type Decoder interface {
	// Decode decodes the encoded data in the reader.
	Decode(r io.Reader) Iterator
}

// NewDecoderFn creates a new decoder
type NewDecoderFn func() Decoder
