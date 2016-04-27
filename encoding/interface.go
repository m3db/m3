package encoding

import (
	"io"
	"time"
)

// Encoder is the generic interface for different types of encoders.
type Encoder interface {
	// Reset resets the start time of the encoder and the internal state.
	Reset(t time.Time)
	// Encode encodes datapoints into raw bytes
	Encode(dp Datapoint)
	// Bytes returns the bytes encoded so far.
	Bytes() []byte
}

// Iterator is the generic interface for iterating over encoded data.
type Iterator interface {
	// Next moves to the next item
	Next() bool
	// Value returns the value of the current datapoint
	Value() Datapoint
	// Err returns the error encountered
	Err() error
}

// Decoder is the generic interface for different types of decoders.
type Decoder interface {
	// Decode decodes the encoded data in the reader.
	Decode(r io.Reader) Iterator
}
