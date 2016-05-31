package tsz

import (
	"io"
	"time"

	"code.uber.internal/infra/memtsdb/encoding"
)

type decoder struct {
	tu time.Duration // time unit
}

// NewDecoder creates a decoder.
func NewDecoder(timeUnit time.Duration) encoding.Decoder {
	return &decoder{timeUnit}
}

// Decode decodes the encoded data captured by the reader.
func (dec *decoder) Decode(r io.Reader) encoding.Iterator {
	return newIterator(r, dec.tu)
}
