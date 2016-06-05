package tsz

import (
	"io"

	"code.uber.internal/infra/memtsdb/encoding"
)

type decoder struct {
	opts Options
}

// NewDecoder creates a decoder.
func NewDecoder(opts Options) encoding.Decoder {
	if opts == nil {
		opts = NewOptions()
	}
	return &decoder{opts: opts}
}

// Decode decodes the encoded data captured by the reader.
func (dec *decoder) Decode(r io.Reader) encoding.Iterator {
	return newIterator(r, dec.opts)
}
