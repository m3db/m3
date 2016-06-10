package tsz

import (
	"io"

	"code.uber.internal/infra/memtsdb"
)

type decoder struct {
	opts Options
}

// NewDecoder creates a decoder.
func NewDecoder(opts Options) memtsdb.Decoder {
	if opts == nil {
		opts = NewOptions()
	}
	return &decoder{opts: opts}
}

// Decode decodes the encoded data captured by the reader.
func (dec *decoder) Decode(r io.Reader) memtsdb.Iterator {
	return NewIterator(r, dec.opts)
}
