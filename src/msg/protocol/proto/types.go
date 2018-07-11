// Copyright (c) 2018 Uber Technologies, Inc.
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

package proto

import (
	"io"

	"github.com/m3db/m3x/pool"
)

// Marshaler can be marshaled.
type Marshaler interface {
	// Size returns the size of the marshaled bytes.
	Size() int

	// MarshalTo marshals the marshaler into the given byte slice.
	MarshalTo(data []byte) (int, error)
}

// Unmarshaler can be unmarshaled from bytes.
type Unmarshaler interface {
	// Unmarshal unmarshals the unmarshaler from the given byte slice.
	Unmarshal(data []byte) error
}

// Encoder encodes the marshaler.
type Encoder interface {
	// Encode encodes the marshaler.
	Encode(m Marshaler) error

	// Bytes returns the encoded bytes, the bytes could be reused by
	// the next encode call.
	Bytes() []byte
}

// Decoder decodes into an unmarshaler.
type Decoder interface {
	// Decode decodes the unmarshaler.
	Decode(m Unmarshaler) error

	// ResetReader resets the reader.
	ResetReader(r io.Reader)
}

// EncodeDecoder can encode and decode.
type EncodeDecoder interface {
	Encoder
	Decoder

	// Close closes the EncodeDecoder.
	Close()
}

// EncodeDecoderPool is a pool of EncodeDecoders.
type EncodeDecoderPool interface {
	// Init initializes the EncodeDecoder pool.
	Init(alloc EncodeDecoderAlloc)

	// Get returns an EncodeDecoder from the pool.
	Get() EncodeDecoder

	// Put puts an EncodeDecoder into the pool.
	Put(c EncodeDecoder)
}

// EncodeDecoderAlloc allocates an EncodeDecoder.
type EncodeDecoderAlloc func() EncodeDecoder

// BaseOptions configures a base encoder or decoder.
type BaseOptions interface {
	// MaxMessageSize returns the maximum message size.
	MaxMessageSize() int

	// SetMaxMessageSize sets the maximum message size.
	SetMaxMessageSize(value int) BaseOptions

	// BytesPool returns the bytes pool.
	BytesPool() pool.BytesPool

	// SetBytesPool sets the bytes pool.
	SetBytesPool(value pool.BytesPool) BaseOptions
}

// EncodeDecoderOptions configures an EncodeDecoder.
type EncodeDecoderOptions interface {
	// EncoderOptions returns the options for encoder.
	EncoderOptions() BaseOptions

	// SetEncoderOptions sets the options for encoder.
	SetEncoderOptions(value BaseOptions) EncodeDecoderOptions

	// DecoderOptions returns the options for decoder.
	DecoderOptions() BaseOptions

	// SetDecoderOptions sets the options for decoder.
	SetDecoderOptions(value BaseOptions) EncodeDecoderOptions

	// EncodeDecoderPool returns the pool for EncodeDecoder.
	EncodeDecoderPool() EncodeDecoderPool

	// SetEncodeDecoderPool sets the pool for EncodeDecoder.
	SetEncodeDecoderPool(pool EncodeDecoderPool) EncodeDecoderOptions
}
