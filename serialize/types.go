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

package serialize

import (
	"math"

	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/ident"
)

var (
	// MaxNumberTags is the maximum number of tags that can be encoded.
	MaxNumberTags = math.MaxUint16

	// MaxTagLiteralLength is the maximum length of a tag Name/Value.
	MaxTagLiteralLength uint16 = 4096

	// HeaderMagicNumber is an internal header used to denote the beginning of
	// an encoded stream.
	HeaderMagicNumber uint16 = 10101
)

// Encoder encodes provided Tag iterators.
type Encoder interface {
	// Encode encodes the provided iterator into it's internal byte stream.
	// NB: leaves the original iterator un-modified.
	Encode(ident.TagIterator) error

	// Data returns the encoded bytes.
	Data() (checked.Bytes, bool)

	// Reset resets the internal state to allow reuse of the encoder.
	Reset()

	// Finalize releases any held resources.
	Finalize()
}

// EncoderPool pools Encoders.
type EncoderPool interface {
	// Init initializes the pool.
	Init()

	// Get returns an encoder. NB: calling Finalize() on the
	// returned Encoder puts it back in the pool.
	Get() Encoder

	// Put puts the encoder back in the pool.
	Put(Encoder)
}

// Decoder decodes an encoded byte stream to a TagIterator.
type Decoder interface {
	ident.TagIterator

	// Reset resets internal state to iterate over the provided bytes.
	Reset(checked.Bytes)

	// Finalize releases any held resources.
	Finalize()
}

// DecoderPool pools Decoders.
type DecoderPool interface {
	// Init initializes the pool.
	Init()

	// Get returns a decoder. NB: calling Finalize() on the
	// returned Decoder puts it back in the pool.
	Get() Decoder

	// Put puts the decoder back in the pool.
	Put(Decoder)
}
