// Copyright (c) 2020 Uber Technologies, Inc.
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

package msgpack

import (
	"errors"
	"hash"
	"hash/adler32"
)

var (
	// errChecksumMismatch returned when the calculated checksum doesn't match the stored checksum
	errChecksumMismatch = errors.New("calculated checksum doesn't match stored checksum")
)

var _ DecoderStream = &decoderStreamWithDigest{}

// decoderStreamWithDigest calculates the digest as it processes a decoder stream.
type decoderStreamWithDigest struct {
	reader        DecoderStream
	readerDigest  hash.Hash32
	unreadByte    bool
	enabled       bool
	singleByteBuf []byte
}

// newDecoderStreamWithDigest returns a new decoderStreamWithDigest
func newDecoderStreamWithDigest(reader DecoderStream) *decoderStreamWithDigest {
	return &decoderStreamWithDigest{
		reader:        reader,
		readerDigest:  adler32.New(),
		singleByteBuf: make([]byte, 1),
	}
}

func (d *decoderStreamWithDigest) Read(p []byte) (n int, err error) {
	n, err = d.reader.Read(p)
	if err != nil {
		return n, err
	}
	if n <= 0 {
		return n, nil
	}

	start := 0
	if d.unreadByte {
		d.unreadByte = false
		start++
	}
	if d.enabled {
		if _, err := d.readerDigest.Write(p[start:n]); err != nil {
			return 0, err
		}
	}
	return n, err
}

func (d *decoderStreamWithDigest) ReadByte() (byte, error) {
	b, err := d.reader.ReadByte()
	if err != nil {
		return 0, err
	}

	if d.unreadByte {
		d.unreadByte = false
	} else if d.enabled {
		d.singleByteBuf[0] = b
		if _, err := d.readerDigest.Write(d.singleByteBuf); err != nil {
			return b, err
		}
	}
	return b, err
}

func (d *decoderStreamWithDigest) UnreadByte() error {
	err := d.reader.UnreadByte()
	if err == nil {
		d.unreadByte = true
	}
	return err
}

// reset resets the reader for use with a new reader.
func (d *decoderStreamWithDigest) reset(stream DecoderStream) {
	d.reader = stream
	d.readerDigest.Reset()
}

// digest returns the digest
func (d *decoderStreamWithDigest) digest() hash.Hash32 {
	return d.readerDigest
}

// validate compares the current digest against the expected digest and returns
// an error if they don't match.
func (d *decoderStreamWithDigest) validate(expectedDigest uint32) error {
	if d.readerDigest.Sum32() != expectedDigest {
		return errChecksumMismatch
	}
	return nil
}

// capture provides a mechanism for manually capturing bytes to add to digest when reader is manipulated
// through atypical means (e.g. reading directly from the backing byte slice of a ByteReader)
func (d *decoderStreamWithDigest) capture(bytes []byte) error {
	// No-op if not actually capturing at the moment
	if d.enabled {
		if _, err := d.readerDigest.Write(bytes); err != nil {
			return err
		}
	}
	return nil
}

// setDigestReaderEnabled enables calculating of digest for bytes read. If this is false, behaves like a regular reader.
func (d *decoderStreamWithDigest) setDigestReaderEnabled(enabled bool) {
	if !d.enabled && enabled {
		d.enabled = true
		d.readerDigest.Reset()
	} else if d.enabled && !enabled {
		d.enabled = false
	}
}

// Returns the decoder stream wrapped by this object
func (d *decoderStreamWithDigest) wrappedStream() DecoderStream {
	return d.reader
}
