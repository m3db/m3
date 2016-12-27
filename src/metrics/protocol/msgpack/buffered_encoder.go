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

package msgpack

import (
	"bytes"

	msgpack "gopkg.in/vmihailenco/msgpack.v2"
)

// newBufferedEncoder creates a new buffered encoder
func newBufferedEncoder() BufferedEncoder {
	return NewPooledBufferedEncoder(nil)
}

// NewPooledBufferedEncoder creates a new pooled buffered encoder
func NewPooledBufferedEncoder(p BufferedEncoderPool) BufferedEncoder {
	buffer := bytes.NewBuffer(nil)

	return BufferedEncoder{
		Encoder: msgpack.NewEncoder(buffer),
		Buffer:  buffer,
		pool:    p,
	}
}

// Bytes returns the buffer data
func (enc BufferedEncoder) Bytes() []byte { return enc.Buffer.Bytes() }

// Reset resets the buffered encoder
func (enc *BufferedEncoder) Reset() {
	enc.closed = false
	enc.Buffer.Truncate(0)
}

// Close returns the buffered encoder to the pool if possible
func (enc *BufferedEncoder) Close() {
	if enc.closed {
		return
	}
	enc.closed = true
	if enc.pool != nil {
		enc.pool.Put(*enc)
	}
}
