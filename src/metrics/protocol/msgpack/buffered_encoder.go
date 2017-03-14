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

type bufferedEncoder struct {
	*msgpack.Encoder

	buf    *bytes.Buffer
	closed bool
	pool   BufferedEncoderPool
}

// NewBufferedEncoder creates a new buffered encoder
func NewBufferedEncoder() BufferedEncoder {
	return NewPooledBufferedEncoder(nil)
}

// NewPooledBufferedEncoder creates a new pooled buffered encoder
func NewPooledBufferedEncoder(p BufferedEncoderPool) BufferedEncoder {
	buf := bytes.NewBuffer(nil)
	return &bufferedEncoder{
		Encoder: msgpack.NewEncoder(buf),
		buf:     buf,
		closed:  false,
		pool:    p,
	}
}

func (enc *bufferedEncoder) Buffer() *bytes.Buffer { return enc.buf }

func (enc *bufferedEncoder) Bytes() []byte { return enc.buf.Bytes() }

func (enc *bufferedEncoder) Reset() {
	enc.closed = false
	enc.buf.Truncate(0)
}

func (enc *bufferedEncoder) Close() {
	if enc.closed {
		return
	}
	enc.closed = true
	if enc.pool != nil {
		enc.pool.Put(enc)
	}
}
