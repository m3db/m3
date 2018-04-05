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
)

type encdec struct {
	rw       io.ReadWriteCloser
	enc      *encoder
	dec      *decoder
	isClosed bool
	pool     EncodeDecoderPool
}

// NewEncodeDecoder creates an EncodeDecoder, the implementation is not thread safe.
func NewEncodeDecoder(
	rw io.ReadWriteCloser,
	opts EncodeDecoderOptions,
) EncodeDecoder {
	if opts == nil {
		opts = NewEncodeDecoderOptions()
	}
	return &encdec{
		rw:       rw,
		enc:      newEncoder(rw, opts.EncoderOptions()),
		dec:      newDecoder(rw, opts.DecoderOptions()),
		isClosed: false,
		pool:     opts.EncodeDecoderPool(),
	}
}

func (c *encdec) Encode(msg Marshaler) error {
	return c.enc.Encode(msg)
}

func (c *encdec) Decode(acks Unmarshaler) error {
	return c.dec.Decode(acks)
}

func (c *encdec) Close() {
	if c.isClosed {
		return
	}
	c.isClosed = true
	if c.rw != nil {
		c.rw.Close()
	}
	if c.pool != nil {
		c.pool.Put(c)
	}
}

func (c *encdec) Reset(rw io.ReadWriteCloser) {
	if c.rw != nil {
		c.rw.Close()
	}
	c.enc.resetWriter(rw)
	c.dec.resetReader(rw)
	c.rw = rw
	c.isClosed = false
}
