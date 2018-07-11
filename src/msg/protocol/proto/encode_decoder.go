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
	Encoder
	Decoder

	isClosed bool
	pool     EncodeDecoderPool
}

// NewEncodeDecoder creates an EncodeDecoder, the implementation is not thread safe.
func NewEncodeDecoder(
	r io.Reader,
	opts EncodeDecoderOptions,
) EncodeDecoder {
	if opts == nil {
		opts = NewEncodeDecoderOptions()
	}
	return &encdec{
		Encoder:  NewEncoder(opts.EncoderOptions()),
		Decoder:  NewDecoder(r, opts.DecoderOptions()),
		isClosed: false,
		pool:     opts.EncodeDecoderPool(),
	}
}

func (c *encdec) Close() {
	if c.isClosed {
		return
	}
	c.isClosed = true
	if c.pool != nil {
		c.pool.Put(c)
	}
}

func (c *encdec) ResetReader(r io.Reader) {
	c.Decoder.ResetReader(r)
	c.isClosed = false
}
