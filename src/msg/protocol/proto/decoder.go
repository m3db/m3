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
	"fmt"
	"io"

	xio "github.com/m3db/m3/src/x/io"
	"github.com/m3db/m3/src/x/pool"
)

type decoder struct {
	r              io.Reader
	rOpts          xio.ResettableReaderOptions
	rr             xio.ResettableReader
	buffer         []byte
	bytesPool      pool.BytesPool
	maxMessageSize int
	opts           Options
}

// NewDecoder decodes a new decoder, the implementation is not thread safe.
func NewDecoder(r io.Reader, opts Options, bufferSize int) Decoder {
	if opts == nil {
		opts = NewOptions()
	}
	pool := opts.BytesPool()
	rOpts := xio.ResettableReaderOptions{ReadBufferSize: bufferSize}
	return &decoder{
		r:              r,
		rr:             opts.RWOptions().ResettableReaderFn()(r, rOpts),
		buffer:         getByteSliceWithLength(sizeEncodingLength, pool),
		bytesPool:      pool,
		maxMessageSize: opts.MaxMessageSize(),
		rOpts:          rOpts,
		opts:           opts,
	}
}

func (d *decoder) Decode(m Unmarshaler) error {
	size, err := d.decodeSize()
	if err != nil {
		return err
	}
	if size > d.maxMessageSize {
		return fmt.Errorf(
			"proto decoded message size %d is larger than maximum supported size %d",
			size, d.maxMessageSize)
	}
	d.buffer = growDataBufferIfNeeded(d.buffer, sizeEncodingLength+size, d.bytesPool)
	return d.decodeData(d.buffer[sizeEncodingLength:sizeEncodingLength+size], m)
}

func (d *decoder) decodeSize() (int, error) {
	if _, err := io.ReadFull(d.rr, d.buffer[:sizeEncodingLength]); err != nil {
		return 0, err
	}
	size := sizeEncodeDecoder.Uint32(d.buffer[:sizeEncodingLength])
	return int(size), nil
}

func (d *decoder) decodeData(buffer []byte, m Unmarshaler) error {
	_, err := io.ReadFull(d.rr, buffer)
	if err != nil {
		return err
	}
	return m.Unmarshal(buffer)
}

func (d *decoder) ResetReader(r io.Reader) {
	d.r = r
	d.rr = d.opts.RWOptions().ResettableReaderFn()(r, d.rOpts)
}
