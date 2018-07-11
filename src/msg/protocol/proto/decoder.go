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

	"github.com/m3db/m3x/pool"
)

type decoder struct {
	r              io.Reader
	buffer         []byte
	bytesPool      pool.BytesPool
	maxMessageSize int
}

// NewDecoder decodes a new decoder, the implementation is not thread safe.
func NewDecoder(r io.Reader, opts BaseOptions) Decoder {
	if opts == nil {
		opts = NewBaseOptions()
	}
	pool := opts.BytesPool()
	return &decoder{
		r:              r,
		buffer:         getByteSliceWithLength(sizeEncodingLength, pool),
		bytesPool:      pool,
		maxMessageSize: opts.MaxMessageSize(),
	}
}

func (d *decoder) Decode(m Unmarshaler) error {
	size, err := d.decodeSize()
	if err != nil {
		return err
	}
	d.buffer = growDataBufferIfNeeded(d.buffer, sizeEncodingLength+size, d.bytesPool)
	if size > d.maxMessageSize {
		return fmt.Errorf("decoded message size %d is larger than maximum supported size %d", size, d.maxMessageSize)
	}
	return d.decodeData(d.buffer[sizeEncodingLength:sizeEncodingLength+size], m)
}

func (d *decoder) decodeSize() (int, error) {
	if _, err := io.ReadFull(d.r, d.buffer[:sizeEncodingLength]); err != nil {
		return 0, err
	}
	size := sizeEncodeDecoder.Uint32(d.buffer[:sizeEncodingLength])
	return int(size), nil
}

func (d *decoder) decodeData(buffer []byte, m Unmarshaler) error {
	if _, err := io.ReadFull(d.r, buffer); err != nil {
		return err
	}
	return m.Unmarshal(buffer)
}

func (d *decoder) ResetReader(r io.Reader) {
	d.r = r
}
