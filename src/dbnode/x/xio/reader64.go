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

package xio

import (
	"encoding/binary"
	"io"
)

// BytesReader64 implements a Reader64 over a slice of bytes.
type BytesReader64 struct {
	data  []byte
	index int
}

// NewBytesReader64 creates a new BytesReader64.
func NewBytesReader64(data []byte) *BytesReader64 {
	return &BytesReader64{data: data}
}

// Read64 reads and returns a 64 bit word plus a number of bytes (up to 8) actually read.
func (r *BytesReader64) Read64() (word uint64, n byte, err error) {
	if r.index+8 <= len(r.data) {
		// NB: this compiles to a single 64 bit load followed by
		// a BSWAPQ on amd64 gc 1.13 (https://godbolt.org/z/oTK1jx).
		res := binary.BigEndian.Uint64(r.data[r.index:])
		r.index += 8
		return res, 8, nil
	}
	if r.index >= len(r.data) {
		return 0, 0, io.EOF
	}
	var res uint64
	var bytes byte
	for ; r.index < len(r.data); r.index++ {
		res = (res << 8) | uint64(r.data[r.index])
		bytes++
	}
	return res << (64 - 8*bytes), bytes, nil
}

// Peek64 peeks and returns the next 64 bit word plus a number of bytes (up to 8) available.
func (r *BytesReader64) Peek64() (word uint64, n byte, err error) {
	if r.index+8 <= len(r.data) {
		// NB: this compiles to a single 64 bit load followed by
		// BSWAPQ on amd64 gc 1.13 (https://godbolt.org/z/oTK1jx).
		res := binary.BigEndian.Uint64(r.data[r.index:])
		return res, 8, nil
	}

	if r.index >= len(r.data) {
		return 0, 0, io.EOF
	}

	var res uint64
	var bytes byte
	for i := r.index; i < len(r.data); i++ {
		res = (res << 8) | uint64(r.data[i])
		bytes++
	}
	return res << (64 - 8*bytes), bytes, nil
}

// Reset resets the BytesReader64 for reuse.
func (r *BytesReader64) Reset(data []byte) {
	r.data = data
	r.index = 0
}
