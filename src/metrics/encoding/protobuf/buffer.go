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

package protobuf

import (
	"math"

	"github.com/m3db/m3/src/x/pool"
)

// PoolReleaseFn is a function used to release underlying slice back to bytes pool.
type PoolReleaseFn func([]byte)

// Buffer contains a byte slice backed by an optional bytes pool.
type Buffer struct {
	buf       []byte
	finalizer PoolReleaseFn
}

// NewBuffer create a new buffer.
func NewBuffer(buf []byte, p PoolReleaseFn) Buffer {
	return Buffer{buf: buf, finalizer: p}
}

// Bytes returns the raw byte slice.
func (b Buffer) Bytes() []byte { return b.buf }

// Truncate truncates the raw byte slice.
func (b *Buffer) Truncate(n int) { b.buf = b.buf[:n] }

// Close closes the buffer.
func (b *Buffer) Close() {
	if b.finalizer != nil && b.buf != nil {
		b.finalizer(b.buf)
	}
	b.buf = nil
	b.finalizer = nil
}

type copyDataMode int

const (
	dontCopyData copyDataMode = iota
	copyData
)

// ensureBufferSize returns a buffer with at least the specified target size.
// If the specified buffer has enough capacity, it is returned as is. Otherwise,
// a new buffer is allocated with at least the specified target size, and the
// specified buffer is returned to pool if possible.
func ensureBufferSize(
	buf []byte,
	p pool.BytesPool,
	targetSize int,
	copyDataMode copyDataMode,
) []byte {
	bufSize := len(buf)
	if bufSize >= targetSize {
		return buf
	}
	newSize := int(math.Max(float64(targetSize), float64(bufSize*2)))
	newBuf := allocate(p, newSize)
	if copyDataMode == copyData {
		copy(newBuf, buf)
	}
	if p != nil && buf != nil {
		p.Put(buf)
	}
	return newBuf
}

// allocate allocates a byte slice with at least the specified size.
func allocate(p pool.BytesPool, targetSize int) []byte {
	if p == nil {
		return make([]byte, targetSize)
	}
	b := p.Get(targetSize)
	return b[:cap(b)]
}
