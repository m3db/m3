// Copyright (c) 2017 Uber Technologies, Inc.
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

package common

import (
	"sync/atomic"

	"github.com/m3db/m3metrics/encoding/msgpack"
)

// RefCountedBuffer is a refcounted buffer.
type RefCountedBuffer struct {
	n   int32
	buf msgpack.Buffer
}

// NewRefCountedBuffer creates a new refcounted buffer.
func NewRefCountedBuffer(buffer msgpack.Buffer) *RefCountedBuffer {
	return &RefCountedBuffer{n: 1, buf: buffer}
}

// Buffer returns the internal msgpack buffer.
func (b *RefCountedBuffer) Buffer() msgpack.Buffer { return b.buf }

// IncRef increments the refcount for the buffer.
func (b *RefCountedBuffer) IncRef() {
	if n := int(atomic.AddInt32(&b.n, 1)); n > 0 {
		return
	}
	panic("invalid ref count")
}

// DecRef decrements the refcount for the buffer.
func (b *RefCountedBuffer) DecRef() {
	if n := int(atomic.AddInt32(&b.n, -1)); n == 0 {
		if b.buf != nil {
			b.buf.Close()
		}
		return
	} else if n > 0 {
		return
	}
	panic("invalid ref count")
}
