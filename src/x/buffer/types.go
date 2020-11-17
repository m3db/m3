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

// Package buffer provides highly-bound, high-concurrency,
// memory buffers. This differs from regular pooling principally in the very
// tight restrictions on how many bytes can be allocated.
package buffer

import (
	"time"
)

// Lease is a lease for a pre-allocated buffer backed by a
// fixed buffer manager.
type Lease interface {
	// Finalize releases the bytes returned with this lease.
	Finalize()
}

// FixedBufferManager yields pre-alloced buffers of fixed size.
type FixedBufferManager interface {
	// Copy copies the source bytes into a fixed buffer, returning a copy backed
	// by this buffer manager, and a finalizer for these bytes.
	Copy(src []byte) ([]byte, Lease, error)
	// MultiCopy copies each of the source byte slices into fixed buffers,
	// returning copies with each slice backed by this buffer manager,
	// and a finalizer for these bytes. These are allocated as a single block,
	// so it should be used when multiple buffers must be allocated as a single
	// operation to avoid deadlocking concerns.
	MultiCopy(buffer [][]byte, sources ...[]byte) ([][]byte, Lease, error)
}

// Options are options for cross block iteration.
type Options interface {
	// Validate validates the options.
	Validate() error

	// SetFixedBufferCount sets the fixed buffer count.
	SetFixedBufferCount(value int) Options

	// FixedBufferCount returns the fixed buffer count.
	FixedBufferCount() int

	// SetFixedBufferCapacity sets the fixed buffer capacity.
	SetFixedBufferCapacity(value int) Options

	// FixedBufferCapacity returns the fixed buffer capacity.
	FixedBufferCapacity() int

	// SetFixedBufferTimeout sets the fixed buffer timeout, 0 implies no timeout.
	SetFixedBufferTimeout(value time.Duration) Options

	// FixedBufferTimeout returns the fixed buffer timeout, 0 implies no timeout.
	FixedBufferTimeout() time.Duration
}
