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

package checked

import (
	"fmt"
	"sync/atomic"
	"unsafe"
)

// RefCount is an embeddable checked.Ref.
type RefCount struct {
	ref       int32
	reads     int32
	writes    int32
	finalizer unsafe.Pointer
}

// IncRef increments the reference count to this entity.
func (c *RefCount) IncRef() {
	n := atomic.AddInt32(&c.ref, 1)
	if traceback {
		tracebackEvent(c, int(n), incRefEvent)
	}
}

// DecRef decrements the reference count to this entity.
func (c *RefCount) DecRef() {
	n := atomic.AddInt32(&c.ref, -1)

	if traceback {
		tracebackEvent(c, int(n), decRefEvent)
	}

	if n < 0 {
		err := fmt.Errorf("negative ref count, ref=%d", n)
		panicRef(c, err)
	}
}

// MoveRef signals a move of the ref to this entity.
func (c *RefCount) MoveRef() {
	if traceback {
		tracebackEvent(c, c.NumRef(), moveRefEvent)
	}
}

// NumRef returns the reference count to this entity.
func (c *RefCount) NumRef() int {
	return int(atomic.LoadInt32(&c.ref))
}

// Finalize will call the finalizer if any, ref count must be zero.
func (c *RefCount) Finalize() {
	n := c.NumRef()

	if traceback {
		tracebackEvent(c, int(n), finalizeEvent)
	}

	if n != 0 {
		err := fmt.Errorf("finalize before zero ref count, ref=%d", n)
		panicRef(c, err)
	}

	finalizerPtr := (*Finalizer)(atomic.LoadPointer(&c.finalizer))
	if finalizerPtr != nil {
		finalizer := *finalizerPtr
		finalizer.Finalize()
	}
}

// Finalizer returns the finalizer if any or nil otherwise.
func (c *RefCount) Finalizer() Finalizer {
	finalizerPtr := (*Finalizer)(atomic.LoadPointer(&c.finalizer))
	if finalizerPtr == nil {
		return nil
	}
	return *finalizerPtr
}

// SetFinalizer sets the finalizer.
func (c *RefCount) SetFinalizer(f Finalizer) {
	atomic.StorePointer(&c.finalizer, unsafe.Pointer(&f))
}

// IncReads increments the reads count to this entity.
func (c *RefCount) IncReads() {
	if traceback {
		tracebackEvent(c, c.NumRef(), incReadsEvent)
	}

	n := atomic.AddInt32(&c.reads, 1)
	if ref := c.NumRef(); n > 0 && ref < 1 {
		err := fmt.Errorf("read after free: reads=%d, ref=%d", n, ref)
		panicRef(c, err)
	}
}

// DecReads decrements the reads count to this entity.
func (c *RefCount) DecReads() {
	if traceback {
		tracebackEvent(c, c.NumRef(), decReadsEvent)
	}

	n := atomic.AddInt32(&c.reads, -1)
	if ref := c.NumRef(); ref < 1 {
		err := fmt.Errorf("read finish after free: reads=%d, ref=%d", n, ref)
		panicRef(c, err)
	}
}

// NumReaders returns the active reads count to this entity.
func (c *RefCount) NumReaders() int {
	return int(atomic.LoadInt32(&c.reads))
}

// IncWrites increments the writes count to this entity.
func (c *RefCount) IncWrites() {
	if traceback {
		tracebackEvent(c, c.NumRef(), incWritesEvent)
	}

	n := atomic.AddInt32(&c.writes, 1)

	ref := c.NumRef()
	if n > 0 && ref < 1 {
		err := fmt.Errorf("write after free: writes=%d, ref=%d", n, ref)
		panicRef(c, err)
	}
	if n > 1 {
		err := fmt.Errorf("double write: writes=%d, ref=%d", n, ref)
		panicRef(c, err)
	}
}

// DecWrites decrements the writes count to this entity.
func (c *RefCount) DecWrites() {
	if traceback {
		tracebackEvent(c, c.NumRef(), decWritesEvent)
	}

	n := atomic.AddInt32(&c.writes, -1)
	if ref := c.NumRef(); ref < 1 {
		err := fmt.Errorf("write finish after free: writes=%d, ref=%d", n, ref)
		panicRef(c, err)
	}
}

// NumWriters returns the active writes count to this entity.
func (c *RefCount) NumWriters() int {
	return int(atomic.LoadInt32(&c.writes))
}
