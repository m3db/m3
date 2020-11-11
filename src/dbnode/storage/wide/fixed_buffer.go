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

package wide

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	errTimeout = errors.New("buffer manager timeout")
)

type fixedBufferManager struct {
	mu sync.Mutex

	capacity int
	timeout  time.Duration
	pool     *fixedBufferPool
	curr     *fixedBuffer
}

func newFixedBufferManager(
	opts Options,
) FixedBufferManager {
	return &fixedBufferManager{
		capacity: opts.FixedBufferCapacity(),
		timeout:  opts.FixedBufferTimeout(),
		pool:     newFixedBufferPool(opts),
	}
}

func (m *fixedBufferManager) Copy(src []byte) ([]byte, BorrowedBuffer, error) {
	size := len(src)
	if size > m.capacity {
		// NB: if the given data overflows the buffer size, allocate instead.
		return append(make([]byte, 0, size), src...), fixedBufferBorrowNoopGlobal, nil
	}

	// All copies mutate.
	m.mu.Lock()
	defer m.mu.Unlock()

	var ok bool
	if m.curr == nil {
		m.curr, ok = m.pool.getWithTimeout(m.timeout)
		if !ok {
			return nil, fixedBufferBorrowNoopGlobal, errTimeout
		}
	}

	dst, borrow, ok := m.curr.copy(src)
	if !ok {
		// Rotate in next buffer.
		m.curr.Finalize()
		m.curr, ok = m.pool.getWithTimeout(m.timeout)
		if !ok {
			return nil, fixedBufferBorrowNoopGlobal, errTimeout
		}

		// Perform copy again, fresh fixed buffer so must fit.
		dst, borrow, ok = m.curr.copy(src)
		if !ok {
			panic(fmt.Errorf("below max size and unable to borrow: size=%d", size))
		}
	}

	return dst, borrow, nil
}

type fixedBufferPool struct {
	timeout time.Duration
	buffers chan *fixedBuffer
}

func newFixedBufferPool(opts Options) *fixedBufferPool {
	count, capacity := opts.FixedBufferCount(), opts.FixedBufferCapacity()
	slab := make([]byte, count*capacity)
	buffers := make(chan *fixedBuffer, count)
	p := &fixedBufferPool{
		buffers: buffers,
	}

	for i := 0; i < count; i++ {
		buffers <- newFixedBuffer(p, slab[:capacity])
		slab = slab[capacity:]
	}

	return p
}

func (p *fixedBufferPool) getWithTimeout(timeout time.Duration) (*fixedBuffer, bool) {
	if timeout < 1 {
		// To ensure this is a fixed size buffer pool, always wait for returns
		// to the pool and so blockingly wait for consumer of the buffers
		// to return their buffers.
		return <-p.buffers, true
	}

	// Attempt to get a buffer if available, before waiting on timeout.
	select {
	case buffer := <-p.buffers:
		return buffer, true
	default:
	}

	ticker := time.NewTicker(timeout)
	defer ticker.Stop()

	select {
	case buffer := <-p.buffers:
		return buffer, true
	case <-ticker.C:
		return nil, false
	}
}

func (p *fixedBufferPool) put(b *fixedBuffer) {
	b.reset()
	select {
	case p.buffers <- b:
	default:
		// This is a code bug if actually cannot return, however do not
		// panic to be defensive.
	}
}

type fixedBuffer struct {
	pool            *fixedBufferPool
	ref             int64
	underlyingSlice []byte
	unconsumed      []byte
}

func newFixedBuffer(pool *fixedBufferPool, underlyingSlice []byte) *fixedBuffer {
	b := &fixedBuffer{
		pool:            pool,
		underlyingSlice: underlyingSlice,
	}

	b.reset()
	return b
}

func (b *fixedBuffer) reset() {
	if n := atomic.LoadInt64(&b.ref); n != 0 {
		panic(fmt.Errorf("bad ref count on reset: %d", n))
	}
	b.unconsumed = b.underlyingSlice
	atomic.StoreInt64(&b.ref, 1)
}

func (b *fixedBuffer) copy(src []byte) ([]byte, BorrowedBuffer, bool) {
	size := len(src)
	if len(b.unconsumed) < size {
		return nil, nil, false
	}

	copy(b.unconsumed, src)
	dst := b.unconsumed[:size]
	b.unconsumed = b.unconsumed[size:]

	b.incRef()
	return dst, b, true
}

func (b *fixedBuffer) Finalize() {
	b.decRef()
}

func (b *fixedBuffer) incRef() {
	atomic.AddInt64(&b.ref, 1)
}

func (b *fixedBuffer) decRef() {
	n := atomic.AddInt64(&b.ref, -1)
	if n == 0 {
		b.pool.put(b)
	} else if n < 0 {
		panic(fmt.Errorf("bad ref count: %d", n))
	}
}

var fixedBufferBorrowNoopGlobal BorrowedBuffer = (*fixedBufferBorrowNoop)(nil)

type fixedBufferBorrowNoop struct {
}

func (b *fixedBufferBorrowNoop) Finalize() {
	// noop
}
