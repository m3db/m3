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

package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
)

const (
	// 4mb total for fixed buffer.
	fixedBufferPoolSize = 64
	fixedBufferCapacity = 65536
)

type fixedBufferPool struct {
	buffers chan *fixedBuffer
}

func newFixedBufferPool() *fixedBufferPool {
	slab := make([]byte, fixedBufferPoolSize*fixedBufferCapacity)
	buffers := make(chan *fixedBuffer, fixedBufferPoolSize)
	p := &fixedBufferPool{
		buffers: buffers,
	}

	for i := 0; i < fixedBufferPoolSize; i++ {
		buffers <- newFixedBuffer(p, slab[:fixedBufferCapacity])
		slab = slab[fixedBufferCapacity:]
	}

	return p
}

func (p *fixedBufferPool) get() *fixedBuffer {
	// To ensure this is a fixed size buffer pool, always wait for returns
	// to the pool and so blockingly wait for consumer of the buffers
	// to return their buffers.
	return <-p.buffers
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
	pool       *fixedBufferPool
	ref        int64
	data       []byte
	unconsumed []byte
}

func newFixedBuffer(pool *fixedBufferPool, data []byte) *fixedBuffer {
	b := &fixedBuffer{
		pool: pool,
		data: data,
	}
	b.reset()
	return b
}

func (b *fixedBuffer) reset() {
	b.unconsumed = b.data
	atomic.StoreInt64(&b.ref, 1)
}

func (b *fixedBuffer) copy(src []byte) ([]byte, fixedBufferBorrow, bool) {
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

func (b *fixedBuffer) done() {
	b.decRef()
}

func (b *fixedBuffer) BufferFinalize() {
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

type fixedBufferManager struct {
	mu   sync.Mutex
	pool *fixedBufferPool
	curr *fixedBuffer
}

func newFixedBufferManager(pool *fixedBufferPool) *fixedBufferManager {
	return &fixedBufferManager{
		pool: pool,
	}
}

type fixedBufferBorrow interface {
	BufferFinalize()
}

var fixedBufferBorrowNoopGlobal fixedBufferBorrow = (*fixedBufferBorrowNoop)(nil)

type fixedBufferBorrowNoop struct {
}

func (b *fixedBufferBorrowNoop) BufferFinalize() {
	// noop
}

func (m *fixedBufferManager) copy(src []byte) ([]byte, fixedBufferBorrow) {
	size := len(src)
	if size > fixedBufferCapacity {
		return make([]byte, size), fixedBufferBorrowNoopGlobal
	}

	// All copies mutate.
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.curr == nil {
		m.curr = m.pool.get()
	}

	dst, borrow, ok := m.curr.copy(src)
	if !ok {
		// Rotate in next buffer.
		m.curr.done()
		m.curr = m.pool.get()

		// Perform copy again, fresh fixed buffer so must fit.
		dst, borrow, ok = m.curr.copy(src)
		if !ok {
			panic(fmt.Errorf("below max size and unable to borrow: size=%d", size))
		}
	}

	return dst, borrow
}
