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

package pool

import "github.com/m3db/m3db/interfaces/m3db"

// TODO(r): instrument this to tune pooling
type singleReaderIteratorPool struct {
	pool m3db.ObjectPool
}

// NewSingleReaderIteratorPool creates a new pool for SingleReaderIterators.
func NewSingleReaderIteratorPool(size int) m3db.SingleReaderIteratorPool {
	return &singleReaderIteratorPool{pool: NewObjectPool(size)}
}

func (p *singleReaderIteratorPool) Init(alloc m3db.SingleReaderIteratorAllocate) {
	p.pool.Init(func() interface{} {
		return alloc()
	})
}

func (p *singleReaderIteratorPool) Get() m3db.SingleReaderIterator {
	return p.pool.Get().(m3db.SingleReaderIterator)
}

func (p *singleReaderIteratorPool) Put(iter m3db.SingleReaderIterator) {
	p.pool.Put(iter)
}

// TODO(xichen): instrument this to tune pooling
type multiReaderIteratorPool struct {
	pool m3db.ObjectPool
}

// NewMultiReaderIteratorPool creates a new pool for MultiReaderIterators.
func NewMultiReaderIteratorPool(size int) m3db.MultiReaderIteratorPool {
	return &multiReaderIteratorPool{pool: NewObjectPool(size)}
}

func (p *multiReaderIteratorPool) Init(alloc m3db.MultiReaderIteratorAllocate) {
	p.pool.Init(func() interface{} {
		return alloc()
	})
}

func (p *multiReaderIteratorPool) Get() m3db.MultiReaderIterator {
	return p.pool.Get().(m3db.MultiReaderIterator)
}

func (p *multiReaderIteratorPool) Put(iter m3db.MultiReaderIterator) {
	p.pool.Put(iter)
}
