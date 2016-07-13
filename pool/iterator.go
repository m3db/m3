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

import (
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/interfaces/m3db"
)

// TODO(r): instrument this to tune pooling
type readerIteratorPool struct {
	pool m3db.ObjectPool
}

// NewReaderIteratorPool creates a new pool for ReaderIterators.
func NewReaderIteratorPool(size int) m3db.ReaderIteratorPool {
	return &readerIteratorPool{pool: NewObjectPool(size)}
}

func (p *readerIteratorPool) Init(alloc m3db.ReaderIteratorAllocate) {
	p.pool.Init(func() interface{} {
		return alloc(nil)
	})
}

func (p *readerIteratorPool) Get() m3db.ReaderIterator {
	return p.pool.Get().(m3db.ReaderIterator)
}

func (p *readerIteratorPool) Put(iter m3db.ReaderIterator) {
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

func (p *multiReaderIteratorPool) Init(alloc m3db.ReaderIteratorAllocate) {
	p.pool.Init(func() interface{} {
		return encoding.NewMultiReaderIterator(alloc, p)
	})
}

func (p *multiReaderIteratorPool) Get() m3db.MultiReaderIterator {
	return p.pool.Get().(m3db.MultiReaderIterator)
}

func (p *multiReaderIteratorPool) Put(iter m3db.MultiReaderIterator) {
	p.pool.Put(iter)
}
