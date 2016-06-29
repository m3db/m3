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

// TODO(xichen): instrument this to tune pooling
type databaseBlockPool struct {
	pool m3db.ObjectPool
}

// NewDatabaseBlockPool creates a new pool for database blocks.
func NewDatabaseBlockPool(size int) m3db.DatabaseBlockPool {
	return &databaseBlockPool{pool: NewObjectPool(size)}
}

func (p *databaseBlockPool) Init(alloc m3db.DatabaseBlockAllocate) {
	p.pool.Init(func() interface{} {
		return alloc()
	})
}

func (p *databaseBlockPool) Get() m3db.DatabaseBlock {
	return p.pool.Get().(m3db.DatabaseBlock)
}

func (p *databaseBlockPool) Put(block m3db.DatabaseBlock) {
	p.pool.Put(block)
}
