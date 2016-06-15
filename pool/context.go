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
	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/interfaces/m3db"
)

// TODO(r): instrument this to tune pooling
type contextPool struct {
	pool m3db.ObjectPool
}

// NewContextPool creates a new pool
func NewContextPool(size int) m3db.ContextPool {
	return &contextPool{pool: NewObjectPool(size)}
}

func (p *contextPool) Init() {
	p.pool.Init(func() interface{} {
		return context.NewPooledContext(p)
	})
}

func (p *contextPool) Get() m3db.Context {
	return p.pool.Get().(m3db.Context)
}

func (p *contextPool) Put(context m3db.Context) {
	p.pool.Put(context)
}
