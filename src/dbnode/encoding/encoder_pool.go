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

package encoding

import (
	"github.com/m3db/m3/src/x/pool"
)

type encoderPool struct {
	pool pool.ObjectPool
	init SchemaInjector
}

// NewEncoderPool creates a new pool
func NewEncoderPool(opts pool.ObjectPoolOptions) EncoderPool {
	return &encoderPool{pool: pool.NewObjectPool(opts)}
}

func (p *encoderPool) Init(alloc EncoderAllocate) {
	p.pool.Init(func() interface{} {
		return alloc()
	})
}

// ReInit reinitialize the pool with a different object initializer.
// the new pool shares the save underlying object pool.
func (p *encoderPool) ReInit(reInit SchemaInjector) EncoderPool {
	return &encoderPool{pool: p.pool, init: reInit}
}

func (p *encoderPool) Get() Encoder {
	if p.init == nil {
		return p.pool.Get().(Encoder)
	}
	e := p.pool.Get().(SchemaInjectable)
	return p.init(e).(Encoder)
}

func (p *encoderPool) Put(encoder Encoder) {
	// Clear schema (in case schema is not set at Get, we will fail fast).
	encoder.SetSchema(nil)
	p.pool.Put(encoder)
}
