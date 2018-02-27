// Copyright (c) 2018 Uber Technologies, Inc.
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

package context

import (
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/resource"
)

// finalizersPool provides a pool for finalizer slices.
type finalizersPool interface {
	// Get provides a pre-allocated slice to store finalizers.
	Get() []resource.Finalizer

	// Put returns a finalizers slice to the pool.
	Put([]resource.Finalizer)
}

type poolOfFinalizers struct {
	pool                  pool.ObjectPool
	maxPooledFinalizerCap int
}

// newFinalizerPool creates a new finalizers pool.
func newFinalizersPool(opts Options) finalizersPool {
	p := &poolOfFinalizers{
		pool: pool.NewObjectPool(opts.FinalizerPoolOptions()),
		maxPooledFinalizerCap: opts.MaxPooledFinalizerCapacity(),
	}

	init := opts.InitPooledFinalizerCapacity()
	p.pool.Init(func() interface{} {
		return allocateFinalizers(init)
	})

	return p
}

func (p *poolOfFinalizers) Get() []resource.Finalizer {
	return p.pool.Get().([]resource.Finalizer)
}

func (p *poolOfFinalizers) Put(finalizers []resource.Finalizer) {
	for i := range finalizers {
		// Free values from collection.
		finalizers[i] = nil
	}

	if cap(finalizers) > p.maxPooledFinalizerCap {
		// Free any large arrays that are created.
		return
	}

	p.pool.Put(finalizers[:0])
}

type poolOfContexts struct {
	ctxPool        pool.ObjectPool
	finalizersPool finalizersPool
}

// NewPool creates a new context pool.
func NewPool(opts Options) Pool {
	p := &poolOfContexts{
		ctxPool:        pool.NewObjectPool(opts.ContextPoolOptions()),
		finalizersPool: newFinalizersPool(opts),
	}

	p.ctxPool.Init(func() interface{} {
		return newPooledContext(p)
	})

	return p
}

func (p *poolOfContexts) Get() Context {
	return p.ctxPool.Get().(Context)
}

func (p *poolOfContexts) Put(context Context) {
	p.ctxPool.Put(context)
}

func (p *poolOfContexts) GetFinalizers() []resource.Finalizer {
	return p.finalizersPool.Get()
}

func (p *poolOfContexts) PutFinalizers(finalizers []resource.Finalizer) {
	p.finalizersPool.Put(finalizers)
}

func allocateFinalizers(initLen int) []resource.Finalizer {
	return make([]resource.Finalizer, 0, initLen)
}
