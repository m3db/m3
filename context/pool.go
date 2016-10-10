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

package context

import (
	"github.com/m3db/m3db/pool"
)

const (
	defaultClosersCapacity = 4
	numClosersPerContext   = 0.05
)

// closersPool provides a pool for closer slices
type closersPool interface {
	// Get provides a pre-allocated slice to store closers
	Get() []Closer

	// Put returns a closer slice to the pool
	Put(closers []Closer)
}

type poolOfClosers struct {
	pool pool.ObjectPool
}

// newClosersPool creates a new closers pool
func newClosersPool(opts pool.ObjectPoolOptions) closersPool {
	p := &poolOfClosers{pool: pool.NewObjectPool(opts)}
	p.pool.Init(func() interface{} {
		return createClosers()
	})
	return p
}

func (p *poolOfClosers) Get() []Closer {
	return p.pool.Get().([]Closer)
}

func (p *poolOfClosers) Put(closers []Closer) {
	for i := range closers {
		// Free values from collection
		closers[i] = nil
	}
	if len(closers) > defaultClosersCapacity {
		// Free any large arrays that are created
		return
	}
	p.pool.Put(closers[:0])
}

type poolOfContexts struct {
	ctxPool     pool.ObjectPool
	closersPool closersPool
}

// NewPool creates a new context pool
func NewPool(opts pool.ObjectPoolOptions) Pool {
	if opts == nil {
		opts = pool.NewObjectPoolOptions()
	}
	copts := opts.
		SetSize(int(float64(opts.Size()) * numClosersPerContext)).
		SetMetricsScope(opts.MetricsScope().SubScope("closers"))

	p := &poolOfContexts{
		ctxPool:     pool.NewObjectPool(opts),
		closersPool: newClosersPool(copts),
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

func (p *poolOfContexts) GetClosers() []Closer {
	return p.closersPool.Get()
}

func (p *poolOfContexts) PutClosers(closers []Closer) {
	p.closersPool.Put(closers)
}

func createClosers() []Closer {
	return make([]Closer, 0, defaultClosersCapacity)
}
