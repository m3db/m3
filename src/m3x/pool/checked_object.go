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

import "github.com/m3db/m3x/checked"

type checkedObjectPool struct {
	pool          ObjectPool
	finalizerPool ObjectPool
}

type checkedObjectFinalizer struct {
	value checked.ReadWriteRef
	p     *checkedObjectPool
}

func (f *checkedObjectFinalizer) Finalize() {
	f.p.pool.Put(f.value)

	finalizerPool := f.p.finalizerPool
	f.value = nil
	f.p = nil

	finalizerPool.Put(f)
}

// NewCheckedObjectPool creates a new checked pool
func NewCheckedObjectPool(opts ObjectPoolOptions) CheckedObjectPool {
	if opts == nil {
		opts = NewObjectPoolOptions()
	}
	return &checkedObjectPool{
		pool: NewObjectPool(opts),
		finalizerPool: NewObjectPool(opts.SetInstrumentOptions(opts.InstrumentOptions().
			SetMetricsScope(opts.InstrumentOptions().
				MetricsScope().
				SubScope("finalizer-pool")))),
	}
}

func (p *checkedObjectPool) Init(alloc CheckedAllocator) {
	p.pool.Init(func() interface{} {
		return alloc()
	})
	p.finalizerPool.Init(func() interface{} {
		return &checkedObjectFinalizer{}
	})
}

func (p *checkedObjectPool) Get() checked.ReadWriteRef {
	value := p.pool.Get().(checked.ReadWriteRef)
	finalizer := p.finalizerPool.Get().(*checkedObjectFinalizer)
	finalizer.value = value
	finalizer.p = p
	value.SetFinalizer(finalizer)
	return value
}
