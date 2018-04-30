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

package arraypool

import (
	"github.com/m3db/m3x/pool"

	"github.com/mauricelam/genny/generic"
)

// elemType is the generic type for use with the specialized array pool.
type elemType generic.Type

// elemArrayPool provides a pool for elemType slices.
type elemArrayPool interface {
	// Init initializes the array pool, it needs to be called
	// before Get/Put use.
	Init()

	// Get returns the a slice from the pool.
	Get() []elemType

	// Put returns the provided slice to the pool.
	Put(elems []elemType)
}

type elemFinalizeFn func([]elemType) []elemType

type elemArrayPoolOpts struct {
	Options     pool.ObjectPoolOptions
	Capacity    int
	MaxCapacity int
	FinalizeFn  elemFinalizeFn
}

type elemArrPool struct {
	opts elemArrayPoolOpts
	pool pool.ObjectPool
}

// nolint
func newElemArrayPool(opts elemArrayPoolOpts) elemArrayPool {
	if opts.FinalizeFn == nil {
		opts.FinalizeFn = defaultElemFinalizerFn
	}
	p := pool.NewObjectPool(opts.Options)
	return &elemArrPool{opts, p}
}

func (p *elemArrPool) Init() {
	p.pool.Init(func() interface{} {
		return make([]elemType, 0, p.opts.Capacity)
	})
}

func (p *elemArrPool) Get() []elemType {
	return p.pool.Get().([]elemType)
}

func (p *elemArrPool) Put(arr []elemType) {
	arr = p.opts.FinalizeFn(arr)
	if max := p.opts.MaxCapacity; max > 0 && cap(arr) > max {
		return
	}
	p.pool.Put(arr)
}

func defaultElemFinalizerFn(elems []elemType) []elemType {
	var empty elemType
	for i := range elems {
		elems[i] = empty
	}
	elems = elems[:0]
	return elems
}

// nolint
type elemArr []elemType

// nolint
func (elems elemArr) grow(n int) []elemType {
	if cap(elems) < n {
		elems = make([]elemType, n)
	}
	elems = elems[:n]
	// following compiler optimized memcpy impl
	// https://github.com/golang/go/wiki/CompilerOptimizations#optimized-memclr
	var empty elemType
	for i := range elems {
		elems[i] = empty
	}
	return elems
}
