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

package client

import (
	"github.com/m3db/m3db/pool"
)

type opArrayPool interface {
	// Init pool
	Init()

	// Get an array of ops
	Get() []op

	// Put an array of ops
	Put(ops []op)
}

type poolOfOpArray struct {
	pool     pool.ObjectPool
	capacity int
}

func newOpArrayPool(opts pool.ObjectPoolOptions, capacity int) opArrayPool {
	p := pool.NewObjectPool(opts)
	return &poolOfOpArray{p, capacity}
}

func (p *poolOfOpArray) Init() {
	p.pool.Init(func() interface{} {
		return make([]op, 0, p.capacity)
	})
}

func (p *poolOfOpArray) Get() []op {
	return p.pool.Get().([]op)
}

func (p *poolOfOpArray) Put(ops []op) {
	for i := range ops {
		ops[i] = nil
	}
	ops = ops[:0]
	p.pool.Put(ops)
}

type fetchBatchOpArrayArrayPool interface {
	// Init pool
	Init()

	// Get an array of fetch ops arrays
	Get() [][]*fetchBatchOp

	// Put an array of fetch ops arrays
	Put(ops [][]*fetchBatchOp)

	// Entries returns the entries of fetch ops array array returned by the pool
	Entries() int

	// Capacity returns the capacity of each fetch ops array in each entry
	Capacity() int
}

type poolOfFetchBatchOpArrayArray struct {
	pool     pool.ObjectPool
	entries  int
	capacity int
}

func newFetchBatchOpArrayArrayPool(opts pool.ObjectPoolOptions, entries int, capacity int) fetchBatchOpArrayArrayPool {
	p := pool.NewObjectPool(opts)
	return &poolOfFetchBatchOpArrayArray{p, entries, capacity}
}

func (p *poolOfFetchBatchOpArrayArray) Init() {
	p.pool.Init(func() interface{} {
		arr := make([][]*fetchBatchOp, p.entries)
		for i := range arr {
			arr[i] = make([]*fetchBatchOp, 0, p.capacity)
		}
		return arr
	})
}

func (p *poolOfFetchBatchOpArrayArray) Get() [][]*fetchBatchOp {
	return p.pool.Get().([][]*fetchBatchOp)
}

func (p *poolOfFetchBatchOpArrayArray) Put(arr [][]*fetchBatchOp) {
	for i := range arr {
		for j := range arr[i] {
			arr[i][j] = nil
		}
		arr[i] = arr[i][:0]
	}
	p.pool.Put(arr)
}

func (p *poolOfFetchBatchOpArrayArray) Entries() int {
	return p.entries
}

func (p *poolOfFetchBatchOpArrayArray) Capacity() int {
	return p.capacity
}
