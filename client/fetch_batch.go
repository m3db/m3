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
	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/pool"
)

var (
	emptyIDWithNamespace = rpc.IDWithNamespace{}
)

type fetchBatchOp struct {
	request       rpc.FetchRawBatchRequest
	completionFns []completionFn
}

func (f *fetchBatchOp) reset() {
	f.request.RangeStart = 0
	f.request.RangeEnd = 0
	f.request.IdsWithNamespace = f.request.IdsWithNamespace[:0]
	f.completionFns = f.completionFns[:0]
}

func (f *fetchBatchOp) append(idn *rpc.IDWithNamespace, completionFn completionFn) {
	f.request.IdsWithNamespace = append(f.request.IdsWithNamespace, idn)
	f.completionFns = append(f.completionFns, completionFn)
}

func (f *fetchBatchOp) Size() int {
	return len(f.request.IdsWithNamespace)
}

func (f *fetchBatchOp) GetCompletionFn() completionFn {
	return f.completeAll
}

func (f *fetchBatchOp) completeAll(result interface{}, err error) {
	for idx := range f.completionFns {
		f.completionFns[idx](result, err)
	}
}

func (f *fetchBatchOp) complete(idx int, result interface{}, err error) {
	f.completionFns[idx](result, err)
}

type fetchBatchOpPool interface {
	// Init pool
	Init()

	// Get a fetch op
	Get() *fetchBatchOp

	// GetIDWithNamespace returns an IDWithNamespace object
	GetIDWithNamespace() *rpc.IDWithNamespace

	// Put a fetch op
	Put(f *fetchBatchOp)
}

type poolOfFetchBatchOp struct {
	batchOpPool pool.ObjectPool
	idnPool     pool.ObjectPool
	capacity    int
}

func newFetchBatchOpPool(size int, capacity int) fetchBatchOpPool {
	batchOpPool := pool.NewObjectPool(size)
	idnPool := pool.NewObjectPool(size * capacity)
	return &poolOfFetchBatchOp{batchOpPool, idnPool, capacity}
}

func (p *poolOfFetchBatchOp) Init() {
	p.batchOpPool.Init(func() interface{} {
		f := &fetchBatchOp{}
		f.request.IdsWithNamespace = make([]*rpc.IDWithNamespace, 0, p.capacity)
		f.completionFns = make([]completionFn, 0, p.capacity)
		f.reset()
		return f
	})
	p.idnPool.Init(func() interface{} {
		return rpc.NewIDWithNamespace()
	})
}

func (p *poolOfFetchBatchOp) Get() *fetchBatchOp {
	f := p.batchOpPool.Get().(*fetchBatchOp)
	return f
}

func (p *poolOfFetchBatchOp) GetIDWithNamespace() *rpc.IDWithNamespace {
	return p.idnPool.Get().(*rpc.IDWithNamespace)
}

func (p *poolOfFetchBatchOp) Put(f *fetchBatchOp) {
	for _, idn := range f.request.IdsWithNamespace {
		*idn = emptyIDWithNamespace
		p.idnPool.Put(idn)
	}
	if cap(f.request.IdsWithNamespace) != p.capacity || cap(f.completionFns) != p.capacity {
		// Grew outside capacity, do not return to pool
		return
	}
	f.reset()
	p.batchOpPool.Put(f)
}
