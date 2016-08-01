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
	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/pool"
)

type fetchBatchOp struct {
	request       rpc.FetchRawBatchRequest
	completionFns []m3db.CompletionFn
}

func (f *fetchBatchOp) reset() {
	f.request.RangeStart = 0
	f.request.RangeEnd = 0
	f.request.Ids = f.request.Ids[:0]
	f.completionFns = f.completionFns[:0]
}

func (f *fetchBatchOp) append(id string, completionFn m3db.CompletionFn) {
	f.request.Ids = append(f.request.Ids, id)
	f.completionFns = append(f.completionFns, completionFn)
}

func (f *fetchBatchOp) Size() int {
	return len(f.request.Ids)
}

func (f *fetchBatchOp) GetCompletionFn() m3db.CompletionFn {
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

	// Put a fetch op
	Put(f *fetchBatchOp)
}

type poolOfFetchBatchOp struct {
	pool     m3db.ObjectPool
	capacity int
}

func newFetchBatchOpPool(size int, capacity int) fetchBatchOpPool {
	p := pool.NewObjectPool(size)
	return &poolOfFetchBatchOp{p, capacity}
}

func (p *poolOfFetchBatchOp) Init() {
	p.pool.Init(func() interface{} {
		f := &fetchBatchOp{}
		f.request.Ids = make([]string, 0, p.capacity)
		f.completionFns = make([]m3db.CompletionFn, 0, p.capacity)
		f.reset()
		return f
	})
}

func (p *poolOfFetchBatchOp) Get() *fetchBatchOp {
	f := p.pool.Get().(*fetchBatchOp)
	return f
}

func (p *poolOfFetchBatchOp) Put(f *fetchBatchOp) {
	if cap(f.request.Ids) != p.capacity || cap(f.completionFns) != p.capacity {
		// Grew outside capacity, do not return to pool
		return
	}
	f.reset()
	p.pool.Put(f)
}
