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

package client

import (
	"github.com/m3db/m3db/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3x/pool"
)

type writeTaggedBatchRawRequestElementArrayPool interface {
	// Init pool
	Init()

	// Get an array of WriteTaggedBatchRawRequestElement objects
	Get() []*rpc.WriteTaggedBatchRawRequestElement

	// Put an array of WriteTaggedBatchRawRequestElement objects
	Put(w []*rpc.WriteTaggedBatchRawRequestElement)
}

type poolOfWriteTaggedBatchRawRequestElementArray struct {
	pool     pool.ObjectPool
	capacity int
}

func newWriteTaggedBatchRawRequestElementArrayPool(
	opts pool.ObjectPoolOptions, capacity int) writeTaggedBatchRawRequestElementArrayPool {

	p := pool.NewObjectPool(opts)
	return &poolOfWriteTaggedBatchRawRequestElementArray{p, capacity}
}

func (p *poolOfWriteTaggedBatchRawRequestElementArray) Init() {
	p.pool.Init(func() interface{} {
		return make([]*rpc.WriteTaggedBatchRawRequestElement, 0, p.capacity)
	})
}

func (p *poolOfWriteTaggedBatchRawRequestElementArray) Get() []*rpc.WriteTaggedBatchRawRequestElement {
	return p.pool.Get().([]*rpc.WriteTaggedBatchRawRequestElement)
}

func (p *poolOfWriteTaggedBatchRawRequestElementArray) Put(w []*rpc.WriteTaggedBatchRawRequestElement) {
	for i := range w {
		w[i] = nil
	}
	w = w[:0]
	p.pool.Put(w)
}
