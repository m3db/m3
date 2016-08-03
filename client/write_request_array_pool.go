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

type writeRequestArrayPool interface {
	// Init pool
	Init()

	// Get an array of write requests
	Get() []*rpc.WriteRequest

	// Put an array of write request
	Put(w []*rpc.WriteRequest)
}

type poolOfWriteRequestArray struct {
	pool     m3db.ObjectPool
	capacity int
}

func newWriteRequestArrayPool(size int, capacity int) writeRequestArrayPool {
	p := pool.NewObjectPool(size)
	return &poolOfWriteRequestArray{p, capacity}
}

func (p *poolOfWriteRequestArray) Init() {
	p.pool.Init(func() interface{} {
		return make([]*rpc.WriteRequest, 0, p.capacity)
	})
}

func (p *poolOfWriteRequestArray) Get() []*rpc.WriteRequest {
	return p.pool.Get().([]*rpc.WriteRequest)
}

func (p *poolOfWriteRequestArray) Put(w []*rpc.WriteRequest) {
	w = w[:0]
	p.pool.Put(w)
}
