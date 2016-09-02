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

type idDatapointArrayPool interface {
	// Init pool
	Init()

	// Get an array of idDatapoint objects
	Get() []*rpc.IDDatapoint

	// Put an array of idDatapoint objects
	Put(w []*rpc.IDDatapoint)
}

type poolOfIDDatapointArray struct {
	pool     pool.ObjectPool
	capacity int
}

func newIDDatapointArrayPool(size int, capacity int) idDatapointArrayPool {
	p := pool.NewObjectPool(size)
	return &poolOfIDDatapointArray{p, capacity}
}

func (p *poolOfIDDatapointArray) Init() {
	p.pool.Init(func() interface{} {
		return make([]*rpc.IDDatapoint, 0, p.capacity)
	})
}

func (p *poolOfIDDatapointArray) Get() []*rpc.IDDatapoint {
	return p.pool.Get().([]*rpc.IDDatapoint)
}

func (p *poolOfIDDatapointArray) Put(w []*rpc.IDDatapoint) {
	w = w[:0]
	p.pool.Put(w)
}
