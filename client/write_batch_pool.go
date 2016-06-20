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
	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/network/server/tchannelthrift/thrift/gen-go/rpc"
	"github.com/m3db/m3db/pool"
)

var (
	writeBatchRequestZeroed rpc.WriteBatchRequest
)

type writeBatchRequestPool interface {
	// Get a write batch request
	Get() *rpc.WriteBatchRequest

	// Put a write batch request
	Put(w *rpc.WriteBatchRequest)
}

type poolOfWriteBatchRequest struct {
	pool m3db.ObjectPool
}

func newWriteBatchRequestPool(size int) writeBatchRequestPool {
	p := pool.NewObjectPool(size)
	p.Init(func() interface{} {
		return &rpc.WriteBatchRequest{}
	})
	return &poolOfWriteBatchRequest{p}
}

func (p *poolOfWriteBatchRequest) Get() *rpc.WriteBatchRequest {
	return p.pool.Get().(*rpc.WriteBatchRequest)
}

func (p *poolOfWriteBatchRequest) Put(w *rpc.WriteBatchRequest) {
	*w = writeBatchRequestZeroed
	p.pool.Put(w)
}
