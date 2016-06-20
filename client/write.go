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
	writeOpZeroed writeOp
)

type completionFn func(result interface{}, err error)

type op interface {
	// CompletionFn sets the completion function for the operation
	CompletionFn(value completionFn)

	// GetCompletionFn gets the completion function for the operation
	GetCompletionFn() completionFn
}

func callAllCompletionFns(ops []op, result interface{}, err error) {
	for i := range ops {
		ops[i].GetCompletionFn()(result, err)
	}
}

type writeOp struct {
	request      rpc.WriteRequest
	datapoint    rpc.Datapoint
	completionFn completionFn
}

func (w *writeOp) init() {
	*w = writeOpZeroed
	w.request.Datapoint = &w.datapoint
}

func (w *writeOp) CompletionFn(value completionFn) {
	w.completionFn = value
}

func (w *writeOp) GetCompletionFn() completionFn {
	return w.completionFn
}

type writeOpPool interface {
	// Get a write op
	Get() *writeOp

	// Put a write op
	Put(w *writeOp)
}

type poolOfWriteOp struct {
	pool m3db.ObjectPool
}

func newWriteOpPool(size int) writeOpPool {
	p := pool.NewObjectPool(size)
	p.Init(func() interface{} {
		return &writeOp{}
	})
	return &poolOfWriteOp{p}
}

func (p *poolOfWriteOp) Get() *writeOp {
	w := p.pool.Get().(*writeOp)
	w.init()
	return w
}

func (p *poolOfWriteOp) Put(w *writeOp) {
	p.pool.Put(w)
}
