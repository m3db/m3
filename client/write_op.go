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
	"math"

	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"
)

var (
	// NB(bl): use an invalid shardID for the zerod op
	writeOpZeroed = writeOp{shardID: math.MaxUint32}
)

type writeOp struct {
	namespace    ident.ID
	shardID      uint32
	request      rpc.WriteBatchRawRequestElement
	datapoint    rpc.Datapoint
	completionFn completionFn
	pool         *writeOpPool
}

func (w *writeOp) reset() {
	*w = writeOpZeroed
	w.request.Datapoint = &w.datapoint
}

func (w *writeOp) Close() {
	p := w.pool
	w.reset()
	if p != nil {
		p.Put(w)
	}
}

func (w *writeOp) Size() int {
	// Writes always represent a single write
	return 1
}

func (w *writeOp) CompletionFn() completionFn {
	return w.completionFn
}

func (w *writeOp) SetCompletionFn(fn completionFn) {
	w.completionFn = fn
}

func (w *writeOp) ShardID() uint32 {
	return w.shardID
}

type writeOpPool struct {
	pool pool.ObjectPool
}

func newWriteOpPool(opts pool.ObjectPoolOptions) *writeOpPool {
	p := pool.NewObjectPool(opts)
	return &writeOpPool{pool: p}
}

func (p *writeOpPool) Init() {
	p.pool.Init(func() interface{} {
		w := &writeOp{}
		w.reset()
		return w
	})
}

func (p *writeOpPool) Get() *writeOp {
	w := p.pool.Get().(*writeOp)
	w.pool = p
	return w
}

func (p *writeOpPool) Put(w *writeOp) {
	w.reset()
	p.pool.Put(w)
}
