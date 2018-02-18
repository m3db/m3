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
	writeTaggedOpZeroed = writeTaggedOp{shardID: math.MaxUint32}
)

type writeTaggedOp struct {
	namespace    ident.ID
	shardID      uint32
	request      rpc.WriteTaggedBatchRawRequestElement
	datapoint    rpc.Datapoint
	completionFn completionFn
	pool         *writeTaggedOpPool
}

func (w *writeTaggedOp) reset() {
	*w = writeTaggedOpZeroed
	w.request.Datapoint = &w.datapoint
	// TODO(prateek): need to test if this works; further if we can actually
	// use the pooled parts of the arrays
	w.request.Tags = w.request.Tags[:0]
}

func (w *writeTaggedOp) Close() {
	p := w.pool
	w.reset()
	if p != nil {
		p.Put(w)
	}
}

func (w *writeTaggedOp) Size() int {
	// Writes always represent a single write
	return 1
}

func (w *writeTaggedOp) CompletionFn() completionFn {
	return w.completionFn
}

func (w *writeTaggedOp) SetCompletionFn(fn completionFn) {
	w.completionFn = fn
}

func (w *writeTaggedOp) ShardID() uint32 {
	return w.shardID
}

type writeTaggedOpPool struct {
	pool pool.ObjectPool
}

func newWriteTaggedOpPool(opts pool.ObjectPoolOptions) *writeTaggedOpPool {
	p := pool.NewObjectPool(opts)
	return &writeTaggedOpPool{pool: p}
}

func (p *writeTaggedOpPool) Init() {
	p.pool.Init(func() interface{} {
		w := &writeTaggedOp{}
		w.reset()
		return w
	})
}

func (p *writeTaggedOpPool) Get() *writeTaggedOp {
	w := p.pool.Get().(*writeTaggedOp)
	w.pool = p
	return w
}

func (p *writeTaggedOpPool) Put(w *writeTaggedOp) {
	w.reset()
	p.pool.Put(w)
}
