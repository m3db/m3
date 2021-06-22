// Copyright (c) 2021 Uber Technologies, Inc.
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

package sync

import (
	"context"
	"time"
)

type fastPooledWorkerPool struct {
	workerPool PooledWorkerPool
	batchSize  int
	count      int
}

var _ PooledWorkerPool = &fastPooledWorkerPool{}

func (f *fastPooledWorkerPool) Init() {
	f.workerPool.Init()
}

func (f *fastPooledWorkerPool) Go(work Work) {
	f.workerPool.Go(work)
}

func (f *fastPooledWorkerPool) GoWithTimeout(work Work, timeout time.Duration) bool {
	return f.workerPool.GoWithTimeout(work, timeout)
}

func (f *fastPooledWorkerPool) GoWithContext(ctx context.Context, work Work) bool {
	f.count++
	if f.count == 1 {
		return f.workerPool.GoWithContext(ctx, work)
	}
	if f.count == f.batchSize {
		f.count = 0
	}
	f.workerPool.Go(work)
	return true
}

func (f *fastPooledWorkerPool) FastContextCheck(batchSize int) PooledWorkerPool {
	return &fastPooledWorkerPool{workerPool: f.workerPool, batchSize: batchSize}
}
