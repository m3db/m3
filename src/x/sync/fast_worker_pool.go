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
	"time"

	"github.com/m3db/m3/src/x/context"
)

type fastWorkerPool struct {
	workerPool WorkerPool
	batchSize  int
	count      int
}

var _ WorkerPool = &fastWorkerPool{}

func (p *fastWorkerPool) Init() {
	p.workerPool.Init()
}

func (p *fastWorkerPool) Go(work Work) {
	p.workerPool.Go(work)
}

func (p *fastWorkerPool) GoInstrument(work Work) ScheduleResult {
	return p.workerPool.GoInstrument(work)
}

func (p *fastWorkerPool) GoIfAvailable(work Work) bool {
	return p.workerPool.GoIfAvailable(work)
}

func (p *fastWorkerPool) GoWithTimeout(work Work, timeout time.Duration) bool {
	return p.workerPool.GoWithTimeout(work, timeout)
}

func (p *fastWorkerPool) GoWithTimeoutInstrument(work Work, timeout time.Duration) ScheduleResult {
	return p.workerPool.GoWithTimeoutInstrument(work, timeout)
}

func (p *fastWorkerPool) GoWithContext(ctx context.Context, work Work) ScheduleResult {
	p.count++
	if p.count == 1 {
		return p.workerPool.GoWithContext(ctx, work)
	}
	if p.count == p.batchSize {
		p.count = 0
	}
	return p.workerPool.GoInstrument(work)
}

func (p *fastWorkerPool) FastContextCheck(batchSize int) WorkerPool {
	return &fastWorkerPool{workerPool: p.workerPool, batchSize: batchSize}
}
