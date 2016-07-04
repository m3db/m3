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

package pool

import (
	"github.com/m3db/m3db/interfaces/m3db"
)

// TODO(xichen): instrument this.
type workerPool struct {
	workCh chan struct{}
}

// NewWorkerPool creates a new worker pool.
func NewWorkerPool(size int) m3db.WorkerPool {
	return &workerPool{workCh: make(chan struct{}, size)}
}

func (p *workerPool) Init() {
	for i := 0; i < cap(p.workCh); i++ {
		p.workCh <- struct{}{}
	}
}

func (p *workerPool) Go(work m3db.Work) {
	token := <-p.workCh
	go func() {
		work()
		p.workCh <- token
	}()
}

func (p *workerPool) GoIfAvailable(work m3db.Work) bool {
	select {
	case token := <-p.workCh:
		go func() {
			work()
			p.workCh <- token
		}()
		return true
	default:
		return false
	}
}
