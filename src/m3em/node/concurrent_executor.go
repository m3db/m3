// Copyright (c) 2017 Uber Technologies, Inc.
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

package node

import (
	"fmt"
	"sync"
	"time"

	xerrors "github.com/m3db/m3x/errors"
	xsync "github.com/m3db/m3x/sync"
)

type executor struct {
	sync.Mutex
	wg      sync.WaitGroup
	timeout time.Duration
	workers xsync.WorkerPool
	nodes   []ServiceNode
	fn      ServiceNodeFn
	err     xerrors.MultiError
}

// NewConcurrentExecutor returns a new concurrent executor
func NewConcurrentExecutor(
	nodes []ServiceNode,
	concurrency int,
	timeout time.Duration,
	fn ServiceNodeFn,
) ConcurrentExecutor {
	workerPool := xsync.NewWorkerPool(concurrency)
	workerPool.Init()
	return &executor{
		timeout: timeout,
		workers: workerPool,
		nodes:   nodes,
		fn:      fn,
	}
}

func (e *executor) addError(err error) {
	e.Lock()
	defer e.Unlock()
	e.err = e.err.Add(err)
}

func (e *executor) Run() error {
	e.wg.Add(len(e.nodes))
	for idx := range e.nodes {
		node := e.nodes[idx]
		if ok := e.workers.GoWithTimeout(func() {
			defer e.wg.Done()
			e.addError(e.fn(node))
		}, e.timeout); !ok {
			e.addError(fmt.Errorf("unable to execute node %s operation due to worker timeout", node.ID()))
		}
	}
	e.wg.Wait()
	return e.err.FinalError()
}
