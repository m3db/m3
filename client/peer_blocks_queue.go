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
	"sync"
	"sync/atomic"
	"time"

	xsync "github.com/m3db/m3x/sync"
)

type peerBlocksQueue struct {
	sync.RWMutex
	closed       bool
	peer         hostQueue
	queue        []*blocksMetadata
	doneFns      []func()
	assigned     int32 // for testing. todo@bl - get rid of this
	pending      int32
	maxQueueSize int
	workers      xsync.WorkerPool
	processFn    processFn
}

type peerBlocksQueues []*peerBlocksQueue

type newPeerBlocksQueueFn func(
	peer hostQueue,
	maxQueueSize int,
	interval time.Duration,
	workers xsync.WorkerPool,
	processFn processFn,
) *peerBlocksQueue

func newPeerBlocksQueue(
	peer hostQueue,
	maxQueueSize int,
	interval time.Duration,
	workers xsync.WorkerPool,
	processFn processFn,
) *peerBlocksQueue {
	q := &peerBlocksQueue{
		peer:         peer,
		maxQueueSize: maxQueueSize,
		workers:      workers,
		processFn:    processFn,
	}
	if interval > 0 {
		go q.drainEvery(interval)
	}
	return q
}

func (q *peerBlocksQueue) drainEvery(interval time.Duration) {
	for {
		q.Lock()
		if q.closed {
			q.Unlock()
			return
		}
		q.drainWithLock()
		q.Unlock()
		time.Sleep(interval)
	}
}

func (q *peerBlocksQueue) close() {
	q.Lock()
	q.closed = true
	q.Unlock()
}

func (q *peerBlocksQueue) trackAssigned(amount int) {
	atomic.AddInt32(&q.assigned, int32(amount))
	atomic.AddInt32(&q.pending, int32(amount))
}

func (q *peerBlocksQueue) trackCompleted(amount int) {
	atomic.AddInt32(&q.pending, -int32(amount))
}

func (q *peerBlocksQueue) enqueue(bm *blocksMetadata, doneFn func()) {
	q.Lock()

	if len(q.queue) == 0 && cap(q.queue) < q.maxQueueSize {
		// Lazy initialize queue
		q.queue = make([]*blocksMetadata, 0, q.maxQueueSize)
	}
	if len(q.doneFns) == 0 && cap(q.doneFns) < q.maxQueueSize {
		// Lazy initialize doneFns
		q.doneFns = make([]func(), 0, q.maxQueueSize)
	}
	q.queue = append(q.queue, bm)
	if doneFn != nil {
		q.doneFns = append(q.doneFns, doneFn)
	}
	q.trackAssigned(len(bm.blocks))

	// Determine if should drain immediately
	if len(q.queue) < q.maxQueueSize {
		// Require more to fill up block
		q.Unlock()
		return
	}
	q.drainWithLock()

	q.Unlock()
}

func (q *peerBlocksQueue) drain() {
	q.Lock()
	q.drainWithLock()
	q.Unlock()
}

func (q *peerBlocksQueue) drainWithLock() {
	if len(q.queue) == 0 {
		// None to drain
		return
	}
	enqueued := q.queue
	doneFns := q.doneFns
	q.queue = nil
	q.doneFns = nil
	q.workers.Go(func() {
		q.processFn(enqueued)
		// Call done callbacks
		for i := range doneFns {
			doneFns[i]()
		}
		// Track completed blocks
		completed := 0
		for i := range enqueued {
			completed += len(enqueued[i].blocks)
		}
		q.trackCompleted(completed)
	})
}

func (qs peerBlocksQueues) findQueue(peer hostQueue) *peerBlocksQueue {
	for _, q := range qs {
		if q.peer == peer {
			return q
		}
	}
	return nil
}

func (qs peerBlocksQueues) closeAll() {
	for _, q := range qs {
		q.close()
	}
}
