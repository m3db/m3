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
	"errors"
	"math"
	"sync"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/topology"

	"github.com/uber/tchannel-go/thrift"
)

var (
	errQueueNotOpen          = errors.New("host operation queue not open")
	errQueueUnknownOperation = errors.New("host operation queue received unknown operation")
	errQueueFetchNoResponse  = errors.New("host operation queue did not receive response for given fetch")
)

type queue struct {
	sync.RWMutex

	opts                  Options
	nowFn                 clock.NowFn
	host                  topology.Host
	connPool              connectionPool
	writeBatchRequestPool writeBatchRequestPool
	idDatapointArrayPool  idDatapointArrayPool
	size                  int
	ops                   []op
	opsSumSize            int
	opsLastRotatedAt      time.Time
	opsArrayPool          opArrayPool
	drainIn               chan []op
	state                 state
}

func newHostQueue(
	host topology.Host,
	writeBatchRequestPool writeBatchRequestPool,
	idDatapointArrayPool idDatapointArrayPool,
	opts Options,
) hostQueue {
	size := opts.GetHostQueueOpsFlushSize()

	opArrayPoolCapacity := int(math.Max(float64(size), float64(opts.GetWriteBatchSize())))
	opArrayPool := newOpArrayPool(opts.GetHostQueueOpsArrayPoolSize(), opArrayPoolCapacity)
	opArrayPool.Init()

	return &queue{
		opts:                  opts,
		nowFn:                 opts.GetClockOptions().GetNowFn(),
		host:                  host,
		connPool:              newConnectionPool(host, opts),
		writeBatchRequestPool: writeBatchRequestPool,
		idDatapointArrayPool:  idDatapointArrayPool,
		size:                  size,
		ops:                   opArrayPool.Get(),
		opsArrayPool:          opArrayPool,
		// NB(r): specifically use non-buffered queue for single flush at a time
		drainIn: make(chan []op),
	}
}

func (q *queue) Open() {
	q.Lock()
	defer q.Unlock()

	if q.state != stateNotOpen {
		return
	}

	q.state = stateOpen

	// Open the connection pool
	q.connPool.Open()

	// Continually drain the queue until closed
	go q.drain()

	flushInterval := q.opts.GetHostQueueOpsFlushInterval()
	if flushInterval > 0 {
		// Continually flush the queue at given interval if set
		go q.flushEvery(flushInterval)
	}
}

func (q *queue) flushEvery(interval time.Duration) {
	// sleepForOverride used change the next sleep based on last ops rotation
	var sleepForOverride time.Duration
	for {
		sleepFor := interval
		if sleepForOverride > 0 {
			sleepFor = sleepForOverride
			sleepForOverride = 0
		}

		time.Sleep(sleepFor)

		q.RLock()
		if q.state != stateOpen {
			q.RUnlock()
			return
		}
		lastRotateAt := q.opsLastRotatedAt
		q.RUnlock()

		sinceLastRotate := q.nowFn().Sub(lastRotateAt)
		if sinceLastRotate < interval {
			// Rotated already recently, sleep until we would next consider flushing
			sleepForOverride = interval - sinceLastRotate
			continue
		}

		q.Lock()
		needsDrain := q.rotateOpsWithLock()
		q.Unlock()

		if len(needsDrain) != 0 {
			q.RLock()
			if q.state != stateOpen {
				q.RUnlock()
				return
			}
			// Need to hold lock while writing to the drainIn
			// channel to ensure it has not been closed
			q.drainIn <- needsDrain
			q.RUnlock()
		}
	}
}

func (q *queue) rotateOpsWithLock() []op {
	if q.opsSumSize == 0 {
		// No need to rotate as queue is empty
		return nil
	}

	needsDrain := q.ops

	// Reset ops
	q.ops = q.opsArrayPool.Get()
	q.opsSumSize = 0
	q.opsLastRotatedAt = q.nowFn()

	return needsDrain
}

func (q *queue) drain() {
	var (
		wgAll                       = &sync.WaitGroup{}
		currWriteOpsByNamespace     = make(map[string][]op)
		currIDDatapointsByNamespace = make(map[string][]*rpc.IDDatapoint)
		writeBatchSize              = q.opts.GetWriteBatchSize()
	)

	for {
		ops, ok := <-q.drainIn
		if !ok {
			break
		}
		var (
			currWriteOps     []op
			currIDDatapoints []*rpc.IDDatapoint
			opsLen           = len(ops)
		)
		for i := 0; i < opsLen; i++ {
			switch v := ops[i].(type) {
			case *writeOp:
				namespace := v.request.NameSpace
				currWriteOps = currWriteOpsByNamespace[namespace]
				currIDDatapoints = currIDDatapointsByNamespace[namespace]
				if currWriteOps == nil {
					currWriteOps = q.opsArrayPool.Get()
					currIDDatapoints = q.idDatapointArrayPool.Get()
				}

				currWriteOps = append(currWriteOps, ops[i])
				currIDDatapoints = append(currIDDatapoints, v.request.IdDatapoint)
				currWriteOpsByNamespace[namespace] = currWriteOps
				currIDDatapointsByNamespace[namespace] = currIDDatapoints

				if len(currWriteOps) == writeBatchSize {
					// Reached write batch limit, write async and reset
					q.asyncWrite(wgAll, namespace, currWriteOps, currIDDatapoints)
					currWriteOpsByNamespace[namespace] = nil
					currIDDatapointsByNamespace[namespace] = nil
				}
			case *fetchBatchOp:
				q.asyncFetch(wgAll, v)
			case *truncateOp:
				q.asyncTruncate(wgAll, v)
			default:
				completionFn := ops[i].GetCompletionFn()
				completionFn(nil, errQueueUnknownOperation)
			}
		}

		// If any outstanding write ops, async write
		for namespace, writeOps := range currWriteOpsByNamespace {
			if len(writeOps) > 0 {
				q.asyncWrite(wgAll, namespace, writeOps, currIDDatapointsByNamespace[namespace])
				currWriteOpsByNamespace[namespace] = nil
				currIDDatapointsByNamespace[namespace] = nil
			}
		}

		if ops != nil {
			q.opsArrayPool.Put(ops)
		}
	}

	// Close the connection pool after all requests done
	wgAll.Wait()
	q.connPool.Close()
}

func (q *queue) asyncWrite(wg *sync.WaitGroup, namespace string, ops []op, elems []*rpc.IDDatapoint) {
	wg.Add(1)
	// TODO(r): Use a worker pool to avoid creating new go routines for async writes
	go func() {
		req := q.writeBatchRequestPool.Get()
		req.NameSpace = namespace
		req.Elements = elems

		// NB(r): Defer is slow in the hot path unfortunately
		cleanup := func() {
			q.writeBatchRequestPool.Put(req)
			q.idDatapointArrayPool.Put(elems)
			q.opsArrayPool.Put(ops)
			wg.Done()
		}

		client, err := q.connPool.NextClient()
		if err != nil {
			// No client available
			callAllCompletionFns(ops, nil, err)
			cleanup()
			return
		}

		ctx, _ := thrift.NewContext(q.opts.GetWriteRequestTimeout())
		err = client.WriteBatch(ctx, req)
		if err == nil {
			// All succeeded
			callAllCompletionFns(ops, nil, nil)
			cleanup()
			return
		}

		if batchErrs, ok := err.(*rpc.WriteBatchErrors); ok {
			// Callback all writes with errors
			hasErr := make(map[int]struct{})
			for _, batchErr := range batchErrs.Errors {
				op := ops[batchErr.Index]
				op.GetCompletionFn()(nil, batchErr.Err)
				hasErr[int(batchErr.Index)] = struct{}{}
			}
			// Callback all writes with no errors
			for i := range ops {
				if _, ok := hasErr[i]; !ok {
					// No error
					ops[i].GetCompletionFn()(nil, nil)
				}
			}
			cleanup()
			return
		}

		// Entire batch failed
		callAllCompletionFns(ops, nil, err)
		cleanup()
	}()
}

func (q *queue) asyncFetch(wg *sync.WaitGroup, op *fetchBatchOp) {
	wg.Add(1)
	// TODO(r): Use a worker pool to avoid creating new go routines for async fetches
	go func() {
		// NB(r): Defer is slow in the hot path unfortunately
		cleanup := wg.Done

		client, err := q.connPool.NextClient()
		if err != nil {
			// No client available
			op.completeAll(nil, err)
			cleanup()
			return
		}

		ctx, _ := thrift.NewContext(q.opts.GetFetchRequestTimeout())
		result, err := client.FetchRawBatch(ctx, &op.request)
		if err != nil {
			op.completeAll(nil, err)
			cleanup()
			return
		}

		resultLen := len(result.Elements)
		for i := 0; i < op.Size(); i++ {
			if !(i < resultLen) {
				// No results for this entry, in practice should never occur
				op.complete(i, nil, errQueueFetchNoResponse)
				continue
			}
			if result.Elements[i].Err != nil {
				op.complete(i, nil, result.Elements[i].Err)
				continue
			}
			op.complete(i, result.Elements[i].Segments, nil)
		}
		cleanup()
	}()
}

func (q *queue) asyncTruncate(wg *sync.WaitGroup, op *truncateOp) {
	wg.Add(1)

	go func() {
		client, err := q.connPool.NextClient()
		if err != nil {
			// No client available
			op.completionFn(nil, err)
			wg.Done()
			return
		}

		ctx, _ := thrift.NewContext(q.opts.GetTruncateRequestTimeout())
		if res, err := client.Truncate(ctx, &op.request); err != nil {
			op.completionFn(nil, err)
		} else {
			op.completionFn(res, nil)
		}

		wg.Done()
	}()
}

func (q *queue) Len() int {
	q.RLock()
	v := q.opsSumSize
	q.RUnlock()
	return v
}

func (q *queue) Enqueue(o op) error {
	var needsDrain []op
	q.Lock()
	if q.state != stateOpen {
		q.Unlock()
		return errQueueNotOpen
	}
	q.ops = append(q.ops, o)
	q.opsSumSize += o.Size()
	// If queue is full flush
	if q.opsSumSize >= q.size {
		needsDrain = q.rotateOpsWithLock()
	}
	// Need to hold lock while writing to the drainIn
	// channel to ensure it has not been closed
	if len(needsDrain) != 0 {
		q.drainIn <- needsDrain
	}
	q.Unlock()
	return nil
}

func (q *queue) Host() topology.Host {
	return q.host
}

func (q *queue) GetConnectionCount() int {
	return q.connPool.GetConnectionCount()
}

func (q *queue) GetConnectionPool() connectionPool {
	return q.connPool
}

func (q *queue) Close() {
	q.Lock()
	if q.state != stateOpen {
		q.Unlock()
		return
	}

	q.state = stateClosed
	// Closed drainIn channel in lock to ensure writers know
	// consistently if channel is open or not by checking state
	close(q.drainIn)
	q.Unlock()
}
