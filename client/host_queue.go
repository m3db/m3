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
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/pool"

	"github.com/uber/tchannel-go/thrift"
)

var (
	errQueueNotOpen          = errors.New("host operation queue not open")
	errQueueUnknownOperation = errors.New("host operation queue received unknown operation")
	errQueueFetchNoResponse  = errors.New("host operation queue did not receive response for given fetch")
)

type queue struct {
	sync.RWMutex

	opts                                 Options
	nowFn                                clock.NowFn
	host                                 topology.Host
	connPool                             connectionPool
	writeBatchRawRequestPool             writeBatchRawRequestPool
	writeBatchRawRequestElementArrayPool writeBatchRawRequestElementArrayPool
	size                                 int
	ops                                  []op
	opsSumSize                           int
	opsLastRotatedAt                     time.Time
	opsArrayPool                         opArrayPool
	drainIn                              chan []op
	state                                state
	//	shardStates                          map[uint32]shardState
}

func newHostQueue(
	host topology.Host,
	writeBatchRawRequestPool writeBatchRawRequestPool,
	writeBatchRawRequestElementArrayPool writeBatchRawRequestElementArrayPool,
	opts Options,
) hostQueue {
	scope := opts.InstrumentOptions().MetricsScope().SubScope("hostqueue").
		Tagged(map[string]string{
			"hostID": host.ID(),
		})

	opts = opts.SetInstrumentOptions(opts.InstrumentOptions().SetMetricsScope(scope))

	size := opts.HostQueueOpsFlushSize()

	opsArraysLen := opts.HostQueueOpsArrayPoolSize()
	opArrayPoolOpts := pool.NewObjectPoolOptions().
		SetSize(opsArraysLen).
		SetInstrumentOptions(opts.InstrumentOptions().SetMetricsScope(
			scope.SubScope("op-array-pool"),
		))
	opArrayPoolCapacity := int(math.Max(float64(size), float64(opts.WriteBatchSize())))
	opArrayPool := newOpArrayPool(opArrayPoolOpts, opArrayPoolCapacity)
	opArrayPool.Init()

	return &queue{
		opts:                                 opts,
		nowFn:                                opts.ClockOptions().NowFn(),
		host:                                 host,
		connPool:                             newConnectionPool(host, opts),
		writeBatchRawRequestPool:             writeBatchRawRequestPool,
		writeBatchRawRequestElementArrayPool: writeBatchRawRequestElementArrayPool,
		size:         size,
		ops:          opArrayPool.Get(),
		opsArrayPool: opArrayPool,
		drainIn:      make(chan []op, opsArraysLen),
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

	flushInterval := q.opts.HostQueueOpsFlushInterval()
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
		wgAll                        = &sync.WaitGroup{}
		currWriteOpsByNamespace      = make(map[ts.Hash][]op)
		currBatchElementsByNamespace = make(map[ts.Hash][]*rpc.WriteBatchRawRequestElement)
		writeBatchSize               = q.opts.WriteBatchSize()
	)

	for ops := range q.drainIn {
		ops := ops
		var (
			currWriteOps      []op
			currBatchElements []*rpc.WriteBatchRawRequestElement
			opsLen            = len(ops)
		)

		for i := 0; i < opsLen; i++ {
			switch v := ops[i].(type) {
			case *writeOp:
				namespace := v.namespace
				hash := namespace.Hash()
				currWriteOps = currWriteOpsByNamespace[hash]
				currBatchElements = currBatchElementsByNamespace[hash]
				if currWriteOps == nil {
					currWriteOps = q.opsArrayPool.Get()
					currBatchElements = q.writeBatchRawRequestElementArrayPool.Get()
				}

				currWriteOps = append(currWriteOps, ops[i])
				currBatchElements = append(currBatchElements, &v.request)
				currWriteOpsByNamespace[hash] = currWriteOps
				currBatchElementsByNamespace[hash] = currBatchElements

				if len(currWriteOps) == writeBatchSize {
					// Reached write batch limit, write async and reset
					q.asyncWrite(wgAll, namespace, currWriteOps, currBatchElements)
					currWriteOpsByNamespace[hash] = nil
					currBatchElementsByNamespace[hash] = nil
				}
			case *fetchBatchOp:
				q.asyncFetch(wgAll, v)
			case *truncateOp:
				q.asyncTruncate(wgAll, v)
			default:
				completionFn := ops[i].CompletionFn()
				completionFn(nil, errQueueUnknownOperation)
			}
		}

		// If any outstanding write ops, async write
		for _, writeOps := range currWriteOpsByNamespace {
			if len(writeOps) > 0 {
				namespace := writeOps[0].(*writeOp).namespace
				hash := namespace.Hash()
				q.asyncWrite(wgAll, namespace, writeOps, currBatchElementsByNamespace[hash])
				currWriteOpsByNamespace[hash] = nil
				currBatchElementsByNamespace[hash] = nil
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

func (q *queue) asyncWrite(
	wg *sync.WaitGroup, namespace ts.ID, ops []op,
	elems []*rpc.WriteBatchRawRequestElement,
) {
	wg.Add(1)
	// TODO(r): Use a worker pool to avoid creating new go routines for async writes
	go func() {
		req := q.writeBatchRawRequestPool.Get()
		req.NameSpace = namespace.Data()
		req.Elements = elems

		// NB(r): Defer is slow in the hot path unfortunately
		cleanup := func() {
			q.writeBatchRawRequestPool.Put(req)
			q.writeBatchRawRequestElementArrayPool.Put(elems)
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

		ctx, _ := thrift.NewContext(q.opts.WriteRequestTimeout())
		err = client.WriteBatchRaw(ctx, req)
		// todo@bl - insert shard state here?
		if err == nil {
			// All succeeded
			callAllCompletionFns(ops, nil, nil)
			cleanup()
			return
		}

		if batchErrs, ok := err.(*rpc.WriteBatchRawErrors); ok {
			// Callback all writes with errors
			hasErr := make(map[int]struct{})
			for _, batchErr := range batchErrs.Errors {
				op := ops[batchErr.Index]
				op.CompletionFn()(nil, batchErr.Err)
				hasErr[int(batchErr.Index)] = struct{}{}
			}
			// Callback all writes with no errors
			for i := range ops {
				if _, ok := hasErr[i]; !ok {
					// No error
					ops[i].CompletionFn()(nil, nil)
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

		ctx, _ := thrift.NewContext(q.opts.FetchRequestTimeout())
		result, err := client.FetchBatchRaw(ctx, &op.request)
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

		ctx, _ := thrift.NewContext(q.opts.TruncateRequestTimeout())
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

func (q *queue) ConnectionCount() int {
	return q.connPool.ConnectionCount()
}

func (q *queue) ConnectionPool() connectionPool {
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
