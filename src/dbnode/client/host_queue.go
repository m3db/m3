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
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"

	"github.com/uber/tchannel-go/thrift"
)

type queue struct {
	sync.WaitGroup
	sync.RWMutex

	opts                                       Options
	nowFn                                      clock.NowFn
	host                                       topology.Host
	connPool                                   connectionPool
	writeBatchRawRequestPool                   writeBatchRawRequestPool
	writeBatchRawRequestElementArrayPool       writeBatchRawRequestElementArrayPool
	writeTaggedBatchRawRequestPool             writeTaggedBatchRawRequestPool
	writeTaggedBatchRawRequestElementArrayPool writeTaggedBatchRawRequestElementArrayPool
	size                                       int
	ops                                        []op
	opsSumSize                                 int
	opsLastRotatedAt                           time.Time
	opsArrayPool                               *opArrayPool
	drainIn                                    chan []op
	status                                     status
}

func newHostQueue(
	host topology.Host,
	hostQueueOpts hostQueueOpts,
) hostQueue {
	opts := hostQueueOpts.opts
	scope := opts.InstrumentOptions().MetricsScope().
		SubScope("hostqueue").
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
		opts:                                       opts,
		nowFn:                                      opts.ClockOptions().NowFn(),
		host:                                       host,
		connPool:                                   newConnectionPool(host, opts),
		writeBatchRawRequestPool:                   hostQueueOpts.writeBatchRawRequestPool,
		writeBatchRawRequestElementArrayPool:       hostQueueOpts.writeBatchRawRequestElementArrayPool,
		writeTaggedBatchRawRequestPool:             hostQueueOpts.writeTaggedBatchRawRequestPool,
		writeTaggedBatchRawRequestElementArrayPool: hostQueueOpts.writeTaggedBatchRawRequestElementArrayPool,
		size:         size,
		ops:          opArrayPool.Get(),
		opsArrayPool: opArrayPool,
		drainIn:      make(chan []op, opsArraysLen),
	}
}

func (q *queue) Open() {
	q.Lock()
	defer q.Unlock()

	if q.status != statusNotOpen {
		return
	}

	q.status = statusOpen

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
		if q.status != statusOpen {
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
		if q.status != statusOpen {
			q.Unlock()
			return
		}
		needsDrain := q.rotateOpsWithLock()
		// Need to hold lock while writing to the drainIn
		// channel to ensure it has not been closed
		if len(needsDrain) != 0 {
			q.drainIn <- needsDrain
		}
		q.Unlock()
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
		currWriteOpsByNamespace       namespaceWriteBatchOpsSlice
		currTaggedWriteOpsByNamespace namespaceWriteTaggedBatchOpsSlice
		writeBatchSize                = q.opts.WriteBatchSize()
	)

	for ops := range q.drainIn {
		opsLen := len(ops)
		for i := 0; i < opsLen; i++ {
			switch v := ops[i].(type) {
			case *writeOperation:
				namespace := v.namespace
				idx := currWriteOpsByNamespace.indexOf(namespace)
				if idx == -1 {
					value := namespaceWriteBatchOps{
						namespace:                            namespace,
						opsArrayPool:                         q.opsArrayPool,
						writeBatchRawRequestElementArrayPool: q.writeBatchRawRequestElementArrayPool,
					}
					idx = len(currWriteOpsByNamespace)
					currWriteOpsByNamespace = append(currWriteOpsByNamespace, value)
				}

				currWriteOpsByNamespace.appendAt(idx, ops[i], &v.request)

				if currWriteOpsByNamespace.lenAt(idx) == writeBatchSize {
					// Reached write batch limit, write async and reset
					q.asyncWrite(namespace, currWriteOpsByNamespace[idx].ops,
						currWriteOpsByNamespace[idx].elems)
					currWriteOpsByNamespace.resetAt(idx)
				}
			case *writeTaggedOperation:
				namespace := v.namespace
				idx := currTaggedWriteOpsByNamespace.indexOf(namespace)
				if idx == -1 {
					value := namespaceWriteTaggedBatchOps{
						namespace:                                  namespace,
						opsArrayPool:                               q.opsArrayPool,
						writeTaggedBatchRawRequestElementArrayPool: q.writeTaggedBatchRawRequestElementArrayPool,
					}
					idx = len(currTaggedWriteOpsByNamespace)
					currTaggedWriteOpsByNamespace = append(currTaggedWriteOpsByNamespace, value)
				}

				currTaggedWriteOpsByNamespace.appendAt(idx, ops[i], &v.request)

				if currTaggedWriteOpsByNamespace.lenAt(idx) == writeBatchSize {
					// Reached write batch limit, write async and reset
					q.asyncTaggedWrite(namespace, currTaggedWriteOpsByNamespace[idx].ops,
						currTaggedWriteOpsByNamespace[idx].elems)
					currTaggedWriteOpsByNamespace.resetAt(idx)
				}
			case *fetchBatchOp:
				q.asyncFetch(v)
			case *fetchTaggedOp:
				q.asyncFetchTagged(v)
			case *truncateOp:
				q.asyncTruncate(v)
			default:
				completionFn := ops[i].CompletionFn()
				completionFn(nil, errQueueUnknownOperation(q.host.ID()))
			}
		}

		// If any outstanding write ops, async write
		for i, writeOps := range currWriteOpsByNamespace {
			if len(writeOps.ops) > 0 {
				q.asyncWrite(writeOps.namespace, writeOps.ops,
					writeOps.elems)
			}
			// Zero the element
			currWriteOpsByNamespace[i] = namespaceWriteBatchOps{}
		}
		// Reset the slice
		currWriteOpsByNamespace = currWriteOpsByNamespace[:0]

		// If any outstanding tagged write ops, async write
		for i, writeOps := range currTaggedWriteOpsByNamespace {
			if len(writeOps.ops) > 0 {
				q.asyncTaggedWrite(writeOps.namespace, writeOps.ops,
					writeOps.elems)
			}
			// Zero the element
			currTaggedWriteOpsByNamespace[i] = namespaceWriteTaggedBatchOps{}
		}
		// Reset the slice
		currTaggedWriteOpsByNamespace = currTaggedWriteOpsByNamespace[:0]

		if ops != nil {
			q.opsArrayPool.Put(ops)
		}
	}

	// Close the connection pool after all requests done
	q.Wait()
	q.connPool.Close()
}

func (q *queue) asyncTaggedWrite(
	namespace ident.ID,
	ops []op,
	elems []*rpc.WriteTaggedBatchRawRequestElement,
) {
	q.Add(1)
	// TODO(r): Use a worker pool to avoid creating new go routines for async writes
	go func() {
		req := q.writeTaggedBatchRawRequestPool.Get()
		req.NameSpace = namespace.Bytes()
		req.Elements = elems

		// NB(r): Defer is slow in the hot path unfortunately
		cleanup := func() {
			q.writeTaggedBatchRawRequestPool.Put(req)
			q.writeTaggedBatchRawRequestElementArrayPool.Put(elems)
			q.opsArrayPool.Put(ops)
			q.Done()
		}

		// NB(bl): host is passed to writeState to determine the state of the
		// shard on the node we're writing to

		client, err := q.connPool.NextClient()
		if err != nil {
			// No client available
			callAllCompletionFns(ops, q.host, err)
			cleanup()
			return
		}

		ctx, _ := thrift.NewContext(q.opts.WriteRequestTimeout())
		err = client.WriteTaggedBatchRaw(ctx, req)
		if err == nil {
			// All succeeded
			callAllCompletionFns(ops, q.host, nil)
			cleanup()
			return
		}

		if batchErrs, ok := err.(*rpc.WriteBatchRawErrors); ok {
			// Callback all writes with errors
			hasErr := make(map[int]struct{})
			for _, batchErr := range batchErrs.Errors {
				op := ops[batchErr.Index]
				op.CompletionFn()(q.host, batchErr.Err)
				hasErr[int(batchErr.Index)] = struct{}{}
			}
			// Callback all writes with no errors
			for i := range ops {
				if _, ok := hasErr[i]; !ok {
					// No error
					ops[i].CompletionFn()(q.host, nil)
				}
			}
			cleanup()
			return
		}

		// Entire batch failed
		callAllCompletionFns(ops, q.host, err)
		cleanup()
	}()
}

func (q *queue) asyncWrite(
	namespace ident.ID,
	ops []op,
	elems []*rpc.WriteBatchRawRequestElement,
) {
	q.Add(1)
	// TODO(r): Use a worker pool to avoid creating new go routines for async writes
	go func() {
		req := q.writeBatchRawRequestPool.Get()
		req.NameSpace = namespace.Bytes()
		req.Elements = elems

		// NB(r): Defer is slow in the hot path unfortunately
		cleanup := func() {
			q.writeBatchRawRequestPool.Put(req)
			q.writeBatchRawRequestElementArrayPool.Put(elems)
			q.opsArrayPool.Put(ops)
			q.Done()
		}

		// NB(bl): host is passed to writeState to determine the state of the
		// shard on the node we're writing to

		client, err := q.connPool.NextClient()
		if err != nil {
			// No client available
			callAllCompletionFns(ops, q.host, err)
			cleanup()
			return
		}

		ctx, _ := thrift.NewContext(q.opts.WriteRequestTimeout())
		err = client.WriteBatchRaw(ctx, req)
		if err == nil {
			// All succeeded
			callAllCompletionFns(ops, q.host, nil)
			cleanup()
			return
		}

		if batchErrs, ok := err.(*rpc.WriteBatchRawErrors); ok {
			// Callback all writes with errors
			hasErr := make(map[int]struct{})
			for _, batchErr := range batchErrs.Errors {
				op := ops[batchErr.Index]
				op.CompletionFn()(q.host, batchErr.Err)
				hasErr[int(batchErr.Index)] = struct{}{}
			}
			// Callback all writes with no errors
			for i := range ops {
				if _, ok := hasErr[i]; !ok {
					// No error
					ops[i].CompletionFn()(q.host, nil)
				}
			}
			cleanup()
			return
		}

		// Entire batch failed
		callAllCompletionFns(ops, q.host, err)
		cleanup()
	}()
}

func (q *queue) asyncFetch(op *fetchBatchOp) {
	q.Add(1)
	// TODO(r): Use a worker pool to avoid creating new go routines for async fetches
	go func() {
		// NB(r): Defer is slow in the hot path unfortunately
		cleanup := func() {
			op.DecRef()
			op.Finalize()
			q.Done()
		}

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
		opLen := op.Size()
		for i := 0; i < opLen; i++ {
			if !(i < resultLen) {
				// No results for this entry, in practice should never occur
				op.complete(i, nil, errQueueFetchNoResponse(q.host.ID()))
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

func (q *queue) asyncFetchTagged(op *fetchTaggedOp) {
	q.Add(1)
	// TODO(r): Use a worker pool to avoid creating new go routines for async fetches
	go func() {
		// NB(r): Defer is slow in the hot path unfortunately
		cleanup := func() {
			op.decRef()
			q.Done()
		}

		client, err := q.connPool.NextClient()
		if err != nil {
			// No client available
			op.CompletionFn()(fetchTaggedResultAccumulatorOpts{host: q.host}, err)
			cleanup()
			return
		}

		ctx, _ := thrift.NewContext(q.opts.FetchRequestTimeout())
		result, err := client.FetchTagged(ctx, &op.request)
		if err != nil {
			op.CompletionFn()(fetchTaggedResultAccumulatorOpts{host: q.host}, err)
			cleanup()
			return
		}

		op.CompletionFn()(fetchTaggedResultAccumulatorOpts{
			host:     q.host,
			response: result,
		}, err)
		cleanup()
	}()
}

func (q *queue) asyncTruncate(op *truncateOp) {
	q.Add(1)

	go func() {
		cleanup := q.Done

		client, err := q.connPool.NextClient()
		if err != nil {
			// No client available
			op.completionFn(nil, err)
			cleanup()
			return
		}

		ctx, _ := thrift.NewContext(q.opts.TruncateRequestTimeout())
		if res, err := client.Truncate(ctx, &op.request); err != nil {
			op.completionFn(nil, err)
		} else {
			op.completionFn(res, nil)
		}

		cleanup()
	}()
}

func (q *queue) Len() int {
	q.RLock()
	v := q.opsSumSize
	q.RUnlock()
	return v
}

func (q *queue) Enqueue(o op) error {
	switch sOp := o.(type) {
	case *fetchBatchOp:
		// Need to take ownership if its a fetch batch op
		sOp.IncRef()
	case *fetchTaggedOp:
		// Need to take ownership if its a fetch tagged op
		sOp.incRef()
	}

	var needsDrain []op
	q.Lock()
	if q.status != statusOpen {
		q.Unlock()
		return errQueueNotOpen(q.host.ID())
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

func (q *queue) BorrowConnection(fn withConnectionFn) error {
	q.RLock()
	if q.status != statusOpen {
		q.RUnlock()
		return errQueueNotOpen(q.host.ID())
	}
	// Add an outstanding operation to avoid connection pool being closed
	q.Add(1)
	defer q.Done()
	q.RUnlock()

	conn, err := q.connPool.NextClient()
	if err != nil {
		return err
	}

	fn(conn)
	return nil
}

func (q *queue) Close() {
	q.Lock()
	if q.status != statusOpen {
		q.Unlock()
		return
	}
	q.status = statusClosed

	// Need to hold lock while writing to the drainIn
	// channel to ensure it has not been closed
	needsDrain := q.rotateOpsWithLock()
	if len(needsDrain) != 0 {
		q.drainIn <- needsDrain
	}

	// Closed drainIn channel in lock to ensure writers know
	// consistently if channel is open or not by checking state
	close(q.drainIn)
	q.Unlock()
}

// errors

func errQueueNotOpen(hostID string) error {
	return fmt.Errorf("host operation queue not open for host: %s", hostID)
}

func errQueueUnknownOperation(hostID string) error {
	return fmt.Errorf("host operation queue received unknown operation for host: %s", hostID)
}

func errQueueFetchNoResponse(hostID string) error {
	return fmt.Errorf("host operation queue did not receive response for given fetch for host: %s", hostID)
}

// ops container types

type namespaceWriteBatchOps struct {
	namespace                            ident.ID
	opsArrayPool                         *opArrayPool
	writeBatchRawRequestElementArrayPool writeBatchRawRequestElementArrayPool
	ops                                  []op
	elems                                []*rpc.WriteBatchRawRequestElement
}

type namespaceWriteBatchOpsSlice []namespaceWriteBatchOps

func (s namespaceWriteBatchOpsSlice) indexOf(
	namespace ident.ID,
) int {
	idx := -1
	for i := range s {
		if s[i].namespace.Equal(namespace) {
			return i
		}
	}
	return idx
}

func (s namespaceWriteBatchOpsSlice) appendAt(
	index int,
	op op,
	elem *rpc.WriteBatchRawRequestElement,
) {
	if s[index].ops == nil {
		s[index].ops = s[index].opsArrayPool.Get()
	}
	if s[index].elems == nil {
		s[index].elems = s[index].writeBatchRawRequestElementArrayPool.Get()
	}
	s[index].ops = append(s[index].ops, op)
	s[index].elems = append(s[index].elems, elem)
}

func (s namespaceWriteBatchOpsSlice) lenAt(
	index int,
) int {
	return len(s[index].ops)
}

func (s namespaceWriteBatchOpsSlice) resetAt(
	index int,
) {
	s[index].ops = nil
	s[index].elems = nil
}

// TODO: use genny to make namespaceWriteBatchOps and namespaceWriteTaggedBatchOps
// share code (https://github.com/m3db/m3/src/dbnode/issues/531)
type namespaceWriteTaggedBatchOps struct {
	namespace                                  ident.ID
	opsArrayPool                               *opArrayPool
	writeTaggedBatchRawRequestElementArrayPool writeTaggedBatchRawRequestElementArrayPool
	ops                                        []op
	elems                                      []*rpc.WriteTaggedBatchRawRequestElement
}

type namespaceWriteTaggedBatchOpsSlice []namespaceWriteTaggedBatchOps

func (s namespaceWriteTaggedBatchOpsSlice) indexOf(
	namespace ident.ID,
) int {
	idx := -1
	for i := range s {
		if s[i].namespace.Equal(namespace) {
			return i
		}
	}
	return idx
}

func (s namespaceWriteTaggedBatchOpsSlice) appendAt(
	index int,
	op op,
	elem *rpc.WriteTaggedBatchRawRequestElement,
) {
	if s[index].ops == nil {
		s[index].ops = s[index].opsArrayPool.Get()
	}
	if s[index].elems == nil {
		s[index].elems = s[index].writeTaggedBatchRawRequestElementArrayPool.Get()
	}
	s[index].ops = append(s[index].ops, op)
	s[index].elems = append(s[index].elems, elem)
}

func (s namespaceWriteTaggedBatchOpsSlice) lenAt(
	index int,
) int {
	return len(s[index].ops)
}

func (s namespaceWriteTaggedBatchOpsSlice) resetAt(
	index int,
) {
	s[index].ops = nil
	s[index].elems = nil
}
