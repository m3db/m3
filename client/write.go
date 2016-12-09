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

	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/topology"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/pool"
)

var (
	writeOpZeroed writeOp
)

type writeOp struct {
	namespace    ts.ID
	shardID      uint32
	request      rpc.WriteBatchRawRequestElement
	datapoint    rpc.Datapoint
	completionFn completionFn
}

func (w *writeOp) reset() {
	*w = writeOpZeroed
	w.request.Datapoint = &w.datapoint
}

func (w *writeOp) Size() int {
	// Writes always represent a single write
	return 1
}

func (w *writeOp) CompletionFn() completionFn {
	return w.completionFn
}

type writeOpPool interface {
	// Init pool
	Init()

	// Get a write op
	Get() *writeOp

	// Put a write op
	Put(w *writeOp)
}

type poolOfWriteOp struct {
	pool pool.ObjectPool
}

func newWriteOpPool(opts pool.ObjectPoolOptions) writeOpPool {
	p := pool.NewObjectPool(opts)
	return &poolOfWriteOp{p}
}

func (p *poolOfWriteOp) Init() {
	p.pool.Init(func() interface{} {
		w := &writeOp{}
		w.reset()
		return w
	})
}

func (p *poolOfWriteOp) Get() *writeOp {
	w := p.pool.Get().(*writeOp)
	return w
}

func (p *poolOfWriteOp) Put(w *writeOp) {
	w.reset()
	p.pool.Put(w)
}

type writeState struct {
	sync.Cond
	sync.Mutex
	refCounter

	session           *session
	op                *writeOp
	majority, pending int32
	halfSuccess       int32
	topoMap           topology.Map
	errors            []error

	queues []hostQueue
}

func (w *writeState) reset() {
	// Set refCounter completion as close
	w.destructorFn = w.close
	// Set the embedded condition locker to the embedded mutex
	w.L = w
}

func (w *writeState) close() {
	w.session.writeOpPool.Put(w.op)

	w.op, w.majority, w.pending, w.halfSuccess = nil, 0, 0, 0
	w.errors = w.errors[:0]
	w.queues = w.queues[:0]

	w.session.writeStatePool.Put(w)
}

// todo@bl - modify comment on what happens to inflight writes
func (w *writeState) completionFn(result interface{}, err error) {
	var (
		successful int32
		remaining  = atomic.AddInt32(&w.pending, -1)
		hostID     = result.(string)
	)

	if err != nil {
		w.Lock()
		w.errors = append(w.errors, err)
		w.Unlock()
	} else {
		// NB(bl) When bootstrapping, we count writes to the initializing and
		// leaving nodes as a single success towards quorum, and only if both
		// writes succeed.

		hostShardSet, ok := w.topoMap.LookupHostShardSet(hostID)
		if !ok {
			panic("Missing host shard set") // todo@bl: handle this gracefully
		}
		shardState := hostShardSet.ShardSet().LookupState(w.op.shardID)

		var addToSuccess int32

		// NB(bl): We use one variable, halfSuccess, to avoid atomically reads
		// to and from multiple vars. success = halfSuccess/2

		// each successful write to an available node increases halfSuccess by 2
		// (i.e. increases success by one); each successful write to an
		// initializing or leaving node increases halfSuccess by 1 (i.e. success
		// goes up by one only if both nodes are written to, and doesn't
		// increase if only one or the other is written to)

		if shardState == shard.Available {
			addToSuccess = 2
		} else {
			addToSuccess = 1
		}
		successful = atomic.AddInt32(&w.halfSuccess, addToSuccess) / 2
	}

	wLevel := w.session.writeLevel

	if remaining == 0 ||
		(wLevel == topology.ConsistencyLevelOne && successful > 0) ||
		(wLevel == topology.ConsistencyLevelMajority && successful >= w.majority) {
		w.Lock()
		w.Signal()
		w.Unlock()
	}

	w.decRef()
}

func (w *writeState) successful() int32 {
	return atomic.LoadInt32(&w.halfSuccess) / 2
}
