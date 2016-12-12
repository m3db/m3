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
	topoMap           topology.Map
	op                *writeOp
	majority, pending int32
	halfSuccess       int32
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
	for i := range w.errors {
		w.errors[i] = nil
	}
	w.errors = w.errors[:0]
	for i := range w.queues {
		w.queues[i] = nil
	}
	w.queues = w.queues[:0]

	w.session.writeStatePool.Put(w)
}

func (w *writeState) addError(err error) {
	w.Lock()
	w.errors = append(w.errors, err)
	w.Unlock()
}

// todo@bl - modify comment on what happens to inflight writes
func (w *writeState) completionFn(result interface{}, err error) {
	var (
		successful int32
		remaining  = atomic.AddInt32(&w.pending, -1)
		hostID     = result.(string)
		// NB(bl) panic on invalid result, it indicates a bug in the code
	)

	if err != nil {
		w.addError(err)
	} else if hostShardSet, ok := w.topoMap.LookupHostShardSet(hostID); !ok {
		w.addError(fmt.Errorf("Missing host shard in writeState completionFn: %s", hostID))
	} else if shardState, err :=
		hostShardSet.ShardSet().LookupStateByID(w.op.shardID); err != nil {
		w.addError(fmt.Errorf("Missing shard %d in host %s", w.op.shardID, hostID))
	} else {
		// NB(bl): We use one variable, halfSuccess, to avoid atomically
		// reading and editing multiple vars. success = halfSuccess/2

		// Each successful write to an available node increases halfSuccess by 2
		// (i.e. increases success by one). Each successful write to an
		// initializing or leaving node increases halfSuccess by 1 (i.e. success
		// goes up by one if and only if both nodes are written to.)

		var addToSuccess int32

		if shardState == shard.Available {
			addToSuccess = 2
		} else {
			addToSuccess = 1
		}

		successful = atomic.AddInt32(&w.halfSuccess, addToSuccess) / 2

	}

	w.Lock()

	if err != nil {
		w.errors = append(w.errors, err)
	}

	switch w.session.writeLevel {
	case topology.ConsistencyLevelOne:
		if successful > 0 || remaining == 0 {
			w.Signal()
		}
	case topology.ConsistencyLevelMajority:
		if successful >= w.majority || remaining == 0 {
			w.Signal()
		}
	case topology.ConsistencyLevelAll:
		if remaining == 0 {
			w.Signal()
		}
	}

	w.Unlock()
	w.decRef()
}

func (w *writeState) successful() int32 {
	return atomic.LoadInt32(&w.halfSuccess) / 2
}
