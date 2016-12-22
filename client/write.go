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

	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/topology"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/pool"
)

var (
	writeOpZeroed = writeOp{shardID: math.MaxUint32}
	// NB(bl): use an invalid shardID for the zeroed op
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
	success           int32
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

	w.op, w.majority, w.pending, w.success = nil, 0, 0, 0

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

func (w *writeState) completionFn(result interface{}, err error) {
	hostID := result.(string)
	// NB(bl) panic on invalid result, it indicates a bug in the code

	w.Lock()
	w.pending--

	if err != nil {
		w.errors = append(w.errors, fmt.Errorf("error writing to host %s: %v", hostID, err))
	} else if hostShardSet, ok := w.topoMap.LookupHostShardSet(hostID); !ok {
		w.errors = append(w.errors, fmt.Errorf("missing host shard in writeState completionFn: %s", hostID))
	} else if shardState, err :=
		hostShardSet.ShardSet().LookupStateByID(w.op.shardID); err != nil {
		w.errors = append(w.errors, fmt.Errorf("missing shard %d in host %s", w.op.shardID, hostID))
	} else if shardState == shard.Available {
		// NB(bl): only count writes to available shards towards success
		w.success++
	}

	switch w.session.writeLevel {
	case topology.ConsistencyLevelOne:
		if w.success > 0 || w.pending == 0 {
			w.Signal()
		}
	case topology.ConsistencyLevelMajority:
		if w.success > w.majority || w.pending == 0 {
			w.Signal()
		}
	case topology.ConsistencyLevelAll:
		if w.pending == 0 {
			w.Signal()
		}
	}

	w.Unlock()
	w.decRef()
}
