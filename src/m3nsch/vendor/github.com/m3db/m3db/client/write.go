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
	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/topology"
	"github.com/m3db/m3db/ts"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/pool"
)

var (
	writeOpZeroed = writeOp{shardID: math.MaxUint32}
	// NB(bl): use an invalid shardID for the zerod op
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

type writeOpPool struct {
	pool pool.ObjectPool
}

func newWriteOpPool(opts pool.ObjectPoolOptions) *writeOpPool {
	p := pool.NewObjectPool(opts)
	return &writeOpPool{pool: p}
}

func (p *writeOpPool) Init() {
	p.pool.Init(func() interface{} {
		w := &writeOp{}
		w.reset()
		return w
	})
}

func (p *writeOpPool) Get() *writeOp {
	w := p.pool.Get().(*writeOp)
	return w
}

func (p *writeOpPool) Put(w *writeOp) {
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
	ctx               context.Context
	nsID              ts.ID
	tsID              ts.ID
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
	w.op.reset()
	w.session.writeOpPool.Put(w.op)

	w.op, w.majority, w.pending, w.success = nil, 0, 0, 0
	w.nsID, w.tsID = nil, nil

	for i := range w.errors {
		w.errors[i] = nil
	}
	w.errors = w.errors[:0]

	for i := range w.queues {
		w.queues[i] = nil
	}
	w.queues = w.queues[:0]

	w.ctx.BlockingClose()
	w.ctx = nil

	w.session.writeStatePool.Put(w)
}

func (w *writeState) completionFn(result interface{}, err error) {
	hostID := result.(topology.Host).ID()
	// NB(bl) panic on invalid result, it indicates a bug in the code

	w.Lock()
	w.pending--

	var wErr error

	if err != nil {
		wErr = xerrors.NewRenamedError(err, fmt.Errorf("error writing to host %s: %v", hostID, err))
	} else if hostShardSet, ok := w.topoMap.LookupHostShardSet(hostID); !ok {
		errStr := "missing host shard in writeState completionFn: %s"
		wErr = xerrors.NewRetryableError(fmt.Errorf(errStr, hostID))
	} else if shardState, err := hostShardSet.ShardSet().LookupStateByID(w.op.shardID); err != nil {
		errStr := "missing shard %d in host %s"
		wErr = xerrors.NewRetryableError(fmt.Errorf(errStr, w.op.shardID, hostID))
	} else if shardState != shard.Available {
		// NB(bl): only count writes to available shards towards success
		var errStr string
		switch shardState {
		case shard.Initializing:
			errStr = "shard %d in host %s is not available (initializing)"
		case shard.Leaving:
			errStr = "shard %d in host %s not available (leaving)"
		default:
			errStr = "shard %d in host %s not available (unknown state)"
		}
		wErr = xerrors.NewRetryableError(fmt.Errorf(errStr, w.op.shardID, hostID))
	} else {
		w.success++
	}

	if wErr != nil {
		w.errors = append(w.errors, wErr)
	}

	switch w.session.writeLevel {
	case topology.ConsistencyLevelOne:
		if w.success > 0 || w.pending == 0 {
			w.Signal()
		}
	case topology.ConsistencyLevelMajority:
		if w.success >= w.majority || w.pending == 0 {
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

type writeStatePool struct {
	pool    pool.ObjectPool
	session *session
}

func newWriteStatePool(
	session *session,
	opts pool.ObjectPoolOptions,
) *writeStatePool {
	p := pool.NewObjectPool(opts)
	return &writeStatePool{pool: p, session: session}
}

func (p *writeStatePool) Init() {
	p.pool.Init(func() interface{} {
		w := &writeState{session: p.session}
		w.reset()
		return w
	})
}

func (p *writeStatePool) Get() *writeState {
	return p.pool.Get().(*writeState)
}

func (p *writeStatePool) Put(w *writeState) {
	p.pool.Put(w)
}
