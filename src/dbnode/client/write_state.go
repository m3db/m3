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

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/topology"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	"github.com/uber-go/tally"
)

const (
	reuseWriteBytesTooLong = 4096
)

// writeOp represents a generic write operation
type writeOp interface {
	op

	ShardID() uint32

	SetOpCallback(cb opCallback)

	Close()
}

type opCallback interface {
	OpComplete(result interface{}, err error)
}

type writeState struct {
	sync.Cond
	sync.Mutex
	refCounter

	consistencyLevel topology.ConsistencyLevel
	topoMap          topology.Map
	op               writeOp
	nsBackingIdentID *ident.ReuseableBytesID
	// nsIdentID is to save iface cast each time take ref to above.
	nsIdentID        ident.ID
	tsBackingIdentID *ident.ReuseableBytesID
	// tsIdentID is to save iface cast each time take ref to above.
	tsIdentID                   ident.ID
	majority, enqueued, pending int32
	success                     int32
	errors                      []error
	batchBuff                   *writeBatchBytes

	opCallback opCallback

	queues []hostQueue
	pool   *writeStatePool
}

func newWriteState(
	pool *writeStatePool,
) *writeState {
	w := &writeState{
		pool:             pool,
		nsBackingIdentID: ident.NewReuseableBytesID(),
		tsBackingIdentID: ident.NewReuseableBytesID(),
	}
	w.nsIdentID = w.nsBackingIdentID
	w.tsIdentID = w.tsBackingIdentID
	w.destructorFn = w.close
	w.L = w
	w.opCallback = w
	return w
}

func (w *writeState) close() {
	w.op.Close()

	w.nsBackingIdentID.Reset(nil)
	w.tsBackingIdentID.Reset(nil)
	if w.batchBuff != nil {
		w.batchBuff.decRef()
	}

	w.op, w.majority, w.enqueued, w.pending, w.success = nil, 0, 0, 0, 0
	w.batchBuff = nil

	for i := range w.errors {
		w.errors[i] = nil
	}
	w.errors = w.errors[:0]

	for i := range w.queues {
		w.queues[i] = nil
	}
	w.queues = w.queues[:0]

	if w.pool == nil {
		return
	}
	w.pool.Put(w)
}

func (w *writeState) setNamespaceID(nsID []byte) {
	w.nsBackingIdentID.Reset(nsID)
}

func (w *writeState) setID(id []byte) {
	w.tsBackingIdentID.Reset(id)
}

func (w *writeState) OpComplete(result interface{}, err error) {
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
	} else if shardState, err := hostShardSet.ShardSet().LookupStateByID(w.op.ShardID()); err != nil {
		errStr := "missing shard %d in host %s"
		wErr = xerrors.NewRetryableError(fmt.Errorf(errStr, w.op.ShardID(), hostID))
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
		wErr = xerrors.NewRetryableError(fmt.Errorf(errStr, w.op.ShardID(), hostID))
	} else {
		w.success++
	}

	if wErr != nil {
		w.errors = append(w.errors, wErr)
	}

	switch w.consistencyLevel {
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

func tryReuseBytes(b []byte) []byte {
	if len(b) >= reuseWriteBytesTooLong {
		return nil
	}
	// The size will get reset when used.
	return b
}

type writeStatePool struct {
	pool pool.ObjectPool
	// todo: remove
	gets tally.Counter
	puts tally.Counter
}

func newWriteStatePool(
	opts pool.ObjectPoolOptions,
) *writeStatePool {
	p := pool.NewObjectPool(opts)
	return &writeStatePool{
		pool: p,
		gets: opts.InstrumentOptions().MetricsScope().Counter("pooling-write-state-gets"),
		puts: opts.InstrumentOptions().MetricsScope().Counter("pooling-write-state-puts"),
	}
}

func (p *writeStatePool) Init() {
	p.pool.Init(func() interface{} {
		return newWriteState(p)
	})
}

func (p *writeStatePool) Get() *writeState {
	p.gets.Inc(1)
	return p.pool.Get().(*writeState)
}

func (p *writeStatePool) Put(w *writeState) {
	p.puts.Inc(1)
	p.pool.Put(w)
}
