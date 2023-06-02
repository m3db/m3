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
	"time"

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/x/checked"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/sampler"
	"github.com/m3db/m3/src/x/serialize"

	"go.uber.org/zap"
)

type countTowardsConsistency int64

const (
	undefined countTowardsConsistency = iota
	available
	leaving
	initializing
	shardLeavingAndLeavingCountsIndividually
	shardLeavingAndLeavingCountsAsPair
	shardInitializingAndInitializingCountsAsPair
)

// writeOp represents a generic write operation
type writeOp interface {
	op

	ShardID() uint32

	SetCompletionFn(fn completionFn)

	Close()
}

type writeState struct {
	sync.Cond
	sync.Mutex
	refCounter

	consistencyLevel                                       topology.ConsistencyLevel
	shardsLeavingCountTowardsConsistency                   bool
	shardsLeavingAndInitializingCountTowardsConsistency    bool
	successAsLeavingAndInitializingCountTowardsConsistency bool
	topoMap                                                topology.Map
	reusableByteID                                         *ident.ReusableBytesID
	hostSuccessList                                        []ident.ID
	op                                                     writeOp
	nsID                                                   ident.ID
	tsID                                                   ident.ID
	tagEncoder                                             serialize.TagEncoder
	annotation                                             checked.Bytes
	majority, pending                                      int32
	success                                                int32
	errors                                                 []error
	lastResetTime                                          time.Time
	queues                                                 []hostQueue
	tagEncoderPool                                         serialize.TagEncoderPool
	pool                                                   *writeStatePool
}

func newWriteState(
	encoderPool serialize.TagEncoderPool,
	pool *writeStatePool,
) *writeState {
	w := &writeState{
		pool:           pool,
		tagEncoderPool: encoderPool,
		reusableByteID: ident.NewReusableBytesID(),
	}
	w.destructorFn = w.close
	w.L = w
	return w
}

func (w *writeState) close() {
	w.op.Close()

	w.nsID.Finalize()
	w.tsID.Finalize()
	w.hostSuccessList = nil
	if w.annotation != nil {
		w.annotation.DecRef()
		w.annotation.Finalize()
	}

	if enc := w.tagEncoder; enc != nil {
		enc.Finalize()
	}

	w.op, w.majority, w.pending, w.success = nil, 0, 0, 0
	w.nsID, w.tsID, w.tagEncoder, w.annotation = nil, nil, nil, nil

	for i := range w.errors {
		w.errors[i] = nil
	}
	w.errors = w.errors[:0]

	w.lastResetTime = time.Time{}

	for i := range w.queues {
		w.queues[i] = nil
	}
	w.queues = w.queues[:0]

	if w.pool == nil {
		return
	}
	w.pool.Put(w)
}

func (w *writeState) completionFn(result interface{}, err error) {
	host := result.(topology.Host)
	hostID := host.ID()
	// NB(bl) panic on invalid result, it indicates a bug in the code

	w.Lock()
	w.pending--

	var (
		took time.Duration
		wErr error
	)
	if !w.lastResetTime.IsZero() {
		took = time.Since(w.lastResetTime)
	}
	if err != nil {
		if IsBadRequestError(err) {
			// Wrap with invalid params and non-retryable so it is
			// not retried.
			err = xerrors.NewInvalidParamsError(err)
			err = xerrors.NewNonRetryableError(err)
		}

		w.pool.MaybeLogHostError(maybeHostWriteError{err: err, host: host, reqRespTime: took})
		wErr = xerrors.NewRenamedError(err, fmt.Errorf("error writing to host %s: %v", hostID, err))
	} else if hostShardSet, ok := w.topoMap.LookupHostShardSet(hostID); !ok {
		errStr := "missing host shard in writeState completionFn: %s"
		wErr = xerrors.NewRetryableError(fmt.Errorf(errStr, hostID))
	} else if shardState, err := hostShardSet.ShardSet().LookupStateByID(w.op.ShardID()); err != nil {
		errStr := "missing shard %d in host %s"
		wErr = xerrors.NewRetryableError(fmt.Errorf(errStr, w.op.ShardID(), hostID))
	} else {
		// NB(bl): Only count writes to available shards towards success.
		// NB(r): If shard is leaving and configured to allow writes to leaving
		// shards to count towards consistency then allow that to count
		// to success
		// If shardsLeavingAndInitializingCountTowardsConsistency is true then write to both leaving and initializing
		switch newCountTowardsConsistency(shardState,
			w.shardsLeavingCountTowardsConsistency,
			w.shardsLeavingAndInitializingCountTowardsConsistency) {
		case available:
			w.success++
		case shardLeavingAndLeavingCountsIndividually:
			w.success++
		case shardLeavingAndLeavingCountsAsPair:
			shard, err := hostShardSet.ShardSet().LookupShard(w.op.ShardID())
			if err != nil {
				errStr := "no shard id %d in host %s"
				wErr = xerrors.NewRetryableError(fmt.Errorf(errStr, w.op.ShardID(), hostID))
			} else {
				// get the initializing host corresponding to the leaving host.
				initializingHostID := shard.DestinationID()
				w.setHostSuccessList(hostID, initializingHostID)
			}
		case shardInitializingAndInitializingCountsAsPair:
			shard, err := hostShardSet.ShardSet().LookupShard(w.op.ShardID())
			if err != nil {
				errStr := "no shard id %d in host %s"
				wErr = xerrors.NewRetryableError(fmt.Errorf(errStr, w.op.ShardID(), hostID))
			} else {
				// get the leaving host corresponding to the initializing host.
				leavingHostID := shard.SourceID()
				w.setHostSuccessList(hostID, leavingHostID)
			}
		case leaving:
			errStr := "shard %d in host %s not available (leaving)"
			wErr = xerrors.NewRetryableError(fmt.Errorf(errStr, w.op.ShardID(), hostID))
		case initializing:
			errStr := "shard %d in host %s is not available (initializing)"
			wErr = xerrors.NewRetryableError(fmt.Errorf(errStr, w.op.ShardID(), hostID))
		default:
			errStr := "shard %d in host %s not available (unknown state)"
			wErr = xerrors.NewRetryableError(fmt.Errorf(errStr, w.op.ShardID(), hostID))
		}
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

func (w *writeState) setHostSuccessList(hostID, pairedHostID string) {
	w.reusableByteID.Reset(ident.StringID(pairedHostID).Bytes())
	if findHost(w.hostSuccessList, w.reusableByteID) {
		w.success++
		w.successAsLeavingAndInitializingCountTowardsConsistency = true
	}
	w.reusableByteID.Reset(ident.StringID(hostID).Bytes())
	w.hostSuccessList = append(w.hostSuccessList, w.reusableByteID)
}

type writeStatePool struct {
	hostSuccessList     []ident.ID
	pool                pool.ObjectPool
	reusableByteID      *ident.ReusableBytesID
	tagEncoderPool      serialize.TagEncoderPool
	logger              *zap.Logger
	logHostErrorSampler *sampler.Sampler
}

func newWriteStatePool(
	tagEncoderPool serialize.TagEncoderPool,
	opts pool.ObjectPoolOptions,
	logger *zap.Logger,
	logHostErrorSampler *sampler.Sampler,
) *writeStatePool {
	p := pool.NewObjectPool(opts)
	return &writeStatePool{
		pool:                p,
		reusableByteID:      ident.NewReusableBytesID(),
		tagEncoderPool:      tagEncoderPool,
		logger:              logger,
		logHostErrorSampler: logHostErrorSampler,
	}
}

func (p *writeStatePool) Init() {
	p.pool.Init(func() interface{} {
		return newWriteState(p.tagEncoderPool, p)
	})
}

func (p *writeStatePool) Get() *writeState {
	return p.pool.Get().(*writeState)
}

func (p *writeStatePool) Put(w *writeState) {
	p.pool.Put(w)
}

func (p *writeStatePool) MaybeLogHostError(hostErr maybeHostWriteError) {
	if hostErr.err == nil {
		// No error, this is an expected code path when host request doesn't
		// encounter an error.
		return
	}

	if !p.logHostErrorSampler.Sample() {
		return
	}

	p.logger.Warn("sampled error writing to host (may not lead to consistency result error)",
		zap.Stringer("host", hostErr.host),
		zap.Duration("reqRespTime", hostErr.reqRespTime),
		zap.Error(hostErr.err))
}

type maybeHostWriteError struct {
	// Note: both these fields should be set always.
	host        topology.Host
	reqRespTime time.Duration

	// Error field is optionally set when there is actually an error.
	err error
}

func newCountTowardsConsistency(shardState shard.State,
	leavingCountsIndividually bool,
	leavingAndInitializingCountsAsPair bool) countTowardsConsistency {
	isAvailable := shardState == shard.Available
	isLeaving := shardState == shard.Leaving
	isInitializing := shardState == shard.Initializing

	if isAvailable {
		return available
	}
	if isLeaving && leavingCountsIndividually {
		return shardLeavingAndLeavingCountsIndividually
	}
	if isLeaving && leavingAndInitializingCountsAsPair {
		return shardLeavingAndLeavingCountsAsPair
	}
	if isInitializing && leavingAndInitializingCountsAsPair {
		return shardInitializingAndInitializingCountsAsPair
	}
	if isLeaving {
		return leaving
	}
	if isInitializing {
		return initializing
	}
	return undefined
}

func findHost(hostSuccessList []ident.ID, hostID ident.ID) bool {
	for _, val := range hostSuccessList {
		if val == hostID {
			return true
		}
	}
	return false
}
