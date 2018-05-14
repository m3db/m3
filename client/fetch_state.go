// Copyright (c) 2018 Uber Technologies, Inc.
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
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/serialize"
	"github.com/m3db/m3db/topology"
	"github.com/m3db/m3db/x/xpool"
	"github.com/m3db/m3x/ident"
)

const (
	maxUint = ^uint(0)
	maxInt  = int(maxUint >> 1)
)

var (
	errFetchStateStillProcessing = errors.New("[invariant violated] fetch state is still processing, unable to create response")
)

type fetchState struct {
	sync.Cond
	sync.Mutex
	refCounter

	nsID                 ident.ID
	op                   *fetchTaggedOp
	tagResultAccumulator fetchTaggedResultAccumulator
	err                  error
	done                 bool

	pool fetchStatePool
}

func newFetchState(pool fetchStatePool) *fetchState {
	f := &fetchState{
		tagResultAccumulator: newFetchTaggedResultAccumulator(),
		pool:                 pool,
	}
	f.destructorFn = f.close // Set refCounter completion as close
	f.L = f                  // Set the embedded condition locker to the embedded mutex
	return f
}

func (f *fetchState) close() {
	if f.nsID != nil {
		f.nsID.Finalize()
		f.nsID = nil
	}
	if f.op != nil {
		f.op.decRef()
		f.op = nil
	}
	f.err = nil
	f.done = false
	f.tagResultAccumulator.Clear()

	if f.pool == nil {
		return
	}
	f.pool.Put(f)
}

func (f *fetchState) Reset(
	startTime time.Time,
	endTime time.Time,
	op *fetchTaggedOp, topoMap topology.Map,
	majority int,
	consistencyLevel topology.ReadConsistencyLevel,
) {
	op.incRef() // take a reference to the provided op
	f.op = op
	f.tagResultAccumulator.Reset(startTime, endTime, topoMap, majority, consistencyLevel)
}

func (f *fetchState) completionFn(
	result interface{},
	resultErr error,
) {
	f.Lock()
	defer func() {
		f.Unlock()
		f.decRef() // release ref held onto by the hostQueue (via op.completionFn)
	}()

	if f.done {
		// i.e. we've already failed, no need to continue processing any additional
		// responses we receive
		return
	}

	opts, ok := result.(fetchTaggedResultAccumulatorOpts)
	if !ok {
		// should never happen
		f.markDoneWithLock(fmt.Errorf(
			"[invariant violated] expected result to be of type fetchTaggedResultAccumulatorOpts, received: %v", result))
		return
	}

	done, err := f.tagResultAccumulator.Add(opts, resultErr)
	if done {
		f.markDoneWithLock(err)
	}
}

func (f *fetchState) markDoneWithLock(err error) {
	f.done = true
	f.err = err
	f.Signal()
}

func (f *fetchState) asTaggedIDsIterator(pools fetchTaggedPools) (TaggedIDsIterator, bool, error) {
	f.Lock()
	defer f.Unlock()

	if !f.done {
		return nil, false, errFetchStateStillProcessing
	}

	if err := f.err; err != nil {
		return nil, false, err
	}

	limit := f.op.requestLimit(maxInt)
	return f.tagResultAccumulator.AsTaggedIDsIterator(limit, pools)
}

func (f *fetchState) asEncodingSeriesIterators(pools fetchTaggedPools) (encoding.SeriesIterators, bool, error) {
	f.Lock()
	defer f.Unlock()

	if !f.done {
		return nil, false, errFetchStateStillProcessing
	}

	if err := f.err; err != nil {
		return nil, false, err
	}

	limit := f.op.requestLimit(maxInt)
	return f.tagResultAccumulator.AsEncodingSeriesIterators(limit, pools)
}

// NB(prateek): this is backed by the sessionPools struct, but we're restricting it to a narrow
// interface to force the fetchTagged code-paths to be explicit about the pools they need access
// to. The alternative is to either expose the sessionPools struct (which is a worse abstraction),
// or make a new concrete implemtation (which requires an extra alloc). Chosing the best of the
// three options and leaving as the interface below.
type fetchTaggedPools interface {
	IteratorPools
	ReaderSliceOfSlicesIterator() *readerSliceOfSlicesIteratorPool
	MutableSeriesIterators() encoding.MutableSeriesIteratorsPool
	MultiReaderIteratorArray() encoding.MultiReaderIteratorArrayPool
	ID() ident.Pool
	CheckedBytesWrapper() xpool.CheckedBytesWrapperPool
	TagDecoder() serialize.TagDecoderPool
}

// IteratorPools exposes a small subset of iterator pools that are sufficient for clients
// to rebuild SeriesIterator
type IteratorPools interface {
	MultiReaderIterator() encoding.MultiReaderIteratorPool
	SeriesIterator() encoding.SeriesIteratorPool
}
