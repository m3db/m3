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
	"sync"
	"time"

	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/serialize"
	"github.com/m3db/m3db/storage/index"
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

	op                   *fetchTaggedOp
	tagResultAccumulator fetchResultsAccumulator
	err                  error
	done                 bool

	pool fetchStatePool
}

func newFetchState(pool fetchStatePool) *fetchState {
	f := &fetchState{
		tagResultAccumulator: &fetchTaggedResultAccumulator{},
		pool:                 pool,
	}
	f.destructorFn = f.close // Set refCounter completion as close
	f.L = f                  // Set the embedded condition locker to the embedded mutex
	return f
}

func (f *fetchState) close() {
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
	op *fetchTaggedOp,
	topoMap topology.Map,
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

	done, err := f.tagResultAccumulator.Add(result, resultErr)
	if done {
		f.err = err
		f.done = true
		f.Signal()
	}
}

func (f *fetchState) asIndexQueryResults(pools fetchTaggedPools) (index.QueryResults, error) {
	f.Lock()
	defer f.Unlock()

	if !f.done {
		return index.QueryResults{}, errFetchStateStillProcessing
	}

	if err := f.err; err != nil {
		return index.QueryResults{}, err
	}

	limit := maxInt
	if l := f.op.request.Limit; l != nil {
		limit = int(*l)
	}

	return f.tagResultAccumulator.AsIndexQueryResults(limit, pools)
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

	limit := maxInt
	if l := f.op.request.Limit; l != nil {
		limit = int(*l)
	}

	return f.tagResultAccumulator.AsEncodingSeriesIterators(limit, pools)
}

type fetchTaggedPools interface {
	ReaderSliceOfSlicesIterator() *readerSliceOfSlicesIteratorPool
	MultiReaderIterator() encoding.MultiReaderIteratorPool
	SeriesIterator() encoding.SeriesIteratorPool
	MutableSeriesIterators() encoding.MutableSeriesIteratorsPool
	IteratorArray() encoding.IteratorArrayPool
	ID() ident.Pool
	CheckedBytesWrapper() xpool.CheckedBytesWrapperPool
	TagDecoder() serialize.TagDecoderPool
}
