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

package storage

import (
	"errors"
	"sync"

	"github.com/m3db/m3db/storage/index"
	"github.com/m3db/m3db/storage/index/convert"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"

	"github.com/uber-go/tally"
)

var (
	errDbIndexAlreadyClosed       = errors.New("database index has already been closed")
	errDbIndexUnableToWriteClosed = errors.New("unable to write to database index, already closed")
	errDbIndexUnableToQueryClosed = errors.New("unable to query database index, already closed")
)

type nsIndexState byte

const (
	nsIndexStateOpen nsIndexState = iota
	nsIndexStateClosed
)

type nsIndex struct {
	sync.RWMutex
	insertMode index.InsertMode
	state      nsIndexState
	active     index.Block

	insertQueue namespaceIndexInsertQueue
	metrics     nsIndexMetrics
	opts        index.Options
	nsID        ident.ID
}

func newNamespaceIndex(
	md namespace.Metadata,
	newIndexQueueFn newNamespaceIndexInsertQueueFn,
	opts index.Options,
) (namespaceIndex, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	now := opts.ClockOptions().NowFn()()
	indexBlockSize := md.Options().IndexOptions().BlockSize()
	start := now.Truncate(indexBlockSize)
	blk, err := index.NewBlock(start, indexBlockSize, opts)
	if err != nil {
		return nil, err
	}

	idx := &nsIndex{
		insertMode: opts.InsertMode(),
		active:     blk,
		metrics:    newNamespaceIndexMetrics(opts.InstrumentOptions().MetricsScope()),
		opts:       opts,
		nsID:       md.ID(),
	}

	queue := newIndexQueueFn(idx.writeBatch, opts.ClockOptions().NowFn(),
		opts.InstrumentOptions().MetricsScope())
	if err := queue.Start(); err != nil {
		return nil, err
	}
	idx.insertQueue = queue

	return idx, nil
}

// NB(prateek): including the call chains leading to this point:
//
// - For new entry (previously unseen in the shard):
//     shard.WriteTagged()
//       => shardInsertQueue.Insert()
//       => shard.writeBatch()
//       => index.Write()
//       => indexQueue.Insert()
//       => index.writeBatch()
//
// - For entry which exists in the shard, but needs indexing (either past
//   the TTL or the last indexing hasn't happened/failed):
//      shard.WriteTagged()
//        => index.Write()
//        => indexQueue.Insert()
//      	=> index.writeBatch()

func (i *nsIndex) Write(
	id ident.ID,
	tags ident.Tags,
	fns index.OnIndexSeries,
) error {
	d, err := convert.FromMetric(id, tags)
	if err != nil {
		fns.OnIndexFinalize()
		return err
	}

	i.RLock()
	if !i.isOpenWithRLock() {
		i.RUnlock()
		i.metrics.InsertAfterClose.Inc(1)
		fns.OnIndexFinalize()
		return errDbIndexUnableToWriteClosed
	}

	// NB(prateek): retrieving insertMode here while we have the RLock.
	insertMode := i.insertMode
	wg, err := i.insertQueue.Insert(d, fns)
	i.RUnlock()

	if err != nil {
		fns.OnIndexFinalize()
		return err
	}

	// once the write has been queued in the indexInsertQueue, it assumes
	// responsibility for calling the lifecycle resource hooks.
	if insertMode != index.InsertAsync {
		wg.Wait()
	}

	return nil
}

func (i *nsIndex) writeBatch(inserts []index.WriteBatchEntry) error {
	// NB(prateek): we use a read lock to guard against mutation of the
	// nsIndexBlock currently active, mutations within the underlying
	// nsIndexBlock are guarded by primitives internal to it.
	i.RLock()
	defer i.RUnlock()

	if !i.isOpenWithRLock() {
		// NB(prateek): deliberately skip calling any of the `OnIndexFinalize` methods
		// on the provided inserts to terminate quicker during shutdown.
		i.metrics.InsertAfterClose.Inc(int64(len(inserts)))
		return errDbIndexUnableToWriteClosed
	}

	// NB: index.Block assumes responsibility for calling all the OnIndexSeries methods
	// when WriteBatch is called up on it. Both on success and on failure.
	_, err := i.active.WriteBatch(inserts)
	return err
}

func (i *nsIndex) Query(
	ctx context.Context,
	query index.Query,
	opts index.QueryOptions,
) (index.QueryResults, error) {
	i.RLock()
	defer i.RUnlock()
	if !i.isOpenWithRLock() {
		return index.QueryResults{}, errDbIndexUnableToQueryClosed
	}

	results := i.opts.ResultsPool().Get()
	results.Reset(i.nsID)
	ctx.RegisterFinalizer(results)

	// FOLLOWUP(prateek): as part of the runtime options wiring, also set a
	// server side max limit which super-seeds the user requested limit.
	// If we do override the limit, we should log the query and the updated
	// limit to disk. Paranoia can be a good thing.

	exhaustive, err := i.active.Query(query, opts, results)
	return index.QueryResults{
		Results:    results,
		Exhaustive: exhaustive,
	}, err
}

func (i *nsIndex) isOpenWithRLock() bool {
	return i.state == nsIndexStateOpen
}

func (i *nsIndex) Close() error {
	i.Lock()
	defer i.Unlock()
	if !i.isOpenWithRLock() {
		return errDbIndexAlreadyClosed
	}

	i.state = nsIndexStateClosed
	return i.insertQueue.Stop()
}

type nsIndexMetrics struct {
	AsyncInsertErrors tally.Counter
	InsertAfterClose  tally.Counter
	QueryAfterClose   tally.Counter
}

func newNamespaceIndexMetrics(scope tally.Scope) nsIndexMetrics {
	return nsIndexMetrics{
		AsyncInsertErrors: scope.Tagged(map[string]string{
			"error_type": "async-insert",
		}).Counter("index-error"),
		InsertAfterClose: scope.Tagged(map[string]string{
			"error_type": "insert-closed",
		}).Counter("index-error"),
		QueryAfterClose: scope.Tagged(map[string]string{
			"error_type": "query-closed",
		}).Counter("index-error"),
	}
}
