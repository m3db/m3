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
	"time"

	"github.com/m3db/m3db/storage/index"
	"github.com/m3db/m3db/storage/index/convert"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3ninx/doc"
	m3ninxindex "github.com/m3db/m3ninx/index"
	"github.com/m3db/m3ninx/index/segment"
	"github.com/m3db/m3ninx/index/segment/mem"
	"github.com/m3db/m3ninx/postings"
	"github.com/m3db/m3ninx/search/executor"
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

type nsIndexBlock struct {
	segment    segment.MutableSegment
	expiryTime time.Time
}

type nsIndex struct {
	sync.RWMutex
	insertMode index.InsertMode
	state      nsIndexState
	active     nsIndexBlock

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
	postingsOffset := postings.ID(0) // FOLLOWUP(prateek): compute based on block compaction
	seg, err := mem.NewSegment(postingsOffset, opts.MemSegmentOptions())
	if err != nil {
		return nil, err
	}

	expiryTime := now.Add(md.Options().RetentionOptions().RetentionPeriod())
	idx := &nsIndex{
		insertMode: opts.InsertMode(),
		active: nsIndexBlock{
			segment:    seg,
			expiryTime: expiryTime, // FOLLOWUP(prateek): compute based on block rotation
		},
		metrics: newNamespaceIndexMetrics(opts.InstrumentOptions().MetricsScope()),
		opts:    opts,
		nsID:    md.ID(),
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
	fns onIndexSeries,
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

func (i *nsIndex) writeBatch(inserts []nsIndexInsert) error {
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

	// TODO(prateek): docs array pooling
	batch := m3ninxindex.Batch{
		Docs:                make([]doc.Document, 0, len(inserts)),
		AllowPartialUpdates: true,
	}
	for _, insert := range inserts {
		batch.Docs = append(batch.Docs, insert.doc)
	}

	err := i.active.segment.InsertBatch(batch)
	if err == nil {
		for idx, insert := range inserts {
			insert.fns.OnIndexSuccess(i.active.expiryTime)
			insert.fns.OnIndexFinalize()
			batch.Docs[idx] = doc.Document{}
		}
		return nil
	}

	partialErr, ok := err.(*m3ninxindex.BatchPartialError)
	if !ok {
		// should never happen
		i.opts.InstrumentOptions().Logger().Errorf(
			"[invariant violated] received non BatchPartialError from m3ninx InsertBatch. %T", err)
		// NB: marking all the inserts as failure, cause we don't know which ones failed
		for _, insert := range inserts {
			insert.fns.OnIndexFinalize()
			insert.doc = doc.Document{}
			insert.fns = nil
		}
		return nil
	}

	// first finalize all the responses which were errors, and mark them
	// nil to indicate they're done.
	numErr := len(partialErr.Indices())
	for _, idx := range partialErr.Indices() {
		inserts[idx].fns.OnIndexFinalize()
		inserts[idx].fns = nil
		inserts[idx].doc = doc.Document{}
	}

	// mark all non-error inserts success, so we don't repeatedly index them,
	// and then finalize any held references.
	for _, insert := range inserts {
		if insert.fns == nil {
			continue
		}
		insert.fns.OnIndexSuccess(i.active.expiryTime)
		insert.fns.OnIndexFinalize()
		insert.fns = nil
		insert.doc = doc.Document{}
	}

	if numErr != 0 {
		i.metrics.AsyncInsertErrors.Inc(int64(numErr))
	}

	return nil
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

	reader, err := i.active.segment.Reader()
	if err != nil {
		return index.QueryResults{}, err
	}

	readers := []m3ninxindex.Reader{reader}
	exec := executor.NewExecutor(readers)

	// FOLLOWUP(prateek): push down QueryOptions to restrict results
	iter, err := exec.Execute(query.Query.SearchQuery())
	if err != nil {
		exec.Close()
		return index.QueryResults{}, err
	}

	var (
		results    = i.opts.ResultsPool().Get()
		size       = results.Size()
		brokeEarly = false
	)
	results.Reset(i.nsID)
	ctx.RegisterFinalizer(results)

	execCloser := safeCloser{closable: exec}
	iterCloser := safeCloser{closable: iter}

	defer func() {
		iterCloser.Close()
		execCloser.Close()
	}()

	for iter.Next() {
		// FOLLOWUP(prateek): as part of the runtime options wiring, also set a server side max limit which
		// super-seeds the user requested limit. Paranoia can be a good thing.
		if opts.Limit > 0 && size >= opts.Limit {
			brokeEarly = true
			break
		}
		d := iter.Current()
		_, size, err = results.Add(d)
		if err != nil {
			return index.QueryResults{}, err
		}
	}

	if err := iter.Err(); err != nil {
		return index.QueryResults{}, err
	}

	if err := iterCloser.Close(); err != nil {
		return index.QueryResults{}, err
	}

	if err := execCloser.Close(); err != nil {
		return index.QueryResults{}, err
	}

	exhaustive := !brokeEarly
	return index.QueryResults{
		Results:    results,
		Exhaustive: exhaustive,
	}, nil
}

type safeCloser struct {
	closable
	closed bool
}

func (c *safeCloser) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	return c.closable.Close()
}

type closable interface {
	Close() error
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
