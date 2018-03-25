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
	"bytes"
	"errors"
	"sync"
	"time"

	"github.com/m3db/m3db/storage/index"
	"github.com/m3db/m3ninx/doc"
	"github.com/m3db/m3ninx/index/segment/mem"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"

	"github.com/uber-go/tally"
)

var (
	maxTime = time.Unix(1<<63-1, 0)

	errDbIndexAlreadyClosed                      = errors.New("database index has already been closed")
	errDbIndexUnableToWriteClosed                = errors.New("unable to write to database index, already closed")
	errDbIndexUnableToQueryClosed                = errors.New("unable to query database index, already closed")
	errDbIndexUnableToIndexWithReservedFieldName = errors.New("unable to index document due to usage of reserved fieldname")
)

type dbIndexState byte

const (
	dbIndexStateOpen dbIndexState = iota
	dbIndexStateClosed
)

type dbIndexInsertMode byte

// nolint
const (
	dbIndexInsertSync dbIndexInsertMode = iota
	dbIndexInsertAsync
)

type dbIndexBlock struct {
	segment    mem.Segment
	expiryTime time.Time
}

type dbIndex struct {
	sync.RWMutex
	insertMode dbIndexInsertMode
	state      dbIndexState
	active     dbIndexBlock

	insertQueue databaseIndexInsertQueue
	metrics     dbIndexMetrics
	opts        index.Options
}

// nolint: deadcode
func newDatabaseIndex(
	fn newDatabaseIndexInsertQueueFn,
	opts index.Options,
) (databaseIndex, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	seg, err := mem.New(1, opts.MemSegmentOptions())
	if err != nil {
		return nil, err
	}

	idx := &dbIndex{
		insertMode: dbIndexInsertAsync,
		active: dbIndexBlock{
			segment:    seg,
			expiryTime: maxTime, // FOLLOWUP(prateek): undo hard-coding to infinite retention
		},
		metrics: newDatabaseIndexMetrics(opts.InstrumentOptions().MetricsScope()),
		opts:    opts,
	}

	queue := fn(idx.writeBatch,
		opts.ClockOptions().NowFn(),
		opts.InstrumentOptions().MetricsScope())
	if err := queue.Start(); err != nil {
		return nil, err
	}
	idx.insertQueue = queue

	return idx, nil
}

func (i *dbIndex) writeBatch(inserts []dbIndexInsert) error {
	// NB(prateek): we use a read lock to guard against mutation of the
	// dbIndexBlock currently active, mutations within the underlying
	// dbIndexBlock are guarded by primitives internal to it.
	i.RLock()
	defer i.RUnlock()

	if !i.isOpenWithRLock() {
		// NB(prateek): skip calling any of the `OnIndexFinalize` methods
		// on the provided inserts to terminate quicker during shutdown.
		i.metrics.InsertAfterClose.Inc(int64(len(inserts)))
		return errDbIndexUnableToWriteClosed
	}

	var numErr int64
	for _, insert := range inserts {
		// FOLLOWUP(prateek): need to query before insert to ensure no duplicates && add test
		err := i.active.segment.Insert(insert.doc)
		if err != nil {
			numErr++
		} else {
			insert.fns.OnIndexSuccess(i.active.expiryTime)
		}
		// NB: we need to release held resources so we un-conditionally execute the Finalize.
		insert.fns.OnIndexFinalize()
	}

	if numErr != 0 {
		i.metrics.AsyncInsertErrors.Inc(numErr)
	}

	return nil
}

func (i *dbIndex) Write(
	namespace ident.ID,
	id ident.ID,
	tags ident.Tags,
	fns indexInsertLifecycleHooks,
) error {
	d, err := i.doc(namespace, id, tags)
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

	async := i.insertMode == dbIndexInsertAsync
	wg, err := i.insertQueue.Insert(d, fns)
	i.RUnlock()

	if err != nil {
		fns.OnIndexFinalize()
		return err
	}

	// once the write has been queued in the indexInsertQueue, it assumes
	// responsibility for calling the lifecycle resource hooks.

	if !async {
		wg.Wait()
	}

	return nil
}

func (i *dbIndex) Query(
	ctx context.Context,
	query index.Query,
	opts index.QueryOptions,
) (index.QueryResults, error) {
	i.RLock()
	if !i.isOpenWithRLock() {
		i.RUnlock()
		return index.QueryResults{}, errDbIndexUnableToQueryClosed
	}

	// FOLLOWUP(prateek): push down QueryOptions to restrict results
	iter, err := i.active.segment.Query(query.Query)
	i.RUnlock()
	if err != nil {
		return index.QueryResults{}, err
	}

	return index.QueryResults{
		Iterator:   index.NewIterator(iter, i.opts),
		Exhaustive: true,
	}, nil
}

func (i *dbIndex) isOpenWithRLock() bool {
	return i.state == dbIndexStateOpen

}

func (i *dbIndex) Close() error {
	i.Lock()
	defer i.Unlock()
	state := i.state
	if state != dbIndexStateOpen {
		return errDbIndexAlreadyClosed
	}

	i.state = dbIndexStateClosed
	return i.insertQueue.Stop()
}

func (i *dbIndex) doc(ns, id ident.ID, tags ident.Tags) (doc.Document, error) {
	fields := make([]doc.Field, 0, 1+len(tags))
	fields = append(fields, doc.Field{
		Name:      index.ReservedFieldNameNamespace,
		Value:     i.clone(ns),
		ValueType: doc.StringValueType,
	})
	for j := 0; j < len(tags); j++ {
		t := tags[j]
		name := i.clone(t.Name)
		if bytes.Equal(name, index.ReservedFieldNameNamespace) {
			return doc.Document{}, errDbIndexUnableToIndexWithReservedFieldName
		}
		fields = append(fields, doc.Field{
			Name:      name,
			Value:     i.clone(t.Value),
			ValueType: doc.StringValueType,
		})
	}
	return doc.Document{
		ID:     i.clone(id),
		Fields: fields,
	}, nil
}

// NB(prateek): we take an independent copy of the bytes underlying
// any ids provided, as we need to maintain the lifecycle of the indexed
// bytes separately from the rest of the storage subsystem.
func (i *dbIndex) clone(id ident.ID) []byte {
	original := id.Data().Get()
	clone := make([]byte, len(original))
	copy(clone, original)
	return clone
}

type dbIndexMetrics struct {
	AsyncInsertErrors tally.Counter
	InsertAfterClose  tally.Counter
	QueryAfterClose   tally.Counter
}

func newDatabaseIndexMetrics(scope tally.Scope) dbIndexMetrics {
	return dbIndexMetrics{
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
