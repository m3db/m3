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

package storage

import (
	"container/list"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3db/interfaces/m3db"
	xerrors "github.com/m3db/m3x/errors"
	xtime "github.com/m3db/m3x/time"
)

const (
	shardIterateBatchPercent = 0.05
)

type databaseShard interface {
	ShardNum() uint32

	// Tick performs any updates to ensure series drain their buffers and blocks are flushed, etc
	Tick()

	Write(
		ctx m3db.Context,
		id string,
		timestamp time.Time,
		value float64,
		unit xtime.Unit,
		annotation []byte,
	) error

	ReadEncoded(
		ctx m3db.Context,
		id string,
		start, end time.Time,
	) ([][]m3db.SegmentReader, error)

	Bootstrap(bs m3db.Bootstrap, writeStart time.Time, cutover time.Time) error

	// Flush flushes the series in this shard.
	Flush(ctx m3db.Context, blockStart time.Time, pm m3db.PersistenceManager) error
}

type dbShard struct {
	sync.RWMutex
	opts                  m3db.DatabaseOptions
	shard                 uint32
	uniqueIndex           uniqueIndex
	writeCommitLogFn      writeCommitLogFn
	lookup                map[string]*dbShardEntry
	list                  *list.List
	bs                    bootstrapState
	newSeriesBootstrapped bool
}

type dbShardEntry struct {
	series      databaseSeries
	uniqueIndex uint64
	elem        *list.Element
	curWriters  int32
}

func (entry *dbShardEntry) writerCount() int32 {
	return atomic.LoadInt32(&entry.curWriters)
}

func (entry *dbShardEntry) incrementWriterCount() {
	atomic.AddInt32(&entry.curWriters, 1)
}

func (entry *dbShardEntry) decrementWriterCount() {
	atomic.AddInt32(&entry.curWriters, -1)
}

type writeCompletionFn func()

type databaseSeriesFn func(series databaseSeries) error

func newDatabaseShard(
	shard uint32,
	uniqueIndex uniqueIndex,
	writeCommitLogFn writeCommitLogFn,
	opts m3db.DatabaseOptions,
) databaseShard {
	return &dbShard{
		opts:             opts,
		shard:            shard,
		uniqueIndex:      uniqueIndex,
		writeCommitLogFn: writeCommitLogFn,
		lookup:           make(map[string]*dbShardEntry),
		list:             list.New(),
	}
}

func (s *dbShard) ShardNum() uint32 {
	return s.shard
}

func (s *dbShard) forEachSeries(continueOnError bool, seriesFn databaseSeriesFn) error {
	// NB(r): consider using a lockless list for ticking
	s.RLock()
	elemsLen := s.list.Len()
	batchSize := int(math.Ceil(shardIterateBatchPercent * float64(elemsLen)))
	nextElem := s.list.Front()
	s.RUnlock()

	// TODO(xichen): pool or cache this.
	curSeries := make([]databaseSeries, 0, batchSize)
	for nextElem != nil {
		s.RLock()
		nextElem = s.forBatchWithLock(nextElem, &curSeries, batchSize)
		s.RUnlock()
		for _, series := range curSeries {
			if err := seriesFn(series); err != nil && !continueOnError {
				return err
			}
		}
	}
	return nil
}

func (s *dbShard) forBatchWithLock(
	elem *list.Element,
	curSeries *[]databaseSeries,
	batchSize int,
) *list.Element {
	var nextElem *list.Element
	*curSeries = (*curSeries)[:0]
	for ticked := 0; ticked < batchSize && elem != nil; ticked++ {
		nextElem = elem.Next()
		series := elem.Value.(databaseSeries)
		*curSeries = append(*curSeries, series)
		elem = nextElem
	}
	return nextElem
}

// tickForEachSeries ticks through each series in the shard and
// returns a list of series that might have expired.
func (s *dbShard) tickForEachSeries() []databaseSeries {
	// TODO(xichen): pool this.
	var expired []databaseSeries

	s.forEachSeries(true, func(series databaseSeries) error {
		err := series.Tick()
		if err == errSeriesAllDatapointsExpired {
			expired = append(expired, series)
		} else if err != nil {
			// TODO(r): log error and increment counter
		}
		return err
	})

	return expired
}

func (s *dbShard) purgeExpiredSeries(expired []databaseSeries) {
	if len(expired) == 0 {
		return
	}

	// Remove all expired series from lookup and list.
	s.Lock()
	for _, series := range expired {
		id := series.ID()
		entry := s.lookup[id]
		// If this series is currently being written to, we don't remove
		// it even though it's empty in that it might become non-empty soon.
		if entry.writerCount() > 0 {
			continue
		}
		// If there have been datapoints written to the series since its
		// last empty check, we don't remove it.
		if !series.Empty() {
			continue
		}
		// NB(xichen): if we get here, we are guaranteed that there can be
		// no more writes to this series while the lock is held, so it's
		// safe to remove it.
		s.list.Remove(entry.elem)
		delete(s.lookup, id)
	}
	s.Unlock()
}

func (s *dbShard) Tick() {
	expired := s.tickForEachSeries()
	s.purgeExpiredSeries(expired)
}

func (s *dbShard) Write(
	ctx m3db.Context,
	id string,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	// Prepare write
	series, idx, completionFn := s.writableSeries(id)

	// Perform write
	err := series.Write(ctx, timestamp, value, unit, annotation)
	completionFn()
	if err != nil {
		return err
	}

	// Write commit log
	info := m3db.CommitLogSeries{
		UniqueIndex: idx,
		ID:          id,
		Shard:       s.shard,
	}
	datapoint := m3db.Datapoint{
		Timestamp: timestamp,
		Value:     value,
	}
	return s.writeCommitLogFn(info, datapoint, unit, annotation)
}

func (s *dbShard) ReadEncoded(
	ctx m3db.Context,
	id string,
	start, end time.Time,
) ([][]m3db.SegmentReader, error) {
	s.RLock()
	entry, exists := s.lookup[id]
	s.RUnlock()
	if !exists {
		return nil, nil
	}
	return entry.series.ReadEncoded(ctx, start, end)
}

func (s *dbShard) writableSeries(id string) (databaseSeries, uint64, writeCompletionFn) {
	s.RLock()
	if entry, exists := s.lookup[id]; exists {
		entry.incrementWriterCount()
		s.RUnlock()
		return entry.series, entry.uniqueIndex, entry.decrementWriterCount
	}
	s.RUnlock()

	s.Lock()
	if entry, exists := s.lookup[id]; exists {
		entry.incrementWriterCount()
		s.Unlock()
		// During Rlock -> Wlock promotion the entry was inserted
		return entry.series, entry.uniqueIndex, entry.decrementWriterCount
	}
	bs := bootstrapNotStarted
	if s.newSeriesBootstrapped {
		bs = bootstrapped
	}
	series := newDatabaseSeries(id, bs, s.opts)
	elem := s.list.PushBack(series)
	entry := &dbShardEntry{
		series:      series,
		uniqueIndex: s.uniqueIndex.nextUniqueIndex(),
		elem:        elem,
		curWriters:  1,
	}
	s.lookup[id] = entry
	s.Unlock()

	return entry.series, entry.uniqueIndex, entry.decrementWriterCount
}

func (s *dbShard) Bootstrap(bs m3db.Bootstrap, writeStart time.Time, cutover time.Time) error {
	s.Lock()
	if s.bs == bootstrapped {
		s.Unlock()
		return nil
	}
	if s.bs == bootstrapping {
		s.Unlock()
		return errShardIsBootstrapping
	}
	s.bs = bootstrapping
	s.Unlock()

	multiErr := xerrors.NewMultiError()
	sr, err := bs.Run(writeStart, s.shard)
	if err != nil {
		renamedErr := fmt.Errorf("error occurred bootstrapping shard %d from external sources: %v", s.shard, err)
		err = xerrors.NewRenamedError(err, renamedErr)
		multiErr = multiErr.Add(err)
	}

	var bootstrappedSeries map[string]m3db.DatabaseSeriesBlocks
	if sr != nil {
		bootstrappedSeries = sr.GetAllSeries()
		for id, dbBlocks := range bootstrappedSeries {
			series, _, completionFn := s.writableSeries(id)
			err := series.Bootstrap(dbBlocks, cutover)
			completionFn()
			multiErr = multiErr.Add(err)
		}
	}

	// From this point onwards, all newly created series that aren't in
	// the existing map should be considered bootstrapped because they
	// have no data within the retention period.
	s.Lock()
	s.newSeriesBootstrapped = true
	s.Unlock()

	// Find the series with no data within the retention period but has
	// buffered data points since server start. Any new series added
	// after this will be marked as bootstrapped.
	s.forEachSeries(true, func(series databaseSeries) error {
		if bootstrappedSeries != nil {
			if _, exists := bootstrappedSeries[series.ID()]; exists {
				return nil
			}
		}
		err := series.Bootstrap(nil, cutover)
		multiErr = multiErr.Add(err)
		return err
	})

	s.Lock()
	s.bs = bootstrapped
	s.Unlock()

	return multiErr.FinalError()
}

func (s *dbShard) Flush(ctx m3db.Context, blockStart time.Time, pm m3db.PersistenceManager) error {
	// We don't flush data when the shard is still bootstrapping
	s.RLock()
	if s.bs != bootstrapped {
		s.RUnlock()
		return errShardNotBootstrapped
	}
	s.RUnlock()

	var multiErr xerrors.MultiError
	prepared, err := pm.Prepare(s.ShardNum(), blockStart)
	multiErr = multiErr.Add(err)

	if prepared.Persist == nil {
		// No action is necessary therefore we bail out early and there is no need to close.
		return multiErr.FinalError()
	}
	defer prepared.Close()

	// If we encounter an error when persisting a series, we continue regardless.
	s.forEachSeries(true, func(series databaseSeries) error {
		err := series.Flush(ctx, blockStart, prepared.Persist)
		multiErr = multiErr.Add(err)
		return err
	})

	return multiErr.FinalError()
}
