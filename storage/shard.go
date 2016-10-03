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

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/persist/fs/commitlog"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/repair"
	"github.com/m3db/m3db/storage/series"
	"github.com/m3db/m3db/ts"
	xio "github.com/m3db/m3db/x/io"
	xerrors "github.com/m3db/m3x/errors"
	xtime "github.com/m3db/m3x/time"
)

const (
	shardIterateBatchPercent               = 0.01
	expireBatchLength                      = 1024
	blocksMetadataResultMaxInitialCapacity = 1024
	defaultTickSleepIfAheadEvery           = 128
)

type filesetBeforeFn func(filePathPrefix string, namespace string, shardID uint32, t time.Time) ([]string, error)

type dbShard struct {
	sync.RWMutex
	opts                  Options
	nowFn                 clock.NowFn
	shard                 uint32
	increasingIndex       increasingIndex
	seriesPool            series.DatabaseSeriesPool
	writeCommitLogFn      writeCommitLogFn
	lookup                map[string]*list.Element
	list                  *list.List
	bs                    bootstrapState
	newSeriesBootstrapped bool
	filesetBeforeFn       filesetBeforeFn
	deleteFilesFn         deleteFilesFn
	tickSleepIfAheadEvery int
	sleepFn               func(time.Duration)
}

type dbShardEntry struct {
	series     series.DatabaseSeries
	index      uint64
	curWriters int32
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

type dbShardEntryWorkFn func(entry *dbShardEntry) error

type dbShardEntryConditionFn func(entry *dbShardEntry) bool

func newDatabaseShard(
	shard uint32,
	increasingIndex increasingIndex,
	writeCommitLogFn writeCommitLogFn,
	needsBootstrap bool,
	opts Options,
) databaseShard {
	d := &dbShard{
		opts:                  opts,
		nowFn:                 opts.ClockOptions().NowFn(),
		shard:                 shard,
		increasingIndex:       increasingIndex,
		seriesPool:            opts.DatabaseSeriesPool(),
		writeCommitLogFn:      writeCommitLogFn,
		lookup:                make(map[string]*list.Element),
		list:                  list.New(),
		filesetBeforeFn:       fs.FilesetBefore,
		deleteFilesFn:         fs.DeleteFiles,
		tickSleepIfAheadEvery: defaultTickSleepIfAheadEvery,
		sleepFn:               time.Sleep,
	}
	if !needsBootstrap {
		d.bs = bootstrapped
		d.newSeriesBootstrapped = true
	}
	return d
}

func (s *dbShard) ID() uint32 {
	return s.shard
}

func (s *dbShard) NumSeries() int64 {
	s.RLock()
	n := s.list.Len()
	s.RUnlock()
	return int64(n)
}

func (s *dbShard) forEachShardEntry(continueOnError bool, entryFn dbShardEntryWorkFn, stopIterFn dbShardEntryConditionFn) error {
	// NB(r): consider using a lockless list for ticking
	s.RLock()
	elemsLen := s.list.Len()
	batchSize := int(math.Ceil(shardIterateBatchPercent * float64(elemsLen)))
	nextElem := s.list.Front()
	s.RUnlock()

	// TODO(xichen): pool or cache this.
	currEntries := make([]*dbShardEntry, 0, batchSize)
	for nextElem != nil {
		s.RLock()
		nextElem = s.forBatchWithLock(nextElem, &currEntries, batchSize, stopIterFn)
		s.RUnlock()
		for _, entry := range currEntries {
			if err := entryFn(entry); err != nil && !continueOnError {
				return err
			}
		}
	}
	return nil
}

func (s *dbShard) forBatchWithLock(
	elem *list.Element,
	curEntries *[]*dbShardEntry,
	batchSize int,
	stopIterFn dbShardEntryConditionFn,
) *list.Element {
	var nextElem *list.Element
	*curEntries = (*curEntries)[:0]
	for ticked := 0; ticked < batchSize && elem != nil; ticked++ {
		nextElem = elem.Next()
		entry := elem.Value.(*dbShardEntry)
		if stopIterFn != nil && stopIterFn(entry) {
			return nil
		}
		*curEntries = append(*curEntries, entry)
		elem = nextElem
	}
	return nextElem
}

func (s *dbShard) Tick(softDeadline time.Duration) {
	s.tickAndExpire(softDeadline)
}

func (s *dbShard) tickAndExpire(softDeadline time.Duration) int {
	var (
		perEntrySoftDeadline time.Duration
		expired              []series.DatabaseSeries
		i, total             int
	)
	if size := s.NumSeries(); size > 0 {
		perEntrySoftDeadline = softDeadline / time.Duration(size)
	}
	start := s.nowFn()
	s.forEachShardEntry(true, func(entry *dbShardEntry) error {
		if i > 0 && i%s.tickSleepIfAheadEvery == 0 {
			// If we are ahead of our our deadline then throttle tick
			prevEntryDeadline := start.Add(time.Duration(i) * perEntrySoftDeadline)
			if now := s.nowFn(); now.Before(prevEntryDeadline) {
				s.sleepFn(prevEntryDeadline.Sub(now))
			}
		}
		err := entry.series.Tick()
		if err == series.ErrSeriesAllDatapointsExpired {
			expired = append(expired, entry.series)
			if len(expired) >= expireBatchLength {
				// Purge when reaching max batch size to avoid large array growth
				// and ensure smooth rate of elements being returned to pools.
				// This method does not run using a lock so this is safe to
				// perform inline.
				s.purgeExpiredSeries(expired)
				total += len(expired)
				expired = expired[:0]
			}
		} else if err != nil {
			// TODO(r): log error and increment counter
		}
		i++
		return err
	}, nil)

	if len(expired) > 0 {
		// Purge any series that still haven't been purged yet
		s.purgeExpiredSeries(expired)
		total += len(expired)
	}

	return total
}

func (s *dbShard) purgeExpiredSeries(expired []series.DatabaseSeries) {
	// Remove all expired series from lookup and list.
	s.Lock()
	for _, series := range expired {
		id := series.ID()
		entry, elem, exists := s.getEntryWithLock(id)
		if !exists {
			continue
		}
		// If this series is currently being written to, we don't remove
		// it even though it's empty in that it might become non-empty soon.
		if entry.writerCount() > 0 {
			continue
		}
		// If there have been datapoints written to the series since its
		// last empty check, we don't remove it.
		if !series.IsEmpty() {
			continue
		}
		// NB(xichen): if we get here, we are guaranteed that there can be
		// no more writes to this series while the lock is held, so it's
		// safe to remove it.
		series.Close()
		s.list.Remove(elem)
		delete(s.lookup, id)
	}
	s.Unlock()
}

func (s *dbShard) Write(
	ctx context.Context,
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
	info := commitlog.Series{
		UniqueIndex: idx,
		ID:          id,
		Shard:       s.shard,
	}
	datapoint := ts.Datapoint{
		Timestamp: timestamp,
		Value:     value,
	}
	return s.writeCommitLogFn(info, datapoint, unit, annotation)
}

func (s *dbShard) ReadEncoded(
	ctx context.Context,
	id string,
	start, end time.Time,
) ([][]xio.SegmentReader, error) {
	s.RLock()
	entry, _, exists := s.getEntryWithLock(id)
	s.RUnlock()
	if !exists {
		return nil, nil
	}
	return entry.series.ReadEncoded(ctx, start, end)
}

// getEntryWithLock returns the entry for a given id while holding a read lock or a write lock.
func (s *dbShard) getEntryWithLock(id string) (*dbShardEntry, *list.Element, bool) {
	elem, exists := s.lookup[id]
	if !exists {
		return nil, nil, false
	}
	return elem.Value.(*dbShardEntry), elem, true
}

func (s *dbShard) writableSeries(id string) (series.DatabaseSeries, uint64, writeCompletionFn) {
	s.RLock()
	if entry, _, exists := s.getEntryWithLock(id); exists {
		entry.incrementWriterCount()
		s.RUnlock()
		return entry.series, entry.index, entry.decrementWriterCount
	}
	s.RUnlock()

	// Retrieve the entry out of any locks to avoid any possible expensive
	// allocations during any unpooled gets blocking other writers
	series := s.seriesPool.Get()
	series.Reset(id)

	entry := &dbShardEntry{
		series:     series,
		index:      s.increasingIndex.nextIndex(),
		curWriters: 1,
	}

	s.Lock()
	if entry, _, exists := s.getEntryWithLock(id); exists {
		entry.incrementWriterCount()
		s.Unlock()
		// During Rlock -> Wlock promotion the entry was inserted
		return entry.series, entry.index, entry.decrementWriterCount
	}
	if s.newSeriesBootstrapped {
		// Transitioned to new series being bootstrapped
		entry.series.Bootstrap(nil)
	}
	elem := s.list.PushBack(entry)
	s.lookup[id] = elem
	s.Unlock()

	return entry.series, entry.index, entry.decrementWriterCount
}

func (s *dbShard) FetchBlocks(
	ctx context.Context,
	id string,
	starts []time.Time,
) []block.FetchBlockResult {
	s.RLock()
	entry, _, exists := s.getEntryWithLock(id)
	s.RUnlock()
	if !exists {
		return nil
	}
	return entry.series.FetchBlocks(ctx, starts)
}

func (s *dbShard) FetchBlocksMetadata(
	ctx context.Context,
	limit int64,
	pageToken int64,
	includeSizes bool,
	includeChecksums bool,
) ([]block.FetchBlocksMetadataResult, *int64) {
	// Restrict the maximum capacity so we don't over allocate or panic if
	// someone passes in a very large limit
	resCapacity := int(limit)
	if resCapacity > blocksMetadataResultMaxInitialCapacity {
		resCapacity = blocksMetadataResultMaxInitialCapacity
	}

	var (
		res            = make([]block.FetchBlocksMetadataResult, 0, resCapacity)
		pNextPageToken *int64
	)
	s.forEachShardEntry(true, func(entry *dbShardEntry) error {
		if int64(entry.index) < pageToken {
			return nil
		}

		// Create a temporary context here so the stream readers can be returned to
		// pool after we finish fetching the metadata for this series.
		tmpCtx := s.opts.ContextPool().Get()
		blocksMetadata := entry.series.FetchBlocksMetadata(tmpCtx, includeSizes, includeChecksums)
		tmpCtx.Close()
		res = append(res, blocksMetadata)

		return nil
	}, func(entry *dbShardEntry) bool {
		// Break out of the iteration loop once we've accumulated enough entries.
		if int64(len(res)) < limit {
			return false
		}
		nextPageToken := int64(entry.index)
		pNextPageToken = &nextPageToken
		return true
	})

	return res, pNextPageToken
}

func (s *dbShard) Bootstrap(
	bootstrappedSeries map[string]block.DatabaseSeriesBlocks,
	writeStart time.Time,
) error {
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
	for id, dbBlocks := range bootstrappedSeries {
		series, _, completionFn := s.writableSeries(id)
		err := series.Bootstrap(dbBlocks)
		completionFn()
		multiErr = multiErr.Add(err)
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
	s.forEachShardEntry(true, func(entry *dbShardEntry) error {
		series := entry.series
		if series.IsBootstrapped() {
			return nil
		}
		err := series.Bootstrap(nil)
		multiErr = multiErr.Add(err)
		return err
	}, nil)

	s.Lock()
	s.bs = bootstrapped
	s.Unlock()

	return multiErr.FinalError()
}

func (s *dbShard) Flush(
	namespace string,
	blockStart time.Time,
	pm persist.Manager,
) error {
	// We don't flush data when the shard is still bootstrapping
	s.RLock()
	if s.bs != bootstrapped {
		s.RUnlock()
		return errShardNotBootstrapped
	}
	s.RUnlock()

	var multiErr xerrors.MultiError
	prepared, err := pm.Prepare(namespace, s.ID(), blockStart)
	multiErr = multiErr.Add(err)

	if prepared.Persist == nil {
		// No action is necessary therefore we bail out early and there is no need to close.
		return multiErr.FinalError()
	}
	defer prepared.Close()

	// If we encounter an error when persisting a series, we continue regardless.
	s.forEachShardEntry(true, func(entry *dbShardEntry) error {
		series := entry.series
		// Create a temporary context here so the stream readers can be returned to
		// pool after we finish fetching flushing the series
		tmpCtx := s.opts.ContextPool().Get()
		err := series.Flush(tmpCtx, blockStart, prepared.Persist)
		tmpCtx.Close()
		multiErr = multiErr.Add(err)
		return err
	}, nil)

	return multiErr.FinalError()
}

func (s *dbShard) CleanupFileset(namespace string, earliestToRetain time.Time) error {
	filePathPrefix := s.opts.CommitLogOptions().FilesystemOptions().FilePathPrefix()
	multiErr := xerrors.NewMultiError()
	expired, err := s.filesetBeforeFn(filePathPrefix, namespace, s.ID(), earliestToRetain)
	if err != nil {
		detailedErr := fmt.Errorf("encountered errors when getting fileset files for prefix %s namespace %s shard %d: %v", filePathPrefix, namespace, s.ID(), err)
		multiErr = multiErr.Add(detailedErr)
	}
	if err := s.deleteFilesFn(expired); err != nil {
		multiErr = multiErr.Add(err)
	}
	return multiErr.FinalError()
}

func (s *dbShard) Repair(namespace string, repairer databaseShardRepairer) (repair.MetadataComparisonResult, error) {
	return repairer.Repair(namespace, s)
}
