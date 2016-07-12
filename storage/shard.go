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
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/persist/fs"
	xtime "github.com/m3db/m3db/x/time"
)

const (
	shardIterateBatchPercent = 0.05
)

type databaseShard interface {
	ShardNum() uint32

	// Tick performs any updates to ensure series drain their buffers and blocks are written to disk, etc
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
	) (m3db.ReaderSliceReader, error)

	Bootstrap(writeStart time.Time, cutover time.Time) error

	// FlushToDisk flushes the data blocks in the shard to disk
	FlushToDisk(ctx m3db.Context, blockStart time.Time) error
}

type dbShard struct {
	sync.RWMutex
	opts                  m3db.DatabaseOptions
	shard                 uint32
	lookup                map[string]*dbShardEntry
	list                  *list.List
	bs                    bootstrapState
	newSeriesBootstrapped bool
	flushWriter           m3db.FileSetWriter
}

type dbShardEntry struct {
	series     databaseSeries
	elem       *list.Element
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

func newDatabaseShard(shard uint32, opts m3db.DatabaseOptions) databaseShard {
	return &dbShard{
		opts:        opts,
		shard:       shard,
		lookup:      make(map[string]*dbShardEntry),
		list:        list.New(),
		flushWriter: opts.GetNewFileSetWriterFn()(opts.GetBlockSize(), opts.GetFilePathPrefix()),
	}
}

func (s *dbShard) ShardNum() uint32 {
	return s.shard
}

type databaseSeriesFn func(series databaseSeries) error

func (s *dbShard) forEachSeries(continueOnError bool, seriesFn databaseSeriesFn) error {
	// NB(r): consider using a lockless list for ticking
	s.RLock()
	elemsLen := s.list.Len()
	batchSize := int(math.Ceil(shardIterateBatchPercent * float64(elemsLen)))
	nextElem := s.list.Front()
	s.RUnlock()

	for nextElem != nil {
		var err error
		s.RLock()
		if nextElem, err = s.forBatchWithLock(continueOnError, nextElem, batchSize, seriesFn); err != nil && !continueOnError {
			s.RUnlock()
			return err
		}
		s.RUnlock()
	}
	return nil
}

func (s *dbShard) forBatchWithLock(
	continueOnError bool,
	elem *list.Element,
	batchSize int,
	seriesFn databaseSeriesFn,
) (*list.Element, error) {
	var nextElem *list.Element
	for ticked := 0; ticked < batchSize && elem != nil; ticked++ {
		nextElem = elem.Next()
		series := elem.Value.(databaseSeries)
		if err := seriesFn(series); err != nil && !continueOnError {
			return nil, err
		}
		elem = nextElem
	}
	return nextElem, nil
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
	series, completionFn := s.writableSeries(id)
	err := series.Write(ctx, timestamp, value, unit, annotation)
	completionFn()
	return err
}

func (s *dbShard) ReadEncoded(
	ctx m3db.Context,
	id string,
	start, end time.Time,
) (m3db.ReaderSliceReader, error) {
	s.RLock()
	entry, exists := s.lookup[id]
	s.RUnlock()
	if !exists {
		return nil, nil
	}
	return entry.series.ReadEncoded(ctx, start, end)
}

func (s *dbShard) writableSeries(id string) (databaseSeries, writeCompletionFn) {
	s.RLock()
	if entry, exists := s.lookup[id]; exists {
		entry.incrementWriterCount()
		s.RUnlock()
		return entry.series, entry.decrementWriterCount
	}
	s.RUnlock()

	s.Lock()
	if entry, exists := s.lookup[id]; exists {
		entry.incrementWriterCount()
		s.Unlock()
		// During Rlock -> Wlock promotion the entry was inserted
		return entry.series, entry.decrementWriterCount
	}
	bs := bootstrapNotStarted
	if s.newSeriesBootstrapped {
		bs = bootstrapped
	}
	series := newDatabaseSeries(id, bs, s.opts)
	elem := s.list.PushBack(series)
	entry := &dbShardEntry{series: series, elem: elem, curWriters: 1}
	s.lookup[id] = entry
	s.Unlock()

	return entry.series, entry.decrementWriterCount
}

func (s *dbShard) Bootstrap(writeStart time.Time, cutover time.Time) error {
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

	bootstrapFn := s.opts.GetBootstrapFn()
	bs := bootstrapFn()
	sr, err := bs.Run(writeStart, s.shard)
	if err != nil {
		return err
	}

	bootstrappedSeries := sr.GetAllSeries()
	for id, dbBlocks := range bootstrappedSeries {
		series, completionFn := s.writableSeries(id)
		err := series.Bootstrap(dbBlocks, cutover)
		completionFn()
		if err != nil {
			return err
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
	var bufferedSeries []databaseSeries
	s.RLock()
	for id, entry := range s.lookup {
		if _, exists := bootstrappedSeries[id]; !exists {
			bufferedSeries = append(bufferedSeries, entry.series)
		}
	}
	s.RUnlock()

	// Finally bootstrapping series with no recent data.
	for _, series := range bufferedSeries {
		if err := series.Bootstrap(nil, cutover); err != nil {
			return err
		}
	}

	s.Lock()
	s.bs = bootstrapped
	s.Unlock()

	return nil
}

func (s *dbShard) FlushToDisk(ctx m3db.Context, blockStart time.Time) error {
	// We don't flush data when the shard is still bootstrapping
	s.RLock()
	if s.bs != bootstrapped {
		s.RUnlock()
		return errShardNotBootstrapped
	}
	s.RUnlock()

	// NB(xichen): if the checkpoint file for blockStart already exists, bail.
	// This allows us to retry failed flushing attempts because they wouldn't
	// have created the checkpoint file.
	if fs.FileExistsAt(s.opts.GetFilePathPrefix(), s.shard, blockStart) {
		return nil
	}
	if err := s.flushWriter.Open(s.shard, blockStart); err != nil {
		return err
	}
	defer s.flushWriter.Close()

	var segmentHolder [2][]byte
	return s.forEachSeries(false, func(series databaseSeries) error {
		return series.FlushToDisk(ctx, s.flushWriter, blockStart, segmentHolder[:])
	})
}
