package storage

import (
	"container/list"
	"math"
	"sync"
	"time"
)

const (
	shardTickBatchPercent = 0.05
)

type databaseShard interface {
	// tick performs any updates to ensure series flush their buffers and blocks are written to disk, etc
	tick()

	write(
		id string,
		timestamp time.Time,
		value float64,
		unit time.Duration,
		annotation []byte,
	) error

	fetchEncodedSegments(id string, start, end time.Time) ([][]byte, error)
}

type dbShard struct {
	sync.RWMutex
	opts   DatabaseOptions
	shard  uint32
	lookup map[string]*dbShardEntry
	list   *list.List
}

type dbShardEntry struct {
	series databaseSeries
	elem   *list.Element
}

func newDatabaseShard(shard uint32, opts DatabaseOptions) databaseShard {
	return &dbShard{
		opts:   opts,
		shard:  shard,
		lookup: make(map[string]*dbShardEntry),
		list:   list.New(),
	}
}

func (s *dbShard) tick() {
	// NB(r): consider using a lockless list for ticking
	s.RLock()
	elemsLen := s.list.Len()
	tickBatchSize := int(math.Ceil(shardTickBatchPercent * float64(elemsLen)))

	// Perform first batch of ticks while we already have the Rlock
	nextElem, expired := s.tickBatchWithLock(s.list.Front(), tickBatchSize)
	s.RUnlock()

	// Now interleave using the Rlock for the rest of the elements
	for nextElem != nil {
		var batchExpired []databaseSeries
		s.RLock()
		nextElem, batchExpired = s.tickBatchWithLock(nextElem, tickBatchSize)
		s.RUnlock()
		if len(batchExpired) > 0 {
			expired = append(expired, batchExpired...)
		}
	}

	if len(expired) == 0 {
		return
	}

	// Remove all expired series from lookup and list
	s.Lock()
	for _, series := range expired {
		id := series.id()
		entry := s.lookup[id]
		s.list.Remove(entry.elem)
		delete(s.lookup, id)
	}
	s.Unlock()
}

func (s *dbShard) tickBatchWithLock(elem *list.Element, batch int) (*list.Element, []databaseSeries) {
	var nextElem *list.Element
	var expired []databaseSeries
	for ticked := 0; ticked < batch && elem != nil; ticked++ {
		nextElem = elem.Next()
		series := elem.Value.(databaseSeries)
		if err := series.tick(); err != nil {
			if err == errSeriesAllDatapointsExpired {
				expired = append(expired, series)
			} else {
				// TODO(r): log error and increment counter
			}
		}
		elem = nextElem
	}
	return nextElem, expired
}

func (s *dbShard) write(
	id string,
	timestamp time.Time,
	value float64,
	unit time.Duration,
	annotation []byte,
) error {
	s.RLock()
	entry, exists := s.lookup[id]
	s.RUnlock()
	if exists {
		return entry.series.write(timestamp, value, unit, annotation)
	}

	s.Lock()
	entry, exists = s.lookup[id]
	if exists {
		s.Unlock()
		// During Rlock -> Wlock promotion the entry was inserted
		return entry.series.write(timestamp, value, unit, annotation)
	}
	series := newDatabaseSeries(id, s.opts)
	elem := s.list.PushBack(series)
	s.lookup[id] = &dbShardEntry{series, elem}
	s.Unlock()
	return series.write(timestamp, value, unit, annotation)
}

func (s *dbShard) fetchEncodedSegments(id string, start, end time.Time) ([][]byte, error) {
	s.RLock()
	entry, exists := s.lookup[id]
	s.RUnlock()
	if !exists {
		return nil, nil
	}
	return entry.series.fetchEncodedSegments(id, start, end)
}
