package storage

import (
	"container/list"
	"io"
	"math"
	"sync"
	"time"

	"code.uber.internal/infra/memtsdb"
	xtime "code.uber.internal/infra/memtsdb/x/time"
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
		unit xtime.Unit,
		annotation []byte,
	) error

	fetchEncodedSegments(id string, start, end time.Time) (io.Reader, error)

	bootstrap(writeStart time.Time) error
}

type dbShard struct {
	sync.RWMutex
	opts                  memtsdb.DatabaseOptions
	shard                 uint32
	lookup                map[string]*dbShardEntry
	list                  *list.List
	bs                    bootstrapState
	newSeriesBootstrapped bool
}

type dbShardEntry struct {
	series databaseSeries
	elem   *list.Element
}

func newDatabaseShard(shard uint32, opts memtsdb.DatabaseOptions) databaseShard {
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
	unit xtime.Unit,
	annotation []byte,
) error {
	series := s.getSeries(id)
	return series.write(timestamp, value, unit, annotation)
}

func (s *dbShard) fetchEncodedSegments(id string, start, end time.Time) (io.Reader, error) {
	s.RLock()
	entry, exists := s.lookup[id]
	s.RUnlock()
	if !exists {
		return nil, nil
	}
	return entry.series.fetchEncodedSegments(id, start, end)
}

func (s *dbShard) getSeries(id string) databaseSeries {
	s.RLock()
	newSeriesBootstrapped := s.newSeriesBootstrapped
	entry, exists := s.lookup[id]
	s.RUnlock()
	if exists {
		return entry.series
	}

	s.Lock()
	entry, exists = s.lookup[id]
	if exists {
		s.Unlock()
		// During Rlock -> Wlock promotion the entry was inserted
		return entry.series
	}
	series := newDatabaseSeries(id, newSeriesBootstrapped, s.opts)
	elem := s.list.PushBack(series)
	s.lookup[id] = &dbShardEntry{series, elem}
	s.Unlock()

	return series
}

func (s *dbShard) bootstrap(writeStart time.Time) error {
	if success, err := tryBootstrap(&s.RWMutex, &s.bs, "shard"); !success {
		return err
	}

	s.Lock()
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
		series := s.getSeries(id)
		if err := series.bootstrap(dbBlocks); err != nil {
			return err
		}
	}

	// from this point onwards, all newly created series that aren't in
	// the existing map should be considered bootstrapped because they
	// have no data within the retention period.
	s.Lock()
	s.newSeriesBootstrapped = true
	s.Unlock()

	// find the series with no data within the retention period but has
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

	// finally bootstrapping series with no recent data.
	for _, series := range bufferedSeries {
		if err := series.bootstrap(nil); err != nil {
			return err
		}
	}

	s.Lock()
	s.bs = bootstrapped
	s.Unlock()

	return nil
}
