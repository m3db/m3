package storage

import (
	"errors"
	"math"
	"sync"
	"time"
)

const (
	defaultFlushQueueLen = 16
)

var (
	errSeriesAllDatapointsExpired = errors.New("all series datapoints expired")

	timeNone = time.Time{}
)

type databaseSeries interface {
	id() string

	// tick performs any updates to ensure buffer flushes, blocks are written to disk, etc
	tick() error

	write(timestamp time.Time, value float64, unit time.Duration, annotation []byte) error

	fetchEncodedSegments(id string, start, end time.Time) ([][]byte, error)

	isEmpty() bool
}

type dbSeries struct {
	sync.RWMutex
	opts      DatabaseOptions
	nowFn     NowFn
	seriesID  string
	buffer    databaseBuffer
	blocks    *dbSeriesBlocks
	blockSize time.Duration
}

type dbSeriesBlocks struct {
	elems map[time.Time]databaseBlock
	min   time.Time
	max   time.Time
}

func newDbSeriesBlocks() *dbSeriesBlocks {
	return &dbSeriesBlocks{elems: make(map[time.Time]databaseBlock)}
}

func (b *dbSeriesBlocks) insert(block databaseBlock) {
	start := block.startTime()
	if b.min.Equal(timeNone) || start.Before(b.min) {
		b.min = start
	}
	if b.max.Equal(timeNone) || start.After(b.max) {
		b.max = start
	}
	b.elems[start] = block
}

func newDatabaseSeries(id string, opts DatabaseOptions) databaseSeries {
	series := &dbSeries{
		opts:      opts,
		nowFn:     opts.GetNowFn(),
		seriesID:  id,
		blocks:    newDbSeriesBlocks(),
		blockSize: opts.GetBlockSize(),
	}
	series.buffer = newDatabaseBuffer(series.bufferFlushed, opts)
	return series
}

func (s *dbSeries) id() string {
	return s.seriesID
}

func (s *dbSeries) tick() error {
	s.buffer.flushStale()

	// TODO(r): expire and write blocks to disk, when done ensure this is async
	// as ticking through series in a shard is done synchronously

	if s.isEmpty() {
		return errSeriesAllDatapointsExpired
	}
	return nil
}

func (s *dbSeries) isEmpty() bool {
	s.RLock()
	blocksLen := len(s.blocks.elems)
	s.RUnlock()

	if blocksLen == 0 && s.buffer.isEmpty() {
		return true
	}
	return false
}

func (s *dbSeries) write(timestamp time.Time, value float64, unit time.Duration, annotation []byte) error {
	return s.buffer.write(timestamp, value, unit, annotation)
}

func (s *dbSeries) fetchEncodedSegments(id string, start, end time.Time) ([][]byte, error) {
	var results [][]byte

	alignedStart := start.Truncate(s.blockSize)
	alignedEnd := end.Truncate(s.blockSize)
	if alignedEnd.Before(end) {
		// If we did shift back we need to include this block for all data
		alignedEnd = alignedEnd.Add(s.blockSize)
	}

	s.RLock()
	if len(s.blocks.elems) > 0 {
		// Squeeze the lookup window by what's available to make range queries like [0, infinity) possible
		if s.blocks.min.After(alignedStart) {
			alignedStart = s.blocks.min
		}
		if s.blocks.max.Before(alignedEnd) {
			alignedEnd = s.blocks.max
		}
		for blockAt := alignedStart; blockAt.Before(alignedEnd); blockAt = blockAt.Add(s.blockSize) {
			if block, ok := s.blocks.elems[blockAt]; ok {
				results = append(results, block.bytes())
			}
		}
	}
	s.RUnlock()

	bufferResult := s.buffer.fetchEncodedSegment(start, end)
	if bufferResult != nil {
		results = append(results, bufferResult)
	}

	return results, nil
}

func (s *dbSeries) bufferFlushed(flush databaseBufferFlush) {
	blockStart := flush.bucketStart.Truncate(s.blockSize)

	s.Lock()

	block, ok := s.blocks.elems[blockStart]
	if !ok {
		block = newDatabaseBlock(blockStart, s.opts)
		s.blocks.insert(block)
	}

	bucketValuesLen := len(flush.bucketValues)
	for i := 0; i < bucketValuesLen; i++ {
		v := flush.bucketValues[i]
		if !math.IsNaN(v.value) {
			ts := flush.bucketStart.Add(time.Duration(i) * flush.bucketStepSize)
			block.write(ts, v.value, v.unit, v.annotation)
		}
	}

	s.Unlock()
}
