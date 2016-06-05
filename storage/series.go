package storage

import (
	"errors"
	"io"
	"sync"
	"time"

	"code.uber.internal/infra/memtsdb"
	xio "code.uber.internal/infra/memtsdb/x/io"
	xtime "code.uber.internal/infra/memtsdb/x/time"
)

const (
	// maxDatapointQueueLen is the maximum number of datapoints that's tolerable
	// for us to encode while holding the lock.
	maxDatapointQueueLen = 1000
)

var (
	errSeriesAllDatapointsExpired = errors.New("all series datapoints expired")
	timeNone                      = time.Time{}
)

type databaseSeries interface {
	id() string

	// tick performs any updates to ensure buffer flushes, blocks are written to disk, etc
	tick() error

	write(timestamp time.Time, value float64, unit xtime.Unit, annotation []byte) error

	fetchEncodedSegments(id string, start, end time.Time) (io.Reader, error)

	isEmpty() bool

	// bootstrap merges the raw series bootstrapped along with the buffered data
	bootstrap(rs memtsdb.DatabaseSeriesBlocks) error
}

type dbSeries struct {
	sync.RWMutex
	opts             memtsdb.DatabaseOptions
	nowFn            memtsdb.NowFn
	seriesID         string
	buffer           databaseBuffer
	blocks           memtsdb.DatabaseSeriesBlocks
	bufferDatapoints []datapoint
	blockSize        time.Duration
	bs               bootstrapState
}

func newDatabaseSeries(id string, bootstrapped bool, opts memtsdb.DatabaseOptions) databaseSeries {
	series := &dbSeries{
		opts:      opts,
		nowFn:     opts.GetNowFn(),
		seriesID:  id,
		blocks:    NewDatabaseSeriesBlocks(opts),
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
	blocksLen := len(s.blocks.GetAllBlocks())
	s.RUnlock()

	if blocksLen == 0 && s.buffer.isEmpty() {
		return true
	}
	return false
}

func (s *dbSeries) write(timestamp time.Time, value float64, unit xtime.Unit, annotation []byte) error {
	return s.buffer.write(timestamp, value, unit, annotation)
}

func (s *dbSeries) fetchEncodedSegments(id string, start, end time.Time) (io.Reader, error) {
	var results []io.Reader

	alignedStart := start.Truncate(s.blockSize)
	alignedEnd := end.Truncate(s.blockSize)
	if alignedEnd.Before(end) {
		// If we did shift back we need to include this block for all data
		alignedEnd = alignedEnd.Add(s.blockSize)
	}

	s.RLock()

	if len(s.blocks.GetAllBlocks()) > 0 {
		// Squeeze the lookup window by what's available to make range queries like [0, infinity) possible
		if s.blocks.GetMinTime().After(alignedStart) {
			alignedStart = s.blocks.GetMinTime()
		}
		if s.blocks.GetMaxTime().Before(alignedEnd) {
			alignedEnd = s.blocks.GetMaxTime()
		}
		for blockAt := alignedStart; blockAt.Before(alignedEnd); blockAt = blockAt.Add(s.blockSize) {
			if block, ok := s.blocks.GetBlockAt(blockAt); ok {
				if s := block.Stream(); s != nil {
					results = append(results, s)
				}
			}
		}
	}
	s.RUnlock()

	bufferResult, err := s.buffer.fetchEncodedSegment(start, end)
	if err != nil {
		return nil, err
	}

	if bufferResult != nil {
		results = append(results, bufferResult)
	}

	return xio.NewReaderSliceReader(results), nil
}

func (s *dbSeries) bufferFlushed(flush databaseBufferFlush) error {
	blockStart := flush.bucketStart.Truncate(s.blockSize)

	var err error

	s.Lock()

	if s.bs != bootstrapped {
		err = foreachBucketValue(flush, func(t time.Time, v databaseBufferValue) error {
			s.bufferDatapoints = append(s.bufferDatapoints, datapoint{t: t, v: v})
			return nil
		})
	} else {
		var block memtsdb.DatabaseBlock
		err = foreachBucketValue(flush, func(t time.Time, v databaseBufferValue) error {
			bs := t.Truncate(s.blockSize)
			if block == nil || bs != blockStart {
				block = s.blocks.GetBlockOrAdd(bs)
				blockStart = bs
			}
			if err := block.Write(t, v.value, v.unit, v.annotation); err != nil {
				return err
			}
			return nil
		})
	}

	s.Unlock()

	return err
}

// TODO(xichen): skip datapoints that fall within the bootstrap time range.
func (s *dbSeries) flushDatapoints(rs memtsdb.DatabaseSeriesBlocks, datapoints []datapoint) error {
	numDatapoints := len(datapoints)
	for i := 0; i < numDatapoints; i++ {
		blockStart := datapoints[i].t.Truncate(s.blockSize)
		block := rs.GetBlockOrAdd(blockStart)
		v := datapoints[i].v
		if err := block.Write(datapoints[i].t, v.value, v.unit, v.annotation); err != nil {
			return err
		}
	}
	return nil
}

func (s *dbSeries) bootstrap(rs memtsdb.DatabaseSeriesBlocks) error {

	if success, err := tryBootstrap(&s.RWMutex, &s.bs, "series"); !success {
		return err
	}

	s.Lock()
	s.bs = bootstrapping
	s.Unlock()

	if rs == nil {
		rs = NewDatabaseSeriesBlocks(s.opts)
	}

	for {
		s.RLock()
		numDatapoints := len(s.bufferDatapoints)
		s.RUnlock()

		// NB(xichen): it could take a while to flush the datapoints, so
		// we use maxDatapointQueueLen to make sure we don't hold the lock
		// for too long.
		if numDatapoints < maxDatapointQueueLen {
			break
		}

		s.Lock()
		datapoints := s.bufferDatapoints
		s.bufferDatapoints = nil
		s.Unlock()

		if err := s.flushDatapoints(rs, datapoints); err != nil {
			return err
		}
	}

	// now we only have a few datatpoints queued up, acquire the lock
	// and finish bootstrapping
	s.Lock()
	if err := s.flushDatapoints(rs, s.bufferDatapoints); err != nil {
		s.Unlock()
		return err
	}
	s.bufferDatapoints = nil
	s.blocks = rs
	s.bs = bootstrapped
	s.Unlock()

	return nil
}
