package storage

import (
	"errors"
	"io"
	"sync"
	"time"

	"code.uber.internal/infra/memtsdb"
	xerrors "code.uber.internal/infra/memtsdb/x/errors"
	xio "code.uber.internal/infra/memtsdb/x/io"
	xtime "code.uber.internal/infra/memtsdb/x/time"
)

var (
	errInvalidRange               = errors.New("invalid time range specified")
	errSeriesAllDatapointsExpired = errors.New("all series datapoints expired")
)

type databaseSeries interface {
	id() string

	// tick performs any updates to ensure buffer flushes, blocks are written to disk, etc
	tick() error

	write(
		ctx memtsdb.Context,
		timestamp time.Time,
		value float64,
		unit xtime.Unit,
		annotation []byte,
	) error

	readEncoded(
		ctx memtsdb.Context,
		start, end time.Time,
	) (memtsdb.ReaderSliceReader, error)

	empty() bool

	// bootstrap merges the raw series bootstrapped along with the buffered data
	bootstrap(rs memtsdb.DatabaseSeriesBlocks) error
}

type dbSeries struct {
	sync.RWMutex
	opts             memtsdb.DatabaseOptions
	seriesID         string
	buffer           databaseBuffer
	blocks           memtsdb.DatabaseSeriesBlocks
	blockSize        time.Duration
	pendingBootstrap []pendingBootstrapFlush
	bs               bootstrapState
}

type pendingBootstrapFlush struct {
	encoder memtsdb.Encoder
}

func newDatabaseSeries(id string, bs bootstrapState, opts memtsdb.DatabaseOptions) databaseSeries {
	series := &dbSeries{
		opts:      opts,
		seriesID:  id,
		blocks:    NewDatabaseSeriesBlocks(opts),
		blockSize: opts.GetBlockSize(),
		bs:        bs,
	}
	series.buffer = newDatabaseBuffer(series.bufferFlushed, opts)
	return series
}

func (s *dbSeries) id() string {
	return s.seriesID
}

func (s *dbSeries) tick() error {
	if s.empty() {
		return errSeriesAllDatapointsExpired
	}

	// In best case when explicitly asked to flush may have no
	// stale buckets, cheaply check this case first with a Rlock
	s.RLock()
	needsFlush := s.buffer.needsFlush()
	s.RUnlock()

	if needsFlush {
		s.Lock()
		s.buffer.flushAndReset()
		s.Unlock()
	}

	// TODO(r): expire and write blocks to disk, when done ensure this is async
	// as ticking through series in a shard is done synchronously
	return nil
}

func (s *dbSeries) empty() bool {
	s.RLock()
	blocksLen := len(s.blocks.GetAllBlocks())
	bufferEmpty := s.buffer.empty()
	s.RUnlock()
	if blocksLen == 0 && bufferEmpty {
		return true
	}
	return false
}

func (s *dbSeries) write(
	ctx memtsdb.Context,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	// TODO(r): as discussed we need to revisit locking to provide a finer grained lock to
	// avoiding block writes while reads from blocks (not the buffer) go through.  There is
	// a few different ways we can accomplish this.  Will revisit soon once we have benchmarks
	// for mixed write/read workload.
	s.Lock()
	err := s.buffer.write(ctx, timestamp, value, unit, annotation)
	s.Unlock()
	return err
}

func (s *dbSeries) readEncoded(
	ctx memtsdb.Context,
	start, end time.Time,
) (memtsdb.ReaderSliceReader, error) {
	if end.Before(start) {
		return nil, xerrors.NewInvalidParamsError(errInvalidRange)
	}

	// TODO(r): pool these results arrays
	var results []io.Reader

	alignedStart := start.Truncate(s.blockSize)
	alignedEnd := end.Truncate(s.blockSize)
	if alignedEnd.Equal(end) {
		// Move back to make range [start, end)
		alignedEnd = alignedEnd.Add(-1 * s.blockSize)
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
		for blockAt := alignedStart; !blockAt.After(alignedEnd); blockAt = blockAt.Add(s.blockSize) {
			if block, ok := s.blocks.GetBlockAt(blockAt); ok {
				if s := block.Stream(); s != nil {
					results = append(results, s)
				}
			}
		}
	}

	bufferResults := s.buffer.readEncoded(ctx, start, end)
	if len(bufferResults) > 0 {
		results = append(results, bufferResults...)
	}

	s.RUnlock()

	return xio.NewReaderSliceReader(results), nil
}

func (s *dbSeries) bufferFlushed(start time.Time, encoder memtsdb.Encoder) {
	// NB(r): by the very nature of this method executing we have the
	// lock already. Executing the flush method occurs during a write if the
	// buffer needs to flush or if tick is called and series explicitly asks
	// the buffer to flush ready buckets.
	if s.bs != bootstrapped {
		s.pendingBootstrap = append(s.pendingBootstrap, pendingBootstrapFlush{encoder})
		return
	}

	if _, ok := s.blocks.GetBlockAt(start); !ok {
		// New completed block
		s.blocks.AddBlock(NewDatabaseBlock(start, encoder, s.opts))
		return
	}

	// NB(r): this will occur if after bootstrap we have a partial
	// block and now the buffer is passing the rest of that block
	stream := encoder.Stream()
	s.flushStream(s.blocks, stream)
	stream.Close()
}

// TODO(xichen): skip datapoints that fall within the bootstrap time range.
func (s *dbSeries) flushStream(blocks memtsdb.DatabaseSeriesBlocks, stream io.Reader) error {
	iter := s.opts.GetIteratorPool().Get()
	iter.Reset(stream)

	// Close the iterator and return to pool when done
	defer iter.Close()

	for iter.Next() {
		dp, unit, annotation := iter.Current()
		blockStart := dp.Timestamp.Truncate(s.blockSize)
		block := blocks.GetBlockOrAdd(blockStart)

		if err := block.Write(dp.Timestamp, dp.Value, unit, annotation); err != nil {
			return err
		}
	}
	if err := iter.Err(); err != nil {
		return err
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
		s.Lock()
		if len(s.pendingBootstrap) == 0 {
			s.blocks = rs
			s.bs = bootstrapped
			s.Unlock()
			break
		}
		flush := s.pendingBootstrap[0]
		s.pendingBootstrap = s.pendingBootstrap[1:]
		s.Unlock()

		stream := flush.encoder.Stream()
		if err := s.flushStream(rs, stream); err != nil {
			stream.Close()
			return err
		}
		stream.Close()
	}

	return nil
}
