package storage

import (
	"errors"
	"io"
	"sort"
	"time"

	"code.uber.internal/infra/memtsdb"
	xerrors "code.uber.internal/infra/memtsdb/x/errors"
	xtime "code.uber.internal/infra/memtsdb/x/time"
)

var (
	errTooFuture = errors.New("datapoint is too far in the future")
	errTooPast   = errors.New("datapoint is too far in the past")
)

const (
	// bucketsLen is three to contain the following buckets:
	// 1. Bucket before current window, can be flushed or not-yet-flushed
	// 2. Bucket currently taking writes
	// 3. Bucket for the future that can be taking writes that is head of
	// the current block if write is for the future within bounds
	bucketsLen = 3
)

type databaseBuffer interface {
	write(
		ctx memtsdb.Context,
		timestamp time.Time,
		value float64,
		unit xtime.Unit,
		annotation []byte,
	) error

	// readEncoded will return the full buffer's data as encoded segments
	// if start and end intersects the buffer at all, nil otherwise
	readEncoded(
		ctx memtsdb.Context,
		start, end time.Time,
	) []io.Reader

	empty() bool

	needsFlush() bool

	flushAndReset()
}

type dbBuffer struct {
	opts      memtsdb.DatabaseOptions
	nowFn     memtsdb.NowFn
	flushFn   databaseBufferFlushFn
	buckets   [bucketsLen]dbBufferBucket
	blockSize time.Duration
}

type databaseBufferFlushFn func(start time.Time, encoder memtsdb.Encoder)

func newDatabaseBuffer(flushFn databaseBufferFlushFn, opts memtsdb.DatabaseOptions) databaseBuffer {
	nowFn := opts.GetNowFn()

	buffer := &dbBuffer{
		opts:      opts,
		nowFn:     nowFn,
		flushFn:   flushFn,
		blockSize: opts.GetBlockSize(),
	}
	buffer.forEachBucketAsc(nowFn(), func(b *dbBufferBucket, start time.Time) {
		b.opts = opts
		b.resetTo(start)
	})

	return buffer
}

func (s *dbBuffer) write(
	ctx memtsdb.Context,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	now := s.nowFn()
	futureLimit := now.Add(s.opts.GetBufferFuture())
	pastLimit := now.Add(-1 * s.opts.GetBufferPast())
	if !futureLimit.After(timestamp) {
		return xerrors.NewInvalidParamsError(errTooFuture)
	}
	if !pastLimit.Before(timestamp) {
		return xerrors.NewInvalidParamsError(errTooPast)
	}

	bucketStart := timestamp.Truncate(s.blockSize)
	bucketIdx := (timestamp.UnixNano() / int64(s.blockSize)) % bucketsLen

	bucket := &s.buckets[bucketIdx]
	_, _, needsReset := s.bucketState(now, bucket, bucketStart)
	if needsReset {
		// Needs reset
		s.flushAndReset()
	}

	return bucket.write(timestamp, value, unit, annotation)
}

func (s *dbBuffer) empty() bool {
	now := s.nowFn()
	canReadAny := false
	s.forEachBucketAsc(now, func(b *dbBufferBucket, current time.Time) {
		if !canReadAny {
			canReadAny, _, _ = s.bucketState(now, b, current)
		}
	})
	return !canReadAny
}

func (s *dbBuffer) needsFlush() bool {
	now := s.nowFn()
	needsFlushAny := false
	s.forEachBucketAsc(now, func(b *dbBufferBucket, current time.Time) {
		if !needsFlushAny {
			_, needsFlushAny, _ = s.bucketState(now, b, current)
		}
	})
	return needsFlushAny
}

func (s *dbBuffer) bucketState(
	now time.Time,
	b *dbBufferBucket,
	bucketStart time.Time,
) (bool, bool, bool) {
	notFlushedHasValues := !b.flushed && !b.lastWriteAt.IsZero()

	shouldRead := notFlushedHasValues
	needsReset := !b.start.Equal(bucketStart)
	needsFlush := (notFlushedHasValues && needsReset) ||
		(notFlushedHasValues && bucketStart.Add(s.blockSize).Before(now.Add(-1*b.opts.GetBufferPast())))

	return shouldRead, needsFlush, needsReset
}

func (s *dbBuffer) flushAndReset() {
	now := s.nowFn()
	s.forEachBucketAsc(now, func(b *dbBufferBucket, current time.Time) {
		_, needsFlush, needsReset := s.bucketState(now, b, current)
		if !needsFlush && !needsReset {
			// No action necessary
			return
		}

		if needsFlush {
			b.sort()

			// After we sort there is always only a single encoder
			encoder := b.encoders[0].encoder
			encoder.Done()
			s.flushFn(b.start, encoder)
			b.flushed = true
		}

		if needsReset {
			// Reset bucket
			b.resetTo(current)
		}
	})
}

func (s *dbBuffer) forEachBucketAsc(now time.Time, fn func(b *dbBufferBucket, current time.Time)) {
	pastMostBucketStart := now.Truncate(s.blockSize).Add(-1 * s.blockSize)
	bucketNum := (pastMostBucketStart.UnixNano() / int64(s.blockSize)) % bucketsLen
	for i := int64(0); i < bucketsLen; i++ {
		idx := int((bucketNum + i) % bucketsLen)
		fn(&s.buckets[idx], pastMostBucketStart.Add(time.Duration(i)*s.blockSize))
	}
}

func (s *dbBuffer) readEncoded(ctx memtsdb.Context, start, end time.Time) []io.Reader {
	now := s.nowFn()
	// TODO(r): pool these results arrays
	var results []io.Reader
	s.forEachBucketAsc(now, func(b *dbBufferBucket, current time.Time) {
		shouldRead, _, _ := s.bucketState(now, b, current)
		if !shouldRead {
			// Requires reset and flushed already or not written to
			return
		}

		if !start.Before(b.start.Add(s.blockSize)) {
			return
		}
		if !b.start.Before(end) {
			return
		}

		// TODO(r): instead of flushing potentially out of order streams, wrap them
		// in one big multi-stream reader so client's can read them in order easily
		for i := range b.encoders {
			stream := b.encoders[i].encoder.Stream()
			if stream == nil {
				// TODO(r): log an error and emit a metric, this is pretty bad as this
				// encoder should have values if "shouldRead" returned true
				continue
			}

			ctx.RegisterCloser(func() {
				// Close the stream and return to pool when read done
				stream.Close()
			})

			results = append(results, stream)
		}
	})

	return results
}

type dbBufferBucket struct {
	opts        memtsdb.DatabaseOptions
	start       time.Time
	encoders    []inOrderEncoder
	lastWriteAt time.Time
	outOfOrder  bool
	flushed     bool
}

type inOrderEncoder struct {
	lastWriteAt time.Time
	encoder     memtsdb.Encoder
}

func (b *dbBufferBucket) resetTo(start time.Time) {
	encoder := b.opts.GetEncoderPool().Get()
	encoder.Reset(start, b.opts.GetBufferBucketAllocSize())
	first := inOrderEncoder{
		lastWriteAt: timeZero,
		encoder:     encoder,
	}

	b.start = start
	b.encoders = append(b.encoders[:0], first)
	b.lastWriteAt = timeZero
	b.outOfOrder = false
	b.flushed = false
}

func (b *dbBufferBucket) write(timestamp time.Time, value float64, unit xtime.Unit, annotation []byte) error {
	if !b.outOfOrder && timestamp.Before(b.lastWriteAt) {
		// Can never revert from out of order until reset or sorted
		b.outOfOrder = true
	}
	if timestamp.After(b.lastWriteAt) {
		b.lastWriteAt = timestamp
	}

	var target *inOrderEncoder
	for i := range b.encoders {
		if !timestamp.Before(b.encoders[i].lastWriteAt) {
			target = &b.encoders[i]
			break
		}
	}
	if target == nil {
		encoder := b.opts.GetEncoderPool().Get()
		encoder.Reset(timestamp.Truncate(b.opts.GetBlockSize()), b.opts.GetBufferBucketAllocSize())
		next := inOrderEncoder{encoder: encoder}
		b.encoders = append(b.encoders, next)
		target = &next
	}

	datapoint := memtsdb.Datapoint{
		Timestamp: timestamp,
		Value:     value,
	}
	if err := target.encoder.Encode(datapoint, unit, annotation); err != nil {
		return err
	}
	target.lastWriteAt = timestamp
	return nil
}

func (b *dbBufferBucket) sort() {
	if !b.outOfOrder {
		// Already sorted
		return
	}

	// TODO(r): instead of old sorting technique below, simply take the next datapoint
	// that's available from all the iterators and proceed in step
	encoder := b.opts.GetEncoderPool().Get()
	encoder.Reset(b.start, b.opts.GetBufferBucketAllocSize())

	// NB(r): consider pooling these for reordering use
	var datapoints []*datapoint
	for i := range b.encoders {
		iter := b.opts.GetIteratorPool().Get()
		iter.Reset(b.encoders[i].encoder.Stream())
		for iter.Next() {
			dp, unit, annotation := iter.Current()
			datapoints = append(datapoints, &datapoint{
				t:          dp.Timestamp,
				value:      dp.Value,
				unit:       unit,
				annotation: annotation,
			})
		}
		iter.Close()
		b.encoders[i].encoder.Close()
	}

	sort.Sort(byTimeAsc(datapoints))

	var dp memtsdb.Datapoint
	for i := range datapoints {
		dp.Timestamp = datapoints[i].t
		dp.Value = datapoints[i].value
		encoder.Encode(dp, datapoints[i].unit, datapoints[i].annotation)
	}

	b.lastWriteAt = dp.Timestamp
	b.encoders = append(b.encoders[:0], inOrderEncoder{
		lastWriteAt: b.lastWriteAt,
		encoder:     encoder,
	})
	b.outOfOrder = false
}

// NB(r): only used for intermediate storage while sorting out of order buffers
type datapoint struct {
	t          time.Time
	value      float64
	unit       xtime.Unit
	annotation []byte
}

// Implements sort.Interface
type byTimeAsc []*datapoint

func (v byTimeAsc) Len() int {
	return len(v)
}

func (v byTimeAsc) Less(lhs, rhs int) bool {
	return v[lhs].t.Before(v[rhs].t)
}

func (v byTimeAsc) Swap(lhs, rhs int) {
	v[lhs], v[rhs] = v[rhs], v[lhs]
}
