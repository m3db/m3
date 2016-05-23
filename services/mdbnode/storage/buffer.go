package storage

import (
	"errors"
	"math"
	"sync"
	"time"

	"code.uber.internal/infra/memtsdb/encoding"
)

var (
	// ErrTooFuture is raised when datapoint being written is too far in the future
	ErrTooFuture = errors.New("datapoint is too far in the future")

	// ErrTooPast is raised when datapoint being written is too far in the past
	ErrTooPast = errors.New("datapoint is too far in the past")
)

const (
	bucketFlushPoolPercent = 0.2
)

type databaseBuffer interface {
	write(timestamp time.Time, value float64, unit time.Duration, annotation []byte) error

	// fetchEncodedSegment will return the full buffer's data as an encoded
	// segment if start and end intersects the buffer at all, nil otherwise
	fetchEncodedSegment(start, end time.Time) []byte

	isEmpty() bool

	flushStale() int
}

type databaseBufferFlush struct {
	bucketStart    time.Time
	bucketValues   []databaseBufferValue
	bucketStepSize time.Duration
}

type databaseBufferFlushFn func(f databaseBufferFlush)

type dbBuffer struct {
	sync.RWMutex
	opts             DatabaseOptions
	flushFn          databaseBufferFlushFn
	nowFn            NowFn
	tooFuture        time.Duration
	tooPast          time.Duration
	buckets          []dbBufferBucket
	bucketSize       time.Duration
	bucketsLen       int
	bucketValuesLen  int
	resolution       time.Duration
	newEncoderFn     encoding.NewEncoderFn
	bucketsFlushPool chan []databaseBufferValue
}

type dbBufferBucket struct {
	startTime      time.Time
	values         []databaseBufferValue
	writesToValues int
}

func (b *dbBufferBucket) resetTo(startTime time.Time) {
	b.startTime = startTime
	for i := 0; i < len(b.values); i++ {
		b.values[i].value = math.NaN()
	}
	b.writesToValues = 0
}

type databaseBufferValue struct {
	value      float64
	unit       time.Duration
	annotation []byte
}

func newDatabaseBuffer(flushFn databaseBufferFlushFn, opts DatabaseOptions) databaseBuffer {
	nowFn := opts.GetNowFn()
	bucketSize := opts.GetBufferFlush()
	resolution := opts.GetBufferResolution()
	tooFuture := opts.GetBufferFuture()
	tooPast := opts.GetBufferPast()
	bucketRange := tooFuture + tooPast
	bucketsLen := int(math.Ceil(float64(bucketRange) / float64(bucketSize)))

	bucketValuesLen := int(math.Ceil(float64(bucketSize) / float64(resolution)))
	bucketsFlushPoolLen := int(math.Ceil(float64(bucketFlushPoolPercent) * float64(bucketsLen)))

	// Slab allocate all values required by buffer
	bufferValues := make([]databaseBufferValue, bucketValuesLen*(bucketsLen+bucketsFlushPoolLen))

	buckets := make([]dbBufferBucket, bucketsLen)
	for i := 0; i < bucketsLen; i++ {
		begin, end := i*bucketValuesLen, (i+1)*bucketValuesLen
		buckets[i].values = bufferValues[begin:end]
	}

	bucketsFlushPool := make(chan []databaseBufferValue, bucketsFlushPoolLen)
	for i := 0; i < bucketsFlushPoolLen; i++ {
		begin, end := (i+bucketsLen)*bucketValuesLen, (i+bucketsLen+1)*bucketValuesLen
		// No need to reset values for these buckets as they will be copied into
		bucketsFlushPool <- bufferValues[begin:end]
	}

	buffer := &dbBuffer{
		opts:             opts,
		flushFn:          flushFn,
		nowFn:            nowFn,
		tooFuture:        tooFuture,
		tooPast:          tooPast,
		buckets:          buckets,
		bucketSize:       bucketSize,
		bucketsLen:       bucketsLen,
		bucketValuesLen:  bucketValuesLen,
		resolution:       resolution,
		newEncoderFn:     opts.GetNewEncoderFn(),
		bucketsFlushPool: bucketsFlushPool,
	}
	buffer.forEachBucketAsc(nowFn(), func(idx int, start time.Time) {
		b := &buckets[idx]
		b.resetTo(start)
	})

	return buffer
}

func (s *dbBuffer) write(timestamp time.Time, value float64, unit time.Duration, annotation []byte) error {
	now := s.nowFn()
	futureLimit := now.Add(s.tooFuture)
	pastLimit := now.Add(-1 * s.tooPast)
	if futureLimit.Before(timestamp) {
		return ErrTooFuture
	}
	if pastLimit.After(timestamp) {
		return ErrTooPast
	}

	bucketStart := timestamp.Truncate(s.bucketSize)
	bucketIdx := (timestamp.UnixNano() / int64(s.bucketSize)) % int64(s.bucketsLen)

	s.Lock()

	var flushed []databaseBufferFlush
	if !s.buckets[bucketIdx].startTime.Equal(bucketStart) {
		// Need to flush this bucket
		flushed = s.withLockFlushStale(now)
	}

	valueIdx := timestamp.Sub(bucketStart) / s.resolution
	s.buckets[bucketIdx].values[valueIdx].value = value
	s.buckets[bucketIdx].values[valueIdx].unit = unit
	s.buckets[bucketIdx].values[valueIdx].annotation = annotation
	s.buckets[bucketIdx].writesToValues++

	s.Unlock()

	// Flush after releasing lock
	s.callFlushFn(flushed)
	return nil
}

func (s *dbBuffer) isEmpty() bool {
	allWritesToValues := 0
	s.RLock()
	s.forEachBucketAsc(s.nowFn(), func(idx int, current time.Time) {
		if s.buckets[idx].startTime.Equal(current) {
			// Not stale
			allWritesToValues += s.buckets[idx].writesToValues
		}
	})
	s.RUnlock()
	return allWritesToValues == 0
}

func (s *dbBuffer) flushStale() int {
	// In best case when explicitly asked to flush may have no
	// stale buckets, cheaply check this case first with a Rlock
	now := s.nowFn()
	staleAny := false
	s.RLock()
	s.forEachBucketAsc(now, func(idx int, current time.Time) {
		if !s.buckets[idx].startTime.Equal(current) {
			staleAny = true
		}
	})
	s.RUnlock()

	if !staleAny {
		return 0
	}

	s.Lock()
	flushed := s.withLockFlushStale(now)
	s.Unlock()

	// Flush after releasing lock
	s.callFlushFn(flushed)
	return len(flushed)
}

func (s *dbBuffer) callFlushFn(flushed []databaseBufferFlush) {
	for i := range flushed {
		s.flushFn(flushed[i])
		select {
		case s.bucketsFlushPool <- flushed[i].bucketValues:
		default:
		}
	}
}

func (s *dbBuffer) withLockFlushStale(now time.Time) []databaseBufferFlush {
	var flushed []databaseBufferFlush
	s.forEachBucketAsc(now, func(idx int, current time.Time) {
		if s.buckets[idx].startTime.Equal(current) {
			// Not stale
			return
		}

		values := s.buckets[idx].values
		staleStart := s.buckets[idx].startTime

		// Copy for flusher to asynchronously read out
		var staleValues []databaseBufferValue
		select {
		case staleValues = <-s.bucketsFlushPool:
		default:
			staleValues = make([]databaseBufferValue, s.bucketValuesLen)
		}
		copy(staleValues, values)

		// Reset buffer
		b := &s.buckets[idx]
		b.resetTo(current)

		// Flush
		flushed = append(flushed, databaseBufferFlush{staleStart, staleValues, s.resolution})
	})
	return flushed
}

func (s *dbBuffer) forEachBucketAsc(now time.Time, fn func(idx int, current time.Time)) {
	pastMostBucketStart := now.Add(-1 * s.tooPast).Truncate(s.bucketSize)
	bucketsLen := int64(s.bucketsLen)
	bucketNum := (pastMostBucketStart.UnixNano() / int64(s.bucketSize)) % bucketsLen
	for i := int64(0); i < bucketsLen; i++ {
		idx := int((bucketNum + i) % bucketsLen)
		fn(idx, pastMostBucketStart.Add(time.Duration(i)*s.bucketSize))
	}
}

func (s *dbBuffer) fetchEncodedSegment(start, end time.Time) []byte {
	// TODO(r): cache and invalidate on write the result of this method
	now := s.nowFn()
	futureLimit := now.Add(s.tooFuture)
	pastLimit := now.Add(-1 * s.tooPast)
	if start.After(futureLimit) {
		return nil
	}
	if end.Before(pastLimit) {
		return nil
	}

	s.RLock()

	var encoder encoding.Encoder
	s.forEachBucketAsc(now, func(idx int, current time.Time) {
		if !s.buckets[idx].startTime.Equal(current) {
			// Stale
			return
		}

		if encoder == nil {
			encoder = s.newEncoderFn(current)
		}

		values := s.buckets[idx].values
		for i := 0; i < s.bucketValuesLen; i++ {
			if !math.IsNaN(values[i].value) {
				ts := current.Add(time.Duration(i) * s.resolution)
				// TODO(r): encoder will take unit per datapoint
				encoder.Encode(encoding.Datapoint{Timestamp: ts, Value: values[i].value}, values[i].annotation)
			}
		}
	})

	s.RUnlock()

	if encoder != nil {
		return encoder.Bytes()
	}
	return nil
}
