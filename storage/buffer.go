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
	"errors"
	"io"
	"time"

	"github.com/m3db/m3db/interfaces/m3db"
	xerrors "github.com/m3db/m3db/x/errors"
	xtime "github.com/m3db/m3db/x/time"
)

var (
	errTooFuture = errors.New("datapoint is too far in the future")
	errTooPast   = errors.New("datapoint is too far in the past")
)

const (
	// bucketsLen is three to contain the following buckets:
	// 1. Bucket before current window, can be drained or not-yet-drained
	// 2. Bucket currently taking writes
	// 3. Bucket for the future that can be taking writes that is head of
	// the current block if write is for the future within bounds
	bucketsLen = 3
)

type databaseBuffer interface {
	Write(
		ctx m3db.Context,
		timestamp time.Time,
		value float64,
		unit xtime.Unit,
		annotation []byte,
	) error

	// ReadEncoded will return the full buffer's data as encoded segments
	// if start and end intersects the buffer at all, nil otherwise
	ReadEncoded(
		ctx m3db.Context,
		start, end time.Time,
	) []io.Reader

	Empty() bool

	NeedsDrain() bool

	DrainAndReset()
}

type dbBuffer struct {
	opts      m3db.DatabaseOptions
	nowFn     m3db.NowFn
	drainFn   databaseBufferDrainFn
	buckets   [bucketsLen]dbBufferBucket
	blockSize time.Duration
}

type databaseBufferDrainFn func(start time.Time, encoder m3db.Encoder)

func newDatabaseBuffer(drainFn databaseBufferDrainFn, opts m3db.DatabaseOptions) databaseBuffer {
	nowFn := opts.GetNowFn()

	buffer := &dbBuffer{
		opts:      opts,
		nowFn:     nowFn,
		drainFn:   drainFn,
		blockSize: opts.GetBlockSize(),
	}
	buffer.forEachBucketAsc(nowFn(), func(b *dbBufferBucket, start time.Time) {
		b.opts = opts
		b.resetTo(start)
	})

	return buffer
}

func (s *dbBuffer) Write(
	ctx m3db.Context,
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
		s.DrainAndReset()
	}

	return bucket.write(timestamp, value, unit, annotation)
}

func (s *dbBuffer) Empty() bool {
	now := s.nowFn()
	canReadAny := false
	s.forEachBucketAsc(now, func(b *dbBufferBucket, current time.Time) {
		if !canReadAny {
			canReadAny, _, _ = s.bucketState(now, b, current)
		}
	})
	return !canReadAny
}

func (s *dbBuffer) NeedsDrain() bool {
	now := s.nowFn()
	needsDrainAny := false
	s.forEachBucketAsc(now, func(b *dbBufferBucket, current time.Time) {
		if !needsDrainAny {
			_, needsDrainAny, _ = s.bucketState(now, b, current)
		}
	})
	return needsDrainAny
}

func (s *dbBuffer) bucketState(
	now time.Time,
	b *dbBufferBucket,
	bucketStart time.Time,
) (bool, bool, bool) {
	notDrainedHasValues := !b.drained && !b.lastWriteAt.IsZero()

	shouldRead := notDrainedHasValues
	needsReset := !b.start.Equal(bucketStart)
	needsDrain := (notDrainedHasValues && needsReset) ||
		(notDrainedHasValues && bucketStart.Add(s.blockSize).Before(now.Add(-1*b.opts.GetBufferPast())))

	return shouldRead, needsDrain, needsReset
}

func (s *dbBuffer) DrainAndReset() {
	now := s.nowFn()
	s.forEachBucketAsc(now, func(b *dbBufferBucket, current time.Time) {
		_, needsDrain, needsReset := s.bucketState(now, b, current)
		if !needsDrain && !needsReset {
			// No action necessary
			return
		}

		if needsDrain {
			b.sort()

			// After we sort there is always only a single encoder
			encoder := b.encoders[0].encoder
			encoder.Done()
			s.drainFn(b.start, encoder)
			b.drained = true
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

func (s *dbBuffer) ReadEncoded(ctx m3db.Context, start, end time.Time) []io.Reader {
	now := s.nowFn()
	// TODO(r): pool these results arrays
	var results []io.Reader
	s.forEachBucketAsc(now, func(b *dbBufferBucket, current time.Time) {
		shouldRead, _, _ := s.bucketState(now, b, current)
		if !shouldRead {
			// Requires reset and drained already or not written to
			return
		}

		if !start.Before(b.start.Add(s.blockSize)) {
			return
		}
		if !b.start.Before(end) {
			return
		}

		// TODO(r): instead of draining potentially out of order streams, wrap them
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
	opts        m3db.DatabaseOptions
	start       time.Time
	encoders    []inOrderEncoder
	lastWriteAt time.Time
	outOfOrder  bool
	drained     bool
}

type inOrderEncoder struct {
	lastWriteAt time.Time
	encoder     m3db.Encoder
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
	b.drained = false
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

	datapoint := m3db.Datapoint{
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

	encoder := b.opts.GetEncoderPool().Get()
	encoder.Reset(b.start, b.opts.GetBufferBucketAllocSize())

	readers := make([]io.Reader, len(b.encoders))
	for i := range b.encoders {
		readers[i] = b.encoders[i].encoder.Stream()
	}

	iter := b.opts.GetMultiReaderIteratorPool().Get()
	iter.Reset(readers)
	for iter.Next() {
		dp, unit, annotation := iter.Current()
		b.lastWriteAt = dp.Timestamp
		encoder.Encode(dp, unit, annotation)
	}
	iter.Close()

	for i := range b.encoders {
		b.encoders[i].encoder.Close()
	}

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
