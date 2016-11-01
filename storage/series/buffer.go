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

package series

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/ts"
	xio "github.com/m3db/m3db/x/io"
	xerrors "github.com/m3db/m3x/errors"
	xtime "github.com/m3db/m3x/time"
)

var (
	errTooFuture = errors.New("datapoint is too far in the future")
	errTooPast   = errors.New("datapoint is too far in the past")
	timeZero     time.Time
)

const (
	// bucketsLen is three to contain the following buckets:
	// 1. Bucket before current window, can be drained or not-yet-drained
	// 2. Bucket currently taking writes
	// 3. Bucket for the future that can be taking writes that is head of
	// the current block if write is for the future within bounds
	bucketsLen = 3
)

type dbBuffer struct {
	opts              Options
	nowFn             clock.NowFn
	drainFn           databaseBufferDrainFn
	pastMostBucketIdx int
	buckets           [bucketsLen]dbBufferBucket
	blockSize         time.Duration
	bufferPast        time.Duration
	bufferFuture      time.Duration
}

type databaseBufferDrainFn func(start time.Time, encoder encoding.Encoder)

func newDatabaseBuffer(drainFn databaseBufferDrainFn, opts Options) databaseBuffer {
	b := &dbBuffer{
		opts:         opts,
		nowFn:        opts.ClockOptions().NowFn(),
		drainFn:      drainFn,
		blockSize:    opts.RetentionOptions().BlockSize(),
		bufferPast:   opts.RetentionOptions().BufferPast(),
		bufferFuture: opts.RetentionOptions().BufferFuture(),
	}
	b.Reset()
	return b
}

func (b *dbBuffer) Reset() {
	b.pastMostBucketIdx = b.forEachBucketAscToDrainOrReset(b.nowFn(), func(bucket *dbBufferBucket, start time.Time) {
		bucket.opts = b.opts
		bucket.resetTo(start)
	})
}

func (b *dbBuffer) MinMax() (time.Time, time.Time) {
	var min, max time.Time
	b.forEachBucketAsc(func(bucket *dbBufferBucket) {
		if min.IsZero() || bucket.start.Before(min) {
			min = bucket.start
		}
		if max.IsZero() || bucket.start.After(max) {
			max = bucket.start
		}
	})
	return min, max
}

func (b *dbBuffer) Write(
	ctx context.Context,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	now := b.nowFn()
	futureLimit := now.Add(1 * b.bufferFuture)
	pastLimit := now.Add(-1 * b.bufferPast)
	if !futureLimit.After(timestamp) {
		return xerrors.NewInvalidParamsError(errTooFuture)
	}
	if !pastLimit.Before(timestamp) {
		return xerrors.NewInvalidParamsError(errTooPast)
	}

	bucketStart := timestamp.Truncate(b.blockSize)
	bucketIdx := (timestamp.UnixNano() / int64(b.blockSize)) % bucketsLen

	bucket := &b.buckets[bucketIdx]
	if bucket.needsReset(bucketStart) {
		// Needs reset
		b.DrainAndReset()
	}

	return bucket.write(timestamp, value, unit, annotation)
}

func (b *dbBuffer) IsEmpty() bool {
	canReadAny := false
	b.forEachBucketAsc(func(bucket *dbBufferBucket) {
		canReadAny = canReadAny || bucket.canRead()
	})
	return !canReadAny
}

func (b *dbBuffer) NeedsDrain() bool {
	now := b.nowFn()
	needsDrainAny := false
	b.forEachBucketAscToDrainOrReset(now, func(bucket *dbBufferBucket, current time.Time) {
		needsDrainAny = needsDrainAny || bucket.needsDrain(now, current)
	})
	return needsDrainAny
}

func (b *dbBuffer) DrainAndReset() {
	now := b.nowFn()
	b.pastMostBucketIdx = b.forEachBucketAscToDrainOrReset(now, func(bucket *dbBufferBucket, current time.Time) {
		if bucket.needsDrain(now, current) {
			bucket.merge()

			// After we merge there is always only a single encoder with all the data
			encoder := bucket.encoders[0].encoder
			encoder.Seal()
			b.drainFn(bucket.start, encoder)
			bucket.drained = true
		}

		if bucket.needsReset(current) {
			// Reset bucket
			bucket.resetTo(current)
		}
	})
}

func (b *dbBuffer) Bootstrap(bl block.DatabaseBlock) error {
	blockStart := bl.StartTime()
	bootstrapped := false
	b.forEachBucketAsc(func(bucket *dbBufferBucket) {
		if bucket.start.Equal(blockStart) {
			bucket.bootstrap(bl)
			bootstrapped = true
		}
	})
	if !bootstrapped {
		return fmt.Errorf("block at %s not contained by buffer", blockStart.String())
	}
	return nil
}

// forEachBucketAsc iterates over the buckets in time ascending order
// to read bucket data
func (b *dbBuffer) forEachBucketAsc(fn func(*dbBufferBucket)) {
	for i := 0; i < bucketsLen; i++ {
		idx := (b.pastMostBucketIdx + i) % bucketsLen
		fn(&b.buckets[idx])
	}
}

// forEachBucketAscToDrainOrReset iterates over the buckets in time ascending order
// to drain or reset the buckets and returns the past most bucket index
func (b *dbBuffer) forEachBucketAscToDrainOrReset(now time.Time, fn func(*dbBufferBucket, time.Time)) int {
	pastMostBucketStart := now.Truncate(b.blockSize).Add(-1 * b.blockSize)
	bucketNum := (pastMostBucketStart.UnixNano() / int64(b.blockSize)) % bucketsLen
	for i := int64(0); i < bucketsLen; i++ {
		idx := int((bucketNum + i) % bucketsLen)
		fn(&b.buckets[idx], pastMostBucketStart.Add(time.Duration(i)*b.blockSize))
	}
	return int(bucketNum)
}

func (b *dbBuffer) ReadEncoded(ctx context.Context, start, end time.Time) [][]xio.SegmentReader {
	// TODO(r): pool these results arrays
	var results [][]xio.SegmentReader
	b.forEachBucketAsc(func(bucket *dbBufferBucket) {
		if !bucket.canRead() {
			return
		}
		if !start.Before(bucket.start.Add(b.blockSize)) {
			return
		}
		if !bucket.start.Before(end) {
			return
		}

		unmerged := make([]xio.SegmentReader, 0, len(bucket.encoders))
		bucket.readStreams(ctx, func(stream xio.SegmentReader) {
			unmerged = append(unmerged, stream)
		})

		results = append(results, unmerged)
	})

	return results
}

func (b *dbBuffer) FetchBlocks(ctx context.Context, starts []time.Time) []block.FetchBlockResult {
	var res []block.FetchBlockResult

	b.forEachBucketAsc(func(bucket *dbBufferBucket) {
		if !bucket.canRead() {
			return
		}
		found := false
		// starts have only a few items, linear search should be okay time-wise to
		// avoid allocating a map here.
		for _, start := range starts {
			if start == bucket.start {
				found = true
				break
			}
		}
		if !found {
			return
		}
		readers := make([]xio.SegmentReader, 0, len(bucket.encoders))
		bucket.readStreams(ctx, func(stream xio.SegmentReader) {
			readers = append(readers, stream)
		})
		res = append(res, block.NewFetchBlockResult(bucket.start, readers, nil))
	})

	return res
}

func (b *dbBuffer) FetchBlocksMetadata(
	ctx context.Context,
	start, end time.Time,
	includeSizes bool,
	includeChecksums bool,
) block.FetchBlockMetadataResults {
	blockSize := b.opts.RetentionOptions().BlockSize()
	res := b.opts.FetchBlockMetadataResultsPool().Get()
	b.forEachBucketAsc(func(bucket *dbBufferBucket) {
		if !bucket.canRead() {
			return
		}
		if !start.Before(bucket.start.Add(blockSize)) || !bucket.start.Before(end) {
			return
		}
		var size int64
		bucket.readStreams(ctx, func(stream xio.SegmentReader) {
			segment := stream.Segment()
			size += int64(len(segment.Head) + len(segment.Tail))
		})
		// If we have no data in this bucket, return early without appending it to the result.
		if size == 0 {
			return
		}
		var pSize *int64
		if includeSizes {
			pSize = &size
		}
		// NB(xichen): intentionally returning nil checksums for buckets
		// because they haven't been flushed yet
		res.Add(block.NewFetchBlockMetadataResult(bucket.start, pSize, nil, nil))
	})

	return res
}

type dbBufferBucketStreamFn func(stream xio.SegmentReader)

type dbBufferBucket struct {
	opts         Options
	start        time.Time
	encoders     []inOrderEncoder
	bootstrapped []block.DatabaseBlock
	empty        bool
	drained      bool
}

type inOrderEncoder struct {
	lastWriteAt time.Time
	encoder     encoding.Encoder
}

func (b *dbBufferBucket) resetTo(start time.Time) {
	bopts := b.opts.DatabaseBlockOptions()
	encoder := bopts.EncoderPool().Get()
	encoder.Reset(start, bopts.DatabaseBlockAllocSize())
	first := inOrderEncoder{
		lastWriteAt: timeZero,
		encoder:     encoder,
	}

	b.start = start
	b.encoders = append(b.encoders[:0], first)
	b.bootstrapped = nil
	b.empty = true
	b.drained = false
}

func (b *dbBufferBucket) canRead() bool {
	return !b.drained && !b.empty
}

func (b *dbBufferBucket) needsReset(targetStart time.Time) bool {
	return !b.start.Equal(targetStart)
}

func (b *dbBufferBucket) needsDrain(now time.Time, targetStart time.Time) bool {
	retentionOpts := b.opts.RetentionOptions()
	blockSize := retentionOpts.BlockSize()
	bufferPast := retentionOpts.BufferPast()

	return b.canRead() && (b.needsReset(targetStart) || b.start.Add(blockSize).Before(now.Add(-bufferPast)))
}

func (b *dbBufferBucket) bootstrap(bl block.DatabaseBlock) {
	b.empty = false
	b.bootstrapped = append(b.bootstrapped, bl)
}

func (b *dbBufferBucket) write(timestamp time.Time, value float64, unit xtime.Unit, annotation []byte) error {
	var target *inOrderEncoder
	for i := range b.encoders {
		if timestamp == b.encoders[i].lastWriteAt {
			// NB(xichen): we discard datapoints with the same timestamps as the ones we've
			// already encoded.
			return nil
		}
		if timestamp.After(b.encoders[i].lastWriteAt) {
			target = &b.encoders[i]
			break
		}
	}
	if target == nil {
		bopts := b.opts.DatabaseBlockOptions()
		blockSize := b.opts.RetentionOptions().BlockSize()
		encoder := bopts.EncoderPool().Get()
		encoder.Reset(timestamp.Truncate(blockSize), bopts.DatabaseBlockAllocSize())
		next := inOrderEncoder{encoder: encoder}
		b.encoders = append(b.encoders, next)
		target = &b.encoders[len(b.encoders)-1]
	}

	datapoint := ts.Datapoint{
		Timestamp: timestamp,
		Value:     value,
	}
	if err := target.encoder.Encode(datapoint, unit, annotation); err != nil {
		return err
	}
	target.lastWriteAt = timestamp
	b.empty = false
	return nil
}

func (b *dbBufferBucket) readStreams(
	ctx context.Context,
	streamFn dbBufferBucketStreamFn,
) {
	for i := range b.bootstrapped {
		if s, err := b.bootstrapped[i].Stream(ctx); err == nil && s != nil {
			// NB(r): block stream method will register the stream closer already
			streamFn(s)
		}
	}
	for i := range b.encoders {
		if s := b.encoders[i].encoder.Stream(); s != nil {
			ctx.RegisterCloser(context.CloserFn(s.Close))
			streamFn(s)
		}
	}
}

func (b *dbBufferBucket) merged() bool {
	return len(b.encoders) == 1 && len(b.bootstrapped) == 0
}

func (b *dbBufferBucket) merge() {
	if b.merged() {
		// Already merged
		return
	}

	bopts := b.opts.DatabaseBlockOptions()
	encoder := bopts.EncoderPool().Get()
	encoder.Reset(b.start, bopts.DatabaseBlockAllocSize())

	readers := make([]io.Reader, 0, len(b.encoders)+len(b.bootstrapped))
	for i := range b.encoders {
		if stream := b.encoders[i].encoder.Stream(); stream != nil {
			readers = append(readers, stream)
		}
	}

	var ctx context.Context
	for i := range b.bootstrapped {
		if ctx == nil {
			ctx = bopts.ContextPool().Get()
		}
		stream, err := b.bootstrapped[i].Stream(ctx)
		if err == nil && stream != nil {
			readers = append(readers, stream)
		}
	}

	var lastWriteAt time.Time
	iter := b.opts.MultiReaderIteratorPool().Get()
	iter.Reset(readers)
	for iter.Next() {
		dp, unit, annotation := iter.Current()
		lastWriteAt = dp.Timestamp
		encoder.Encode(dp, unit, annotation)
	}
	iter.Close()

	for i := range b.encoders {
		b.encoders[i].encoder.Close()
	}

	b.encoders = append(b.encoders[:0], inOrderEncoder{
		lastWriteAt: lastWriteAt,
		encoder:     encoder,
	})
	b.bootstrapped = nil

	if ctx != nil {
		ctx.Close()
	}
}
