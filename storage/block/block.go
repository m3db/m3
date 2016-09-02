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

package block

import (
	"errors"
	"time"

	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/ts"
	xio "github.com/m3db/m3db/x/io"
	"github.com/m3db/m3x/time"
)

var (
	errReadFromClosedBlock = errors.New("attempt to read from a closed block")
	errWriteToClosedBlock  = errors.New("attempt to write to a closed block")
	errWriteToSealedBlock  = errors.New("attempt to write to a sealed block")

	timeZero = time.Time{}
)

// NB(xichen): locking of dbBlock instances is currently done outside the dbBlock struct at the series level.
// Specifically, read lock is acquired for accessing operations like Stream(), and write lock is acquired
// for mutating operations like Write(), Reset(), and Close(). Adding a explicit lock to the dbBlock struct might
// make it more clear w.r.t. how/when we acquire locks, though.
type dbBlock struct {
	opts     Options
	start    time.Time
	encoder  encoding.Encoder
	segment  ts.Segment
	ctx      context.Context
	closed   bool
	writable bool
}

// NewDatabaseBlock creates a new DatabaseBlock instance.
func NewDatabaseBlock(start time.Time, encoder encoding.Encoder, opts Options) DatabaseBlock {
	return &dbBlock{
		opts:     opts,
		start:    start,
		encoder:  encoder,
		ctx:      opts.GetContextPool().Get(),
		closed:   false,
		writable: true,
	}
}

func (b *dbBlock) StartTime() time.Time {
	return b.start
}

func (b *dbBlock) IsSealed() bool {
	return !b.writable
}

func (b *dbBlock) Write(timestamp time.Time, value float64, unit xtime.Unit, annotation ts.Annotation) error {
	if b.closed {
		return errWriteToClosedBlock
	}
	if !b.writable {
		return errWriteToSealedBlock
	}
	return b.encoder.Encode(ts.Datapoint{Timestamp: timestamp, Value: value}, unit, annotation)
}

func (b *dbBlock) Stream(blocker context.Context) (xio.SegmentReader, error) {
	if b.closed {
		return nil, errReadFromClosedBlock
	}
	if blocker != nil {
		b.ctx.DependsOn(blocker)
	}
	if b.writable {
		return b.encoder.Stream(), nil
	}
	// If the block is not writable, and the segment is empty, it means
	// there are no data encoded in this block, so we return a nil reader.
	if b.segment.Head == nil && b.segment.Tail == nil {
		return nil, nil
	}
	s := b.opts.GetSegmentReaderPool().Get()
	s.Reset(b.segment)
	return s, nil
}

// close closes internal context and returns encoder and stream to pool.
func (b *dbBlock) close() {
	// If the context is nil (e.g., when it's just obtained from the pool),
	// we return immediately.
	if b.ctx == nil {
		return
	}

	cleanUp := func() {
		b.ctx.Close()
		b.ctx = nil
		b.encoder = nil
		b.segment = ts.Segment{}
	}
	defer cleanUp()

	if b.writable {
		// If the block is not sealed, we need to close the encoder.
		if encoder := b.encoder; encoder != nil {
			b.ctx.RegisterCloser(encoder.Close)
		}
		return
	}

	// Otherwise, we need to return bytes to the bytes pool.
	segment := b.segment
	bytesPool := b.opts.GetBytesPool()
	b.ctx.RegisterCloser(func() {
		if segment.Head != nil && !segment.HeadShared {
			bytesPool.Put(segment.Head)
		}
		if segment.Tail != nil && !segment.TailShared {
			bytesPool.Put(segment.Tail)
		}
	})
}

// Reset resets the block start time and the encoder.
func (b *dbBlock) Reset(startTime time.Time, encoder encoding.Encoder) {
	b.close()
	b.start = startTime
	b.encoder = encoder
	b.segment = ts.Segment{}
	b.ctx = b.opts.GetContextPool().Get()
	b.closed = false
	b.writable = true
}

func (b *dbBlock) Close() {
	if b.closed {
		return
	}
	b.closed = true
	b.close()
	if pool := b.opts.GetDatabaseBlockPool(); pool != nil {
		pool.Put(b)
	}
}

func (b *dbBlock) Seal() {
	if b.closed || !b.writable {
		return
	}
	b.writable = false
	if stream := b.encoder.Stream(); stream != nil {
		b.segment = stream.Segment()
	}
	// Reset encoder to prevent the byte stream inside the encoder
	// from being returned to the bytes pool.
	b.encoder.ResetSetData(b.start, nil, false)
	b.encoder.Close()
	b.encoder = nil
}

type databaseSeriesBlocks struct {
	opts  Options
	elems map[time.Time]DatabaseBlock
	min   time.Time
	max   time.Time
}

// NewDatabaseSeriesBlocks creates a databaseSeriesBlocks instance.
func NewDatabaseSeriesBlocks(opts Options) DatabaseSeriesBlocks {
	return &databaseSeriesBlocks{
		opts:  opts,
		elems: make(map[time.Time]DatabaseBlock),
	}
}

func (dbb *databaseSeriesBlocks) Options() Options {
	return dbb.opts
}

func (dbb *databaseSeriesBlocks) Len() int {
	return len(dbb.elems)
}

func (dbb *databaseSeriesBlocks) AddBlock(block DatabaseBlock) {
	start := block.StartTime()
	if dbb.min.Equal(timeZero) || start.Before(dbb.min) {
		dbb.min = start
	}
	if dbb.max.Equal(timeZero) || start.After(dbb.max) {
		dbb.max = start
	}
	dbb.elems[start] = block
}

func (dbb *databaseSeriesBlocks) AddSeries(other DatabaseSeriesBlocks) {
	if other == nil {
		return
	}
	blocks := other.AllBlocks()
	for _, b := range blocks {
		dbb.AddBlock(b)
	}
}

// GetMinTime returns the min time of the blocks contained.
func (dbb *databaseSeriesBlocks) GetMinTime() time.Time {
	return dbb.min
}

// GetMaxTime returns the max time of the blocks contained.
func (dbb *databaseSeriesBlocks) GetMaxTime() time.Time {
	return dbb.max
}

func (dbb *databaseSeriesBlocks) GetBlockAt(t time.Time) (DatabaseBlock, bool) {
	b, ok := dbb.elems[t]
	return b, ok
}

func (dbb *databaseSeriesBlocks) GetBlockOrAdd(t time.Time) DatabaseBlock {
	b, ok := dbb.elems[t]
	if ok {
		return b
	}
	encoder := dbb.opts.GetEncoderPool().Get()
	encoder.Reset(t, dbb.opts.GetDatabaseBlockAllocSize())
	newBlock := dbb.opts.GetDatabaseBlockPool().Get()
	newBlock.Reset(t, encoder)
	dbb.AddBlock(newBlock)
	return newBlock
}

func (dbb *databaseSeriesBlocks) AllBlocks() map[time.Time]DatabaseBlock {
	return dbb.elems
}

func (dbb *databaseSeriesBlocks) RemoveBlockAt(t time.Time) {
	if _, exists := dbb.elems[t]; !exists {
		return
	}
	delete(dbb.elems, t)
	if !dbb.min.Equal(t) && !dbb.max.Equal(t) {
		return
	}
	dbb.min, dbb.max = timeZero, timeZero
	for k := range dbb.elems {
		if dbb.min == timeZero || dbb.min.After(k) {
			dbb.min = k
		}
		if dbb.max == timeZero || dbb.max.Before(k) {
			dbb.max = k
		}
	}
}

func (dbb *databaseSeriesBlocks) Close() {
	for _, block := range dbb.elems {
		block.Close()
	}
}
