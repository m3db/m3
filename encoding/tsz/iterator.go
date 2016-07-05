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

package tsz

import (
	"container/heap"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/interfaces/m3db"
	xtime "github.com/m3db/m3db/x/time"
)

// singleReaderIterator provides an interface for clients to incrementally
// read datapoints off of an encoded stream.
type singleReaderIterator struct {
	is   *istream
	opts Options
	tess TimeEncodingSchemes
	mes  MarkerEncodingScheme

	// internal bookkeeping
	t    time.Time     // current time
	dt   time.Duration // current time delta
	vb   uint64        // current value
	xor  uint64        // current xor
	done bool          // has reached the end
	err  error         // current error

	ant m3db.Annotation // current annotation
	tu  xtime.Unit      // current time unit

	closed bool
}

// NewSingleReaderIterator returns a new iterator for a given reader
func NewSingleReaderIterator(reader io.Reader, opts Options) m3db.SingleReaderIterator {
	return &singleReaderIterator{
		is:   newIStream(reader),
		opts: opts,
		tess: opts.GetTimeEncodingSchemes(),
		mes:  opts.GetMarkerEncodingScheme(),
	}
}

// Next moves to the next item
func (it *singleReaderIterator) Next() bool {
	if !it.hasNext() {
		return false
	}
	it.ant = nil
	if it.t.IsZero() {
		it.readFirstTimestamp()
		it.readFirstValue()
	} else {
		it.readNextTimestamp()
		it.readNextValue()
	}
	return it.hasNext()
}

func (it *singleReaderIterator) readFirstTimestamp() {
	nt := int64(it.readBits(64))
	it.readNextTimestamp()
	st := xtime.FromNormalizedTime(nt, it.timeUnit())
	it.t = st.Add(it.dt)
}

func (it *singleReaderIterator) readFirstValue() {
	it.vb = it.readBits(64)
	it.xor = it.vb
}

func (it *singleReaderIterator) readNextTimestamp() {
	dod := it.readMarkerOrDeltaOfDelta()
	it.dt += xtime.FromNormalizedDuration(dod, it.timeUnit())
	it.t = it.t.Add(it.dt)
}

func (it *singleReaderIterator) tryReadMarker() (int64, bool) {
	numBits := it.mes.NumOpcodeBits() + it.mes.NumValueBits()
	opcodeAndValue, success := it.tryPeekBits(numBits)
	if !success {
		return 0, false
	}
	opcode := opcodeAndValue >> uint(it.mes.NumValueBits())
	if opcode != it.mes.Opcode() {
		return 0, false
	}
	valueMask := (1 << uint(it.mes.NumValueBits())) - 1
	value := int64(opcodeAndValue & uint64(valueMask))
	switch Marker(value) {
	case it.mes.EndOfStream():
		it.readBits(numBits)
		it.done = true
	case it.mes.Annotation():
		it.readBits(numBits)
		it.readAnnotation()
		value = it.readMarkerOrDeltaOfDelta()
	case it.mes.TimeUnit():
		it.readBits(numBits)
		it.readTimeUnit()
		value = it.readMarkerOrDeltaOfDelta()
	default:
		return 0, false
	}
	return value, true
}

func (it *singleReaderIterator) readMarkerOrDeltaOfDelta() int64 {
	if dod, success := it.tryReadMarker(); success {
		return dod
	}
	tes, exists := it.tess[it.tu]
	if !exists {
		it.err = fmt.Errorf("time encoding scheme for time unit %v doesn't exist", it.tu)
		return 0
	}
	return it.readDeltaOfDelta(tes)
}

func (it *singleReaderIterator) readDeltaOfDelta(tes TimeEncodingScheme) int64 {
	cb := it.readBits(1)
	if cb == tes.ZeroBucket().Opcode() {
		return 0
	}
	buckets := tes.Buckets()
	for i := 0; i < len(buckets); i++ {
		cb = (cb << 1) | it.readBits(1)
		if cb == buckets[i].Opcode() {
			return signExtend(it.readBits(buckets[i].NumValueBits()), buckets[i].NumValueBits())
		}
	}
	numValueBits := tes.DefaultBucket().NumValueBits()
	return signExtend(it.readBits(numValueBits), numValueBits)
}

func (it *singleReaderIterator) readNextValue() {
	it.xor = it.readXOR()
	it.vb ^= it.xor
}

func (it *singleReaderIterator) readAnnotation() {
	// NB: we add 1 here to offset the 1 we subtracted during encoding
	antLen := it.readVarint() + 1
	if it.hasError() {
		return
	}
	if antLen <= 0 {
		it.err = fmt.Errorf("unexpected annotation length %d", antLen)
		return
	}
	// TODO(xichen): use pool to allocate the buffer once the pool diff lands.
	buf := make([]byte, antLen)
	for i := 0; i < antLen; i++ {
		buf[i] = byte(it.readBits(8))
	}
	it.ant = buf
}

func (it *singleReaderIterator) readTimeUnit() {
	it.tu = xtime.Unit(it.readBits(8))
}

func (it *singleReaderIterator) readXOR() uint64 {
	cb := it.readBits(1)
	if cb == opcodeZeroValueXOR {
		return 0
	}

	cb = (cb << 1) | it.readBits(1)
	if cb == opcodeContainedValueXOR {
		previousLeading, previousTrailing := leadingAndTrailingZeros(it.xor)
		numMeaningfulBits := 64 - previousLeading - previousTrailing
		return it.readBits(numMeaningfulBits) << uint(previousTrailing)
	}

	numLeadingZeros := int(it.readBits(6))
	numMeaningfulBits := int(it.readBits(6)) + 1
	numTrailingZeros := 64 - numLeadingZeros - numMeaningfulBits
	meaningfulBits := it.readBits(numMeaningfulBits)
	return meaningfulBits << uint(numTrailingZeros)
}

func (it *singleReaderIterator) readBits(numBits int) uint64 {
	if !it.hasNext() {
		return 0
	}
	var res uint64
	res, it.err = it.is.ReadBits(numBits)
	return res
}

func (it *singleReaderIterator) readVarint() int {
	if !it.hasNext() {
		return 0
	}
	var res int64
	res, it.err = binary.ReadVarint(it.is)
	return int(res)
}

func (it *singleReaderIterator) tryPeekBits(numBits int) (uint64, bool) {
	if !it.hasNext() {
		return 0, false
	}
	res, err := it.is.PeekBits(numBits)
	if err != nil {
		return 0, false
	}
	return res, true
}

func (it *singleReaderIterator) timeUnit() time.Duration {
	if it.hasError() {
		return 0
	}
	var tu time.Duration
	tu, it.err = it.tu.Value()
	return tu
}

// Current returns the value as well as the annotation associated with the current datapoint.
// Users should not hold on to the returned Annotation object as it may get invalidated when
// the iterator calls Next().
func (it *singleReaderIterator) Current() (m3db.Datapoint, xtime.Unit, m3db.Annotation) {
	return m3db.Datapoint{
		Timestamp: it.t,
		Value:     math.Float64frombits(it.vb),
	}, it.tu, it.ant
}

// Err returns the error encountered
func (it *singleReaderIterator) Err() error {
	return it.err
}

func (it *singleReaderIterator) hasError() bool {
	return it.err != nil
}

func (it *singleReaderIterator) isDone() bool {
	return it.done
}

func (it *singleReaderIterator) isClosed() bool {
	return it.closed
}

func (it *singleReaderIterator) hasNext() bool {
	return !it.hasError() && !it.isDone() && !it.isClosed()
}

func (it *singleReaderIterator) Reset(reader io.Reader) {
	it.is.Reset(reader)
	it.t = time.Time{}
	it.dt = 0
	it.vb = 0
	it.xor = 0
	it.done = false
	it.err = nil
	it.ant = nil
	it.tu = xtime.None
	it.closed = false
}

func (it *singleReaderIterator) Close() {
	if it.closed {
		return
	}
	it.closed = true
	pool := it.opts.GetSingleReaderIteratorPool()
	if pool != nil {
		pool.Put(it)
	}
}

// multiReaderIterator provides an interface for clients to incrementally
// read datapoints off of multiple encoded streams whose datapoints may
// interleave in time.
// TODO(xichen): optimize for one reader case.
type multiReaderIterator struct {
	iters   encoding.IteratorHeap             // a heap of iterators
	readers []io.Reader                       // underlying readers
	alloc   m3db.SingleReaderIteratorAllocate // allocation function for single reader iterators
	opts    Options                           // decoding options
	err     error                             // current error
	closed  bool                              // has been closed
}

// NewMultiReaderIterator creates a new multi-reader iterator.
func NewMultiReaderIterator(readers []io.Reader, opts Options) m3db.MultiReaderIterator {
	alloc := func() m3db.SingleReaderIterator {
		return NewSingleReaderIterator(nil, opts)
	}
	if pool := opts.GetSingleReaderIteratorPool(); pool != nil {
		alloc = pool.Get
	}
	return &multiReaderIterator{
		readers: readers,
		alloc:   alloc,
		opts:    opts,
	}
}

func (it *multiReaderIterator) Next() bool {
	if !it.hasNext() {
		return false
	}
	if it.iters == nil {
		it.initHeap()
	} else {
		it.moveToNext()
	}
	return it.hasNext()
}

func (it *multiReaderIterator) initHeap() {
	iterHeap := make(encoding.IteratorHeap, 0, len(it.readers))
	heap.Init(&iterHeap)
	for i := range it.readers {
		newIt := it.alloc()
		newIt.Reset(it.readers[i])
		if newIt.Next() {
			heap.Push(&iterHeap, newIt)
		} else {
			err := newIt.Err()
			newIt.Close()
			if err != nil {
				it.err = err
				return
			}
		}
	}
	it.iters = iterHeap
}

func (it *multiReaderIterator) moveToNext() {
	earliest := heap.Pop(&it.iters).(m3db.SingleReaderIterator)
	if earliest.Next() {
		heap.Push(&it.iters, earliest)
	} else {
		err := earliest.Err()
		earliest.Close()
		if err != nil {
			it.err = err
		}
	}
}

func (it *multiReaderIterator) Current() (m3db.Datapoint, xtime.Unit, m3db.Annotation) {
	return it.iters[0].Current()
}

func (it *multiReaderIterator) hasError() bool {
	return it.err != nil
}

func (it *multiReaderIterator) isClosed() bool {
	return it.closed
}

func (it *multiReaderIterator) hasMore() bool {
	return it.iters == nil || it.iters.Len() > 0
}

func (it *multiReaderIterator) hasNext() bool {
	return !it.hasError() && !it.isClosed() && it.hasMore()
}

func (it *multiReaderIterator) Err() error {
	return it.err
}

func (it *multiReaderIterator) Reset(readers []io.Reader) {
	it.iters = nil
	it.readers = readers
	it.err = nil
	it.closed = false
}

func (it *multiReaderIterator) Close() {
	if it.closed {
		return
	}
	for i := range it.iters {
		it.iters[i].Close()
	}
	pool := it.opts.GetMultiReaderIteratorPool()
	if pool != nil {
		pool.Put(it)
	}
	it.closed = true
}
