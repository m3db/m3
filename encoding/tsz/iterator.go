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
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/m3db/m3db/interfaces/m3db"
	xtime "github.com/m3db/m3db/x/time"
)

// iterator provides an interface for clients to incrementally
// read datapoints off of an encoded stream.
type iterator struct {
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

// NewIterator returns a new iterator for a given reader
func NewIterator(reader io.Reader, opts Options) m3db.Iterator {
	return &iterator{
		is:   newIStream(reader),
		opts: opts,
		tess: opts.GetTimeEncodingSchemes(),
		mes:  opts.GetMarkerEncodingScheme(),
	}
}

// Next moves to the next item
func (it *iterator) Next() bool {
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

func (it *iterator) readFirstTimestamp() {
	nt := int64(it.readBits(64))
	it.readNextTimestamp()
	st := xtime.FromNormalizedTime(nt, it.timeUnit())
	it.t = st.Add(it.dt)
}

func (it *iterator) readFirstValue() {
	it.vb = it.readBits(64)
	it.xor = it.vb
}

func (it *iterator) readNextTimestamp() {
	dod := it.readMarkerOrDeltaOfDelta()
	it.dt += xtime.FromNormalizedDuration(dod, it.timeUnit())
	it.t = it.t.Add(it.dt)
}

func (it *iterator) tryReadMarker() (int64, bool) {
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

func (it *iterator) readMarkerOrDeltaOfDelta() int64 {
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

func (it *iterator) readDeltaOfDelta(tes TimeEncodingScheme) int64 {
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

func (it *iterator) readNextValue() {
	it.xor = it.readXOR()
	it.vb ^= it.xor
}

func (it *iterator) readAnnotation() {
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

func (it *iterator) readTimeUnit() {
	it.tu = xtime.Unit(it.readBits(8))
}

func (it *iterator) readXOR() uint64 {
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

func (it *iterator) readBits(numBits int) uint64 {
	if !it.hasNext() {
		return 0
	}
	var res uint64
	res, it.err = it.is.ReadBits(numBits)
	return res
}

func (it *iterator) readVarint() int {
	if !it.hasNext() {
		return 0
	}
	var res int64
	res, it.err = binary.ReadVarint(it.is)
	return int(res)
}

func (it *iterator) tryPeekBits(numBits int) (uint64, bool) {
	if !it.hasNext() {
		return 0, false
	}
	res, err := it.is.PeekBits(numBits)
	if err != nil {
		return 0, false
	}
	return res, true
}

func (it *iterator) timeUnit() time.Duration {
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
func (it *iterator) Current() (m3db.Datapoint, xtime.Unit, m3db.Annotation) {
	return m3db.Datapoint{
		Timestamp: it.t,
		Value:     math.Float64frombits(it.vb),
	}, it.tu, it.ant
}

// Err returns the error encountered
func (it *iterator) Err() error {
	return it.err
}

func (it *iterator) hasError() bool {
	return it.err != nil
}

func (it *iterator) isDone() bool {
	return it.done
}

func (it *iterator) isClosed() bool {
	return it.closed
}

func (it *iterator) hasNext() bool {
	return !it.hasError() && !it.isDone() && !it.isClosed()
}

func (it *iterator) Reset(reader io.Reader) {
	it.is.Reset(reader)
	it.t = time.Time{}
	it.dt = 0
	it.vb = 0
	it.xor = 0
	it.done = false
	it.err = nil
	it.ant = nil
	it.closed = false
}

func (it *iterator) Close() {
	if it.closed {
		return
	}
	it.closed = true
	pool := it.opts.GetIteratorPool()
	if pool != nil {
		pool.Put(it)
	}
}
