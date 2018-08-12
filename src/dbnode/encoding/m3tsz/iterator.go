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

package m3tsz

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	xtime "github.com/m3db/m3x/time"
)

// readerIterator provides an interface for clients to incrementally
// read datapoints off of an encoded stream.
type readerIterator struct {
	is   encoding.IStream
	opts encoding.Options
	tess encoding.TimeEncodingSchemes
	mes  encoding.MarkerEncodingScheme

	// internal bookkeeping
	t   time.Time     // current time
	dt  time.Duration // current time delta
	vb  uint64        // current float value
	xor uint64        // current float xor
	err error         // current error

	intVal float64 // current int value

	ant ts.Annotation // current annotation
	tu  xtime.Unit    // current time unit

	mult uint8 // current int multiplier
	sig  uint8 // current number of significant bits for int diff

	intOptimized bool // whether encoding scheme is optimized for ints
	isFloat      bool // whether encoding is in int or float

	tuChanged bool // whether we have a new time unit
	done      bool // has reached the end
	closed    bool
}

// NewReaderIterator returns a new iterator for a given reader
func NewReaderIterator(reader io.Reader, intOptimized bool, opts encoding.Options) encoding.ReaderIterator {
	return &readerIterator{
		is:           encoding.NewIStream(reader),
		opts:         opts,
		tess:         opts.TimeEncodingSchemes(),
		mes:          opts.MarkerEncodingScheme(),
		intOptimized: intOptimized,
	}
}

// Next moves to the next item
func (it *readerIterator) Next() bool {
	if !it.hasNext() {
		return false
	}
	it.ant = nil
	it.tuChanged = false
	if it.t.IsZero() {
		it.readFirstTimestamp()
		it.readFirstValue()
	} else {
		it.readNextTimestamp()
		it.readNextValue()
	}
	// NB(xichen): reset time delta to 0 when there is a time unit change to be
	// consistent with the encoder.
	if it.tuChanged {
		it.dt = 0
	}

	return it.hasNext()
}

func (it *readerIterator) readFirstTimestamp() {
	nt := int64(it.readBits(64))
	// NB(xichen): first time stamp is always normalized to nanoseconds.
	st := xtime.FromNormalizedTime(nt, time.Nanosecond)
	it.tu = initialTimeUnit(st, it.opts.DefaultTimeUnit())
	it.readNextTimestamp()
	it.t = st.Add(it.dt)
}

func (it *readerIterator) readNextTimestamp() {
	it.dt += it.readMarkerOrDeltaOfDelta()
	it.t = it.t.Add(it.dt)
}

func (it *readerIterator) tryReadMarker() (time.Duration, bool) {
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
	markerValue := int64(opcodeAndValue & uint64(valueMask))
	switch encoding.Marker(markerValue) {
	case it.mes.EndOfStream():
		it.readBits(numBits)
		it.done = true
		return 0, true
	case it.mes.Annotation():
		it.readBits(numBits)
		it.readAnnotation()
		return it.readMarkerOrDeltaOfDelta(), true
	case it.mes.TimeUnit():
		it.readBits(numBits)
		it.readTimeUnit()
		return it.readMarkerOrDeltaOfDelta(), true
	default:
		return 0, false
	}
}

func (it *readerIterator) readMarkerOrDeltaOfDelta() time.Duration {
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

func (it *readerIterator) readDeltaOfDelta(tes encoding.TimeEncodingScheme) (d time.Duration) {
	if it.tuChanged {
		// NB(xichen): if the time unit has changed, always read 64 bits as normalized
		// dod in nanoseconds.
		dod := encoding.SignExtend(it.readBits(64), 64)
		return time.Duration(dod)
	}

	cb := it.readBits(1)
	if cb == tes.ZeroBucket().Opcode() {
		return 0
	}
	buckets := tes.Buckets()
	for i := 0; i < len(buckets); i++ {
		cb = (cb << 1) | it.readBits(1)
		if cb == buckets[i].Opcode() {
			dod := encoding.SignExtend(it.readBits(buckets[i].NumValueBits()), buckets[i].NumValueBits())
			return xtime.FromNormalizedDuration(dod, it.timeUnit())
		}
	}
	numValueBits := tes.DefaultBucket().NumValueBits()
	dod := encoding.SignExtend(it.readBits(numValueBits), numValueBits)
	return xtime.FromNormalizedDuration(dod, it.timeUnit())
}

func (it *readerIterator) readFirstValue() {
	if !it.intOptimized {
		it.readFullFloatVal()
		return
	}

	if it.readBits(1) == opcodeFloatMode {
		it.readFullFloatVal()
		it.isFloat = true
		return
	}

	it.readIntSigMult()
	it.readIntValDiff()
}

func (it *readerIterator) readNextValue() {
	if !it.intOptimized {
		it.readFloatXOR()
		return
	}

	if it.readBits(1) == opcodeUpdate {
		if it.readBits(1) == opcodeRepeat {
			return
		}

		if it.readBits(1) == opcodeFloatMode {
			// Change to floatVal
			it.readFullFloatVal()
			it.isFloat = true
			return
		}

		it.readIntSigMult()
		it.readIntValDiff()
		it.isFloat = false
		return
	}

	if it.isFloat {
		it.readFloatXOR()
	} else {
		it.readIntValDiff()
	}
}

func (it *readerIterator) readFullFloatVal() {
	it.vb = it.readBits(64)
	it.xor = it.vb
}

func (it *readerIterator) readFloatXOR() {
	it.xor = it.readXOR()
	it.vb ^= it.xor
}

func (it *readerIterator) readIntSigMult() {
	if it.readBits(1) == opcodeUpdateSig {
		if it.readBits(1) == opcodeZeroSig {
			it.sig = 0
		} else {
			it.sig = uint8(it.readBits(numSigBits)) + 1
		}
	}

	if it.readBits(1) == opcodeUpdateMult {
		it.mult = uint8(it.readBits(numMultBits))
		if it.mult > maxMult {
			it.err = errInvalidMultiplier
		}
	}
}

func (it *readerIterator) readIntValDiff() {
	sign := -1.0
	if it.readBits(1) == opcodeNegative {
		sign = 1.0
	}

	it.intVal += sign * float64(it.readBits(int(it.sig)))
}

func (it *readerIterator) readAnnotation() {
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

func (it *readerIterator) readTimeUnit() {
	tu := xtime.Unit(it.readBits(8))
	if tu.IsValid() && tu != it.tu {
		it.tuChanged = true
	}
	it.tu = tu
}

func (it *readerIterator) readXOR() uint64 {
	cb := it.readBits(1)
	if cb == opcodeZeroValueXOR {
		return 0
	}

	cb = (cb << 1) | it.readBits(1)
	if cb == opcodeContainedValueXOR {
		previousLeading, previousTrailing := encoding.LeadingAndTrailingZeros(it.xor)
		numMeaningfulBits := 64 - previousLeading - previousTrailing
		return it.readBits(numMeaningfulBits) << uint(previousTrailing)
	}

	numLeadingZeros := int(it.readBits(6))
	numMeaningfulBits := int(it.readBits(6)) + 1
	numTrailingZeros := 64 - numLeadingZeros - numMeaningfulBits
	meaningfulBits := it.readBits(numMeaningfulBits)
	return meaningfulBits << uint(numTrailingZeros)
}

func (it *readerIterator) readBits(numBits int) uint64 {
	if !it.hasNext() {
		return 0
	}
	var res uint64
	res, it.err = it.is.ReadBits(numBits)
	return res
}

func (it *readerIterator) readVarint() int {
	if !it.hasNext() {
		return 0
	}
	var res int64
	res, it.err = binary.ReadVarint(it.is)
	return int(res)
}

func (it *readerIterator) tryPeekBits(numBits int) (uint64, bool) {
	if !it.hasNext() {
		return 0, false
	}
	res, err := it.is.PeekBits(numBits)
	if err != nil {
		return 0, false
	}
	return res, true
}

func (it *readerIterator) timeUnit() time.Duration {
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
func (it *readerIterator) Current() (ts.Datapoint, xtime.Unit, ts.Annotation) {
	if !it.intOptimized || it.isFloat {
		return ts.Datapoint{
			Timestamp: it.t,
			Value:     math.Float64frombits(it.vb),
		}, it.tu, it.ant
	}

	return ts.Datapoint{
		Timestamp: it.t,
		Value:     convertFromIntFloat(it.intVal, it.mult),
	}, it.tu, it.ant
}

// Err returns the error encountered
func (it *readerIterator) Err() error {
	return it.err
}

func (it *readerIterator) hasError() bool {
	return it.err != nil
}

func (it *readerIterator) isDone() bool {
	return it.done
}

func (it *readerIterator) isClosed() bool {
	return it.closed
}

func (it *readerIterator) hasNext() bool {
	return !it.hasError() && !it.isDone() && !it.isClosed()
}

func (it *readerIterator) Reset(reader io.Reader) {
	it.is.Reset(reader)
	it.t = time.Time{}
	it.dt = 0
	it.vb = 0
	it.xor = 0
	it.done = false
	it.err = nil
	it.ant = nil
	it.isFloat = false
	it.intVal = 0.0
	it.mult = 0
	it.sig = 0
	it.tu = xtime.None
	it.closed = false
}

func (it *readerIterator) Close() {
	if it.closed {
		return
	}
	it.closed = true
	pool := it.opts.ReaderIteratorPool()
	if pool != nil {
		pool.Put(it)
	}
}
