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
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/m3db/m3db/interfaces/m3db"
	xio "github.com/m3db/m3db/x/io"
	xtime "github.com/m3db/m3db/x/time"
)

var (
	errEncoderNotWritable = errors.New("encoder is not writable")
)

type encoder struct {
	os   *ostream
	opts Options

	// internal bookkeeping
	t   time.Time       // current time
	dt  time.Duration   // current time delta
	vb  uint64          // current value
	xor uint64          // current xor
	ant m3db.Annotation // current annotation
	tu  xtime.Unit      // current time unit

	writable bool
	closed   bool
}

// NewEncoder creates a new encoder.
func NewEncoder(start time.Time, bytes []byte, opts Options) m3db.Encoder {
	if opts == nil {
		opts = NewOptions()
	}
	// NB(r): only perform an initial allocation if there is no pool that
	// will be used for this encoder.  If a pool is being used alloc when the
	// `Reset` method is called.
	initAllocIfEmpty := opts.GetEncoderPool() == nil
	return &encoder{
		os:       newOStream(bytes, initAllocIfEmpty),
		opts:     opts,
		t:        start,
		writable: true,
		closed:   false,
	}
}

// Encode encodes the timestamp and the value of a datapoint.
func (enc *encoder) Encode(dp m3db.Datapoint, tu xtime.Unit, ant m3db.Annotation) error {
	if !enc.writable {
		return errEncoderNotWritable
	}
	if enc.os.len() == 0 {
		return enc.writeFirst(dp, ant, tu)
	}
	return enc.writeNext(dp, ant, tu)
}

// writeFirst writes the first datapoint with annotation.
func (enc *encoder) writeFirst(dp m3db.Datapoint, ant m3db.Annotation, tu xtime.Unit) error {
	if err := enc.writeFirstTime(dp.Timestamp, ant, tu); err != nil {
		return err
	}
	enc.writeFirstValue(dp.Value)
	return nil
}

// writeNext writes the next datapoint with annotation.
func (enc *encoder) writeNext(dp m3db.Datapoint, ant m3db.Annotation, tu xtime.Unit) error {
	if err := enc.writeNextTime(dp.Timestamp, ant, tu); err != nil {
		return err
	}
	enc.writeNextValue(dp.Value)
	return nil
}

// shouldWriteAnnotation determines whether we should write ant as an annotation.
// Returns true if ant is not empty and differs from the existing annotation, false otherwise.
func (enc *encoder) shouldWriteAnnotation(ant m3db.Annotation) bool {
	numAnnotationBytes := len(ant)
	if numAnnotationBytes == 0 {
		return false
	}
	if numAnnotationBytes != len(enc.ant) {
		return true
	}
	for i := 0; i < numAnnotationBytes; i++ {
		if enc.ant[i] != ant[i] {
			return true
		}
	}
	return false
}

func (enc *encoder) writeAnnotation(ant m3db.Annotation) {
	if !enc.shouldWriteAnnotation(ant) {
		return
	}
	scheme := enc.opts.GetMarkerEncodingScheme()
	writeSpecialMarker(enc.os, scheme, scheme.Annotation())

	var buf [binary.MaxVarintLen32]byte
	// NB: we subtract 1 for possible varint encoding savings
	annotationLength := binary.PutVarint(buf[:], int64(len(ant)-1))
	enc.os.WriteBytes(buf[:annotationLength])
	enc.os.WriteBytes(ant)
	enc.ant = ant
}

// shouldWriteTimeUnit determines whether we should write tu as a time unit.
// Returns true if tu is valid and differs from the existing time unit, false otherwise.
func (enc *encoder) shouldWriteTimeUnit(tu xtime.Unit) bool {
	if !tu.IsValid() || tu == enc.tu {
		return false
	}
	return true
}

func (enc *encoder) writeTimeUnit(tu xtime.Unit) {
	if !enc.shouldWriteTimeUnit(tu) {
		return
	}
	scheme := enc.opts.GetMarkerEncodingScheme()
	writeSpecialMarker(enc.os, scheme, scheme.TimeUnit())
	enc.os.WriteByte(byte(tu))
	enc.tu = tu
}

func (enc *encoder) writeFirstTime(t time.Time, ant m3db.Annotation, tu xtime.Unit) error {
	u, err := tu.Value()
	if err != nil {
		return err
	}
	nt := xtime.ToNormalizedTime(enc.t, u)
	enc.os.WriteBits(uint64(nt), 64)
	return enc.writeNextTime(t, ant, tu)
}

func (enc *encoder) writeNextTime(t time.Time, ant m3db.Annotation, tu xtime.Unit) error {
	enc.writeAnnotation(ant)
	enc.writeTimeUnit(tu)

	dt := t.Sub(enc.t)
	if err := enc.writeDeltaOfDelta(enc.dt, dt, tu); err != nil {
		return err
	}
	enc.t = t
	enc.dt = dt

	return nil
}

func (enc *encoder) writeDeltaOfDelta(prevDelta, curDelta time.Duration, tu xtime.Unit) error {
	u, err := tu.Value()
	if err != nil {
		return err
	}
	deltaOfDelta := xtime.ToNormalizedDuration(curDelta-prevDelta, u)
	tes, exists := enc.opts.GetTimeEncodingSchemes()[tu]
	if !exists {
		return fmt.Errorf("time encoding scheme for time unit %v doesn't exist", tu)
	}

	if deltaOfDelta == 0 {
		zeroBucket := tes.ZeroBucket()
		enc.os.WriteBits(zeroBucket.Opcode(), zeroBucket.NumOpcodeBits())
		return nil
	}
	buckets := tes.Buckets()
	for i := 0; i < len(buckets); i++ {
		if deltaOfDelta >= buckets[i].Min() && deltaOfDelta <= buckets[i].Max() {
			enc.os.WriteBits(buckets[i].Opcode(), buckets[i].NumOpcodeBits())
			enc.os.WriteBits(uint64(deltaOfDelta), buckets[i].NumValueBits())
			return nil
		}
	}
	defaultBucket := tes.DefaultBucket()
	enc.os.WriteBits(defaultBucket.Opcode(), defaultBucket.NumOpcodeBits())
	enc.os.WriteBits(uint64(deltaOfDelta), defaultBucket.NumValueBits())
	return nil
}

func (enc *encoder) writeFirstValue(v float64) {
	enc.vb = math.Float64bits(v)
	enc.xor = enc.vb
	enc.os.WriteBits(enc.vb, 64)
}

func (enc *encoder) writeNextValue(v float64) {
	vb := math.Float64bits(v)
	xor := enc.vb ^ vb
	enc.writeXOR(enc.xor, xor)
	enc.vb = vb
	enc.xor = xor
}

func (enc *encoder) writeXOR(prevXOR, curXOR uint64) {
	if curXOR == 0 {
		enc.os.WriteBits(opcodeZeroValueXOR, 1)
		return
	}

	// NB(xichen): can be further optimized by keeping track of leading and trailing zeros in enc.
	prevLeading, prevTrailing := leadingAndTrailingZeros(prevXOR)
	curLeading, curTrailing := leadingAndTrailingZeros(curXOR)
	if curLeading >= prevLeading && curTrailing >= prevTrailing {
		enc.os.WriteBits(opcodeContainedValueXOR, 2)
		enc.os.WriteBits(curXOR>>uint(prevTrailing), 64-prevLeading-prevTrailing)
		return
	}
	enc.os.WriteBits(opcodeUncontainedValueXOR, 2)
	enc.os.WriteBits(uint64(curLeading), 6)
	numMeaningfulBits := 64 - curLeading - curTrailing
	// numMeaningfulBits is at least 1, so we can subtract 1 from it and encode it in 6 bits
	enc.os.WriteBits(uint64(numMeaningfulBits-1), 6)
	enc.os.WriteBits(curXOR>>uint(curTrailing), numMeaningfulBits)
}

func (enc *encoder) Reset(start time.Time, capacity int) {
	var newBuffer []byte
	bytesPool := enc.opts.GetBytesPool()
	if bytesPool != nil {
		newBuffer = bytesPool.Get(capacity)
	} else {
		newBuffer = make([]byte, 0, capacity)
	}
	enc.ResetSetData(start, newBuffer, true)
}

func (enc *encoder) ResetSetData(start time.Time, data []byte, writable bool) {
	enc.os.Reset(data)
	enc.t = start
	enc.dt = 0
	enc.vb = 0
	enc.xor = 0
	enc.ant = nil
	enc.tu = xtime.None
	enc.closed = false
	enc.writable = writable
}

func (enc *encoder) Stream() m3db.SegmentReader {
	if enc.os.empty() {
		return nil
	}
	b, pos := enc.os.rawbytes()
	blen := len(b)
	head := b

	var tail []byte
	if enc.writable {
		// Only if still writable do we need a multibyte tail,
		// otherwise the tail has already been written to the underlying
		// stream by `Done`.
		head = b[:blen-1]

		scheme := enc.opts.GetMarkerEncodingScheme()
		tail = scheme.Tail(b[blen-1], pos)
	}

	readerPool := enc.opts.GetSegmentReaderPool()
	if readerPool != nil {
		reader := readerPool.Get()
		reader.Reset(m3db.Segment{Head: head, Tail: tail})
		return reader
	}
	return xio.NewSegmentReader(m3db.Segment{Head: head, Tail: tail})
}

func (enc *encoder) Done() {
	if !enc.writable {
		// Already written the tail
		return
	}
	enc.writable = false

	if enc.os.empty() {
		return
	}

	b, pos := enc.os.rawbytes()
	blen := len(b)

	scheme := enc.opts.GetMarkerEncodingScheme()
	tail := scheme.Tail(b[blen-1], pos)

	// Trim to before last byte
	enc.os.Reset(b[:blen-1])

	// Append the tail including contents of the last byte
	enc.os.WriteBytes(tail)
}

func (enc *encoder) Close() {
	if enc.closed {
		return
	}
	enc.writable = false
	enc.closed = true

	bytesPool := enc.opts.GetBytesPool()
	if bytesPool != nil {
		buffer, _ := enc.os.rawbytes()

		// Reset the ostream to avoid reusing this encoder
		// using the buffer we are returning to the pool
		enc.os.Reset(nil)

		bytesPool.Put(buffer)
	}

	pool := enc.opts.GetEncoderPool()
	if pool != nil {
		pool.Put(enc)
	}
}

// writeSpecialMarker writes the marker that marks the start of a special symbol,
// e.g., the eos marker, the annotation marker, or the time unit marker.
func writeSpecialMarker(os *ostream, scheme MarkerEncodingScheme, marker Marker) {
	os.WriteBits(scheme.Opcode(), scheme.NumOpcodeBits())
	os.WriteBits(uint64(marker), scheme.NumValueBits())
}
