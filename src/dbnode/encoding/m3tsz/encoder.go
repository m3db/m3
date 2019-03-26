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
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/checked"
	xtime "github.com/m3db/m3/src/x/time"
)

var (
	errEncoderClosed       = errors.New("encoder is closed")
	errNoEncodedDatapoints = errors.New("encoder has no encoded datapoints")
)

// Encoder is an M3TSZ encoder that can encode a stream of data in M3TSZ format.
type Encoder struct {
	os   encoding.OStream
	opts encoding.Options

	// internal bookkeeping
	t   time.Time     // current time
	dt  time.Duration // current time delta
	vb  uint64        // current value as float bits
	xor uint64        // current float XOR

	ant ts.Annotation // current annotation

	intVal       float64    // current int val
	tu           xtime.Unit // current time unit
	intOptimized bool       // whether the encoding scheme is optimized for ints
	isFloat      bool       // whether we are encoding ints/floats
	numEncoded   uint32     // whether any datapoints have been written yet
	maxMult      uint8      // current max multiplier for int vals

	sigTracker IntSigBitsTracker

	closed bool
}

// NewEncoder creates a new encoder.
func NewEncoder(
	start time.Time,
	bytes checked.Bytes,
	os encoding.OStream,
	intOptimized bool,
	opts encoding.Options,
) encoding.Encoder {
	if opts == nil {
		opts = encoding.NewOptions()
	}
	// NB(r): only perform an initial allocation if there is no pool that
	// will be used for this encoder.  If a pool is being used alloc when the
	// `Reset` method is called.
	initAllocIfEmpty := opts.EncoderPool() == nil
	tu := initialTimeUnit(start, opts.DefaultTimeUnit())

	if os == nil {
		os = encoding.NewOStream(bytes, initAllocIfEmpty, opts.BytesPool())
	}
	return &Encoder{
		os:           os,
		opts:         opts,
		t:            start,
		tu:           tu,
		closed:       false,
		intOptimized: intOptimized,
	}
}

func initialTimeUnit(start time.Time, tu xtime.Unit) xtime.Unit {
	tv, err := tu.Value()
	if err != nil {
		return xtime.None
	}
	// If we want to use tu as the time unit for start, start must
	// be a multiple of tu.
	startInNano := xtime.ToNormalizedTime(start, time.Nanosecond)
	tvInNano := xtime.ToNormalizedDuration(tv, time.Nanosecond)
	if startInNano%tvInNano == 0 {
		return tu
	}
	return xtime.None
}

// Encode encodes the timestamp and the value of a datapoint.
func (enc *Encoder) Encode(dp ts.Datapoint, tu xtime.Unit, ant ts.Annotation) error {
	if enc.closed {
		return errEncoderClosed
	}

	var err error
	if enc.numEncoded == 0 {
		err = enc.writeFirst(dp, ant, tu)
	} else {
		err = enc.writeNext(dp, ant, tu)
	}
	if err == nil {
		enc.numEncoded++
	}

	return err
}

// writeFirst writes the first datapoint with annotation.
func (enc *Encoder) writeFirst(dp ts.Datapoint, ant ts.Annotation, tu xtime.Unit) error {
	if err := enc.WriteFirstTime(dp.Timestamp, ant, tu); err != nil {
		return err
	}
	enc.writeFirstValue(dp.Value)
	return nil
}

// writeNext writes the next datapoint with annotation.
func (enc *Encoder) writeNext(dp ts.Datapoint, ant ts.Annotation, tu xtime.Unit) error {
	enc.writeAnnotation(ant)
	if err := enc.WriteNextTime(dp.Timestamp, ant, tu); err != nil {
		return err
	}
	enc.writeNextValue(dp.Value)
	return nil
}

// shouldWriteAnnotation determines whether we should write ant as an annotation.
// Returns true if ant is not empty and differs from the existing annotation, false otherwise.
func (enc *Encoder) shouldWriteAnnotation(ant ts.Annotation) bool {
	numAnnotationBytes := len(ant)
	if numAnnotationBytes == 0 {
		return false
	}
	if numAnnotationBytes != len(enc.ant) {
		return true
	}
	// TODO(rartoul): Use bytes.Equal()
	for i := 0; i < numAnnotationBytes; i++ {
		if enc.ant[i] != ant[i] {
			return true
		}
	}
	return false
}

func (enc *Encoder) writeAnnotation(ant ts.Annotation) {
	if !enc.shouldWriteAnnotation(ant) {
		return
	}
	scheme := enc.opts.MarkerEncodingScheme()
	encoding.WriteSpecialMarker(enc.os, scheme, scheme.Annotation())

	var buf [binary.MaxVarintLen32]byte
	// NB: we subtract 1 for possible varint encoding savings
	annotationLength := binary.PutVarint(buf[:], int64(len(ant)-1))

	enc.os.WriteBytes(buf[:annotationLength])
	enc.os.WriteBytes(ant)
	enc.ant = ant
}

// shouldWriteTimeUnit determines whether we should write tu as a time unit.
// Returns true if tu is valid and differs from the existing time unit, false otherwise.
func (enc *Encoder) shouldWriteTimeUnit(tu xtime.Unit) bool {
	if !tu.IsValid() || tu == enc.tu {
		return false
	}
	return true
}

// writeTimeUnit encodes the time unit and returns true if the time unit has
// changed, and false otherwise.
func (enc *Encoder) writeTimeUnit(tu xtime.Unit) bool {
	if !enc.shouldWriteTimeUnit(tu) {
		return false
	}
	scheme := enc.opts.MarkerEncodingScheme()
	encoding.WriteSpecialMarker(enc.os, scheme, scheme.TimeUnit())
	enc.os.WriteByte(byte(tu))
	enc.tu = tu
	return true
}

// WriteFirstTime encodes the first timestamp.
func (enc *Encoder) WriteFirstTime(t time.Time, ant ts.Annotation, tu xtime.Unit) error {
	// NB(xichen): Always write the first time in nanoseconds because we don't know
	// if the start time is going to be a multiple of the time unit provided.
	nt := xtime.ToNormalizedTime(enc.t, time.Nanosecond)
	enc.os.WriteBits(uint64(nt), 64)
	enc.writeAnnotation(ant)
	return enc.WriteNextTime(t, ant, tu)
}

// WriteNextTime encodes the next (non-first) timestamp.
func (enc *Encoder) WriteNextTime(t time.Time, ant ts.Annotation, tu xtime.Unit) error {
	tuChanged := enc.writeTimeUnit(tu)

	dt := t.Sub(enc.t)
	enc.t = t
	if tuChanged {
		enc.writeDeltaOfDeltaTimeUnitChanged(enc.dt, dt)
		// NB(xichen): if the time unit has changed, we reset the time delta to zero
		// because we can't guarantee that dt is a multiple of the new time unit, which
		// means we can't guarantee that the delta of delta when encoding the next
		// data point is a multiple of the new time unit.
		enc.dt = 0
		return nil
	}
	err := enc.writeDeltaOfDeltaTimeUnitUnchanged(enc.dt, dt, tu)
	enc.dt = dt
	return err
}

func (enc *Encoder) writeDeltaOfDeltaTimeUnitChanged(prevDelta, curDelta time.Duration) {
	// NB(xichen): if the time unit has changed, always normalize delta-of-delta
	// to nanoseconds and encode it using 64 bits.
	dodInNano := int64(curDelta - prevDelta)
	enc.os.WriteBits(uint64(dodInNano), 64)
}

func (enc *Encoder) writeDeltaOfDeltaTimeUnitUnchanged(prevDelta, curDelta time.Duration, tu xtime.Unit) error {
	u, err := tu.Value()
	if err != nil {
		return err
	}
	deltaOfDelta := xtime.ToNormalizedDuration(curDelta-prevDelta, u)
	tes, exists := enc.opts.TimeEncodingSchemes()[tu]
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

func (enc *Encoder) writeFirstValue(v float64) error {
	if !enc.intOptimized {
		enc.writeFullFloatVal(math.Float64bits(v))
		return nil
	}

	// Attempt to convert float to int for int optimization
	val, mult, isFloat, err := convertToIntFloat(v, 0)
	if err != nil {
		return err
	}

	if isFloat {
		enc.os.WriteBit(opcodeFloatMode)
		enc.writeFullFloatVal(math.Float64bits(val))
		enc.isFloat = true
		enc.maxMult = mult
		return nil
	}

	// val can be converted to int
	enc.os.WriteBit(opcodeIntMode)
	enc.intVal = val
	negDiff := true
	if val < 0 {
		negDiff = false
		val = -1 * val
	}

	valBits := uint64(int64(val))
	numSig := encoding.NumSig(valBits)
	enc.writeIntSigMult(numSig, mult, false)
	enc.writeIntValDiff(valBits, negDiff)
	return nil
}

func (enc *Encoder) writeNextValue(v float64) error {
	if !enc.intOptimized {
		enc.writeFloatXOR(math.Float64bits(v))
		return nil
	}

	// Attempt to convert float to int for int optimization
	val, mult, isFloat, err := convertToIntFloat(v, enc.maxMult)
	if err != nil {
		return err
	}

	var valDiff float64
	if !isFloat {
		valDiff = enc.intVal - val
	}

	if isFloat || valDiff >= maxInt || valDiff <= minInt {
		enc.writeFloatVal(math.Float64bits(val), mult)
		return nil
	}

	enc.writeIntVal(val, mult, isFloat, valDiff)
	return nil
}

// writeFloatVal writes the value as XOR of the
// bits that represent the float
func (enc *Encoder) writeFloatVal(val uint64, mult uint8) {
	if !enc.isFloat {
		// Converting from int to float
		enc.os.WriteBit(opcodeUpdate)
		enc.os.WriteBit(opcodeNoRepeat)
		enc.os.WriteBit(opcodeFloatMode)
		enc.writeFullFloatVal(val)
		enc.isFloat = true
		enc.maxMult = mult
		return
	}

	if val == enc.vb {
		// Value is repeated
		enc.os.WriteBit(opcodeUpdate)
		enc.os.WriteBit(opcodeRepeat)
		return
	}

	enc.os.WriteBit(opcodeNoUpdate)
	enc.writeFloatXOR(val)
}

// writeFloatVal writes the full 64 bits of the float
func (enc *Encoder) writeFullFloatVal(val uint64) {
	enc.vb = val
	enc.xor = val
	enc.os.WriteBits(val, 64)
}

// writeFloatXOR writes the XOR of the 64bits of the float
func (enc *Encoder) writeFloatXOR(val uint64) {
	xor := enc.vb ^ val
	WriteXOR(enc.os, enc.xor, xor)
	enc.xor = xor
	enc.vb = val
}

// writeIntVal writes the val as a diff of ints
func (enc *Encoder) writeIntVal(val float64, mult uint8, isFloat bool, valDiff float64) {
	if valDiff == 0 && isFloat == enc.isFloat && mult == enc.maxMult {
		// Value is repeated
		enc.os.WriteBit(opcodeUpdate)
		enc.os.WriteBit(opcodeRepeat)
		return
	}

	neg := false
	if valDiff < 0 {
		neg = true
		valDiff = -1 * valDiff
	}

	valDiffBits := uint64(int64(valDiff))
	numSig := encoding.NumSig(valDiffBits)
	newSig := enc.sigTracker.TrackNewSig(numSig)
	isFloatChanged := isFloat != enc.isFloat
	if mult > enc.maxMult || enc.sigTracker.NumSig != newSig || isFloatChanged {
		enc.os.WriteBit(opcodeUpdate)
		enc.os.WriteBit(opcodeNoRepeat)
		enc.os.WriteBit(opcodeIntMode)
		enc.writeIntSigMult(newSig, mult, isFloatChanged)
		enc.writeIntValDiff(valDiffBits, neg)
		enc.isFloat = false
	} else {
		enc.os.WriteBit(opcodeNoUpdate)
		enc.writeIntValDiff(valDiffBits, neg)
	}

	enc.intVal = val
}

// writeIntValDiff writes the provided val diff bits along with
// whether the bits are negative or not
func (enc *Encoder) writeIntValDiff(valBits uint64, neg bool) {
	if neg {
		enc.os.WriteBit(opcodeNegative)
	} else {
		enc.os.WriteBit(opcodePositive)
	}

	enc.os.WriteBits(valBits, int(enc.sigTracker.NumSig))
}

// writeIntSigMult writes the number of significant
// bits of the diff and the multiplier if they have changed
func (enc *Encoder) writeIntSigMult(sig, mult uint8, floatChanged bool) {
	WriteIntSig(enc.os, &enc.sigTracker, sig)

	if mult > enc.maxMult {
		enc.os.WriteBit(opcodeUpdateMult)
		enc.os.WriteBits(uint64(mult), numMultBits)
		enc.maxMult = mult
	} else if enc.sigTracker.NumSig == sig && enc.maxMult == mult && floatChanged {
		// If only the float mode has changed, update the Mult regardless
		// so that we can support the annotation peek
		enc.os.WriteBit(opcodeUpdateMult)
		enc.os.WriteBits(uint64(enc.maxMult), numMultBits)
	} else {
		enc.os.WriteBit(opcodeNoUpdateMult)
	}
}

func (enc *Encoder) newBuffer(capacity int) checked.Bytes {
	if bytesPool := enc.opts.BytesPool(); bytesPool != nil {
		return bytesPool.Get(capacity)
	}
	return checked.NewBytes(make([]byte, 0, capacity), nil)
}

// Reset resets the encoder for reuse.
func (enc *Encoder) Reset(start time.Time, capacity int) {
	enc.reset(start, enc.newBuffer(capacity))
}

func (enc *Encoder) reset(start time.Time, bytes checked.Bytes) {
	enc.os.Reset(bytes)
	enc.t = start
	enc.dt = 0
	enc.vb = 0
	enc.xor = 0
	enc.intVal = 0
	enc.isFloat = false
	enc.maxMult = 0
	enc.sigTracker.Reset()
	enc.ant = nil
	enc.tu = initialTimeUnit(start, enc.opts.DefaultTimeUnit())
	enc.numEncoded = 0
	enc.closed = false
}

// Stream returns a copy of the underlying data stream.
func (enc *Encoder) Stream() xio.SegmentReader {
	segment := enc.segment(byCopyResultType)
	if segment.Len() == 0 {
		return nil
	}
	if readerPool := enc.opts.SegmentReaderPool(); readerPool != nil {
		reader := readerPool.Get()
		reader.Reset(segment)
		return reader
	}
	return xio.NewSegmentReader(segment)
}

// NumEncoded returns the number of encoded datapoints.
func (enc *Encoder) NumEncoded() int {
	return int(enc.numEncoded)
}

// LastEncoded returns the last encoded datapoint.
func (enc *Encoder) LastEncoded() (ts.Datapoint, error) {
	if enc.numEncoded == 0 {
		return ts.Datapoint{}, errNoEncodedDatapoints
	}

	result := ts.Datapoint{Timestamp: enc.t}
	if enc.isFloat {
		result.Value = math.Float64frombits(enc.vb)
	} else {
		result.Value = enc.intVal
	}
	return result, nil
}

// Len returns the length of the data stream.
func (enc *Encoder) Len() int {
	return enc.os.Len()
}

// Close closes the encoder.
func (enc *Encoder) Close() {
	if enc.closed {
		return
	}

	enc.closed = true

	// Ensure to free ref to ostream bytes
	enc.os.Reset(nil)

	if pool := enc.opts.EncoderPool(); pool != nil {
		pool.Put(enc)
	}
}

func (enc *Encoder) discard() ts.Segment {
	return enc.segment(byRefResultType)
}

// Discard closes the encoder and transfers ownership of the data stream to
// the caller.
func (enc *Encoder) Discard() ts.Segment {
	segment := enc.discard()

	// Close the encoder no longer needed
	enc.Close()

	return segment
}

// DiscardReset does the same thing as Discard except it also resets the encoder
// for reuse.
func (enc *Encoder) DiscardReset(start time.Time, capacity int) ts.Segment {
	segment := enc.discard()
	enc.Reset(start, capacity)
	return segment
}

func (enc *Encoder) segment(resType resultType) ts.Segment {
	length := enc.os.Len()
	if length == 0 {
		return ts.Segment{}
	}

	// We need a multibyte tail to capture an immutable snapshot
	// of the encoder data.
	var head checked.Bytes
	buffer, pos := enc.os.Rawbytes()
	lastByte := buffer[length-1]
	if resType == byRefResultType {
		// Take ref from the ostream
		head = enc.os.Discard()

		// Resize to crop out last byte
		head.IncRef()
		defer head.DecRef()

		head.Resize(length - 1)
	} else {
		// Copy into new buffer
		head = enc.newBuffer(length - 1)

		head.IncRef()
		defer head.DecRef()

		// Copy up to last byte
		head.AppendAll(buffer[:length-1])
	}

	// Take a shared ref to a known good tail
	scheme := enc.opts.MarkerEncodingScheme()
	tail := scheme.Tail(lastByte, pos)

	// NB(r): Finalize the head bytes whether this is by ref or copy. If by
	// ref we have no ref to it anymore and if by copy then the owner should
	// be finalizing the bytes when the segment is finalized.
	return ts.NewSegment(head, tail, ts.FinalizeHead)
}

type resultType int

const (
	byCopyResultType resultType = iota
	byRefResultType
)
