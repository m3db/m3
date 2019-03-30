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
	"bytes"
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

// encoder is an M3TSZ encoder that can encode a stream of data in M3TSZ format.
type encoder struct {
	os   encoding.OStream
	opts encoding.Options

	// internal bookkeeping
	tsEncoderState  TimestampEncoder
	xorEncoderState XOREncoder

	ant ts.Annotation // current annotation

	intVal       float64 // current int val
	intOptimized bool    // whether the encoding scheme is optimized for ints
	isFloat      bool    // whether we are encoding ints/floats
	numEncoded   uint32  // whether any datapoints have been written yet
	maxMult      uint8   // current max multiplier for int vals

	sigTracker IntSigBitsTracker

	closed bool
}

// NewEncoder creates a new encoder.
func NewEncoder(
	start time.Time,
	bytes checked.Bytes,
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
	return &encoder{
		os:             encoding.NewOStream(bytes, initAllocIfEmpty, opts.BytesPool()),
		opts:           opts,
		tsEncoderState: NewTimestampEncoder(start, opts.DefaultTimeUnit(), opts),
		closed:         false,
		intOptimized:   intOptimized,
	}
}

// Encode encodes the timestamp and the value of a datapoint.
func (enc *encoder) Encode(dp ts.Datapoint, tu xtime.Unit, ant ts.Annotation) error {
	if enc.closed {
		return errEncoderClosed
	}

	var err error
	if err = enc.tsEncoderState.WriteTime(enc.os, dp.Timestamp, ant, tu); err != nil {
		return err
	}

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
func (enc *encoder) writeFirst(dp ts.Datapoint, ant ts.Annotation, tu xtime.Unit) error {
	enc.writeFirstValue(dp.Value)
	return nil
}

// writeNext writes the next datapoint with annotation.
func (enc *encoder) writeNext(dp ts.Datapoint, ant ts.Annotation, tu xtime.Unit) error {
	enc.writeNextValue(dp.Value)
	return nil
}

func (enc *encoder) writeFirstValue(v float64) error {
	if !enc.intOptimized {
		enc.xorEncoderState.WriteFullFloatVal(enc.os, math.Float64bits(v))
		return nil
	}

	// Attempt to convert float to int for int optimization
	val, mult, isFloat, err := convertToIntFloat(v, 0)
	if err != nil {
		return err
	}

	if isFloat {
		enc.os.WriteBit(opcodeFloatMode)
		enc.xorEncoderState.WriteFullFloatVal(enc.os, math.Float64bits(v))
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
	enc.sigTracker.WriteIntValDiff(enc.os, valBits, negDiff)
	return nil
}

func (enc *encoder) writeNextValue(v float64) error {
	if !enc.intOptimized {
		enc.xorEncoderState.WriteFloatXOR(enc.os, math.Float64bits(v))
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
func (enc *encoder) writeFloatVal(val uint64, mult uint8) {
	if !enc.isFloat {
		// Converting from int to float
		enc.os.WriteBit(opcodeUpdate)
		enc.os.WriteBit(opcodeNoRepeat)
		enc.os.WriteBit(opcodeFloatMode)
		enc.xorEncoderState.WriteFullFloatVal(enc.os, val)
		enc.isFloat = true
		enc.maxMult = mult
		return
	}

	if val == enc.xorEncoderState.PrevFloatBits {
		// Value is repeated
		enc.os.WriteBit(opcodeUpdate)
		enc.os.WriteBit(opcodeRepeat)
		return
	}

	enc.os.WriteBit(opcodeNoUpdate)
	enc.xorEncoderState.WriteFloatXOR(enc.os, val)
}

// writeIntVal writes the val as a diff of ints
func (enc *encoder) writeIntVal(val float64, mult uint8, isFloat bool, valDiff float64) {
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
		enc.sigTracker.WriteIntValDiff(enc.os, valDiffBits, neg)
		enc.isFloat = false
	} else {
		enc.os.WriteBit(opcodeNoUpdate)
		enc.sigTracker.WriteIntValDiff(enc.os, valDiffBits, neg)
	}

	enc.intVal = val
}

// writeIntSigMult writes the number of significant
// bits of the diff and the multiplier if they have changed
func (enc *encoder) writeIntSigMult(sig, mult uint8, floatChanged bool) {
	enc.sigTracker.WriteIntSig(enc.os, sig)

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

func (enc *encoder) newBuffer(capacity int) checked.Bytes {
	if bytesPool := enc.opts.BytesPool(); bytesPool != nil {
		return bytesPool.Get(capacity)
	}
	return checked.NewBytes(make([]byte, 0, capacity), nil)
}

// Reset resets the encoder for reuse.
func (enc *encoder) Reset(start time.Time, capacity int) {
	enc.reset(start, enc.newBuffer(capacity))
}

func (enc *encoder) reset(start time.Time, bytes checked.Bytes) {
	enc.os.Reset(bytes)

	timeUnit := initialTimeUnit(start, enc.opts.DefaultTimeUnit())
	enc.tsEncoderState = NewTimestampEncoder(start, timeUnit, enc.opts)

	enc.xorEncoderState = XOREncoder{}
	enc.intVal = 0
	enc.isFloat = false
	enc.maxMult = 0
	enc.sigTracker.Reset()
	enc.ant = nil
	enc.numEncoded = 0
	enc.closed = false
}

// Stream returns a copy of the underlying data stream.
func (enc *encoder) Stream() xio.SegmentReader {
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
func (enc *encoder) NumEncoded() int {
	return int(enc.numEncoded)
}

// LastEncoded returns the last encoded datapoint.
func (enc *encoder) LastEncoded() (ts.Datapoint, error) {
	if enc.numEncoded == 0 {
		return ts.Datapoint{}, errNoEncodedDatapoints
	}

	result := ts.Datapoint{Timestamp: enc.tsEncoderState.PrevTime}
	if enc.isFloat {
		result.Value = math.Float64frombits(enc.xorEncoderState.PrevFloatBits)
	} else {
		result.Value = enc.intVal
	}
	return result, nil
}

// Len returns the length of the data stream.
func (enc *encoder) Len() int {
	return enc.os.Len()
}

// Close closes the encoder.
func (enc *encoder) Close() {
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

func (enc *encoder) discard() ts.Segment {
	return enc.segment(byRefResultType)
}

// Discard closes the encoder and transfers ownership of the data stream to
// the caller.
func (enc *encoder) Discard() ts.Segment {
	segment := enc.discard()

	// Close the encoder no longer needed
	enc.Close()

	return segment
}

// DiscardReset does the same thing as Discard except it also resets the encoder
// for reuse.
func (enc *encoder) DiscardReset(start time.Time, capacity int) ts.Segment {
	segment := enc.discard()
	enc.Reset(start, capacity)
	return segment
}

func (enc *encoder) segment(resType resultType) ts.Segment {
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

// TimestampEncoder encapsulates the state required for a logical stream of
// bits that represent a stream of timestamps compressed using delta-of-delta
type TimestampEncoder struct {
	PrevTime       time.Time
	PrevTimeDelta  time.Duration
	PrevAnnotation []byte

	TimeUnit xtime.Unit

	Opts encoding.Options

	hasWrittenFirst bool // Only taken into account if using the WriteTime() API.
}

// NewTimestampEncoder creates a new TimestampEncoder.
func NewTimestampEncoder(
	start time.Time, timeUnit xtime.Unit, opts encoding.Options) TimestampEncoder {
	return TimestampEncoder{
		PrevTime: start,
		TimeUnit: initialTimeUnit(start, timeUnit),
		Opts:     opts,
	}
}

// WriteTime encode the timestamp using delta-of-delta compression.
func (enc *TimestampEncoder) WriteTime(
	stream encoding.OStream, currTime time.Time, ant ts.Annotation, timeUnit xtime.Unit) error {
	if enc.hasWrittenFirst {
		return enc.WriteNextTime(stream, currTime, ant, timeUnit)
	}

	if err := enc.WriteFirstTime(stream, currTime, ant, timeUnit); err != nil {
		return err
	}

	enc.hasWrittenFirst = true
	return nil
}

// WriteFirstTime encodes the first timestamp.
func (enc *TimestampEncoder) WriteFirstTime(
	stream encoding.OStream, currTime time.Time, ant ts.Annotation, timeUnit xtime.Unit) error {
	// NB(xichen): Always write the first time in nanoseconds because we don't know
	// if the start time is going to be a multiple of the time unit provided.
	nt := xtime.ToNormalizedTime(enc.PrevTime, time.Nanosecond)
	stream.WriteBits(uint64(nt), 64)
	return enc.WriteNextTime(stream, currTime, ant, timeUnit)
}

// WriteNextTime encodes the next (non-first) timestamp.
func (enc *TimestampEncoder) WriteNextTime(
	stream encoding.OStream, currTime time.Time, ant ts.Annotation, timeUnit xtime.Unit) error {
	enc.writeAnnotation(stream, ant)
	tuChanged := enc.writeTimeUnit(stream, timeUnit)

	timeDelta := currTime.Sub(enc.PrevTime)
	enc.PrevTime = currTime
	if tuChanged {
		enc.writeDeltaOfDeltaTimeUnitChanged(stream, enc.PrevTimeDelta, timeDelta)
		// NB(xichen): if the time unit has changed, we reset the time delta to zero
		// because we can't guarantee that dt is a multiple of the new time unit, which
		// means we can't guarantee that the delta of delta when encoding the next
		// data point is a multiple of the new time unit.
		enc.PrevTimeDelta = 0
		return nil
	}
	err := enc.writeDeltaOfDeltaTimeUnitUnchanged(
		stream, enc.PrevTimeDelta, timeDelta, timeUnit)
	enc.PrevTimeDelta = timeDelta
	return err
}

// shouldWriteTimeUnit determines whether we should write tu as a time unit.
// Returns true if tu is valid and differs from the existing time unit, false otherwise.
func (enc *TimestampEncoder) shouldWriteTimeUnit(timeUnit xtime.Unit) bool {
	if !timeUnit.IsValid() || timeUnit == enc.TimeUnit {
		return false
	}
	return true
}

// writeTimeUnit encodes the time unit and returns true if the time unit has
// changed, and false otherwise.
func (enc *TimestampEncoder) writeTimeUnit(stream encoding.OStream, timeUnit xtime.Unit) bool {
	if !enc.shouldWriteTimeUnit(timeUnit) {
		return false
	}

	scheme := enc.Opts.MarkerEncodingScheme()
	encoding.WriteSpecialMarker(stream, scheme, scheme.TimeUnit())
	stream.WriteByte(byte(timeUnit))
	enc.TimeUnit = timeUnit
	return true
}

// shouldWriteAnnotation determines whether we should write ant as an annotation.
// Returns true if ant is not empty and differs from the existing annotation, false otherwise.
func (enc *TimestampEncoder) shouldWriteAnnotation(ant ts.Annotation) bool {
	numAnnotationBytes := len(ant)
	if numAnnotationBytes == 0 {
		return false
	}
	return !bytes.Equal(enc.PrevAnnotation, ant)
}

func (enc *TimestampEncoder) writeAnnotation(stream encoding.OStream, ant ts.Annotation) {
	if !enc.shouldWriteAnnotation(ant) {
		return
	}

	scheme := enc.Opts.MarkerEncodingScheme()
	encoding.WriteSpecialMarker(stream, scheme, scheme.Annotation())

	var buf [binary.MaxVarintLen32]byte
	// NB: we subtract 1 for possible varint encoding savings
	annotationLength := binary.PutVarint(buf[:], int64(len(ant)-1))

	stream.WriteBytes(buf[:annotationLength])
	stream.WriteBytes(ant)
	enc.PrevAnnotation = ant
}

func (enc *TimestampEncoder) writeDeltaOfDeltaTimeUnitChanged(
	stream encoding.OStream, prevDelta, curDelta time.Duration) {
	// NB(xichen): if the time unit has changed, always normalize delta-of-delta
	// to nanoseconds and encode it using 64 bits.
	dodInNano := int64(curDelta - prevDelta)
	stream.WriteBits(uint64(dodInNano), 64)
}

func (enc *TimestampEncoder) writeDeltaOfDeltaTimeUnitUnchanged(
	stream encoding.OStream, prevDelta, curDelta time.Duration, timeUnit xtime.Unit) error {
	u, err := timeUnit.Value()
	if err != nil {
		return err
	}

	deltaOfDelta := xtime.ToNormalizedDuration(curDelta-prevDelta, u)
	tes, exists := enc.Opts.TimeEncodingSchemes()[timeUnit]
	if !exists {
		return fmt.Errorf("time encoding scheme for time unit %v doesn't exist", timeUnit)
	}

	if deltaOfDelta == 0 {
		zeroBucket := tes.ZeroBucket()
		stream.WriteBits(zeroBucket.Opcode(), zeroBucket.NumOpcodeBits())
		return nil
	}

	buckets := tes.Buckets()
	for i := 0; i < len(buckets); i++ {
		if deltaOfDelta >= buckets[i].Min() && deltaOfDelta <= buckets[i].Max() {
			stream.WriteBits(buckets[i].Opcode(), buckets[i].NumOpcodeBits())
			stream.WriteBits(uint64(deltaOfDelta), buckets[i].NumValueBits())
			return nil
		}
	}
	defaultBucket := tes.DefaultBucket()
	stream.WriteBits(defaultBucket.Opcode(), defaultBucket.NumOpcodeBits())
	stream.WriteBits(uint64(deltaOfDelta), defaultBucket.NumValueBits())
	return nil
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

// XOREncoder encapsulates the state required for a logical stream of bits
// that represent a stream of float values compressed with XOR.
type XOREncoder struct {
	HasWrittenFirst bool // Only taken into account if using the WriteFloat() API.
	PrevXOR         uint64
	PrevFloatBits   uint64
}

// WriteFloat writes a float into the stream, writing the full value or a compressed
// XOR as appropriate.
func (enc *XOREncoder) WriteFloat(stream encoding.OStream, val float64) {
	fb := math.Float64bits(val)
	if enc.HasWrittenFirst {
		enc.WriteFloatXOR(stream, fb)
	} else {
		enc.WriteFullFloatVal(stream, fb)
		enc.HasWrittenFirst = true
	}
}

// WriteFullFloatVal writes out the float value using a full 64 bits.
func (enc *XOREncoder) WriteFullFloatVal(stream encoding.OStream, val uint64) {
	enc.PrevFloatBits = val
	enc.PrevXOR = val
	stream.WriteBits(val, 64)
}

// WriteFloatXOR writes out the float value using XOR compression.
func (enc *XOREncoder) WriteFloatXOR(stream encoding.OStream, val uint64) {
	xor := enc.PrevFloatBits ^ val
	enc.WriteXOR(stream, xor)
	enc.PrevXOR = xor
	enc.PrevFloatBits = val
}

// WriteXOR writes out the new XOR based on the value of the previous XOR.
func (enc *XOREncoder) WriteXOR(stream encoding.OStream, currXOR uint64) {
	if currXOR == 0 {
		stream.WriteBits(opcodeZeroValueXOR, 1)
		return
	}

	// NB(xichen): can be further optimized by keeping track of leading and trailing zeros in enc.
	prevLeading, prevTrailing := encoding.LeadingAndTrailingZeros(enc.PrevXOR)
	curLeading, curTrailing := encoding.LeadingAndTrailingZeros(currXOR)
	if curLeading >= prevLeading && curTrailing >= prevTrailing {
		stream.WriteBits(opcodeContainedValueXOR, 2)
		stream.WriteBits(currXOR>>uint(prevTrailing), 64-prevLeading-prevTrailing)
		return
	}

	stream.WriteBits(opcodeUncontainedValueXOR, 2)
	stream.WriteBits(uint64(curLeading), 6)
	numMeaningfulBits := 64 - curLeading - curTrailing
	// numMeaningfulBits is at least 1, so we can subtract 1 from it and encode it in 6 bits
	stream.WriteBits(uint64(numMeaningfulBits-1), 6)
	stream.WriteBits(currXOR>>uint(curTrailing), numMeaningfulBits)
}

// IntSigBitsTracker is used to track the number of significant bits
// which should be used to encode the delta between two integers.
type IntSigBitsTracker struct {
	NumSig             uint8 // current largest number of significant places for int diffs
	CurHighestLowerSig uint8
	NumLowerSig        uint8
}

// WriteIntValDiff writes the provided val diff bits along with
// whether the bits are negative or not.
func (t *IntSigBitsTracker) WriteIntValDiff(
	stream encoding.OStream, valBits uint64, neg bool) {
	if neg {
		stream.WriteBit(opcodeNegative)
	} else {
		stream.WriteBit(opcodePositive)
	}

	stream.WriteBits(valBits, int(t.NumSig))
}

// WriteIntSig writes the number of significant bits of the diff if it has changed and
// updates the IntSigBitsTracker.
func (t *IntSigBitsTracker) WriteIntSig(stream encoding.OStream, sig uint8) {
	if t.NumSig != sig {
		stream.WriteBit(opcodeUpdateSig)
		if sig == 0 {
			stream.WriteBit(OpcodeZeroSig)
		} else {
			stream.WriteBit(OpcodeNonZeroSig)
			stream.WriteBits(uint64(sig-1), NumSigBits)
		}
	} else {
		stream.WriteBit(opcodeNoUpdateSig)
	}

	t.NumSig = sig
}

// TrackNewSig gets the new number of significant bits given the
// number of significant bits of the current diff. It takes into
// account thresholds to try and find a value that's best for the
// current data
func (t *IntSigBitsTracker) TrackNewSig(numSig uint8) uint8 {
	newSig := t.NumSig

	if numSig > t.NumSig {
		newSig = numSig
	} else if t.NumSig-numSig >= sigDiffThreshold {
		if t.NumLowerSig == 0 {
			t.CurHighestLowerSig = numSig
		} else if numSig > t.CurHighestLowerSig {
			t.CurHighestLowerSig = numSig
		}

		t.NumLowerSig++
		if t.NumLowerSig >= sigRepeatThreshold {
			newSig = t.CurHighestLowerSig
			t.NumLowerSig = 0
		}

	} else {
		t.NumLowerSig = 0
	}

	return newSig
}

// Reset resets the IntSigBitsTracker for reuse.
func (t *IntSigBitsTracker) Reset() {
	t.NumSig = 0
	t.CurHighestLowerSig = 0
	t.NumLowerSig = 0
}

type resultType int

const (
	byCopyResultType resultType = iota
	byRefResultType
)
