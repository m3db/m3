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
	"errors"
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
	tsEncoderState TimestampEncoder
	floatEnc       FloatEncoderAndIterator
	sigTracker     IntSigBitsTracker

	ant ts.Annotation // current annotation

	intVal     float64 // current int val
	numEncoded uint32  // whether any datapoints have been written yet
	maxMult    uint8   // current max multiplier for int vals

	intOptimized bool // whether the encoding scheme is optimized for ints
	isFloat      bool // whether we are encoding ints/floats
	closed       bool
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

	err := enc.tsEncoderState.WriteTime(enc.os, dp.Timestamp, ant, tu)
	if err != nil {
		return err
	}

	if enc.numEncoded == 0 {
		err = enc.writeFirstValue(dp.Value)
	} else {
		err = enc.writeNextValue(dp.Value)
	}
	if err == nil {
		enc.numEncoded++
	}

	return err
}

func (enc *encoder) writeFirstValue(v float64) error {
	if !enc.intOptimized {
		enc.floatEnc.writeFullFloat(enc.os, math.Float64bits(v))
		return nil
	}

	// Attempt to convert float to int for int optimization
	val, mult, isFloat, err := convertToIntFloat(v, 0)
	if err != nil {
		return err
	}

	if isFloat {
		enc.os.WriteBit(opcodeFloatMode)
		enc.floatEnc.writeFullFloat(enc.os, math.Float64bits(v))
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
		enc.floatEnc.writeNextFloat(enc.os, math.Float64bits(v))
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
		enc.floatEnc.writeFullFloat(enc.os, val)
		enc.isFloat = true
		enc.maxMult = mult
		return
	}

	if val == enc.floatEnc.PrevFloatBits {
		// Value is repeated
		enc.os.WriteBit(opcodeUpdate)
		enc.os.WriteBit(opcodeRepeat)
		return
	}

	enc.os.WriteBit(opcodeNoUpdate)
	enc.floatEnc.writeNextFloat(enc.os, val)
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

	enc.floatEnc = FloatEncoderAndIterator{}
	enc.intVal = 0
	enc.isFloat = false
	enc.maxMult = 0
	enc.sigTracker = IntSigBitsTracker{}
	enc.ant = nil
	enc.numEncoded = 0
	enc.closed = false
}

// Stream returns a copy of the underlying data stream.
func (enc *encoder) Stream(opts encoding.StreamOptions) (xio.SegmentReader, bool) {
	segment := enc.segment(byCopyResultType)
	if segment.Len() == 0 {
		return nil, false
	}

	if readerPool := enc.opts.SegmentReaderPool(); readerPool != nil {
		reader := readerPool.Get()
		reader.Reset(segment)
		return reader, true
	}
	return xio.NewSegmentReader(segment), true
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
		result.Value = math.Float64frombits(enc.floatEnc.PrevFloatBits)
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

type resultType int

const (
	byCopyResultType resultType = iota
	byRefResultType
)
