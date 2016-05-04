package tsz

import (
	"encoding/binary"
	"math"
	"time"

	"code.uber.internal/infra/memtsdb"
	"code.uber.internal/infra/memtsdb/encoding"
)

// encoder implements the encoding scheme in the facebook paper
// "Gorilla: A Fast, Scalable, In-Memory Time Series Database".
type encoder struct {
	os *ostream
	tu time.Duration // time unit

	// internal bookkeeping
	nt  int64  // current time
	dt  int64  // current time delta
	vb  uint64 // current value
	xor uint64 // current xor

	buf [binary.MaxVarintLen32]byte // temporary buffer
}

// NewEncoder creates a new encoder.
func NewEncoder(start time.Time, timeUnit time.Duration) encoding.Encoder {
	return &encoder{
		os: newOStream(),
		nt: memtsdb.ToNormalizedTime(start, timeUnit),
		tu: timeUnit,
	}
}

// Encode encodes the timestamp and the value of a datapoint.
func (enc *encoder) Encode(dp encoding.Datapoint, ant encoding.Annotation) error {
	if enc.os.len() == 0 {
		return enc.writeFirst(dp, ant)
	}
	return enc.writeNext(dp, ant)
}

// writeFirst writes the first datapoint with annotation.
func (enc *encoder) writeFirst(dp encoding.Datapoint, ant encoding.Annotation) error {
	if err := enc.writeFirstTimeWithAnnotation(dp.Timestamp, ant); err != nil {
		return err
	}
	enc.writeFirstValue(dp.Value)
	return nil
}

// writeNext writes the next datapoint with annotation.
func (enc *encoder) writeNext(dp encoding.Datapoint, ant encoding.Annotation) error {
	if err := enc.writeNextTimeWithAnnotation(dp.Timestamp, ant); err != nil {
		return err
	}
	enc.writeNextValue(dp.Value)
	return nil
}

// writeSpecialMarker writes the marker that marks the start of a special symbol,
// For example, the eos marker, or the annotation marker.
func writeSpecialMarker(os *ostream, marker uint64) {
	os.WriteBits(defaultDoDRange.opcode, defaultDoDRange.numOpcodeBits)
	os.WriteBits(marker, defaultDoDRange.numDoDBits)
}

func (enc *encoder) writeAnnotation(ant encoding.Annotation) error {
	numAnnotationBytes := len(ant)
	if numAnnotationBytes == 0 {
		return nil
	}
	writeSpecialMarker(enc.os, annotationMarker)
	// NB: we subtract 1 for possible varint encoding savings
	annotationLength := binary.PutVarint(enc.buf[:], int64(numAnnotationBytes-1))
	enc.os.WriteBytes(enc.buf[:annotationLength])
	enc.os.WriteBytes(ant)
	return nil
}

func (enc *encoder) writeFirstTimeWithAnnotation(t time.Time, ant encoding.Annotation) error {
	enc.os.WriteBits(uint64(enc.nt), 64)
	return enc.writeNextTimeWithAnnotation(t, ant)
}

func (enc *encoder) writeNextTimeWithAnnotation(t time.Time, ant encoding.Annotation) error {
	if err := enc.writeAnnotation(ant); err != nil {
		return err
	}
	nt := memtsdb.ToNormalizedTime(t, enc.tu)
	dt := nt - enc.nt
	enc.writeDeltaOfDelta(enc.dt, dt)
	enc.nt = nt
	enc.dt = dt
	return nil
}

func (enc *encoder) writeDeltaOfDelta(prevDoD, curDoD int64) {
	deltaOfDelta := curDoD - prevDoD
	if deltaOfDelta == 0 {
		enc.os.WriteBits(zeroDoDRange.opcode, zeroDoDRange.numOpcodeBits)
		return
	}
	for i := 0; i < len(dodRanges); i++ {
		if deltaOfDelta >= dodRanges[i].min && deltaOfDelta <= dodRanges[i].max {
			enc.os.WriteBits(dodRanges[i].opcode, dodRanges[i].numOpcodeBits)
			enc.os.WriteBits(uint64(deltaOfDelta), dodRanges[i].numDoDBits)
			return
		}
	}
	enc.os.WriteBits(defaultDoDRange.opcode, defaultDoDRange.numOpcodeBits)
	enc.os.WriteBits(uint64(deltaOfDelta), defaultDoDRange.numDoDBits)
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
	enc.os.WriteBits(opcodeNotContainedValueXOR, 2)
	enc.os.WriteBits(uint64(curLeading), 6)
	numMeaningfulBits := 64 - curLeading - curTrailing
	// numMeaningfulBits is at least 1, so we can subtract 1 from it and encode it in 6 bits
	enc.os.WriteBits(uint64(numMeaningfulBits-1), 6)
	enc.os.WriteBits(curXOR>>uint(curTrailing), numMeaningfulBits)
}

// Reset clears the encoded byte stream and resets the start time of the encoder.
func (enc *encoder) Reset(start time.Time) {
	enc.os.Reset()
	enc.nt = memtsdb.ToNormalizedTime(start, enc.tu)
	enc.dt = 0
}

// Bytes returns nil if there are no encoded bytes, or the encoded byte stream with the eos marker appended.
// This doesn't change the current byte stream being encoded.
func (enc *encoder) Bytes() []byte {
	if enc.os.empty() {
		return nil
	}
	bc := enc.os.clone()
	writeSpecialMarker(bc, eosMarker)
	return bc.rawbytes()
}
