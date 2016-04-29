package tsz

import (
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
func (enc *encoder) Encode(dp encoding.Datapoint) {
	if enc.os.len() == 0 {
		enc.writeFirstTime(dp.Timestamp)
		enc.writeFirstValue(dp.Value)
		return
	}
	enc.writeNextTime(dp.Timestamp)
	enc.writeNextValue(dp.Value)
}

func (enc *encoder) writeFirstTime(t time.Time) {
	enc.os.writeBits(uint64(enc.nt), 64)
	enc.writeNextTime(t)
}

func (enc *encoder) writeNextTime(t time.Time) {
	nt := memtsdb.ToNormalizedTime(t, enc.tu)
	dt := nt - enc.nt
	enc.writeDeltaOfDelta(enc.dt, dt)
	enc.nt = nt
	enc.dt = dt
}

func (enc *encoder) writeDeltaOfDelta(prevDoD, curDoD int64) {
	deltaOfDelta := curDoD - prevDoD
	if deltaOfDelta == 0 {
		enc.os.writeBits(zeroDoDRange.opcode, zeroDoDRange.numOpcodeBits)
		return
	}
	for i := 0; i < len(dodRanges); i++ {
		if deltaOfDelta >= dodRanges[i].min && deltaOfDelta <= dodRanges[i].max {
			enc.os.writeBits(dodRanges[i].opcode, dodRanges[i].numOpcodeBits)
			enc.os.writeBits(uint64(deltaOfDelta), dodRanges[i].numDoDBits)
			return
		}
	}
	enc.os.writeBits(defaultDoDRange.opcode, defaultDoDRange.numOpcodeBits)
	enc.os.writeBits(uint64(deltaOfDelta), defaultDoDRange.numDoDBits)
}

func (enc *encoder) writeFirstValue(v float64) {
	enc.vb = math.Float64bits(v)
	enc.xor = enc.vb
	enc.os.writeBits(enc.vb, 64)
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
		enc.os.writeBits(opcodeZeroValueXOR, 1)
		return
	}

	// NB(xichen): can be further optimized by keeping track of leading and trailing zeros in enc.
	prevLeading, prevTrailing := leadingAndTrailingZeros(prevXOR)
	curLeading, curTrailing := leadingAndTrailingZeros(curXOR)
	if curLeading >= prevLeading && curTrailing >= prevTrailing {
		enc.os.writeBits(opcodeContainedValueXOR, 2)
		enc.os.writeBits(curXOR>>uint(prevTrailing), 64-prevLeading-prevTrailing)
		return
	}
	enc.os.writeBits(opcodeNotContainedValueXOR, 2)
	enc.os.writeBits(uint64(curLeading), 6)
	numMeaningfulBits := 64 - curLeading - curTrailing
	// numMeaningfulBits is at least 1, so we can subtract 1 from it and encode it in 6 bits
	enc.os.writeBits(uint64(numMeaningfulBits-1), 6)
	enc.os.writeBits(curXOR>>uint(curTrailing), numMeaningfulBits)
}

// Reset clears the encoded byte stream and resets the start time of the encoder.
func (enc *encoder) Reset(start time.Time) {
	enc.os.reset()
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
	bc.writeBits(defaultDoDRange.opcode, defaultDoDRange.numOpcodeBits)
	bc.writeBits(eosMarker, defaultDoDRange.numDoDBits)
	return bc.rawbytes()
}
