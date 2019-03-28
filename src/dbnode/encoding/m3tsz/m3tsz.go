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
	xtime "github.com/m3db/m3x/time"
)

const (
	// DefaultIntOptimizationEnabled is the default switch for m3tsz int optimization
	DefaultIntOptimizationEnabled = true

	// OpcodeZeroSig indicates that there were zero significant digits.
	OpcodeZeroSig = 0x0
	// OpcodeNonZeroSig indicates that there were a non-zero number of significant digits.
	OpcodeNonZeroSig = 0x1

	// NumSigBits is the number of bits required to encode the maximum possible value
	// of significant digits.
	NumSigBits = 6

	opcodeZeroValueXOR        = 0x0
	opcodeContainedValueXOR   = 0x2
	opcodeUncontainedValueXOR = 0x3
	opcodeNoUpdateSig         = 0x0
	opcodeUpdateSig           = 0x1
	opcodeUpdate              = 0x0
	opcodeNoUpdate            = 0x1
	opcodeUpdateMult          = 0x1
	opcodeNoUpdateMult        = 0x0
	opcodePositive            = 0x0
	opcodeNegative            = 0x1
	opcodeRepeat              = 0x1
	opcodeNoRepeat            = 0x0
	opcodeFloatMode           = 0x1
	opcodeIntMode             = 0x0

	sigDiffThreshold   = uint8(3)
	sigRepeatThreshold = uint8(5)

	maxMult     = uint8(6)
	numMultBits = 3
)

var (
	maxInt               = float64(math.MaxInt64)
	minInt               = float64(math.MinInt64)
	maxOptInt            = math.Pow(10.0, 13) // Max int for int optimization
	multipliers          = createMultipliers()
	errInvalidMultiplier = errors.New("supplied multiplier is invalid")
)

// TimestampEncoderState encapsulates the state required for a logical stream of
// bits that represent a stream of timestamps compressed using delta-of-delta
type TimestampEncoderState struct {
	PrevTime       time.Time
	PrevTimeDelta  time.Duration
	PrevAnnotation []byte

	TimeUnit xtime.Unit

	Opts encoding.Options
}

// NewTimestampEncoderState creates a new TimestampEncoderState.
func NewTimestampEncoderState(
	start time.Time, timeUnit xtime.Unit, opts encoding.Options) TimestampEncoderState {
	return TimestampEncoderState{
		PrevTime: start,
		TimeUnit: initialTimeUnit(start, timeUnit),
		Opts:     opts,
	}
}

// WriteFirstTime encodes the first timestamp.
func (enc *TimestampEncoderState) WriteFirstTime(
	stream encoding.OStream, currTime time.Time, ant ts.Annotation, timeUnit xtime.Unit) error {
	// NB(xichen): Always write the first time in nanoseconds because we don't know
	// if the start time is going to be a multiple of the time unit provided.
	nt := xtime.ToNormalizedTime(enc.PrevTime, time.Nanosecond)
	stream.WriteBits(uint64(nt), 64)
	return enc.WriteNextTime(stream, currTime, ant, timeUnit)
}

// WriteNextTime encodes the next (non-first) timestamp.
func (enc *TimestampEncoderState) WriteNextTime(
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
func (enc *TimestampEncoderState) shouldWriteTimeUnit(timeUnit xtime.Unit) bool {
	if !timeUnit.IsValid() || timeUnit == enc.TimeUnit {
		return false
	}
	return true
}

// writeTimeUnit encodes the time unit and returns true if the time unit has
// changed, and false otherwise.
func (enc *TimestampEncoderState) writeTimeUnit(stream encoding.OStream, timeUnit xtime.Unit) bool {
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
func (enc *TimestampEncoderState) shouldWriteAnnotation(ant ts.Annotation) bool {
	numAnnotationBytes := len(ant)
	if numAnnotationBytes == 0 {
		return false
	}
	return !bytes.Equal(enc.PrevAnnotation, ant)
}

func (enc *TimestampEncoderState) writeAnnotation(stream encoding.OStream, ant ts.Annotation) {
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

func (enc *TimestampEncoderState) writeDeltaOfDeltaTimeUnitChanged(
	stream encoding.OStream, prevDelta, curDelta time.Duration) {
	// NB(xichen): if the time unit has changed, always normalize delta-of-delta
	// to nanoseconds and encode it using 64 bits.
	dodInNano := int64(curDelta - prevDelta)
	stream.WriteBits(uint64(dodInNano), 64)
}

func (enc *TimestampEncoderState) writeDeltaOfDeltaTimeUnitUnchanged(
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

// XOREncoderState encapsulates the state required for a logical stream of bits
// that represent a stream of float values compressed with XOR.
type XOREncoderState struct {
	HasWrittenFirst bool // Only taken into account if using the WriteFloat() API.
	PrevXOR         uint64
	PrevFloatBits   uint64
}

// WriteFloat writes a float into the stream, writing the full value or a compressed
// XOR as appropriate.
func (enc *XOREncoderState) WriteFloat(stream encoding.OStream, val float64) {
	fb := math.Float64bits(val)
	if enc.HasWrittenFirst {
		enc.WriteFloatXOR(stream, fb)
	} else {
		enc.WriteFullFloatVal(stream, fb)
		enc.HasWrittenFirst = true
	}
}

// WriteFullFloatVal writes out the float value using a full 64 bits.
func (enc *XOREncoderState) WriteFullFloatVal(stream encoding.OStream, val uint64) {
	enc.PrevFloatBits = val
	enc.PrevXOR = val
	stream.WriteBits(val, 64)
}

// WriteFloatXOR writes out the float value using XOR compression.
func (enc *XOREncoderState) WriteFloatXOR(stream encoding.OStream, val uint64) {
	xor := enc.PrevFloatBits ^ val
	enc.WriteXOR(stream, xor)
	enc.PrevXOR = xor
	enc.PrevFloatBits = val
}

// WriteXOR writes out the new XOR based on the value of the previous XOR.
func (enc *XOREncoderState) WriteXOR(stream encoding.OStream, currXOR uint64) {
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

// convertToIntFloat takes a float64 val and the current max multiplier
// and attempts to transform the float into an int with multiplier. There
// is potential for a small accuracy loss for float values that are very
// close to ints eg. 46.000000000000001 would be returned as 46. This only
// applies to values where the next possible smaller or larger float changes
// the integer component of the float
func convertToIntFloat(v float64, curMaxMult uint8) (float64, uint8, bool, error) {
	if curMaxMult == 0 && v < maxInt {
		// Quick check for vals that are already ints
		i, r := math.Modf(v)
		if r == 0 {
			return i, 0, false, nil
		}
	}

	if curMaxMult > maxMult {
		return 0.0, 0, false, errInvalidMultiplier
	}

	val := v * multipliers[int(curMaxMult)]
	sign := 1.0
	if v < 0 {
		sign = -1.0
		val = val * -1.0
	}

	for mult := curMaxMult; mult <= maxMult && val < maxOptInt; mult++ {
		i, r := math.Modf(val)
		if r == 0 {
			return sign * i, mult, false, nil
		} else if r < 0.1 {
			// Round down and check
			if math.Nextafter(val, 0) <= i {
				return sign * i, mult, false, nil
			}
		} else if r > 0.9 {
			// Round up and check
			next := i + 1
			if math.Nextafter(val, next) >= next {
				return sign * next, mult, false, nil
			}
		}
		val = val * 10.0
	}

	return v, 0, true, nil
}

func convertFromIntFloat(val float64, mult uint8) float64 {
	if mult == 0 {
		return val
	}

	return val / multipliers[int(mult)]
}

// createMultipliers creates all the multipliers up to maxMult
// and places them into a slice
func createMultipliers() []float64 {
	multipliers := make([]float64, maxMult+1)
	base := 1.0
	for i := 0; i <= int(maxMult); i++ {
		multipliers[i] = base
		base = base * 10.0
	}

	return multipliers
}
