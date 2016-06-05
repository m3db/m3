package tsz

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"

	"code.uber.internal/infra/memtsdb/encoding"
	xio "code.uber.internal/infra/memtsdb/x/io"
	xtime "code.uber.internal/infra/memtsdb/x/time"
)

type encoder struct {
	os   *ostream
	opts Options
	tess TimeEncodingSchemes
	mes  MarkerEncodingScheme

	// internal bookkeeping
	t   time.Time     // current time
	dt  time.Duration // current time delta
	vb  uint64        // current value
	xor uint64        // current xor

	ant encoding.Annotation         // current annotation
	tu  xtime.Unit                  // current time unit
	buf [binary.MaxVarintLen32]byte // temporary buffer
}

// NewEncoder creates a new encoder.
// TODO(xichen): add a closed flag as well as Open() / Close() to indicate whether the encoder is already closed.
func NewEncoder(start time.Time, bytes []byte, opts Options) encoding.Encoder {
	if opts == nil {
		opts = NewOptions()
	}
	return &encoder{
		os:   newOStream(bytes),
		opts: opts,
		tess: opts.GetTimeEncodingSchemes(),
		mes:  opts.GetMarkerEncodingScheme(),
		t:    start,
	}
}

// Encode encodes the timestamp and the value of a datapoint.
func (enc *encoder) Encode(dp encoding.Datapoint, ant encoding.Annotation, tu xtime.Unit) error {
	if enc.os.len() == 0 {
		return enc.writeFirst(dp, ant, tu)
	}
	return enc.writeNext(dp, ant, tu)
}

// writeFirst writes the first datapoint with annotation.
func (enc *encoder) writeFirst(dp encoding.Datapoint, ant encoding.Annotation, tu xtime.Unit) error {
	if err := enc.writeFirstTime(dp.Timestamp, ant, tu); err != nil {
		return err
	}
	enc.writeFirstValue(dp.Value)
	return nil
}

// writeNext writes the next datapoint with annotation.
func (enc *encoder) writeNext(dp encoding.Datapoint, ant encoding.Annotation, tu xtime.Unit) error {
	if err := enc.writeNextTime(dp.Timestamp, ant, tu); err != nil {
		return err
	}
	enc.writeNextValue(dp.Value)
	return nil
}

// writeSpecialMarker writes the marker that marks the start of a special symbol,
// e.g., the eos marker, the annotation marker, or the time unit marker.
func (enc *encoder) writeSpecialMarker(os *ostream, marker Marker) {
	os.WriteBits(enc.mes.Opcode(), enc.mes.NumOpcodeBits())
	os.WriteBits(uint64(marker), enc.mes.NumValueBits())
}

// shouldWriteAnnotation determines whether we should write ant as an annotation.
// Returns true if ant is not empty and differs from the existing annotation, false otherwise.
func (enc *encoder) shouldWriteAnnotation(ant encoding.Annotation) bool {
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

func (enc *encoder) writeAnnotation(ant encoding.Annotation) {
	if !enc.shouldWriteAnnotation(ant) {
		return
	}
	enc.writeSpecialMarker(enc.os, enc.mes.Annotation())
	// NB: we subtract 1 for possible varint encoding savings
	annotationLength := binary.PutVarint(enc.buf[:], int64(len(ant)-1))
	enc.os.WriteBytes(enc.buf[:annotationLength])
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
	enc.writeSpecialMarker(enc.os, enc.mes.TimeUnit())
	enc.os.WriteByte(byte(tu))
	enc.tu = tu
}

func (enc *encoder) writeFirstTime(t time.Time, ant encoding.Annotation, tu xtime.Unit) error {
	u, err := tu.Value()
	if err != nil {
		return err
	}
	nt := xtime.ToNormalizedTime(enc.t, u)
	enc.os.WriteBits(uint64(nt), 64)
	return enc.writeNextTime(t, ant, tu)
}

func (enc *encoder) writeNextTime(t time.Time, ant encoding.Annotation, tu xtime.Unit) error {
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
	tes, exists := enc.tess[tu]
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

// Reset clears the encoded byte stream and resets the start time of the encoder.
func (enc *encoder) Reset(start time.Time) {
	enc.os.Reset()
	enc.t = start
	enc.dt = 0
	enc.ant = nil
	enc.tu = xtime.None
}

func (enc *encoder) Stream() io.Reader {
	if enc.os.empty() {
		return nil
	}
	b, pos := enc.os.rawbytes()
	blen := len(b)
	head := b[:blen-1]
	tmp := newOStream(nil)
	tmp.WriteBits(uint64(b[blen-1]>>uint(8-pos)), pos)
	enc.writeSpecialMarker(tmp, enc.mes.EndOfStream())
	tail, _ := tmp.rawbytes()
	return xio.NewSegmentReader(&xio.Segment{Head: head, Tail: tail})
}
