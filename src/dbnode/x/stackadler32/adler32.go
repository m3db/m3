package stackadler32

var prime uint32 = 65521

const nMax = 5552 // maximum number of bytes before modulo reduction

// Digest computes an adler32 hash and will very likely
// be allocated on the stack when used locally and not casted
// to an interface.
type Digest struct {
	initialized bool
	s1          uint32
	s2          uint32
}

// NewDigest returns an adler32 digest struct.
func NewDigest() Digest {
	return Digest{
		initialized: true,
		s1:          1, // Adler-32 starts with s1=1 and s2=0
		s2:          0,
	}
}

// Update returns a new derived adler32 digest struct.
func (d Digest) Update(buf []byte) Digest {
	r := d
	if !r.initialized {
		r = NewDigest()
	}

	length := len(buf)
	for i := 0; i < length; {
		// Process in blocks of at most nMax bytes
		end := i + nMax
		if end > length {
			end = length
		}
		for j := i; j < end; j++ {
			r.s1 += uint32(buf[j])
			r.s2 += r.s1
		}
		// perform modulo reduction once per block
		r.s1 %= prime
		r.s2 %= prime
		i = end
	}
	return r
}

// Sum32 returns the currently computed adler32 hash.
func (d Digest) Sum32() uint32 {
	if !d.initialized {
		return NewDigest().Sum32()
	}
	return (d.s2 << 16) | d.s1
}

// Checksum returns an adler32 checksum of the buffer specified.
func Checksum(buf []byte) uint32 {
	return NewDigest().Update(buf).Sum32()
}
