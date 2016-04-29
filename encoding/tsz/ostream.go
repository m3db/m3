package tsz

const (
	initAllocSize = 1024
)

// ostream encapsulates a writable stream.
type ostream struct {
	rawBuffer []byte
	pos       int
}

func newOStream() *ostream {
	return &ostream{rawBuffer: make([]byte, 0, initAllocSize)}
}

func (os *ostream) clone() *ostream {
	return &ostream{os.rawBuffer, os.pos}
}

func (os *ostream) len() int {
	return len(os.rawBuffer)
}

func (os *ostream) empty() bool {
	return os.len() == 0 && os.pos == 0
}

func (os *ostream) lastIndex() int {
	return os.len() - 1
}

func (os *ostream) hasUnusedBits() bool {
	return os.pos > 0 && os.pos < 8
}

// grow appends the last byte of v to rawBuffer and sets pos to np.
func (os *ostream) grow(v byte, np int) {
	os.rawBuffer = append(os.rawBuffer, v)
	os.pos = np
}

func (os *ostream) fillUnused(v byte) {
	os.rawBuffer[os.lastIndex()] |= v << uint(os.pos)
}

// writeBit writes the last bit of v.
func (os *ostream) writeBit(v bit) {
	if !os.hasUnusedBits() {
		os.grow(byte(v), 1)
		return
	}
	os.fillUnused(byte(v))
	os.pos++
}

// writeByte writes the last byte of v.
func (os *ostream) writeByte(v byte) {
	if !os.hasUnusedBits() {
		os.grow(v, 8)
		return
	}
	os.fillUnused(v)
	os.grow(v>>uint(8-os.pos), os.pos)
}

func (os *ostream) writeBits(v uint64, numBits int) {
	if numBits == 0 {
		return
	}

	// we should never write more than 64 bits for a uint64
	if numBits > 64 {
		numBits = 64
	}

	for numBits >= 8 {
		os.writeByte(byte(v))
		v >>= 8
		numBits -= 8
	}

	for numBits > 0 {
		os.writeBit(bit(v & 1))
		v >>= 1
		numBits--
	}
}

func (os *ostream) reset() {
	os.rawBuffer = os.rawBuffer[:0]
	os.pos = 0
}

func (os *ostream) rawbytes() []byte {
	return os.rawBuffer
}
