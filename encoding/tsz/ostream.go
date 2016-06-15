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

const (
	initAllocSize = 1024
)

// ostream encapsulates a writable stream.
type ostream struct {
	rawBuffer []byte // raw bytes
	pos       int    // how many bits have been used in the last byte
}

func newOStream(bytes []byte, initAllocIfEmpty bool) *ostream {
	if cap(bytes) == 0 && initAllocIfEmpty {
		bytes = make([]byte, 0, initAllocSize)
	}
	stream := &ostream{}
	stream.Reset(bytes)
	return stream
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
	os.rawBuffer[os.lastIndex()] |= v >> uint(os.pos)
}

// WriteBit writes the last bit of v.
func (os *ostream) WriteBit(v bit) {
	v <<= 7
	if !os.hasUnusedBits() {
		os.grow(byte(v), 1)
		return
	}
	os.fillUnused(byte(v))
	os.pos++
}

// WriteByte writes the last byte of v.
func (os *ostream) WriteByte(v byte) {
	if !os.hasUnusedBits() {
		os.grow(v, 8)
		return
	}
	os.fillUnused(v)
	os.grow(v<<uint(8-os.pos), os.pos)
}

// WriteBytes writes a byte slice.
func (os *ostream) WriteBytes(bytes []byte) {
	for i := 0; i < len(bytes); i++ {
		os.WriteByte(bytes[i])
	}
}

// WritBits writes the lowest numBits of v to the stream, starting
// from the most significant bit to the least significant bit.
func (os *ostream) WriteBits(v uint64, numBits int) {
	if numBits == 0 {
		return
	}

	// we should never write more than 64 bits for a uint64
	if numBits > 64 {
		numBits = 64
	}

	v <<= uint(64 - numBits)
	for numBits >= 8 {
		os.WriteByte(byte(v >> 56))
		v <<= 8
		numBits -= 8
	}

	for numBits > 0 {
		os.WriteBit(bit((v >> 63) & 1))
		v <<= 1
		numBits--
	}
}

func (os *ostream) Reset(buffer []byte) {
	os.rawBuffer = buffer
	os.pos = 0
	if len(buffer) > 0 {
		// If the byte array passed in is not empty, we set
		// pos to 8 indicating the last byte is fully used.
		os.pos = 8
	}
}

func (os *ostream) rawbytes() ([]byte, int) {
	return os.rawBuffer, os.pos
}
