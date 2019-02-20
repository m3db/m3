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

package encoding

import (
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/pool"
)

const (
	initAllocSize = 1024
)

// Ostream encapsulates a writable stream.
type ostream struct {
	rawBuffer []byte
	checked   checked.Bytes // raw bytes
	pos       int           // how many bits have been used in the last byte
	bytesPool pool.CheckedBytesPool
}

// NewOStream creates a new Ostream
func NewOStream(
	bytes checked.Bytes,
	initAllocIfEmpty bool,
	bytesPool pool.CheckedBytesPool,
) OStream {
	if bytes == nil && initAllocIfEmpty {
		bytes = checked.NewBytes(make([]byte, 0, initAllocSize), nil)
	}
	stream := &ostream{bytesPool: bytesPool}
	stream.Reset(bytes)
	return stream
}

// Len returns the length of the Ostream
func (os *ostream) Len() int {
	return len(os.rawBuffer)
}

// Empty returns whether the Ostream is empty
func (os *ostream) Empty() bool {
	return os.Len() == 0 && os.pos == 0
}

func (os *ostream) lastIndex() int {
	return os.Len() - 1
}

func (os *ostream) hasUnusedBits() bool {
	return os.pos > 0 && os.pos < 8
}

// grow appends the last byte of v to checked and sets pos to np.
func (os *ostream) grow(v byte, np int) {
	os.ensureCapacityFor(1)
	os.rawBuffer = append(os.rawBuffer, v)

	os.pos = np
}

// ensureCapacity ensures that there is at least capacity for n more bytes.
func (os *ostream) ensureCapacityFor(n int) {
	var (
		currCap      = cap(os.rawBuffer)
		currLen      = len(os.rawBuffer)
		availableCap = currCap - currLen
		missingCap   = n - availableCap
	)
	if missingCap <= 0 {
		// Already have enough capacity.
		return
	}

	newCap := max(cap(os.rawBuffer)*2, currCap+missingCap)
	if p := os.bytesPool; p != nil {
		newChecked := p.Get(newCap)
		newChecked.IncRef()
		newChecked.AppendAll(os.rawBuffer)

		if os.checked != nil {
			os.checked.DecRef()
			os.checked.Finalize()
		}

		os.checked = newChecked
		os.rawBuffer = os.checked.Bytes()
	} else {
		newRawBuffer := make([]byte, 0, newCap)
		newRawBuffer = append(newRawBuffer, os.rawBuffer...)
		os.rawBuffer = newRawBuffer

		os.checked = checked.NewBytes(os.rawBuffer, nil)
		os.checked.IncRef()
	}
}

func (os *ostream) fillUnused(v byte) {
	os.rawBuffer[os.lastIndex()] |= v >> uint(os.pos)
}

// WriteBit writes the last bit of v.
func (os *ostream) WriteBit(v Bit) {
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
	os.ensureCapacityFor(len(bytes))
	for i := 0; i < len(bytes); i++ {
		os.WriteByte(bytes[i])
	}
}

// WriteBits writes the lowest numBits of v to the stream, starting
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
		os.WriteBit(Bit((v >> 63) & 1))
		v <<= 1
		numBits--
	}
}

// Discard takes the ref to the checked bytes from the ostream
func (os *ostream) Discard() checked.Bytes {
	os.checked.Reset(os.rawBuffer)

	buffer := os.checked
	buffer.DecRef()

	os.rawBuffer = nil
	os.pos = 0
	os.checked = nil

	return buffer
}

// Reset resets the ostream
func (os *ostream) Reset(buffer checked.Bytes) {
	// TODO: Replace with call to discard?
	if os.checked != nil {
		// Release ref of the current raw buffer
		os.checked.DecRef()
		os.checked.Finalize()

		os.rawBuffer = nil
		os.checked = nil
	}

	if buffer != nil {
		// Track ref to the new raw buffer
		buffer.IncRef()

		os.checked = buffer
		os.rawBuffer = os.checked.Bytes()
	}

	os.pos = 0
	if os.Len() > 0 {
		// If the byte array passed in is not empty, we set
		// pos to 8 indicating the last byte is fully used.
		os.pos = 8
	}
}

// Rawbytes returns the Osteam's raw bytes
func (os *ostream) Rawbytes() (checked.Bytes, int) {
	os.checked.Reset(os.rawBuffer)
	return os.checked, os.pos
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}
