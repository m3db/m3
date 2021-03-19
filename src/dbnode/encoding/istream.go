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
	"io"

	"github.com/m3db/m3/src/dbnode/x/xio"
)

// IStream encapsulates a readable stream.
type IStream struct {
	r         xio.Reader64
	err       error  // error encountered
	current   uint64 // current uint64 we are working off of
	index     int    // current index within data slice
	remaining uint8  // bits remaining in current to be read
}

var (
	_ io.ByteReader = (*IStream)(nil)
	_ io.Reader     = (*IStream)(nil)
)

// NewIStream creates a new IStream
func NewIStream(reader64 xio.Reader64) *IStream {
	return &IStream{r: reader64}
}

// Read reads len(b) bytes.
func (is *IStream) Read(b []byte) (int, error) {
	var i int
	for ; i < len(b); i++ {
		res, err := is.ReadBits(8)
		if err != nil {
			return i, err
		}
		b[i] = byte(res)
	}
	return i, nil
}

// ReadByte reads the next Byte.
func (is *IStream) ReadByte() (byte, error) {
	res, err := is.ReadBits(8)
	return byte(res), err
}

// ReadBit reads the next Bit.
func (is *IStream) ReadBit() (Bit, error) {
	res, err := is.ReadBits(1)
	return Bit(res), err
}

// ReadBits reads the next Bits.
func (is *IStream) ReadBits(numBits uint8) (uint64, error) {
	res := is.current >> (64 - numBits)
	remaining := is.remaining
	if numBits <= remaining {
		// Have enough bits buffered.
		is.current <<= numBits
		is.remaining -= numBits
		return res, nil
	}

	// Not enough bits buffered, read next word from the stream.
	bitsNeeded := numBits - remaining

	current, n, err := is.r.Read64()
	if err != nil {
		return 0, err
	}
	n *= 8
	if n < bitsNeeded {
		return 0, io.EOF
	}

	is.current = current << bitsNeeded
	is.remaining = n - bitsNeeded
	return res | current>>(64-bitsNeeded), nil
}

// PeekBits looks at the next Bits, but doesn't move the pos.
func (is *IStream) PeekBits(numBits uint8) (uint64, error) {
	if numBits <= is.remaining {
		return readBitsInWord(is.current, numBits), nil
	}
	res := readBitsInWord(is.current, numBits)
	bitsNeeded := numBits - is.remaining
	next, bytes, err := is.r.Peek64()
	if err != nil {
		return 0, err
	}
	if rem := 8 * bytes; rem < bitsNeeded {
		return 0, io.EOF
	}
	return res | readBitsInWord(next, bitsNeeded), nil
}

// RemainingBitsInCurrentByte returns the number of bits remaining to be read in the current byte.
func (is *IStream) RemainingBitsInCurrentByte() uint {
	return uint(is.remaining % 8)
}

// readBitsInWord reads the first numBits in word w.
func readBitsInWord(w uint64, numBits uint8) uint64 {
	return w >> (64 - numBits)
}

// Reset resets the IStream.
func (is *IStream) Reset(reader xio.Reader64) {
	is.current = 0
	is.remaining = 0
	is.index = 0
	is.r = reader
}
