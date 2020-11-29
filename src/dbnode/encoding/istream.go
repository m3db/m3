// Copyright (c) 2020 Uber Technologies, Inc.
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

// iStream encapsulates a readable stream.
type iStream struct {
	r         xio.Reader64
	err       error  // error encountered
	current   uint64 // current uint64 we are working off of
	index     int    // current index within data slice
	remaining uint8  // bits remaining in current to be read
}

// NewIStream creates a new iStream
func NewIStream(reader64 xio.Reader64) IStream {
	return &iStream{r: reader64}
}

func (is *iStream) Read(b []byte) (int, error) {
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

func (is *iStream) ReadByte() (byte, error) {
	res, err := is.ReadBits(8)
	return byte(res), err
}

func (is *iStream) ReadBit() (Bit, error) {
	res, err := is.ReadBits(1)
	return Bit(res), err
}

func (is *iStream) ReadBits(numBits uint8) (uint64, error) {
	if is.err != nil {
		return 0, is.err
	}
	if numBits <= is.remaining {
		return is.consumeBuffer(numBits), nil
	}
	res := readBitsInWord(is.current, numBits)
	bitsNeeded := numBits - is.remaining
	if err := is.readWordFromStream(); err != nil {
		return 0, err
	}
	if is.remaining < bitsNeeded {
		return 0, io.EOF
	}
	return res | is.consumeBuffer(bitsNeeded), nil
}

func (is *iStream) PeekBits(numBits uint8) (uint64, error) {
	if numBits <= is.remaining {
		return readBitsInWord(is.current, numBits), nil
	}
	res := readBitsInWord(is.current, numBits)
	bitsNeeded := numBits - is.remaining
	next, bytes, err := is.r.Peek64()
	if err != nil {
		return 0, err
	}
	rem := 8 * bytes
	if rem < bitsNeeded {
		return 0, io.EOF
	}
	return res | readBitsInWord(next, bitsNeeded), nil
}

func (is *iStream) RemainingBitsInCurrentByte() uint {
	return uint(is.remaining % 8)
}

// readBitsInWord reads the first numBits in word w.
func readBitsInWord(w uint64, numBits uint8) uint64 {
	return w >> (64 - numBits)
}

// consumeBuffer consumes numBits in is.current.
func (is *iStream) consumeBuffer(numBits uint8) uint64 {
	res := readBitsInWord(is.current, numBits)
	is.current <<= numBits
	is.remaining -= numBits
	return res
}

func (is *iStream) readWordFromStream() error {
	current, bytes, err := is.r.Read64()
	is.current = current
	is.remaining = 8 * bytes
	is.err = err

	return err
}

func (is *iStream) Reset(reader xio.Reader64) {
	is.err = nil
	is.current = 0
	is.remaining = 0
	is.index = 0
	is.r = reader
}
