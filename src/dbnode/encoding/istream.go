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
	"bufio"
	"io"
	"math"
)

const (
	// defaultReaderSize is the default bufio.Reader size, we can keep this
	// small as we rarely need to peek more than a byte or two at a time.
	defaultReaderSize = 16
)

// istream encapsulates a readable stream.
type istream struct {
	r         *bufio.Reader // encoded stream
	err       error         // error encountered
	current   byte          // current byte we are working off of
	remaining int           // bits remaining in current to be read
}

// NewIStream creates a new Istream
func NewIStream(reader io.Reader) IStream {
	return &istream{r: bufio.NewReaderSize(reader, defaultReaderSize)}
}

// ReadBit reads the next Bit
func (is *istream) ReadBit() (Bit, error) {
	if is.err != nil {
		return 0, is.err
	}
	if is.remaining == 0 {
		if err := is.readByteFromStream(); err != nil {
			return 0, err
		}
	}
	return Bit(is.consumeBuffer(1)), nil
}

// Read reads len(b) bytes.
func (is *istream) Read(b []byte) (int, error) {
	var (
		i   int
		err error
	)

	for ; i < len(b); i++ {
		b[i], err = is.ReadByte()
		if err != nil {
			return i, err
		}
	}
	return i, nil
}

// ReadByte reads the next Byte
func (is *istream) ReadByte() (byte, error) {
	if is.err != nil {
		return 0, is.err
	}
	remaining := is.remaining
	res := is.consumeBuffer(remaining)
	if remaining == 8 {
		return res, nil
	}
	if err := is.readByteFromStream(); err != nil {
		return 0, err
	}
	res = (res << uint(8-remaining)) | is.consumeBuffer(8-remaining)
	return res, nil
}

// ReadBits reads the next Bits
func (is *istream) ReadBits(numBits int) (uint64, error) {
	if is.err != nil {
		return 0, is.err
	}
	var res uint64
	for numBits >= 8 {
		byteRead, err := is.ReadByte()
		if err != nil {
			return 0, err
		}
		res = (res << 8) | uint64(byteRead)
		numBits -= 8
	}
	for numBits > 0 {
		bitRead, err := is.ReadBit()
		if err != nil {
			return 0, err
		}
		res = (res << 1) | uint64(bitRead)
		numBits--
	}
	return res, nil
}

// PeekBits looks at the next Bits, but doesn't move the pos
func (is *istream) PeekBits(numBits int) (uint64, error) {
	if is.err != nil {
		return 0, is.err
	}
	// check the last byte first
	if numBits <= is.remaining {
		return uint64(readBitsInByte(is.current, numBits)), nil
	}
	// now check the bytes buffered and read more if necessary.
	numBitsRead := is.remaining
	res := uint64(readBitsInByte(is.current, is.remaining))
	numBytesToRead := int(math.Ceil(float64(numBits-numBitsRead) / 8))
	bytesRead, err := is.r.Peek(numBytesToRead)
	if err != nil {
		return 0, err
	}
	for i := 0; i < numBytesToRead-1; i++ {
		res = (res << 8) | uint64(bytesRead[i])
		numBitsRead += 8
	}
	remainder := readBitsInByte(bytesRead[numBytesToRead-1], numBits-numBitsRead)
	res = (res << uint(numBits-numBitsRead)) | uint64(remainder)
	return res, nil
}

// readBitsInByte reads numBits in byte b.
func readBitsInByte(b byte, numBits int) byte {
	return b >> uint(8-numBits)
}

// consumeBuffer consumes numBits in is.current.
func (is *istream) consumeBuffer(numBits int) byte {
	res := readBitsInByte(is.current, numBits)
	is.current <<= uint(numBits)
	is.remaining -= numBits
	return res
}

func (is *istream) readByteFromStream() error {
	is.current, is.err = is.r.ReadByte()
	is.remaining = 8
	return is.err
}

// Reset resets the Istream
func (is *istream) Reset(r io.Reader) {
	is.r.Reset(r)
	is.err = nil
	is.current = 0
	is.remaining = 0
}
