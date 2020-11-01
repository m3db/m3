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
	"encoding/binary"
	"io"
	"io/ioutil"
)

// istream64 encapsulates a readable stream based directly on []byte slice and operating in 64 bit words.
type istream64 struct {
	data      []byte // encoded data
	err       error  // error encountered
	current   uint64 // current uint64 we are working off of
	index     int    // current index within data slice
	remaining uint   // bits remaining in current to be read
}

// NewIStream64 creates a new istream64
func NewIStream64(data []byte) IStream {
	return &istream64{data: data}
}

func (is *istream64) ReadBit() (Bit, error) {
	res, err := is.ReadBits(1)
	return Bit(res), err
}

func (is *istream64) Read(b []byte) (int, error) {
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

func (is *istream64) ReadByte() (byte, error) {
	res, err := is.ReadBits(8)
	return byte(res), err
}

func (is *istream64) ReadBits(numBits uint) (uint64, error) {
	if is.err != nil {
		return 0, is.err
	}
	if numBits <= is.remaining {
		return is.consumeBuffer(numBits), nil
	}
	res := readBitsInWord(is.current, is.remaining)
	bitsNeeded := numBits - is.remaining
	if err := is.readWordFromStream(); err != nil {
		return 0, err
	}
	if is.remaining < bitsNeeded {
		return 0, io.EOF
	}
	return (res << bitsNeeded) | is.consumeBuffer(bitsNeeded), nil
}

func (is *istream64) PeekBits(numBits uint) (uint64, error) {
	if numBits <= is.remaining {
		return readBitsInWord(is.current, numBits), nil
	}
	res := readBitsInWord(is.current, is.remaining)
	bitsNeeded := numBits - is.remaining
	next, rem, err := is.peekWordFromStream()
	if err != nil {
		return 0, err
	}
	if rem < bitsNeeded {
		return 0, io.EOF
	}
	return (res << bitsNeeded) | readBitsInWord(next, bitsNeeded), nil
}

//TODO: tests
func (is *istream64) RemainingBitsInCurrentByte() uint {
	return is.remaining % 8
}

// readBitsInWord reads the first numBits in word w.
func readBitsInWord(w uint64, numBits uint) uint64 {
	return w >> (64 - numBits)
}

// consumeBuffer consumes numBits in is.current.
func (is *istream64) consumeBuffer(numBits uint) uint64 {
	res := readBitsInWord(is.current, numBits)
	is.current <<= numBits
	is.remaining -= numBits
	return res
}

func (is *istream64) peekWordFromStream() (uint64, uint, error) {
	if is.index+8 <= len(is.data) {
		return binary.BigEndian.Uint64(is.data[is.index:]), 64, nil
	}
	if is.index >= len(is.data) {
		return 0, 0, io.EOF
	}
	var res uint64
	var rem uint
	for i := is.index; i < len(is.data); i++ {
		res = (res << 8) | uint64(is.data[i])
		rem += 8
	}
	return res << (64 - rem), rem, nil
}

func (is *istream64) readWordFromStream() error {
	if is.index+8 <= len(is.data) {
		is.current = binary.BigEndian.Uint64(is.data[is.index:])
		is.remaining = 64
		is.index += 8
		return is.err
	}
	if is.index >= len(is.data) {
		is.current = 0
		is.err = io.EOF
		return is.err
	}
	var res uint64
	var rem uint
	for ; is.index < len(is.data); is.index++ {
		res = (res << 8) | uint64(is.data[is.index])
		rem += 8
	}
	is.remaining = rem
	is.current = res << (64 - rem)
	return nil
}

func (is *istream64) Reset(r io.Reader) {
	is.err = nil
	is.current = 0
	is.remaining = 0
	is.index = 0
	if r == nil {
		is.data = nil
		is.err = nil
		return
	}
	//FIXME: this is slow and should accept a slice of bytes directly as an argument instead.
	is.data, is.err = ioutil.ReadAll(r)
}
