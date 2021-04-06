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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/x/xio"
)

func TestIStreamReadBits(t *testing.T) {
	byteStream := []byte{
		0xca, 0xfe, 0xfd, 0x89, 0x1a, 0x2b, 0x3c, 0x48, 0x55, 0xe6, 0xf7,
		0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x80,
	}

	is := NewIStream(xio.NewBytesReader64(byteStream))
	numBits := []byte{1, 3, 4, 8, 7, 2, 64, 64}
	var res []uint64
	for _, v := range numBits {
		read, err := is.ReadBits(v)
		require.NoError(t, err)
		res = append(res, read)
	}
	expected := []uint64{0x1, 0x4, 0xa, 0xfe, 0x7e, 0x3, 0x1234567890abcdef, 0x1}
	require.Equal(t, expected, res)

	_, err := is.ReadBits(8)
	require.EqualError(t, err, io.EOF.Error())
}

func TestIStreamReadByte(t *testing.T) {
	var (
		byteStream = []uint8{
			0xca, 0xfe, 0xfd, 0x89, 0x1a, 0x2b, 0x3c, 0x48, 0x55, 0xe6, 0xf7,
			0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x80,
		}
		is  = NewIStream(xio.NewBytesReader64(byteStream))
		res = make([]byte, 0, len(byteStream))
	)

	for range byteStream {
		read, err := is.ReadByte()
		require.NoError(t, err)
		res = append(res, read)
	}
	require.Equal(t, byteStream, res)

	_, err := is.ReadByte()
	require.EqualError(t, err, io.EOF.Error())
}

func TestIStreamPeekBitsSuccess(t *testing.T) {
	byteStream := []byte{0xa9, 0xfe, 0xfe, 0xdf, 0x9b, 0x57, 0x21, 0xf1}
	is := NewIStream(xio.NewBytesReader64(byteStream))
	inputs := []struct {
		numBits  uint8
		expected uint64
	}{
		{0, 0},
		{1, 0x1},
		{8, 0xa9},
		{10, 0x2a7},
		{13, 0x153f},
		{16, 0xa9fe},
		{32, 0xa9fefedf},
		{64, 0xa9fefedf9b5721f1},
	}
	for _, input := range inputs {
		res, err := is.PeekBits(input.numBits)
		require.NoError(t, err)
		require.Equal(t, input.expected, res)
	}
	require.Equal(t, uint64(0), is.current)
	require.Equal(t, 0, int(is.remaining))
}

func TestIStreamPeekBitsError(t *testing.T) {
	byteStream := []byte{0x1, 0x2}
	is := NewIStream(xio.NewBytesReader64(byteStream))
	res, err := is.PeekBits(20)
	require.EqualError(t, err, io.EOF.Error())
	require.Equal(t, uint64(0), res)
}

func TestIStreamReadAfterPeekBits(t *testing.T) {
	byteStream := []byte{0xab, 0xcd}
	is := NewIStream(xio.NewBytesReader64(byteStream))
	res, err := is.PeekBits(10)
	require.NoError(t, err)
	require.Equal(t, uint64(0x2af), res)
	_, err = is.PeekBits(20)
	require.EqualError(t, err, io.EOF.Error())

	inputs := []struct {
		numBits  uint8
		expected uint64
	}{
		{2, 0x2},
		{9, 0x15e},
	}
	for _, input := range inputs {
		res, err := is.ReadBits(input.numBits)
		require.NoError(t, err)
		require.Equal(t, input.expected, res)
	}
	_, err = is.ReadBits(8)
	require.EqualError(t, err, io.EOF.Error())
}

func TestIStreamPeekAfterReadBits(t *testing.T) {
	byteStream := []byte{0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xA}
	is := NewIStream(xio.NewBytesReader64(byteStream))

	res, err := is.ReadBits(16)
	require.NoError(t, err)
	require.Equal(t, uint64(0x102), res)

	res, err = is.PeekBits(63)
	require.NoError(t, err)
	require.Equal(t, uint64(0x30405060708090A)>>1, res)

	res, err = is.PeekBits(64)
	require.NoError(t, err)
	require.Equal(t, uint64(0x30405060708090A), res)

	res, err = is.ReadBits(1)
	require.NoError(t, err)
	require.Equal(t, uint64(0), res)

	res, err = is.PeekBits(63)
	require.NoError(t, err)
	require.Equal(t, uint64(0x30405060708090A), res)

	_, err = is.PeekBits(64)
	require.EqualError(t, err, io.EOF.Error())
}

func TestIStreamRemainingBitsInCurrentByte(t *testing.T) {
	byteStream := []byte{0xff, 0, 0x42}
	is := NewIStream(xio.NewBytesReader64(byteStream))
	for _, b := range byteStream {
		for i := 0; i < 8; i++ {
			var expected uint
			if i > 0 {
				expected = uint(8 - i)
			}
			require.Equal(t, expected, is.RemainingBitsInCurrentByte())
			bit, err := is.ReadBit()
			require.NoError(t, err)
			expectedBit := Bit(b>>i) & 1
			require.Equal(t, expectedBit, bit)
		}
	}
}

func TestIStreamReset(t *testing.T) {
	is := NewIStream(xio.NewBytesReader64([]byte{0xff}))
	_, _ = is.ReadBits(8)
	_, _ = is.ReadBits(1)
	is.Reset(xio.NewBytesReader64(nil))
	require.Equal(t, uint64(0), is.current)
	require.Equal(t, uint8(0), is.remaining)
	require.Equal(t, 0, is.index)
	require.NoError(t, is.err)
}
