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
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadBits(t *testing.T) {
	byteStream := []byte{
		0xca, 0xfe, 0xfd, 0x89, 0x1a, 0x2b, 0x3c, 0x48, 0x55, 0xe6, 0xf7,
		0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x80,
	}

	o := NewIStream(bytes.NewReader(byteStream))
	is := o.(*istream)
	numBits := []int{1, 3, 4, 8, 7, 2, 64, 64}
	var res []uint64
	for _, v := range numBits {
		read, err := is.ReadBits(v)
		require.NoError(t, err)
		res = append(res, read)
	}
	expected := []uint64{0x1, 0x4, 0xa, 0xfe, 0x7e, 0x3, 0x1234567890abcdef, 0x1}
	require.Equal(t, expected, res)
	require.NoError(t, is.err)

	_, err := is.ReadBits(8)
	require.Error(t, err)
	require.Error(t, is.err)
}

func TestPeekBitsSuccess(t *testing.T) {
	byteStream := []byte{0xa9, 0xfe, 0xfe, 0xdf, 0x9b, 0x57, 0x21, 0xf1}
	o := NewIStream(bytes.NewReader(byteStream))
	is := o.(*istream)
	inputs := []struct {
		numBits  int
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
	require.NoError(t, is.err)
	require.Equal(t, byte(0), is.current)
	require.Equal(t, 0, is.remaining)
}

func TestPeekBitsError(t *testing.T) {
	byteStream := []byte{0x1, 0x2}
	o := NewIStream(bytes.NewReader(byteStream))
	is := o.(*istream)
	res, err := is.PeekBits(20)
	require.Error(t, err)
	require.Equal(t, uint64(0), res)
}

func TestReadAfterPeekBits(t *testing.T) {
	byteStream := []byte{0xab, 0xcd}
	o := NewIStream(bytes.NewReader(byteStream))
	is := o.(*istream)
	res, err := is.PeekBits(10)
	require.NoError(t, err)
	require.Equal(t, uint64(0x2af), res)
	_, err = is.PeekBits(20)
	require.Error(t, err)

	inputs := []struct {
		numBits  int
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
	require.Error(t, err)
}

func TestResetIStream(t *testing.T) {
	o := NewIStream(bytes.NewReader(nil))
	is := o.(*istream)
	is.ReadBits(1)
	require.Error(t, is.err)
	is.Reset(bytes.NewReader(nil))
	require.NoError(t, is.err)
	require.Equal(t, byte(0), is.current)
	require.Equal(t, 0, is.remaining)
}
