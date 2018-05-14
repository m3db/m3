// Copyright (c) 2018 Uber Technologies, Inc.
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
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUint32(t *testing.T) {
	tests := []struct {
		x uint32
	}{
		{
			x: 0,
		},
		{
			x: 42,
		},
		{
			x: math.MaxUint32,
		},
	}

	for _, test := range tests {
		name := fmt.Sprintf("Encode and Decode %d", test.x)
		t.Run(name, func(t *testing.T) {
			enc := NewEncoder(1024)
			n := enc.PutUint32(test.x)
			require.Equal(t, 4, n)

			dec := NewDecoder(enc.Bytes())
			actual, err := dec.Uint32()

			require.NoError(t, err)
			require.Equal(t, test.x, actual)
		})
	}
}

func TestUint64(t *testing.T) {
	tests := []struct {
		x uint64
	}{
		{
			x: 0,
		},
		{
			x: 42,
		},
		{
			x: math.MaxUint64,
		},
	}

	for _, test := range tests {
		name := fmt.Sprintf("Encode and Decode %d", test.x)
		t.Run(name, func(t *testing.T) {
			enc := NewEncoder(1024)
			n := enc.PutUint64(test.x)
			require.Equal(t, 8, n)

			dec := NewDecoder(enc.Bytes())
			actual, err := dec.Uint64()

			require.NoError(t, err)
			require.Equal(t, test.x, actual)
		})
	}
}

func TestUvarint(t *testing.T) {
	tests := []struct {
		x uint64
		n int
	}{
		{
			x: 0,
			n: 1,
		},
		{
			x: 42,
			n: 1,
		},
		{
			x: math.MaxUint64,
			n: 10,
		},
	}

	for _, test := range tests {
		name := fmt.Sprintf("Encode and Decode %d", test.n)
		t.Run(name, func(t *testing.T) {
			enc := NewEncoder(1024)
			n := enc.PutUvarint(test.x)
			require.Equal(t, test.n, n)

			dec := NewDecoder(enc.Bytes())
			actual, err := dec.Uvarint()

			require.NoError(t, err)
			require.Equal(t, test.x, actual)
		})
	}
}

func TestBytes(t *testing.T) {
	tests := []struct {
		name string
		b    []byte
		n    int
	}{
		{
			name: "Encode and Decode Empty Byte Slice",
			b:    []byte(""),
			n:    1,
		},
		{
			name: "Encode and Decode Non-Empty Byte Slice",
			b:    []byte("foo bar baz"),
			n:    12,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			enc := NewEncoder(1024)
			n := enc.PutBytes(test.b)
			require.Equal(t, test.n, n)

			dec := NewDecoder(enc.Bytes())
			actual, err := dec.Bytes()

			require.NoError(t, err)
			require.Equal(t, test.b, actual)
		})
	}
}

func TestEncoderLen(t *testing.T) {
	enc := NewEncoder(1024)

	enc.PutUint32(42)
	require.Equal(t, 4, enc.Len())

	enc.PutUint64(42)
	require.Equal(t, 4+8, enc.Len())

	enc.PutUvarint(42)
	require.Equal(t, 4+8+1, enc.Len())

	enc.PutBytes([]byte("42"))
	require.Equal(t, 4+8+1+3, enc.Len())
}

func TestEncoderReset(t *testing.T) {
	enc := NewEncoder(1024)
	enc.PutUint32(42)
	enc.Reset()

	b := enc.Bytes()
	require.Equal(t, 0, len(b))
}

func TestDecoderReset(t *testing.T) {
	enc := NewEncoder(1024)
	enc.PutUint32(42)
	b := enc.Bytes()

	dec := NewDecoder(nil)
	dec.Reset(b)

	require.Equal(t, b, dec.buf)
}
