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
	"testing"

	"github.com/m3db/m3/src/x/pool"

	"github.com/stretchr/testify/require"
)

var (
	nilBytes      []byte
	testBytesPool = newTestCheckedBytesPool()
)

func TestWriteBits(t *testing.T) {
	testWriteBits(t, NewOStream(nil, true, nil))
}

func TestWriteBitsWithPooling(t *testing.T) {
	testWriteBits(t, NewOStream(nil, true, testBytesPool))
}

func testWriteBits(t *testing.T, o OStream) {
	inputs := []struct {
		value         uint64
		numBits       int
		expectedBytes []byte
		expectedPos   int
	}{
		{0x1, 1, []byte{0x80}, 1},
		{0x4, 3, []byte{0xc0}, 4},
		{0xa, 4, []byte{0xca}, 8},
		{0xfe, 8, []byte{0xca, 0xfe}, 8},
		{0xaafe, 7, []byte{0xca, 0xfe, 0xfc}, 7},
		{0x3, 2, []byte{0xca, 0xfe, 0xfd, 0x80}, 1},
		{0x1234567890abcdef, 64, []byte{0xca, 0xfe, 0xfd, 0x89, 0x1a, 0x2b, 0x3c, 0x48, 0x55, 0xe6, 0xf7, 0x80}, 1},
		{0x1, 0, []byte{0xca, 0xfe, 0xfd, 0x89, 0x1a, 0x2b, 0x3c, 0x48, 0x55, 0xe6, 0xf7, 0x80}, 1},
		{0x1, 65, []byte{0xca, 0xfe, 0xfd, 0x89, 0x1a, 0x2b, 0x3c, 0x48, 0x55, 0xe6, 0xf7, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x80}, 1},
	}

	os := o.(*ostream)
	require.True(t, os.Empty())
	for _, input := range inputs {
		os.WriteBits(input.value, input.numBits)
		require.Equal(t, input.expectedBytes, os.rawBuffer)
		b, _ := os.RawBytes()
		require.Equal(t, input.expectedBytes, b)
		require.Equal(t, input.expectedPos, os.pos)
	}
	require.False(t, os.Empty())
}

func TestWriteBytes(t *testing.T) {
	testWriteBytes(t, NewOStream(nil, true, nil))
}

func TestWriteBytesWithPooling(t *testing.T) {
	testWriteBytes(t, NewOStream(nil, true, testBytesPool))
}

func testWriteBytes(t *testing.T, o OStream) {
	os := o.(*ostream)
	rawBytes := []byte{0x1, 0x2}
	os.WriteBytes(rawBytes)

	require.Equal(t, rawBytes, os.rawBuffer)

	b, pos := os.RawBytes()
	require.Equal(t, rawBytes, b)
	require.Equal(t, 8, pos)
	require.Equal(t, 8, os.pos)
}

func TestResetOStream(t *testing.T) {
	testResetOStream(t, NewOStream(nil, true, testBytesPool))
}

func TestResetOStreamWithPooling(t *testing.T) {
	testResetOStream(t, NewOStream(nil, true, testBytesPool))
}

func testResetOStream(t *testing.T, o OStream) {
	os := o.(*ostream)
	os.WriteByte(0xfe)
	os.Reset(nil)

	require.True(t, os.Empty())
	require.Equal(t, 0, os.Len())
	require.Equal(t, 0, os.pos)

	b, _ := os.RawBytes()
	require.Equal(t, nilBytes, b)
}

func BenchmarkWriteBytes(b *testing.B) {
	var (
		bytes     = make([]byte, 298)
		bytesPool = testBytesPool
		o         = NewOStream(nil, false, bytesPool)
	)
	for n := 0; n < b.N; n++ {
		o.Reset(nil)
		o.WriteBytes(bytes)
	}
}

func newTestCheckedBytesPool() pool.CheckedBytesPool {
	bytesPoolOpts := pool.NewObjectPoolOptions()

	bytesPool := pool.NewCheckedBytesPool([]pool.Bucket{
		pool.Bucket{
			Capacity: 16,
			Count:    1,
		},
		pool.Bucket{
			Capacity: 32,
			Count:    1,
		},
		pool.Bucket{
			Capacity: 64,
			Count:    1,
		},
		pool.Bucket{
			Capacity: 1,
			Count:    1,
		},
		pool.Bucket{
			Capacity: 256,
			Count:    1,
		},
		pool.Bucket{
			Capacity: 1440,
			Count:    1,
		},
		pool.Bucket{
			Capacity: 4096,
			Count:    1,
		},
		pool.Bucket{
			Capacity: 8192,
			Count:    1,
		},
	}, bytesPoolOpts, func(s []pool.Bucket) pool.BytesPool {
		return pool.NewBytesPool(s, bytesPoolOpts)
	})
	bytesPool.Init()
	return bytesPool
}
