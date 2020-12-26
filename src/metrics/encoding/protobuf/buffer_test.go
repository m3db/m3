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

package protobuf

import (
	"testing"

	"github.com/m3db/m3/src/x/pool"

	"github.com/stretchr/testify/require"
)

func TestBufferWithPool(t *testing.T) {
	buckets := []pool.Bucket{
		{Capacity: 16, Count: 1},
	}
	p := pool.NewBytesPool(buckets, nil)
	p.Init()
	data := p.Get(16)[:16]
	data[0] = 0xff

	buf := NewBuffer(data, p.Put)
	require.NotNil(t, buf.buf)

	buf.Close()
	require.Nil(t, buf.finalizer)
	require.Nil(t, buf.buf)

	// Verify that closing the buffer returns the buffer to pool.
	buf2 := p.Get(16)[:16]
	require.Equal(t, byte(0xff), buf2[0])

	// Verify that closing the buffer a second time is a no op.
	buf.Close()
	buf3 := p.Get(16)[:16]
	require.NotEqual(t, 0xff, buf3[0])
}

func TestBufferNilPool(t *testing.T) {
	data := make([]byte, 16)
	buf := NewBuffer(data, nil)
	require.Equal(t, data, buf.Bytes())
	require.NotNil(t, buf.buf)

	buf.Close()
	require.Nil(t, buf.finalizer)
	require.Nil(t, buf.buf)
}

func TestBufferTruncate(t *testing.T) {
	data := []byte{1, 2, 3, 4}
	buf := NewBuffer(data, nil)
	require.Equal(t, data, buf.Bytes())

	for i := len(data); i >= 0; i-- {
		buf.Truncate(i)
		require.Equal(t, data[:i], buf.Bytes())
	}
}

func TestAllocate(t *testing.T) {
	buckets := []pool.Bucket{
		{Capacity: 16, Count: 1},
		{Capacity: 32, Count: 1},
	}
	p := pool.NewBytesPool(buckets, nil)
	p.Init()

	// Verify allocating a byte slice of size 16 will return a byte
	// slice of exactly the same size.
	res := allocate(p, 16)
	require.Equal(t, 16, len(res))

	// Verify allocating a byte slice of size 24 will return a byte
	// slice whose size is larger than 24.
	res = allocate(p, 24)
	require.True(t, len(res) > 24)
}

func TestAllocateNilPool(t *testing.T) {
	res := allocate(nil, 12)
	require.Equal(t, 12, len(res))
}

func TestEnsureBufferSizeEnoughCapacity(t *testing.T) {
	buf := []byte{1, 2, 0, 0, 0}
	res := ensureBufferSize(buf, nil, 3, dontCopyData)
	require.Equal(t, buf, res)
	buf[0] = 4
	require.Equal(t, byte(4), res[0])
}

func TestEnsureBufferSizeSmallTargetSizeDontCopyData(t *testing.T) {
	buckets := []pool.Bucket{
		{Capacity: 16, Count: 1},
		{Capacity: 32, Count: 1},
	}
	p := pool.NewBytesPool(buckets, nil)
	p.Init()

	buf := p.Get(16)[:16]
	buf[0] = 12
	res := ensureBufferSize(buf, p, 24, dontCopyData)
	require.Equal(t, make([]byte, 32), res)

	// Verify the original buffer has been returned to pool.
	buf2 := p.Get(16)[:16]
	require.Equal(t, byte(12), buf2[0])
}

func TestEnsureBufferSizeLargeTargetSizeDontCopyData(t *testing.T) {
	buckets := []pool.Bucket{
		{Capacity: 16, Count: 1},
		{Capacity: 32, Count: 1},
	}
	p := pool.NewBytesPool(buckets, nil)
	p.Init()

	buf := p.Get(16)[:16]
	buf[0] = 12
	res := ensureBufferSize(buf, p, 128, dontCopyData)
	require.Equal(t, make([]byte, 128), res)

	// Verify the original buffer has been returned to pool.
	buf2 := p.Get(16)[:16]
	require.Equal(t, byte(12), buf2[0])
}

func TestEnsureBufferSizeSmallTargetSizeCopyData(t *testing.T) {
	buckets := []pool.Bucket{
		{Capacity: 16, Count: 1},
		{Capacity: 32, Count: 1},
	}
	p := pool.NewBytesPool(buckets, nil)
	p.Init()

	buf := p.Get(16)[:16]
	buf[0] = 12
	res := ensureBufferSize(buf, p, 24, copyData)
	require.Equal(t, 32, len(res))
	require.Equal(t, byte(12), res[0])

	// Verify the returned value is a copy of the original slice.
	buf[0] = 34
	require.Equal(t, byte(12), res[0])

	// Verify the original buffer has been returned to pool.
	buf2 := p.Get(16)[:16]
	require.Equal(t, byte(34), buf2[0])
}

func TestEnsureBufferSizeLargeTargetSizeCopyData(t *testing.T) {
	buckets := []pool.Bucket{
		{Capacity: 16, Count: 1},
		{Capacity: 32, Count: 1},
	}
	p := pool.NewBytesPool(buckets, nil)
	p.Init()

	buf := p.Get(16)[:16]
	buf[0] = 12
	res := ensureBufferSize(buf, p, 128, copyData)
	require.Equal(t, 128, len(res))
	require.Equal(t, byte(12), res[0])

	// Verify the returned value is a copy of the original slice.
	buf[0] = 34
	require.Equal(t, byte(12), res[0])

	// Verify the original buffer has been returned to pool.
	buf2 := p.Get(16)[:16]
	require.Equal(t, byte(34), buf2[0])
}

func TestEnsureBufferSizeNilPool(t *testing.T) {
	buf := []byte{1, 2, 0, 0, 0}
	res := ensureBufferSize(buf, nil, 20, copyData)
	require.Equal(t, 20, len(res))
	require.Equal(t, byte(1), res[0])
	require.Equal(t, byte(2), res[1])

	// Verify the returned value is a copy of the original slice.
	buf[0] = 123
	require.Equal(t, byte(1), res[0])
}
