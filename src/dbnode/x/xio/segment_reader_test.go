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

package xio

import (
	"encoding/binary"
	"io"
	"testing"

	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/pool"

	"github.com/stretchr/testify/require"
)

var (
	head = []byte{
		0x20, 0xc5, 0x10, 0x55, 0x0, 0x0, 0x0, 0x0, 0x1f, 0x0, 0x0, 0x0, 0x0,
		0xa0, 0x90, 0xf, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x5, 0x28, 0x3f, 0x2f,
		0xc0, 0x1, 0xf4, 0x1, 0x0, 0x0, 0x0, 0x2, 0x1, 0x2, 0x7, 0x10, 0x1e,
		0x0, 0x1, 0x0, 0xe0, 0x65, 0x58,
	}
	tail = []byte{
		0xcd, 0x3, 0x0, 0x0, 0x0, 0x0,
	}

	expected = []byte{
		0x20, 0xc5, 0x10, 0x55, 0x0, 0x0, 0x0, 0x0, 0x1f, 0x0, 0x0, 0x0, 0x0,
		0xa0, 0x90, 0xf, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x5, 0x28, 0x3f, 0x2f,
		0xc0, 0x1, 0xf4, 0x1, 0x0, 0x0, 0x0, 0x2, 0x1, 0x2, 0x7, 0x10, 0x1e,
		0x0, 0x1, 0x0, 0xe0, 0x65, 0x58, 0xcd, 0x3, 0x0, 0x0, 0x0, 0x0,
	}
)

type byteFunc func(d []byte) checked.Bytes

func checkedNoPool(d []byte) checked.Bytes {
	return checked.NewBytes(d, nil)
}

func testSegmentReader(
	t *testing.T,
	checkd byteFunc,
	pool pool.CheckedBytesPool,
) {
	checksum := uint32(10)
	segment := ts.NewSegment(checkd(head), checkd(tail), checksum, ts.FinalizeNone)
	r := NewSegmentReader(segment)
	bytes, err := ToBytes(r)
	require.Equal(t, io.EOF, err)
	require.Equal(t, expected, bytes)

	seg, err := r.Segment()
	require.NoError(t, err)
	require.Equal(t, head, seg.Head.Bytes())
	require.Equal(t, tail, seg.Tail.Bytes())
	require.Equal(t, checksum, seg.CalculateChecksum())

	// Ensure cloned segment reader does not share original head and tail.
	cloned, err := r.Clone(pool)
	require.NoError(t, err)

	segment.Head.Reset(tail)
	segment.Tail.Reset(head)

	seg, err = cloned.Segment()
	require.NoError(t, err)
	require.Equal(t, head, seg.Head.Bytes())
	require.Equal(t, tail, seg.Tail.Bytes())
	require.Equal(t, checksum, seg.CalculateChecksum())

	cloned.Finalize()
	segment.Finalize()
}

func TestSegmentReaderNoPool(t *testing.T) {
	testSegmentReader(t, checkedNoPool, nil)
}

func TestSegmentReaderWithPool(t *testing.T) {
	bytesPool := pool.NewCheckedBytesPool([]pool.Bucket{{
		Capacity: 1024,
		Count:    10,
	}}, nil, func(s []pool.Bucket) pool.BytesPool {
		return pool.NewBytesPool(s, nil)
	})
	bytesPool.Init()
	checkd := func(d []byte) checked.Bytes {
		b := bytesPool.Get(len(d))
		b.IncRef()
		b.Reset(d)
		b.DecRef()
		return b
	}

	testSegmentReader(t, checkd, bytesPool)
}

func TestSegmentReader64(t *testing.T) {
	data := make([]byte, 32)
	for i := range data {
		data[i] = 100 + byte(i)
	}

	for headLen := 0; headLen < len(data); headLen++ {
		for tailLen := 0; tailLen < len(data)-headLen; tailLen++ {
			testSegmentReader64(t, data[:headLen], data[headLen:headLen+tailLen])
		}
	}
}

func testSegmentReader64(t *testing.T, head []byte, tail []byte) {
	t.Helper()

	var expected []byte
	expected = append(expected, head...)
	expected = append(expected, tail...)

	var (
		segment      = ts.NewSegment(checkedNoPool(head), checkedNoPool(tail), 0, ts.FinalizeNone)
		r            = NewSegmentReader(segment)
		peeked, read []byte
		buf          [8]byte
		word         uint64
		n            byte
		err          error
	)

	for {
		word, n, err = r.Peek64()
		if err != nil {
			break
		}
		binary.BigEndian.PutUint64(buf[:], word)
		peeked = append(peeked, buf[:n]...)

		word, n, err = r.Read64()
		require.NoError(t, err)

		binary.BigEndian.PutUint64(buf[:], word)
		read = append(read, buf[:n]...)
	}

	require.Equal(t, io.EOF, err)
	require.Equal(t, expected, peeked)
	require.Equal(t, expected, read)

	_, _, err = r.Read64()
	require.Equal(t, io.EOF, err)
}
