// Copyright (c) 2019 Uber Technologies, Inc.
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

package ts

import (
	"testing"

	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/pool"

	"github.com/stretchr/testify/assert"
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

func testSegmentCloneWithPools(
	t *testing.T,
	checkd byteFunc,
	pool pool.CheckedBytesPool,
) {
	seg := NewSegment(checkd(head), checkd(tail), FinalizeNone)

	assert.Equal(t, len(expected), seg.Len())
	cloned := seg.Clone(pool)
	assert.True(t, seg.Equal(&cloned))
	assert.True(t, seg.Flags == FinalizeNone)

	seg.Head.Reset(tail)
	assert.Equal(t, len(tail)*2, seg.Len())

	assert.False(t, seg.Equal(&cloned))
	assert.Equal(t, len(expected), cloned.Len())
	assert.True(t, cloned.Flags|FinalizeHead > 0)
	assert.True(t, cloned.Flags|FinalizeTail > 0)

	cloned.Finalize()
	seg.Finalize()
}

func TestSegmentNoPools(t *testing.T) {
	checkd := func(d []byte) checked.Bytes { return checked.NewBytes(d, nil) }
	testSegmentCloneWithPools(t, checkd, nil)
}

func TestPooledSegmentClone(t *testing.T) {
	bytesPool := pool.NewCheckedBytesPool([]pool.Bucket{pool.Bucket{
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

	testSegmentCloneWithPools(t, checkd, bytesPool)
}
