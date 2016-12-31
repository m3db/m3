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

package pool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBytesPool(t *testing.T) {
	p := getBytesPool(2, []int{5, 10})
	p.Init()

	assert.Equal(t, []byte(nil), p.Get(0))

	b1 := p.Get(1)
	assert.Equal(t, 0, len(b1))
	assert.Equal(t, 5, cap(b1))
	b1 = append(b1, 'a')

	b2 := p.Get(3)
	assert.Equal(t, 0, len(b2))
	assert.Equal(t, 5, cap(b2))
	b2 = append(b1, 'b')
	assert.NotEqual(t, b1, b2)
	p.Put(b1)

	b3 := p.Get(2)
	assert.Equal(t, 0, len(b3))
	assert.Equal(t, 5, cap(b3))
	assert.Equal(t, b1, b3[:1])
}

func TestBytesPoolGetLargerThanLargestBucket(t *testing.T) {
	p := getBytesPool(2, []int{8})
	p.Init()

	x := p.Get(16)
	assert.NotNil(t, x)
	assert.Equal(t, 16, cap(x))
	assert.Equal(t, 0, len(x))

	// Assert not from pool
	bucketed := p.pool.(*bucketizedObjectPool)
	assert.Equal(t, 1, len(bucketed.buckets))
	assert.Equal(t, 2, len(bucketed.buckets[0].pool.(*objectPool).values))
}

func TestAppendByte(t *testing.T) {
	p := getBytesPool(1, []int{3, 10})
	p.Init()
	vals := []byte{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i'}

	b1 := p.Get(2)
	b2 := b1
	for _, val := range vals {
		b2 = AppendByte(b2, val, p)
	}

	assert.Equal(t, b2, vals)
	assert.Equal(t, 9, len(b2))
	assert.Equal(t, 10, cap(b2))

	b3 := p.Get(2)
	assert.Equal(t, cap(b1), cap(b3))
	assert.Equal(t, b1[:3], b3[:3])
}

func getBytesPool(bucketSizes int, bucketCaps []int) *bytesPool {
	buckets := make([]Bucket, len(bucketCaps))
	for i, cap := range bucketCaps {
		buckets[i] = Bucket{
			Count:    bucketSizes,
			Capacity: cap,
		}
	}

	return NewBytesPool(buckets, nil).(*bytesPool)
}
