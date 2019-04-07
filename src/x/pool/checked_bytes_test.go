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

func TestCheckedBytesPool(t *testing.T) {
	p := getCheckedBytesPool(2, []int{5, 10})
	p.Init()

	assert.Equal(t, 2, checkedBytesPoolBucketLen(p, 0))
	assert.Equal(t, 2, checkedBytesPoolBucketLen(p, 1))

	b1 := p.Get(1)
	b1.IncRef()

	assert.Equal(t, 1, checkedBytesPoolBucketLen(p, 0))
	assert.Equal(t, 2, checkedBytesPoolBucketLen(p, 1))

	assert.Equal(t, 0, b1.Len())
	assert.Equal(t, 5, b1.Cap())
	b1.Append('a')

	b2 := p.Get(3)
	b2.IncRef()

	assert.Equal(t, 0, checkedBytesPoolBucketLen(p, 0))
	assert.Equal(t, 2, checkedBytesPoolBucketLen(p, 1))

	assert.Equal(t, 0, b2.Len())
	assert.Equal(t, 5, b2.Cap())
	b2.Append('b')

	assert.NotEqual(t, b1.Bytes(), b2.Bytes())

	copiedB1 := append([]byte(nil), b1.Bytes()...)
	b1.DecRef()
	b1.Finalize()

	assert.Equal(t, 1, checkedBytesPoolBucketLen(p, 0))
	assert.Equal(t, 2, checkedBytesPoolBucketLen(p, 1))

	b3 := p.Get(2)
	b3.IncRef()
	assert.Equal(t, 0, b3.Len())
	assert.Equal(t, 5, b3.Cap())
	assert.Equal(t, copiedB1, b3.Bytes()[:1])
}

func TestAppendByteChecked(t *testing.T) {
	p := getCheckedBytesPool(1, []int{3, 10})
	p.Init()
	vals := []byte{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i'}

	b1 := p.Get(2)
	b1.IncRef()

	firstB1 := b1

	for _, val := range vals {
		if b, swapped := AppendByteChecked(b1, val, p); swapped {
			b1.DecRef()
			b1.Finalize()
			b1 = b
			b1.IncRef()
		}
	}

	// Ensure swapped out with new pooled bytes
	assert.NotEqual(t, firstB1, b1)

	assert.Equal(t, vals, b1.Bytes())
	assert.Equal(t, 9, b1.Len())
	assert.Equal(t, 10, b1.Cap())

	// Ensure reusing the first slice we retrieved
	b2 := p.Get(2)
	b2.IncRef()
	assert.Equal(t, 3, b2.Cap())
	assert.Equal(t, b1.Bytes()[:3], b2.Bytes()[:3])
}

func getCheckedBytesPool(
	bucketSizes int,
	bucketCaps []int,
) *checkedBytesPool {
	buckets := make([]Bucket, len(bucketCaps))
	for i, cap := range bucketCaps {
		buckets[i] = Bucket{
			Count:    bucketSizes,
			Capacity: cap,
		}
	}

	return NewCheckedBytesPool(buckets, nil, func(s []Bucket) BytesPool {
		return NewBytesPool(s, nil)
	}).(*checkedBytesPool)
}

func checkedBytesPoolBucketLen(
	p *checkedBytesPool,
	bucket int,
) int {
	bucketizedPool := p.pool.(*bucketizedObjectPool)
	objectPool := bucketizedPool.buckets[bucket].pool.(*objectPool)
	return len(objectPool.values)
}
