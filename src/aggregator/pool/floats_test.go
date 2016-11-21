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

func TestFloatsPool(t *testing.T) {
	p := getFloatsPool(2, []int{5, 10})
	p.Init()
	assert.Equal(t, 2, len(p.buckets))

	b1 := p.Get(1)
	assert.Equal(t, 0, len(b1))
	assert.Equal(t, 5, cap(b1))
	b1 = append(b1, 1.0)

	b2 := p.Get(3)
	assert.Equal(t, 0, len(b2))
	assert.Equal(t, 5, cap(b2))
	b2 = append(b1, 2.0)
	assert.NotEqual(t, b1, b2)
	p.Put(b1)

	b3 := p.Get(2)
	assert.Equal(t, 0, len(b3))
	assert.Equal(t, 5, cap(b3))
	assert.Equal(t, b1, b3[:1])
}

func getFloatsPool(bucketSizes int, bucketCaps []int) *floatsPool {
	buckets := make([]Bucket, len(bucketCaps))
	for i, cap := range bucketCaps {
		buckets[i] = Bucket{
			Count:    bucketSizes,
			Capacity: cap,
		}
	}

	opts := NewBucketPoolOptions().SetBuckets(buckets)
	return NewFloatsPool(opts).(*floatsPool)
}
