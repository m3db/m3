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

	f1 := p.Get(1)
	assert.Equal(t, 0, len(f1))
	assert.Equal(t, 5, cap(f1))
	f1 = append(f1, 1.0)

	f2 := p.Get(3)
	assert.Equal(t, 0, len(f2))
	assert.Equal(t, 5, cap(f2))
	f2 = append(f1, 2.0)
	assert.NotEqual(t, f1, f2)
	p.Put(f1)

	f3 := p.Get(2)
	assert.Equal(t, 0, len(f3))
	assert.Equal(t, 5, cap(f3))
	assert.Equal(t, f1, f3[:1])
}

// nolint: unparam
func getFloatsPool(bucketSizes int, bucketCaps []int) *floatsPool {
	buckets := make([]Bucket, len(bucketCaps))
	for i, cap := range bucketCaps {
		buckets[i] = Bucket{
			Count:    bucketSizes,
			Capacity: cap,
		}
	}

	return NewFloatsPool(buckets, nil).(*floatsPool)
}
