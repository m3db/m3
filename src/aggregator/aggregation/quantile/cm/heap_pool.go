// Copyright (c) 2021 Uber Technologies, Inc.
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

package cm

import (
	"sync"
)

const (
	// create buckets of 64, 256, 1024, 4096, 16484, 65536 floats
	_initialHeapBucketSize      = 64
	_heapSizeBuckets            = 6
	_heapSizeBucketGrowthFactor = 4
)

var sharedHeapPool heapPool

type heapPool struct {
	pools []*sync.Pool
	sizes []int
}

func newHeapPool() heapPool {
	var (
		p = heapPool{
			pools: make([]*sync.Pool, _heapSizeBuckets),
			sizes: make([]int, _heapSizeBuckets),
		}
		sz = _initialHeapBucketSize
	)

	for i := 0; i < _heapSizeBuckets; i++ {
		size := sz

		p.sizes[i] = size
		p.pools[i] = &sync.Pool{
			New: func() interface{} {
				buf := make(minHeap, 0, size)
				return &buf
			},
		}

		sz *= _heapSizeBucketGrowthFactor
	}

	return p
}

func (p heapPool) Get(size int) *minHeap {
	for i := range p.sizes {
		if p.sizes[i] < size {
			continue
		}

		v, ok := p.pools[i].Get().(*minHeap)
		if !ok { // should never happen
			goto makeNew
		}

		*v = (*v)[:0]
		return v
	}
makeNew:
	newVal := make(minHeap, 0, size)

	return &newVal
}

func (p heapPool) Put(value minHeap) {
	size := cap(value)
	for i := 0; i < len(p.sizes); i++ {
		if p.sizes[i] < size {
			continue
		}
		pool := p.pools[i]
		value = value[:0]
		pool.Put(&value)
		return
	}
}

func init() {
	sharedHeapPool = newHeapPool()
}
