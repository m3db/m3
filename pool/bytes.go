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
	"sort"

	"github.com/m3db/m3db/interfaces/m3db"
)

// TODO(r): instrument this to tune pooling
type bytesPool struct {
	sizesAsc          []m3db.PoolBucket
	buckets           []bytesPoolBucket
	maxBucketCapacity int
}

type bytesPoolBucket struct {
	capacity int
	values   chan []byte
}

// NewBytesPool creates a new pool
func NewBytesPool(sizes []m3db.PoolBucket) m3db.BytesPool {
	sizesAsc := make([]m3db.PoolBucket, len(sizes))
	copy(sizesAsc, sizes)
	sort.Sort(m3db.PoolBucketByCapacity(sizesAsc))
	var maxBucketCapacity int
	if len(sizesAsc) != 0 {
		maxBucketCapacity = sizesAsc[len(sizesAsc)-1].Capacity
	}
	return &bytesPool{sizesAsc: sizesAsc, maxBucketCapacity: maxBucketCapacity}
}

func (p *bytesPool) alloc(capacity int) []byte {
	return make([]byte, 0, capacity)
}

func (p *bytesPool) Init() {
	buckets := make([]bytesPoolBucket, len(p.sizesAsc))
	for i := range p.sizesAsc {
		buckets[i].capacity = p.sizesAsc[i].Capacity
		buckets[i].values = make(chan []byte, p.sizesAsc[i].Count)
		for j := 0; j < p.sizesAsc[i].Count; j++ {
			buckets[i].values <- p.alloc(p.sizesAsc[i].Capacity)
		}
	}
	p.buckets = buckets
}

func (p *bytesPool) Get(capacity int) []byte {
	if capacity > p.maxBucketCapacity {
		return p.alloc(capacity)
	}
	for i := range p.buckets {
		if p.buckets[i].capacity >= capacity {
			select {
			case b := <-p.buckets[i].values:
				return b
			default:
				// NB(r): use the bucket's capacity so can potentially
				// be returned to pool when it's finished with.
				return p.alloc(p.buckets[i].capacity)
			}
		}
	}
	return p.alloc(capacity)
}

func (p *bytesPool) Put(buffer []byte) {
	capacity := cap(buffer)
	if capacity > p.maxBucketCapacity {
		return
	}

	buffer = buffer[:0]
	for i := range p.buckets {
		if p.buckets[i].capacity >= capacity {
			select {
			case p.buckets[i].values <- buffer:
				return
			default:
				return
			}
		}
	}
}
