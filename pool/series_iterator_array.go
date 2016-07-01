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
type seriesIteratorArrayPool struct {
	sizesAsc          []m3db.PoolBucket
	buckets           []seriesIteratorArrayPoolBucket
	maxBucketCapacity int
}

type seriesIteratorArrayPoolBucket struct {
	capacity int
	values   chan []m3db.SeriesIterator
}

// NewSeriesIteratorArrayPool creates a new pool
func NewSeriesIteratorArrayPool(sizes []m3db.PoolBucket) m3db.SeriesIteratorArrayPool {
	sizesAsc := make([]m3db.PoolBucket, len(sizes))
	copy(sizesAsc, sizes)
	sort.Sort(m3db.PoolBucketByCapacity(sizesAsc))
	var maxBucketCapacity int
	if len(sizesAsc) != 0 {
		maxBucketCapacity = sizesAsc[len(sizesAsc)-1].Capacity
	}
	return &seriesIteratorArrayPool{sizesAsc: sizesAsc, maxBucketCapacity: maxBucketCapacity}
}

func (p *seriesIteratorArrayPool) alloc(capacity int) []m3db.SeriesIterator {
	return make([]m3db.SeriesIterator, 0, capacity)
}

func (p *seriesIteratorArrayPool) Init() {
	buckets := make([]seriesIteratorArrayPoolBucket, len(p.sizesAsc))
	for i := range p.sizesAsc {
		buckets[i].capacity = p.sizesAsc[i].Capacity
		buckets[i].values = make(chan []m3db.SeriesIterator, p.sizesAsc[i].Count)
		for j := 0; j < p.sizesAsc[i].Count; j++ {
			buckets[i].values <- p.alloc(p.sizesAsc[i].Capacity)
		}
	}
	p.buckets = buckets
}

func (p *seriesIteratorArrayPool) Get(capacity int) []m3db.SeriesIterator {
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

func (p *seriesIteratorArrayPool) Put(array []m3db.SeriesIterator) {
	capacity := cap(array)
	if capacity > p.maxBucketCapacity {
		return
	}

	array = array[:0]
	for i := range p.buckets {
		if p.buckets[i].capacity >= capacity {
			select {
			case p.buckets[i].values <- array:
				return
			default:
				return
			}
		}
	}
}
