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
	"fmt"
	"sort"
)

// TODO(xichen): switch to a native pool based implementation when ready
type bucketPool struct {
	capacity int
	pool     ObjectPool
}

type floatsPool struct {
	opts              BucketPoolOptions
	sizesAsc          []Bucket
	buckets           []bucketPool
	maxBucketCapacity int
}

// NewFloatsPool creates a float slice pool
func NewFloatsPool(opts BucketPoolOptions) FloatsPool {
	if opts == nil {
		opts = NewBucketPoolOptions()
	}
	sizes := opts.Buckets()
	sizesAsc := make([]Bucket, len(sizes))
	copy(sizesAsc, sizes)
	sort.Sort(BucketByCapacity(sizesAsc))
	var maxBucketCapacity int
	if len(sizesAsc) != 0 {
		maxBucketCapacity = sizesAsc[len(sizesAsc)-1].Capacity
	}

	return &floatsPool{
		opts:              opts,
		sizesAsc:          sizesAsc,
		maxBucketCapacity: maxBucketCapacity,
	}
}

func (p *floatsPool) alloc(capacity int) []float64 {
	return make([]float64, 0, capacity)
}

func (p *floatsPool) Init() {
	var (
		opts    = NewObjectPoolOptions()
		buckets = make([]bucketPool, len(p.sizesAsc))
	)
	for i := range p.sizesAsc {
		size := p.sizesAsc[i].Count
		capacity := p.sizesAsc[i].Capacity

		o := opts.SetSize(size)
		if opts.MetricsScope() != nil {
			o = o.SetMetricsScope(opts.MetricsScope().Tagged(map[string]string{
				"bucket-capacity": fmt.Sprintf("%d", capacity),
			}))
		}

		buckets[i].capacity = capacity
		buckets[i].pool = NewObjectPool(o)
		buckets[i].pool.Init(func() interface{} {
			return p.alloc(capacity)
		})
	}
	p.buckets = buckets
}

func (p *floatsPool) Get(capacity int) []float64 {
	if capacity > p.maxBucketCapacity {
		return p.alloc(capacity)
	}
	for i := range p.buckets {
		if p.buckets[i].capacity >= capacity {
			return p.buckets[i].pool.Get().([]float64)
		}
	}
	return p.alloc(capacity)
}

func (p *floatsPool) Put(buffer []float64) {
	capacity := cap(buffer)
	if capacity > p.maxBucketCapacity {
		return
	}

	buffer = buffer[:0]
	for i := len(p.buckets) - 1; i >= 0; i-- {
		if capacity >= p.buckets[i].capacity {
			p.buckets[i].pool.Put(buffer)
			return
		}
	}
}
