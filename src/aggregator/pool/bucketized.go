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

type bucketPool struct {
	capacity int
	pool     ObjectPool
}

// TODO(xichen): switch to a native pool based implementation when ready
type bucketizedObjectPool struct {
	opts              BucketizedObjectPoolOptions
	alloc             BucketizedAllocator
	sizesAsc          []Bucket
	buckets           []bucketPool
	maxBucketCapacity int
}

// NewBucketizedObjectPool creates a bucketized object pool
func NewBucketizedObjectPool(opts BucketizedObjectPoolOptions) BucketizedObjectPool {
	if opts == nil {
		opts = NewBucketizedObjectPoolOptions()
	}
	sizes := opts.Buckets()
	sizesAsc := make([]Bucket, len(sizes))
	copy(sizesAsc, sizes)
	sort.Sort(BucketByCapacity(sizesAsc))
	var maxBucketCapacity int
	if len(sizesAsc) != 0 {
		maxBucketCapacity = sizesAsc[len(sizesAsc)-1].Capacity
	}

	return &bucketizedObjectPool{
		opts:              opts,
		sizesAsc:          sizesAsc,
		maxBucketCapacity: maxBucketCapacity,
	}
}

func (p *bucketizedObjectPool) Init(alloc BucketizedAllocator) {
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
			return alloc(capacity)
		})
	}
	p.buckets = buckets
	p.alloc = alloc
}

func (p *bucketizedObjectPool) Get(capacity int) interface{} {
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

func (p *bucketizedObjectPool) Put(obj interface{}, capacity int) {
	if capacity > p.maxBucketCapacity {
		return
	}

	for i := len(p.buckets) - 1; i >= 0; i-- {
		if capacity >= p.buckets[i].capacity {
			p.buckets[i].pool.Put(obj)
			return
		}
	}
}
