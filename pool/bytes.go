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

	"github.com/uber-go/tally"
)

type bytesPool struct {
	scope             tally.Scope
	sizesAsc          []Bucket
	buckets           []bytesBucket
	maxBucketCapacity int
}

type bytesBucket struct {
	capacity int
	pool     ObjectPool
}

// NewBytesPool creates a new pool
func NewBytesPool(sizes []Bucket, scope tally.Scope) BytesPool {
	sizesAsc := make([]Bucket, len(sizes))
	copy(sizesAsc, sizes)
	sort.Sort(BucketByCapacity(sizesAsc))
	var maxBucketCapacity int
	if len(sizesAsc) != 0 {
		maxBucketCapacity = sizesAsc[len(sizesAsc)-1].Capacity
	}
	return &bytesPool{
		scope:             scope,
		sizesAsc:          sizesAsc,
		maxBucketCapacity: maxBucketCapacity,
	}
}

func (p *bytesPool) alloc(capacity int) []byte {
	return make([]byte, 0, capacity)
}

func (p *bytesPool) Init() {
	buckets := make([]bytesBucket, len(p.sizesAsc))
	for i := range p.sizesAsc {
		size := p.sizesAsc[i].Count
		capacity := p.sizesAsc[i].Capacity

		opts := NewObjectPoolOptions().SetSize(size)
		if p.scope != nil {
			opts = opts.SetMetricsScope(p.scope.Tagged(map[string]string{
				"bucket-capacity": fmt.Sprintf("%d", capacity),
			}))
		}

		buckets[i].capacity = capacity
		buckets[i].pool = NewObjectPool(opts)
		buckets[i].pool.Init(func() interface{} {
			return p.alloc(capacity)
		})
	}
	p.buckets = buckets
}

func (p *bytesPool) Get(capacity int) []byte {
	if capacity > p.maxBucketCapacity {
		return p.alloc(capacity)
	}
	for i := range p.buckets {
		if p.buckets[i].capacity >= capacity {
			return p.buckets[i].pool.Get().([]byte)
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
			p.buckets[i].pool.Put(buffer)
			return
		}
	}
}
