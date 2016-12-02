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
	sizesAsc          []Bucket
	buckets           []bytesBucket
	maxBucketCapacity int
	opts              ObjectPoolOptions
	maxAlloc          tally.Counter
}

type bytesBucket struct {
	capacity int
	pool     ObjectPool
}

// NewBytesPool creates a new pool
func NewBytesPool(sizes []Bucket, opts ObjectPoolOptions) BytesPool {
	if opts == nil {
		opts = NewObjectPoolOptions()
	}

	sizesAsc := make([]Bucket, len(sizes))
	copy(sizesAsc, sizes)
	sort.Sort(BucketByCapacity(sizesAsc))

	var maxBucketCapacity int
	if len(sizesAsc) != 0 {
		maxBucketCapacity = sizesAsc[len(sizesAsc)-1].Capacity
	}

	iopts := opts.InstrumentOptions()

	return &bytesPool{
		opts:              opts,
		sizesAsc:          sizesAsc,
		maxBucketCapacity: maxBucketCapacity,
		maxAlloc:          iopts.MetricsScope().Counter("alloc-max"),
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

		opts := p.opts.SetSize(size)
		iopts := opts.InstrumentOptions()

		if iopts.MetricsScope() != nil {
			opts = opts.SetInstrumentOptions(iopts.SetMetricsScope(
				iopts.MetricsScope().Tagged(map[string]string{
					"bucket-capacity": fmt.Sprintf("%d", capacity),
				})))
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
		p.maxAlloc.Inc(1)
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
	for i := len(p.buckets) - 1; i >= 0; i-- {
		if capacity >= p.buckets[i].capacity {
			p.buckets[i].pool.Put(buffer)
			return
		}
	}
}

// AppendByte appends a byte to a byte slice getting a new slice from the
// BytesPool if the slice is at capacity
func AppendByte(bytes []byte, b byte, pool BytesPool) []byte {
	if len(bytes) == cap(bytes) {
		newBytes := pool.Get(cap(bytes) * 2)
		n := copy(newBytes[:len(bytes)], bytes)
		pool.Put(bytes)
		bytes = newBytes[:n]
	}

	return append(bytes, b)
}
