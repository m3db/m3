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
	"sync/atomic"
	"time"

	"github.com/uber-go/tally"
)

const (
	// TODO(r): Use tally sampling when available
	sampleObjectPoolLengthEvery = 100
)

type objectPool struct {
	opts               ObjectPoolOptions
	values             chan interface{}
	alloc              Allocator
	size               int
	refillLowWatermark int
	filling            int64
	metrics            objectPoolMetrics
}

type objectPoolMetrics struct {
	free       tally.Gauge
	total      tally.Gauge
	getOnEmpty tally.Counter
	putOnFull  tally.Counter
}

// NewObjectPool creates a new pool
func NewObjectPool(opts ObjectPoolOptions) ObjectPool {
	if opts == nil {
		opts = NewObjectPoolOptions()
	}
	p := &objectPool{
		opts:               opts,
		values:             make(chan interface{}, opts.Size()),
		size:               opts.Size(),
		refillLowWatermark: opts.RefillLowWatermark(),
		metrics: objectPoolMetrics{
			free:       opts.MetricsScope().Gauge("free"),
			total:      opts.MetricsScope().Gauge("total"),
			getOnEmpty: opts.MetricsScope().Counter("get-on-empty"),
			putOnFull:  opts.MetricsScope().Counter("put-on-full"),
		},
	}
	p.setGauges()
	return p
}

func (p *objectPool) Init(alloc Allocator) {
	capacity := cap(p.values)
	for i := 0; i < capacity; i++ {
		p.values <- alloc()
	}
	p.alloc = alloc
}

func (p *objectPool) Get() interface{} {
	select {
	case v := <-p.values:
		p.trySetGauges()
		if p.refillLowWatermark > 0 && len(p.values) < p.refillLowWatermark {
			p.tryFill()
		}
		return v
	default:
		p.trySetGauges()
		if p.refillLowWatermark > 0 {
			p.tryFill()
		}
		p.metrics.getOnEmpty.Inc(1)
		return p.alloc()
	}
}

func (p *objectPool) Put(obj interface{}) {
	select {
	case p.values <- obj:
		p.trySetGauges()
		return
	default:
		p.trySetGauges()
		p.metrics.putOnFull.Inc(1)
		return
	}
}

func (p *objectPool) trySetGauges() {
	if time.Now().UnixNano()%sampleObjectPoolLengthEvery == 0 {
		p.setGauges()
	}
}

func (p *objectPool) setGauges() {
	p.metrics.free.Update(int64(len(p.values)))
	p.metrics.total.Update(int64(p.size))
}

func (p *objectPool) tryFill() {
	if !atomic.CompareAndSwapInt64(&p.filling, 0, 1) {
		return
	}
	go func() {
		full := false
		for !full {
			select {
			case p.values <- p.alloc():
			default:
				full = true
			}
		}
		atomic.StoreInt64(&p.filling, 0)
	}()
}
