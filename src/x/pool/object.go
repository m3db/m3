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
	"errors"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber-go/tally"
)

var (
	errPoolAlreadyInitialized   = errors.New("object pool already initialized")
	errPoolGetBeforeInitialized = errors.New("object pool get before initialized")
	errPoolPutBeforeInitialized = errors.New("object pool put before initialized")

	_defaultShardCount = int(math.Max(math.Ceil(float64(runtime.GOMAXPROCS(0)/2.0)), 1.0))
)

const _emitMetricsRate = 2 * time.Second

type objectPool struct {
	wg                  sync.WaitGroup
	done                chan struct{}
	values              chan interface{}
	alloc               Allocator
	size                int
	refillLowWatermark  int
	refillHighWatermark int
	filling             int32
	dice                int32
	metrics             objectPoolMetrics
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

	m := opts.InstrumentOptions().MetricsScope()

	p := &objectPool{
		size: opts.Size(),
		refillLowWatermark: int(math.Ceil(
			opts.RefillLowWatermark() * float64(opts.Size()))),
		refillHighWatermark: int(math.Ceil(
			opts.RefillHighWatermark() * float64(opts.Size()))),
		metrics: objectPoolMetrics{
			free:       m.Gauge("free"),
			total:      m.Gauge("total"),
			getOnEmpty: m.Counter("get-on-empty"),
			putOnFull:  m.Counter("put-on-full"),
		},
	}

	p.setGauges()

	return p
}

// Init initializes and allocates object pool
func (p *objectPool) Init(alloc Allocator) {
	if p.alloc != nil {
		panic("pool already initalized")
	}
	p.alloc = alloc

	p.done = make(chan struct{})
	p.values = make(chan interface{}, p.size)

	for i := 0; i < cap(p.values); i++ {
		p.values <- p.alloc()
	}

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()

		ticker := time.NewTicker(_emitMetricsRate)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				p.setGauges()
			case <-p.done:
				return
			}
		}
	}()
}

// Get returns an object from the pool
func (p *objectPool) Get() interface{} {
	var v interface{}
	select {
	case v = <-p.values:
	default:
		v = p.alloc()
		p.metrics.getOnEmpty.Inc(1)
	}

	if p.refillLowWatermark > 0 && len(p.values) <= p.refillLowWatermark {
		p.tryFill()
	}

	return v
}

// Put puts object back into the object pool
func (p *objectPool) Put(obj interface{}) {
	select {
	case p.values <- obj:
	default:
		p.metrics.putOnFull.Inc(1)
	}

}

func (p *objectPool) setGauges() {
	p.metrics.free.Update(float64(len(p.values)))
	p.metrics.total.Update(float64(p.size))
}

func (p *objectPool) tryFill() {
	if !atomic.CompareAndSwapInt32(&p.filling, 0, 1) {
		return
	}

	go func() {
		defer atomic.StoreInt32(&p.filling, 0)

		for len(p.values) < p.refillHighWatermark {
			select {
			case p.values <- p.alloc():
			default:
				return
			}
		}
	}()
}

// Close stops background processes
func (p *objectPool) Close() {
	close(p.done)
	p.wg.Wait()
}
