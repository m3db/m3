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
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber-go/tally"
)

var (
	errPoolAlreadyInitialized   = errors.New("object pool already initialized")
	errPoolGetBeforeInitialized = errors.New("object pool get before initialized")
	errPoolPutBeforeInitialized = errors.New("object pool put before initialized")
)

const _emitMetricsRate = 2 * time.Second

type objectPool struct {
	wg                  sync.WaitGroup
	done                chan struct{}
	opts                ObjectPoolOptions
	values              chan interface{}
	alloc               Allocator
	size                int
	refillLowWatermark  int
	refillHighWatermark int
	filling             int32
	initialized         int32
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
		opts:   opts,
		done:   make(chan struct{}),
		values: make(chan interface{}, opts.Size()),
		size:   opts.Size(),
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

	return p
}

func (p *objectPool) Init(alloc Allocator) {
	if !atomic.CompareAndSwapInt32(&p.initialized, 0, 1) {
		fn := p.opts.OnPoolAccessErrorFn()
		fn(errPoolAlreadyInitialized)
		return
	}

	p.alloc = alloc

	for i := 0; i < cap(p.values); i++ {
		p.values <- p.alloc()
	}

	// TODO:remove
	// debug.PrintStack()

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()

		ticker := time.NewTicker(_emitMetricsRate)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				p.emitMetrics()
			case <-p.done:
				return
			}
		}
	}()
}

func (p *objectPool) Get() interface{} {
	if atomic.LoadInt32(&p.initialized) != 1 {
		fn := p.opts.OnPoolAccessErrorFn()
		fn(errPoolGetBeforeInitialized)
		return p.alloc()
	}

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

func (p *objectPool) Put(obj interface{}) {
	if atomic.LoadInt32(&p.initialized) != 1 {
		fn := p.opts.OnPoolAccessErrorFn()
		fn(errPoolPutBeforeInitialized)
		return
	}

	select {
	case p.values <- obj:
	default:
		p.metrics.putOnFull.Inc(1)
	}
}

func (p *objectPool) emitMetrics() {
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

// Close shuts down background routines
func (p *objectPool) Close() {
	close(p.done)
	p.wg.Wait()
}
