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

package resend

import (
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
	"github.com/uber-go/tally"
)

// BufferPool is a pool of Buffers.
type BufferPool interface {
	// Get gets a Buffer from the pool.
	Get() Buffer

	// put returns a Buffer to the pool. This is a private method called
	// by the resend buffer itself when it is closed.
	put(b Buffer)
}

type bufferPool struct {
	pool pool.ObjectPool
}

// BufferPoolOptions are options for resend buffer pools.
type BufferPoolOptions struct {
	// PoolSize is the initial size for the resend buffer pool itself.
	PoolSize int
	// BufferSize is the size for each resend buffer.
	BufferSize int
	// WarnThreshold will increment a warning metric when any buffer exceeds
	// this percentage of BufferSize. 0 <= WarnThreshold <= 1
	WarnThreshold float64
	// CriticalThreshold will increment a critical metric when any buffer exceeds
	// this percentage of BufferSize. 0 <= CriticalThreshold <= 1
	CriticalThreshold float64
	// InstrumentOptions are instrumentation options.
	InstrumentOptions instrument.Options
}

// NewBufferPool creates and initializes a resend buffer pool.
func NewBufferPool(opts BufferPoolOptions) BufferPool {
	iOpts := opts.InstrumentOptions
	scope := iOpts.MetricsScope()

	poolOpts := pool.NewObjectPoolOptions().
		SetSize(opts.PoolSize).
		SetInstrumentOptions(iOpts.SetMetricsScope(scope.SubScope("resend_pool")))

	objPool := pool.NewObjectPool(poolOpts)
	bufferPool := &bufferPool{
		pool: objPool,
	}

	metricsScopeForType := func(rsType string) tally.Scope {
		return scope.SubScope("resend").Tagged(map[string]string{"type": rsType})
	}

	size := opts.BufferSize
	warnThreshold := opts.WarnThreshold
	if warnThreshold > 1 {
		warnThreshold = 1
	}

	critThreshold := opts.CriticalThreshold
	if critThreshold > 1 {
		critThreshold = 1
	}

	resendBufferOpts := bufferOpts{
		size:              size,
		pool:              bufferPool,
		warnThreshold:     float64(size) * warnThreshold,
		criticalThreshold: float64(size) * critThreshold,
		minMetrics:        newBufferMetrics(size, metricsScopeForType("min")),
		maxMetrics:        newBufferMetrics(size, metricsScopeForType("max")),
	}

	objPool.Init(func() interface{} { return newBuffer(resendBufferOpts) })
	return bufferPool
}

func (p *bufferPool) Get() Buffer {
	return p.pool.Get().(*buffer)
}

func (p *bufferPool) put(b Buffer) {
	p.pool.Put(b)
}
