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

package aggregator

import (
	"github.com/m3db/m3/src/x/pool"
)

// CounterElemAlloc allocates a new counter element.
type CounterElemAlloc func() *CounterElem

// CounterElemPool provides a pool of counter elements.
type CounterElemPool interface {
	// Init initializes the counter element pool.
	Init(alloc CounterElemAlloc)

	// Get gets a counter element from the pool.
	Get() *CounterElem

	// Put returns a counter element to the pool.
	Put(value *CounterElem)
}

// TimerElemAlloc allocates a new timer element.
type TimerElemAlloc func() *TimerElem

// TimerElemPool provides a pool of timer elements.
type TimerElemPool interface {
	// Init initializes the timer element pool.
	Init(alloc TimerElemAlloc)

	// Get gets a timer element from the pool.
	Get() *TimerElem

	// Put returns a timer element to the pool.
	Put(value *TimerElem)
}

// GaugeElemAlloc allocates a new gauge element.
type GaugeElemAlloc func() *GaugeElem

// GaugeElemPool provides a pool of gauge elements.
type GaugeElemPool interface {
	// Init initializes the gauge element pool.
	Init(alloc GaugeElemAlloc)

	// Get gets a gauge element from the pool.
	Get() *GaugeElem

	// Put returns a gauge element to the pool.
	Put(value *GaugeElem)
}

type counterElemPool struct {
	pool pool.ObjectPool
}

// NewCounterElemPool creates a new pool for counter elements.
func NewCounterElemPool(opts pool.ObjectPoolOptions) CounterElemPool {
	return &counterElemPool{pool: pool.NewObjectPool(opts)}
}

func (p *counterElemPool) Init(alloc CounterElemAlloc) {
	p.pool.Init(func() interface{} {
		return alloc()
	})
}

func (p *counterElemPool) Get() *CounterElem {
	return p.pool.Get().(*CounterElem)
}

func (p *counterElemPool) Put(value *CounterElem) {
	p.pool.Put(value)
}

type timerElemPool struct {
	pool pool.ObjectPool
}

// NewTimerElemPool creates a new pool for timer elements.
func NewTimerElemPool(opts pool.ObjectPoolOptions) TimerElemPool {
	return &timerElemPool{pool: pool.NewObjectPool(opts)}
}

func (p *timerElemPool) Init(alloc TimerElemAlloc) {
	p.pool.Init(func() interface{} {
		return alloc()
	})
}

func (p *timerElemPool) Get() *TimerElem {
	return p.pool.Get().(*TimerElem)
}

func (p *timerElemPool) Put(value *TimerElem) {
	p.pool.Put(value)
}

type gaugeElemPool struct {
	pool pool.ObjectPool
}

// NewGaugeElemPool creates a new pool for gauge elements.
func NewGaugeElemPool(opts pool.ObjectPoolOptions) GaugeElemPool {
	return &gaugeElemPool{pool: pool.NewObjectPool(opts)}
}

func (p *gaugeElemPool) Init(alloc GaugeElemAlloc) {
	p.pool.Init(func() interface{} {
		return alloc()
	})
}

func (p *gaugeElemPool) Get() *GaugeElem {
	return p.pool.Get().(*GaugeElem)
}

func (p *gaugeElemPool) Put(value *GaugeElem) {
	p.pool.Put(value)
}
