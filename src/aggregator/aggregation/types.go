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

package aggregation

// CounterPool provides a pool of counters
type CounterPool interface {
	// Init initializes the counter pool
	Init()

	// Get gets a counter from the pool
	Get() *Counter

	// Put returns a counter to the pool
	Put(value *Counter)
}

// TimerAlloc allocates a new timer
type TimerAlloc func() *Timer

// TimerPool provides a pool of timers
type TimerPool interface {
	// Init initializes the timer pool
	Init(alloc TimerAlloc)

	// Get gets a timer from the pool
	Get() *Timer

	// Put returns a timer to the pool
	Put(value *Timer)
}

// GaugePool provides a pool of gauges
type GaugePool interface {
	// Init initializes the gauge pool
	Init()

	// Get gets a gauge from the pool
	Get() *Gauge

	// Put returns a gauge to the pool
	Put(value *Gauge)
}
