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
	"math"
	"time"

	"github.com/uber-go/tally"
)

const bufferLimit = time.Minute

// Buffer is a fixed-size buffer for servicing resends of min and max
// aggregations.
type Buffer interface {
	// Reset resets the resend buffer to the given buffer type.
	Reset(isMaxBuffer bool)
	// Insert inserts a value into the buffer.
	//
	//  For a max buffer of size `k`, this will capture the `k` largest elements
	//  inserted into the buffer.
	//
	//  For a min buffer of size `k`, this will capture the `k` smallest elements
	//  inserted into the buffer.
	Insert(val float64)

	// Value returns the value for the buffer.
	//
	//  For a max buffer this will return the max value seen.
	//
	//  For a min buffer this will return the min value seen.
	Value() float64

	// Update updates a given value in the buffer, if eligible,
	// Regardless of buffer type, if `prevVal` currently appears in the buffer,
	// it is replaced by `newVal`.
	//
	//  For a max buffer, if a currently captured value is smaller than `newVal`,
	//  `prevVal` does not exist in the buffer, and the buffer is at capacity,
	//  the smallest currently captured value is replaced by newVal.
	//
	//  For a min buffer, if a currently captured value is larger than `newVal`,
	//  `prevVal` does not exist in the buffer, and the buffer is at capacity,
	//  the largest currently captured value is replaced by newVal.
	Update(prevVal float64, newVal float64)

	// Close closes the buffer.
	Close()
}

// metrics are metrics for resend buffers.
type metrics struct {
	// count is the total count of all created resend buffers.
	count tally.Counter
	// inserts is the total number of fresh inserts across all resend buffers.
	inserts tally.Counter
	// updates is he total number of resends across all resend buffers.
	updates tally.Counter

	// overWarningThreshold increments for every update passes the
	// resend buffer's warning threshold.
	overWarningThreshold tally.Counter
	// overCriticalThreshold increments for every update passes the
	// resend buffer's critical threshold.
	overCriticalThreshold tally.Counter
}

type buffer struct {
	// these vars are reset on Reset
	open             bool
	updatesPersisted float64
	comparisonFn     comparisonFn
	activeMetrics    *metrics
	list             []float64

	// these vars are set on initialization
	warnThreshold     float64
	criticalThreshold float64
	minMetrics        *metrics
	maxMetrics        *metrics
	pool              BufferPool
}

func newBufferMetrics(size int, scope tally.Scope) *metrics {
	var (
		// Start reporting loop for reporting the buffer size limit.
		bufferLimitGauge = scope.Gauge("buffer_limit")
		timer            = time.NewTimer(bufferLimit)
		bufferLimit      = float64(size)
	)

	bufferLimitGauge.Update(bufferLimit)

	go func() {
		for {
			<-timer.C
			bufferLimitGauge.Update(bufferLimit)
		}
	}()

	thresholWithLevel := func(level string) tally.Counter {
		return scope.Tagged(map[string]string{"level": level}).Counter("over_threshold")
	}

	return &metrics{
		count:   scope.Counter("count"),
		inserts: scope.Counter("inserted"),
		updates: scope.Counter("updated"),

		overWarningThreshold:  thresholWithLevel("warn"),
		overCriticalThreshold: thresholWithLevel("critical"),
	}
}

type comparisonFn func(a, b float64) bool

func min(a, b float64) bool {
	if math.IsNaN(a) {
		return false
	}
	if math.IsNaN(b) {
		return true
	}
	return a < b
}

func max(a, b float64) bool {
	if math.IsNaN(a) {
		return false
	}
	if math.IsNaN(b) {
		return true
	}
	return a > b
}

type bufferOpts struct {
	size              int
	minMetrics        *metrics
	maxMetrics        *metrics
	pool              BufferPool
	warnThreshold     float64
	criticalThreshold float64
}

func newBuffer(opts bufferOpts) Buffer {
	return &buffer{
		pool:              opts.pool,
		list:              make([]float64, 0, opts.size),
		minMetrics:        opts.minMetrics,
		maxMetrics:        opts.maxMetrics,
		warnThreshold:     opts.warnThreshold,
		criticalThreshold: opts.criticalThreshold,
	}
}

func (b *buffer) Insert(val float64) {
	if !b.open {
		return
	}

	b.activeMetrics.inserts.Inc(1)

	// if list not full yet, fill it up.
	if len(b.list) < cap(b.list) {
		b.list = append(b.list, val)
		return
	}

	toUpdateVal := b.list[0]
	toUpdateIdx := 0

	for idx, listVal := range b.list {
		// find the best candidate to replace with the new value
		if b.comparisonFn(toUpdateVal, listVal) {
			toUpdateVal = listVal
			toUpdateIdx = idx
		}
	}

	// if the current value is a better candidate than the value to replace,
	// update it.
	if b.comparisonFn(val, toUpdateVal) {
		b.list[toUpdateIdx] = val
	}
}

func (b *buffer) Value() float64 {
	if len(b.list) == 0 {
		return math.NaN()
	}

	toReturn := b.list[0]
	for _, val := range b.list[1:] {
		if b.comparisonFn(val, toReturn) {
			toReturn = val
		}
	}

	return toReturn
}

func (b *buffer) Update(prevVal float64, newVal float64) {
	if !b.open {
		return
	}

	if len(b.list) == 0 {
		// received a resend before recording any values, which is an invalid case.
		return
	}

	b.activeMetrics.updates.Inc(1)

	toUpdateVal := b.list[0]
	toUpdateIdx := 0

	for idx, listVal := range b.list {
		// found the previously recorded value in the list. Update and shortcircuit.
		if listVal == prevVal {
			b.list[idx] = newVal
			b.persistUpdate()
			return
		}

		if b.comparisonFn(toUpdateVal, listVal) {
			toUpdateVal = listVal
			toUpdateIdx = idx
		}
	}

	// newVal is a better candidate than an existing value in the buffer.
	// Replace the least viable candidate in the buffer value with the new value.
	// This is only possible if the buffer is full; otherwise we are trying
	// to update a value which SHOULD be in the list, which is an invalid case.
	if len(b.list) == cap(b.list) && b.comparisonFn(newVal, toUpdateVal) {
		b.list[toUpdateIdx] = newVal
		b.persistUpdate()
	}
}

// persistUpdate persists the update and signals on any tripped thresholds.
func (b *buffer) persistUpdate() {
	b.updatesPersisted++
	if b.warnThreshold > 0 && b.updatesPersisted > b.warnThreshold {
		b.activeMetrics.overWarningThreshold.Inc(1)
	}
	if b.criticalThreshold > 0 && b.updatesPersisted > b.criticalThreshold {
		b.activeMetrics.overCriticalThreshold.Inc(1)
	}
}

func (b *buffer) Reset(isMaxBuffer bool) {
	if b.open {
		return
	}

	b.open = true
	b.list = b.list[:0]
	b.updatesPersisted = 0

	if isMaxBuffer {
		b.comparisonFn = max
		b.activeMetrics = b.maxMetrics
	} else {
		b.comparisonFn = min
		b.activeMetrics = b.minMetrics
	}

	b.activeMetrics.count.Inc(1)
}

func (b *buffer) Close() {
	if !b.open {
		return
	}

	b.open = false
	b.list = b.list[:0]
	b.updatesPersisted = 0
	b.pool.put(b)
}
