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
	"sync"
	"time"

	"github.com/m3db/m3aggregator/aggregation/quantile/cm"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
)

// CounterElemAlloc allocates a new counter element
type CounterElemAlloc func() *CounterElem

// CounterElemPool provides a pool of counter elements
type CounterElemPool interface {
	// Init initializes the counter element pool
	Init(alloc CounterElemAlloc)

	// Get gets a counter element from the pool
	Get() *CounterElem

	// Put returns a counter element to the pool
	Put(value *CounterElem)
}

// TimerElemAlloc allocates a new timer element
type TimerElemAlloc func() *TimerElem

// TimerElemPool provides a pool of timer elements
type TimerElemPool interface {
	// Init initializes the timer element pool
	Init(alloc TimerElemAlloc)

	// Get gets a timer element from the pool
	Get() *TimerElem

	// Put returns a timer element to the pool
	Put(value *TimerElem)
}

// GaugeElemAlloc allocates a new gauge element
type GaugeElemAlloc func() *GaugeElem

// GaugeElemPool provides a pool of gauge elements
type GaugeElemPool interface {
	// Init initializes the gauge element pool
	Init(alloc GaugeElemAlloc)

	// Get gets a gauge element from the pool
	Get() *GaugeElem

	// Put returns a gauge element to the pool
	Put(value *GaugeElem)
}

// EntryAlloc allocates a new entry
type EntryAlloc func() *Entry

// EntryPool provides a pool of entries
type EntryPool interface {
	// Init initializes the entry pool
	Init(alloc EntryAlloc)

	// Get gets a entry from the pool
	Get() *Entry

	// Put returns a entry to the pool
	Put(value *Entry)
}

// Aggregator aggregates different types of metrics
type Aggregator interface {
	// AddMetricWithPolicies adds a metric with policies for aggregation
	AddMetricWithPolicies(mu unaggregated.MetricUnion, policies policy.VersionedPolicies) error

	// Close closes the aggregator
	Close()
}

// QuantileSuffixFn returns the byte-slice suffix for a quantile value
type QuantileSuffixFn func(quantile float64) []byte

// FlushFn flushes a buffered encoder
type FlushFn func(buffer msgpack.BufferedEncoder) error

// Options provide a set of base and derived options for the aggregator
type Options interface {
	/// Read-write base options

	// SetMetricPrefix sets the common prefix for all metric types
	SetMetricPrefix(value []byte) Options

	// MetricPrefix returns the common prefix for all metric types
	MetricPrefix() []byte

	// SetCounterPrefix sets the prefix for counters
	SetCounterPrefix(value []byte) Options

	// CounterPrefix returns the prefix for counters
	CounterPrefix() []byte

	// SetTimerPrefix sets the prefix for timers
	SetTimerPrefix(value []byte) Options

	// TimerPrefix returns the prefix for timers
	TimerPrefix() []byte

	// SetTimerSumSuffix sets the sum suffix for timers
	SetTimerSumSuffix(value []byte) Options

	// TimerSumSuffix returns the sum suffix for timers
	TimerSumSuffix() []byte

	// SetTimerSumSqSuffix sets the sum square suffix for timers
	SetTimerSumSqSuffix(value []byte) Options

	// TimerSumSqSuffix returns the sum square suffix for timers
	TimerSumSqSuffix() []byte

	// SetTimerMeanSuffix sets the mean suffix for timers
	SetTimerMeanSuffix(value []byte) Options

	// TimerMeanSuffix returns the mean suffix for timers
	TimerMeanSuffix() []byte

	// SetTimerLowerSuffix sets the lower suffix for timers
	SetTimerLowerSuffix(value []byte) Options

	// TimerLowerSuffix returns the lower suffix for timers
	TimerLowerSuffix() []byte

	// SetTimerUpperSuffix sets the upper suffix for timers
	SetTimerUpperSuffix(value []byte) Options

	// TimerUpperSuffix returns the upper suffix for timers
	TimerUpperSuffix() []byte

	// SetTimerCountSuffix sets the count suffix for timers
	SetTimerCountSuffix(value []byte) Options

	// TimerCountSuffix returns the count suffix for timers
	TimerCountSuffix() []byte

	// SetTimerStdevSuffix sets the standard deviation suffix for timers
	SetTimerStdevSuffix(value []byte) Options

	// TimerStdevSuffix returns the standard deviation suffix for timers
	TimerStdevSuffix() []byte

	// SetTimerMedianSuffix sets the median suffix for timers
	SetTimerMedianSuffix(value []byte) Options

	// TimerMedianSuffix returns the median suffix for timers
	TimerMedianSuffix() []byte

	// SetTimerQuantileSuffixFn sets the quantile suffix function for timers
	SetTimerQuantileSuffixFn(value QuantileSuffixFn) Options

	// TimerQuantileSuffixFn returns the quantile suffix function for timers
	TimerQuantileSuffixFn() QuantileSuffixFn

	// SetGaugePrefix sets the prefix for gauges
	SetGaugePrefix(value []byte) Options

	// GaugePrefix returns the prefix for gauges
	GaugePrefix() []byte

	// SetClockOptions sets the clock options
	SetClockOptions(value clock.Options) Options

	// ClockOptions returns the clock options
	ClockOptions() clock.Options

	// SetInstrumentOptions sets the instrument options
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrument options
	InstrumentOptions() instrument.Options

	// SetStreamOptions sets the stream options
	SetStreamOptions(value cm.Options) Options

	// StreamOptions returns the stream options
	StreamOptions() cm.Options

	// SetTimeLock sets the time lock
	SetTimeLock(value *sync.RWMutex) Options

	// TimeLock returns the time lock
	TimeLock() *sync.RWMutex

	// SetMinFlushInterval sets the minimum flush interval
	SetMinFlushInterval(value time.Duration) Options

	// MinFlushInterval returns the minimum flush interval
	MinFlushInterval() time.Duration

	// SetMaxFlushSize sets the maximum buffer size to trigger a flush
	SetMaxFlushSize(value int) Options

	// MaxFlushSize returns the maximum buffer size to trigger a flush
	MaxFlushSize() int

	// SetFlushFn sets the function that flushes buffered encoders
	SetFlushFn(value FlushFn) Options

	// FlushFn returns the function that flushes buffered encoders
	FlushFn() FlushFn

	// SetEntryTTL sets the ttl for expiring stale entries
	SetEntryTTL(value time.Duration) Options

	// EntryTTL returns the ttl for expiring stale entries
	EntryTTL() time.Duration

	// SetEntryCheckInterval sets the interval for checking expired entries
	SetEntryCheckInterval(value time.Duration) Options

	// EntryCheckInterval returns the interval for checking expired entries
	EntryCheckInterval() time.Duration

	// SetEntryCheckBatchPercent sets the batch percentage for checking expired entries
	SetEntryCheckBatchPercent(value float64) Options

	// EntryCheckBatchPercent returns the batch percentage for checking expired entries
	EntryCheckBatchPercent() float64

	// SetEntryPool sets the entry pool
	SetEntryPool(value EntryPool) Options

	// EntryPool returns the entry pool
	EntryPool() EntryPool

	// SetCounterElemPool sets the counter element pool
	SetCounterElemPool(value CounterElemPool) Options

	// CounterElemPool returns the counter element pool
	CounterElemPool() CounterElemPool

	// SetTimerElemPool sets the timer element pool
	SetTimerElemPool(value TimerElemPool) Options

	// TimerElemPool returns the timer element pool
	TimerElemPool() TimerElemPool

	// SetGaugeElemPool sets the gauge element pool
	SetGaugeElemPool(value GaugeElemPool) Options

	// GaugeElemPool returns the gauge element pool
	GaugeElemPool() GaugeElemPool

	// SetBufferedEncoderPool sets the buffered encoder pool
	SetBufferedEncoderPool(value msgpack.BufferedEncoderPool) Options

	// BufferedEncoderPool returns the buffered encoder pool
	BufferedEncoderPool() msgpack.BufferedEncoderPool

	/// Read-only derived options

	// FullCounterPrefix returns the full prefix for counters
	FullCounterPrefix() []byte

	// FullTimerPrefix returns the full prefix for timers
	FullTimerPrefix() []byte

	// FullGaugePrefix returns the full prefix for gauges
	FullGaugePrefix() []byte

	// TimerQuantiles returns the quantiles for timers
	TimerQuantiles() []float64

	// TimerQuantileSuffixes returns the quantile suffixes for timers
	TimerQuantileSuffixes() [][]byte
}
