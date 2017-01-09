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
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3aggregator/aggregation"
	"github.com/m3db/m3aggregator/aggregation/quantile/cm"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
)

var (
	defaultMetricPrefix       = []byte("stats.")
	defaultCounterPrefix      = []byte("counts.")
	defaultTimerPrefix        = []byte("timers.")
	defaultTimerSumSuffix     = []byte(".sum")
	defaultTimerSumSqSuffix   = []byte(".sum_sq")
	defaultTimerMeanSuffix    = []byte(".mean")
	defaultTimerLowerSuffix   = []byte(".lower")
	defaultTimerUpperSuffix   = []byte(".upper")
	defaultTimerCountSuffix   = []byte(".count")
	defaultTimerStdevSuffix   = []byte(".stdev")
	defaultTimerMedianSuffix  = []byte(".median")
	defaultGaugePrefix        = []byte("gauges.")
	defaultMinFlushInterval   = 5 * time.Second
	defaultMaxFlushSize       = 1440
	defaultEntryTTL           = 24 * time.Hour
	defaultEntryCheckInterval = time.Hour
)

// By default we use e.g. ".p50", ".p95", ".p99" for the 50th/95th/99th percentile
func defaultTimerQuantileSuffixFn(quantile float64) []byte {
	return []byte(fmt.Sprintf(".p%0.0f", quantile*100))
}

// By default we print out the buffer size
func defaultFlushFn(buffer msgpack.BufferedEncoder) error {
	fmt.Printf("buffer size=%d\n", len(buffer.Bytes()))
	return nil
}

type options struct {
	// Base options
	metricPrefix          []byte
	counterPrefix         []byte
	timerPrefix           []byte
	timerSumSuffix        []byte
	timerSumSqSuffix      []byte
	timerMeanSuffix       []byte
	timerLowerSuffix      []byte
	timerUpperSuffix      []byte
	timerCountSuffix      []byte
	timerStdevSuffix      []byte
	timerMedianSuffix     []byte
	timerQuantileSuffixFn QuantileSuffixFn
	gaugePrefix           []byte
	clockOpts             clock.Options
	instrumentOpts        instrument.Options
	streamOpts            cm.Options
	timeLock              *sync.RWMutex
	minFlushInterval      time.Duration
	maxFlushSize          int
	flushFn               FlushFn
	entryTTL              time.Duration
	entryCheckInterval    time.Duration
	entryPool             EntryPool
	counterElemPool       CounterElemPool
	timerElemPool         TimerElemPool
	gaugeElemPool         GaugeElemPool
	counterPool           aggregation.CounterPool
	timerPool             aggregation.TimerPool
	gaugePool             aggregation.GaugePool
	bufferedEncoderPool   msgpack.BufferedEncoderPool

	// Derived options
	fullCounterPrefix     []byte
	fullTimerPrefix       []byte
	fullGaugePrefix       []byte
	timerQuantiles        []float64
	timerQuantileSuffixes [][]byte
}

// NewOptions create a new set of options
func NewOptions() Options {
	o := &options{
		metricPrefix:          defaultMetricPrefix,
		counterPrefix:         defaultCounterPrefix,
		timerPrefix:           defaultTimerPrefix,
		timerSumSuffix:        defaultTimerSumSuffix,
		timerSumSqSuffix:      defaultTimerSumSqSuffix,
		timerMeanSuffix:       defaultTimerMeanSuffix,
		timerLowerSuffix:      defaultTimerLowerSuffix,
		timerUpperSuffix:      defaultTimerUpperSuffix,
		timerCountSuffix:      defaultTimerCountSuffix,
		timerStdevSuffix:      defaultTimerStdevSuffix,
		timerMedianSuffix:     defaultTimerMedianSuffix,
		timerQuantileSuffixFn: defaultTimerQuantileSuffixFn,
		gaugePrefix:           defaultGaugePrefix,
		clockOpts:             clock.NewOptions(),
		instrumentOpts:        instrument.NewOptions(),
		streamOpts:            cm.NewOptions(),
		timeLock:              &sync.RWMutex{},
		minFlushInterval:      defaultMinFlushInterval,
		maxFlushSize:          defaultMaxFlushSize,
		flushFn:               defaultFlushFn,
		entryTTL:              defaultEntryTTL,
		entryCheckInterval:    defaultEntryCheckInterval,
	}

	// Initialize pools
	o.initPools()

	// Compute derived options
	o.computeAllDerived()

	return o
}

func (o *options) SetMetricPrefix(value []byte) Options {
	opts := *o
	opts.metricPrefix = value
	opts.computeFullPrefixes()
	return &opts
}

func (o *options) MetricPrefix() []byte {
	return o.metricPrefix
}

func (o *options) SetCounterPrefix(value []byte) Options {
	opts := *o
	opts.counterPrefix = value
	opts.computeFullCounterPrefix()
	return &opts
}

func (o *options) CounterPrefix() []byte {
	return o.counterPrefix
}

func (o *options) SetTimerPrefix(value []byte) Options {
	opts := *o
	opts.timerPrefix = value
	opts.computeFullTimerPrefix()
	return &opts
}

func (o *options) TimerPrefix() []byte {
	return o.timerPrefix
}

func (o *options) SetTimerSumSuffix(value []byte) Options {
	opts := *o
	opts.timerSumSuffix = value
	return &opts
}

func (o *options) TimerSumSuffix() []byte {
	return o.timerSumSuffix
}

func (o *options) SetTimerSumSqSuffix(value []byte) Options {
	opts := *o
	opts.timerSumSqSuffix = value
	return &opts
}

func (o *options) TimerSumSqSuffix() []byte {
	return o.timerSumSqSuffix
}

func (o *options) SetTimerMeanSuffix(value []byte) Options {
	opts := *o
	opts.timerMeanSuffix = value
	return &opts
}

func (o *options) TimerMeanSuffix() []byte {
	return o.timerMeanSuffix
}

func (o *options) SetTimerLowerSuffix(value []byte) Options {
	opts := *o
	opts.timerLowerSuffix = value
	return &opts
}

func (o *options) TimerLowerSuffix() []byte {
	return o.timerLowerSuffix
}

func (o *options) SetTimerUpperSuffix(value []byte) Options {
	opts := *o
	opts.timerUpperSuffix = value
	return &opts
}

func (o *options) TimerUpperSuffix() []byte {
	return o.timerUpperSuffix
}

func (o *options) SetTimerCountSuffix(value []byte) Options {
	opts := *o
	opts.timerCountSuffix = value
	return &opts
}

func (o *options) TimerCountSuffix() []byte {
	return o.timerCountSuffix
}

func (o *options) SetTimerStdevSuffix(value []byte) Options {
	opts := *o
	opts.timerStdevSuffix = value
	return &opts
}

func (o *options) TimerStdevSuffix() []byte {
	return o.timerStdevSuffix
}

func (o *options) SetTimerMedianSuffix(value []byte) Options {
	opts := *o
	opts.timerMedianSuffix = value
	return &opts
}

func (o *options) TimerMedianSuffix() []byte {
	return o.timerMedianSuffix
}

func (o *options) SetTimerQuantileSuffixFn(value QuantileSuffixFn) Options {
	opts := *o
	opts.timerQuantileSuffixFn = value
	opts.computeTimerQuantilesAndSuffixes()
	return &opts
}

func (o *options) TimerQuantileSuffixFn() QuantileSuffixFn {
	return o.timerQuantileSuffixFn
}

func (o *options) SetGaugePrefix(value []byte) Options {
	opts := *o
	opts.gaugePrefix = value
	opts.computeFullGaugePrefix()
	return &opts
}

func (o *options) GaugePrefix() []byte {
	return o.gaugePrefix
}

func (o *options) SetClockOptions(value clock.Options) Options {
	opts := *o
	opts.clockOpts = value
	return &opts
}

func (o *options) ClockOptions() clock.Options {
	return o.clockOpts
}

func (o *options) SetInstrumentOptions(value instrument.Options) Options {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *options) SetStreamOptions(value cm.Options) Options {
	opts := *o
	opts.streamOpts = value
	opts.computeTimerQuantilesAndSuffixes()
	return &opts
}

func (o *options) StreamOptions() cm.Options {
	return o.streamOpts
}

func (o *options) SetTimeLock(value *sync.RWMutex) Options {
	opts := *o
	opts.timeLock = value
	return &opts
}

func (o *options) TimeLock() *sync.RWMutex {
	return o.timeLock
}

func (o *options) SetMinFlushInterval(value time.Duration) Options {
	opts := *o
	opts.minFlushInterval = value
	return &opts
}

func (o *options) MinFlushInterval() time.Duration {
	return o.minFlushInterval
}

func (o *options) SetMaxFlushSize(value int) Options {
	opts := *o
	opts.maxFlushSize = value
	return &opts
}

func (o *options) MaxFlushSize() int {
	return o.maxFlushSize
}

func (o *options) SetFlushFn(value FlushFn) Options {
	opts := *o
	opts.flushFn = value
	return &opts
}

func (o *options) FlushFn() FlushFn {
	return o.flushFn
}

func (o *options) SetEntryTTL(value time.Duration) Options {
	opts := *o
	opts.entryTTL = value
	return &opts
}

func (o *options) EntryTTL() time.Duration {
	return o.entryTTL
}

func (o *options) SetEntryCheckInterval(value time.Duration) Options {
	opts := *o
	opts.entryCheckInterval = value
	return &opts
}

func (o *options) EntryCheckInterval() time.Duration {
	return o.entryCheckInterval
}

func (o *options) SetEntryPool(value EntryPool) Options {
	opts := *o
	opts.entryPool = value
	return &opts
}

func (o *options) EntryPool() EntryPool {
	return o.entryPool
}

func (o *options) SetCounterElemPool(value CounterElemPool) Options {
	opts := *o
	opts.counterElemPool = value
	return &opts
}

func (o *options) CounterElemPool() CounterElemPool {
	return o.counterElemPool
}

func (o *options) SetTimerElemPool(value TimerElemPool) Options {
	opts := *o
	opts.timerElemPool = value
	return &opts
}

func (o *options) TimerElemPool() TimerElemPool {
	return o.timerElemPool
}

func (o *options) SetGaugeElemPool(value GaugeElemPool) Options {
	opts := *o
	opts.gaugeElemPool = value
	return &opts
}

func (o *options) GaugeElemPool() GaugeElemPool {
	return o.gaugeElemPool
}

func (o *options) SetCounterPool(value aggregation.CounterPool) Options {
	opts := *o
	opts.counterPool = value
	return &opts
}

func (o *options) CounterPool() aggregation.CounterPool {
	return o.counterPool
}

func (o *options) SetTimerPool(value aggregation.TimerPool) Options {
	opts := *o
	opts.timerPool = value
	return &opts
}

func (o *options) TimerPool() aggregation.TimerPool {
	return o.timerPool
}

func (o *options) SetGaugePool(value aggregation.GaugePool) Options {
	opts := *o
	opts.gaugePool = value
	return &opts
}

func (o *options) GaugePool() aggregation.GaugePool {
	return o.gaugePool
}

func (o *options) SetBufferedEncoderPool(value msgpack.BufferedEncoderPool) Options {
	opts := *o
	opts.bufferedEncoderPool = value
	return &opts
}

func (o *options) BufferedEncoderPool() msgpack.BufferedEncoderPool {
	return o.bufferedEncoderPool
}

func (o *options) FullCounterPrefix() []byte {
	return o.fullCounterPrefix
}

func (o *options) FullTimerPrefix() []byte {
	return o.fullTimerPrefix
}

func (o *options) FullGaugePrefix() []byte {
	return o.fullGaugePrefix
}

func (o *options) TimerQuantiles() []float64 {
	return o.timerQuantiles
}

func (o *options) TimerQuantileSuffixes() [][]byte {
	return o.timerQuantileSuffixes
}

func (o *options) initPools() {
	entryPool := NewEntryPool(nil)
	entryPool.Init(func() *Entry {
		return NewEntry(nil, o)
	})

	var emptyPolicy policy.Policy
	counterElemPool := NewCounterElemPool(nil)
	counterElemPool.Init(func() *CounterElem {
		return NewCounterElem(nil, emptyPolicy, o)
	})
	timerElemPool := NewTimerElemPool(nil)
	timerElemPool.Init(func() *TimerElem {
		return NewTimerElem(nil, emptyPolicy, o)
	})
	gaugeElemPool := NewGaugeElemPool(nil)
	gaugeElemPool.Init(func() *GaugeElem {
		return NewGaugeElem(nil, emptyPolicy, o)
	})

	counterPool := aggregation.NewCounterPool(nil)
	counterPool.Init()

	timerPool := aggregation.NewTimerPool(nil)
	timerPool.Init(func() *aggregation.Timer {
		return aggregation.NewTimer(o.streamOpts)
	})

	gaugePool := aggregation.NewGaugePool(nil)
	gaugePool.Init()

	encoderPool := msgpack.NewBufferedEncoderPool(nil)
	encoderPool.Init(func() msgpack.BufferedEncoder {
		return msgpack.NewPooledBufferedEncoder(encoderPool)
	})

	o.entryPool = entryPool
	o.counterElemPool = counterElemPool
	o.timerElemPool = timerElemPool
	o.gaugeElemPool = gaugeElemPool
	o.counterPool = counterPool
	o.timerPool = timerPool
	o.gaugePool = gaugePool
	o.bufferedEncoderPool = encoderPool
}

func (o *options) computeAllDerived() {
	o.computeFullPrefixes()
	o.computeTimerQuantilesAndSuffixes()
}

func (o *options) computeFullPrefixes() {
	o.computeFullCounterPrefix()
	o.computeFullTimerPrefix()
	o.computeFullGaugePrefix()
}

func (o *options) computeTimerQuantilesAndSuffixes() {
	o.timerQuantiles = o.streamOpts.Quantiles()
	suffixes := make([][]byte, len(o.timerQuantiles))
	for i, q := range o.timerQuantiles {
		suffixes[i] = o.timerQuantileSuffixFn(q)
	}
	o.timerQuantileSuffixes = suffixes
}

func (o *options) computeFullCounterPrefix() {
	fullCounterPrefix := make([]byte, len(o.metricPrefix)+len(o.counterPrefix))
	n := copy(fullCounterPrefix, o.metricPrefix)
	copy(fullCounterPrefix[n:], o.counterPrefix)
	o.fullCounterPrefix = fullCounterPrefix
}

func (o *options) computeFullTimerPrefix() {
	fullTimerPrefix := make([]byte, len(o.metricPrefix)+len(o.timerPrefix))
	n := copy(fullTimerPrefix, o.metricPrefix)
	copy(fullTimerPrefix[n:], o.timerPrefix)
	o.fullTimerPrefix = fullTimerPrefix
}

func (o *options) computeFullGaugePrefix() {
	fullGaugePrefix := make([]byte, len(o.metricPrefix)+len(o.gaugePrefix))
	n := copy(fullGaugePrefix, o.metricPrefix)
	copy(fullGaugePrefix[n:], o.gaugePrefix)
	o.fullGaugePrefix = fullGaugePrefix
}
