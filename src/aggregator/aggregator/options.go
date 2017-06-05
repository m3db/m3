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

	"github.com/m3db/m3aggregator/aggregation/quantile/cm"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/time"

	"github.com/spaolacci/murmur3"
)

const (
	minQuantile = 0.0
	maxQuantile = 1.0
)

var (
	defaultMetricPrefix              = []byte("stats.")
	defaultCounterPrefix             = []byte("counts.")
	defaultTimerPrefix               = []byte("timers.")
	defaultTimerSumSuffix            = []byte(".sum")
	defaultTimerSumSqSuffix          = []byte(".sum_sq")
	defaultTimerMeanSuffix           = []byte(".mean")
	defaultTimerLowerSuffix          = []byte(".lower")
	defaultTimerUpperSuffix          = []byte(".upper")
	defaultTimerCountSuffix          = []byte(".count")
	defaultTimerStdevSuffix          = []byte(".stdev")
	defaultTimerMedianSuffix         = []byte(".median")
	defaultGaugePrefix               = []byte("gauges.")
	defaultInstanceID                = "localhost"
	defaultMinFlushInterval          = 5 * time.Second
	defaultMaxFlushSize              = 1440
	defaultEntryTTL                  = 24 * time.Hour
	defaultEntryCheckInterval        = time.Hour
	defaultEntryCheckBatchPercent    = 0.01
	defaultMaxTimerBatchSizePerWrite = 0
	defaultDefaultPolicies           = []policy.Policy{
		policy.NewPolicy(10*time.Second, xtime.Second, 2*24*time.Hour),
		policy.NewPolicy(time.Minute, xtime.Minute, 40*24*time.Hour),
	}
	defaultTimerQuantiles = []float64{0.5, 0.95, 0.99}

	errInvalidQuantiles = fmt.Errorf("quantiles must be nonempty and between %f and %f", minQuantile, maxQuantile)
)

type options struct {
	// Base options.
	metricPrefix              []byte
	counterPrefix             []byte
	timerPrefix               []byte
	timerSumSuffix            []byte
	timerSumSqSuffix          []byte
	timerMeanSuffix           []byte
	timerLowerSuffix          []byte
	timerUpperSuffix          []byte
	timerCountSuffix          []byte
	timerStdevSuffix          []byte
	timerMedianSuffix         []byte
	timerQuantiles            []float64
	timerQuantileSuffixFn     QuantileSuffixFn
	gaugePrefix               []byte
	timeLock                  *sync.RWMutex
	clockOpts                 clock.Options
	instrumentOpts            instrument.Options
	streamOpts                cm.Options
	placementWatcherOpts      services.StagedPlacementWatcherOptions
	instanceID                string
	shardFn                   ShardFn
	flushManager              FlushManager
	minFlushInterval          time.Duration
	maxFlushSize              int
	flushHandler              Handler
	entryTTL                  time.Duration
	entryCheckInterval        time.Duration
	entryCheckBatchPercent    float64
	maxTimerBatchSizePerWrite int
	defaultPolicies           []policy.Policy
	entryPool                 EntryPool
	counterElemPool           CounterElemPool
	timerElemPool             TimerElemPool
	gaugeElemPool             GaugeElemPool
	bufferedEncoderPool       msgpack.BufferedEncoderPool

	// Derived options.
	fullCounterPrefix     []byte
	fullTimerPrefix       []byte
	fullGaugePrefix       []byte
	timerQuantileSuffixes [][]byte
}

// NewOptions create a new set of options.
func NewOptions() Options {
	o := &options{
		metricPrefix:              defaultMetricPrefix,
		counterPrefix:             defaultCounterPrefix,
		timerPrefix:               defaultTimerPrefix,
		timerSumSuffix:            defaultTimerSumSuffix,
		timerSumSqSuffix:          defaultTimerSumSqSuffix,
		timerMeanSuffix:           defaultTimerMeanSuffix,
		timerLowerSuffix:          defaultTimerLowerSuffix,
		timerUpperSuffix:          defaultTimerUpperSuffix,
		timerCountSuffix:          defaultTimerCountSuffix,
		timerStdevSuffix:          defaultTimerStdevSuffix,
		timerMedianSuffix:         defaultTimerMedianSuffix,
		timerQuantiles:            defaultTimerQuantiles,
		timerQuantileSuffixFn:     defaultTimerQuantileSuffixFn,
		gaugePrefix:               defaultGaugePrefix,
		timeLock:                  &sync.RWMutex{},
		clockOpts:                 clock.NewOptions(),
		instrumentOpts:            instrument.NewOptions(),
		streamOpts:                cm.NewOptions(),
		placementWatcherOpts:      placement.NewStagedPlacementWatcherOptions(),
		instanceID:                defaultInstanceID,
		shardFn:                   defaultShardFn,
		minFlushInterval:          defaultMinFlushInterval,
		maxFlushSize:              defaultMaxFlushSize,
		entryTTL:                  defaultEntryTTL,
		entryCheckInterval:        defaultEntryCheckInterval,
		entryCheckBatchPercent:    defaultEntryCheckBatchPercent,
		maxTimerBatchSizePerWrite: defaultMaxTimerBatchSizePerWrite,
		defaultPolicies:           defaultDefaultPolicies,
	}

	// Initialize pools.
	o.initPools()

	// Compute derived options.
	o.computeAllDerived()

	return o
}

func (o *options) Validate() error {
	if len(o.timerQuantiles) == 0 {
		return errInvalidQuantiles
	}
	for _, quantile := range o.timerQuantiles {
		if quantile <= minQuantile || quantile >= maxQuantile {
			return errInvalidQuantiles
		}
	}
	return nil
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

func (o *options) SetTimerQuantiles(quantiles []float64) Options {
	opts := *o
	opts.timerQuantiles = quantiles
	opts.computeTimerQuantilesSuffixes()
	return &opts
}

func (o *options) TimerQuantiles() []float64 {
	return o.timerQuantiles
}

func (o *options) SetTimerQuantileSuffixFn(value QuantileSuffixFn) Options {
	opts := *o
	opts.timerQuantileSuffixFn = value
	opts.computeTimerQuantilesSuffixes()
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

func (o *options) SetTimeLock(value *sync.RWMutex) Options {
	opts := *o
	opts.timeLock = value
	return &opts
}

func (o *options) TimeLock() *sync.RWMutex {
	return o.timeLock
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
	return &opts
}

func (o *options) StreamOptions() cm.Options {
	return o.streamOpts
}

func (o *options) SetStagedPlacementWatcherOptions(value services.StagedPlacementWatcherOptions) Options {
	opts := *o
	opts.placementWatcherOpts = value
	return &opts
}

func (o *options) StagedPlacementWatcherOptions() services.StagedPlacementWatcherOptions {
	return o.placementWatcherOpts
}

func (o *options) SetInstanceID(value string) Options {
	opts := *o
	opts.instanceID = value
	return &opts
}

func (o *options) InstanceID() string {
	return o.instanceID
}

func (o *options) SetShardFn(value ShardFn) Options {
	opts := *o
	opts.shardFn = value
	return &opts
}

func (o *options) ShardFn() ShardFn {
	return o.shardFn
}

func (o *options) SetFlushManager(value FlushManager) Options {
	opts := *o
	opts.flushManager = value
	return &opts
}

func (o *options) FlushManager() FlushManager {
	return o.flushManager
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

func (o *options) SetFlushHandler(value Handler) Options {
	opts := *o
	opts.flushHandler = value
	return &opts
}

func (o *options) FlushHandler() Handler {
	return o.flushHandler
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

func (o *options) SetEntryCheckBatchPercent(value float64) Options {
	opts := *o
	opts.entryCheckBatchPercent = value
	return &opts
}

func (o *options) EntryCheckBatchPercent() float64 {
	return o.entryCheckBatchPercent
}

func (o *options) SetMaxTimerBatchSizePerWrite(value int) Options {
	opts := *o
	opts.maxTimerBatchSizePerWrite = value
	return &opts
}

func (o *options) MaxTimerBatchSizePerWrite() int {
	return o.maxTimerBatchSizePerWrite
}

func (o *options) SetDefaultPolicies(value []policy.Policy) Options {
	opts := *o
	opts.defaultPolicies = value
	return &opts
}

func (o *options) DefaultPolicies() []policy.Policy {
	return o.defaultPolicies
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

func (o *options) TimerQuantileSuffixes() [][]byte {
	return o.timerQuantileSuffixes
}

func (o *options) initPools() {
	o.entryPool = NewEntryPool(nil)
	o.entryPool.Init(func() *Entry {
		return NewEntry(nil, o)
	})

	var emptyPolicy policy.Policy
	o.counterElemPool = NewCounterElemPool(nil)
	o.counterElemPool.Init(func() *CounterElem {
		return NewCounterElem(nil, emptyPolicy, o)
	})

	o.timerElemPool = NewTimerElemPool(nil)
	o.timerElemPool.Init(func() *TimerElem {
		return NewTimerElem(nil, emptyPolicy, o)
	})

	o.gaugeElemPool = NewGaugeElemPool(nil)
	o.gaugeElemPool.Init(func() *GaugeElem {
		return NewGaugeElem(nil, emptyPolicy, o)
	})

	o.bufferedEncoderPool = msgpack.NewBufferedEncoderPool(nil)
	o.bufferedEncoderPool.Init(func() msgpack.BufferedEncoder {
		return msgpack.NewPooledBufferedEncoder(o.bufferedEncoderPool)
	})
}

func (o *options) computeAllDerived() {
	o.computeFullPrefixes()
	o.computeTimerQuantilesSuffixes()
}

func (o *options) computeFullPrefixes() {
	o.computeFullCounterPrefix()
	o.computeFullTimerPrefix()
	o.computeFullGaugePrefix()
}

func (o *options) computeTimerQuantilesSuffixes() {
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

// By default we use e.g. ".p50", ".p95", ".p99" for the 50th/95th/99th percentile.
func defaultTimerQuantileSuffixFn(quantile float64) []byte {
	return []byte(fmt.Sprintf(".p%0.0f", quantile*100))
}

func defaultShardFn(id []byte, numShards int) uint32 {
	return murmur3.Sum32(id) % uint32(numShards)
}
