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
	"strconv"
	"sync"
	"time"

	"github.com/m3db/m3aggregator/aggregation/quantile/cm"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"

	"github.com/spaolacci/murmur3"
)

var (
	defaultMetricPrefix              = []byte("stats.")
	defaultCounterPrefix             = []byte("counts.")
	defaultTimerPrefix               = []byte("timers.")
	defaultGaugePrefix               = []byte("gauges.")
	defaultAggregationLastSuffix     = []byte(".last")
	defaultAggregationSumSuffix      = []byte(".sum")
	defaultAggregationSumSqSuffix    = []byte(".sum_sq")
	defaultAggregationMeanSuffix     = []byte(".mean")
	defaultAggregationMinSuffix      = []byte(".min")
	defaultAggregationMaxSuffix      = []byte(".max")
	defaultAggregationCountSuffix    = []byte(".count")
	defaultAggregationStdevSuffix    = []byte(".stdev")
	defaultAggregationMedianSuffix   = []byte(".median")
	defaultMinFlushInterval          = 5 * time.Second
	defaultMaxFlushSize              = 1440
	defaultEntryTTL                  = 24 * time.Hour
	defaultEntryCheckInterval        = time.Hour
	defaultEntryCheckBatchPercent    = 0.01
	defaultMaxTimerBatchSizePerWrite = 0
	defaultResignTimeout             = 5 * time.Minute
	defaultDefaultPolicies           = []policy.Policy{
		policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*24*time.Hour), policy.DefaultAggregationID),
		policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 40*24*time.Hour), policy.DefaultAggregationID),
	}

	defaultDefaultCounterAggregationTypes = policy.AggregationTypes{
		policy.Sum,
	}
	defaultDefaultTimerAggregationTypes = policy.AggregationTypes{
		policy.Sum,
		policy.SumSq,
		policy.Mean,
		policy.Min,
		policy.Max,
		policy.Count,
		policy.Stdev,
		policy.Median,
		policy.P50,
		policy.P95,
		policy.P99,
	}
	defaultDefaultGaugeAggregationTypes = policy.AggregationTypes{
		policy.Last,
	}

	defaultCounterSuffixOverride = map[policy.AggregationType][]byte{
		policy.Sum: nil,
	}
	defaultTimerSuffixOverride = map[policy.AggregationType][]byte{
		policy.Min: []byte(".lower"),
		policy.Max: []byte(".upper"),
	}
	defaultGaugeSuffixOverride = map[policy.AggregationType][]byte{
		policy.Last: nil,
	}

	// By default writes are buffered for 10 minutes before traffic is cut over to a shard
	// in case there are issues with a new instance taking over shards.
	defaultBufferDurationBeforeShardCutover = 10 * time.Minute

	// By default writes are buffered for one hour after traffic is cut off in case there
	// are issues with the instances taking over the shards and as such we need to switch
	// the traffic back to the previous owner of the shards immediately.
	defaultBufferDurationAfterShardCutoff = time.Hour
)

type options struct {
	// Base options.
	defaultCounterAggregationTypes   policy.AggregationTypes
	defaultTimerAggregationTypes     policy.AggregationTypes
	defaultGaugeAggregationTypes     policy.AggregationTypes
	metricPrefix                     []byte
	counterPrefix                    []byte
	timerPrefix                      []byte
	gaugePrefix                      []byte
	sumSuffix                        []byte
	sumSqSuffix                      []byte
	meanSuffix                       []byte
	lastSuffix                       []byte
	minSuffix                        []byte
	maxSuffix                        []byte
	countSuffix                      []byte
	stdevSuffix                      []byte
	medianSuffix                     []byte
	timerQuantileSuffixFn            QuantileSuffixFn
	timeLock                         *sync.RWMutex
	clockOpts                        clock.Options
	instrumentOpts                   instrument.Options
	streamOpts                       cm.Options
	placementManager                 PlacementManager
	shardFn                          ShardFn
	bufferDurationBeforeShardCutover time.Duration
	bufferDurationAfterShardCutoff   time.Duration
	flushManager                     FlushManager
	minFlushInterval                 time.Duration
	maxFlushSize                     int
	flushHandler                     Handler
	entryTTL                         time.Duration
	entryCheckInterval               time.Duration
	entryCheckBatchPercent           float64
	maxTimerBatchSizePerWrite        int
	defaultPolicies                  []policy.Policy
	flushTimesManager                FlushTimesManager
	electionManager                  ElectionManager
	resignTimeout                    time.Duration
	entryPool                        EntryPool
	counterElemPool                  CounterElemPool
	timerElemPool                    TimerElemPool
	gaugeElemPool                    GaugeElemPool
	bufferedEncoderPool              msgpack.BufferedEncoderPool
	aggTypesPool                     policy.AggregationTypesPool
	quantilesPool                    pool.FloatsPool

	counterSuffixOverride map[policy.AggregationType][]byte
	timerSuffixOverride   map[policy.AggregationType][]byte
	gaugeSuffixOverride   map[policy.AggregationType][]byte

	defaultSuffixes [][]byte
	counterSuffixes [][]byte
	timerSuffixes   [][]byte
	gaugeSuffixes   [][]byte

	// Derived options.
	fullCounterPrefix                 []byte
	fullTimerPrefix                   []byte
	fullGaugePrefix                   []byte
	defaultCounterAggregationSuffixes [][]byte
	defaultTimerAggregationSuffixes   [][]byte
	defaultGaugeAggregationSuffixes   [][]byte
	timerQuantiles                    []float64
}

// NewOptions create a new set of options.
func NewOptions() Options {
	o := &options{
		defaultCounterAggregationTypes: defaultDefaultCounterAggregationTypes,
		defaultTimerAggregationTypes:   defaultDefaultTimerAggregationTypes,
		defaultGaugeAggregationTypes:   defaultDefaultGaugeAggregationTypes,
		metricPrefix:                   defaultMetricPrefix,
		counterPrefix:                  defaultCounterPrefix,
		timerPrefix:                    defaultTimerPrefix,
		gaugePrefix:                    defaultGaugePrefix,
		lastSuffix:                     defaultAggregationLastSuffix,
		minSuffix:                      defaultAggregationMinSuffix,
		maxSuffix:                      defaultAggregationMaxSuffix,
		meanSuffix:                     defaultAggregationMeanSuffix,
		medianSuffix:                   defaultAggregationMedianSuffix,
		countSuffix:                    defaultAggregationCountSuffix,
		sumSuffix:                      defaultAggregationSumSuffix,
		sumSqSuffix:                    defaultAggregationSumSqSuffix,
		stdevSuffix:                    defaultAggregationStdevSuffix,
		timerQuantileSuffixFn:          defaultTimerQuantileSuffixFn,
		timeLock:                       &sync.RWMutex{},
		clockOpts:                      clock.NewOptions(),
		instrumentOpts:                 instrument.NewOptions(),
		streamOpts:                     cm.NewOptions(),
		shardFn:                        defaultShardFn,
		bufferDurationBeforeShardCutover: defaultBufferDurationBeforeShardCutover,
		bufferDurationAfterShardCutoff:   defaultBufferDurationAfterShardCutoff,
		minFlushInterval:                 defaultMinFlushInterval,
		maxFlushSize:                     defaultMaxFlushSize,
		entryTTL:                         defaultEntryTTL,
		entryCheckInterval:               defaultEntryCheckInterval,
		entryCheckBatchPercent:           defaultEntryCheckBatchPercent,
		maxTimerBatchSizePerWrite:        defaultMaxTimerBatchSizePerWrite,
		defaultPolicies:                  defaultDefaultPolicies,
		resignTimeout:                    defaultResignTimeout,
		counterSuffixOverride:            defaultCounterSuffixOverride,
		timerSuffixOverride:              defaultTimerSuffixOverride,
		gaugeSuffixOverride:              defaultGaugeSuffixOverride,
	}

	// Initialize pools.
	o.initPools()

	// Compute derived options.
	o.computeAllDerived()

	return o
}

func (o *options) SetDefaultCounterAggregationTypes(aggTypes policy.AggregationTypes) Options {
	opts := *o
	opts.defaultCounterAggregationTypes = aggTypes
	opts.computeSuffixes()
	return &opts
}

func (o *options) DefaultCounterAggregationTypes() policy.AggregationTypes {
	return o.defaultCounterAggregationTypes
}

func (o *options) SetDefaultTimerAggregationTypes(aggTypes policy.AggregationTypes) Options {
	opts := *o
	opts.defaultTimerAggregationTypes = aggTypes
	opts.computeQuantiles()
	opts.computeSuffixes()
	return &opts
}

func (o *options) DefaultTimerAggregationTypes() policy.AggregationTypes {
	return o.defaultTimerAggregationTypes
}

func (o *options) SetDefaultGaugeAggregationTypes(aggTypes policy.AggregationTypes) Options {
	opts := *o
	opts.defaultGaugeAggregationTypes = aggTypes
	opts.computeSuffixes()
	return &opts
}

func (o *options) DefaultGaugeAggregationTypes() policy.AggregationTypes {
	return o.defaultGaugeAggregationTypes
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

func (o *options) SetGaugePrefix(value []byte) Options {
	opts := *o
	opts.gaugePrefix = value
	opts.computeFullGaugePrefix()
	return &opts
}

func (o *options) GaugePrefix() []byte {
	return o.gaugePrefix
}

func (o *options) SetAggregationLastSuffix(value []byte) Options {
	opts := *o
	opts.lastSuffix = value
	opts.computeSuffixes()
	return &opts
}

func (o *options) AggregationLastSuffix() []byte {
	return o.lastSuffix
}

func (o *options) SetAggregationMinSuffix(value []byte) Options {
	opts := *o
	opts.minSuffix = value
	opts.computeSuffixes()
	return &opts
}

func (o *options) AggregationMinSuffix() []byte {
	return o.minSuffix
}

func (o *options) SetAggregationMaxSuffix(value []byte) Options {
	opts := *o
	opts.maxSuffix = value
	opts.computeSuffixes()
	return &opts
}

func (o *options) AggregationMaxSuffix() []byte {
	return o.maxSuffix
}

func (o *options) SetAggregationMeanSuffix(value []byte) Options {
	opts := *o
	opts.meanSuffix = value
	opts.computeSuffixes()
	return &opts
}

func (o *options) AggregationMeanSuffix() []byte {
	return o.meanSuffix
}

func (o *options) SetAggregationMedianSuffix(value []byte) Options {
	opts := *o
	opts.medianSuffix = value
	opts.computeSuffixes()
	return &opts
}

func (o *options) AggregationMedianSuffix() []byte {
	return o.medianSuffix
}

func (o *options) SetAggregationCountSuffix(value []byte) Options {
	opts := *o
	opts.countSuffix = value
	opts.computeSuffixes()
	return &opts
}

func (o *options) AggregationCountSuffix() []byte {
	return o.countSuffix
}

func (o *options) SetAggregationSumSuffix(value []byte) Options {
	opts := *o
	opts.sumSuffix = value
	opts.computeSuffixes()
	return &opts
}

func (o *options) AggregationSumSuffix() []byte {
	return o.sumSuffix
}

func (o *options) SetAggregationSumSqSuffix(value []byte) Options {
	opts := *o
	opts.sumSqSuffix = value
	opts.computeSuffixes()
	return &opts
}

func (o *options) AggregationSumSqSuffix() []byte {
	return o.sumSqSuffix
}

func (o *options) SetAggregationStdevSuffix(value []byte) Options {
	opts := *o
	opts.stdevSuffix = value
	opts.computeSuffixes()
	return &opts
}

func (o *options) AggregationStdevSuffix() []byte {
	return o.stdevSuffix
}

func (o *options) SetTimerQuantileSuffixFn(value QuantileSuffixFn) Options {
	opts := *o
	opts.timerQuantileSuffixFn = value
	opts.computeSuffixes()
	return &opts
}

func (o *options) TimerQuantileSuffixFn() QuantileSuffixFn {
	return o.timerQuantileSuffixFn
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

func (o *options) SetPlacementManager(value PlacementManager) Options {
	opts := *o
	opts.placementManager = value
	return &opts
}

func (o *options) PlacementManager() PlacementManager {
	return o.placementManager
}

func (o *options) SetShardFn(value ShardFn) Options {
	opts := *o
	opts.shardFn = value
	return &opts
}

func (o *options) ShardFn() ShardFn {
	return o.shardFn
}

func (o *options) SetBufferDurationBeforeShardCutover(value time.Duration) Options {
	opts := *o
	opts.bufferDurationBeforeShardCutover = value
	return &opts
}

func (o *options) BufferDurationBeforeShardCutover() time.Duration {
	return o.bufferDurationBeforeShardCutover
}

func (o *options) SetBufferDurationAfterShardCutoff(value time.Duration) Options {
	opts := *o
	opts.bufferDurationAfterShardCutoff = value
	return &opts
}

func (o *options) BufferDurationAfterShardCutoff() time.Duration {
	return o.bufferDurationAfterShardCutoff
}

func (o *options) SetFlushTimesManager(value FlushTimesManager) Options {
	opts := *o
	opts.flushTimesManager = value
	return &opts
}

func (o *options) FlushTimesManager() FlushTimesManager {
	return o.flushTimesManager
}

func (o *options) SetElectionManager(value ElectionManager) Options {
	opts := *o
	opts.electionManager = value
	return &opts
}

func (o *options) ElectionManager() ElectionManager {
	return o.electionManager
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

func (o *options) SetResignTimeout(value time.Duration) Options {
	opts := *o
	opts.resignTimeout = value
	return &opts
}

func (o *options) ResignTimeout() time.Duration {
	return o.resignTimeout
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

func (o *options) SetAggregationTypesPool(pool policy.AggregationTypesPool) Options {
	opts := *o
	opts.aggTypesPool = pool
	return &opts
}

func (o *options) AggregationTypesPool() policy.AggregationTypesPool {
	return o.aggTypesPool
}

func (o *options) SetQuantilesPool(pool pool.FloatsPool) Options {
	opts := *o
	opts.quantilesPool = pool
	return &opts
}

func (o *options) SetCounterSuffixOverride(m map[policy.AggregationType][]byte) Options {
	opts := *o
	opts.counterSuffixOverride = m
	opts.computeSuffixes()
	return &opts
}

func (o *options) SetTimerSuffixOverride(m map[policy.AggregationType][]byte) Options {
	opts := *o
	opts.timerSuffixOverride = m
	opts.computeSuffixes()
	return &opts
}

func (o *options) SetGaugeSuffixOverride(m map[policy.AggregationType][]byte) Options {
	opts := *o
	opts.gaugeSuffixOverride = m
	opts.computeSuffixes()
	return &opts
}

func (o *options) QuantilesPool() pool.FloatsPool {
	return o.quantilesPool
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

func (o *options) DefaultCounterAggregationSuffixes() [][]byte {
	return o.defaultCounterAggregationSuffixes
}

func (o *options) DefaultTimerAggregationSuffixes() [][]byte {
	return o.defaultTimerAggregationSuffixes
}

func (o *options) DefaultGaugeAggregationSuffixes() [][]byte {
	return o.defaultGaugeAggregationSuffixes
}

func (o *options) SuffixForCounter(aggType policy.AggregationType) []byte {
	return o.counterSuffixes[aggType.ID()]
}

func (o *options) SuffixForTimer(aggType policy.AggregationType) []byte {
	return o.timerSuffixes[aggType.ID()]
}

func (o *options) SuffixForGauge(aggType policy.AggregationType) []byte {
	return o.gaugeSuffixes[aggType.ID()]
}

func (o *options) initPools() {
	o.entryPool = NewEntryPool(nil)
	o.entryPool.Init(func() *Entry {
		return NewEntry(nil, o)
	})

	o.aggTypesPool = policy.NewAggregationTypesPool(nil)
	o.aggTypesPool.Init(func() policy.AggregationTypes {
		return make(policy.AggregationTypes, 0, len(policy.ValidAggregationTypes))
	})

	o.counterElemPool = NewCounterElemPool(nil)
	o.counterElemPool.Init(func() *CounterElem {
		return NewCounterElem(nil, policy.EmptyStoragePolicy, policy.DefaultAggregationTypes, o)
	})

	o.timerElemPool = NewTimerElemPool(nil)
	o.timerElemPool.Init(func() *TimerElem {
		return NewTimerElem(nil, policy.EmptyStoragePolicy, policy.DefaultAggregationTypes, o)
	})

	o.gaugeElemPool = NewGaugeElemPool(nil)
	o.gaugeElemPool.Init(func() *GaugeElem {
		return NewGaugeElem(nil, policy.EmptyStoragePolicy, policy.DefaultAggregationTypes, o)
	})

	o.bufferedEncoderPool = msgpack.NewBufferedEncoderPool(nil)
	o.bufferedEncoderPool.Init(func() msgpack.BufferedEncoder {
		return msgpack.NewPooledBufferedEncoder(o.bufferedEncoderPool)
	})

	o.quantilesPool = pool.NewFloatsPool(nil, nil)
	o.quantilesPool.Init()
}

func (o *options) computeAllDerived() {
	o.computeFullPrefixes()
	o.computeQuantiles()
	o.computeSuffixes()
}

func (o *options) computeFullPrefixes() {
	o.computeFullCounterPrefix()
	o.computeFullTimerPrefix()
	o.computeFullGaugePrefix()
}

func (o *options) computeQuantiles() {
	o.timerQuantiles, _ = o.DefaultTimerAggregationTypes().PooledQuantiles(o.QuantilesPool())
}

func (o *options) computeSuffixes() {
	// NB(cw) The order matters.
	o.computeDefaultSuffixes()
	o.computeCounterSuffixes()
	o.computeTimerSuffixes()
	o.computeGaugeSuffixes()
	o.computeDefaultCounterAggregationSuffix()
	o.computeDefaultTimerAggregationSuffix()
	o.computeDefaultGaugeAggregationSuffix()
}

func (o *options) computeDefaultSuffixes() {
	o.defaultSuffixes = make([][]byte, policy.MaxAggregationTypeID+1)

	for aggType := range policy.ValidAggregationTypes {
		switch aggType {
		case policy.Last:
			o.defaultSuffixes[aggType.ID()] = o.AggregationLastSuffix()
		case policy.Min:
			o.defaultSuffixes[aggType.ID()] = o.AggregationMinSuffix()
		case policy.Max:
			o.defaultSuffixes[aggType.ID()] = o.AggregationMaxSuffix()
		case policy.Mean:
			o.defaultSuffixes[aggType.ID()] = o.AggregationMeanSuffix()
		case policy.Median:
			o.defaultSuffixes[aggType.ID()] = o.AggregationMedianSuffix()
		case policy.Count:
			o.defaultSuffixes[aggType.ID()] = o.AggregationCountSuffix()
		case policy.Sum:
			o.defaultSuffixes[aggType.ID()] = o.AggregationSumSuffix()
		case policy.SumSq:
			o.defaultSuffixes[aggType.ID()] = o.AggregationSumSqSuffix()
		case policy.Stdev:
			o.defaultSuffixes[aggType.ID()] = o.AggregationStdevSuffix()
		default:
			q, ok := aggType.Quantile()
			if ok {
				o.defaultSuffixes[aggType.ID()] = o.timerQuantileSuffixFn(q)
			}
		}
	}
}

func (o *options) computeCounterSuffixes() {
	o.counterSuffixes = o.computeOverrideSuffixes(o.counterSuffixOverride)
}

func (o *options) computeTimerSuffixes() {
	o.timerSuffixes = o.computeOverrideSuffixes(o.timerSuffixOverride)
}

func (o *options) computeGaugeSuffixes() {
	o.gaugeSuffixes = o.computeOverrideSuffixes(o.gaugeSuffixOverride)
}

func (o options) computeOverrideSuffixes(m map[policy.AggregationType][]byte) [][]byte {
	res := make([][]byte, policy.MaxAggregationTypeID+1)
	for aggType := range policy.ValidAggregationTypes {
		if suffix, ok := m[aggType]; ok {
			res[aggType.ID()] = suffix
			continue
		}
		res[aggType.ID()] = o.defaultSuffixes[aggType.ID()]
	}
	return res
}

func (o *options) computeDefaultCounterAggregationSuffix() {
	o.defaultCounterAggregationSuffixes = make([][]byte, len(o.DefaultCounterAggregationTypes()))
	for i, aggType := range o.DefaultCounterAggregationTypes() {
		o.defaultCounterAggregationSuffixes[i] = o.SuffixForCounter(aggType)
	}
}

func (o *options) computeDefaultTimerAggregationSuffix() {
	o.defaultTimerAggregationSuffixes = make([][]byte, len(o.DefaultTimerAggregationTypes()))
	for i, aggType := range o.DefaultTimerAggregationTypes() {
		o.defaultTimerAggregationSuffixes[i] = o.SuffixForTimer(aggType)
	}
}

func (o *options) computeDefaultGaugeAggregationSuffix() {
	o.defaultGaugeAggregationSuffixes = make([][]byte, len(o.DefaultGaugeAggregationTypes()))
	for i, aggType := range o.DefaultGaugeAggregationTypes() {
		o.defaultGaugeAggregationSuffixes[i] = o.SuffixForGauge(aggType)
	}
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
	return []byte(".p" + strconv.FormatFloat(quantile*100, 'f', -1, 64))
}

func defaultShardFn(id []byte, numShards int) uint32 {
	return murmur3.Sum32(id) % uint32(numShards)
}
