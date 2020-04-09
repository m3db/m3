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

	"github.com/m3db/m3/src/aggregator/aggregation/quantile/cm"
	"github.com/m3db/m3/src/aggregator/aggregator/handler"
	"github.com/m3db/m3/src/aggregator/aggregator/handler/writer"
	"github.com/m3db/m3/src/aggregator/client"
	"github.com/m3db/m3/src/aggregator/runtime"
	"github.com/m3db/m3/src/aggregator/sharding"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/pipeline/applied"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"
	xtime "github.com/m3db/m3/src/x/time"
)

var (
	defaultMetricPrefix               = []byte("stats.")
	defaultCounterPrefix              = []byte("counts.")
	defaultTimerPrefix                = []byte("timers.")
	defaultGaugePrefix                = []byte("gauges.")
	defaultEntryTTL                   = time.Hour
	defaultEntryCheckInterval         = time.Hour
	defaultEntryCheckBatchPercent     = 0.01
	defaultMaxTimerBatchSizePerWrite  = 0
	defaultMaxNumCachedSourceSets     = 2
	defaultDiscardNaNAggregatedValues = true
	defaultResignTimeout              = 5 * time.Minute
	defaultDefaultStoragePolicies     = []policy.StoragePolicy{
		policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*24*time.Hour),
		policy.NewStoragePolicy(time.Minute, xtime.Minute, 40*24*time.Hour),
	}
	defaultVerboseErrors = false

	defaultTimedMetricBuffer = time.Minute

	// By default writes are buffered for 10 minutes before traffic is cut over to a shard
	// in case there are issues with a new instance taking over shards.
	defaultBufferDurationBeforeShardCutover = 10 * time.Minute

	// By default writes are buffered for one hour after traffic is cut off in case there
	// are issues with the instances taking over the shards and as such we need to switch
	// the traffic back to the previous owner of the shards immediately.
	defaultBufferDurationAfterShardCutoff = time.Hour
)

// MaxAllowedForwardingDelayFn returns the maximum allowed forwarding delay given
// the metric resolution and number of times the metric has been forwarded. The forwarding
// delay refers to the maximum tolerated delay between when a metric is ready to be
// forwarded (i.e., when the current aggregation window is closed) at the originating
// server and when the forwarded metric is ingested successfully at the destination server.
// This is the overall maximum delay accounting for between when a metric is ready to be
// forwarded and when the actual flush happens due to scheduling delay and flush jittering
// if any, flushing delay, queuing delay, encoding delay, network delay, decoding delay at
// destination server, and ingestion delay at the destination server.
type MaxAllowedForwardingDelayFn func(resolution time.Duration, numForwardedTimes int) time.Duration

// BufferForPastTimedMetricFn returns the buffer duration for past timed metrics.
type BufferForPastTimedMetricFn func(resolution time.Duration) time.Duration

// Options provide a set of base and derived options for the aggregator.
type Options interface {
	/// Read-write base options.

	// SetMetricPrefix sets the common prefix for all metric types.
	SetMetricPrefix(value []byte) Options

	// MetricPrefix returns the common prefix for all metric types.
	MetricPrefix() []byte

	// SetCounterPrefix sets the prefix for counters.
	SetCounterPrefix(value []byte) Options

	// CounterPrefix returns the prefix for counters.
	CounterPrefix() []byte

	// SetTimerPrefix sets the prefix for timers.
	SetTimerPrefix(value []byte) Options

	// TimerPrefix returns the prefix for timers.
	TimerPrefix() []byte

	// SetGaugePrefix sets the prefix for gauges.
	SetGaugePrefix(value []byte) Options

	// GaugePrefix returns the prefix for gauges.
	GaugePrefix() []byte

	// SetTimeLock sets the time lock.
	SetTimeLock(value *sync.RWMutex) Options

	// TimeLock returns the time lock.
	TimeLock() *sync.RWMutex

	// SetAggregationTypesOptions sets the aggregation types options.
	SetAggregationTypesOptions(value aggregation.TypesOptions) Options

	// AggregationTypesOptions returns the aggregation types options.
	AggregationTypesOptions() aggregation.TypesOptions

	// SetClockOptions sets the clock options.
	SetClockOptions(value clock.Options) Options

	// ClockOptions returns the clock options.
	ClockOptions() clock.Options

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetStreamOptions sets the stream options.
	SetStreamOptions(value cm.Options) Options

	// StreamOptions returns the stream options.
	StreamOptions() cm.Options

	// SetAdminClient sets the administrative client.
	SetAdminClient(value client.AdminClient) Options

	// AdminClient returns the administrative client.
	AdminClient() client.AdminClient

	// SetRuntimeOptionsManager sets the runtime options manager.
	SetRuntimeOptionsManager(value runtime.OptionsManager) Options

	// RuntimeOptionsManager returns the runtime options manager.
	RuntimeOptionsManager() runtime.OptionsManager

	// SetPlacementManager sets the placement manager.
	SetPlacementManager(value PlacementManager) Options

	// PlacementManager returns the placement manager.
	PlacementManager() PlacementManager

	// SetShardFn sets the sharding function.
	SetShardFn(value sharding.ShardFn) Options

	// ShardFn returns the sharding function.
	ShardFn() sharding.ShardFn

	// SetBufferDurationBeforeShardCutover sets the duration for buffering writes before shard cutover.
	SetBufferDurationBeforeShardCutover(value time.Duration) Options

	// BufferDurationBeforeShardCutover returns the duration for buffering writes before shard cutover.
	BufferDurationBeforeShardCutover() time.Duration

	// SetBufferDurationAfterShardCutoff sets the duration for buffering writes after shard cutoff.
	SetBufferDurationAfterShardCutoff(value time.Duration) Options

	// BufferDurationAfterShardCutoff returns the duration for buffering writes after shard cutoff.
	BufferDurationAfterShardCutoff() time.Duration

	// SetFlushTimesManager sets the flush times manager.
	SetFlushTimesManager(value FlushTimesManager) Options

	// FlushTimesManager returns the flush times manager.
	FlushTimesManager() FlushTimesManager

	// SetElectionManager sets the election manager.
	SetElectionManager(value ElectionManager) Options

	// ElectionManager returns the election manager.
	ElectionManager() ElectionManager

	// SetFlushManager sets the flush manager.
	SetFlushManager(value FlushManager) Options

	// FlushManager returns the flush manager.
	FlushManager() FlushManager

	// SetFlushHandler sets the handler that flushes buffered encoders.
	SetFlushHandler(value handler.Handler) Options

	// FlushHandler returns the handler that flushes buffered encoders.
	FlushHandler() handler.Handler

	// SetPassThroughWriter sets the writer for pass-through metrics.
	SetPassThroughWriter(value writer.Writer) Options

	// PassThroughWriter returns the writer for pass-through metrics.
	PassThroughWriter() writer.Writer

	// SetEntryTTL sets the ttl for expiring stale entries.
	SetEntryTTL(value time.Duration) Options

	// EntryTTL returns the ttl for expiring stale entries.
	EntryTTL() time.Duration

	// SetEntryCheckInterval sets the interval for checking expired entries.
	SetEntryCheckInterval(value time.Duration) Options

	// EntryCheckInterval returns the interval for checking expired entries.
	EntryCheckInterval() time.Duration

	// SetEntryCheckBatchPercent sets the batch percentage for checking expired entries.
	SetEntryCheckBatchPercent(value float64) Options

	// EntryCheckBatchPercent returns the batch percentage for checking expired entries.
	EntryCheckBatchPercent() float64

	// SetMaxTimerBatchSizePerWrite sets the maximum timer batch size for each batched write.
	SetMaxTimerBatchSizePerWrite(value int) Options

	// MaxTimerBatchSizePerWrite returns the maximum timer batch size for each batched write.
	MaxTimerBatchSizePerWrite() int

	// SetDefaultStoragePolicies sets the default policies.
	SetDefaultStoragePolicies(value []policy.StoragePolicy) Options

	// DefaultStoragePolicies returns the default storage policies.
	DefaultStoragePolicies() []policy.StoragePolicy

	// SetResignTimeout sets the resign timeout.
	SetResignTimeout(value time.Duration) Options

	// ResignTimeout returns the resign timeout.
	ResignTimeout() time.Duration

	// SetMaxAllowedForwardingDelayFn sets the function that determines the maximum forwarding
	// delay for given metric resolution and number of times the metric has been forwarded.
	SetMaxAllowedForwardingDelayFn(value MaxAllowedForwardingDelayFn) Options

	// MaxAllowedForwardingDelayFn returns the function that determines the maximum forwarding
	// delay for given metric resolution and number of times the metric has been forwarded.
	MaxAllowedForwardingDelayFn() MaxAllowedForwardingDelayFn

	// SetBufferForPastTimedMetricFn sets the size of the buffer for timed metrics in the past.
	SetBufferForPastTimedMetricFn(value BufferForPastTimedMetricFn) Options

	// BufferForPastTimedMetricFn returns the size of the buffer for timed metrics in the past.
	BufferForPastTimedMetricFn() BufferForPastTimedMetricFn

	// SetBufferForFutureTimedMetric sets the size of the buffer for timed metrics in the future.
	SetBufferForFutureTimedMetric(value time.Duration) Options

	// BufferForFutureTimedMetric returns the size of the buffer for timed metrics in the future.
	BufferForFutureTimedMetric() time.Duration

	// SetMaxNumCachedSourceSets sets the maximum number of cached source sets.
	SetMaxNumCachedSourceSets(value int) Options

	// MaxNumCachedSourceSets returns the maximum number of cached source sets.
	MaxNumCachedSourceSets() int

	// SetDiscardNaNAggregatedValues determines whether NaN aggregated values are discarded.
	SetDiscardNaNAggregatedValues(value bool) Options

	// DiscardNaNAggregatedValues determines whether NaN aggregated values are discarded.
	DiscardNaNAggregatedValues() bool

	// SetEntryPool sets the entry pool.
	SetEntryPool(value EntryPool) Options

	// EntryPool returns the entry pool.
	EntryPool() EntryPool

	// SetCounterElemPool sets the counter element pool.
	SetCounterElemPool(value CounterElemPool) Options

	// CounterElemPool returns the counter element pool.
	CounterElemPool() CounterElemPool

	// SetTimerElemPool sets the timer element pool.
	SetTimerElemPool(value TimerElemPool) Options

	// TimerElemPool returns the timer element pool.
	TimerElemPool() TimerElemPool

	// SetGaugeElemPool sets the gauge element pool.
	SetGaugeElemPool(value GaugeElemPool) Options

	// GaugeElemPool returns the gauge element pool.
	GaugeElemPool() GaugeElemPool

	/// Read-only derived options.

	// FullCounterPrefix returns the full prefix for counters.
	FullCounterPrefix() []byte

	// FullTimerPrefix returns the full prefix for timers.
	FullTimerPrefix() []byte

	// FullGaugePrefix returns the full prefix for gauges.
	FullGaugePrefix() []byte

	// SetVerboseErrors returns whether to return verbose errors or not.
	SetVerboseErrors(value bool) Options

	// VerboseErrors returns whether to return verbose errors or not.
	VerboseErrors() bool
}

type options struct {
	// Base options.
	aggTypesOptions                  aggregation.TypesOptions
	metricPrefix                     []byte
	counterPrefix                    []byte
	timerPrefix                      []byte
	gaugePrefix                      []byte
	timeLock                         *sync.RWMutex
	clockOpts                        clock.Options
	instrumentOpts                   instrument.Options
	streamOpts                       cm.Options
	adminClient                      client.AdminClient
	runtimeOptsManager               runtime.OptionsManager
	placementManager                 PlacementManager
	shardFn                          sharding.ShardFn
	bufferDurationBeforeShardCutover time.Duration
	bufferDurationAfterShardCutoff   time.Duration
	flushManager                     FlushManager
	flushHandler                     handler.Handler
	passThroughWriter                writer.Writer
	entryTTL                         time.Duration
	entryCheckInterval               time.Duration
	entryCheckBatchPercent           float64
	maxTimerBatchSizePerWrite        int
	defaultStoragePolicies           []policy.StoragePolicy
	flushTimesManager                FlushTimesManager
	electionManager                  ElectionManager
	resignTimeout                    time.Duration
	maxAllowedForwardingDelayFn      MaxAllowedForwardingDelayFn
	bufferForPastTimedMetricFn       BufferForPastTimedMetricFn
	bufferForFutureTimedMetric       time.Duration
	maxNumCachedSourceSets           int
	discardNaNAggregatedValues       bool
	entryPool                        EntryPool
	counterElemPool                  CounterElemPool
	timerElemPool                    TimerElemPool
	gaugeElemPool                    GaugeElemPool
	verboseErrors                    bool

	// Derived options.
	fullCounterPrefix []byte
	fullTimerPrefix   []byte
	fullGaugePrefix   []byte
	timerQuantiles    []float64
}

// NewOptions create a new set of options.
func NewOptions() Options {
	aggTypesOptions := aggregation.NewTypesOptions().
		SetCounterTypeStringTransformFn(aggregation.EmptyTransform).
		SetTimerTypeStringTransformFn(aggregation.SuffixTransform).
		SetGaugeTypeStringTransformFn(aggregation.EmptyTransform)
	o := &options{
		aggTypesOptions:                  aggTypesOptions,
		metricPrefix:                     defaultMetricPrefix,
		counterPrefix:                    defaultCounterPrefix,
		timerPrefix:                      defaultTimerPrefix,
		gaugePrefix:                      defaultGaugePrefix,
		timeLock:                         &sync.RWMutex{},
		clockOpts:                        clock.NewOptions(),
		instrumentOpts:                   instrument.NewOptions(),
		streamOpts:                       cm.NewOptions(),
		runtimeOptsManager:               runtime.NewOptionsManager(runtime.NewOptions()),
		shardFn:                          sharding.Murmur32Hash.MustShardFn(),
		bufferDurationBeforeShardCutover: defaultBufferDurationBeforeShardCutover,
		bufferDurationAfterShardCutoff:   defaultBufferDurationAfterShardCutoff,
		entryTTL:                         defaultEntryTTL,
		entryCheckInterval:               defaultEntryCheckInterval,
		entryCheckBatchPercent:           defaultEntryCheckBatchPercent,
		maxTimerBatchSizePerWrite:        defaultMaxTimerBatchSizePerWrite,
		defaultStoragePolicies:           defaultDefaultStoragePolicies,
		resignTimeout:                    defaultResignTimeout,
		maxAllowedForwardingDelayFn:      defaultMaxAllowedForwardingDelayFn,
		bufferForPastTimedMetricFn:       defaultBufferForPastTimedMetricFn,
		bufferForFutureTimedMetric:       defaultTimedMetricBuffer,
		maxNumCachedSourceSets:           defaultMaxNumCachedSourceSets,
		discardNaNAggregatedValues:       defaultDiscardNaNAggregatedValues,
		verboseErrors:                    defaultVerboseErrors,
	}

	// Initialize pools.
	o.initPools()

	// Compute derived options.
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

func (o *options) SetAggregationTypesOptions(value aggregation.TypesOptions) Options {
	opts := *o
	opts.aggTypesOptions = value
	return &opts
}

func (o *options) AggregationTypesOptions() aggregation.TypesOptions {
	return o.aggTypesOptions
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

func (o *options) SetAdminClient(value client.AdminClient) Options {
	opts := *o
	opts.adminClient = value
	return &opts
}

func (o *options) AdminClient() client.AdminClient {
	return o.adminClient
}

func (o *options) SetRuntimeOptionsManager(value runtime.OptionsManager) Options {
	opts := *o
	opts.runtimeOptsManager = value
	return &opts
}

func (o *options) RuntimeOptionsManager() runtime.OptionsManager {
	return o.runtimeOptsManager
}

func (o *options) SetPlacementManager(value PlacementManager) Options {
	opts := *o
	opts.placementManager = value
	return &opts
}

func (o *options) PlacementManager() PlacementManager {
	return o.placementManager
}

func (o *options) SetShardFn(value sharding.ShardFn) Options {
	opts := *o
	opts.shardFn = value
	return &opts
}

func (o *options) ShardFn() sharding.ShardFn {
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

func (o *options) SetFlushHandler(value handler.Handler) Options {
	opts := *o
	opts.flushHandler = value
	return &opts
}

func (o *options) FlushHandler() handler.Handler {
	return o.flushHandler
}

func (o *options) SetPassThroughWriter(value writer.Writer) Options {
	opts := *o
	opts.passThroughWriter = value
	return &opts
}

func (o *options) PassThroughWriter() writer.Writer {
	return o.passThroughWriter
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

func (o *options) SetDefaultStoragePolicies(value []policy.StoragePolicy) Options {
	opts := *o
	opts.defaultStoragePolicies = value
	return &opts
}

func (o *options) DefaultStoragePolicies() []policy.StoragePolicy {
	return o.defaultStoragePolicies
}

func (o *options) SetResignTimeout(value time.Duration) Options {
	opts := *o
	opts.resignTimeout = value
	return &opts
}

func (o *options) ResignTimeout() time.Duration {
	return o.resignTimeout
}

func (o *options) SetMaxAllowedForwardingDelayFn(value MaxAllowedForwardingDelayFn) Options {
	opts := *o
	opts.maxAllowedForwardingDelayFn = value
	return &opts
}

func (o *options) MaxAllowedForwardingDelayFn() MaxAllowedForwardingDelayFn {
	return o.maxAllowedForwardingDelayFn
}

func (o *options) SetBufferForPastTimedMetricFn(value BufferForPastTimedMetricFn) Options {
	opts := *o
	opts.bufferForPastTimedMetricFn = value
	return &opts
}

func (o *options) BufferForPastTimedMetricFn() BufferForPastTimedMetricFn {
	return o.bufferForPastTimedMetricFn
}

func (o *options) SetBufferForFutureTimedMetric(value time.Duration) Options {
	opts := *o
	opts.bufferForFutureTimedMetric = value
	return &opts
}

func (o *options) BufferForFutureTimedMetric() time.Duration {
	return o.bufferForFutureTimedMetric
}

func (o *options) SetMaxNumCachedSourceSets(value int) Options {
	opts := *o
	opts.maxNumCachedSourceSets = value
	return &opts
}

func (o *options) MaxNumCachedSourceSets() int {
	return o.maxNumCachedSourceSets
}

func (o *options) SetDiscardNaNAggregatedValues(value bool) Options {
	opts := *o
	opts.discardNaNAggregatedValues = value
	return &opts
}

func (o *options) DiscardNaNAggregatedValues() bool {
	return o.discardNaNAggregatedValues
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

func (o *options) SetVerboseErrors(value bool) Options {
	opts := *o
	opts.verboseErrors = value
	return &opts
}

func (o *options) VerboseErrors() bool {
	return o.verboseErrors
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

func (o *options) initPools() {
	defaultRuntimeOpts := runtime.NewOptions()
	o.entryPool = NewEntryPool(nil)
	o.entryPool.Init(func() *Entry {
		return NewEntry(nil, defaultRuntimeOpts, o)
	})

	o.counterElemPool = NewCounterElemPool(nil)
	o.counterElemPool.Init(func() *CounterElem {
		return MustNewCounterElem(nil, policy.EmptyStoragePolicy, aggregation.DefaultTypes, applied.DefaultPipeline, 0, WithPrefixWithSuffix, o)
	})

	o.timerElemPool = NewTimerElemPool(nil)
	o.timerElemPool.Init(func() *TimerElem {
		return MustNewTimerElem(nil, policy.EmptyStoragePolicy, aggregation.DefaultTypes, applied.DefaultPipeline, 0, WithPrefixWithSuffix, o)
	})

	o.gaugeElemPool = NewGaugeElemPool(nil)
	o.gaugeElemPool.Init(func() *GaugeElem {
		return MustNewGaugeElem(nil, policy.EmptyStoragePolicy, aggregation.DefaultTypes, applied.DefaultPipeline, 0, WithPrefixWithSuffix, o)
	})
}

func (o *options) computeAllDerived() {
	o.computeFullPrefixes()
}

func (o *options) computeFullPrefixes() {
	o.computeFullCounterPrefix()
	o.computeFullTimerPrefix()
	o.computeFullGaugePrefix()
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

func defaultMaxAllowedForwardingDelayFn(
	resolution time.Duration,
	numForwardedTimes int,
) time.Duration {
	return resolution * time.Duration(numForwardedTimes)
}

func defaultBufferForPastTimedMetricFn(resolution time.Duration) time.Duration {
	return resolution + defaultTimedMetricBuffer
}
