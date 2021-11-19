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
	"container/list"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3/src/aggregator/bitset"
	"github.com/m3db/m3/src/aggregator/rate"
	"github.com/m3db/m3/src/aggregator/runtime"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	metricid "github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/x/clock"
	xerrors "github.com/m3db/m3/src/x/errors"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/uber-go/tally"
	"go.uber.org/atomic"
	"go.uber.org/multierr"
)

const (
	// initialAggregationCapacity is the initial number of slots
	// allocated for aggregation metadata.
	initialAggregationCapacity = 2
)

var (
	errEntryClosed                 = errors.New("entry is closed")
	errWriteValueRateLimitExceeded = errors.New("write value rate limit is exceeded")
	errEmptyMetadatas              = errors.New("empty metadata list")
	errNoApplicableMetadata        = errors.New("no applicable metadata")
	errNoPipelinesInMetadata       = errors.New("no pipelines in metadata")
	errOnlyDefaultStagedMetadata   = xerrors.NewInvalidParamsError(
		errors.New("only default staged metadata provided"),
	)
	errOnlyDropPolicyStagedMetadata = xerrors.NewInvalidParamsError(
		errors.New("only drop policy staged metadata provided"),
	)
	errTooFarInTheFuture = xerrors.NewInvalidParamsError(errors.New("too far in the future"))
	errTooFarInThePast   = xerrors.NewInvalidParamsError(errors.New("too far in the past"))
	errArrivedTooLate    = xerrors.NewInvalidParamsError(errors.New("arrived too late"))
	errTimestampFormat   = time.RFC3339
)

// baseEntryMetrics are common to all entry types.
type baseEntryMetrics struct {
	rateLimit rateLimitEntryMetrics
	// count of metrics added to the entry.
	added tally.Counter
}

func newBaseEntryMetrics(scope tally.Scope) baseEntryMetrics {
	return baseEntryMetrics{
		rateLimit: newRateLimitEntryMetrics(scope),
		added:     scope.Counter("added"),
	}
}

type rateLimitEntryMetrics struct {
	valueRateLimitExceeded tally.Counter
	droppedValues          tally.Counter
}

func newRateLimitEntryMetrics(scope tally.Scope) rateLimitEntryMetrics {
	return rateLimitEntryMetrics{
		valueRateLimitExceeded: scope.Counter("value-rate-limit-exceeded"),
		droppedValues:          scope.Counter("dropped-values"),
	}
}

type untimedEntryMetrics struct {
	baseEntryMetrics
	emptyMetadatas          tally.Counter
	noApplicableMetadata    tally.Counter
	noPipelinesInMetadata   tally.Counter
	emptyPipeline           tally.Counter
	noAggregationInPipeline tally.Counter
	staleMetadata           tally.Counter
	tombstonedMetadata      tally.Counter
	metadatasUpdates        tally.Counter
}

func newUntimedEntryMetrics(scope tally.Scope) untimedEntryMetrics {
	return untimedEntryMetrics{
		baseEntryMetrics:        newBaseEntryMetrics(scope),
		emptyMetadatas:          scope.Counter("empty-metadatas"),
		noApplicableMetadata:    scope.Counter("no-applicable-metadata"),
		noPipelinesInMetadata:   scope.Counter("no-pipelines-in-metadata"),
		emptyPipeline:           scope.Counter("empty-pipeline"),
		noAggregationInPipeline: scope.Counter("no-aggregation-in-pipeline"),
		staleMetadata:           scope.Counter("stale-metadata"),
		tombstonedMetadata:      scope.Counter("tombstoned-metadata"),
		metadatasUpdates:        scope.Counter("metadatas-updates"),
	}
}

type timedEntryMetrics struct {
	baseEntryMetrics
	tooFarInTheFuture     tally.Counter
	tooFarInThePast       tally.Counter
	ingestDelay           tally.Histogram
	noPipelinesInMetadata tally.Counter
	tombstonedMetadata    tally.Counter
	metadataUpdates       tally.Counter
	metadatasUpdates      tally.Counter
}

func newTimedEntryMetrics(scope tally.Scope) timedEntryMetrics {
	return timedEntryMetrics{
		baseEntryMetrics:      newBaseEntryMetrics(scope),
		tooFarInTheFuture:     scope.Counter("too-far-in-the-future"),
		tooFarInThePast:       scope.Counter("too-far-in-the-past"),
		noPipelinesInMetadata: scope.Counter("no-pipelines-in-metadata"),
		tombstonedMetadata:    scope.Counter("tombstoned-metadata"),
		metadataUpdates:       scope.Counter("metadata-updates"),
		metadatasUpdates:      scope.Counter("metadatas-updates"),
		ingestDelay: scope.Histogram("ingest-delay", tally.DurationBuckets{
			time.Millisecond,
			time.Millisecond * 10,
			time.Millisecond * 100,
			time.Second,
			time.Second * 5,
			time.Second * 10,
			time.Second * 30,
			time.Minute,
			time.Minute * 2,
			time.Minute * 3,
			time.Minute * 5,
			time.Minute * 10,
		}),
	}
}

type forwardedEntryMetrics struct {
	baseEntryMetrics
	arrivedTooLate   tally.Counter
	duplicateSources tally.Counter
	metadataUpdates  tally.Counter
}

func newForwardedEntryMetrics(scope tally.Scope) forwardedEntryMetrics {
	return forwardedEntryMetrics{
		baseEntryMetrics: newBaseEntryMetrics(scope),
		arrivedTooLate:   scope.Counter("arrived-too-late"),
		duplicateSources: scope.Counter("duplicate-sources"),
		metadataUpdates:  scope.Counter("metadata-updates"),
	}
}

type entryMetrics struct {
	resendEnabled         tally.Counter
	retriedValues         tally.Counter
	untimed               untimedEntryMetrics
	timed                 timedEntryMetrics
	forwarded             forwardedEntryMetrics
	entryExpiryByCategory map[metricCategory]tally.Histogram
}

// NewEntryMetrics creates new entry metrics.
//nolint:golint,revive
func NewEntryMetrics(scope tally.Scope) *entryMetrics {
	scope = scope.SubScope("entry")
	untimedEntryScope := scope.Tagged(map[string]string{"entry-type": "untimed"})
	timedEntryScope := scope.Tagged(map[string]string{"entry-type": "timed"})
	forwardedEntryScope := scope.Tagged(map[string]string{"entry-type": "forwarded"})
	// NB: add a histogram tracking entry expiries to help tune entry TTL.
	expiries := make(map[metricCategory]tally.Histogram, len(validMetricCategories))
	for _, category := range validMetricCategories {
		expiries[category] = scope.
			Tagged(map[string]string{"metric-category": category.String()}).
			Histogram("expiry", tally.DurationBuckets{
				time.Minute,
				time.Minute * 2,
				time.Minute * 5,
				time.Minute * 10,
				time.Minute * 15,
				time.Minute * 20,
				time.Minute * 30,
				time.Minute * 40,
				time.Minute * 50,
				time.Minute * 60,
			})
	}
	return &entryMetrics{
		resendEnabled:         scope.Counter("resend-enabled"),
		retriedValues:         scope.Counter("retried-values"),
		untimed:               newUntimedEntryMetrics(untimedEntryScope),
		timed:                 newTimedEntryMetrics(timedEntryScope),
		forwarded:             newForwardedEntryMetrics(forwardedEntryScope),
		entryExpiryByCategory: expiries,
	}
}

// Entry keeps track of a metric's aggregations alongside the aggregation
// metadatas including storage policies, aggregation types, and remaining pipeline
// steps if any.
//
// TODO(xichen): make the access time per aggregation key for entries associated
// with forwarded metrics so we can reclaim aggregation elements associated with
// individual aggregation keys even though the entry is still active.
// nolint: maligned
type Entry struct {
	opts                Options
	decompressor        aggregation.IDDecompressor
	timeLock            *sync.RWMutex
	nowFn               clock.NowFn
	lists               *metricLists
	metrics             *entryMetrics
	rateLimiter         *rate.Limiter
	aggregations        aggregationValues
	numWriters          atomic.Int32
	lastAccessNanos     atomic.Int64
	cutoverNanos        int64
	mtx                 sync.RWMutex
	closed              bool
	hasDefaultMetadatas bool
}

// NewEntry creates a new entry.
func NewEntry(lists *metricLists, runtimeOpts runtime.Options, opts Options) *Entry {
	scope := opts.InstrumentOptions().MetricsScope()
	return NewEntryWithMetrics(lists, NewEntryMetrics(scope), runtimeOpts, opts)
}

// NewEntryWithMetrics creates a new entry.
func NewEntryWithMetrics(
	lists *metricLists,
	metrics *entryMetrics,
	runtimeOpts runtime.Options,
	opts Options,
) *Entry {
	e := &Entry{
		timeLock:     opts.TimeLock(),
		aggregations: make(aggregationValues, 0, initialAggregationCapacity),
		metrics:      metrics,
		decompressor: aggregation.NewPooledIDDecompressor(opts.AggregationTypesOptions().TypesPool()),
		rateLimiter:  rate.NewLimiter(0),
		nowFn:        opts.ClockOptions().NowFn(),
	}
	e.ResetSetData(lists, runtimeOpts, opts)
	return e
}

// IncWriter increases the writer count.
func (e *Entry) IncWriter() { e.numWriters.Inc() }

// DecWriter decreases the writer count.
func (e *Entry) DecWriter() { e.numWriters.Dec() }

// ResetSetData resets the entry and sets initial data.
// NB(xichen): we need to reset the options here to use the correct
// time lock contained in the options.
func (e *Entry) ResetSetData(lists *metricLists, runtimeOpts runtime.Options, opts Options) {
	e.mtx.Lock()
	e.closed = false
	e.opts = opts
	e.resetRateLimiterWithLock(runtimeOpts)
	e.hasDefaultMetadatas = false
	e.cutoverNanos = uninitializedCutoverNanos
	e.lists = lists
	e.numWriters.Store(0)
	e.lastAccessNanos.Store(int64(xtime.ToUnixNano(e.nowFn())))
	e.mtx.Unlock()
}

// SetRuntimeOptions updates the parameters of the rate limiter.
func (e *Entry) SetRuntimeOptions(opts runtime.Options) {
	e.mtx.Lock()
	if e.closed {
		e.mtx.Unlock()
		return
	}
	e.resetRateLimiterWithLock(opts)
	e.mtx.Unlock()
}

// AddUntimed adds an untimed metric along with its metadatas.
func (e *Entry) AddUntimed(
	metricUnion unaggregated.MetricUnion,
	metadatas metadata.StagedMetadatas,
) error {
	switch metricUnion.Type {
	case metric.TimerType:
		var err error
		if err = e.applyValueRateLimit(
			int64(len(metricUnion.BatchTimerVal)),
			e.metrics.untimed.rateLimit,
		); err == nil {
			err = e.writeBatchTimerWithMetadatas(metricUnion, metadatas)
		}
		if metricUnion.BatchTimerVal != nil && metricUnion.TimerValPool != nil {
			metricUnion.TimerValPool.Put(metricUnion.BatchTimerVal)
		}
		return err
	default:
		// For counters and gauges, there is a single value in the metric union.
		if err := e.applyValueRateLimit(1, e.metrics.untimed.rateLimit); err != nil {
			return err
		}
		return e.addUntimed(metricUnion, metadatas)
	}
}

// AddTimed adds a timed metric alongside its metadata.
func (e *Entry) AddTimed(
	metric aggregated.Metric,
	metadata metadata.TimedMetadata,
) error {
	if err := e.applyValueRateLimit(1, e.metrics.timed.rateLimit); err != nil {
		return err
	}
	return e.addTimed(metric, metadata, nil)
}

// AddTimedWithStagedMetadatas adds a timed metric with staged metadatas.
func (e *Entry) AddTimedWithStagedMetadatas(
	metric aggregated.Metric,
	metas metadata.StagedMetadatas,
) error {
	if err := e.applyValueRateLimit(1, e.metrics.timed.rateLimit); err != nil {
		return err
	}
	// Must have at least one metadata. addTimed further confirms that this metadata isn't the default metadata.
	if len(metas) == 0 {
		return errEmptyMetadatas
	}
	return e.addTimed(metric, metadata.TimedMetadata{}, metas)
}

// AddForwarded adds a forwarded metric alongside its metadata.
func (e *Entry) AddForwarded(
	metric aggregated.ForwardedMetric,
	metadata metadata.ForwardMetadata,
) error {
	if err := e.applyValueRateLimit(1, e.metrics.forwarded.rateLimit); err != nil {
		return err
	}
	return e.addForwarded(metric, metadata)
}

// ShouldExpire returns whether the entry should expire.
func (e *Entry) ShouldExpire(now time.Time) bool {
	e.mtx.RLock()
	if e.closed {
		e.mtx.RUnlock()
		return false
	}
	e.mtx.RUnlock()

	return e.shouldExpire(xtime.UnixNano(now.UnixNano()), unknownMetricCategory, false)
}

// TryExpire attempts to expire the entry, returning true
// if the entry is expired, and false otherwise.
func (e *Entry) TryExpire(now time.Time, metricCategory metricCategory) bool {
	e.mtx.Lock()
	if e.closed {
		e.mtx.Unlock()
		return false
	}
	if !e.shouldExpire(xtime.UnixNano(now.UnixNano()), metricCategory, true) {
		e.mtx.Unlock()
		return false
	}
	e.closed = true
	// Empty out the aggregation elements so they don't hold references
	// to other objects after being put back to pool to reduce GC overhead.
	for i := range e.aggregations {
		e.aggregations[i].elem.Value.(metricElem).MarkAsTombstoned()
		e.aggregations[i] = aggregationValue{}
	}
	e.aggregations = e.aggregations[:0]
	e.lists = nil
	pool := e.opts.EntryPool()
	e.mtx.Unlock()

	pool.Put(e)
	return true
}

func (e *Entry) writeBatchTimerWithMetadatas(
	metric unaggregated.MetricUnion,
	metadatas metadata.StagedMetadatas,
) error {
	// If there is no limit on the maximum batch size per write, write
	// all timers at once.
	maxTimerBatchSizePerWrite := e.opts.MaxTimerBatchSizePerWrite()
	if maxTimerBatchSizePerWrite == 0 {
		return e.addUntimed(metric, metadatas)
	}

	// Otherwise, honor maximum timer batch size.
	var (
		timerValues    = metric.BatchTimerVal
		numTimerValues = len(timerValues)
		start, end     int
	)
	for start = 0; start < numTimerValues; start = end {
		end = start + maxTimerBatchSizePerWrite
		if end > numTimerValues {
			end = numTimerValues
		}
		splitTimer := metric
		splitTimer.BatchTimerVal = timerValues[start:end]
		if err := e.addUntimed(splitTimer, metadatas); err != nil {
			return err
		}
	}
	return nil
}

func (e *Entry) addUntimed(
	metric unaggregated.MetricUnion,
	metadatas metadata.StagedMetadatas,
) error {
	e.timeLock.RLock()
	defer e.timeLock.RUnlock()

	// NB(xichen): it is important that we determine the current time
	// within the time lock. This ensures time ordering by wrapping
	// actions that need to happen before a given time within a read lock,
	// so it is guaranteed that actions before when a write lock is acquired
	// must have all completed. This is used to ensure we never write metrics
	// for times that have already been flushed.
	currTime := e.nowFn()
	e.lastAccessNanos.Store(currTime.UnixNano())

	e.mtx.RLock()
	if e.closed {
		e.mtx.RUnlock()
		return errEntryClosed
	}

	// Fast exit path for the common case where the metric has default metadatas for aggregation.
	hasDefaultMetadatas := metadatas.IsDefault()
	if e.hasDefaultMetadatas && hasDefaultMetadatas {
		err := e.addUntimedWithLock(currTime, metric)
		e.mtx.RUnlock()
		return err
	}

	sm, err := e.activeStagedMetadataWithLock(currTime, metadatas)
	if err != nil {
		e.mtx.RUnlock()
		return err
	}

	// If the metadata indicates the (rollup) metric has been tombstoned, the metric is
	// not ingested for aggregation. However, we do not update the policies asssociated
	// with this entry and mark it tombstoned because there may be a different raw metric
	// generating this same (rollup) metric that is actively emitting, meaning this entry
	// may still be very much alive.
	if sm.Tombstoned {
		e.mtx.RUnlock()
		e.metrics.untimed.tombstonedMetadata.Inc(1)
		return nil
	}

	// It is expected that there is at least one pipeline in the metadata.
	if len(sm.Pipelines) == 0 {
		e.mtx.RUnlock()
		e.metrics.untimed.noPipelinesInMetadata.Inc(1)
		return errNoPipelinesInMetadata
	}

	if !e.shouldUpdateStagedMetadatasWithLock(sm) {
		err = e.addUntimedWithLock(currTime, metric)
		e.mtx.RUnlock()
		return err
	}
	e.mtx.RUnlock()

	e.mtx.Lock()
	if e.closed {
		e.mtx.Unlock()
		return errEntryClosed
	}

	if e.shouldUpdateStagedMetadatasWithLock(sm) {
		err := e.updateStagedMetadatasWithLock(metric.ID, metric.Type,
			hasDefaultMetadatas, sm, false)
		if err != nil {
			// NB(xichen): if an error occurred during policy update, the policies
			// will remain as they are, i.e., there are no half-updated policies.
			e.mtx.Unlock()
			return err
		}
		e.metrics.untimed.metadatasUpdates.Inc(1)
	}

	err = e.addUntimedWithLock(currTime, metric)
	e.mtx.Unlock()

	return err
}

// NB(xichen): we assume the metadatas are sorted by their cutover times
// in ascending order.
func (e *Entry) activeStagedMetadataWithLock(
	t time.Time,
	metadatas metadata.StagedMetadatas,
) (metadata.StagedMetadata, error) {
	// If we have no metadata to apply, simply bail.
	if len(metadatas) == 0 {
		e.metrics.untimed.emptyMetadatas.Inc(1)
		return metadata.DefaultStagedMetadata, errEmptyMetadatas
	}
	timeNanos := t.UnixNano()
	for idx := len(metadatas) - 1; idx >= 0; idx-- {
		if metadatas[idx].CutoverNanos <= timeNanos {
			return metadatas[idx], nil
		}
	}
	e.metrics.untimed.noApplicableMetadata.Inc(1)
	return metadata.DefaultStagedMetadata, errNoApplicableMetadata
}

// NB: The metadata passed in is guaranteed to have cut over based on the current time.
func (e *Entry) shouldUpdateStagedMetadatasWithLock(sm metadata.StagedMetadata) bool {
	// If this is a stale metadata, we don't update the existing metadata.
	if e.cutoverNanos > sm.CutoverNanos {
		e.metrics.untimed.staleMetadata.Inc(1)
		return false
	}

	// If this is a newer metadata, we always update.
	if e.cutoverNanos < sm.CutoverNanos {
		return true
	}

	// Iterate over the list of pipelines and check whether we have metadata changes.
	// NB: If the incoming metadata have the same set of aggregation keys as the cached
	// set but also have duplicates, there is no need to update metadatas as long as
	// the cached set has all aggregation keys in the incoming metadata and vice versa.
	bs := bitset.New(uint(len(e.aggregations)))
	for i := range sm.Pipelines {
		storagePolicies := e.storagePolicies(sm.Pipelines[i].StoragePolicies)
		for j := range storagePolicies {
			key := aggregationKey{
				aggregationID:      sm.Pipelines[i].AggregationID,
				storagePolicy:      storagePolicies[j],
				pipeline:           sm.Pipelines[i].Pipeline,
				idPrefixSuffixType: WithPrefixWithSuffix,
			}
			val, idx := e.aggregations.get(key)
			if idx < 0 {
				return true
			}
			if val.resendEnabled != sm.Pipelines[i].ResendEnabled {
				// If resendEnabled has changed force an update of the staged metadata. This won't actually change
				// the aggregations, since the aggregationKeys have not changed. However, it will allow toggling
				// the resendEnabled state on the aggregations.
				return true
			}
			bs.Set(uint(idx))
		}
	}
	return !bs.All(uint(len(e.aggregations)))
}

func (e *Entry) storagePolicies(policies policy.StoragePolicies) policy.StoragePolicies {
	if !policies.IsDefault() {
		return policies
	}
	return e.opts.DefaultStoragePolicies()
}

func (e *Entry) maybeCopyIDWithLock(id metricid.RawID) metricid.RawID {
	// If there are existing elements for this id, try reusing
	// the id from the elements because those are owned by us.
	if len(e.aggregations) > 0 {
		return e.aggregations[0].elem.Value.(metricElem).ID()
	}

	// Otherwise it is necessary to make a copy because it's not owned by us.
	elemID := make(metricid.RawID, len(id))
	copy(elemID, id)
	return elemID
}

// addAggregationKey adds a new aggregation key to the list of new aggregations.
func (e *Entry) addNewAggregationKeyWithLock(
	metricType metric.Type,
	metricID metricid.RawID,
	key aggregationKey,
	listID metricListID,
	newAggregations aggregationValues,
	resendEnabled bool,
) (aggregationValues, error) {
	// Remove duplicate aggregation pipelines.
	if newAggregations.contains(key) {
		return newAggregations, nil
	}
	if idx := e.aggregations.index(key); idx >= 0 {
		// updating staged metadata was triggered, but the aggregation keys did not change. most likely because the
		// resendEnabled state changed, which we update below.
		a := e.aggregations[idx]
		a.resendEnabled = resendEnabled
		newAggregations = append(newAggregations, a)
		return newAggregations, nil
	}
	aggTypes, err := e.decompressor.Decompress(key.aggregationID)
	if err != nil {
		return nil, err
	}
	var newElem metricElem
	switch metricType {
	case metric.CounterType:
		newElem = e.opts.CounterElemPool().Get()
	case metric.TimerType:
		newElem = e.opts.TimerElemPool().Get()
	case metric.GaugeType:
		newElem = e.opts.GaugeElemPool().Get()
	default:
		return nil, errInvalidMetricType
	}
	// NB: The pipeline may not be owned by us and as such we need to make a copy here.
	key.pipeline = key.pipeline.Clone()
	if err = newElem.ResetSetData(ElemData{
		ID:                 metricID,
		StoragePolicy:      key.storagePolicy,
		AggTypes:           aggTypes,
		Pipeline:           key.pipeline,
		NumForwardedTimes:  key.numForwardedTimes,
		IDPrefixSuffixType: key.idPrefixSuffixType,
		ListType:           listID.listType,
	}); err != nil {
		return nil, err
	}
	list, err := e.lists.FindOrCreate(listID)
	if err != nil {
		return nil, err
	}
	newListElem, err := list.PushBack(newElem)
	if err != nil {
		return nil, err
	}
	newAggregations = append(newAggregations, aggregationValue{
		key:           key,
		elem:          newListElem,
		resendEnabled: resendEnabled,
	})
	return newAggregations, nil
}

func (e *Entry) removeOldAggregations(newAggregations aggregationValues) {
	for i := range e.aggregations {
		if !newAggregations.contains(e.aggregations[i].key) {
			e.aggregations[i].elem.Value.(metricElem).MarkAsTombstoned()
		}
	}
}

func (e *Entry) updateStagedMetadatasWithLock(
	metricID metricid.RawID,
	metricType metric.Type,
	hasDefaultMetadatas bool,
	sm metadata.StagedMetadata,
	timed bool,
) error {
	var (
		elemID          = e.maybeCopyIDWithLock(metricID)
		newAggregations = make(aggregationValues, 0, initialAggregationCapacity)
	)

	// Update the metadatas.
	for i := range sm.Pipelines {
		storagePolicies := e.storagePolicies(sm.Pipelines[i].StoragePolicies)
		for j := range storagePolicies {
			key := aggregationKey{
				aggregationID:      sm.Pipelines[i].AggregationID,
				storagePolicy:      storagePolicies[j],
				pipeline:           sm.Pipelines[i].Pipeline,
				idPrefixSuffixType: WithPrefixWithSuffix,
			}
			var (
				resendEnabled = sm.Pipelines[i].ResendEnabled
				listID        metricListID
			)
			if timed {
				listID = timedMetricListID{
					resolution: storagePolicies[j].Resolution().Window,
				}.toMetricListID()
			} else {
				listID = standardMetricListID{
					resolution: storagePolicies[j].Resolution().Window,
				}.toMetricListID()
			}
			var err error
			newAggregations, err = e.addNewAggregationKeyWithLock(metricType, elemID, key, listID, newAggregations,
				resendEnabled)
			if err != nil {
				return err
			}
		}
	}

	// Mark the outdated elements as tombstoned.
	e.removeOldAggregations(newAggregations)

	// Replace the existing aggregations with new aggregations.
	e.aggregations = newAggregations
	e.hasDefaultMetadatas = hasDefaultMetadatas
	e.cutoverNanos = sm.CutoverNanos

	return nil
}

func (e *Entry) addUntimedWithLock(serverTimestamp time.Time, mu unaggregated.MetricUnion) error {
	var err error
	for i := range e.aggregations {
		multierr.AppendInto(&err, e.addUntimedValueWithLock(
			e.aggregations[i], serverTimestamp, mu, e.aggregations[i].resendEnabled, false))
	}
	return err
}

// addUntimedValueWithLock adds the untimed value to the aggregationValue.
// this method handles all the various cases of switching to use a client timestamp if resendEnabled is set for the
// rollup rule.
func (e *Entry) addUntimedValueWithLock(
	aggValue aggregationValue,
	serverTimestamp time.Time,
	mu unaggregated.MetricUnion,
	resendEnabled bool,
	retry bool) error {
	elem := aggValue.elem.Value.(metricElem)
	resolution := aggValue.key.storagePolicy.Resolution().Window
	if resendEnabled && mu.ClientTimeNanos > 0 {
		// Migrate an originally untimed metric (server timestamp) to a "timed" metric (client timestamp) if
		// resendEnabled is set on the rollup rule. Continuing to use untimed allows for a seamless transition since
		// the Entry does not change.
		e.metrics.resendEnabled.Inc(1)
		err := e.checkTimestampForMetric(int64(mu.ClientTimeNanos), e.nowFn().UnixNano(), resolution)
		if err != nil {
			return err
		}
		err = elem.AddUnion(mu.ClientTimeNanos.ToTime(), mu, true)
		if xerrors.Is(err, errClosedBeforeResendEnabledMigration) {
			// this handles a race where the rule was just migrated to resendEnabled. if the client timestamp is
			// delayed, most likely the aggregation has already been closed, since it did not previously have
			// resendEnabled set. continue using the serverTimestamp and this will eventually resolve itself for future
			// aggregations.
			e.metrics.retriedValues.Inc(1)
			return e.addUntimedValueWithLock(aggValue, serverTimestamp, mu, false, false)
		}
		return err
	}
	err := elem.AddUnion(serverTimestamp, mu, false)
	if xerrors.Is(err, errAggregationClosed) && !retry {
		// the aggregation just closed and we lost the race. roll the value into the next aggregation.
		e.metrics.retriedValues.Inc(1)
		return e.addUntimedValueWithLock(aggValue, serverTimestamp.Add(resolution), mu, false, true)
	}
	return err
}

func (e *Entry) addTimed(
	metric aggregated.Metric,
	metadata metadata.TimedMetadata,
	stagedMetadatas metadata.StagedMetadatas,
) error {
	e.timeLock.RLock()
	defer e.timeLock.RUnlock()

	// NB(xichen): it is important that we determine the current time
	// within the time lock. This ensures time ordering by wrapping
	// actions that need to happen before a given time within a read lock,
	// so it is guaranteed that actions before when a write lock is acquired
	// must have all completed. This is used to ensure we never write metrics
	// for times that have already been flushed.
	currTime := e.nowFn()
	e.lastAccessNanos.Store(currTime.UnixNano())

	e.mtx.RLock()
	if e.closed {
		e.mtx.RUnlock()
		return errEntryClosed
	}

	// Only apply processing of staged metadatas if has sent staged metadatas
	// that isn't the default staged metadatas. The default staged metadata
	// would not produce a meaningful aggregation, so we error out in that case.
	hasDefaultMetadatas := stagedMetadatas.IsDefault()
	if len(stagedMetadatas) > 0 {
		if hasDefaultMetadatas {
			e.mtx.RUnlock()
			return errOnlyDefaultStagedMetadata
		} else if stagedMetadatas.IsDropPolicySet() {
			e.mtx.RUnlock()
			return errOnlyDropPolicyStagedMetadata
		}

		sm, err := e.activeStagedMetadataWithLock(currTime, stagedMetadatas)
		if err != nil {
			e.mtx.RUnlock()
			return err
		}

		// If the metadata indicates the (rollup) metric has been tombstoned, the metric is
		// not ingested for aggregation. However, we do not update the policies asssociated
		// with this entry and mark it tombstoned because there may be a different raw metric
		// generating this same (rollup) metric that is actively emitting, meaning this entry
		// may still be very much alive.
		if sm.Tombstoned {
			e.mtx.RUnlock()
			e.metrics.timed.tombstonedMetadata.Inc(1)
			return nil
		}

		// It is expected that there is at least one pipeline in the metadata.
		if len(sm.Pipelines) == 0 {
			e.mtx.RUnlock()
			e.metrics.timed.noPipelinesInMetadata.Inc(1)
			return errNoPipelinesInMetadata
		}

		if !e.shouldUpdateStagedMetadatasWithLock(sm) {
			err = e.addTimedWithStagedMetadatasAndLock(metric)
			e.mtx.RUnlock()
			return err
		}
		e.mtx.RUnlock()

		e.mtx.Lock()
		if e.closed {
			e.mtx.Unlock()
			return errEntryClosed
		}

		if e.shouldUpdateStagedMetadatasWithLock(sm) {
			err := e.updateStagedMetadatasWithLock(metric.ID, metric.Type,
				hasDefaultMetadatas, sm, true)
			if err != nil {
				// NB(xichen): if an error occurred during policy update, the policies
				// will remain as they are, i.e., there are no half-updated policies.
				e.mtx.Unlock()
				return err
			}
			e.metrics.timed.metadatasUpdates.Inc(1)
		}

		err = e.addTimedWithStagedMetadatasAndLock(metric)
		e.mtx.Unlock()

		return err
	}

	// Check if we should update metadata, and add metric if not.
	key := aggregationKey{
		aggregationID:      metadata.AggregationID,
		storagePolicy:      metadata.StoragePolicy,
		idPrefixSuffixType: NoPrefixNoSuffix,
	}
	if idx := e.aggregations.index(key); idx >= 0 {
		err := e.addTimedWithLock(e.aggregations[idx], metric)
		e.mtx.RUnlock()
		return err
	}
	e.mtx.RUnlock()

	e.mtx.Lock()
	if e.closed {
		e.mtx.Unlock()
		return errEntryClosed
	}

	if idx := e.aggregations.index(key); idx >= 0 {
		err := e.addTimedWithLock(e.aggregations[idx], metric)
		e.mtx.Unlock()
		return err
	}

	// Update metadata if not exists, and add metric.
	if err := e.updateTimedMetadataWithLock(metric, metadata); err != nil {
		e.mtx.Unlock()
		return err
	}
	idx := e.aggregations.index(key)
	err := e.addTimedWithLock(e.aggregations[idx], metric)
	e.mtx.Unlock()
	return err
}

// Reject datapoints that arrive too late or too early.
func (e *Entry) checkTimestampForMetric(
	metricTimeNanos int64,
	currNanos int64,
	resolution time.Duration,
) error {
	e.metrics.timed.ingestDelay.RecordDuration(time.Duration(e.nowFn().UnixNano() - metricTimeNanos))
	timedBufferFuture := e.opts.BufferForFutureTimedMetric()
	if metricTimeNanos-currNanos > timedBufferFuture.Nanoseconds() {
		e.metrics.timed.tooFarInTheFuture.Inc(1)
		if !e.opts.VerboseErrors() {
			// Don't return verbose errors if not enabled.
			return errTooFarInTheFuture
		}
		timestamp := time.Unix(0, metricTimeNanos)
		futureLimit := time.Unix(0, currNanos+timedBufferFuture.Nanoseconds())
		err := fmt.Errorf("datapoint for aggregation too far in future: "+
			"off_by=%s, timestamp=%s, future_limit=%s, "+
			"timestamp_unix_nanos=%d, future_limit_unix_nanos=%d",
			timestamp.Sub(futureLimit).String(),
			timestamp.Format(errTimestampFormat),
			futureLimit.Format(errTimestampFormat),
			timestamp.UnixNano(), futureLimit.UnixNano())
		return xerrors.NewRenamedError(errTooFarInTheFuture, err)
	}
	bufferPastFn := e.opts.BufferForPastTimedMetricFn()
	timedBufferPast := bufferPastFn(resolution)
	if currNanos-metricTimeNanos > timedBufferPast.Nanoseconds() {
		e.metrics.timed.tooFarInThePast.Inc(1)
		if !e.opts.VerboseErrors() {
			// Don't return verbose errors if not enabled.
			return errTooFarInThePast
		}
		timestamp := time.Unix(0, metricTimeNanos)
		pastLimit := time.Unix(0, currNanos-timedBufferPast.Nanoseconds())
		err := fmt.Errorf("datapoint for aggregation too far in past: "+
			"off_by=%s, timestamp=%s, past_limit=%s, "+
			"timestamp_unix_nanos=%d, past_limit_unix_nanos=%d",
			pastLimit.Sub(timestamp).String(),
			timestamp.Format(errTimestampFormat),
			pastLimit.Format(errTimestampFormat),
			timestamp.UnixNano(), pastLimit.UnixNano())
		return xerrors.NewRenamedError(errTooFarInThePast, err)
	}
	return nil
}

func (e *Entry) updateTimedMetadataWithLock(
	metric aggregated.Metric,
	metadata metadata.TimedMetadata,
) error {
	var (
		elemID = e.maybeCopyIDWithLock(metric.ID)
		err    error
	)

	// Update the timed metadata.
	key := aggregationKey{
		aggregationID:      metadata.AggregationID,
		storagePolicy:      metadata.StoragePolicy,
		idPrefixSuffixType: NoPrefixNoSuffix,
	}
	listID := timedMetricListID{
		resolution: metadata.StoragePolicy.Resolution().Window,
	}.toMetricListID()
	newAggregations, err := e.addNewAggregationKeyWithLock(metric.Type, elemID, key, listID, e.aggregations, false)
	if err != nil {
		return err
	}

	e.aggregations = newAggregations
	e.metrics.timed.metadataUpdates.Inc(1)
	return nil
}

func (e *Entry) addTimedWithLock(
	value aggregationValue,
	metric aggregated.Metric,
) error {
	timestamp := time.Unix(0, metric.TimeNanos)
	err := e.checkTimestampForMetric(metric.TimeNanos, e.nowFn().UnixNano(),
		value.key.storagePolicy.Resolution().Window)
	if err != nil {
		return err
	}
	return value.elem.Value.(metricElem).AddValue(timestamp, metric.Value, metric.Annotation)
}

func (e *Entry) addTimedWithStagedMetadatasAndLock(metric aggregated.Metric) error {
	var (
		timestamp = time.Unix(0, metric.TimeNanos)
		err       error
	)

	for i := range e.aggregations {
		if multierr.AppendInto(
			&err,
			e.checkTimestampForMetric(
				metric.TimeNanos,
				e.nowFn().UnixNano(),
				e.aggregations[i].key.storagePolicy.Resolution().Window),
		) {
			continue
		}
		multierr.AppendInto(
			&err,
			e.aggregations[i].elem.Value.(metricElem).AddValue(timestamp, metric.Value, metric.Annotation),
		)
	}
	return err
}

func (e *Entry) addForwarded(
	metric aggregated.ForwardedMetric,
	metadata metadata.ForwardMetadata,
) error {
	e.timeLock.RLock()
	defer e.timeLock.RUnlock()

	// NB(xichen): it is important that we determine the current time
	// within the time lock. This ensures time ordering by wrapping
	// actions that need to happen before a given time within a read lock,
	// so it is guaranteed that actions before when a write lock is acquired
	// must have all completed. This is used to ensure we never write metrics
	// for times that have already been flushed.
	currTime := e.nowFn()
	currTimeNanos := currTime.UnixNano()
	e.lastAccessNanos.Store(currTimeNanos)

	e.mtx.RLock()
	if e.closed {
		e.mtx.RUnlock()
		return errEntryClosed
	}

	// Reject datapoints that arrive too late.
	if err := e.checkLatenessForForwardedMetric(
		metric,
		metadata,
		currTimeNanos,
		metadata.StoragePolicy.Resolution().Window,
		metadata.NumForwardedTimes,
	); err != nil {
		e.mtx.RUnlock()
		return err
	}

	// Check if we should update metadata, and add metric if not.
	key := aggregationKey{
		aggregationID:      metadata.AggregationID,
		storagePolicy:      metadata.StoragePolicy,
		pipeline:           metadata.Pipeline,
		numForwardedTimes:  metadata.NumForwardedTimes,
		idPrefixSuffixType: WithPrefixWithSuffix,
	}
	if idx := e.aggregations.index(key); idx >= 0 {
		err := e.addForwardedWithLock(e.aggregations[idx], metric, metadata)
		e.mtx.RUnlock()
		return err
	}
	e.mtx.RUnlock()

	e.mtx.Lock()
	if e.closed {
		e.mtx.Unlock()
		return errEntryClosed
	}

	if idx := e.aggregations.index(key); idx >= 0 {
		err := e.addForwardedWithLock(e.aggregations[idx], metric, metadata)
		e.mtx.Unlock()
		return err
	}

	// Update metatadata if not exists, and add metric.
	if err := e.updateForwardMetadataWithLock(metric, metadata); err != nil {
		e.mtx.Unlock()
		return err
	}
	idx := e.aggregations.index(key)
	err := e.addForwardedWithLock(e.aggregations[idx], metric, metadata)
	e.mtx.Unlock()
	return err
}

func (e *Entry) checkLatenessForForwardedMetric(
	metric aggregated.ForwardedMetric,
	metadata metadata.ForwardMetadata,
	currNanos int64,
	resolution time.Duration,
	numForwardedTimes int,
) error {
	metricTimeNanos := metric.TimeNanos
	maxAllowedForwardingDelayFn := e.opts.MaxAllowedForwardingDelayFn()
	maxLatenessAllowed := maxAllowedForwardingDelayFn(resolution, numForwardedTimes)
	if metadata.ResendEnabled {
		maxLatenessAllowed = e.opts.BufferForPastTimedMetricFn()(resolution)
	}
	if currNanos-metricTimeNanos <= maxLatenessAllowed.Nanoseconds() {
		return nil
	}

	e.metrics.forwarded.arrivedTooLate.Inc(1)

	if !e.opts.VerboseErrors() {
		// Don't return verbose errors if not enabled.
		return errArrivedTooLate
	}

	timestamp := time.Unix(0, metricTimeNanos)
	pastLimit := time.Unix(0, currNanos-maxLatenessAllowed.Nanoseconds())
	err := fmt.Errorf("datapoint for aggregation forwarded too late: "+
		"id=%s, off_by=%s, timestamp=%s, past_limit=%s, "+
		"timestamp_unix_nanos=%d, past_limit_unix_nanos=%d",
		metric.ID, maxLatenessAllowed.String(),
		timestamp.Format(errTimestampFormat),
		pastLimit.Format(errTimestampFormat),
		timestamp.UnixNano(), pastLimit.UnixNano())
	return xerrors.NewRenamedError(errArrivedTooLate, err)
}

func (e *Entry) updateForwardMetadataWithLock(
	metric aggregated.ForwardedMetric,
	metadata metadata.ForwardMetadata,
) error {
	var (
		elemID = e.maybeCopyIDWithLock(metric.ID)
		err    error
	)

	// Update the forward metadata.
	key := aggregationKey{
		aggregationID:      metadata.AggregationID,
		storagePolicy:      metadata.StoragePolicy,
		pipeline:           metadata.Pipeline,
		numForwardedTimes:  metadata.NumForwardedTimes,
		idPrefixSuffixType: WithPrefixWithSuffix,
	}
	listID := forwardedMetricListID{
		resolution:        metadata.StoragePolicy.Resolution().Window,
		numForwardedTimes: metadata.NumForwardedTimes,
	}.toMetricListID()
	newAggregations, err := e.addNewAggregationKeyWithLock(metric.Type, elemID, key, listID, e.aggregations,
		metadata.ResendEnabled)
	if err != nil {
		return err
	}

	e.aggregations = newAggregations
	e.metrics.forwarded.metadataUpdates.Inc(1)
	return nil
}

func (e *Entry) addForwardedWithLock(
	value aggregationValue,
	metric aggregated.ForwardedMetric,
	metadata metadata.ForwardMetadata,
) error {
	timestamp := time.Unix(0, metric.TimeNanos)
	err := value.elem.Value.(metricElem).AddUnique(timestamp, metric, metadata)
	if err == errDuplicateForwardingSource {
		// Duplicate forwarding sources may occur during a leader re-election and is not
		// considered an external facing error. Hence, we record it and move on.
		e.metrics.forwarded.duplicateSources.Inc(1)
		return nil
	}
	return err
}

func (e *Entry) shouldExpire(
	now xtime.UnixNano,
	metricCategory metricCategory,
	recordTiming bool,
) bool {
	// Only expire the entry if there are no active writers
	// and it has reached its ttl since last accessed.
	age := now.Sub(xtime.UnixNano(e.lastAccessNanos.Load()))
	if recordTiming {
		e.metrics.entryExpiryByCategory[metricCategory].RecordDuration(age)
	}

	return e.numWriters.Load() == 0 && age > e.opts.EntryTTL()
}

func (e *Entry) resetRateLimiterWithLock(runtimeOpts runtime.Options) {
	newLimit := runtimeOpts.WriteValuesPerMetricLimitPerSecond()
	e.rateLimiter.Reset(newLimit)
}

func (e *Entry) applyValueRateLimit(numValues int64, m rateLimitEntryMetrics) error {
	rateLimiter := e.rateLimiter

	if rateLimiter.IsAllowed(numValues, xtime.ToUnixNano(e.nowFn())) {
		return nil
	}
	m.valueRateLimitExceeded.Inc(1)
	m.droppedValues.Inc(numValues)
	return errWriteValueRateLimitExceeded
}

type aggregationValue struct {
	elem *list.Element
	key  aggregationKey

	// mutable data that is not part of the aggregationKey.
	// This allows changing the resendEnabled bit without creating a new aggregation. This allows seamlessly
	// transitioning to resendEnabled without a gap in the aggregation.
	resendEnabled bool
}

// TODO(xichen): benchmark the performance of using a single slice
// versus a map with a partial key versus a map with a hash of full key.
type aggregationValues []aggregationValue

// returns the aggregation value and the index position. if not found, the index position is -1.
func (vals aggregationValues) get(k aggregationKey) (aggregationValue, int) {
	// keep in sync with aggregationKey.Equal()
	// this is >2x slower if not inlined manually.
	for i := range vals {
		if vals[i].key.aggregationID == k.aggregationID &&
			vals[i].key.storagePolicy == k.storagePolicy &&
			vals[i].key.pipeline.Equal(k.pipeline) &&
			vals[i].key.numForwardedTimes == k.numForwardedTimes &&
			vals[i].key.idPrefixSuffixType == k.idPrefixSuffixType {
			return vals[i], i
		}
	}
	return aggregationValue{}, -1
}

func (vals aggregationValues) index(k aggregationKey) int {
	_, i := vals.get(k)
	return i
}

func (vals aggregationValues) contains(k aggregationKey) bool {
	return vals.index(k) != -1
}
