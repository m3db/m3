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
	"sync/atomic"
	"time"

	metricid "github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	xerrors "github.com/m3db/m3x/errors"

	"github.com/uber-go/tally"
)

var (
	errEmptyPoliciesList = errors.New("empty policies list")
	errEntryClosed       = errors.New("entry is closed")
)

type entryMetrics struct {
	emptyPoliciesList tally.Counter
	stalePolicy       tally.Counter
	futurePolicy      tally.Counter
	tombstonedPolicy  tally.Counter
	policyUpdates     tally.Counter
}

func newEntryMetrics(scope tally.Scope) entryMetrics {
	return entryMetrics{
		emptyPoliciesList: scope.Counter("empty-policies-list"),
		stalePolicy:       scope.Counter("stale-policy"),
		futurePolicy:      scope.Counter("future-policy"),
		tombstonedPolicy:  scope.Counter("tombstoned-policy"),
		policyUpdates:     scope.Counter("policy-updates"),
	}
}

// Entry stores metadata about current policies and aggregations for a metric.
type Entry struct {
	sync.RWMutex

	closed                 bool
	opts                   Options
	hasDefaultPoliciesList bool
	useDefaultPolicies     bool
	cutoverNanos           int64
	lists                  *metricLists
	numWriters             int32
	lastAccessNanos        int64
	aggregations           map[policy.Policy]*list.Element
	metrics                entryMetrics
	// The entry keeps a decompressor to reuse the bitset in it, so we can
	// save some heap allocations.
	decompressor policy.AggregationIDDecompressor
}

// NewEntry creates a new entry.
func NewEntry(lists *metricLists, opts Options) *Entry {
	scope := opts.InstrumentOptions().MetricsScope().SubScope("entry")
	e := &Entry{
		aggregations: make(map[policy.Policy]*list.Element),
		metrics:      newEntryMetrics(scope),
		decompressor: policy.NewPooledAggregationIDDecompressor(opts.AggregationTypesOptions().AggregationTypesPool()),
	}
	e.ResetSetData(lists, opts)
	return e
}

// IncWriter increases the writer count.
func (e *Entry) IncWriter() { atomic.AddInt32(&e.numWriters, 1) }

// DecWriter decreases the writer count.
func (e *Entry) DecWriter() { atomic.AddInt32(&e.numWriters, -1) }

// ResetSetData resets the entry and sets initial data.
// NB(xichen): we need to reset the options here to use the correct
// time lock contained in the options.
func (e *Entry) ResetSetData(lists *metricLists, opts Options) {
	e.closed = false
	e.opts = opts
	e.hasDefaultPoliciesList = false
	e.useDefaultPolicies = false
	e.cutoverNanos = uninitializedCutoverNanos
	e.lists = lists
	e.numWriters = 0
	e.recordLastAccessed(e.opts.ClockOptions().NowFn()())
}

// AddMetricWithPoliciesList adds a metric along with applicable policies list.
func (e *Entry) AddMetricWithPoliciesList(
	mu unaggregated.MetricUnion,
	pl policy.PoliciesList,
) error {
	switch mu.Type {
	case unaggregated.BatchTimerType:
		err := e.writeBatchTimerWithPoliciesList(mu, pl)
		if mu.BatchTimerVal != nil && mu.TimerValPool != nil {
			mu.TimerValPool.Put(mu.BatchTimerVal)
		}
		return err
	default:
		return e.addMetricWithPoliciesList(mu, pl)
	}
}

func (e *Entry) writeBatchTimerWithPoliciesList(
	mu unaggregated.MetricUnion,
	pl policy.PoliciesList,
) error {
	// If there is no limit on the maximum batch size per write, write
	// all timers at once.
	maxTimerBatchSizePerWrite := e.opts.MaxTimerBatchSizePerWrite()
	if maxTimerBatchSizePerWrite == 0 {
		return e.addMetricWithPoliciesList(mu, pl)
	}

	// Otherwise, honor maximum timer batch size.
	var (
		timerValues    = mu.BatchTimerVal
		numTimerValues = len(timerValues)
		start, end     int
	)
	for start = 0; start < numTimerValues; start = end {
		end = start + maxTimerBatchSizePerWrite
		if end > numTimerValues {
			end = numTimerValues
		}
		splitTimer := mu
		splitTimer.BatchTimerVal = timerValues[start:end]
		if err := e.addMetricWithPoliciesList(splitTimer, pl); err != nil {
			return err
		}
	}
	return nil
}

func (e *Entry) addMetricWithPoliciesList(
	mu unaggregated.MetricUnion,
	pl policy.PoliciesList,
) error {
	timeLock := e.opts.TimeLock()
	timeLock.RLock()

	// NB(xichen): it is important that we determine the current time
	// within the time lock. This ensures time ordering by wrapping
	// actions that need to happen before a given time within a read lock,
	// so it is guaranteed that actions before when a write lock is acquired
	// must have all completed. This is used to ensure we never write metrics
	// for times that have already been flushed.
	currTime := e.opts.ClockOptions().NowFn()()
	e.recordLastAccessed(currTime)

	e.RLock()
	if e.closed {
		e.RUnlock()
		timeLock.RUnlock()
		return errEntryClosed
	}

	// Fast exit path for the common case.
	hasDefaultPoliciesList := pl.IsDefault()
	if e.hasDefaultPoliciesList && hasDefaultPoliciesList {
		err := e.addMetricWithLock(currTime, mu)
		e.RUnlock()
		timeLock.RUnlock()
		return err
	}

	sp, err := e.activeStagedPoliciesWithLock(pl, currTime)
	if err != nil {
		e.RUnlock()
		timeLock.RUnlock()
		return err
	}

	// If the policy indicates the (rollup) metric has been tombstoned, the metric is
	// not ingested for aggregation. However, we do not update the policies asssociated
	// with this entry and mark it tombstoned because there may be a different raw metric
	// generating this same (rollup) metric that is actively emitting, meaning this entry
	// may still be very much alive.
	if sp.Tombstoned {
		e.RUnlock()
		timeLock.RUnlock()
		e.metrics.tombstonedPolicy.Inc(1)
		return nil
	}

	if !e.shouldUpdatePoliciesWithLock(currTime, sp) {
		err := e.addMetricWithLock(currTime, mu)
		e.RUnlock()
		timeLock.RUnlock()
		return err
	}
	e.RUnlock()

	e.Lock()
	if e.closed {
		e.Unlock()
		timeLock.RUnlock()
		return errEntryClosed
	}

	if e.shouldUpdatePoliciesWithLock(currTime, sp) {
		if err := e.updatePoliciesWithLock(mu.Type, mu.ID, mu.OwnsID, hasDefaultPoliciesList, sp); err != nil {
			// NB(xichen): if an error occurred during policy update, the policies
			// will remain as they are, i.e., there are no half-updated policies.
			e.Unlock()
			timeLock.RUnlock()
			return err
		}
	}

	err = e.addMetricWithLock(currTime, mu)
	e.Unlock()
	timeLock.RUnlock()

	return err
}

// ShouldExpire returns whether the entry should expire.
func (e *Entry) ShouldExpire(now time.Time) bool {
	e.RLock()
	if e.closed {
		e.RUnlock()
		return false
	}
	e.RUnlock()

	return e.shouldExpire(now)
}

// TryExpire attempts to expire the entry, returning true
// if the entry is expired, and false otherwise.
func (e *Entry) TryExpire(now time.Time) bool {
	e.Lock()
	if e.closed {
		e.Unlock()
		return false
	}
	if !e.shouldExpire(now) {
		e.Unlock()
		return false
	}
	e.closed = true
	for p, agg := range e.aggregations {
		agg.Value.(metricElem).MarkAsTombstoned()
		delete(e.aggregations, p)
	}
	e.lists = nil
	pool := e.opts.EntryPool()
	e.Unlock()

	pool.Put(e)
	return true
}

func (e *Entry) writerCount() int        { return int(atomic.LoadInt32(&e.numWriters)) }
func (e *Entry) lastAccessed() time.Time { return time.Unix(0, atomic.LoadInt64(&e.lastAccessNanos)) }

func (e *Entry) recordLastAccessed(currTime time.Time) {
	atomic.StoreInt64(&e.lastAccessNanos, currTime.UnixNano())
}

// NB(xichen): we assume the policies are sorted by their cutover times
// in ascending order.
func (e *Entry) activeStagedPoliciesWithLock(
	pl policy.PoliciesList,
	t time.Time,
) (policy.StagedPolicies, error) {
	// If we have no policy to apply, simply bail.
	if len(pl) == 0 {
		e.metrics.emptyPoliciesList.Inc(1)
		return policy.DefaultStagedPolicies, errEmptyPoliciesList
	}
	timeNanos := t.UnixNano()
	for idx := len(pl) - 1; idx >= 0; idx-- {
		if pl[idx].CutoverNanos <= timeNanos {
			return pl[idx], nil
		}
	}
	return pl[0], nil
}

func (e *Entry) shouldUpdatePoliciesWithLock(currTime time.Time, sp policy.StagedPolicies) bool {
	if e.cutoverNanos == uninitializedCutoverNanos {
		return true
	}
	// If this is a future policy, we don't update the existing policy
	// and instead use the cached policy.
	if currTime.UnixNano() < sp.CutoverNanos {
		e.metrics.futurePolicy.Inc(1)
		return false
	}
	// If this is a stale policy, we don't update the existing policy
	// and instead use the cached policy.
	if sp.CutoverNanos < e.cutoverNanos {
		e.metrics.stalePolicy.Inc(1)
		return false
	}
	if e.cutoverNanos != sp.CutoverNanos {
		return true
	}
	policies, useDefaultPolicies := sp.Policies()
	if e.useDefaultPolicies && useDefaultPolicies {
		return false
	}
	if useDefaultPolicies {
		policies = e.opts.DefaultPolicies()
	}
	return e.hasPolicyChangesWithLock(policies)
}

func (e *Entry) hasPolicyChangesWithLock(newPolicies []policy.Policy) bool {
	if len(e.aggregations) != len(newPolicies) {
		return true
	}
	for _, policy := range newPolicies {
		if _, exists := e.aggregations[policy]; !exists {
			return true
		}
	}
	return false
}

func (e *Entry) updatePoliciesWithLock(
	typ unaggregated.Type,
	id metricid.RawID,
	ownsID bool,
	hasDefaultPoliciesList bool,
	sp policy.StagedPolicies,
) error {
	policies, useDefaultPolicies := sp.Policies()
	if useDefaultPolicies {
		policies = e.opts.DefaultPolicies()
	}

	// Fast path to exit in case the policies didn't change.
	if !e.hasPolicyChangesWithLock(policies) {
		e.hasDefaultPoliciesList = hasDefaultPoliciesList
		e.useDefaultPolicies = useDefaultPolicies
		e.cutoverNanos = sp.CutoverNanos
		e.metrics.policyUpdates.Inc(1)
		return nil
	}

	elemID := id
	if !ownsID {
		if len(e.aggregations) > 0 {
			// If there are existing elements for this id, try reusing
			// the id from the elements because those are owned by us.
			for _, elem := range e.aggregations {
				elemID = elem.Value.(metricElem).ID()
				break
			}
		} else {
			// Otherwise this is a new id so it is necessary to make a
			// copy because it's not owned by us.
			elemID = make(metricid.RawID, len(id))
			copy(elemID, id)
		}
	}

	// We should update the policies first.
	newAggregations := make(map[policy.Policy]*list.Element, len(policies))
	for _, p := range policies {
		if elem, exists := e.aggregations[p]; exists {
			newAggregations[p] = elem
		} else {
			aggTypes, err := e.decompressor.Decompress(p.AggregationID)
			if err != nil {
				return err
			}

			var newElem metricElem
			switch typ {
			case unaggregated.CounterType:
				if !aggTypes.IsValidForCounter() {
					return fmt.Errorf("invalid aggregation types %s for Counter", aggTypes.String())
				}
				newElem = e.opts.CounterElemPool().Get()
			case unaggregated.BatchTimerType:
				if !aggTypes.IsValidForTimer() {
					return fmt.Errorf("invalid aggregation types %s for Timer", aggTypes.String())
				}
				newElem = e.opts.TimerElemPool().Get()
			case unaggregated.GaugeType:
				if !aggTypes.IsValidForGauge() {
					return fmt.Errorf("invalid aggregation types %s for Gauge", aggTypes.String())
				}
				newElem = e.opts.GaugeElemPool().Get()
			default:
				return errInvalidMetricType
			}
			newElem.ResetSetData(elemID, p.StoragePolicy, aggTypes)
			list, err := e.lists.FindOrCreate(p.Resolution().Window)
			if err != nil {
				return err
			}
			newListElem, err := list.PushBack(newElem)
			if err != nil {
				return err
			}
			newAggregations[p] = newListElem
		}
	}

	// Mark the outdated elements as tombstoned.
	for policy, elem := range e.aggregations {
		if _, exists := newAggregations[policy]; !exists {
			elem.Value.(metricElem).MarkAsTombstoned()
		}
	}

	// Replace the existing aggregations with new aggregations.
	e.aggregations = newAggregations
	e.hasDefaultPoliciesList = hasDefaultPoliciesList
	e.useDefaultPolicies = useDefaultPolicies
	e.cutoverNanos = sp.CutoverNanos
	e.metrics.policyUpdates.Inc(1)

	return nil
}

func (e *Entry) addMetricWithLock(timestamp time.Time, mu unaggregated.MetricUnion) error {
	multiErr := xerrors.NewMultiError()
	for _, elem := range e.aggregations {
		if err := elem.Value.(metricElem).AddMetric(timestamp, mu); err != nil {
			multiErr = multiErr.Add(err)
		}
	}
	return multiErr.FinalError()
}

func (e *Entry) shouldExpire(now time.Time) bool {
	// Only expire the entry if there are no active writers
	// and it has reached its ttl since last accessed.
	return e.writerCount() == 0 && now.After(e.lastAccessed().Add(e.opts.EntryTTL()))
}
