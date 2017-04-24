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

	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/errors"
)

var (
	errEntryClosed = errors.New("entry is closed")
)

// Entry stores metadata about current policies and aggregations
// for a metric. Note since the metric namespace is part of the
// metric ID, we only need to keep track of the policy version
// to detect policy changes.
type Entry struct {
	sync.RWMutex

	opts           Options                         // options
	lists          *metricLists                    // metric lists
	version        int                             // current policy version
	numWriters     int32                           // number of writers writing to this entry
	lastAccessInNs int64                           // last access time
	aggregations   map[policy.Policy]*list.Element // aggregations at different resolutions
	closed         bool                            // whether the entry is closed
}

// NewEntry creates a new entry
func NewEntry(lists *metricLists, opts Options) *Entry {
	e := &Entry{
		opts:         opts,
		aggregations: make(map[policy.Policy]*list.Element),
	}
	e.ResetSetData(lists)
	return e
}

// IncWriter increases the writer count.
func (e *Entry) IncWriter() { atomic.AddInt32(&e.numWriters, 1) }

// DecWriter decreases the writer count.
func (e *Entry) DecWriter() { atomic.AddInt32(&e.numWriters, -1) }

// ResetSetData resets the entry and sets initial data.
func (e *Entry) ResetSetData(lists *metricLists) {
	e.closed = false
	e.lists = lists
	e.version = policy.InitPolicyVersion
	e.numWriters = 0
	e.recordLastAccessed(e.opts.ClockOptions().NowFn()())
}

// AddMetricWithPolicies adds a metric along with applicable policies.
func (e *Entry) AddMetricWithPolicies(
	mu unaggregated.MetricUnion,
	policies policy.VersionedPolicies,
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

	if !e.shouldUpdatePoliciesWithLock(currTime, policies.Version, policies.Cutover) {
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

	if e.shouldUpdatePoliciesWithLock(currTime, policies.Version, policies.Cutover) {
		if err := e.updatePoliciesWithLock(mu.Type, mu.ID, mu.OwnsID, policies.Version, policies.Policies()); err != nil {
			// NB(xichen): if an error occurred during policy update, the policies
			// will remain as they are, i.e., there are no half-updated policies.
			e.Unlock()
			timeLock.RUnlock()
			return err
		}
	}

	err := e.addMetricWithLock(currTime, mu)
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
func (e *Entry) lastAccessed() time.Time { return time.Unix(0, atomic.LoadInt64(&e.lastAccessInNs)) }

func (e *Entry) recordLastAccessed(currTime time.Time) {
	atomic.StoreInt64(&e.lastAccessInNs, currTime.UnixNano())
}

func (e *Entry) shouldExpire(now time.Time) bool {
	// Only expire the entry if there are no active writers
	// and it has reached its ttl since last accessed.
	return e.writerCount() == 0 && now.After(e.lastAccessed().Add(e.opts.EntryTTL()))
}

func (e *Entry) addMetricWithLock(timestamp time.Time, mu unaggregated.MetricUnion) error {
	multiErr := xerrors.NewMultiError()
	for _, elem := range e.aggregations {
		if err := elem.Value.(metricElem).AddMetric(timestamp, mu); err != nil {
			multiErr = multiErr.Add(err)
		}
	}
	// If the timer values were allocated from a pool, return them to the pool.
	if mu.Type == unaggregated.BatchTimerType && mu.BatchTimerVal != nil && mu.TimerValPool != nil {
		mu.TimerValPool.Put(mu.BatchTimerVal)
	}
	return multiErr.FinalError()
}

// If the current version is older than the incoming version,
// and we've surpassed the cutover, we should update the policies.
func (e *Entry) shouldUpdatePoliciesWithLock(
	currTime time.Time,
	version int,
	cutover time.Time,
) bool {
	return e.version == policy.InitPolicyVersion || (e.version < version && !currTime.Before(cutover))
}

func (e *Entry) updatePoliciesWithLock(
	typ unaggregated.Type,
	id metric.ID,
	ownsID bool,
	newVersion int,
	policies []policy.Policy,
) error {
	// Fast path to exit in case the policies didn't change.
	if !e.hasPolicyChangesWithLock(policies) {
		e.version = newVersion
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
			elemID = make(metric.ID, len(id))
			copy(elemID, id)
		}
	}

	// We should update the policies first.
	newAggregations := make(map[policy.Policy]*list.Element, len(policies))
	for _, policy := range policies {
		if elem, exists := e.aggregations[policy]; exists {
			newAggregations[policy] = elem
		} else {
			var newElem metricElem
			switch typ {
			case unaggregated.CounterType:
				newElem = e.opts.CounterElemPool().Get()
			case unaggregated.BatchTimerType:
				newElem = e.opts.TimerElemPool().Get()
			case unaggregated.GaugeType:
				newElem = e.opts.GaugeElemPool().Get()
			default:
				return fmt.Errorf("unrecognized element type:%v", typ)
			}
			newElem.ResetSetData(elemID, policy)
			list, err := e.lists.FindOrCreate(policy.Resolution().Window)
			if err != nil {
				return err
			}
			newListElem, err := list.PushBack(newElem)
			if err != nil {
				return err
			}
			newAggregations[policy] = newListElem
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
	e.version = newVersion

	return nil
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
