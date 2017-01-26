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
)

var (
	errEntryClosed = errors.New("entry is closed")
)

// Entry stores metadata about current policies and aggregations
// for a metric. Note since the metric namespace is part of the
// metric ID, we only need to keep track of the policy version
// to detect policy changes
type Entry struct {
	sync.RWMutex

	opts           Options                         // options
	lists          *MetricLists                    // metric lists
	version        int                             // current policy version
	numWriters     int32                           // number of writers writing to this entry
	lastAccessInNs int64                           // last access time
	aggregations   map[policy.Policy]*list.Element // aggregations at different resolutions
	closed         bool                            // whether the entry is closed
}

// NewEntry creates a new entry
func NewEntry(lists *MetricLists, opts Options) *Entry {
	e := &Entry{
		opts:         opts,
		aggregations: make(map[policy.Policy]*list.Element),
	}
	e.ResetSetData(lists)
	return e
}

// IncWriter increases the writer count
func (e *Entry) IncWriter() { atomic.AddInt32(&e.numWriters, 1) }

// DecWriter decreases the writer count
func (e *Entry) DecWriter() { atomic.AddInt32(&e.numWriters, -1) }

// ResetSetData resets the entry and sets initial data
func (e *Entry) ResetSetData(lists *MetricLists) {
	e.closed = false
	e.lists = lists
	e.version = policy.InitPolicyVersion
	e.numWriters = 0
	e.recordLastAccessed(e.opts.ClockOptions().NowFn()())
}

// AddMetricWithPolicies adds a metric along with applicable policies
func (e *Entry) AddMetricWithPolicies(
	mu unaggregated.MetricUnion,
	policies policy.VersionedPolicies,
) error {
	// NB(xichen): it is important that we determine the current time
	// within the time lock. This ensures time ordering by wrapping
	// actions that need to happen before a given time within a read lock,
	// so it is guaranteed that actions before when a write lock is acquired
	// must have all completed. This is used to ensure we never write metrics
	// for times that have already been flushed.
	timeLock := e.opts.TimeLock()
	timeLock.RLock()
	defer timeLock.RUnlock()

	e.RLock()
	if e.closed {
		e.RUnlock()
		return errEntryClosed
	}

	currTime := e.opts.ClockOptions().NowFn()()
	e.recordLastAccessed(currTime)

	if !e.shouldUpdatePoliciesWithLock(currTime, policies.Version, policies.Cutover) {
		e.addMetricWithLock(currTime, mu)
		e.RUnlock()
		return nil
	}
	e.RUnlock()

	e.Lock()
	if e.closed {
		e.Unlock()
		return errEntryClosed
	}

	if e.shouldUpdatePoliciesWithLock(currTime, policies.Version, policies.Cutover) {
		if err := e.updatePoliciesWithLock(mu.Type, mu.ID, policies.Version, policies.Policies); err != nil {
			// NB(xichen): if an error occurred during policy update, the policies
			// will remain as they are, i.e., there are no half-updated policies
			e.Unlock()
			return err
		}
	}

	e.addMetricWithLock(currTime, mu)
	e.Unlock()

	return nil
}

// ShouldExpire returns whether the entry should expire
func (e *Entry) ShouldExpire(now time.Time) bool {
	e.RLock()
	if e.closed {
		e.RUnlock()
		return false
	}
	// Only expire the entry if there are no active writers
	// and it has reached its ttl since last accessed
	shouldExpire := e.writerCount() == 0 && now.After(e.lastAccessed().Add(e.opts.EntryTTL()))
	e.RUnlock()
	return shouldExpire
}

// Expire expires the entry
func (e *Entry) Expire() {
	e.Lock()
	if e.closed {
		e.Unlock()
		return
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
}

func (e *Entry) writerCount() int        { return int(atomic.LoadInt32(&e.numWriters)) }
func (e *Entry) lastAccessed() time.Time { return time.Unix(0, atomic.LoadInt64(&e.lastAccessInNs)) }

func (e *Entry) recordLastAccessed(currTime time.Time) {
	atomic.StoreInt64(&e.lastAccessInNs, currTime.UnixNano())
}

func (e *Entry) addMetricWithLock(timestamp time.Time, mu unaggregated.MetricUnion) {
	for _, elem := range e.aggregations {
		elem.Value.(metricElem).AddMetric(timestamp, mu)
	}
}

// If the current version is older than the incoming version,
// and we've surpassed the cutover, we should update the policies
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
	newVersion int,
	policies []policy.Policy,
) error {
	// We should update the policies first
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
			newElem.ResetSetData(id, policy)
			list, err := e.lists.FindOrCreate(policy.Resolution.Window)
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

	// Mark the outdated elements as tombstoned
	for policy, elem := range e.aggregations {
		if _, exists := newAggregations[policy]; !exists {
			elem.Value.(metricElem).MarkAsTombstoned()
		}
	}

	// Replace the existing aggregations with new aggregations
	e.aggregations = newAggregations
	e.version = newVersion

	return nil
}
