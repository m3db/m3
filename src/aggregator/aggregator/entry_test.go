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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	testPoliciesVersion = 2
	testPolicies        = []policy.Policy{
		{
			Resolution: policy.Resolution{Window: 10 * time.Second, Precision: xtime.Second},
			Retention:  policy.Retention(6 * time.Hour),
		},
		{
			Resolution: policy.Resolution{Window: time.Minute, Precision: xtime.Minute},
			Retention:  policy.Retention(2 * 24 * time.Hour),
		},
		{
			Resolution: policy.Resolution{Window: 10 * time.Minute, Precision: xtime.Minute},
			Retention:  policy.Retention(30 * 24 * time.Hour),
		},
	}
	testNewPolicies = []policy.Policy{
		{
			Resolution: policy.Resolution{Window: 10 * time.Second, Precision: xtime.Second},
			Retention:  policy.Retention(6 * time.Hour),
		},
		{
			Resolution: policy.Resolution{Window: time.Minute, Precision: xtime.Minute},
			Retention:  policy.Retention(7 * 24 * time.Hour),
		},
		{
			Resolution: policy.Resolution{Window: 5 * time.Minute, Precision: xtime.Minute},
			Retention:  policy.Retention(7 * 24 * time.Hour),
		},
	}
)

type testPreProcessFn func(e *Entry)
type testElemValidateFn func(t *testing.T, elem *list.Element, alignedStart time.Time)
type testPostProcessFn func(t *testing.T)

type testEntryData struct {
	mu unaggregated.MetricUnion
	fn testElemValidateFn
}

func testEntry() (*Entry, *metricLists, time.Time) {
	now := time.Now()
	clockOpts := clock.NewOptions().SetNowFn(func() time.Time {
		return now
	})
	opts := testOptions().
		SetClockOptions(clockOpts).
		SetMinFlushInterval(0)

	lists := newMetricLists(opts)
	// This effectively disable flushing
	lists.newMetricListFn = func(res time.Duration, opts Options) *metricList {
		return newMetricList(0, opts)
	}

	e := NewEntry(nil, opts)
	e.ResetSetData(lists)

	return e, lists, now
}

func populateTestAggregations(
	t *testing.T,
	e *Entry,
	typ unaggregated.Type,
) {
	for _, policy := range testPolicies {
		var newElem metricElem
		switch typ {
		case unaggregated.CounterType:
			newElem = e.opts.CounterElemPool().Get()
		case unaggregated.BatchTimerType:
			newElem = e.opts.TimerElemPool().Get()
		case unaggregated.GaugeType:
			newElem = e.opts.GaugeElemPool().Get()
		default:
			require.Fail(t, fmt.Sprintf("unrecognized metric type: %v", typ))
		}
		newElem.ResetSetData(testID, policy)
		list, err := e.lists.FindOrCreate(policy.Resolution.Window)
		require.NoError(t, err)
		newListElem, err := list.PushBack(newElem)
		require.NoError(t, err)
		e.aggregations[policy] = newListElem
	}
}

func TestEntryIncDecWriter(t *testing.T) {
	e := NewEntry(nil, testOptions())
	require.Equal(t, int32(0), e.numWriters)

	var (
		numWriters = 10
		wg         sync.WaitGroup
	)

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func() {
			e.IncWriter()
			wg.Done()
		}()
	}
	wg.Wait()
	require.Equal(t, int32(numWriters), e.numWriters)

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func() {
			e.DecWriter()
			wg.Done()
		}()
	}
	wg.Wait()
	require.Equal(t, int32(0), e.numWriters)
}

func TestEntryResetSetData(t *testing.T) {
	e, lists, now := testEntry()

	require.False(t, e.closed)
	require.Equal(t, lists, e.lists)
	require.Equal(t, policy.InitPolicyVersion, e.version)
	require.Equal(t, int32(0), e.numWriters)
	require.Equal(t, now.UnixNano(), e.lastAccessInNs)
}

func checkElemTombstoned(t *testing.T, elem metricElem, deleted map[policy.Policy]struct{}) {
	switch elem := elem.(type) {
	case *CounterElem:
		if _, exists := deleted[elem.policy]; exists {
			require.True(t, elem.tombstoned)
		} else {
			require.False(t, elem.tombstoned)
		}
	case *TimerElem:
		if _, exists := deleted[elem.policy]; exists {
			require.True(t, elem.tombstoned)
		} else {
			require.False(t, elem.tombstoned)
		}
	case *GaugeElem:
		if _, exists := deleted[elem.policy]; exists {
			require.True(t, elem.tombstoned)
		} else {
			require.False(t, elem.tombstoned)
		}
	default:
		require.Fail(t, fmt.Sprintf("unexpected elem type %T", elem))
	}
}

func testEntryAddMetricWithPolicies(
	t *testing.T,
	newPoliciesVersion int,
	preAddFn testPreProcessFn,
	postAddFn testPostProcessFn,
	expectedPolicies []policy.Policy,
) {
	inputs := []testEntryData{
		{
			mu: testCounter,
			fn: func(t *testing.T, elem *list.Element, alignedStart time.Time) {
				aggregations := elem.Value.(*CounterElem).values
				require.Equal(t, 1, len(aggregations))
				require.Equal(t, alignedStart.UnixNano(), aggregations[0].timeNs)
				require.Equal(t, int64(1234), aggregations[0].counter.Sum())
			},
		},
		{
			mu: testBatchTimer,
			fn: func(t *testing.T, elem *list.Element, alignedStart time.Time) {
				aggregations := elem.Value.(*TimerElem).values
				require.Equal(t, 1, len(aggregations))
				require.Equal(t, alignedStart.UnixNano(), aggregations[0].timeNs)
				require.Equal(t, 18.0, aggregations[0].timer.Sum())
			},
		},
		{
			mu: testGauge,
			fn: func(t *testing.T, elem *list.Element, alignedStart time.Time) {
				aggregations := elem.Value.(*GaugeElem).values
				require.Equal(t, 1, len(aggregations))
				require.Equal(t, alignedStart.UnixNano(), aggregations[0].timeNs)
				require.Equal(t, 123.456, aggregations[0].gauge.Value())
			},
		},
	}

	for _, input := range inputs {
		e, _, now := testEntry()
		e.version = testPoliciesVersion
		populateTestAggregations(t, e, input.mu.Type)

		preAddFn(e)

		require.NoError(t, e.AddMetricWithPolicies(
			input.mu,
			policy.CustomVersionedPolicies(
				newPoliciesVersion,
				now.Add(-time.Second),
				expectedPolicies,
			),
		))

		require.Equal(t, now.UnixNano(), e.lastAccessInNs)
		require.Equal(t, len(expectedPolicies), len(e.aggregations))
		for _, p := range expectedPolicies {
			elem, exists := e.aggregations[p]
			require.True(t, exists)
			input.fn(t, elem, now.Truncate(p.Resolution.Window))
		}
		require.Equal(t, newPoliciesVersion, e.version)

		postAddFn(t)
	}
}

func TestEntryAddMetricWithPoliciesNoPolicyUpdate(t *testing.T) {
	var lists *metricLists
	preAddFn := func(e *Entry) { lists = e.lists }
	postAddFn := func(t *testing.T) {
		require.Equal(t, 3, len(lists.lists))
		for _, p := range testPolicies {
			list, exists := lists.lists[p.Resolution.Window]
			require.True(t, exists)
			require.Equal(t, 1, list.aggregations.Len())
			checkElemTombstoned(t, list.aggregations.Front().Value.(metricElem), nil)
		}
	}
	testEntryAddMetricWithPolicies(t, testPoliciesVersion, preAddFn, postAddFn, testPolicies)
}

func TestEntryAddMetricWithPoliciesWithPolicyUpdate(t *testing.T) {
	var lists *metricLists
	deletedPolicies := make(map[policy.Policy]struct{})
	deletedPolicies[testPolicies[1]] = struct{}{}
	deletedPolicies[testPolicies[2]] = struct{}{}

	preAddFn := func(e *Entry) { lists = e.lists }
	postAddFn := func(t *testing.T) {
		require.Equal(t, 4, len(lists.lists))
		expectedLengths := []int{1, 2, 1}
		for _, policies := range [][]policy.Policy{testPolicies, testNewPolicies} {
			for i := range policies {
				list, exists := lists.lists[policies[i].Resolution.Window]
				require.True(t, exists)
				require.Equal(t, expectedLengths[i], list.aggregations.Len())
				for elem := list.aggregations.Front(); elem != nil; elem = elem.Next() {
					checkElemTombstoned(t, elem.Value.(metricElem), deletedPolicies)
				}
			}
		}
	}
	testEntryAddMetricWithPolicies(t, testPoliciesVersion+1, preAddFn, postAddFn, testNewPolicies)
}

func TestEntryAddMetricsWithPolicyError(t *testing.T) {
	e, lists, now := testEntry()
	e.version = testPoliciesVersion
	versionedPolicies := policy.CustomVersionedPolicies(
		testPoliciesVersion+1,
		now.Add(-time.Second),
		testNewPolicies,
	)

	// Add an invalid metric should result in an error
	require.Error(t, e.AddMetricWithPolicies(
		testInvalidMetric,
		versionedPolicies,
	))

	// Add a metric to a closed entry should result in an error
	e.closed = true
	require.Equal(t, errEntryClosed, e.AddMetricWithPolicies(
		testCounter,
		versionedPolicies,
	))

	// Add a metric with closed lists should result in an error
	e.closed = false
	lists.closed = true
	require.Error(t, e.AddMetricWithPolicies(
		testCounter,
		versionedPolicies,
	))
}

func TestEntryMaybeExpireNoExpiry(t *testing.T) {
	e, _, now := testEntry()

	// If we are still within entry TTL, should not expire
	require.False(t, e.ShouldExpire(now.Add(e.opts.EntryTTL()).Add(-time.Second)))

	// If the entry is closed, should not expire
	e.closed = true
	require.False(t, e.ShouldExpire(now.Add(e.opts.EntryTTL()).Add(time.Second)))

	// If there are still active writers, should not expire
	e.closed = false
	e.numWriters = 1
	require.False(t, e.ShouldExpire(now.Add(e.opts.EntryTTL()).Add(time.Second)))
}

func TestEntryMaybeExpireWithExpiry(t *testing.T) {
	e, _, now := testEntry()
	populateTestAggregations(t, e, unaggregated.CounterType)

	var elems []*CounterElem
	for _, elem := range e.aggregations {
		elems = append(elems, elem.Value.(*CounterElem))
	}

	// Try expiring this entry and assert it's not expired
	require.False(t, e.TryExpire(now))

	// Try expiring the entry with time in the future and
	// assert it's expired
	require.True(t, e.TryExpire(now.Add(e.opts.EntryTTL()).Add(time.Second)))

	// Assert elements have been tombstoned
	require.Equal(t, 0, len(e.aggregations))
	require.NotNil(t, e.aggregations)
	require.Nil(t, e.lists)
	for _, elem := range elems {
		require.True(t, elem.tombstoned)
	}
}

func TestShouldUpdatePoliciesWithLock(t *testing.T) {
	e := NewEntry(nil, testOptions())

	// If entry version is the init version, we should update
	currTime := time.Now()
	cutover := currTime.Add(-time.Second)
	require.True(t, e.shouldUpdatePoliciesWithLock(currTime, -100, cutover))

	// If the current version is older than the incoming version,
	// and we've surpassed the cutover, we should update the policies
	e.version = 2
	require.True(t, e.shouldUpdatePoliciesWithLock(currTime, 3, cutover))

	// Otherwise we shouldn't update
	require.False(t, e.shouldUpdatePoliciesWithLock(currTime, 2, cutover))
	require.False(t, e.shouldUpdatePoliciesWithLock(currTime, 3, currTime.Add(time.Second)))
}
