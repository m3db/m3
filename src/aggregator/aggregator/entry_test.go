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

	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	testPoliciesVersion = 2
	compressor          = policy.NewAggregationIDCompressor()
	compressedMax, _    = compressor.Compress(policy.AggregationTypes{policy.Max})
	testPolicies        = []policy.Policy{
		policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
		policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 2*24*time.Hour), compressedMax),
		policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 30*24*time.Hour), policy.DefaultAggregationID),
	}
	testNewPolicies = []policy.Policy{
		policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
		policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 7*24*time.Hour), policy.DefaultAggregationID),
		policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 7*24*time.Hour), policy.DefaultAggregationID),
	}
	testDefaultPolicies = []policy.Policy{
		policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour), policy.DefaultAggregationID),
		policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 720*time.Hour), policy.DefaultAggregationID),
	}
)

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
	require.False(t, e.hasDefaultPoliciesList)
	require.False(t, e.useDefaultPolicies)
	require.Equal(t, int64(uninitializedCutoverNanos), e.cutoverNanos)
	require.False(t, e.tombstoned)
	require.Equal(t, lists, e.lists)
	require.Equal(t, int32(0), e.numWriters)
	require.Equal(t, now.UnixNano(), e.lastAccessNanos)
}

func TestEntryAddBatchTimerWithPoolAlloc(t *testing.T) {
	timerValPool := pool.NewFloatsPool([]pool.Bucket{
		{Capacity: 16, Count: 1},
	}, nil)
	timerValPool.Init()

	// Consume the element in the pool.
	input := timerValPool.Get(10)
	input = append(input, []float64{1.0, 3.5, 2.2, 6.5, 4.8}...)
	bt := unaggregated.MetricUnion{
		Type:          unaggregated.BatchTimerType,
		ID:            testBatchTimerID,
		BatchTimerVal: input,
		TimerValPool:  timerValPool,
	}
	e, _, _ := testEntry()
	require.NoError(t, e.AddMetricWithPoliciesList(bt, policy.DefaultPoliciesList))

	// Assert the timer values have been returned to pool.
	vals := timerValPool.Get(10)
	require.Equal(t, []float64{1.0, 3.5, 2.2, 6.5, 4.8}, vals[:5])
}

func TestEntryAddBatchTimerWithTimerBatchSizeLimit(t *testing.T) {
	e, _, now := testEntry()
	*now = time.Unix(105, 0)
	e.opts = e.opts.SetMaxTimerBatchSizePerWrite(2).SetDefaultPolicies(testDefaultPolicies)

	bt := unaggregated.MetricUnion{
		Type:          unaggregated.BatchTimerType,
		ID:            testBatchTimerID,
		BatchTimerVal: []float64{1.0, 3.5, 2.2, 6.5, 4.8},
	}
	require.NoError(t, e.AddMetricWithPoliciesList(bt, policy.DefaultPoliciesList))
	require.Equal(t, 2, len(e.aggregations))
	for _, p := range testDefaultPolicies {
		elem := e.aggregations[p].Value.(*TimerElem)
		require.Equal(t, 1, len(elem.values))
		require.Equal(t, 18.0, elem.values[0].timer.Sum())
	}
}

func TestEntryAddBatchTimerWithTimerBatchSizeLimitError(t *testing.T) {
	e, _, _ := testEntry()
	e.opts = e.opts.SetMaxTimerBatchSizePerWrite(2)
	e.closed = true

	bt := unaggregated.MetricUnion{
		Type:          unaggregated.BatchTimerType,
		ID:            testBatchTimerID,
		BatchTimerVal: []float64{1.0, 3.5, 2.2, 6.5, 4.8},
	}
	require.Equal(t, errEntryClosed, e.AddMetricWithPoliciesList(bt, policy.DefaultPoliciesList))
}

func TestEntryHasPolicyChangesWithLockDifferentLength(t *testing.T) {
	e, _, _ := testEntry()
	require.True(t, e.hasPolicyChangesWithLock(testPolicies))
}

func TestEntryHasPolicyChangesWithLockSameLengthDifferentPolicies(t *testing.T) {
	e, _, _ := testEntry()
	for i, p := range testPolicies {
		if i == len(testPolicies)-1 {
			resolution := p.Resolution()
			retention := p.Retention()
			newRetention := time.Duration(retention) - time.Second
			p = policy.NewPolicy(policy.NewStoragePolicy(resolution.Window, resolution.Precision, newRetention), policy.DefaultAggregationID)
		}
		e.aggregations[p] = &list.Element{}
	}
	require.True(t, e.hasPolicyChangesWithLock(testPolicies))
}

func TestEntryHasPolicyChangesWithLockSameLengthSamePolicies(t *testing.T) {
	e, _, _ := testEntry()
	for _, p := range testPolicies {
		e.aggregations[p] = &list.Element{}
	}
	require.False(t, e.hasPolicyChangesWithLock(testPolicies))
}

func TestEntryAddMetricWithPoliciesListDefaultPoliciesList(t *testing.T) {
	var (
		withPrepopulation   = true
		prePopulatePolicies = testDefaultPolicies
		ownsID              = false
		inputPoliciesList   = policy.DefaultPoliciesList
		expectedPolicies    = policy.NewStagedPolicies(0, false, nil)
		lists               *metricLists
	)

	preAddFn := func(e *Entry, now *time.Time) {
		e.hasDefaultPoliciesList = true
		e.cutoverNanos = 0
		e.tombstoned = false
		e.useDefaultPolicies = true
		lists = e.lists
	}
	postAddFn := func(t *testing.T) {
		require.Equal(t, 2, len(lists.lists))
		for _, p := range testDefaultPolicies {
			list, exists := lists.lists[p.Resolution().Window]
			require.True(t, exists)
			require.Equal(t, 1, list.aggregations.Len())
			checkElemTombstoned(t, list.aggregations.Front().Value.(metricElem), nil)
		}
	}

	testEntryAddMetricWithPoliciesList(
		t, withPrepopulation, prePopulatePolicies, ownsID,
		preAddFn, inputPoliciesList, postAddFn, expectedPolicies,
	)
}

func TestEntryAddMetricWithPoliciesListEmptyPoliciesListError(t *testing.T) {
	e, _, _ := testEntry()
	inputPoliciesList := policy.PoliciesList{}
	require.Equal(t, errEmptyPoliciesList, e.AddMetricWithPoliciesList(testCounter, inputPoliciesList))
}

func TestEntryAddMetricWithPoliciesListFuturePolicies(t *testing.T) {
	var (
		withPrepopulation   = true
		prePopulatePolicies = testDefaultPolicies
		ownsID              = false
		nowNanos            = time.Now().UnixNano()
		inputPoliciesList   = policy.PoliciesList{
			policy.NewStagedPolicies(nowNanos+100, false, testPolicies),
		}
		expectedPolicies = policy.NewStagedPolicies(0, false, nil)
		lists            *metricLists
	)

	preAddFn := func(e *Entry, now *time.Time) {
		*now = time.Unix(0, nowNanos)
		e.hasDefaultPoliciesList = false
		e.cutoverNanos = 0
		e.tombstoned = false
		e.useDefaultPolicies = true
		lists = e.lists
	}
	postAddFn := func(t *testing.T) {
		require.Equal(t, 2, len(lists.lists))
		for _, p := range testDefaultPolicies {
			list, exists := lists.lists[p.Resolution().Window]
			require.True(t, exists)
			require.Equal(t, 1, list.aggregations.Len())
			checkElemTombstoned(t, list.aggregations.Front().Value.(metricElem), nil)
		}
	}

	testEntryAddMetricWithPoliciesList(
		t, withPrepopulation, prePopulatePolicies, ownsID,
		preAddFn, inputPoliciesList, postAddFn, expectedPolicies,
	)
}

func TestEntryAddMetricWithPoliciesListStalePolicies(t *testing.T) {
	var (
		withPrepopulation   = true
		prePopulatePolicies = testDefaultPolicies
		ownsID              = false
		nowNanos            = time.Now().UnixNano()
		inputPoliciesList   = policy.PoliciesList{
			policy.NewStagedPolicies(nowNanos-1000, false, testPolicies),
		}
		expectedPolicies = policy.NewStagedPolicies(nowNanos-100, false, nil)
		lists            *metricLists
	)

	preAddFn := func(e *Entry, now *time.Time) {
		*now = time.Unix(0, nowNanos)
		e.hasDefaultPoliciesList = false
		e.cutoverNanos = nowNanos - 100
		e.tombstoned = false
		e.useDefaultPolicies = true
		lists = e.lists
	}
	postAddFn := func(t *testing.T) {
		require.Equal(t, 2, len(lists.lists))
		for _, p := range testDefaultPolicies {
			list, exists := lists.lists[p.Resolution().Window]
			require.True(t, exists)
			require.Equal(t, 1, list.aggregations.Len())
			checkElemTombstoned(t, list.aggregations.Front().Value.(metricElem), nil)
		}
	}

	testEntryAddMetricWithPoliciesList(
		t, withPrepopulation, prePopulatePolicies, ownsID,
		preAddFn, inputPoliciesList, postAddFn, expectedPolicies,
	)
}

func TestEntryAddMetricWithPoliciesListSameDefaultPolicies(t *testing.T) {
	var (
		withPrepopulation   = true
		prePopulatePolicies = testDefaultPolicies
		ownsID              = false
		nowNanos            = time.Now().UnixNano()
		inputPoliciesList   = policy.PoliciesList{
			policy.NewStagedPolicies(nowNanos-1000, false, testPolicies),
			policy.NewStagedPolicies(nowNanos-100, false, testDefaultPolicies),
			policy.NewStagedPolicies(nowNanos+100, false, testPolicies),
		}
		expectedPolicies = policy.NewStagedPolicies(nowNanos-100, false, nil)
		lists            *metricLists
	)

	preAddFn := func(e *Entry, now *time.Time) {
		*now = time.Unix(0, nowNanos)
		e.hasDefaultPoliciesList = false
		e.cutoverNanos = nowNanos - 100
		e.tombstoned = false
		e.useDefaultPolicies = true
		lists = e.lists
	}
	postAddFn := func(t *testing.T) {
		require.Equal(t, 2, len(lists.lists))
		for _, p := range testDefaultPolicies {
			list, exists := lists.lists[p.Resolution().Window]
			require.True(t, exists)
			require.Equal(t, 1, list.aggregations.Len())
			checkElemTombstoned(t, list.aggregations.Front().Value.(metricElem), nil)
		}
	}

	testEntryAddMetricWithPoliciesList(
		t, withPrepopulation, prePopulatePolicies, ownsID,
		preAddFn, inputPoliciesList, postAddFn, expectedPolicies,
	)
}

func TestEntryAddMetricWithPoliciesListSameCustomPolicies(t *testing.T) {
	var (
		withPrepopulation   = true
		prePopulatePolicies = testPolicies
		ownsID              = false
		nowNanos            = time.Now().UnixNano()
		inputPoliciesList   = policy.PoliciesList{
			policy.NewStagedPolicies(nowNanos-1000, false, testDefaultPolicies),
			policy.NewStagedPolicies(nowNanos-100, false, testPolicies),
			policy.NewStagedPolicies(nowNanos+100, false, testDefaultPolicies),
		}
		expectedPolicies = policy.NewStagedPolicies(nowNanos-100, false, testPolicies)
		lists            *metricLists
	)

	preAddFn := func(e *Entry, now *time.Time) {
		*now = time.Unix(0, nowNanos)
		e.hasDefaultPoliciesList = false
		e.cutoverNanos = nowNanos - 100
		e.tombstoned = false
		e.useDefaultPolicies = false
		lists = e.lists
	}
	postAddFn := func(t *testing.T) {
		require.Equal(t, 3, len(lists.lists))
		for _, p := range testPolicies {
			list, exists := lists.lists[p.Resolution().Window]
			require.True(t, exists)
			require.Equal(t, 1, list.aggregations.Len())
			checkElemTombstoned(t, list.aggregations.Front().Value.(metricElem), nil)
		}
	}

	testEntryAddMetricWithPoliciesList(
		t, withPrepopulation, prePopulatePolicies, ownsID,
		preAddFn, inputPoliciesList, postAddFn, expectedPolicies,
	)
}

func TestEntryAddMetricWithPoliciesListDifferentTombstone(t *testing.T) {
	var (
		withPrepopulation   = true
		prePopulatePolicies = testPolicies
		ownsID              = false
		nowNanos            = time.Now().UnixNano()
		inputPoliciesList   = policy.PoliciesList{
			policy.NewStagedPolicies(nowNanos-1000, false, testDefaultPolicies),
			policy.NewStagedPolicies(nowNanos-100, true, nil),
			policy.NewStagedPolicies(nowNanos+100, false, testDefaultPolicies),
		}
		expectedPolicies = policy.NewStagedPolicies(nowNanos-100, true, nil)
		lists            *metricLists
	)

	deletedPolicies := make(map[policy.StoragePolicy]struct{})
	for _, policy := range testPolicies {
		deletedPolicies[policy.StoragePolicy] = struct{}{}
	}
	preAddFn := func(e *Entry, now *time.Time) {
		*now = time.Unix(0, nowNanos)
		e.hasDefaultPoliciesList = false
		e.cutoverNanos = nowNanos - 100
		e.tombstoned = false
		e.useDefaultPolicies = false
		lists = e.lists
	}
	postAddFn := func(t *testing.T) {
		require.Equal(t, 3, len(lists.lists))
		for _, p := range testPolicies {
			list, exists := lists.lists[p.Resolution().Window]
			require.True(t, exists)
			require.Equal(t, 1, list.aggregations.Len())
			checkElemTombstoned(t, list.aggregations.Front().Value.(metricElem), deletedPolicies)
		}
	}

	testEntryAddMetricWithPoliciesList(
		t, withPrepopulation, prePopulatePolicies, ownsID,
		preAddFn, inputPoliciesList, postAddFn, expectedPolicies,
	)
}

func TestEntryAddMetricWithPoliciesListDifferentCutoverSamePolicies(t *testing.T) {
	var (
		withPrepopulation   = true
		prePopulatePolicies = testPolicies
		ownsID              = false
		nowNanos            = time.Now().UnixNano()
		inputPoliciesList   = policy.PoliciesList{
			policy.NewStagedPolicies(nowNanos-1000, false, testDefaultPolicies),
			policy.NewStagedPolicies(nowNanos-10, false, testPolicies),
			policy.NewStagedPolicies(nowNanos+100, false, testDefaultPolicies),
		}
		expectedPolicies = policy.NewStagedPolicies(nowNanos-10, false, testPolicies)
		lists            *metricLists
	)

	preAddFn := func(e *Entry, now *time.Time) {
		*now = time.Unix(0, nowNanos)
		e.hasDefaultPoliciesList = false
		e.cutoverNanos = nowNanos - 100
		e.tombstoned = false
		e.useDefaultPolicies = false
		lists = e.lists
	}
	postAddFn := func(t *testing.T) {
		require.Equal(t, 3, len(lists.lists))
		for _, p := range testPolicies {
			list, exists := lists.lists[p.Resolution().Window]
			require.True(t, exists)
			require.Equal(t, 1, list.aggregations.Len())
			checkElemTombstoned(t, list.aggregations.Front().Value.(metricElem), nil)
		}
	}

	testEntryAddMetricWithPoliciesList(
		t, withPrepopulation, prePopulatePolicies, ownsID,
		preAddFn, inputPoliciesList, postAddFn, expectedPolicies,
	)
}

func TestEntryAddMetricWithPoliciesListDifferentCutoverDifferentPolicies(t *testing.T) {
	var (
		withPrepopulation   = true
		prePopulatePolicies = testPolicies
		ownsID              = false
		nowNanos            = time.Now().UnixNano()
		inputPoliciesList   = policy.PoliciesList{
			policy.NewStagedPolicies(nowNanos-1000, false, testDefaultPolicies),
			policy.NewStagedPolicies(nowNanos-10, false, testNewPolicies),
			policy.NewStagedPolicies(nowNanos+100, false, testPolicies),
		}
		expectedPolicies = policy.NewStagedPolicies(nowNanos-10, false, testNewPolicies)
		lists            *metricLists
	)

	deletedPolicies := make(map[policy.StoragePolicy]struct{})
	deletedPolicies[testPolicies[1].StoragePolicy] = struct{}{}
	deletedPolicies[testPolicies[2].StoragePolicy] = struct{}{}

	preAddFn := func(e *Entry, now *time.Time) {
		*now = time.Unix(0, nowNanos)
		e.hasDefaultPoliciesList = false
		e.cutoverNanos = nowNanos - 100
		e.tombstoned = false
		e.useDefaultPolicies = false
		lists = e.lists
	}
	postAddFn := func(t *testing.T) {
		require.Equal(t, 4, len(lists.lists))
		expectedLengths := []int{1, 2, 1}
		for _, policies := range [][]policy.Policy{testPolicies, testNewPolicies} {
			for i := range policies {
				list, exists := lists.lists[policies[i].Resolution().Window]
				require.True(t, exists)
				require.Equal(t, expectedLengths[i], list.aggregations.Len())
				for elem := list.aggregations.Front(); elem != nil; elem = elem.Next() {
					checkElemTombstoned(t, elem.Value.(metricElem), deletedPolicies)
				}
			}
		}
	}

	testEntryAddMetricWithPoliciesList(
		t, withPrepopulation, prePopulatePolicies, ownsID,
		preAddFn, inputPoliciesList, postAddFn, expectedPolicies,
	)
}

func TestEntryAddMetricWithPoliciesListWithPolicyUpdateIDNotOwnedCopyID(t *testing.T) {
	var (
		withPrepopulation   = false
		prePopulatePolicies = testPolicies
		ownsID              = false
		nowNanos            = time.Now().UnixNano()
		inputPoliciesList   = policy.PoliciesList{
			policy.NewStagedPolicies(nowNanos-1000, false, testDefaultPolicies),
			policy.NewStagedPolicies(nowNanos-10, false, testPolicies),
			policy.NewStagedPolicies(nowNanos+100, false, testPolicies),
		}
		expectedPolicies = policy.NewStagedPolicies(nowNanos-10, false, testPolicies)
		lists            *metricLists
	)

	preAddFn := func(e *Entry, now *time.Time) {
		*now = time.Unix(0, nowNanos)
		e.cutoverNanos = uninitializedCutoverNanos
		lists = e.lists
	}
	postAddFn := func(t *testing.T) {
		require.Equal(t, 3, len(lists.lists))
		for _, policy := range testPolicies {
			list, exists := lists.lists[policy.Resolution().Window]
			require.True(t, exists)
			require.Equal(t, 1, list.aggregations.Len())
			for elem := list.aggregations.Front(); elem != nil; elem = elem.Next() {
				checkElemTombstoned(t, elem.Value.(metricElem), nil)
			}
		}
	}
	testEntryAddMetricWithPoliciesList(
		t, withPrepopulation, prePopulatePolicies, ownsID,
		preAddFn, inputPoliciesList, postAddFn, expectedPolicies,
	)
}

func TestEntryAddMetricWithPoliciesListWithPolicyUpdateIDOwnsID(t *testing.T) {
	var (
		withPrepopulation   = false
		prePopulatePolicies = testPolicies
		ownsID              = true
		nowNanos            = time.Now().UnixNano()
		inputPoliciesList   = policy.PoliciesList{
			policy.NewStagedPolicies(nowNanos-1000, false, testDefaultPolicies),
			policy.NewStagedPolicies(nowNanos-10, false, testPolicies),
			policy.NewStagedPolicies(nowNanos+100, false, testPolicies),
		}
		expectedPolicies = policy.NewStagedPolicies(nowNanos-10, false, testPolicies)
		lists            *metricLists
	)

	preAddFn := func(e *Entry, now *time.Time) {
		*now = time.Unix(0, nowNanos)
		e.cutoverNanos = uninitializedCutoverNanos
		lists = e.lists
	}
	postAddFn := func(t *testing.T) {
		require.Equal(t, 3, len(lists.lists))
		for _, policy := range testPolicies {
			list, exists := lists.lists[policy.Resolution().Window]
			require.True(t, exists)
			require.Equal(t, 1, list.aggregations.Len())
			for elem := list.aggregations.Front(); elem != nil; elem = elem.Next() {
				checkElemTombstoned(t, elem.Value.(metricElem), nil)
			}
		}
	}
	testEntryAddMetricWithPoliciesList(
		t, withPrepopulation, prePopulatePolicies, ownsID,
		preAddFn, inputPoliciesList, postAddFn, expectedPolicies,
	)
}

func TestEntryAddMetricWithPoliciesListWithInvalidAggregationType(t *testing.T) {
	compressor := policy.NewAggregationIDCompressor()
	compressedMin, err := compressor.Compress(policy.AggregationTypes{policy.Min})
	require.NoError(t, err)
	compressedLast, err := compressor.Compress(policy.AggregationTypes{policy.Last})
	require.NoError(t, err)
	compressedP9999, err := compressor.Compress(policy.AggregationTypes{policy.P9999})
	require.NoError(t, err)

	e, _, _ := testEntry()

	require.NoError(t, e.AddMetricWithPoliciesList(testCounter, policy.PoliciesList{policy.NewStagedPolicies(0, false, []policy.Policy{
		policy.NewPolicy(policy.EmptyStoragePolicy, compressedMin),
	})}))
	require.Error(t, e.AddMetricWithPoliciesList(testCounter, policy.PoliciesList{policy.NewStagedPolicies(0, false, []policy.Policy{
		policy.NewPolicy(policy.EmptyStoragePolicy, compressedP9999),
	})}))

	require.NoError(t, e.AddMetricWithPoliciesList(testGauge, policy.PoliciesList{policy.NewStagedPolicies(0, false, []policy.Policy{
		policy.NewPolicy(policy.EmptyStoragePolicy, compressedMin),
	})}))
	require.Error(t, e.AddMetricWithPoliciesList(testGauge, policy.PoliciesList{policy.NewStagedPolicies(0, false, []policy.Policy{
		policy.NewPolicy(policy.EmptyStoragePolicy, compressedP9999),
	})}))

	require.NoError(t, e.AddMetricWithPoliciesList(testBatchTimer, policy.PoliciesList{policy.NewStagedPolicies(0, false, []policy.Policy{
		policy.NewPolicy(policy.EmptyStoragePolicy, compressedMin),
	})}))
	require.Error(t, e.AddMetricWithPoliciesList(testBatchTimer, policy.PoliciesList{policy.NewStagedPolicies(0, false, []policy.Policy{
		policy.NewPolicy(policy.EmptyStoragePolicy, compressedLast),
	})}))
}

func TestEntryAddMetricsWithPoliciesListError(t *testing.T) {
	e, lists, _ := testEntry()
	policiesList := policy.PoliciesList{policy.NewStagedPolicies(0, false, testPolicies)}

	// Add an invalid metric should result in an error.
	require.Error(t, e.AddMetricWithPoliciesList(testInvalidMetric, policiesList))

	// Add a metric to a closed entry should result in an error.
	e.closed = true
	require.Equal(t, errEntryClosed, e.AddMetricWithPoliciesList(testCounter, policiesList))

	// Add a metric with closed lists should result in an error.
	e.closed = false
	lists.closed = true
	require.Error(t, e.AddMetricWithPoliciesList(testCounter, policiesList))
}

func TestEntryMaybeExpireNoExpiry(t *testing.T) {
	e, _, now := testEntry()

	// If we are still within entry TTL, should not expire.
	require.False(t, e.ShouldExpire(now.Add(e.opts.EntryTTL()).Add(-time.Second)))

	// If the entry is closed, should not expire.
	e.closed = true
	require.False(t, e.ShouldExpire(now.Add(e.opts.EntryTTL()).Add(time.Second)))

	// If there are still active writers, should not expire.
	e.closed = false
	e.numWriters = 1
	require.False(t, e.ShouldExpire(now.Add(e.opts.EntryTTL()).Add(time.Second)))
}

func TestEntryMaybeExpireWithExpiry(t *testing.T) {
	e, _, now := testEntry()
	populateTestAggregations(t, e, testPolicies, unaggregated.CounterType)

	var elems []*CounterElem
	for _, elem := range e.aggregations {
		elems = append(elems, elem.Value.(*CounterElem))
	}

	// Try expiring this entry and assert it's not expired.
	require.False(t, e.TryExpire(*now))

	// Try expiring the entry with time in the future and
	// assert it's expired.
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
	currTimeNanos := time.Now().UnixNano()
	inputs := []struct {
		cutoverNanos int64
		tombstoned   bool
		sp           policy.StagedPolicies
		expected     bool
	}{
		{
			cutoverNanos: uninitializedCutoverNanos,
			tombstoned:   false,
			sp:           policy.NewStagedPolicies(currTimeNanos-100, false, nil),
			expected:     true,
		},
		{
			cutoverNanos: currTimeNanos - 100,
			tombstoned:   false,
			sp:           policy.NewStagedPolicies(currTimeNanos, false, nil),
			expected:     true,
		},
		{
			cutoverNanos: currTimeNanos,
			tombstoned:   false,
			sp:           policy.NewStagedPolicies(currTimeNanos, true, nil),
			expected:     true,
		},
		{
			cutoverNanos: currTimeNanos,
			tombstoned:   false,
			sp:           policy.NewStagedPolicies(currTimeNanos-100, true, nil),
			expected:     false,
		},
	}

	for _, input := range inputs {
		e.cutoverNanos = input.cutoverNanos
		e.tombstoned = input.tombstoned
		require.Equal(t, input.expected, e.shouldUpdatePoliciesWithLock(time.Unix(0, currTimeNanos), input.sp))
	}

}

func testEntry() (*Entry, *metricLists, *time.Time) {
	now := time.Now()
	clockOpts := clock.NewOptions().SetNowFn(func() time.Time {
		return now
	})
	opts := testOptions().
		SetClockOptions(clockOpts).
		SetMinFlushInterval(0).
		SetDefaultPolicies(testDefaultPolicies)

	lists := newMetricLists(testShard, opts)
	// This effectively disable flushing.
	lists.newMetricListFn = func(shard uint32, res time.Duration, opts Options) *metricList {
		return newMetricList(testShard, 0, opts)
	}

	e := NewEntry(nil, opts)
	e.ResetSetData(lists, opts)

	return e, lists, &now
}

func populateTestAggregations(
	t *testing.T,
	e *Entry,
	policies []policy.Policy,
	typ unaggregated.Type,
) {
	for _, p := range policies {
		var (
			newElem metricElem
			testID  id.RawID
		)
		switch typ {
		case unaggregated.CounterType:
			newElem = e.opts.CounterElemPool().Get()
			testID = testCounterID
		case unaggregated.BatchTimerType:
			newElem = e.opts.TimerElemPool().Get()
			testID = testBatchTimerID
		case unaggregated.GaugeType:
			newElem = e.opts.GaugeElemPool().Get()
			testID = testGaugeID
		default:
			require.Fail(t, fmt.Sprintf("unrecognized metric type: %v", typ))
		}
		newElem.ResetSetData(testID, p.StoragePolicy, policy.DefaultAggregationTypes)
		list, err := e.lists.FindOrCreate(p.Resolution().Window)
		require.NoError(t, err)
		newListElem, err := list.PushBack(newElem)
		require.NoError(t, err)
		e.aggregations[p] = newListElem
	}
}

func checkElemTombstoned(t *testing.T, elem metricElem, deleted map[policy.StoragePolicy]struct{}) {
	switch elem := elem.(type) {
	case *CounterElem:
		if _, exists := deleted[elem.sp]; exists {
			require.True(t, elem.tombstoned)
		} else {
			require.False(t, elem.tombstoned)
		}
	case *TimerElem:
		if _, exists := deleted[elem.sp]; exists {
			require.True(t, elem.tombstoned)
		} else {
			require.False(t, elem.tombstoned)
		}
	case *GaugeElem:
		if _, exists := deleted[elem.sp]; exists {
			require.True(t, elem.tombstoned)
		} else {
			require.False(t, elem.tombstoned)
		}
	default:
		require.Fail(t, fmt.Sprintf("unexpected elem type %T", elem))
	}
}

func testEntryAddMetricWithPoliciesList(
	t *testing.T,
	withPrePopulation bool,
	prePopulatePolicies []policy.Policy,
	ownsID bool,
	preAddFn testPreProcessFn,
	inputPoliciesList policy.PoliciesList,
	postAddFn testPostProcessFn,
	expectedPolicies policy.StagedPolicies,
) {
	inputs := []testEntryData{
		{
			mu: testCounter,
			fn: func(t *testing.T, elem *list.Element, alignedStart time.Time) {
				id := elem.Value.(*CounterElem).ID()
				require.Equal(t, testCounterID, id)
				aggregations := elem.Value.(*CounterElem).values
				require.Equal(t, 1, len(aggregations))
				require.Equal(t, alignedStart.UnixNano(), aggregations[0].timeNanos)
				require.Equal(t, int64(1234), aggregations[0].counter.Sum())
			},
		},
		{
			mu: testBatchTimer,
			fn: func(t *testing.T, elem *list.Element, alignedStart time.Time) {
				id := elem.Value.(*TimerElem).ID()
				require.Equal(t, testBatchTimerID, id)
				aggregations := elem.Value.(*TimerElem).values
				require.Equal(t, 1, len(aggregations))
				require.Equal(t, alignedStart.UnixNano(), aggregations[0].timeNanos)
				require.Equal(t, 18.0, aggregations[0].timer.Sum())
			},
		},
		{
			mu: testGauge,
			fn: func(t *testing.T, elem *list.Element, alignedStart time.Time) {
				id := elem.Value.(*GaugeElem).ID()
				require.Equal(t, testGaugeID, id)
				aggregations := elem.Value.(*GaugeElem).values
				require.Equal(t, 1, len(aggregations))
				require.Equal(t, alignedStart.UnixNano(), aggregations[0].timeNanos)
				require.Equal(t, 123.456, aggregations[0].gauge.Last())
			},
		},
	}

	for _, input := range inputs {
		input.mu.OwnsID = ownsID

		e, _, now := testEntry()
		if withPrePopulation {
			populateTestAggregations(t, e, prePopulatePolicies, input.mu.Type)
		}

		preAddFn(e, now)
		require.NoError(t, e.AddMetricWithPoliciesList(
			input.mu,
			inputPoliciesList,
		))

		require.Equal(t, now.UnixNano(), e.lastAccessNanos)
		policies, isDefault := expectedPolicies.Policies()
		if !expectedPolicies.Tombstoned && isDefault {
			policies = e.opts.DefaultPolicies()
		}
		require.Equal(t, len(policies), len(e.aggregations))
		for _, p := range policies {
			elem, exists := e.aggregations[p]
			require.True(t, exists)
			input.fn(t, elem, now.Truncate(p.Resolution().Window))
		}
		require.Equal(t, expectedPolicies.CutoverNanos, e.cutoverNanos)
		require.Equal(t, expectedPolicies.Tombstoned, e.tombstoned)

		postAddFn(t)
	}
}

type testPreProcessFn func(e *Entry, now *time.Time)
type testElemValidateFn func(t *testing.T, elem *list.Element, alignedStart time.Time)
type testPostProcessFn func(t *testing.T)

type testEntryData struct {
	mu unaggregated.MetricUnion
	fn testElemValidateFn
}
