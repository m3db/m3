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
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/clock"
	xid "github.com/m3db/m3x/id"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	testDefaultPoliciesList = policy.DefaultPoliciesList
	testCustomPoliciesList  = policy.PoliciesList{
		policy.NewStagedPolicies(
			time.Now().UnixNano(),
			false,
			[]policy.Policy{
				policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
				policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 2*24*time.Hour), policy.DefaultAggregationID),
				policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 30*24*time.Hour), policy.DefaultAggregationID),
			},
		),
	}
)

func TestMetricMapAddMetricWithPoliciesList(t *testing.T) {
	opts := testOptions()
	m := newMetricMap(testShard, opts)
	policies := testDefaultPoliciesList

	// Add a counter metric and assert there is one entry afterwards.
	key := entryKey{
		metricType: unaggregated.CounterType,
		idHash:     xid.HashFn(testCounterID),
	}
	require.NoError(t, m.AddMetricWithPoliciesList(testCounter, policies))
	require.Equal(t, 1, len(m.entries))
	require.Equal(t, 1, m.entryList.Len())

	elem, exists := m.entries[key]
	require.True(t, exists)
	entry := elem.Value.(hashedEntry)
	require.Equal(t, int32(0), atomic.LoadInt32(&entry.entry.numWriters))
	require.Equal(t, key, entry.key)
	require.Equal(t, 2, m.metricLists.Len())

	// Add the same counter and assert there is still one entry.
	require.NoError(t, m.AddMetricWithPoliciesList(testCounter, policies))
	require.Equal(t, 1, len(m.entries))
	require.Equal(t, 1, m.entryList.Len())
	elem2, exists := m.entries[key]
	require.True(t, exists)
	entry2 := elem2.Value.(hashedEntry)
	require.Equal(t, entry, entry2)
	require.Equal(t, int32(0), atomic.LoadInt32(&entry2.entry.numWriters))
	require.Equal(t, 2, m.metricLists.Len())

	// Add a metric with different type and assert there are
	// now two entries.
	key2 := entryKey{
		metricType: unaggregated.GaugeType,
		idHash:     xid.HashFn(testCounterID),
	}
	metricWithDifferentType := unaggregated.MetricUnion{
		Type:     unaggregated.GaugeType,
		ID:       testCounterID,
		GaugeVal: 123.456,
	}
	require.NoError(t, m.AddMetricWithPoliciesList(
		metricWithDifferentType,
		testCustomPoliciesList,
	))
	require.Equal(t, 2, len(m.entries))
	require.Equal(t, 2, m.entryList.Len())
	require.Equal(t, 3, m.metricLists.Len())
	e1, exists1 := m.entries[key]
	e2, exists2 := m.entries[key2]
	require.True(t, exists1)
	require.True(t, exists2)
	require.NotEqual(t, e1, e2)

	// Add a metric with a different id and assert there are now three entries.
	metricWithDifferentID := unaggregated.MetricUnion{
		Type:     unaggregated.GaugeType,
		ID:       []byte("bar"),
		GaugeVal: 123.456,
	}
	require.NoError(t, m.AddMetricWithPoliciesList(
		metricWithDifferentID,
		testCustomPoliciesList,
	))
	require.Equal(t, 3, len(m.entries))
	require.Equal(t, 3, m.entryList.Len())
	require.Equal(t, 3, m.metricLists.Len())
}

func TestMetricMapDeleteExpired(t *testing.T) {
	var (
		batchPercent = 0.07
		ttl          = time.Hour
		now          = time.Now()
	)
	liveClockOpts := clock.NewOptions().SetNowFn(func() time.Time {
		return now
	})
	expiredClockOpt := clock.NewOptions().SetNowFn(func() time.Time {
		return now.Add(-ttl).Add(-time.Second)
	})
	opts := testOptions().
		SetClockOptions(liveClockOpts).
		SetEntryCheckBatchPercent(batchPercent)
	liveEntryOpts := opts.
		SetClockOptions(liveClockOpts).
		SetEntryTTL(ttl)
	expiredEntryOpts := opts.
		SetClockOptions(expiredClockOpt).
		SetEntryTTL(ttl)

	m := newMetricMap(testShard, opts)
	var sleepIntervals []time.Duration
	m.sleepFn = func(d time.Duration) { sleepIntervals = append(sleepIntervals, d) }

	// Insert some live entries and some expired entries.
	numEntries := 100
	for i := 0; i < numEntries; i++ {
		key := entryKey{
			metricType: unaggregated.CounterType,
			idHash:     xid.HashFn([]byte(fmt.Sprintf("%d", i))),
		}
		if i%2 == 0 {
			m.entries[key] = m.entryList.PushBack(hashedEntry{
				key:   key,
				entry: NewEntry(m.metricLists, liveEntryOpts),
			})
		} else {
			m.entries[key] = m.entryList.PushBack(hashedEntry{
				key:   key,
				entry: NewEntry(m.metricLists, expiredEntryOpts),
			})
		}
	}

	// Delete expired entries.
	m.deleteExpired(opts.EntryCheckInterval())

	// Assert there should be only half of the entries left.
	require.Equal(t, numEntries/2, len(m.entries))
	require.Equal(t, numEntries/2, m.entryList.Len())
	require.True(t, len(sleepIntervals) > 0)
	for k, v := range m.entries {
		e := v.Value.(hashedEntry)
		require.Equal(t, k, e.key)
		require.NotNil(t, e.entry)
	}
}
