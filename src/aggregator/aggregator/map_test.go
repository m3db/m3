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

	"github.com/m3db/m3aggregator/id"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/clock"

	"github.com/stretchr/testify/require"
)

var (
	testDefaultVersionedPolicies = policy.DefaultVersionedPolicies(
		1,
		time.Now(),
	)
	testCustomVersionedPolicies = policy.CustomVersionedPolicies(
		1,
		time.Now(),
		testPolicies,
	)
)

func TestMetricMapAddMetricWithPolicies(t *testing.T) {
	opts := testOptions()
	m := newMetricMap(opts)
	policies := testDefaultVersionedPolicies

	// Add a counter metric and assert there is one entry afterwards.
	require.NoError(t, m.AddMetricWithPolicies(testCounter, policies))
	require.Equal(t, 1, len(m.entries))
	require.Equal(t, 1, m.entryList.Len())
	idHash := id.HashFn(testCounterID)
	elem, exists := m.entries[idHash]
	require.True(t, exists)
	entry := elem.Value.(hashedEntry)
	require.Equal(t, int32(0), atomic.LoadInt32(&entry.entry.numWriters))
	require.Equal(t, idHash, entry.idHash)
	require.Equal(t, 2, m.metricLists.Len())

	// Add the same counter and assert there is still one entry.
	require.NoError(t, m.AddMetricWithPolicies(testCounter, policies))
	require.Equal(t, 1, len(m.entries))
	require.Equal(t, 1, m.entryList.Len())
	elem2, exists := m.entries[idHash]
	require.True(t, exists)
	entry2 := elem2.Value.(hashedEntry)
	require.Equal(t, entry, entry2)
	require.Equal(t, int32(0), atomic.LoadInt32(&entry2.entry.numWriters))
	require.Equal(t, 2, m.metricLists.Len())

	// Add a different metric and assert there are now two entries.
	require.NoError(t, m.AddMetricWithPolicies(
		unaggregated.MetricUnion{
			Type:     unaggregated.GaugeType,
			ID:       []byte("bar"),
			GaugeVal: 123.456,
		},
		testCustomVersionedPolicies,
	))
	require.Equal(t, 2, len(m.entries))
	require.Equal(t, 2, m.entryList.Len())
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

	m := newMetricMap(opts)
	var waitIntervals []time.Duration
	m.waitForFn = func(d time.Duration) <-chan time.Time {
		waitIntervals = append(waitIntervals, d)
		c := make(chan time.Time)
		close(c)
		return c
	}

	// Insert some live entries and some expired entries.
	numEntries := 100
	for i := 0; i < numEntries; i++ {
		idHash := id.HashFn([]byte(fmt.Sprintf("%d", i)))
		if i%2 == 0 {
			m.entries[idHash] = m.entryList.PushBack(hashedEntry{
				idHash: idHash,
				entry:  NewEntry(m.metricLists, liveEntryOpts),
			})
		} else {
			m.entries[idHash] = m.entryList.PushBack(hashedEntry{
				idHash: idHash,
				entry:  NewEntry(m.metricLists, expiredEntryOpts),
			})
		}
	}

	// Delete expired entries.
	m.DeleteExpired(opts.EntryCheckInterval())

	// Assert there should be only half of the entries left.
	require.Equal(t, numEntries/2, len(m.entries))
	require.Equal(t, numEntries/2, m.entryList.Len())
	require.True(t, len(waitIntervals) > 0)
	for k, v := range m.entries {
		e := v.Value.(hashedEntry)
		require.Equal(t, k, e.idHash)
		require.NotNil(t, e.entry)
	}
}
