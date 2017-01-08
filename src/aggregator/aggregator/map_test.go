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
	testVersionedPolicies = policy.VersionedPolicies{
		Version:  1,
		Cutover:  time.Now(),
		Policies: testPolicies,
	}
)

func TestMetricMapAddMetricWithPolicies(t *testing.T) {
	opts := testOptions()
	lists := newMetricLists(opts)
	m := newMetricMap(lists, opts)
	policies := policy.DefaultVersionedPolicies

	// Add a counter metric and assert there is one entry afterwards
	require.NoError(t, m.AddMetricWithPolicies(testCounter, policies))
	require.Equal(t, 1, len(m.entries))
	idHash := id.HashFn(testID)
	e, exists := m.entries[idHash]
	require.True(t, exists)
	require.Equal(t, int32(0), atomic.LoadInt32(&e.numWriters))
	require.Equal(t, 2, lists.Len())

	// Add the same counter and assert there is still one entry
	require.NoError(t, m.AddMetricWithPolicies(testCounter, policies))
	require.Equal(t, 1, len(m.entries))
	e2, exists := m.entries[idHash]
	require.True(t, exists)
	require.Equal(t, e, e2)
	require.Equal(t, int32(0), atomic.LoadInt32(&e2.numWriters))
	require.Equal(t, 2, lists.Len())

	// Add a different metric and assert there are now two entries
	require.NoError(t, m.AddMetricWithPolicies(
		unaggregated.MetricUnion{
			Type:     unaggregated.GaugeType,
			ID:       []byte("bar"),
			GaugeVal: 123.456,
		},
		testVersionedPolicies,
	))
	require.Equal(t, 2, len(m.entries))
	require.Equal(t, 3, lists.Len())
}

func TestMetricMapDeleteExpired(t *testing.T) {
	var (
		idFoo = []byte("foo")
		idBar = []byte("bar")
		ttl   = time.Hour
		now   = time.Now()
	)
	liveClockOpts := clock.NewOptions().SetNowFn(func() time.Time {
		return now
	})
	liveEntryOpts := testOptions().
		SetClockOptions(liveClockOpts).
		SetEntryTTL(ttl)
	expiredClockOpt := clock.NewOptions().SetNowFn(func() time.Time {
		return now.Add(-ttl).Add(-time.Second)
	})
	expiredEntryOpts := testOptions().
		SetClockOptions(expiredClockOpt).
		SetEntryTTL(ttl)
	opts := testOptions().SetClockOptions(liveClockOpts)
	lists := newMetricLists(opts)
	m := newMetricMap(lists, opts)

	// Insert a live entry and an expired entry
	m.entries[id.HashFn(idFoo)] = NewEntry(lists, liveEntryOpts)
	m.entries[id.HashFn(idBar)] = NewEntry(lists, expiredEntryOpts)

	// Delete expired entries
	m.DeleteExpired()

	// Assert there should be only one entry left
	require.Equal(t, 1, len(m.entries))
	_, exists := m.entries[id.HashFn(idFoo)]
	require.True(t, exists)
}
