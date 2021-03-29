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

	"github.com/m3db/m3/src/aggregator/hash"
	"github.com/m3db/m3/src/aggregator/runtime"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/x/clock"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

var (
	testDefaultStagedMetadatas = metadata.DefaultStagedMetadatas
	testCustomStagedMetadatas  = metadata.StagedMetadatas{
		metadata.StagedMetadata{
			CutoverNanos: time.Now().UnixNano(),
			Tombstoned:   false,
			Metadata: metadata.Metadata{
				Pipelines: []metadata.PipelineMetadata{
					{
						AggregationID: aggregation.DefaultID,
						StoragePolicies: []policy.StoragePolicy{
							policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
							policy.NewStoragePolicy(time.Minute, xtime.Minute, 2*24*time.Hour),
							policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 30*24*time.Hour),
						},
					},
				},
			},
		},
	}
)

func TestMetricMapAddUntimedMapClosed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testOptions(ctrl)
	m := newMetricMap(testShard, opts)
	m.Close()

	require.Equal(t, errMetricMapClosed, m.AddUntimed(testCounter, testDefaultStagedMetadatas))
}

func TestMetricMapAddUntimedNoRateLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testOptions(ctrl)
	m := newMetricMap(testShard, opts)
	policies := testDefaultStagedMetadatas

	// Add a counter metric and assert there is one entry afterwards.
	key := entryKey{
		metricCategory: untimedMetric,
		metricType:     metric.CounterType,
		idHash:         hash.Murmur3Hash128(testCounterID),
	}
	require.NoError(t, m.AddUntimed(testCounter, policies))
	require.Equal(t, 1, len(m.entries))
	require.Equal(t, 1, m.entryList.Len())

	elem, exists := m.entries[key]
	require.True(t, exists)
	entry := elem.Value.(hashedEntry)
	require.Equal(t, int32(0), atomic.LoadInt32(&entry.entry.numWriters))
	require.Equal(t, key, entry.key)
	require.Equal(t, 2, m.metricLists.Len())

	// Add the same counter and assert there is still one entry.
	require.NoError(t, m.AddUntimed(testCounter, policies))
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
		metricCategory: untimedMetric,
		metricType:     metric.GaugeType,
		idHash:         hash.Murmur3Hash128(testCounterID),
	}
	metricWithDifferentType := unaggregated.MetricUnion{
		Type:     metric.GaugeType,
		ID:       testCounterID,
		GaugeVal: 123.456,
	}
	require.NoError(t, m.AddUntimed(
		metricWithDifferentType,
		testCustomStagedMetadatas,
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
		Type:     metric.GaugeType,
		ID:       []byte("bar"),
		GaugeVal: 123.456,
	}
	require.NoError(t, m.AddUntimed(
		metricWithDifferentID,
		testCustomStagedMetadatas,
	))
	require.Equal(t, 3, len(m.entries))
	require.Equal(t, 3, m.entryList.Len())
	require.Equal(t, 3, m.metricLists.Len())
}

func TestMetricMapSetRuntimeOptions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testOptions(ctrl)
	m := newMetricMap(testShard, opts)

	// Add three metrics.
	require.NoError(t, m.AddUntimed(testCounter, testDefaultStagedMetadatas))
	require.NoError(t, m.AddUntimed(testBatchTimer, testDefaultStagedMetadatas))
	require.NoError(t, m.AddUntimed(testGauge, testDefaultStagedMetadatas))

	// Assert no entries have rate limits.
	runtimeOpts := runtime.NewOptions()
	require.Equal(t, runtimeOpts, m.runtimeOpts)
	for elem := m.entryList.Front(); elem != nil; elem = elem.Next() {
		require.Equal(t, int64(0), elem.Value.(hashedEntry).entry.rateLimiter.Limit())
	}

	// Update runtime options and assert all entries now have rate limiters.
	newRateLimit := int64(100)
	runtimeOpts = runtime.NewOptions().SetWriteValuesPerMetricLimitPerSecond(newRateLimit)
	m.SetRuntimeOptions(runtimeOpts)
	require.Equal(t, runtimeOpts, m.runtimeOpts)
	for elem := m.entryList.Front(); elem != nil; elem = elem.Next() {
		require.Equal(t, newRateLimit, elem.Value.(hashedEntry).entry.rateLimiter.Limit())
	}
}

func TestMetricMapAddUntimedWithRateLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	now := time.Now()
	nowFn := func() time.Time { return now }
	clockOpts := clock.NewOptions().SetNowFn(nowFn)
	m := newMetricMap(testShard, testOptions(ctrl).SetClockOptions(clockOpts))

	// Reset runtime options to disable rate limiting.
	noRateLimitRuntimeOpts := runtime.NewOptions().SetWriteNewMetricLimitPerShardPerSecond(0)
	m.SetRuntimeOptions(noRateLimitRuntimeOpts)
	require.NoError(t, m.AddUntimed(testCounter, testDefaultStagedMetadatas))

	// Reset runtime options to enable rate limit of 1/s with a warmup period of 1 minute.
	limitPerSecond := 1
	warmupPeriod := time.Minute
	runtimeOpts := runtime.NewOptions().
		SetWriteNewMetricLimitPerShardPerSecond(int64(limitPerSecond)).
		SetWriteNewMetricNoLimitWarmupDuration(warmupPeriod)
	m.SetRuntimeOptions(runtimeOpts)

	// Verify all insertions should go through.
	startIdx := 0
	endIdx := 100
	for i := startIdx; i < endIdx; i++ {
		metric := unaggregated.MetricUnion{
			Type: metric.CounterType,
			ID:   id.RawID(fmt.Sprintf("testC%d", i)),
		}
		require.NoError(t, m.AddUntimed(metric, testDefaultStagedMetadatas))
	}
	require.Equal(t, now, m.firstInsertAt)

	// Advance time so we are no longer warming up and verify that the second insertion
	// results in a rate limit exceeded error.
	oldNow := now
	now = now.Add(2 * time.Minute)
	startIdx = endIdx
	endIdx = startIdx + 1
	for i := startIdx; i < endIdx; i++ {
		metric := unaggregated.MetricUnion{
			Type: metric.CounterType,
			ID:   id.RawID(fmt.Sprintf("testC%d", i)),
		}
		if i == startIdx {
			require.NoError(t, m.AddUntimed(metric, testDefaultStagedMetadatas))
		}
		if i == endIdx {
			require.Equal(t, errWriteNewMetricRateLimitExceeded, m.AddUntimed(metric, testDefaultStagedMetadatas))
		}
	}

	// Reset runtime options to enable rate limit of 100/s.
	limitPerSecond = 100
	runtimeOpts = runtime.NewOptions().SetWriteNewMetricLimitPerShardPerSecond(int64(limitPerSecond))
	m.SetRuntimeOptions(runtimeOpts)
	require.Equal(t, oldNow, m.firstInsertAt)

	// Verify we can insert 6 entries without issues.
	startIdx = endIdx
	endIdx = startIdx + 100
	for i := startIdx; i < endIdx; i++ {
		metric := unaggregated.MetricUnion{
			Type: metric.CounterType,
			ID:   id.RawID(fmt.Sprintf("testC%d", i)),
		}
		require.NoError(t, m.AddUntimed(metric, testDefaultStagedMetadatas))
	}

	// Verify one more insert results in rate limit violation.
	metric := unaggregated.MetricUnion{
		Type: metric.CounterType,
		ID:   id.RawID(fmt.Sprintf("testC%d", endIdx)),
	}
	require.Equal(t, errWriteNewMetricRateLimitExceeded, m.AddUntimed(metric, testDefaultStagedMetadatas))
}

func TestMetricMapAddTimedNoRateLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testOptions(ctrl)
	m := newMetricMap(testShard, opts)

	// Add a counter metric and assert there is one entry afterwards.
	am := aggregated.Metric{
		Type:      metric.CounterType,
		ID:        []byte("aggregatedMetric"),
		TimeNanos: 12345,
		Value:     76109,
	}
	key := entryKey{
		metricCategory: timedMetric,
		metricType:     metric.CounterType,
		idHash:         hash.Murmur3Hash128(am.ID),
	}
	require.NoError(t, m.AddTimed(am, testTimedMetadata))
	require.Equal(t, 1, len(m.entries))
	require.Equal(t, 1, m.entryList.Len())

	elem, exists := m.entries[key]
	require.True(t, exists)
	entry := elem.Value.(hashedEntry)
	require.Equal(t, int32(0), atomic.LoadInt32(&entry.entry.numWriters))
	require.Equal(t, key, entry.key)
	require.Equal(t, 1, m.metricLists.Len())

	// Add the same counter and assert there is still one entry.
	require.NoError(t, m.AddTimed(am, testTimedMetadata))
	require.Equal(t, 1, len(m.entries))
	require.Equal(t, 1, m.entryList.Len())
	elem2, exists := m.entries[key]
	require.True(t, exists)
	entry2 := elem2.Value.(hashedEntry)
	require.Equal(t, entry, entry2)
	require.Equal(t, int32(0), atomic.LoadInt32(&entry2.entry.numWriters))
	require.Equal(t, 1, m.metricLists.Len())

	// Add a metric with a different metric category and assert a new entry is added.
	um := unaggregated.MetricUnion{
		Type:       metric.CounterType,
		ID:         am.ID,
		CounterVal: 123,
	}
	key2 := entryKey{
		metricCategory: untimedMetric,
		metricType:     metric.CounterType,
		idHash:         hash.Murmur3Hash128(um.ID),
	}
	require.NoError(t, m.AddUntimed(um, testStagedMetadatas))
	require.Equal(t, 2, len(m.entries))
	require.Equal(t, 2, m.entryList.Len())
	require.Equal(t, 3, m.metricLists.Len())
	e1, exists1 := m.entries[key]
	e2, exists2 := m.entries[key2]
	require.True(t, exists1)
	require.True(t, exists2)
	require.False(t, e1 == e2)

	// Add a metric with different type and assert a new entry is added.
	metricWithDifferentType := aggregated.Metric{
		Type:      metric.GaugeType,
		ID:        am.ID,
		TimeNanos: 1234,
		Value:     123.456,
	}
	key3 := entryKey{
		metricCategory: timedMetric,
		metricType:     metric.GaugeType,
		idHash:         hash.Murmur3Hash128(metricWithDifferentType.ID),
	}
	require.NoError(t, m.AddTimed(metricWithDifferentType, testTimedMetadata))
	require.Equal(t, 3, len(m.entries))
	require.Equal(t, 3, m.entryList.Len())
	require.Equal(t, 3, m.metricLists.Len())
	e3, exists3 := m.entries[key3]
	require.True(t, exists3)
	require.False(t, e1 == e3)

	// Add a metric with a different id and assert a new entry is added.
	metricWithDifferentID := aggregated.Metric{
		Type:      metric.GaugeType,
		ID:        []byte("metricWithDifferentID"),
		TimeNanos: 1234,
		Value:     123.456,
	}
	key4 := entryKey{
		metricCategory: timedMetric,
		metricType:     metric.GaugeType,
		idHash:         hash.Murmur3Hash128(metricWithDifferentID.ID),
	}
	require.NoError(t, m.AddTimed(metricWithDifferentID, testTimedMetadata))
	require.Equal(t, 4, len(m.entries))
	require.Equal(t, 4, m.entryList.Len())
	require.Equal(t, 3, m.metricLists.Len())
	e4, exists4 := m.entries[key4]
	require.True(t, exists4)
	require.False(t, e1 == e4)
}

func TestMetricMapAddForwardedNoRateLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testOptions(ctrl)
	m := newMetricMap(testShard, opts)

	// Add a counter metric and assert there is one entry afterwards.
	am := aggregated.ForwardedMetric{
		Type:      metric.CounterType,
		ID:        []byte("aggregatedMetric"),
		TimeNanos: 12345,
		Values:    []float64{76109},
	}
	key := entryKey{
		metricCategory: forwardedMetric,
		metricType:     metric.CounterType,
		idHash:         hash.Murmur3Hash128(am.ID),
	}
	require.NoError(t, m.AddForwarded(am, testForwardMetadata))
	require.Equal(t, 1, len(m.entries))
	require.Equal(t, 1, m.entryList.Len())

	elem, exists := m.entries[key]
	require.True(t, exists)
	entry := elem.Value.(hashedEntry)
	require.Equal(t, int32(0), atomic.LoadInt32(&entry.entry.numWriters))
	require.Equal(t, key, entry.key)
	require.Equal(t, 1, m.metricLists.Len())

	// Add the same counter and assert there is still one entry.
	require.NoError(t, m.AddForwarded(am, testForwardMetadata))
	require.Equal(t, 1, len(m.entries))
	require.Equal(t, 1, m.entryList.Len())
	elem2, exists := m.entries[key]
	require.True(t, exists)
	entry2 := elem2.Value.(hashedEntry)
	require.Equal(t, entry, entry2)
	require.Equal(t, int32(0), atomic.LoadInt32(&entry2.entry.numWriters))
	require.Equal(t, 1, m.metricLists.Len())

	// Add a metric with a different metric category and assert a new entry is added.
	um := unaggregated.MetricUnion{
		Type:       metric.CounterType,
		ID:         am.ID,
		CounterVal: 123,
	}
	key2 := entryKey{
		metricCategory: untimedMetric,
		metricType:     metric.CounterType,
		idHash:         hash.Murmur3Hash128(um.ID),
	}
	require.NoError(t, m.AddUntimed(um, testStagedMetadatas))
	require.Equal(t, 2, len(m.entries))
	require.Equal(t, 2, m.entryList.Len())
	require.Equal(t, 3, m.metricLists.Len())
	e1, exists1 := m.entries[key]
	e2, exists2 := m.entries[key2]
	require.True(t, exists1)
	require.True(t, exists2)
	require.False(t, e1 == e2)

	// Add a metric with different type and assert a new entry is added.
	metricWithDifferentType := aggregated.ForwardedMetric{
		Type:      metric.GaugeType,
		ID:        am.ID,
		TimeNanos: 1234,
		Values:    []float64{123.456},
	}
	key3 := entryKey{
		metricCategory: forwardedMetric,
		metricType:     metric.GaugeType,
		idHash:         hash.Murmur3Hash128(metricWithDifferentType.ID),
	}
	require.NoError(t, m.AddForwarded(metricWithDifferentType, testForwardMetadata))
	require.Equal(t, 3, len(m.entries))
	require.Equal(t, 3, m.entryList.Len())
	require.Equal(t, 3, m.metricLists.Len())
	e3, exists3 := m.entries[key3]
	require.True(t, exists3)
	require.False(t, e1 == e3)

	// Add a metric with a different id and assert a new entry is added.
	metricWithDifferentID := aggregated.ForwardedMetric{
		Type:      metric.GaugeType,
		ID:        []byte("metricWithDifferentID"),
		TimeNanos: 1234,
		Values:    []float64{123.456},
	}
	key4 := entryKey{
		metricCategory: forwardedMetric,
		metricType:     metric.GaugeType,
		idHash:         hash.Murmur3Hash128(metricWithDifferentID.ID),
	}
	require.NoError(t, m.AddForwarded(metricWithDifferentID, testForwardMetadata))
	require.Equal(t, 4, len(m.entries))
	require.Equal(t, 4, m.entryList.Len())
	require.Equal(t, 3, m.metricLists.Len())
	e4, exists4 := m.entries[key4]
	require.True(t, exists4)
	require.False(t, e1 == e4)
}

func TestMetricMapDeleteExpired(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

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
	opts := testOptions(ctrl).
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
	numEntries := 500
	for i := 0; i < numEntries; i++ {
		key := entryKey{
			metricType: metric.CounterType,
			idHash:     hash.Murmur3Hash128([]byte(fmt.Sprintf("%d", i))),
		}
		if i%2 == 0 {
			m.entries[key] = m.entryList.PushBack(hashedEntry{
				key:   key,
				entry: NewEntry(m.metricLists, runtime.NewOptions(), liveEntryOpts),
			})
		} else {
			m.entries[key] = m.entryList.PushBack(hashedEntry{
				key:   key,
				entry: NewEntry(m.metricLists, runtime.NewOptions(), expiredEntryOpts),
			})
		}
	}

	// Delete expired entries.
	m.tick(opts.EntryCheckInterval())

	// Assert there should be only half of the entries left.
	require.Equal(t, numEntries/2, len(m.entries))
	require.Equal(t, numEntries/2, m.entryList.Len())
	require.Equal(t, len(sleepIntervals), numEntries/defaultSoftDeadlineCheckEvery)
	for k, v := range m.entries {
		e := v.Value.(hashedEntry)
		require.Equal(t, k, e.key)
		require.NotNil(t, e.entry)
	}
}
