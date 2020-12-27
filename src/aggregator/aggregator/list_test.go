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
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3/src/aggregator/aggregator/handler"
	"github.com/m3db/m3/src/aggregator/aggregator/handler/writer"
	"github.com/m3db/m3/src/aggregator/client"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/pipeline"
	"github.com/m3db/m3/src/metrics/pipeline/applied"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/x/clock"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestBaseMetricListPushBackElemWithDefaultPipeline(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	l, err := newBaseMetricList(testShard, time.Second, nil, nil, nil, testOptions(ctrl))
	require.NoError(t, err)
	elem, err := NewCounterElem(nil, policy.EmptyStoragePolicy, aggregation.DefaultTypes, applied.DefaultPipeline, 0, NoPrefixNoSuffix, l.opts)
	require.NoError(t, err)

	// Push a counter to the list.
	e, err := l.PushBack(elem)
	require.NoError(t, err)
	require.Equal(t, 1, l.aggregations.Len())
	require.Equal(t, elem, e.Value.(*CounterElem))
	require.Nil(t, elem.writeForwardedMetricFn)
	require.Nil(t, elem.onForwardedAggregationWrittenFn)

	// Push a counter to a closed list should result in an error.
	l.Lock()
	l.closed = true
	l.Unlock()

	_, err = l.PushBack(elem)
	require.Equal(t, err, errListClosed)
}

func TestBaseMetricListPushBackElemWithForwardingPipeline(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	l, err := newBaseMetricList(testShard, time.Second, nil, nil, nil, testOptions(ctrl))
	require.NoError(t, err)
	elem, err := NewCounterElem(nil, policy.EmptyStoragePolicy, aggregation.DefaultTypes, testPipeline, 0, NoPrefixNoSuffix, l.opts)
	require.NoError(t, err)

	// Push a counter to the list.
	e, err := l.PushBack(elem)
	require.NoError(t, err)
	require.Equal(t, 1, l.aggregations.Len())
	require.Equal(t, elem, e.Value.(*CounterElem))
	require.NotNil(t, elem.writeForwardedMetricFn)
	require.NotNil(t, elem.onForwardedAggregationWrittenFn)
}

func TestBaseMetricListClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testOptions(ctrl)
	l, err := newBaseMetricList(testShard, time.Second, nil, nil, nil, opts)
	require.NoError(t, err)

	l.RLock()
	require.False(t, l.closed)
	l.RUnlock()

	l.Close()
	require.True(t, l.closed)

	// Close for a second time should have no impact.
	l.Close()
	require.True(t, l.closed)
}

func TestBaseMetricListFlushWithRequests(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		now              = time.Unix(12345, 0)
		nowFn            = func() time.Time { return now }
		targetNanosFn    = standardMetricTargetNanos
		isEarlierThanFn  = isStandardMetricEarlierThan
		timestampNanosFn = standardMetricTimestampNanos
		results          []flushBeforeResult
	)
	opts := testOptions(ctrl).SetClockOptions(clock.NewOptions().SetNowFn(nowFn))
	l, err := newBaseMetricList(testShard, time.Second, targetNanosFn, isEarlierThanFn, timestampNanosFn, opts)
	require.NoError(t, err)
	l.flushBeforeFn = func(beforeNanos int64, flushType flushType) {
		results = append(results, flushBeforeResult{
			beforeNanos: beforeNanos,
			flushType:   flushType,
		})
	}

	inputs := []struct {
		request  flushRequest
		expected []flushBeforeResult
	}{
		{
			request: flushRequest{
				CutoverNanos:      20000 * int64(time.Second),
				CutoffNanos:       30000 * int64(time.Second),
				BufferAfterCutoff: time.Second,
			},
			expected: []flushBeforeResult{
				{
					beforeNanos: 12345 * int64(time.Second),
					flushType:   discardType,
				},
			},
		},
		{
			request: flushRequest{
				CutoverNanos:      10000 * int64(time.Second),
				CutoffNanos:       30000 * int64(time.Second),
				BufferAfterCutoff: time.Second,
			},
			expected: []flushBeforeResult{
				{
					beforeNanos: 10000 * int64(time.Second),
					flushType:   discardType,
				},
				{
					beforeNanos: 12345 * int64(time.Second),
					flushType:   consumeType,
				},
			},
		},
		{
			request: flushRequest{
				CutoverNanos:      10000 * int64(time.Second),
				CutoffNanos:       12300 * int64(time.Second),
				BufferAfterCutoff: time.Minute,
			},
			expected: []flushBeforeResult{
				{
					beforeNanos: 10000 * int64(time.Second),
					flushType:   discardType,
				},
				{
					beforeNanos: 12300 * int64(time.Second),
					flushType:   consumeType,
				},
			},
		},
		{
			request: flushRequest{
				CutoverNanos:      10000 * int64(time.Second),
				CutoffNanos:       12300 * int64(time.Second),
				BufferAfterCutoff: 10 * time.Second,
			},
			expected: []flushBeforeResult{
				{
					beforeNanos: 10000 * int64(time.Second),
					flushType:   discardType,
				},
				{
					beforeNanos: 12300 * int64(time.Second),
					flushType:   consumeType,
				},
				{
					beforeNanos: 12335 * int64(time.Second),
					flushType:   discardType,
				},
			},
		},
		{
			request: flushRequest{
				CutoverNanos:      0,
				CutoffNanos:       30000 * int64(time.Second),
				BufferAfterCutoff: time.Second,
			},
			expected: []flushBeforeResult{
				{
					beforeNanos: 12345 * int64(time.Second),
					flushType:   consumeType,
				},
			},
		},
	}
	for _, input := range inputs {
		results = results[:0]
		l.Flush(input.request)
		require.Equal(t, input.expected, results)
	}
}

func TestBaseMetricListFlushBeforeStale(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		targetNanosFn    = standardMetricTargetNanos
		isEarlierThanFn  = isStandardMetricEarlierThan
		timestampNanosFn = standardMetricTimestampNanos
		opts             = testOptions(ctrl)
	)
	l, err := newBaseMetricList(testShard, 0, targetNanosFn, isEarlierThanFn, timestampNanosFn, opts)
	require.NoError(t, err)
	l.lastFlushedNanos = 1234
	l.flushBefore(1000, discardType)
	require.Equal(t, int64(1234), l.LastFlushedNanos())
}

func TestStandardMetricListID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	resolution := 10 * time.Second
	opts := testOptions(ctrl)
	listID := standardMetricListID{resolution: resolution}
	l, err := newStandardMetricList(testShard, listID, opts)
	require.NoError(t, err)

	expectedListID := metricListID{
		listType: standardMetricListType,
		standard: listID,
	}
	require.Equal(t, expectedListID, l.ID())
}

func TestStandardMetricListFlushConsumingAndCollectingLocalMetrics(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		errTestFlush = errors.New("foo")
		cutoverNanos = int64(0)
		cutoffNanos  = int64(math.MaxInt64)
		count        int
		flushLock    sync.Mutex
		flushed      []aggregated.ChunkedMetricWithStoragePolicy
	)

	// Intentionally cause a one-time error during encoding.
	writeFn := func(mp aggregated.ChunkedMetricWithStoragePolicy) error {
		flushLock.Lock()
		defer flushLock.Unlock()

		if count == 0 {
			count++
			return errTestFlush
		}
		flushed = append(flushed, mp)
		return nil
	}
	w := writer.NewMockWriter(ctrl)
	w.EXPECT().Write(gomock.Any()).DoAndReturn(writeFn).AnyTimes()
	w.EXPECT().Flush().Return(nil).AnyTimes()
	handler := handler.NewMockHandler(ctrl)
	handler.EXPECT().NewWriter(gomock.Any()).Return(w, nil).AnyTimes()

	var (
		now        = time.Unix(216, 0).UnixNano()
		nowTs      = time.Unix(0, now)
		resolution = testStoragePolicy.Resolution().Window
	)
	clockOpts := clock.NewOptions().SetNowFn(func() time.Time {
		return time.Unix(0, atomic.LoadInt64(&now))
	})
	opts := testOptions(ctrl).
		SetClockOptions(clockOpts).
		SetFlushHandler(handler)

	listID := standardMetricListID{resolution: resolution}
	l, err := newStandardMetricList(testShard, listID, opts)
	require.NoError(t, err)

	// Intentionally cause a one-time error during encoding.
	elemPairs := []struct {
		elem   metricElem
		metric unaggregated.MetricUnion
	}{
		{
			elem:   MustNewCounterElem(testCounterID, testStoragePolicy, aggregation.DefaultTypes, applied.DefaultPipeline, 0, WithPrefixWithSuffix, opts),
			metric: testCounter,
		},
		{
			elem:   MustNewTimerElem(testBatchTimerID, testStoragePolicy, aggregation.DefaultTypes, applied.DefaultPipeline, 0, WithPrefixWithSuffix, opts),
			metric: testBatchTimer,
		},
		{
			elem:   MustNewGaugeElem(testGaugeID, testStoragePolicy, aggregation.DefaultTypes, applied.DefaultPipeline, 0, WithPrefixWithSuffix, opts),
			metric: testGauge,
		},
	}

	for _, ep := range elemPairs {
		require.NoError(t, ep.elem.AddUnion(nowTs, ep.metric))
		require.NoError(t, ep.elem.AddUnion(nowTs.Add(l.resolution), ep.metric))
		_, err := l.PushBack(ep.elem)
		require.NoError(t, err)
	}

	// Force a flush.
	l.Flush(flushRequest{
		CutoverNanos: cutoverNanos,
		CutoffNanos:  cutoffNanos,
	})

	// Assert nothing has been flushed.
	flushLock.Lock()
	require.Equal(t, 0, len(flushed))
	flushLock.Unlock()

	for i := 0; i < 2; i++ {
		// Move the time forward by one aggregation interval.
		nowTs = nowTs.Add(l.resolution)
		atomic.StoreInt64(&now, nowTs.UnixNano())

		// Force a flush.
		l.Flush(flushRequest{
			CutoverNanos: cutoverNanos,
			CutoffNanos:  cutoffNanos,
		})

		var expected []testLocalMetricWithMetadata
		alignedStart := nowTs.Truncate(l.resolution).UnixNano()
		expected = append(expected, expectedLocalMetricsForCounter(alignedStart, testStoragePolicy, aggregation.DefaultTypes)...)
		expected = append(expected, expectedLocalMetricsForTimer(alignedStart, testStoragePolicy, aggregation.DefaultTypes)...)
		expected = append(expected, expectedLocalMetricsForGauge(alignedStart, testStoragePolicy, aggregation.DefaultTypes)...)

		// Skip the first item because we intentionally triggered
		// an encoder error when encoding the first item.
		if i == 0 {
			expected = expected[1:]
		}

		flushLock.Lock()
		require.NotNil(t, flushed)
		validateLocalFlushed(t, expected, flushed)
		flushed = flushed[:0]
		flushLock.Unlock()
	}

	// Move the time forward by one aggregation interval.
	nowTs = nowTs.Add(l.resolution)
	atomic.StoreInt64(&now, nowTs.UnixNano())

	// Force a flush.
	l.Flush(flushRequest{
		CutoverNanos: cutoverNanos,
		CutoffNanos:  cutoffNanos,
	})

	// Assert nothing has been flushed.
	flushLock.Lock()
	require.Equal(t, 0, len(flushed))
	flushLock.Unlock()
	require.Equal(t, 3, l.aggregations.Len())

	// Mark all elements as tombstoned.
	for e := l.aggregations.Front(); e != nil; e = e.Next() {
		e.Value.(metricElem).MarkAsTombstoned()
	}

	// Move the time forward and force a flush.
	nowTs = nowTs.Add(l.resolution)
	atomic.StoreInt64(&now, nowTs.UnixNano())
	l.Flush(flushRequest{
		CutoverNanos: cutoverNanos,
		CutoffNanos:  cutoffNanos,
	})

	// Assert all elements have been collected.
	require.Equal(t, 0, l.aggregations.Len())

	require.Equal(t, l.lastFlushedNanos, nowTs.UnixNano())
}

func TestStandardMetricListClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		registered   int
		unregistered int
	)
	flushManager := NewMockFlushManager(ctrl)
	flushManager.EXPECT().
		Register(gomock.Any()).
		DoAndReturn(func(flushingMetricList) error {
			registered++
			return nil
		})
	flushManager.EXPECT().
		Unregister(gomock.Any()).
		DoAndReturn(func(flushingMetricList) error {
			unregistered++
			return nil
		})

	resolution := 10 * time.Second
	opts := testOptions(ctrl).SetFlushManager(flushManager)
	listID := standardMetricListID{resolution: resolution}
	l, err := newStandardMetricList(testShard, listID, opts)
	require.NoError(t, err)

	l.RLock()
	require.False(t, l.closed)
	l.RUnlock()
	require.Equal(t, 1, registered)

	l.Close()
	require.True(t, l.closed)
	require.Equal(t, 1, unregistered)

	// Close for a second time should have no impact.
	l.Close()
	require.True(t, l.closed)
	require.Equal(t, 1, registered)
	require.Equal(t, 1, unregistered)
}

func TestTimedMetricListID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	resolution := 10 * time.Second
	opts := testOptions(ctrl)
	listID := timedMetricListID{resolution: resolution}
	l, err := newTimedMetricList(testShard, listID, opts)
	require.NoError(t, err)

	expectedListID := metricListID{
		listType: timedMetricListType,
		timed:    listID,
	}
	require.Equal(t, expectedListID, l.ID())
}

func TestTimedMetricListFlushConsumingAndCollectingTimedMetrics(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		errTestFlush = errors.New("foo")
		cutoverNanos = int64(0)
		cutoffNanos  = int64(math.MaxInt64)
		count        int
		flushLock    sync.Mutex
		flushed      []aggregated.ChunkedMetricWithStoragePolicy
	)

	// Intentionally cause a one-time error during encoding.
	writeFn := func(mp aggregated.ChunkedMetricWithStoragePolicy) error {
		flushLock.Lock()
		defer flushLock.Unlock()

		if count == 0 {
			count++
			return errTestFlush
		}
		flushed = append(flushed, mp)
		return nil
	}

	w := writer.NewMockWriter(ctrl)
	w.EXPECT().Write(gomock.Any()).DoAndReturn(writeFn).AnyTimes()
	w.EXPECT().Flush().Return(nil).AnyTimes()
	handler := handler.NewMockHandler(ctrl)
	handler.EXPECT().NewWriter(gomock.Any()).Return(w, nil).AnyTimes()

	var (
		now                          = time.Unix(216, 0).UnixNano()
		nowTs                        = time.Unix(0, now)
		resolution                   = testStoragePolicy.Resolution().Window
		alignedTimeNanos             = nowTs.Truncate(resolution).UnixNano()
		bufferPast                   = 9 * time.Second
		timedAggregationBufferPastFn = func(time.Duration) time.Duration { return bufferPast }
	)
	clockOpts := clock.NewOptions().SetNowFn(func() time.Time {
		return time.Unix(0, atomic.LoadInt64(&now))
	})
	opts := testOptions(ctrl).
		SetClockOptions(clockOpts).
		SetFlushHandler(handler).
		SetBufferForPastTimedMetricFn(timedAggregationBufferPastFn)

	listID := timedMetricListID{
		resolution: resolution,
	}
	l, err := newTimedMetricList(testShard, listID, opts)
	require.NoError(t, err)

	elemPairs := []struct {
		elem   metricElem
		metric aggregated.Metric
	}{
		{
			elem: MustNewCounterElem([]byte("testTimedCounter"), testStoragePolicy, aggregation.DefaultTypes, applied.Pipeline{}, testNumForwardedTimes, NoPrefixNoSuffix, opts),
			metric: aggregated.Metric{
				Type:      metric.CounterType,
				ID:        []byte("testTimedCounter"),
				TimeNanos: alignedTimeNanos,
				Value:     123,
			},
		},
		{
			elem: MustNewGaugeElem([]byte("testTimedGauge"), testStoragePolicy, aggregation.DefaultTypes, applied.Pipeline{}, testNumForwardedTimes, NoPrefixNoSuffix, opts),
			metric: aggregated.Metric{
				Type:      metric.GaugeType,
				ID:        []byte("testTimedGauge"),
				TimeNanos: alignedTimeNanos,
				Value:     1.762,
			},
		},
	}

	for _, ep := range elemPairs {
		require.NoError(t, ep.elem.AddValue(time.Unix(0, ep.metric.TimeNanos), ep.metric.Value))
		require.NoError(t, ep.elem.AddValue(time.Unix(0, ep.metric.TimeNanos).Add(l.resolution), ep.metric.Value))
		_, err := l.PushBack(ep.elem)
		require.NoError(t, err)
	}

	require.Equal(t, 0, l.forwardedWriter.Len())

	// Force a flush.
	l.Flush(flushRequest{
		CutoverNanos: cutoverNanos,
		CutoffNanos:  cutoffNanos,
	})

	// Assert nothing has been flushed.
	flushLock.Lock()
	require.Equal(t, 0, len(flushed))
	flushLock.Unlock()

	for {
		if nowTs.UnixNano() > alignedTimeNanos+bufferPast.Nanoseconds() {
			break
		}
		// Move the time forward by one aggregation interval.
		nowTs = nowTs.Add(l.resolution)
		atomic.StoreInt64(&now, nowTs.UnixNano())
		// Force a flush.
		l.Flush(flushRequest{
			CutoverNanos: cutoverNanos,
			CutoffNanos:  cutoffNanos,
		})
		// Assert nothing has been flushed.
		flushLock.Lock()
		require.Equal(t, 0, len(flushed))
		flushLock.Unlock()

	}
	for i := 0; i < 2; i++ {
		// Move the time forward by one aggregation interval.
		nowTs = nowTs.Add(l.resolution)
		atomic.StoreInt64(&now, nowTs.UnixNano())

		// Force a flush.
		l.Flush(flushRequest{
			CutoverNanos: cutoverNanos,
			CutoffNanos:  cutoffNanos,
		})

		var expected []aggregated.ChunkedMetricWithStoragePolicy
		alignedStart := (nowTs.Add(-bufferPast)).Truncate(l.resolution).UnixNano()
		for _, ep := range elemPairs {
			expected = append(expected, aggregated.ChunkedMetricWithStoragePolicy{
				ChunkedMetric: aggregated.ChunkedMetric{
					ChunkedID: id.ChunkedID{
						Data: ep.metric.ID,
					},
					Type:      ep.metric.Type,
					TimeNanos: alignedStart,
					Value:     ep.metric.Value,
				},
				StoragePolicy: testStoragePolicy,
			})
		}

		// Skip the first item because we intentionally triggered
		// an encoder error when encoding the first item.
		if i == 0 {
			expected = expected[1:]
		}

		flushLock.Lock()
		require.NotNil(t, flushed)
		require.Equal(t, expected, flushed)
		flushed = flushed[:0]
		flushLock.Unlock()
	}

	// Move the time forward by one aggregation interval.
	nowTs = nowTs.Add(l.resolution)
	atomic.StoreInt64(&now, nowTs.UnixNano())

	// Force a flush.
	l.Flush(flushRequest{
		CutoverNanos: cutoverNanos,
		CutoffNanos:  cutoffNanos,
	})

	// Assert nothing has been flushed.
	flushLock.Lock()
	require.Equal(t, 0, len(flushed))
	flushLock.Unlock()
	require.Equal(t, 2, l.aggregations.Len())

	// Mark all elements as tombstoned.
	for e := l.aggregations.Front(); e != nil; e = e.Next() {
		e.Value.(metricElem).MarkAsTombstoned()
	}

	// Move the time forward and force a flush.
	nowTs = nowTs.Add(l.resolution)
	atomic.StoreInt64(&now, nowTs.UnixNano())
	l.Flush(flushRequest{
		CutoverNanos: cutoverNanos,
		CutoffNanos:  cutoffNanos,
	})

	// Assert all elements have been collected.
	require.Equal(t, 0, l.aggregations.Len())

	// Assert there are no more forwarded metrics tracked by the writer.
	require.Equal(t, 0, l.forwardedWriter.Len())

	require.Equal(t, l.lastFlushedNanos, nowTs.UnixNano()-bufferPast.Nanoseconds())
}

func TestTimedMetricListClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		registered   int
		unregistered int
	)
	flushManager := NewMockFlushManager(ctrl)
	flushManager.EXPECT().
		Register(gomock.Any()).
		DoAndReturn(func(flushingMetricList) error {
			registered++
			return nil
		})
	flushManager.EXPECT().
		Unregister(gomock.Any()).
		DoAndReturn(func(flushingMetricList) error {
			unregistered++
			return nil
		})

	resolution := 10 * time.Second
	opts := testOptions(ctrl).SetFlushManager(flushManager)
	listID := timedMetricListID{resolution: resolution}
	l, err := newTimedMetricList(testShard, listID, opts)
	require.NoError(t, err)

	l.RLock()
	require.False(t, l.closed)
	l.RUnlock()
	require.Equal(t, 1, registered)

	l.Close()
	require.True(t, l.closed)
	require.Equal(t, 1, unregistered)

	// Close for a second time should have no impact.
	l.Close()
	require.True(t, l.closed)
	require.Equal(t, 1, registered)
	require.Equal(t, 1, unregistered)
}

func TestForwardedMetricListID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	resolution := 10 * time.Second
	opts := testOptions(ctrl)
	listID := forwardedMetricListID{resolution: resolution, numForwardedTimes: testNumForwardedTimes}
	l, err := newForwardedMetricList(testShard, listID, opts)
	require.NoError(t, err)

	expectedListID := metricListID{
		listType:  forwardedMetricListType,
		forwarded: listID,
	}
	require.Equal(t, expectedListID, l.ID())
}

func TestForwardedMetricListFlushOffset(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	maxForwardingDelayFn := func(resolution time.Duration, numForwardedTimes int) time.Duration {
		return resolution + time.Second*time.Duration(numForwardedTimes)
	}
	resolution := 10 * time.Second
	opts := testOptions(ctrl).SetMaxAllowedForwardingDelayFn(maxForwardingDelayFn)
	listID := forwardedMetricListID{resolution: resolution, numForwardedTimes: 2}
	l, err := newForwardedMetricList(testShard, listID, opts)
	require.NoError(t, err)

	require.Equal(t, 2*time.Second, l.FlushOffset())
}

func TestForwardedMetricListFlushConsumingAndCollectingForwardedMetrics(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		errTestWrite = errors.New("foo")
		cutoverNanos = int64(0)
		cutoffNanos  = int64(math.MaxInt64)
		count        int
		flushLock    sync.Mutex
		flushed      []aggregated.ForwardedMetricWithMetadata
	)

	// Intentionally cause a one-time error during encoding.
	writeFn := func(metric aggregated.ForwardedMetric, meta metadata.ForwardMetadata) error {
		flushLock.Lock()
		defer flushLock.Unlock()

		if count == 0 {
			count++
			return errTestWrite
		}
		flushed = append(flushed, aggregated.ForwardedMetricWithMetadata{
			ForwardedMetric: metric,
			ForwardMetadata: meta,
		})
		return nil
	}

	client := client.NewMockAdminClient(ctrl)
	client.EXPECT().WriteForwarded(gomock.Any(), gomock.Any()).DoAndReturn(writeFn).MinTimes(1)
	client.EXPECT().Flush().Return(nil).MinTimes(1)

	var (
		now                  = time.Unix(216, 0).UnixNano()
		nowTs                = time.Unix(0, now)
		resolution           = testStoragePolicy.Resolution().Window
		alignedTimeNanos     = nowTs.Truncate(resolution).UnixNano()
		maxLatenessAllowed   = 9 * time.Second
		maxForwardingDelayFn = func(time.Duration, int) time.Duration { return maxLatenessAllowed }
	)
	clockOpts := clock.NewOptions().SetNowFn(func() time.Time {
		return time.Unix(0, atomic.LoadInt64(&now))
	})
	opts := testOptions(ctrl).
		SetClockOptions(clockOpts).
		SetAdminClient(client).
		SetMaxAllowedForwardingDelayFn(maxForwardingDelayFn)

	listID := forwardedMetricListID{
		resolution:        resolution,
		numForwardedTimes: testNumForwardedTimes,
	}
	l, err := newForwardedMetricList(testShard, listID, opts)
	require.NoError(t, err)

	sourceID := uint32(testShard)
	pipeline := applied.NewPipeline([]applied.OpUnion{
		{
			Type: pipeline.RollupOpType,
			Rollup: applied.RollupOp{
				ID:            []byte("foo.bar"),
				AggregationID: aggregation.MustCompressTypes(aggregation.Max),
			},
		},
	})
	elemPairs := []struct {
		elem   metricElem
		metric aggregated.ForwardedMetric
	}{
		{
			elem: MustNewCounterElem([]byte("testForwardedCounter"), testStoragePolicy, aggregation.DefaultTypes, pipeline, testNumForwardedTimes, NoPrefixNoSuffix, opts),
			metric: aggregated.ForwardedMetric{
				Type:      metric.CounterType,
				ID:        []byte("testForwardedCounter"),
				TimeNanos: alignedTimeNanos,
				Values:    []float64{123},
			},
		},
		{
			elem: MustNewGaugeElem([]byte("testForwardedGauge"), testStoragePolicy, aggregation.DefaultTypes, pipeline, testNumForwardedTimes, NoPrefixNoSuffix, opts),
			metric: aggregated.ForwardedMetric{
				Type:      metric.GaugeType,
				ID:        []byte("testForwardedGauge"),
				TimeNanos: alignedTimeNanos,
				Values:    []float64{1.762},
			},
		},
	}

	for _, ep := range elemPairs {
		require.NoError(t, ep.elem.AddUnique(time.Unix(0, ep.metric.TimeNanos), ep.metric.Values, sourceID))
		require.NoError(t, ep.elem.AddUnique(time.Unix(0, ep.metric.TimeNanos).Add(l.resolution), ep.metric.Values, sourceID))
		_, err := l.PushBack(ep.elem)
		require.NoError(t, err)
	}

	require.Equal(t, len(elemPairs), l.forwardedWriter.Len())

	// Force a flush.
	l.Flush(flushRequest{
		CutoverNanos: cutoverNanos,
		CutoffNanos:  cutoffNanos,
	})

	// Assert nothing has been flushed.
	flushLock.Lock()
	require.Equal(t, 0, len(flushed))
	flushLock.Unlock()

	for i := 0; i < 2; i++ {
		// Move the time forward by one aggregation interval.
		nowTs = nowTs.Add(l.resolution)
		atomic.StoreInt64(&now, nowTs.UnixNano())

		// Force a flush.
		l.Flush(flushRequest{
			CutoverNanos: cutoverNanos,
			CutoffNanos:  cutoffNanos,
		})

		var expected []aggregated.ForwardedMetricWithMetadata
		alignedStart := (nowTs.Add(-maxLatenessAllowed)).Truncate(l.resolution).UnixNano()
		for _, ep := range elemPairs {
			expectedMetric := aggregated.ForwardedMetric{
				Type:      ep.metric.Type,
				ID:        []byte("foo.bar"),
				TimeNanos: alignedStart,
				Values:    ep.metric.Values,
			}
			metadata := metadata.ForwardMetadata{
				AggregationID:     aggregation.MustCompressTypes(aggregation.Max),
				StoragePolicy:     testStoragePolicy,
				Pipeline:          applied.NewPipeline([]applied.OpUnion{}),
				SourceID:          sourceID,
				NumForwardedTimes: testNumForwardedTimes + 1,
			}
			expected = append(expected, aggregated.ForwardedMetricWithMetadata{
				ForwardedMetric: expectedMetric,
				ForwardMetadata: metadata,
			})
		}

		// Skip the first item because we intentionally triggered
		// an encoder error when encoding the first item.
		if i == 0 {
			expected = expected[1:]
		}

		flushLock.Lock()
		require.NotNil(t, flushed)
		require.Equal(t, expected, flushed)
		flushed = flushed[:0]
		flushLock.Unlock()
	}

	// Move the time forward by one aggregation interval.
	nowTs = nowTs.Add(l.resolution)
	atomic.StoreInt64(&now, nowTs.UnixNano())

	// Force a flush.
	l.Flush(flushRequest{
		CutoverNanos: cutoverNanos,
		CutoffNanos:  cutoffNanos,
	})

	// Assert nothing has been flushed.
	flushLock.Lock()
	require.Equal(t, 0, len(flushed))
	flushLock.Unlock()
	require.Equal(t, 2, l.aggregations.Len())

	// Mark all elements as tombstoned.
	for e := l.aggregations.Front(); e != nil; e = e.Next() {
		e.Value.(metricElem).MarkAsTombstoned()
	}

	// Move the time forward and force a flush.
	nowTs = nowTs.Add(l.resolution)
	atomic.StoreInt64(&now, nowTs.UnixNano())
	l.Flush(flushRequest{
		CutoverNanos: cutoverNanos,
		CutoffNanos:  cutoffNanos,
	})

	// Assert all elements have been collected.
	require.Equal(t, 0, l.aggregations.Len())

	// Assert there are no more forwarded metrics tracked by the writer.
	require.Equal(t, 0, l.forwardedWriter.Len())

	require.Equal(t, l.lastFlushedNanos, nowTs.UnixNano()-maxLatenessAllowed.Nanoseconds())
}

func TestForwardedMetricListLastStepLocalFlush(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		errTestFlush = errors.New("foo")
		cutoverNanos = int64(0)
		cutoffNanos  = int64(math.MaxInt64)
		count        int
		flushLock    sync.Mutex
		flushed      []aggregated.ChunkedMetricWithStoragePolicy
	)

	// Intentionally cause a one-time error during encoding.
	writeFn := func(mp aggregated.ChunkedMetricWithStoragePolicy) error {
		flushLock.Lock()
		defer flushLock.Unlock()

		if count == 0 {
			count++
			return errTestFlush
		}
		flushed = append(flushed, mp)
		return nil
	}

	w := writer.NewMockWriter(ctrl)
	w.EXPECT().Write(gomock.Any()).DoAndReturn(writeFn).AnyTimes()
	w.EXPECT().Flush().Return(nil).AnyTimes()
	handler := handler.NewMockHandler(ctrl)
	handler.EXPECT().NewWriter(gomock.Any()).Return(w, nil).AnyTimes()

	var (
		now                  = time.Unix(216, 0).UnixNano()
		nowTs                = time.Unix(0, now)
		resolution           = testStoragePolicy.Resolution().Window
		alignedTimeNanos     = nowTs.Truncate(resolution).UnixNano()
		maxLatenessAllowed   = 9 * time.Second
		maxForwardingDelayFn = func(time.Duration, int) time.Duration { return maxLatenessAllowed }
	)
	clockOpts := clock.NewOptions().SetNowFn(func() time.Time {
		return time.Unix(0, atomic.LoadInt64(&now))
	})
	opts := testOptions(ctrl).
		SetClockOptions(clockOpts).
		SetFlushHandler(handler).
		SetMaxAllowedForwardingDelayFn(maxForwardingDelayFn)

	listID := forwardedMetricListID{
		resolution:        resolution,
		numForwardedTimes: testNumForwardedTimes,
	}
	l, err := newForwardedMetricList(testShard, listID, opts)
	require.NoError(t, err)

	sourceID := uint32(testShard)
	elemPairs := []struct {
		elem           metricElem
		expectedPrefix []byte
		metric         aggregated.ForwardedMetric
	}{
		{
			elem:           MustNewCounterElem([]byte("testForwardedCounter"), testStoragePolicy, aggregation.DefaultTypes, applied.DefaultPipeline, testNumForwardedTimes, WithPrefixWithSuffix, opts),
			expectedPrefix: opts.FullCounterPrefix(),
			metric: aggregated.ForwardedMetric{
				Type:      metric.CounterType,
				ID:        []byte("testForwardedCounter"),
				TimeNanos: alignedTimeNanos,
				Values:    []float64{123},
			},
		},
		{
			elem:           MustNewGaugeElem([]byte("testForwardedGauge"), testStoragePolicy, aggregation.DefaultTypes, applied.DefaultPipeline, testNumForwardedTimes, WithPrefixWithSuffix, opts),
			expectedPrefix: opts.FullGaugePrefix(),
			metric: aggregated.ForwardedMetric{
				Type:      metric.GaugeType,
				ID:        []byte("testForwardedGauge"),
				TimeNanos: alignedTimeNanos,
				Values:    []float64{1.762},
			},
		},
	}

	for _, ep := range elemPairs {
		require.NoError(t, ep.elem.AddUnique(time.Unix(0, ep.metric.TimeNanos), ep.metric.Values, sourceID))
		require.NoError(t, ep.elem.AddUnique(time.Unix(0, ep.metric.TimeNanos).Add(l.resolution), ep.metric.Values, sourceID))
		_, err := l.PushBack(ep.elem)
		require.NoError(t, err)
	}

	require.Equal(t, 0, l.forwardedWriter.Len())

	// Force a flush.
	l.Flush(flushRequest{
		CutoverNanos: cutoverNanos,
		CutoffNanos:  cutoffNanos,
	})

	// Assert nothing has been flushed.
	flushLock.Lock()
	require.Equal(t, 0, len(flushed))
	flushLock.Unlock()

	for i := 0; i < 2; i++ {
		// Move the time forward by one aggregation interval.
		nowTs = nowTs.Add(l.resolution)
		atomic.StoreInt64(&now, nowTs.UnixNano())

		// Force a flush.
		l.Flush(flushRequest{
			CutoverNanos: cutoverNanos,
			CutoffNanos:  cutoffNanos,
		})

		var expected []aggregated.ChunkedMetricWithStoragePolicy
		alignedStart := (nowTs.Add(-maxLatenessAllowed)).Truncate(l.resolution).UnixNano()
		for _, ep := range elemPairs {
			expected = append(expected, aggregated.ChunkedMetricWithStoragePolicy{
				ChunkedMetric: aggregated.ChunkedMetric{
					ChunkedID: id.ChunkedID{
						Prefix: ep.expectedPrefix,
						Data:   ep.metric.ID,
					},
					Type:      ep.metric.Type,
					TimeNanos: alignedStart,
					Value:     ep.metric.Values[0],
				},
				StoragePolicy: testStoragePolicy,
			})
		}

		// Skip the first item because we intentionally triggered
		// an encoder error when encoding the first item.
		if i == 0 {
			expected = expected[1:]
		}

		flushLock.Lock()
		require.NotNil(t, flushed)
		require.Equal(t, expected, flushed)
		flushed = flushed[:0]
		flushLock.Unlock()
	}

	// Move the time forward by one aggregation interval.
	nowTs = nowTs.Add(l.resolution)
	atomic.StoreInt64(&now, nowTs.UnixNano())

	// Force a flush.
	l.Flush(flushRequest{
		CutoverNanos: cutoverNanos,
		CutoffNanos:  cutoffNanos,
	})

	// Assert nothing has been flushed.
	flushLock.Lock()
	require.Equal(t, 0, len(flushed))
	flushLock.Unlock()
	require.Equal(t, 2, l.aggregations.Len())

	// Mark all elements as tombstoned.
	for e := l.aggregations.Front(); e != nil; e = e.Next() {
		e.Value.(metricElem).MarkAsTombstoned()
	}

	// Move the time forward and force a flush.
	nowTs = nowTs.Add(l.resolution)
	atomic.StoreInt64(&now, nowTs.UnixNano())
	l.Flush(flushRequest{
		CutoverNanos: cutoverNanos,
		CutoffNanos:  cutoffNanos,
	})

	// Assert all elements have been collected.
	require.Equal(t, 0, l.aggregations.Len())

	// Assert there are no more forwarded metrics tracked by the writer.
	require.Equal(t, 0, l.forwardedWriter.Len())

	require.Equal(t, l.lastFlushedNanos, nowTs.UnixNano()-maxLatenessAllowed.Nanoseconds())
}

func TestForwardedMetricListClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		registered   int
		unregistered int
	)
	flushManager := NewMockFlushManager(ctrl)
	flushManager.EXPECT().
		Register(gomock.Any()).
		DoAndReturn(func(flushingMetricList) error {
			registered++
			return nil
		})
	flushManager.EXPECT().
		Unregister(gomock.Any()).
		DoAndReturn(func(flushingMetricList) error {
			unregistered++
			return nil
		})

	resolution := 10 * time.Second
	opts := testOptions(ctrl).SetFlushManager(flushManager)
	listID := forwardedMetricListID{resolution: resolution, numForwardedTimes: testNumForwardedTimes}
	l, err := newForwardedMetricList(testShard, listID, opts)
	require.NoError(t, err)

	l.RLock()
	require.False(t, l.closed)
	l.RUnlock()
	require.Equal(t, 1, registered)

	l.Close()
	require.True(t, l.closed)
	require.Equal(t, 1, unregistered)

	// Close for a second time should have no impact.
	l.Close()
	require.True(t, l.closed)
	require.Equal(t, 1, registered)
	require.Equal(t, 1, unregistered)
}

func TestMetricLists(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testOptions(ctrl)
	lists := newMetricLists(testShard, opts)
	require.False(t, lists.closed)

	// Create a new standard metric list.
	listID := standardMetricListID{resolution: time.Second}.toMetricListID()
	sl, err := lists.FindOrCreate(listID)
	require.NoError(t, err)
	require.NotNil(t, sl)
	require.Equal(t, 1, lists.Len())

	// Find the same standard metric list.
	sl2, err := lists.FindOrCreate(listID)
	require.NoError(t, err)
	require.True(t, sl == sl2)
	require.Equal(t, 1, lists.Len())

	// Create a new forwarded metric list.
	listID = forwardedMetricListID{
		resolution:        10 * time.Second,
		numForwardedTimes: testNumForwardedTimes,
	}.toMetricListID()
	fl, err := lists.FindOrCreate(listID)
	require.NoError(t, err)
	require.NotNil(t, fl)
	require.Equal(t, 2, lists.Len())

	// Find the same forwarded metric list.
	fl2, err := lists.FindOrCreate(listID)
	require.NoError(t, err)
	require.True(t, fl == fl2)
	require.Equal(t, 2, lists.Len())

	// Create a new timed metric list.
	listID = timedMetricListID{
		resolution: time.Minute,
	}.toMetricListID()
	tl, err := lists.FindOrCreate(listID)
	require.NoError(t, err)
	require.NotNil(t, tl)
	require.Equal(t, 3, lists.Len())

	// Find the same timed metric list.
	tl2, err := lists.FindOrCreate(listID)
	require.NoError(t, err)
	require.True(t, tl == tl2)
	require.Equal(t, 3, lists.Len())

	// Perform a tick.
	tickRes := lists.Tick()
	expectedRes := listsTickResult{
		standard: map[time.Duration]int{
			time.Second: 0,
		},
		forwarded: map[time.Duration]int{
			10 * time.Second: 0,
		},
		timed: map[time.Duration]int{
			time.Minute: 0,
		},
	}
	require.Equal(t, expectedRes, tickRes)

	// Finding or creating in a closed list should result in an error.
	lists.Close()
	_, err = lists.FindOrCreate(listID)
	require.Equal(t, errListsClosed, err)
	require.True(t, lists.closed)

	// Closing a second time should have no impact.
	lists.Close()
	require.True(t, lists.closed)
}

func validateLocalFlushed(
	t *testing.T,
	expected []testLocalMetricWithMetadata,
	flushed []aggregated.ChunkedMetricWithStoragePolicy,
) {
	require.Equal(t, len(expected), len(flushed))
	for i := 0; i < len(flushed); i++ {
		require.Equal(t, expected[i].idPrefix, flushed[i].ChunkedID.Prefix)
		require.Equal(t, []byte(expected[i].id), flushed[i].ChunkedID.Data)
		require.Equal(t, expected[i].idSuffix, flushed[i].ChunkedID.Suffix)
		require.Equal(t, expected[i].timeNanos, flushed[i].TimeNanos)
		require.Equal(t, expected[i].value, flushed[i].Value)
		require.Equal(t, expected[i].sp, flushed[i].StoragePolicy)
	}
}

type flushBeforeResult struct {
	beforeNanos int64
	flushType   flushType
}
