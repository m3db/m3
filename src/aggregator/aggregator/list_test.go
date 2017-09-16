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
	"io"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3metrics/metric/aggregated"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/clock"

	"github.com/stretchr/testify/require"
)

func TestMetricListPushBack(t *testing.T) {
	l := newMetricList(testShard, time.Second, testOptions())
	elem := NewCounterElem(nil, policy.EmptyStoragePolicy, policy.DefaultAggregationTypes, l.opts)

	// Push a counter to the list
	e, err := l.PushBack(elem)
	require.NoError(t, err)
	require.Equal(t, 1, l.aggregations.Len())
	require.Equal(t, elem, e.Value.(*CounterElem))

	// Push a counter to a closed list should result in an error.
	l.Lock()
	l.closed = true
	l.Unlock()

	_, err = l.PushBack(elem)
	require.Equal(t, err, errListClosed)
}

func TestMetricListClose(t *testing.T) {
	var (
		registered   int
		unregistered int
	)
	mockFlushManager := &mockFlushManager{
		registerFn:   func(PeriodicFlusher) error { registered++; return nil },
		unregisterFn: func(PeriodicFlusher) error { unregistered++; return nil },
	}
	opts := testOptions().SetFlushManager(mockFlushManager)
	l := newMetricList(testShard, time.Second, opts)
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

func TestMetricListFlushWithRequests(t *testing.T) {
	var (
		now     = time.Unix(12345, 0)
		nowFn   = func() time.Time { return now }
		results []flushBeforeResult
	)
	opts := testOptions().SetClockOptions(clock.NewOptions().SetNowFn(nowFn))
	l := newMetricList(testShard, time.Second, opts)
	l.flushBeforeFn = func(beforeNanos int64, flushType flushType) {
		results = append(results, flushBeforeResult{
			beforeNanos: beforeNanos,
			flushType:   flushType,
		})
	}

	inputs := []struct {
		request  FlushRequest
		expected []flushBeforeResult
	}{
		{
			request: FlushRequest{
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
			request: FlushRequest{
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
			request: FlushRequest{
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
			request: FlushRequest{
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
			request: FlushRequest{
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

func TestMetricListFlushConsumingAndCollectingElems(t *testing.T) {
	var (
		cutoverNanos = int64(0)
		cutoffNanos  = int64(math.MaxInt64)
		bufferLock   sync.Mutex
		buffers      []*RefCountedBuffer
	)
	flushFn := func(buffer *RefCountedBuffer) error {
		bufferLock.Lock()
		buffers = append(buffers, buffer)
		bufferLock.Unlock()
		return nil
	}
	handler := &mockHandler{handleFn: flushFn}

	var now = time.Unix(216, 0).UnixNano()
	nowTs := time.Unix(0, now)
	clockOpts := clock.NewOptions().SetNowFn(func() time.Time {
		return time.Unix(0, atomic.LoadInt64(&now))
	})
	opts := testOptions().
		SetClockOptions(clockOpts).
		SetMinFlushInterval(0).
		SetMaxFlushSize(100).
		SetFlushHandler(handler)

	l := newMetricList(testShard, 0, opts)
	l.resolution = testStoragePolicy.Resolution().Window

	// Intentionally cause a one-time error during encoding.
	var count int
	l.encodeFn = func(mp aggregated.ChunkedMetricWithStoragePolicy) error {
		if count == 0 {
			count++
			return errors.New("foo")
		}
		return l.encoder.EncodeChunkedMetricWithStoragePolicy(mp)
	}

	elemPairs := []testElemPair{
		{
			elem:   NewCounterElem(testCounterID, testStoragePolicy, policy.DefaultAggregationTypes, opts),
			metric: testCounter,
		},
		{
			elem:   NewTimerElem(testBatchTimerID, testStoragePolicy, policy.DefaultAggregationTypes, opts),
			metric: testBatchTimer,
		},
		{
			elem:   NewGaugeElem(testGaugeID, testStoragePolicy, policy.DefaultAggregationTypes, opts),
			metric: testGauge,
		},
	}

	for _, ep := range elemPairs {
		require.NoError(t, ep.elem.AddMetric(nowTs, ep.metric))
		require.NoError(t, ep.elem.AddMetric(nowTs.Add(l.resolution), ep.metric))
		_, err := l.PushBack(ep.elem)
		require.NoError(t, err)
	}

	// Force a flush.
	l.Flush(FlushRequest{
		CutoverNanos: cutoverNanos,
		CutoffNanos:  cutoffNanos,
	})

	// Assert nothing has been collected.
	bufferLock.Lock()
	require.Equal(t, 0, len(buffers))
	bufferLock.Unlock()

	for i := 0; i < 2; i++ {
		// Move the time forward by one aggregation interval.
		nowTs = nowTs.Add(l.resolution)
		atomic.StoreInt64(&now, nowTs.UnixNano())

		// Force a flush.
		l.Flush(FlushRequest{
			CutoverNanos: cutoverNanos,
			CutoffNanos:  cutoffNanos,
		})

		var expected []testAggMetric
		alignedStart := nowTs.Truncate(l.resolution).UnixNano()
		expected = append(expected, expectedAggMetricsForCounter(alignedStart, testStoragePolicy, policy.DefaultAggregationTypes)...)
		expected = append(expected, expectedAggMetricsForTimer(alignedStart, testStoragePolicy, policy.DefaultAggregationTypes)...)
		expected = append(expected, expectedAggMetricsForGauge(alignedStart, testStoragePolicy, policy.DefaultAggregationTypes)...)

		// Skip the first item because we intentionally triggered
		// an encoder error when encoding the first item.
		if i == 0 {
			expected = expected[1:]
		}

		bufferLock.Lock()
		require.NotNil(t, buffers)
		validateBuffers(t, expected, buffers)
		buffers = buffers[:0]
		bufferLock.Unlock()
	}

	// Move the time forward by one aggregation interval.
	nowTs = nowTs.Add(l.resolution)
	atomic.StoreInt64(&now, nowTs.UnixNano())

	// Force a flush.
	l.Flush(FlushRequest{
		CutoverNanos: cutoverNanos,
		CutoffNanos:  cutoffNanos,
	})

	// Assert nothing has been collected.
	bufferLock.Lock()
	require.Equal(t, 0, len(buffers))
	bufferLock.Unlock()
	require.Equal(t, 3, l.aggregations.Len())

	// Mark all elements as tombstoned.
	for e := l.aggregations.Front(); e != nil; e = e.Next() {
		e.Value.(metricElem).MarkAsTombstoned()
	}

	// Move the time forward and force a flush.
	nowTs = nowTs.Add(l.resolution)
	atomic.StoreInt64(&now, nowTs.UnixNano())
	l.Flush(FlushRequest{
		CutoverNanos: cutoverNanos,
		CutoffNanos:  cutoffNanos,
	})

	// Assert all elements have been collected.
	require.Equal(t, 0, l.aggregations.Len())

	require.Equal(t, l.lastFlushedNanos, nowTs.Truncate(l.resolution).UnixNano())
}

func TestMetricListFlushBeforeStale(t *testing.T) {
	opts := testOptions()
	l := newMetricList(testShard, 0, opts)
	l.lastFlushedNanos = 1234
	l.flushBefore(1000, discardType)
	require.Equal(t, int64(1234), l.LastFlushedNanos())
}

func TestMetricLists(t *testing.T) {
	lists := newMetricLists(testShard, testOptions())
	require.False(t, lists.closed)

	// Create a new list.
	l, err := lists.FindOrCreate(time.Second)
	require.NoError(t, err)
	require.NotNil(t, l)
	require.Equal(t, 1, lists.Len())

	// Find the same list.
	l2, err := lists.FindOrCreate(time.Second)
	require.NoError(t, err)
	require.Equal(t, l, l2)
	require.Equal(t, 1, lists.Len())

	// Finding or creating in a closed list should result in an error.
	lists.Close()
	_, err = lists.FindOrCreate(time.Second)
	require.Equal(t, errListsClosed, err)
	require.True(t, lists.closed)

	// Closing a second time should have no impact.
	lists.Close()
	require.True(t, lists.closed)
}

type testElemPair struct {
	elem   metricElem
	metric unaggregated.MetricUnion
}

func validateBuffers(
	t *testing.T,
	expected []testAggMetric,
	buffers []*RefCountedBuffer,
) {
	var decoded []aggregated.MetricWithStoragePolicy
	it := msgpack.NewAggregatedIterator(nil, nil)
	for _, b := range buffers {
		it.Reset(b.Buffer().Buffer())
		for it.Next() {
			rm, sp := it.Value()
			m, err := rm.Metric()
			require.NoError(t, err)
			decoded = append(decoded, aggregated.MetricWithStoragePolicy{
				Metric:        m,
				StoragePolicy: sp,
			})
		}
		b.DecRef()
		require.Equal(t, io.EOF, it.Err())
	}

	require.Equal(t, len(expected), len(decoded))
	for i := 0; i < len(decoded); i++ {
		numBytes := len(expected[i].idPrefix) + len(expected[i].id) + len(expected[i].idSuffix)
		expectedID := make([]byte, numBytes)
		n := copy(expectedID, expected[i].idPrefix)
		n += copy(expectedID[n:], expected[i].id)
		copy(expectedID[n:], expected[i].idSuffix)
		require.Equal(t, expectedID, []byte(decoded[i].ID))
		require.Equal(t, expected[i].timeNanos, decoded[i].TimeNanos)
		require.Equal(t, expected[i].value, decoded[i].Value)
		require.Equal(t, expected[i].sp, decoded[i].StoragePolicy)
	}
}

type flushBeforeResult struct {
	beforeNanos int64
	flushType   flushType
}

type handleFn func(buffer *RefCountedBuffer) error

type mockHandler struct {
	handleFn handleFn
}

func (h *mockHandler) Handle(buffer *RefCountedBuffer) error { return h.handleFn(buffer) }
func (h *mockHandler) Close()                                {}
