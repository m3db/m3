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

type testElemPair struct {
	elem   metricElem
	metric unaggregated.MetricUnion
}

func validateBuffers(
	t *testing.T,
	expected []testAggMetric,
	buffers []msgpack.BufferedEncoder,
) {
	var decoded []aggregated.MetricWithPolicy
	it := msgpack.NewAggregatedIterator(nil, nil)
	for _, b := range buffers {
		it.Reset(b.Buffer)
		for it.Next() {
			rm, p := it.Value()
			m, err := rm.Metric()
			require.NoError(t, err)
			decoded = append(decoded, aggregated.MetricWithPolicy{
				Metric: m,
				Policy: p,
			})
		}
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
		require.Equal(t, expected[i].timestamp, decoded[i].Timestamp)
		require.Equal(t, expected[i].value, decoded[i].Value)
		require.Equal(t, expected[i].policy, decoded[i].Policy)
	}
}

func TestMetricListPushBack(t *testing.T) {
	l := newMetricList(time.Second, testOptions())
	elem := NewCounterElem(nil, policy.Policy{}, l.opts)

	// Push a counter to the list
	e, err := l.PushBack(elem)
	require.NoError(t, err)
	require.Equal(t, 1, l.aggregations.Len())
	require.Equal(t, elem, e.Value.(*CounterElem))

	// Push a counter to a closed list should result in an error
	l.Lock()
	l.closed = true
	l.Unlock()

	_, err = l.PushBack(elem)
	require.Equal(t, err, errListClosed)
}

func TestMetricListClose(t *testing.T) {
	l := newMetricList(time.Second, testOptions())
	l.RLock()
	require.False(t, l.closed)
	l.RUnlock()

	l.Close()
	require.True(t, l.closed)

	// Close for a second time should have no impact
	l.Close()
	require.True(t, l.closed)
}

func TestMetricListTick(t *testing.T) {
	var (
		bufferLock sync.Mutex
		buffers    []msgpack.BufferedEncoder
	)
	flushFn := func(buffer msgpack.BufferedEncoder) error {
		bufferLock.Lock()
		buffers = append(buffers, buffer)
		bufferLock.Unlock()
		return nil
	}

	var now = time.Unix(216, 0).UnixNano()
	nowTs := time.Unix(0, now)
	clockOpts := clock.NewOptions().SetNowFn(func() time.Time {
		return time.Unix(0, atomic.LoadInt64(&now))
	})
	opts := testOptions().
		SetClockOptions(clockOpts).
		SetMinFlushInterval(0).
		SetMaxFlushSize(100).
		SetFlushFn(flushFn)
	l := newMetricList(0, opts)
	l.resolution = testPolicy.Resolution.Window
	l.waitForFn = func(time.Duration) <-chan time.Time {
		c := make(chan time.Time)
		close(c)
		return c
	}

	// Intentionally cause a one-time error during encoding
	var count int
	l.encodeFn = func(mp aggregated.ChunkedMetricWithPolicy) error {
		if count == 0 {
			count++
			return errors.New("foo")
		}
		return l.encoder.EncodeChunkedMetricWithPolicy(mp)
	}

	elemPairs := []testElemPair{
		{
			elem:   NewCounterElem(testID, testPolicy, opts),
			metric: testCounter,
		},
		{
			elem:   NewTimerElem(testID, testPolicy, opts),
			metric: testBatchTimer,
		},
		{
			elem:   NewGaugeElem(testID, testPolicy, opts),
			metric: testGauge,
		},
	}
	for _, ep := range elemPairs {
		require.NoError(t, ep.elem.AddMetric(nowTs, ep.metric))
		require.NoError(t, ep.elem.AddMetric(nowTs.Add(testPolicy.Resolution.Window), ep.metric))
		_, err := l.PushBack(ep.elem)
		require.NoError(t, err)
	}

	// Force a tick
	l.tickInternal()

	// Assert nothing has been collected
	bufferLock.Lock()
	require.Equal(t, 0, len(buffers))
	bufferLock.Unlock()

	for i := 0; i < 2; i++ {
		// Move the time forward by one aggregation interval
		nowTs = nowTs.Add(testPolicy.Resolution.Window)
		atomic.StoreInt64(&now, nowTs.UnixNano())

		// Force a tick
		l.tickInternal()

		var expected []testAggMetric
		alignedStart := nowTs.Truncate(testPolicy.Resolution.Window)
		expected = append(expected, expectedAggMetricsForCounter(alignedStart, testPolicy)...)
		expected = append(expected, expectedAggMetricsForTimer(alignedStart, testPolicy)...)
		expected = append(expected, expectedAggMetricsForGauge(alignedStart, testPolicy)...)

		// Skip the first item because we intentionally triggered
		// an encoder error when encoding the first item
		if i == 0 {
			expected = expected[1:]
		}

		bufferLock.Lock()
		require.NotNil(t, buffers)
		validateBuffers(t, expected, buffers)
		buffers = buffers[:0]
		bufferLock.Unlock()
	}

	// Move the time forward by one aggregation interval
	nowTs = nowTs.Add(testPolicy.Resolution.Window)
	atomic.StoreInt64(&now, nowTs.UnixNano())

	// Force a tick
	l.tickInternal()

	// Assert nothing has been collected
	bufferLock.Lock()
	require.Equal(t, 0, len(buffers))
	bufferLock.Unlock()
	require.Equal(t, 3, l.aggregations.Len())

	// Mark all elements as tombstoned
	for e := l.aggregations.Front(); e != nil; e = e.Next() {
		e.Value.(metricElem).MarkAsTombstoned()
	}

	// Force a tick
	l.tickInternal()

	// Assert all elements have been collected
	require.Equal(t, 0, l.aggregations.Len())
}

func TestMetricLists(t *testing.T) {
	lists := newMetricLists(testOptions())
	require.False(t, lists.closed)

	// Create a new list
	l, err := lists.FindOrCreate(time.Second)
	require.NoError(t, err)
	require.NotNil(t, l)
	require.Equal(t, 1, lists.Len())

	// Find the same list
	l2, err := lists.FindOrCreate(time.Second)
	require.NoError(t, err)
	require.Equal(t, l, l2)
	require.Equal(t, 1, lists.Len())

	// Finding or creating in a closed list should result in an error
	lists.Close()
	_, err = lists.FindOrCreate(time.Second)
	require.Equal(t, errListsClosed, err)
	require.True(t, lists.closed)

	// Closing a second time should have no impact
	lists.Close()
	require.True(t, lists.closed)
}
