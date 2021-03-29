// Copyright (c) 2018 Uber Technologies, Inc.
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
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/aggregator/client"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/policy"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

var (
	testForwardedWriterAggregationKey = aggregationKey{
		aggregationID:     aggregation.MustCompressTypes(aggregation.Count),
		storagePolicy:     policy.MustParseStoragePolicy("10s:2d"),
		numForwardedTimes: 1,
	}
)

func TestForwardedWriterRegisterWriterClosed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		c      = client.NewMockAdminClient(ctrl)
		w      = newForwardedWriter(0, c, tally.NoopScope)
		mt     = metric.CounterType
		mid    = id.RawID("foo")
		aggKey = testForwardedWriterAggregationKey
		closed = make(chan struct{})
		wg     sync.WaitGroup
	)

	c.EXPECT().Flush().AnyTimes()

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			var err error

			assert.NotPanics(t, func() {
				if err = w.Flush(); err != nil {
					require.Equal(t, errForwardedWriterClosed, err)
				}
			})

			if err != nil {
				break
			}
			time.Sleep(1 * time.Microsecond)
		}

		assert.NotPanics(t, func() {
			_, _, err := w.Register(mt, mid, aggKey)
			require.Equal(t, errForwardedWriterClosed, err)

			err = w.Flush()
			require.Equal(t, errForwardedWriterClosed, err)
		})
	}()

	require.NoError(t, w.Close())
	close(closed)
	wg.Wait()
}

func TestForwardedWriterRegisterNewAggregation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		c      = client.NewMockAdminClient(ctrl)
		w      = newForwardedWriter(0, c, tally.NoopScope)
		mt     = metric.GaugeType
		mid    = id.RawID("foo")
		aggKey = testForwardedWriterAggregationKey
	)

	// Validate that no error is returned.
	writeFn, onDoneFn, err := w.Register(mt, mid, aggKey)
	require.NoError(t, err)
	require.NotNil(t, writeFn)
	require.NotNil(t, onDoneFn)

	// Validate that the new aggregation matches expectation.
	fw := w.(*forwardedWriter)
	require.Equal(t, 1, len(fw.aggregations))
	ik := newIDKey(mt, mid)
	agg, exists := fw.aggregations[ik]
	require.True(t, exists)
	require.Equal(t, mt, agg.metricType)
	require.Equal(t, mid, agg.metricID)
	require.Equal(t, uint32(0), agg.shard)
	require.True(t, c == agg.client)

	// Validate that the aggregation key has been added.
	require.Equal(t, 1, len(agg.byKey))
	require.Equal(t, 1, agg.byKey[0].totalRefCnt)
	require.True(t, aggKey.Equal(agg.byKey[0].key))
	require.Equal(t, 0, len(agg.byKey[0].buckets))

	// Validate that writeFn can be used to write data to the aggregation.
	writeFn(aggKey, 1234, 5.67)
	require.Equal(t, 1, len(agg.byKey[0].buckets))
	require.Equal(t, int64(1234), agg.byKey[0].buckets[0].timeNanos)
	require.Equal(t, []float64{5.67}, agg.byKey[0].buckets[0].values)

	writeFn(aggKey, 1234, 1.78)
	require.Equal(t, 1, len(agg.byKey[0].buckets))
	require.Equal(t, int64(1234), agg.byKey[0].buckets[0].timeNanos)
	require.Equal(t, []float64{5.67, 1.78}, agg.byKey[0].buckets[0].values)

	writeFn(aggKey, 1240, -2.95)
	require.Equal(t, 2, len(agg.byKey[0].buckets))
	require.Equal(t, int64(1240), agg.byKey[0].buckets[1].timeNanos)
	require.Equal(t, []float64{-2.95}, agg.byKey[0].buckets[1].values)

	// Validate that onDoneFn can be used to flush data out.
	expectedMetric1 := aggregated.ForwardedMetric{
		Type:      mt,
		ID:        mid,
		TimeNanos: 1234,
		Values:    []float64{5.67, 1.78},
	}
	expectedMetric2 := aggregated.ForwardedMetric{
		Type:      mt,
		ID:        mid,
		TimeNanos: 1240,
		Values:    []float64{-2.95},
	}
	expectedMeta := metadata.ForwardMetadata{
		AggregationID:     aggregation.MustCompressTypes(aggregation.Count),
		StoragePolicy:     policy.MustParseStoragePolicy("10s:2d"),
		SourceID:          0,
		NumForwardedTimes: 1,
	}
	c.EXPECT().WriteForwarded(expectedMetric1, expectedMeta).Return(nil)
	c.EXPECT().WriteForwarded(expectedMetric2, expectedMeta).Return(nil)
	require.NoError(t, onDoneFn(aggKey))
	require.Equal(t, 1, agg.byKey[0].currRefCnt)
}

func TestForwardedWriterRegisterExistingAggregation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		c      = client.NewMockAdminClient(ctrl)
		w      = newForwardedWriter(0, c, tally.NoopScope)
		mt     = metric.GaugeType
		mid    = id.RawID("foo")
		aggKey = testForwardedWriterAggregationKey
	)

	// Register an aggregation first.
	writeFn, onDoneFn, err := w.Register(mt, mid, aggKey)
	require.NoError(t, err)
	require.NotNil(t, writeFn)
	require.NotNil(t, onDoneFn)

	// Validate that the aggregation key has been added.
	fw := w.(*forwardedWriter)
	require.Equal(t, 1, len(fw.aggregations))
	ik := newIDKey(mt, mid)
	agg, exists := fw.aggregations[ik]
	require.True(t, exists)
	require.Equal(t, 1, len(agg.byKey))
	require.Equal(t, 1, agg.byKey[0].totalRefCnt)

	// Register the same aggregation again.
	writeFn, onDoneFn, err = w.Register(mt, mid, aggKey)
	require.NoError(t, err)
	require.NotNil(t, writeFn)
	require.NotNil(t, onDoneFn)
	require.Equal(t, 1, len(agg.byKey))
	require.Equal(t, 2, agg.byKey[0].totalRefCnt)
}

func TestForwardedWriterUnregisterWriterClosed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		c      = client.NewMockAdminClient(ctrl)
		w      = newForwardedWriter(0, c, tally.NoopScope)
		mt     = metric.GaugeType
		mid    = id.RawID("foo")
		aggKey = testForwardedWriterAggregationKey
	)

	require.NoError(t, w.Close())
	require.Equal(t, errForwardedWriterClosed, w.Unregister(mt, mid, aggKey))
}

func TestForwardedWriterUnregisterMetricNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		c      = client.NewMockAdminClient(ctrl)
		w      = newForwardedWriter(0, c, tally.NoopScope)
		mt     = metric.GaugeType
		mid    = id.RawID("foo")
		aggKey = testForwardedWriterAggregationKey
	)

	require.Equal(t, errMetricNotFound, w.Unregister(mt, mid, aggKey))
}

func TestForwardedWriterUnregisterAggregationKeyNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		c      = client.NewMockAdminClient(ctrl)
		w      = newForwardedWriter(0, c, tally.NoopScope)
		mt     = metric.GaugeType
		mid    = id.RawID("foo")
		aggKey = testForwardedWriterAggregationKey
	)

	// Register an aggregation first.
	_, _, err := w.Register(mt, mid, aggKey)
	require.NoError(t, err)

	// Unregister a different aggregation key.
	aggKey2 := aggKey
	aggKey2.numForwardedTimes++
	require.Equal(t, errAggregationKeyNotFound, w.Unregister(mt, mid, aggKey2))
}

func TestForwardedWriterUnregister(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		c      = client.NewMockAdminClient(ctrl)
		w      = newForwardedWriter(0, c, tally.NoopScope)
		mt     = metric.GaugeType
		mid    = id.RawID("foo")
		aggKey = testForwardedWriterAggregationKey
	)

	// Register an aggregation first.
	_, _, err := w.Register(mt, mid, aggKey)
	require.NoError(t, err)
	fw := w.(*forwardedWriter)
	require.Equal(t, 1, len(fw.aggregations))

	// Register the aggregation again.
	_, _, err = w.Register(mt, mid, aggKey)
	require.NoError(t, err)
	require.Equal(t, 1, len(fw.aggregations))

	// Unregister the aggregation.
	require.NoError(t, w.Unregister(mt, mid, aggKey))
	require.Equal(t, 1, len(fw.aggregations))

	// Unregister the aggregation again.
	require.NoError(t, w.Unregister(mt, mid, aggKey))
	require.Equal(t, 0, len(fw.aggregations))
}

func TestForwardedWriterPrepare(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		c      = client.NewMockAdminClient(ctrl)
		w      = newForwardedWriter(0, c, tally.NoopScope)
		mt     = metric.GaugeType
		mid    = id.RawID("foo")
		mid2   = id.RawID("bar")
		aggKey = testForwardedWriterAggregationKey
	)

	// Register an aggregation.
	writeFn, onDoneFn, err := w.Register(mt, mid, aggKey)
	require.NoError(t, err)

	// Write some datapoints.
	writeFn(aggKey, 1234, 3.4)
	writeFn(aggKey, 1234, 3.5)
	writeFn(aggKey, 1240, 98.2)

	// Register another aggregation.
	writeFn2, onDoneFn2, err := w.Register(mt, mid2, aggKey)
	require.NoError(t, err)

	// Write some more datapoints.
	writeFn2(aggKey, 1238, 3.4)
	writeFn2(aggKey, 1239, 3.5)

	expectedMetric1 := aggregated.ForwardedMetric{
		Type:      mt,
		ID:        mid,
		TimeNanos: 1234,
		Values:    []float64{3.4, 3.5},
	}
	expectedMetric2 := aggregated.ForwardedMetric{
		Type:      mt,
		ID:        mid,
		TimeNanos: 1240,
		Values:    []float64{98.2},
	}
	expectedMetric3 := aggregated.ForwardedMetric{
		Type:      mt,
		ID:        mid2,
		TimeNanos: 1238,
		Values:    []float64{3.4},
	}
	expectedMetric4 := aggregated.ForwardedMetric{
		Type:      mt,
		ID:        mid2,
		TimeNanos: 1239,
		Values:    []float64{3.5},
	}
	expectedMeta := metadata.ForwardMetadata{
		AggregationID:     aggregation.MustCompressTypes(aggregation.Count),
		StoragePolicy:     policy.MustParseStoragePolicy("10s:2d"),
		SourceID:          0,
		NumForwardedTimes: 1,
	}
	c.EXPECT().WriteForwarded(expectedMetric1, expectedMeta).Return(nil).Times(2)
	c.EXPECT().WriteForwarded(expectedMetric2, expectedMeta).Return(nil).Times(2)
	c.EXPECT().WriteForwarded(expectedMetric3, expectedMeta).Return(nil).Times(2)
	c.EXPECT().WriteForwarded(expectedMetric4, expectedMeta).Return(nil).Times(2)
	require.NoError(t, onDoneFn(aggKey))
	require.NoError(t, onDoneFn2(aggKey))

	fw := w.(*forwardedWriter)
	require.Equal(t, 2, len(fw.aggregations))
	agg, exists := fw.aggregations[newIDKey(mt, mid)]
	require.True(t, exists)
	require.Equal(t, 1, len(agg.byKey))
	require.Equal(t, 2, len(agg.byKey[0].buckets))
	require.Equal(t, 1, agg.byKey[0].currRefCnt)
	require.Equal(t, 0, len(agg.byKey[0].cachedValueArrays))
	agg, exists = fw.aggregations[newIDKey(mt, mid2)]
	require.True(t, exists)
	require.Equal(t, 1, len(agg.byKey))
	require.Equal(t, 2, len(agg.byKey[0].buckets))
	require.Equal(t, 1, agg.byKey[0].currRefCnt)
	require.Equal(t, 0, len(agg.byKey[0].cachedValueArrays))

	w.Prepare()

	// Assert the internal state has been reset.
	require.Equal(t, 2, len(fw.aggregations))
	agg, exists = fw.aggregations[newIDKey(mt, mid)]
	require.True(t, exists)
	require.Equal(t, 1, len(agg.byKey))
	require.Equal(t, 0, len(agg.byKey[0].buckets))
	require.Equal(t, 0, agg.byKey[0].currRefCnt)
	require.Equal(t, 2, len(agg.byKey[0].cachedValueArrays))
	agg, exists = fw.aggregations[newIDKey(mt, mid2)]
	require.True(t, exists)
	require.Equal(t, 1, len(agg.byKey))
	require.Equal(t, 0, len(agg.byKey[0].buckets))
	require.Equal(t, 0, agg.byKey[0].currRefCnt)
	require.Equal(t, 2, len(agg.byKey[0].cachedValueArrays))

	// Write datapoints again.
	writeFn(aggKey, 1234, 3.4)
	writeFn(aggKey, 1234, 3.5)
	writeFn(aggKey, 1240, 98.2)
	writeFn2(aggKey, 1238, 3.4)
	writeFn2(aggKey, 1239, 3.5)
	require.NoError(t, onDoneFn(aggKey))
	require.NoError(t, onDoneFn2(aggKey))

	require.Equal(t, 2, len(fw.aggregations))
	agg, exists = fw.aggregations[newIDKey(mt, mid)]
	require.True(t, exists)
	require.Equal(t, 1, len(agg.byKey))
	require.Equal(t, 2, len(agg.byKey[0].buckets))
	require.Equal(t, 1, agg.byKey[0].currRefCnt)
	require.Equal(t, 0, len(agg.byKey[0].cachedValueArrays))
	agg, exists = fw.aggregations[newIDKey(mt, mid2)]
	require.True(t, exists)
	require.Equal(t, 1, len(agg.byKey))
	require.Equal(t, 2, len(agg.byKey[0].buckets))
	require.Equal(t, 1, agg.byKey[0].currRefCnt)
	require.Equal(t, 0, len(agg.byKey[0].cachedValueArrays))
}

func TestForwardedWriterCloseWriterClosed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		c = client.NewMockAdminClient(ctrl)
		w = newForwardedWriter(0, c, tally.NoopScope)
	)

	// Close the writer
	require.NoError(t, w.Close())
	fw := w.(*forwardedWriter)
	require.True(t, fw.closed.Load())

	// Closing the writer a second time results in an error.
	require.Equal(t, errForwardedWriterClosed, w.Close())
}
