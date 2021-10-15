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
	"github.com/m3db/m3/src/x/clock"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testForwardedWriterAggregationKey = aggregationKey{
	aggregationID:     aggregation.MustCompressTypes(aggregation.Count),
	storagePolicy:     policy.MustParseStoragePolicy("10s:2d"),
	numForwardedTimes: 1,
}

func TestCachedValuesGrowth(t *testing.T) {
	timeNanos := time.Now().UnixNano()
	key := forwardedAggregationWithKey{
		key:                 aggregationKey{},
		totalRefCnt:         1,
		currRefCnt:          0,
		buckets:             make([]forwardedAggregationBucket, 0, 0),
		versionsByTimeNanos: make(map[int64]uint32, 0),
		resendEnabled:       true,
	}
	for i := 0; i < 100; i++ {
		for n := 0; n < 3; n++ {
			timeNanos++
			key.add(timeNanos, 1.0, 0.5, nil)
		}
		key.reset()
	}
	// 3 calls to add, each call adds 2 slices to the cache arrays.
	require.Equal(t, 3*2, len(key.cachedValueArrays))
	// go might increase the underlying array more than 1 when appending, so the capacity is > 6. Just need to make sure
	// it doesn't grow on the order of calls to reset.
	require.Equal(t, 8, cap(key.cachedValueArrays))
}

func TestForwardedWriterRegisterWriterClosed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		c      = client.NewMockAdminClient(ctrl)
		opts   = NewOptions(clock.NewOptions()).SetAdminClient(c)
		w      = newForwardedWriter(0, opts)
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
			_, _, err := w.Register(testRegisterable{
				metricType: mt,
				id:         mid,
				key:        aggKey,
			})
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
		opts   = NewOptions(clock.NewOptions()).SetAdminClient(c)
		w      = newForwardedWriter(0, opts)
		mt     = metric.GaugeType
		mid    = id.RawID("foo")
		aggKey = testForwardedWriterAggregationKey
	)

	// Validate that no error is returned.
	writeFn, onDoneFn, err := w.Register(testRegisterable{
		metricType: mt,
		id:         mid,
		key:        aggKey,
	})
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
	writeFn(aggKey, 1234, 5.67, 5.0, nil)
	require.Equal(t, 1, len(agg.byKey[0].buckets))
	require.Equal(t, int64(1234), agg.byKey[0].buckets[0].timeNanos)
	require.Equal(t, []float64{5.67}, agg.byKey[0].buckets[0].values)
	require.Equal(t, []float64{5.0}, agg.byKey[0].buckets[0].prevValues)
	require.Equal(t, uint32(0), agg.byKey[0].versionsByTimeNanos[1234])
	require.Nil(t, agg.byKey[0].buckets[0].annotation)

	writeFn(aggKey, 1234, 1.78, 1.0, testAnnot)
	require.Equal(t, 1, len(agg.byKey[0].buckets))
	require.Equal(t, int64(1234), agg.byKey[0].buckets[0].timeNanos)
	require.Equal(t, []float64{5.67, 1.78}, agg.byKey[0].buckets[0].values)
	require.Equal(t, []float64{5.0, 1.0}, agg.byKey[0].buckets[0].prevValues)
	require.Equal(t, testAnnot, agg.byKey[0].buckets[0].annotation)
	require.Equal(t, uint32(0), agg.byKey[0].versionsByTimeNanos[1234])

	writeFn(aggKey, 1240, -2.95, 0.0, nil)
	require.Equal(t, 2, len(agg.byKey[0].buckets))
	require.Equal(t, int64(1240), agg.byKey[0].buckets[1].timeNanos)
	require.Equal(t, []float64{-2.95}, agg.byKey[0].buckets[1].values)
	require.Equal(t, []float64{0.0}, agg.byKey[0].buckets[1].prevValues)
	require.Equal(t, uint32(0), agg.byKey[0].versionsByTimeNanos[1240])

	// Validate that onDoneFn can be used to flush data out.
	expectedMetric1 := aggregated.ForwardedMetric{
		Type:       mt,
		ID:         mid,
		TimeNanos:  1234,
		Values:     []float64{5.67, 1.78},
		PrevValues: []float64{5.0, 1.0},
		Annotation: testAnnot,
	}
	expectedMetric2 := aggregated.ForwardedMetric{
		Type:       mt,
		ID:         mid,
		TimeNanos:  1240,
		Values:     []float64{-2.95},
		PrevValues: []float64{0.0},
		Version:    0,
	}
	expectedMeta := metadata.ForwardMetadata{
		AggregationID:     aggregation.MustCompressTypes(aggregation.Count),
		StoragePolicy:     policy.MustParseStoragePolicy("10s:2d"),
		SourceID:          0,
		NumForwardedTimes: 1,
	}
	c.EXPECT().WriteForwarded(expectedMetric1, expectedMeta).Return(nil)
	c.EXPECT().WriteForwarded(expectedMetric2, expectedMeta).Return(nil)
	require.NoError(t, onDoneFn(aggKey, nil))
	require.Equal(t, 1, agg.byKey[0].currRefCnt)
}

func TestForwardedWriterRegisterExistingAggregation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		c      = client.NewMockAdminClient(ctrl)
		opts   = NewOptions(clock.NewOptions()).SetAdminClient(c)
		w      = newForwardedWriter(0, opts)
		mt     = metric.GaugeType
		mid    = id.RawID("foo")
		aggKey = testForwardedWriterAggregationKey
	)

	// Register an aggregation first.
	writeFn, onDoneFn, err := w.Register(testRegisterable{
		metricType: mt,
		id:         mid,
		key:        aggKey,
	})
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
	writeFn, onDoneFn, err = w.Register(testRegisterable{
		metricType: mt,
		id:         mid,
		key:        aggKey,
	})
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
		opts   = NewOptions(clock.NewOptions()).SetAdminClient(c)
		w      = newForwardedWriter(0, opts)
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
		opts   = NewOptions(clock.NewOptions()).SetAdminClient(c)
		w      = newForwardedWriter(0, opts)
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
		opts   = NewOptions(clock.NewOptions()).SetAdminClient(c)
		w      = newForwardedWriter(0, opts)
		mt     = metric.GaugeType
		mid    = id.RawID("foo")
		aggKey = testForwardedWriterAggregationKey
	)

	// Register an aggregation first.
	_, _, err := w.Register(testRegisterable{
		metricType: mt,
		id:         mid,
		key:        aggKey,
	})
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
		opts   = NewOptions(clock.NewOptions()).SetAdminClient(c)
		w      = newForwardedWriter(0, opts)
		mt     = metric.GaugeType
		mid    = id.RawID("foo")
		aggKey = testForwardedWriterAggregationKey
	)

	// Register an aggregation first.
	_, _, err := w.Register(testRegisterable{
		metricType: mt,
		id:         mid,
		key:        aggKey,
	})
	require.NoError(t, err)
	fw := w.(*forwardedWriter)
	require.Equal(t, 1, len(fw.aggregations))

	// Register the aggregation again.
	_, _, err = w.Register(testRegisterable{
		metricType: mt,
		id:         mid,
		key:        aggKey,
	})
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
		opts   = NewOptions(clock.NewOptions()).SetAdminClient(c)
		w      = newForwardedWriter(0, opts)
		mt     = metric.GaugeType
		mid    = id.RawID("foo")
		mid2   = id.RawID("bar")
		aggKey = testForwardedWriterAggregationKey
	)

	// Register an aggregation.
	writeFn, onDoneFn, err := w.Register(testRegisterable{
		metricType: mt,
		id:         mid,
		key:        aggKey,
	})
	require.NoError(t, err)

	// Write some datapoints.
	writeFn(aggKey, 1234, 3.4, 3.0, nil)
	writeFn(aggKey, 1234, 3.5, 2.0, nil)
	writeFn(aggKey, 1240, 98.2, 98.0, nil)

	// Register another aggregation.
	writeFn2, onDoneFn2, err := w.Register(testRegisterable{
		metricType: mt,
		id:         mid2,
		key:        aggKey,
	})
	require.NoError(t, err)

	// Write some more datapoints.
	writeFn2(aggKey, 1238, 3.4, 0.0, nil)
	writeFn2(aggKey, 1239, 3.5, 0.0, nil)

	expectedMetric1 := aggregated.ForwardedMetric{
		Type:       mt,
		ID:         mid,
		TimeNanos:  1234,
		Values:     []float64{3.4, 3.5},
		PrevValues: []float64{3.0, 2.0},
	}
	expectedMetric2 := aggregated.ForwardedMetric{
		Type:       mt,
		ID:         mid,
		TimeNanos:  1240,
		Values:     []float64{98.2},
		PrevValues: []float64{98.0},
		Version:    0,
	}
	expectedMetric3 := aggregated.ForwardedMetric{
		Type:       mt,
		ID:         mid2,
		TimeNanos:  1238,
		Values:     []float64{3.4},
		PrevValues: []float64{0.0},
	}
	expectedMetric4 := aggregated.ForwardedMetric{
		Type:       mt,
		ID:         mid2,
		TimeNanos:  1239,
		Values:     []float64{3.5},
		PrevValues: []float64{0.0},
		Version:    0,
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
	require.NoError(t, onDoneFn(aggKey, nil))
	require.NoError(t, onDoneFn2(aggKey, nil))

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
	require.Equal(t, 4, len(agg.byKey[0].cachedValueArrays))
	agg, exists = fw.aggregations[newIDKey(mt, mid2)]
	require.True(t, exists)
	require.Equal(t, 1, len(agg.byKey))
	require.Equal(t, 0, len(agg.byKey[0].buckets))
	require.Equal(t, 0, agg.byKey[0].currRefCnt)
	require.Equal(t, 4, len(agg.byKey[0].cachedValueArrays))

	// Write datapoints again.
	writeFn(aggKey, 1234, 3.4, 3.0, nil)
	writeFn(aggKey, 1234, 3.5, 2.0, nil)
	writeFn(aggKey, 1240, 98.2, 98.0, nil)
	writeFn2(aggKey, 1238, 3.4, 0.0, nil)
	writeFn2(aggKey, 1239, 3.5, 0.0, nil)
	require.NoError(t, onDoneFn(aggKey, nil))
	require.NoError(t, onDoneFn2(aggKey, nil))

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

func TestForwardedWriterResend(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		c      = client.NewMockAdminClient(ctrl)
		now    = time.Unix(0, 1300)
		nowPtr = &now
		nowFn  = func() time.Time { return *nowPtr }
		opts   = NewOptions(clock.NewOptions().SetNowFn(nowFn)).SetAdminClient(c).
			SetBufferForPastTimedMetricFn(func(resolution time.Duration) time.Duration {
				return resolution
			})
		w      = newForwardedWriter(0, opts)
		mt     = metric.GaugeType
		mid    = id.RawID("foo")
		mid2   = id.RawID("bar")
		aggKey = testForwardedWriterAggregationKey
	)

	// Register an aggregation.
	writeFn, onDoneFn, err := w.Register(testRegisterable{
		metricType:    mt,
		id:            mid,
		key:           aggKey,
		resendEnabled: true,
	})
	require.NoError(t, err)

	// Write some datapoints.
	writeFn(aggKey, 1234, 3.4, 3.0, nil)
	writeFn(aggKey, 1234, 3.5, 2.0, nil)
	writeFn(aggKey, 1240, 98.2, 98.0, nil)

	// Register another aggregation.
	writeFn2, onDoneFn2, err := w.Register(testRegisterable{
		metricType:    mt,
		id:            mid2,
		key:           aggKey,
		resendEnabled: true,
	})
	require.NoError(t, err)

	// Write some more datapoints.
	writeFn2(aggKey, 1238, 3.4, 0.0, nil)
	writeFn2(aggKey, 1239, 3.5, 0.0, nil)

	expectedMetric1 := aggregated.ForwardedMetric{
		Type:       mt,
		ID:         mid,
		TimeNanos:  1234,
		Values:     []float64{3.4, 3.5},
		PrevValues: []float64{3.0, 2.0},
	}
	expectedMetric2 := aggregated.ForwardedMetric{
		Type:       mt,
		ID:         mid,
		TimeNanos:  1240,
		Values:     []float64{98.2},
		PrevValues: []float64{98.0},
		Version:    0,
	}
	expectedMetric3 := aggregated.ForwardedMetric{
		Type:       mt,
		ID:         mid2,
		TimeNanos:  1238,
		Values:     []float64{3.4},
		PrevValues: []float64{0.0},
	}
	expectedMetric4 := aggregated.ForwardedMetric{
		Type:       mt,
		ID:         mid2,
		TimeNanos:  1239,
		Values:     []float64{3.5},
		PrevValues: []float64{0.0},
		Version:    0,
	}
	expectedMeta := metadata.ForwardMetadata{
		AggregationID:     aggregation.MustCompressTypes(aggregation.Count),
		StoragePolicy:     policy.MustParseStoragePolicy("10s:2d"),
		SourceID:          0,
		NumForwardedTimes: 1,
		Pipeline:          aggKey.pipeline,
		ResendEnabled:     true,
	}
	c.EXPECT().WriteForwarded(expectedMetric1, expectedMeta).Return(nil)
	c.EXPECT().WriteForwarded(expectedMetric2, expectedMeta).Return(nil)
	c.EXPECT().WriteForwarded(expectedMetric3, expectedMeta).Return(nil)
	c.EXPECT().WriteForwarded(expectedMetric4, expectedMeta).Return(nil)
	require.NoError(t, onDoneFn(aggKey, nil))
	require.NoError(t, onDoneFn2(aggKey, nil))

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
	require.Equal(t, 2, len(agg.byKey[0].buckets))
	require.Equal(t, 0, agg.byKey[0].currRefCnt)
	require.Equal(t, 4, len(agg.byKey[0].cachedValueArrays))
	agg, exists = fw.aggregations[newIDKey(mt, mid2)]
	require.True(t, exists)
	require.Equal(t, 1, len(agg.byKey))
	require.Equal(t, 2, len(agg.byKey[0].buckets))
	require.Equal(t, 0, agg.byKey[0].currRefCnt)
	require.Equal(t, 4, len(agg.byKey[0].cachedValueArrays))

	// Write datapoints again.
	writeFn(aggKey, 1234, 3.4, 3.0, nil)
	writeFn(aggKey, 1234, 3.5, 2.0, nil)
	writeFn(aggKey, 1240, 98.2, 98.0, nil)
	writeFn2(aggKey, 1238, 3.4, 0.0, nil)
	writeFn2(aggKey, 1239, 3.5, 0.0, nil)

	expectedMetric1.Version = 1
	expectedMetric2.Version = 1
	expectedMetric3.Version = 1
	expectedMetric4.Version = 1

	c.EXPECT().WriteForwarded(expectedMetric1, expectedMeta).Return(nil)
	c.EXPECT().WriteForwarded(expectedMetric2, expectedMeta).Return(nil)
	c.EXPECT().WriteForwarded(expectedMetric3, expectedMeta).Return(nil)
	c.EXPECT().WriteForwarded(expectedMetric4, expectedMeta).Return(nil)

	require.NoError(t, onDoneFn(aggKey, nil))
	require.NoError(t, onDoneFn2(aggKey, nil))

	require.Equal(t, 2, len(fw.aggregations))
	agg, exists = fw.aggregations[newIDKey(mt, mid)]
	require.True(t, exists)
	require.Equal(t, 1, len(agg.byKey))
	require.Equal(t, 2, len(agg.byKey[0].buckets))
	require.Equal(t, 1, agg.byKey[0].currRefCnt)
	require.Equal(t, 4, len(agg.byKey[0].cachedValueArrays))
	agg, exists = fw.aggregations[newIDKey(mt, mid2)]
	require.True(t, exists)
	require.Equal(t, 1, len(agg.byKey))
	require.Equal(t, 2, len(agg.byKey[0].buckets))
	require.Equal(t, 1, agg.byKey[0].currRefCnt)
	require.Equal(t, 4, len(agg.byKey[0].cachedValueArrays))

	// advance the time past the buffer to GC the buckets.
	now = now.Add(time.Second * 10)
	nowPtr = &now
	w.Prepare()
	require.Equal(t, 2, len(fw.aggregations))
	agg, exists = fw.aggregations[newIDKey(mt, mid)]
	require.True(t, exists)
	require.Equal(t, 1, len(agg.byKey))
	require.Equal(t, 0, len(agg.byKey[0].buckets))
	agg, exists = fw.aggregations[newIDKey(mt, mid2)]
	require.True(t, exists)
	require.Equal(t, 1, len(agg.byKey))
	require.Equal(t, 0, len(agg.byKey[0].buckets))
}

func TestForwardedWriterCloseWriterClosed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		c    = client.NewMockAdminClient(ctrl)
		opts = NewOptions(clock.NewOptions()).SetAdminClient(c)
		w    = newForwardedWriter(0, opts)
	)

	// Close the writer
	require.NoError(t, w.Close())
	fw := w.(*forwardedWriter)
	require.True(t, fw.closed.Load())

	// Closing the writer a second time results in an error.
	require.Equal(t, errForwardedWriterClosed, w.Close())
}

type testRegisterable struct {
	metricType    metric.Type
	id            id.RawID
	key           aggregationKey
	resendEnabled bool
}

func (t testRegisterable) Type() metric.Type {
	return t.metricType
}

func (t testRegisterable) ForwardedID() (id.RawID, bool) {
	return t.id, true
}

func (t testRegisterable) ForwardedAggregationKey() (aggregationKey, bool) {
	return t.key, true
}

func (t testRegisterable) ResendEnabled() bool {
	return t.resendEnabled
}

var _ Registerable = &testRegisterable{}
