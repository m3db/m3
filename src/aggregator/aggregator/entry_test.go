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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3aggregator/runtime"
	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/metadata"
	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/pipeline"
	"github.com/m3db/m3metrics/pipeline/applied"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/transformation"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

var (
	testDefaultStoragePolicies = []policy.StoragePolicy{
		policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour),
		policy.NewStoragePolicy(time.Minute, xtime.Minute, 720*time.Hour),
	}
	testDefaultPipelines = []metadata.PipelineMetadata{
		{
			AggregationID:   aggregation.DefaultID,
			StoragePolicies: testDefaultStoragePolicies,
		},
	}
	testPipelines = []metadata.PipelineMetadata{
		{
			AggregationID: aggregation.DefaultID,
			StoragePolicies: []policy.StoragePolicy{
				policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
				policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 30*24*time.Hour),
			},
		},
		{
			AggregationID: aggregation.MustCompressTypes(aggregation.Max),
			StoragePolicies: []policy.StoragePolicy{
				policy.NewStoragePolicy(time.Minute, xtime.Minute, 2*24*time.Hour),
			},
		},
	}
	testNewPipelines = []metadata.PipelineMetadata{
		{
			AggregationID: aggregation.DefaultID,
			StoragePolicies: []policy.StoragePolicy{
				policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
				policy.NewStoragePolicy(time.Minute, xtime.Minute, 7*24*time.Hour),
				policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 7*24*time.Hour),
			},
		},
	}
	testNewPipelines2 = []metadata.PipelineMetadata{
		{
			AggregationID:   aggregation.DefaultID,
			StoragePolicies: testDefaultStoragePolicies,
			Pipeline: applied.NewPipeline([]applied.OpUnion{
				{
					Type:           pipeline.TransformationOpType,
					Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
				},
				{
					Type: pipeline.RollupOpType,
					Rollup: applied.RollupOp{
						ID:            []byte("foo.bar"),
						AggregationID: aggregation.DefaultID,
					},
				},
			}),
		},
	}
	testNewPipelines3 = []metadata.PipelineMetadata{
		{
			AggregationID:   aggregation.DefaultID,
			StoragePolicies: testDefaultStoragePolicies,
			Pipeline: applied.NewPipeline([]applied.OpUnion{
				{
					Type:           pipeline.TransformationOpType,
					Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
				},
				{
					Type: pipeline.RollupOpType,
					Rollup: applied.RollupOp{
						ID:            []byte("foo.baz"),
						AggregationID: aggregation.DefaultID,
					},
				},
			}),
		},
	}
	testPipelinesWithDuplicates = []metadata.PipelineMetadata{
		{
			AggregationID:   aggregation.DefaultID,
			StoragePolicies: testDefaultStoragePolicies,
		},
		{
			AggregationID:   aggregation.DefaultID,
			StoragePolicies: testDefaultStoragePolicies,
		},
	}
	testPipelinesWithDuplicates2 = []metadata.PipelineMetadata{
		{
			AggregationID:   aggregation.DefaultID,
			StoragePolicies: testDefaultStoragePolicies,
			Pipeline: applied.NewPipeline([]applied.OpUnion{
				{
					Type:           pipeline.TransformationOpType,
					Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
				},
				{
					Type: pipeline.RollupOpType,
					Rollup: applied.RollupOp{
						ID:            []byte("foo.baz"),
						AggregationID: aggregation.DefaultID,
					},
				},
			}),
		},
		{
			AggregationID:   aggregation.DefaultID,
			StoragePolicies: testDefaultStoragePolicies,
		},
		{
			AggregationID:   aggregation.DefaultID,
			StoragePolicies: testDefaultStoragePolicies,
			Pipeline: applied.NewPipeline([]applied.OpUnion{
				{
					Type:           pipeline.TransformationOpType,
					Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
				},
				{
					Type: pipeline.RollupOpType,
					Rollup: applied.RollupOp{
						ID:            []byte("foo.baz"),
						AggregationID: aggregation.DefaultID,
					},
				},
			}),
		},
	}
	testForwardMetadata1 = metadata.ForwardMetadata{
		AggregationID: aggregation.DefaultID,
		StoragePolicy: policy.NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour),
		Pipeline: applied.NewPipeline([]applied.OpUnion{
			{
				Type: pipeline.RollupOpType,
				Rollup: applied.RollupOp{
					ID:            []byte("foo"),
					AggregationID: aggregation.MustCompressTypes(aggregation.Count),
				},
			},
		}),
		SourceID:          []byte("testForwardSource1"),
		NumForwardedTimes: 3,
	}
	testForwardMetadata2 = metadata.ForwardMetadata{
		AggregationID: aggregation.DefaultID,
		StoragePolicy: policy.NewStoragePolicy(10*time.Second, xtime.Second, time.Hour),
		Pipeline: applied.NewPipeline([]applied.OpUnion{
			{
				Type: pipeline.RollupOpType,
				Rollup: applied.RollupOp{
					ID:            []byte("bar"),
					AggregationID: aggregation.DefaultID,
				},
			},
		}),
		SourceID:          []byte("testForwardSource2"),
		NumForwardedTimes: 4,
	}
	testDefaultAggregationKeys         = aggregationKeys(testDefaultPipelines)
	testAggregationKeys                = aggregationKeys(testPipelines)
	testNewAggregationKeys             = aggregationKeys(testNewPipelines)
	testNewAggregationKeys2            = aggregationKeys(testNewPipelines2)
	testNewAggregationKeys3            = aggregationKeys(testNewPipelines3)
	testWithDuplicates2AggregationKeys = aggregationKeys(testPipelinesWithDuplicates2)
)

func TestEntryIncDecWriter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	e := NewEntry(nil, runtime.NewOptions(), testOptions(ctrl))
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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	e, lists, now := testEntry(ctrl)

	require.False(t, e.closed)
	require.Nil(t, e.rateLimiter)
	require.False(t, e.hasDefaultMetadatas)
	require.Equal(t, int64(uninitializedCutoverNanos), e.cutoverNanos)
	require.Equal(t, lists, e.lists)
	require.Equal(t, int32(0), e.numWriters)
	require.Equal(t, now.UnixNano(), e.lastAccessNanos)
}

func TestEntryBatchTimerRateLimiting(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bt := unaggregated.MetricUnion{
		Type:          metric.TimerType,
		ID:            testBatchTimerID,
		BatchTimerVal: make([]float64, 1000),
	}
	e, _, now := testEntry(ctrl)

	// Reset runtime options to disable rate limiting.
	noRateLimitRuntimeOpts := runtime.NewOptions().SetWriteValuesPerMetricLimitPerSecond(0)
	e.SetRuntimeOptions(noRateLimitRuntimeOpts)
	require.NoError(t, e.AddUntimed(bt, testDefaultStagedMetadatas))

	// Reset runtime options to enable a rate limit of 100/s.
	limitPerSecond := 100
	runtimeOpts := runtime.NewOptions().SetWriteValuesPerMetricLimitPerSecond(int64(limitPerSecond))
	e.SetRuntimeOptions(runtimeOpts)
	require.Equal(t, errWriteValueRateLimitExceeded, e.AddUntimed(bt, testDefaultStagedMetadatas))

	// Reset limit to enable a rate limit of 1000/s.
	limitPerSecond = 1000
	runtimeOpts = runtime.NewOptions().SetWriteValuesPerMetricLimitPerSecond(int64(limitPerSecond))
	e.SetRuntimeOptions(runtimeOpts)
	require.NoError(t, e.AddUntimed(bt, testDefaultStagedMetadatas))

	// Adding a new batch will exceed the limit.
	require.Equal(t, errWriteValueRateLimitExceeded, e.AddUntimed(bt, testDefaultStagedMetadatas))

	// Advancing the time will reset the quota.
	*now = (*now).Add(time.Second)
	require.NoError(t, e.AddUntimed(bt, testDefaultStagedMetadatas))
}

func TestEntryCounterRateLimiting(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	e, _, now := testEntry(ctrl)

	// Reset runtime options to disable rate limiting.
	noRateLimitRuntimeOpts := runtime.NewOptions().SetWriteValuesPerMetricLimitPerSecond(0)
	e.SetRuntimeOptions(noRateLimitRuntimeOpts)
	require.NoError(t, e.AddUntimed(testCounter, testDefaultStagedMetadatas))

	// Reset runtime options to enable a rate limit of 10/s.
	limitPerSecond := 10
	runtimeOpts := runtime.NewOptions().SetWriteValuesPerMetricLimitPerSecond(int64(limitPerSecond))
	e.SetRuntimeOptions(runtimeOpts)
	for i := 0; i < limitPerSecond; i++ {
		require.NoError(t, e.AddUntimed(testCounter, testDefaultStagedMetadatas))
	}
	require.Equal(t, errWriteValueRateLimitExceeded, e.AddUntimed(testCounter, testDefaultStagedMetadatas))

	// Reset limit to enable a rate limit of 100/s.
	limitPerSecond = 100
	runtimeOpts = runtime.NewOptions().SetWriteValuesPerMetricLimitPerSecond(int64(limitPerSecond))
	e.SetRuntimeOptions(runtimeOpts)
	for i := 0; i < limitPerSecond; i++ {
		require.NoError(t, e.AddUntimed(testCounter, testDefaultStagedMetadatas))
	}
	require.Equal(t, errWriteValueRateLimitExceeded, e.AddUntimed(testCounter, testDefaultStagedMetadatas))

	// Advancing the time will reset the quota.
	*now = (*now).Add(time.Second)
	require.NoError(t, e.AddUntimed(testCounter, testDefaultStagedMetadatas))
}

func TestEntryAddBatchTimerWithPoolAlloc(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	timerValPool := pool.NewFloatsPool([]pool.Bucket{
		{Capacity: 16, Count: 1},
	}, nil)
	timerValPool.Init()

	// Consume the element in the pool.
	input := timerValPool.Get(10)
	input = append(input, []float64{1.0, 3.5, 2.2, 6.5, 4.8}...)
	bt := unaggregated.MetricUnion{
		Type:          metric.TimerType,
		ID:            testBatchTimerID,
		BatchTimerVal: input,
		TimerValPool:  timerValPool,
	}
	e, _, _ := testEntry(ctrl)
	require.NoError(t, e.AddUntimed(bt, testDefaultStagedMetadatas))

	// Assert the timer values have been returned to pool.
	vals := timerValPool.Get(10)
	require.Equal(t, []float64{1.0, 3.5, 2.2, 6.5, 4.8}, vals[:5])
}

func TestEntryAddBatchTimerWithTimerBatchSizeLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	e, _, now := testEntry(ctrl)
	*now = time.Unix(105, 0)
	e.opts = e.opts.
		SetMaxTimerBatchSizePerWrite(2).
		SetDefaultStoragePolicies(testDefaultStoragePolicies)

	bt := unaggregated.MetricUnion{
		Type:          metric.TimerType,
		ID:            testBatchTimerID,
		BatchTimerVal: []float64{1.0, 3.5, 2.2, 6.5, 4.8},
	}
	require.NoError(t, e.AddUntimed(bt, testDefaultStagedMetadatas))
	require.Equal(t, 2, len(e.aggregations))
	require.Equal(t, 1, len(testDefaultStagedMetadatas))
	require.Equal(t, 1, len(testDefaultStagedMetadatas[0].Pipelines))
	for _, key := range testDefaultAggregationKeys {
		idx := e.aggregations.index(key)
		require.True(t, idx >= 0)
		elem := e.aggregations[idx].elem.Value.(*TimerElem)
		require.Equal(t, 1, len(elem.values))
		require.Equal(t, 18.0, elem.values[0].lockedAgg.aggregation.Sum())
	}
}

func TestEntryAddBatchTimerWithTimerBatchSizeLimitError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	e, _, _ := testEntry(ctrl)
	e.opts = e.opts.SetMaxTimerBatchSizePerWrite(2)
	e.closed = true

	bt := unaggregated.MetricUnion{
		Type:          metric.TimerType,
		ID:            testBatchTimerID,
		BatchTimerVal: []float64{1.0, 3.5, 2.2, 6.5, 4.8},
	}
	require.Equal(t, errEntryClosed, e.AddUntimed(bt, testDefaultStagedMetadatas))
}

func TestEntryAddUntimedEmptyMetadatasError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	e, _, _ := testEntry(ctrl)
	inputMetadatas := metadata.StagedMetadatas{}
	require.Equal(t, errEmptyMetadatas, e.AddUntimed(testCounter, inputMetadatas))
}

func TestEntryAddUntimedFutureMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	nowNanos := time.Now().UnixNano()
	inputMetadatas := metadata.StagedMetadatas{
		metadata.StagedMetadata{
			CutoverNanos: nowNanos + 100,
			Tombstoned:   false,
			Metadata:     metadata.Metadata{Pipelines: testPipelines},
		},
	}
	e, _, now := testEntry(ctrl)
	*now = time.Unix(0, nowNanos)
	require.Equal(t, errNoApplicableMetadata, e.AddUntimed(testCounter, inputMetadatas))
}

func TestEntryAddUntimedNoPipelinesInMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	nowNanos := time.Now().UnixNano()
	inputMetadatas := metadata.StagedMetadatas{
		metadata.StagedMetadata{
			CutoverNanos: nowNanos - 100,
			Tombstoned:   false,
		},
	}
	e, _, now := testEntry(ctrl)
	*now = time.Unix(0, nowNanos)
	require.Equal(t, errNoPipelinesInMetadata, e.AddUntimed(testCounter, inputMetadatas))
}

func TestEntryAddUntimedInvalidMetricError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	e, _, _ := testEntry(ctrl)
	require.Error(t, e.AddUntimed(testInvalidMetric, metadata.DefaultStagedMetadatas))
}

func TestEntryAddUntimedClosedEntryError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	e, _, _ := testEntry(ctrl)
	e.closed = true
	require.Equal(t, errEntryClosed, e.AddUntimed(testCounter, metadata.DefaultStagedMetadatas))
}

func TestEntryAddUntimedClosedListError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	e, lists, _ := testEntry(ctrl)
	e.closed = false
	lists.closed = true
	require.Error(t, e.AddUntimed(testCounter, metadata.DefaultStagedMetadatas))
}

func TestEntryAddUntimedDefaultStagedMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		withPrepopulation       = true
		prePopulateData         = testDefaultAggregationKeys
		inputMetadatas          = testDefaultStagedMetadatas
		expectedShouldAdd       = true
		expectedCutoverNanos    = int64(0)
		expectedAggregationKeys = testDefaultAggregationKeys
		lists                   *metricLists
	)

	preAddFn := func(e *Entry, now *time.Time) {
		e.hasDefaultMetadatas = true
		e.cutoverNanos = 0
		lists = e.lists
	}
	postAddFn := func(t *testing.T) {
		require.Equal(t, 2, len(lists.lists))
		for _, key := range expectedAggregationKeys {
			listID := standardMetricListID{
				resolution: key.storagePolicy.Resolution().Window,
			}.toMetricListID()
			res, exists := lists.lists[listID]
			require.True(t, exists)
			list := res.(*standardMetricList)
			require.Equal(t, 1, list.aggregations.Len())
			checkElemTombstoned(t, list.aggregations.Front().Value.(metricElem), nil)
		}
	}

	testEntryAddUntimed(
		t, ctrl, withPrepopulation, prePopulateData,
		preAddFn, inputMetadatas, postAddFn, expectedShouldAdd,
		expectedCutoverNanos, expectedAggregationKeys,
	)
}

func TestEntryAddUntimedSameDefaultMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		withPrepopulation = true
		prePopulateData   = testDefaultAggregationKeys
		nowNanos          = time.Now().UnixNano()
		inputMetadatas    = metadata.StagedMetadatas{
			metadata.StagedMetadata{
				CutoverNanos: nowNanos - 1000,
				Tombstoned:   false,
				Metadata:     metadata.Metadata{Pipelines: testPipelines},
			},
			metadata.StagedMetadata{
				CutoverNanos: nowNanos - 100,
				Tombstoned:   false,
				Metadata:     metadata.Metadata{Pipelines: testDefaultPipelines},
			},
			metadata.StagedMetadata{
				CutoverNanos: nowNanos + 100,
				Tombstoned:   false,
				Metadata:     metadata.Metadata{Pipelines: testPipelines},
			},
		}
		expectedShouldAdd       = true
		expectedCutoverNanos    = nowNanos - 100
		expectedAggregationKeys = testDefaultAggregationKeys
		lists                   *metricLists
	)

	preAddFn := func(e *Entry, now *time.Time) {
		*now = time.Unix(0, nowNanos)
		e.hasDefaultMetadatas = false
		e.cutoverNanos = nowNanos - 100
		lists = e.lists
	}
	postAddFn := func(t *testing.T) {
		require.Equal(t, 2, len(lists.lists))
		for _, key := range expectedAggregationKeys {
			listID := standardMetricListID{
				resolution: key.storagePolicy.Resolution().Window,
			}.toMetricListID()
			res, exists := lists.lists[listID]
			require.True(t, exists)
			list := res.(*standardMetricList)
			require.Equal(t, 1, list.aggregations.Len())
			checkElemTombstoned(t, list.aggregations.Front().Value.(metricElem), nil)
		}
	}

	testEntryAddUntimed(
		t, ctrl, withPrepopulation, prePopulateData,
		preAddFn, inputMetadatas, postAddFn, expectedShouldAdd,
		expectedCutoverNanos, expectedAggregationKeys,
	)
}

func TestEntryAddUntimedSameCustomMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		withPrepopulation = true
		prePopulateData   = testAggregationKeys
		nowNanos          = time.Now().UnixNano()
		inputMetadatas    = metadata.StagedMetadatas{
			metadata.StagedMetadata{
				CutoverNanos: nowNanos - 1000,
				Tombstoned:   false,
				Metadata:     metadata.Metadata{Pipelines: testDefaultPipelines},
			},
			metadata.StagedMetadata{
				CutoverNanos: nowNanos - 100,
				Tombstoned:   false,
				Metadata:     metadata.Metadata{Pipelines: testPipelines},
			},
			metadata.StagedMetadata{
				CutoverNanos: nowNanos + 100,
				Tombstoned:   false,
				Metadata:     metadata.Metadata{Pipelines: testDefaultPipelines},
			},
		}
		expectedShouldAdd       = true
		expectedCutoverNanos    = nowNanos - 100
		expectedAggregationKeys = testAggregationKeys
		lists                   *metricLists
	)

	preAddFn := func(e *Entry, now *time.Time) {
		*now = time.Unix(0, nowNanos)
		e.hasDefaultMetadatas = false
		e.cutoverNanos = nowNanos - 100
		lists = e.lists
	}
	postAddFn := func(t *testing.T) {
		require.Equal(t, 3, len(lists.lists))
		for _, key := range testAggregationKeys {
			listID := standardMetricListID{
				resolution: key.storagePolicy.Resolution().Window,
			}.toMetricListID()
			res, exists := lists.lists[listID]
			require.True(t, exists)
			list := res.(*standardMetricList)
			require.Equal(t, 1, list.aggregations.Len())
			checkElemTombstoned(t, list.aggregations.Front().Value.(metricElem), nil)
		}
	}

	testEntryAddUntimed(
		t, ctrl, withPrepopulation, prePopulateData,
		preAddFn, inputMetadatas, postAddFn, expectedShouldAdd,
		expectedCutoverNanos, expectedAggregationKeys,
	)
}

func TestEntryAddUntimedDifferentTombstone(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		withPrepopulation = true
		prePopulateData   = testAggregationKeys
		nowNanos          = time.Now().UnixNano()
		inputMetadatas    = metadata.StagedMetadatas{
			metadata.StagedMetadata{
				CutoverNanos: nowNanos - 1000,
				Tombstoned:   false,
				Metadata:     metadata.Metadata{Pipelines: testDefaultPipelines},
			},
			metadata.StagedMetadata{
				CutoverNanos: nowNanos - 100,
				Tombstoned:   true,
				Metadata:     metadata.Metadata{Pipelines: nil},
			},
			metadata.StagedMetadata{
				CutoverNanos: nowNanos + 100,
				Tombstoned:   false,
				Metadata:     metadata.Metadata{Pipelines: testDefaultPipelines},
			},
		}
		expectedShouldAdd       = false
		expectedCutoverNanos    = nowNanos - 100
		expectedAggregationKeys = testAggregationKeys
		lists                   *metricLists
	)

	preAddFn := func(e *Entry, now *time.Time) {
		*now = time.Unix(0, nowNanos)
		e.hasDefaultMetadatas = false
		e.cutoverNanos = nowNanos - 100
		lists = e.lists
	}
	postAddFn := func(t *testing.T) {
		require.Equal(t, 3, len(lists.lists))
		for _, key := range expectedAggregationKeys {
			listID := standardMetricListID{
				resolution: key.storagePolicy.Resolution().Window,
			}.toMetricListID()
			res, exists := lists.lists[listID]
			require.True(t, exists)
			list := res.(*standardMetricList)
			require.Equal(t, 1, list.aggregations.Len())
			checkElemTombstoned(t, list.aggregations.Front().Value.(metricElem), nil)
		}
	}

	testEntryAddUntimed(
		t, ctrl, withPrepopulation, prePopulateData,
		preAddFn, inputMetadatas, postAddFn, expectedShouldAdd,
		expectedCutoverNanos, expectedAggregationKeys,
	)
}

func TestEntryAddUntimedDifferentCutoverSameMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		withPrepopulation = true
		prePopulateData   = testAggregationKeys
		nowNanos          = time.Now().UnixNano()
		inputMetadatas    = metadata.StagedMetadatas{
			metadata.StagedMetadata{
				CutoverNanos: nowNanos - 1000,
				Tombstoned:   false,
				Metadata:     metadata.Metadata{Pipelines: testDefaultPipelines},
			},
			metadata.StagedMetadata{
				CutoverNanos: nowNanos - 10,
				Tombstoned:   false,
				Metadata:     metadata.Metadata{Pipelines: testPipelines},
			},
			metadata.StagedMetadata{
				CutoverNanos: nowNanos + 100,
				Tombstoned:   false,
				Metadata:     metadata.Metadata{Pipelines: testDefaultPipelines},
			},
		}
		expectedShouldAdd       = true
		expectedCutoverNanos    = nowNanos - 10
		expectedAggregationKeys = testAggregationKeys
		lists                   *metricLists
	)

	preAddFn := func(e *Entry, now *time.Time) {
		*now = time.Unix(0, nowNanos)
		e.hasDefaultMetadatas = false
		e.cutoverNanos = nowNanos - 100
		lists = e.lists
	}
	postAddFn := func(t *testing.T) {
		require.Equal(t, 3, len(lists.lists))
		for _, key := range expectedAggregationKeys {
			listID := standardMetricListID{
				resolution: key.storagePolicy.Resolution().Window,
			}.toMetricListID()
			res, exists := lists.lists[listID]
			require.True(t, exists)
			list := res.(*standardMetricList)
			require.Equal(t, 1, list.aggregations.Len())
			checkElemTombstoned(t, list.aggregations.Front().Value.(metricElem), nil)
		}
	}

	testEntryAddUntimed(
		t, ctrl, withPrepopulation, prePopulateData,
		preAddFn, inputMetadatas, postAddFn, expectedShouldAdd,
		expectedCutoverNanos, expectedAggregationKeys,
	)
}

func TestEntryAddUntimedDifferentCutoverDifferentMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		withPrepopulation = true
		prePopulateData   = testAggregationKeys
		nowNanos          = time.Now().UnixNano()
		inputMetadatas    = metadata.StagedMetadatas{
			metadata.StagedMetadata{
				CutoverNanos: nowNanos - 1000,
				Tombstoned:   false,
				Metadata:     metadata.Metadata{Pipelines: testDefaultPipelines},
			},
			metadata.StagedMetadata{
				CutoverNanos: nowNanos - 10,
				Tombstoned:   false,
				Metadata:     metadata.Metadata{Pipelines: testNewPipelines},
			},
			metadata.StagedMetadata{
				CutoverNanos: nowNanos + 100,
				Tombstoned:   false,
				Metadata:     metadata.Metadata{Pipelines: testPipelines},
			},
		}
		expectedShouldAdd       = true
		expectedCutoverNanos    = nowNanos - 10
		expectedAggregationKeys = testNewAggregationKeys
		lists                   *metricLists
	)

	deletedStoragePolicies := make(map[policy.StoragePolicy]struct{})
	deletedStoragePolicies[testAggregationKeys[1].storagePolicy] = struct{}{}
	deletedStoragePolicies[testAggregationKeys[2].storagePolicy] = struct{}{}

	preAddFn := func(e *Entry, now *time.Time) {
		*now = time.Unix(0, nowNanos)
		e.hasDefaultMetadatas = false
		e.cutoverNanos = nowNanos - 100
		lists = e.lists
	}
	postAddFn := func(t *testing.T) {
		require.Equal(t, 4, len(lists.lists))
		expectedLengths := [][]int{{1, 1, 2}, {1, 2, 1}}
		for i, keys := range [][]aggregationKey{testAggregationKeys, testNewAggregationKeys} {
			for j := range keys {
				listID := standardMetricListID{
					resolution: keys[j].storagePolicy.Resolution().Window,
				}.toMetricListID()
				res, exists := lists.lists[listID]
				require.True(t, exists)
				list := res.(*standardMetricList)
				require.Equal(t, expectedLengths[i][j], list.aggregations.Len())
				for elem := list.aggregations.Front(); elem != nil; elem = elem.Next() {
					checkElemTombstoned(t, elem.Value.(metricElem), deletedStoragePolicies)
				}
			}
		}
	}

	testEntryAddUntimed(
		t, ctrl, withPrepopulation, prePopulateData,
		preAddFn, inputMetadatas, postAddFn, expectedShouldAdd,
		expectedCutoverNanos, expectedAggregationKeys,
	)
}

func TestEntryAddUntimedWithPolicyUpdateIDNotOwnedCopyID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		withPrepopulation = false
		prePopulateData   = testAggregationKeys
		nowNanos          = time.Now().UnixNano()
		inputMetadatas    = metadata.StagedMetadatas{
			metadata.StagedMetadata{
				CutoverNanos: nowNanos - 1000,
				Tombstoned:   false,
				Metadata:     metadata.Metadata{Pipelines: testDefaultPipelines},
			},
			metadata.StagedMetadata{
				CutoverNanos: nowNanos - 10,
				Tombstoned:   false,
				Metadata:     metadata.Metadata{Pipelines: testPipelines},
			},
			metadata.StagedMetadata{
				CutoverNanos: nowNanos + 100,
				Tombstoned:   false,
				Metadata:     metadata.Metadata{Pipelines: testPipelines},
			},
		}
		expectedShouldAdd       = true
		expectedCutoverNanos    = nowNanos - 10
		expectedAggregationKeys = testAggregationKeys
		lists                   *metricLists
	)

	preAddFn := func(e *Entry, now *time.Time) {
		*now = time.Unix(0, nowNanos)
		e.cutoverNanos = uninitializedCutoverNanos
		lists = e.lists
	}
	postAddFn := func(t *testing.T) {
		require.Equal(t, 3, len(lists.lists))
		for _, key := range expectedAggregationKeys {
			listID := standardMetricListID{
				resolution: key.storagePolicy.Resolution().Window,
			}.toMetricListID()
			res, exists := lists.lists[listID]
			require.True(t, exists)
			list := res.(*standardMetricList)
			require.Equal(t, 1, list.aggregations.Len())
			for elem := list.aggregations.Front(); elem != nil; elem = elem.Next() {
				checkElemTombstoned(t, elem.Value.(metricElem), nil)
			}
		}
	}
	testEntryAddUntimed(
		t, ctrl, withPrepopulation, prePopulateData,
		preAddFn, inputMetadatas, postAddFn, expectedShouldAdd,
		expectedCutoverNanos, expectedAggregationKeys,
	)
}

func TestEntryAddUntimedDuplicateAggregationKeys(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		withPrepopulation = true
		prePopulateData   = testAggregationKeys
		nowNanos          = time.Now().UnixNano()
		inputMetadatas    = metadata.StagedMetadatas{
			metadata.StagedMetadata{
				CutoverNanos: nowNanos - 1000,
				Tombstoned:   false,
				Metadata:     metadata.Metadata{Pipelines: testDefaultPipelines},
			},
			metadata.StagedMetadata{
				CutoverNanos: nowNanos - 10,
				Tombstoned:   false,
				Metadata:     metadata.Metadata{Pipelines: testPipelinesWithDuplicates2},
			},
			metadata.StagedMetadata{
				CutoverNanos: nowNanos + 100,
				Tombstoned:   false,
				Metadata:     metadata.Metadata{Pipelines: testPipelines},
			},
		}
		expectedShouldAdd       = true
		expectedCutoverNanos    = nowNanos - 10
		expectedAggregationKeys = testWithDuplicates2AggregationKeys
		lists                   *metricLists
	)

	deletedStoragePolicies := make(map[policy.StoragePolicy]struct{})
	for _, key := range testAggregationKeys {
		deletedStoragePolicies[key.storagePolicy] = struct{}{}
	}

	preAddFn := func(e *Entry, now *time.Time) {
		*now = time.Unix(0, nowNanos)
		e.cutoverNanos = uninitializedCutoverNanos
		lists = e.lists
	}
	postAddFn := func(t *testing.T) {
		require.Equal(t, 3, len(lists.lists))
		expectedLengths := [][]int{
			{3, 1, 3},
			{3, 3, 3, 3},
		}
		for i, keys := range [][]aggregationKey{testAggregationKeys, expectedAggregationKeys} {
			for j, key := range keys {
				listID := standardMetricListID{
					resolution: key.storagePolicy.Resolution().Window,
				}.toMetricListID()
				res, exists := lists.lists[listID]
				require.True(t, exists)
				list := res.(*standardMetricList)
				require.Equal(t, expectedLengths[i][j], list.aggregations.Len())
				for elem := list.aggregations.Front(); elem != nil; elem = elem.Next() {
					checkElemTombstoned(t, elem.Value.(metricElem), deletedStoragePolicies)
				}
			}
		}
	}
	testEntryAddUntimed(
		t, ctrl, withPrepopulation, prePopulateData,
		preAddFn, inputMetadatas, postAddFn, expectedShouldAdd,
		expectedCutoverNanos, expectedAggregationKeys,
	)
}

func TestEntryAddUntimedWithInvalidAggregationType(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		compressedMin   = aggregation.MustCompressTypes(aggregation.Min)
		compressedLast  = aggregation.MustCompressTypes(aggregation.Last)
		compressedP9999 = aggregation.MustCompressTypes(aggregation.P9999)
	)
	inputs := []struct {
		metric        unaggregated.MetricUnion
		aggregationID aggregation.ID
		expectError   bool
	}{
		{
			metric:        testCounter,
			aggregationID: compressedMin,
			expectError:   false,
		},
		{
			metric:        testCounter,
			aggregationID: compressedP9999,
			expectError:   true,
		},
		{
			metric:        testGauge,
			aggregationID: compressedMin,
			expectError:   false,
		},
		{
			metric:        testGauge,
			aggregationID: compressedP9999,
			expectError:   true,
		},
		{
			metric:        testBatchTimer,
			aggregationID: compressedMin,
			expectError:   false,
		},
		{
			metric:        testBatchTimer,
			aggregationID: compressedLast,
			expectError:   true,
		},
	}

	for _, input := range inputs {
		e, _, _ := testEntry(ctrl)
		metadatas := metadata.StagedMetadatas{
			{
				Metadata: metadata.Metadata{
					Pipelines: []metadata.PipelineMetadata{
						{
							AggregationID: input.aggregationID,
						},
					},
				},
			},
		}
		if input.expectError {
			require.Error(t, e.AddUntimed(input.metric, metadatas))
		} else {
			require.NoError(t, e.AddUntimed(input.metric, metadatas))
		}
	}
}

func TestEntryAddUntimedWithInvalidPipeline(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	invalidPipeline := metadata.PipelineMetadata{
		Pipeline: applied.NewPipeline([]applied.OpUnion{
			{
				Type:           pipeline.TransformationOpType,
				Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
			},
		}),
	}
	e, _, _ := testEntry(ctrl)
	metadatas := metadata.StagedMetadatas{
		{Metadata: metadata.Metadata{Pipelines: []metadata.PipelineMetadata{invalidPipeline}}},
	}
	err := e.AddUntimed(testCounter, metadatas)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "has no rollup operations"))
}

func TestShouldUpdateStagedMetadataWithLock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	currTimeNanos := time.Now().UnixNano()
	inputs := []struct {
		cutoverNanos    int64
		aggregationKeys []aggregationKey
		metadata        metadata.StagedMetadata
		expected        bool
	}{
		{
			// Uninitialized cutover time.
			cutoverNanos: uninitializedCutoverNanos,
			metadata: metadata.StagedMetadata{
				CutoverNanos: currTimeNanos - 100,
				Metadata:     metadata.Metadata{Pipelines: []metadata.PipelineMetadata{{}}},
			},
			expected: true,
		},
		{
			// Earlier cutover time with default metadata.
			cutoverNanos: currTimeNanos,
			metadata: metadata.StagedMetadata{
				CutoverNanos: currTimeNanos - 100,
				Metadata:     metadata.Metadata{Pipelines: []metadata.PipelineMetadata{{}}},
			},
			expected: false,
		},
		{
			// Later cutover time.
			cutoverNanos: currTimeNanos - 100,
			metadata: metadata.StagedMetadata{
				CutoverNanos: currTimeNanos,
				Metadata:     metadata.Metadata{Pipelines: []metadata.PipelineMetadata{{}}},
			},
			expected: true,
		},
		{
			// Same cutover time with empty aggregations and default metadata.
			cutoverNanos: currTimeNanos,
			metadata: metadata.StagedMetadata{
				CutoverNanos: currTimeNanos,
				Metadata:     metadata.Metadata{Pipelines: []metadata.PipelineMetadata{{}}},
			},
			expected: true,
		},
		{
			// Same cutover time with default aggregations and default metadata.
			cutoverNanos:    currTimeNanos,
			aggregationKeys: testDefaultAggregationKeys,
			metadata: metadata.StagedMetadata{
				CutoverNanos: currTimeNanos,
				Metadata:     metadata.Metadata{Pipelines: []metadata.PipelineMetadata{{}}},
			},
			expected: false,
		},
		{
			// Same cutover time with custom aggregations (more aggregation keys) and default metadata.
			cutoverNanos:    currTimeNanos,
			aggregationKeys: testAggregationKeys,
			metadata: metadata.StagedMetadata{
				CutoverNanos: currTimeNanos,
				Metadata:     metadata.Metadata{Pipelines: []metadata.PipelineMetadata{{}}},
			},
			expected: true,
		},
		{
			// Same cutover time with custom aggregations (less aggregation keys) and custom metadata.
			cutoverNanos:    currTimeNanos,
			aggregationKeys: testDefaultAggregationKeys,
			metadata: metadata.StagedMetadata{
				CutoverNanos: currTimeNanos,
				Metadata:     metadata.Metadata{Pipelines: testPipelines},
			},
			expected: true,
		},
		{
			// Same cutover time with custom aggregations (same number of aggregation keys)
			// and custom metadata.
			cutoverNanos:    currTimeNanos,
			aggregationKeys: testAggregationKeys,
			metadata: metadata.StagedMetadata{
				CutoverNanos: currTimeNanos,
				Metadata:     metadata.Metadata{Pipelines: testNewPipelines},
			},
			expected: true,
		},
		{
			// Same cutover time with custom aggregations and custom metadata containing pipelines.
			cutoverNanos:    currTimeNanos,
			aggregationKeys: testDefaultAggregationKeys,
			metadata: metadata.StagedMetadata{
				CutoverNanos: currTimeNanos,
				Metadata:     metadata.Metadata{Pipelines: testNewPipelines2},
			},
			expected: true,
		},
		{
			// Same cutover time with custom aggregations containing pipelines and
			// custom metadata containing pipelines.
			cutoverNanos:    currTimeNanos,
			aggregationKeys: testNewAggregationKeys2,
			metadata: metadata.StagedMetadata{
				CutoverNanos: currTimeNanos,
				Metadata:     metadata.Metadata{Pipelines: testNewPipelines3},
			},
			expected: true,
		},
		{
			// Same cutover time with custom aggregations containing pipelines and
			// custom metadata containing pipelines.
			cutoverNanos:    currTimeNanos,
			aggregationKeys: testNewAggregationKeys3,
			metadata: metadata.StagedMetadata{
				CutoverNanos: currTimeNanos,
				Metadata:     metadata.Metadata{Pipelines: testNewPipelines3},
			},
			expected: false,
		},
		{
			// Same cutover time with default aggregations and default metadata.
			cutoverNanos:    currTimeNanos,
			aggregationKeys: testDefaultAggregationKeys,
			metadata: metadata.StagedMetadata{
				CutoverNanos: currTimeNanos,
				Metadata:     metadata.Metadata{Pipelines: testPipelinesWithDuplicates},
			},
			expected: false,
		},
	}

	for _, input := range inputs {
		e, _, _ := testEntry(ctrl)
		e.cutoverNanos = input.cutoverNanos
		populateTestUntimedAggregations(t, e, input.aggregationKeys, metric.CounterType)
		e.Lock()
		require.Equal(t, input.expected, e.shouldUpdateStagedMetadatasWithLock(input.metadata))
		e.Unlock()
	}
}

func TestEntryStoragePolicies(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	inputs := []struct {
		policies policy.StoragePolicies
		expected policy.StoragePolicies
	}{
		{
			policies: []policy.StoragePolicy{},
			expected: testDefaultStoragePolicies,
		},
		{
			policies: testDefaultStoragePolicies,
			expected: testDefaultStoragePolicies,
		},
		{
			policies: []policy.StoragePolicy{
				policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour),
			},
			expected: []policy.StoragePolicy{
				policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour),
			},
		},
	}

	e, _, _ := testEntry(ctrl)
	for _, input := range inputs {
		require.Equal(t, input.expected, e.storagePolicies(input.policies))
	}
}

func TestEntryForwardedRateLimiting(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	e, _, now := testEntry(ctrl)

	// Reset runtime options to disable rate limiting.
	noRateLimitRuntimeOpts := runtime.NewOptions().SetWriteValuesPerMetricLimitPerSecond(0)
	e.SetRuntimeOptions(noRateLimitRuntimeOpts)
	require.NoError(t, e.AddForwarded(testForwardedMetric, testForwardMetadata))

	// Reset runtime options to enable a rate limit of 10/s.
	limitPerSecond := 10
	runtimeOpts := runtime.NewOptions().SetWriteValuesPerMetricLimitPerSecond(int64(limitPerSecond))
	e.SetRuntimeOptions(runtimeOpts)
	for i := 0; i < limitPerSecond; i++ {
		require.NoError(t, e.AddForwarded(testForwardedMetric, testForwardMetadata))
	}
	require.Equal(t, errWriteValueRateLimitExceeded, e.AddForwarded(testForwardedMetric, testForwardMetadata))

	// Reset limit to enable a rate limit of 100/s.
	limitPerSecond = 100
	runtimeOpts = runtime.NewOptions().SetWriteValuesPerMetricLimitPerSecond(int64(limitPerSecond))
	e.SetRuntimeOptions(runtimeOpts)
	for i := 0; i < limitPerSecond; i++ {
		require.NoError(t, e.AddForwarded(testForwardedMetric, testForwardMetadata))
	}
	require.Equal(t, errWriteValueRateLimitExceeded, e.AddForwarded(testForwardedMetric, testForwardMetadata))

	// Advancing the time will reset the quota.
	*now = (*now).Add(time.Second)
	require.NoError(t, e.AddForwarded(testForwardedMetric, testForwardMetadata))
}

func TestEntryAddForwardedEntryClosed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	e, _, _ := testEntry(ctrl)
	e.closed = true
	require.Equal(t, errEntryClosed, e.AddForwarded(testForwardedMetric, testForwardMetadata))
}

func TestEntryAddForwardedMetricTooLate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	maxAllowedForwardingDelayFn := func(
		resolution time.Duration,
		numForwardedTimes int,
	) time.Duration {
		return resolution + time.Second*time.Duration(numForwardedTimes)
	}
	e, _, now := testEntry(ctrl)
	e.opts = e.opts.SetMaxAllowedForwardingDelayFn(maxAllowedForwardingDelayFn)

	inputs := []struct {
		now               time.Time
		timeNanos         int64
		storagePolicy     policy.StoragePolicy
		numForwardedTimes int
	}{
		{
			now:               time.Unix(1264, 0),
			timeNanos:         1224 * time.Second.Nanoseconds(),
			storagePolicy:     policy.NewStoragePolicy(10*time.Second, xtime.Second, time.Hour),
			numForwardedTimes: 3,
		},
		{
			now:               time.Unix(1286, 0),
			timeNanos:         1224 * time.Second.Nanoseconds(),
			storagePolicy:     policy.NewStoragePolicy(time.Minute, xtime.Minute, time.Hour),
			numForwardedTimes: 1,
		},
	}

	for _, input := range inputs {
		*now = input.now
		metric := testForwardedMetric
		metric.TimeNanos = input.timeNanos
		metadata := testForwardMetadata
		metadata.StoragePolicy = input.storagePolicy
		metadata.NumForwardedTimes = input.numForwardedTimes
		require.Equal(t, errArrivedTooLate, e.AddForwarded(metric, metadata))
	}
}

func TestEntryAddForwarded(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	e, lists, _ := testEntry(ctrl)

	// Add an initial forwarded metric.
	require.NoError(t, e.AddForwarded(testForwardedMetric, testForwardMetadata1))
	require.Equal(t, 1, len(e.aggregations))
	expectedKey := aggregationKey{
		aggregationID:     testForwardMetadata1.AggregationID,
		storagePolicy:     testForwardMetadata1.StoragePolicy,
		pipeline:          testForwardMetadata1.Pipeline,
		numForwardedTimes: testForwardMetadata1.NumForwardedTimes,
	}
	idx := e.aggregations.index(expectedKey)
	require.True(t, idx >= 0)
	expectedElem := e.aggregations[idx].elem
	require.Equal(t, 1, len(lists.lists))
	expectedListID := forwardedMetricListID{
		resolution:        testForwardMetadata1.StoragePolicy.Resolution().Window,
		numForwardedTimes: testForwardMetadata1.NumForwardedTimes,
	}.toMetricListID()
	res, exists := lists.lists[expectedListID]
	require.True(t, exists)
	list := res.(*forwardedMetricList)
	require.Equal(t, expectedListID.forwarded.resolution, list.resolution)
	require.Equal(t, expectedListID.forwarded.numForwardedTimes, list.numForwardedTimes)
	require.Equal(t, 1, list.Len())
	require.True(t, expectedElem == list.aggregations.Front())
	checkElemTombstoned(t, expectedElem.Value.(metricElem), nil)
	values := expectedElem.Value.(*CounterElem).values
	require.Equal(t, 1, len(values))
	resolution := testForwardMetadata1.StoragePolicy.Resolution().Window
	expectedNanos := time.Unix(0, testForwardedMetric.TimeNanos).Truncate(resolution).UnixNano()
	require.Equal(t, expectedNanos, values[0].startAtNanos)
	require.Equal(t, int64(1), values[0].lockedAgg.aggregation.Count())
	require.Equal(t, int64(testForwardedMetric.Value), values[0].lockedAgg.aggregation.Sum())

	// Add the forwarded metric again with duplicate metadata should not result in an error.
	require.NoError(t, e.AddForwarded(testForwardedMetric, testForwardMetadata1))
	require.Equal(t, 1, len(e.aggregations))
	idx = e.aggregations.index(expectedKey)
	require.True(t, idx >= 0)
	expectedElem = e.aggregations[idx].elem
	values = expectedElem.Value.(*CounterElem).values
	require.Equal(t, 1, len(values))
	require.Equal(t, int64(1), values[0].lockedAgg.aggregation.Count())
	require.Equal(t, int64(testForwardedMetric.Value), values[0].lockedAgg.aggregation.Sum())

	// Add the forwarded metric with same forward metadata and different source ID.
	metadata := testForwardMetadata1
	metadata.SourceID = []byte("newSourceID")
	require.NoError(t, e.AddForwarded(testForwardedMetric, metadata))
	require.Equal(t, 1, len(e.aggregations))
	idx = e.aggregations.index(expectedKey)
	require.True(t, idx >= 0)
	expectedElem = e.aggregations[idx].elem
	values = expectedElem.Value.(*CounterElem).values
	require.Equal(t, 1, len(values))
	require.Equal(t, int64(2), values[0].lockedAgg.aggregation.Count())
	require.Equal(t, 2*int64(testForwardedMetric.Value), values[0].lockedAgg.aggregation.Sum())

	// Add the forwarded metric with different timestamp and same forward metadata.
	metric := testForwardedMetric
	metric.TimeNanos += testForwardMetadata1.StoragePolicy.Resolution().Window.Nanoseconds()
	require.NoError(t, e.AddForwarded(metric, testForwardMetadata1))
	require.Equal(t, 1, len(e.aggregations))
	idx = e.aggregations.index(expectedKey)
	require.True(t, idx >= 0)
	expectedElem = e.aggregations[idx].elem
	values = expectedElem.Value.(*CounterElem).values
	require.Equal(t, 2, len(values))
	expectedNanos += testForwardMetadata1.StoragePolicy.Resolution().Window.Nanoseconds()
	require.Equal(t, expectedNanos, values[1].startAtNanos)
	require.Equal(t, int64(1), values[1].lockedAgg.aggregation.Count())
	require.Equal(t, int64(testForwardedMetric.Value), values[1].lockedAgg.aggregation.Sum())

	// Add the forwarded metric with a different metadata.
	metric.ID = make(id.RawID, len(testForwardedMetric.ID))
	copy(metric.ID, testForwardedMetric.ID)
	require.NoError(t, e.AddForwarded(metric, testForwardMetadata2))
	require.Equal(t, 1, len(e.aggregations))
	expectedKeyNew := aggregationKey{
		aggregationID:     testForwardMetadata2.AggregationID,
		storagePolicy:     testForwardMetadata2.StoragePolicy,
		pipeline:          testForwardMetadata2.Pipeline,
		numForwardedTimes: testForwardMetadata2.NumForwardedTimes,
	}
	idx = e.aggregations.index(expectedKeyNew)
	require.True(t, idx >= 0)
	expectedElemNew := e.aggregations[idx].elem
	require.Equal(t, 2, len(lists.lists))
	expectedListIDNew := forwardedMetricListID{
		resolution:        testForwardMetadata2.StoragePolicy.Resolution().Window,
		numForwardedTimes: testForwardMetadata2.NumForwardedTimes,
	}.toMetricListID()
	res, exists = lists.lists[expectedListIDNew]
	require.True(t, exists)
	listNew := res.(*forwardedMetricList)
	require.Equal(t, expectedListIDNew.forwarded.resolution, listNew.resolution)
	require.Equal(t, expectedListIDNew.forwarded.numForwardedTimes, listNew.numForwardedTimes)
	require.Equal(t, 1, listNew.Len())
	require.True(t, expectedElemNew == listNew.aggregations.Front())
	expectedTombstoned := map[policy.StoragePolicy]struct{}{
		expectedKey.storagePolicy: struct{}{},
	}
	checkElemTombstoned(t, expectedElem.Value.(metricElem), expectedTombstoned)
	counterElem := expectedElemNew.Value.(*CounterElem)
	values = counterElem.values
	require.Equal(t, 1, len(values))
	resolution = testForwardMetadata2.StoragePolicy.Resolution().Window
	expectedNanos = time.Unix(0, metric.TimeNanos).Truncate(resolution).UnixNano()
	require.Equal(t, expectedNanos, values[0].startAtNanos)
	require.Equal(t, int64(1), values[0].lockedAgg.aggregation.Count())
	require.Equal(t, int64(testForwardedMetric.Value), values[0].lockedAgg.aggregation.Sum())
	require.Equal(t, metric.ID, counterElem.ID())

	// Ensure the ID is properly cloned so mutating the ID externally does not mutate the
	// metric ID stored in the elements.
	metric.ID[0] = '2'
	require.Equal(t, testForwardedMetric.ID, counterElem.ID())
}

func TestEntryMaybeExpireNoExpiry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	e, _, now := testEntry(ctrl)

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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	e, _, now := testEntry(ctrl)
	populateTestUntimedAggregations(t, e, testAggregationKeys, metric.CounterType)

	var elems []*CounterElem
	for _, agg := range e.aggregations {
		elems = append(elems, agg.elem.Value.(*CounterElem))
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

func TestEntryMaybeCopyIDWithLock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	id := id.RawID("foo")
	e, _, _ := testEntry(ctrl)
	res := e.maybeCopyIDWithLock(id)
	require.Equal(t, id, res)

	// Verify the returned ID is a clone of the original ID.
	id[0] = 'b'
	require.NotEqual(t, id, res)
}

func TestAggregationKeyEqual(t *testing.T) {
	inputs := []struct {
		a        aggregationKey
		b        aggregationKey
		expected bool
	}{
		{
			a:        aggregationKey{},
			b:        aggregationKey{},
			expected: true,
		},
		{
			a: aggregationKey{
				aggregationID: aggregation.DefaultID,
				storagePolicy: policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour),
			},
			b: aggregationKey{
				aggregationID: aggregation.DefaultID,
				storagePolicy: policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour),
			},
			expected: true,
		},
		{
			a: aggregationKey{
				aggregationID: aggregation.DefaultID,
				storagePolicy: policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour),
				pipeline: applied.NewPipeline([]applied.OpUnion{
					{
						Type:           pipeline.TransformationOpType,
						Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
					},
					{
						Type: pipeline.RollupOpType,
						Rollup: applied.RollupOp{
							ID:            []byte("foo.baz"),
							AggregationID: aggregation.DefaultID,
						},
					},
				}),
			},
			b: aggregationKey{
				aggregationID: aggregation.DefaultID,
				storagePolicy: policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour),
				pipeline: applied.NewPipeline([]applied.OpUnion{
					{
						Type:           pipeline.TransformationOpType,
						Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
					},
					{
						Type: pipeline.RollupOpType,
						Rollup: applied.RollupOp{
							ID:            []byte("foo.baz"),
							AggregationID: aggregation.DefaultID,
						},
					},
				}),
			},
			expected: true,
		},
		{
			a: aggregationKey{
				aggregationID: aggregation.DefaultID,
				storagePolicy: policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour),
			},
			b: aggregationKey{
				aggregationID: aggregation.MustCompressTypes(aggregation.Count),
				storagePolicy: policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour),
			},
			expected: false,
		},
		{
			a: aggregationKey{
				aggregationID: aggregation.DefaultID,
				storagePolicy: policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour),
			},
			b: aggregationKey{
				aggregationID: aggregation.DefaultID,
				storagePolicy: policy.NewStoragePolicy(time.Minute, xtime.Minute, 48*time.Hour),
			},
			expected: false,
		},
		{
			a: aggregationKey{
				aggregationID: aggregation.DefaultID,
				storagePolicy: policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour),
				pipeline: applied.NewPipeline([]applied.OpUnion{
					{
						Type:           pipeline.TransformationOpType,
						Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
					},
					{
						Type: pipeline.RollupOpType,
						Rollup: applied.RollupOp{
							ID:            []byte("foo.baz"),
							AggregationID: aggregation.DefaultID,
						},
					},
				}),
			},
			b: aggregationKey{
				aggregationID: aggregation.DefaultID,
				storagePolicy: policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour),
				pipeline: applied.NewPipeline([]applied.OpUnion{
					{
						Type:           pipeline.TransformationOpType,
						Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
					},
					{
						Type: pipeline.RollupOpType,
						Rollup: applied.RollupOp{
							ID:            []byte("foo.bar"),
							AggregationID: aggregation.DefaultID,
						},
					},
				}),
			},
			expected: false,
		},
	}

	for _, input := range inputs {
		require.Equal(t, input.expected, input.a.Equal(input.b))
		require.Equal(t, input.expected, input.b.Equal(input.a))
	}
}

func TestAggregationValues(t *testing.T) {
	aggregationKeys := []aggregationKey{
		{},
		{
			aggregationID: aggregation.DefaultID,
			storagePolicy: policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour),
		},
		{
			aggregationID: aggregation.DefaultID,
			storagePolicy: policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour),
			pipeline: applied.NewPipeline([]applied.OpUnion{
				{
					Type:           pipeline.TransformationOpType,
					Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
				},
				{
					Type: pipeline.RollupOpType,
					Rollup: applied.RollupOp{
						ID:            []byte("foo.baz"),
						AggregationID: aggregation.DefaultID,
					},
				},
			}),
		},
	}
	vals := make(aggregationValues, len(aggregationKeys))
	for i := 0; i < len(aggregationKeys); i++ {
		vals[i] = aggregationValue{key: aggregationKeys[i]}
	}

	inputs := []struct {
		key              aggregationKey
		expectedIndex    int
		expectedContains bool
	}{
		{
			key:              aggregationKeys[0],
			expectedIndex:    0,
			expectedContains: true,
		},
		{
			key:              aggregationKeys[1],
			expectedIndex:    1,
			expectedContains: true,
		},
		{
			key:              aggregationKeys[2],
			expectedIndex:    2,
			expectedContains: true,
		},
		{
			key: aggregationKey{
				aggregationID: aggregation.DefaultID,
				storagePolicy: policy.NewStoragePolicy(time.Minute, xtime.Minute, 48*time.Hour),
			},
			expectedIndex:    -1,
			expectedContains: false,
		},
		{
			key: aggregationKey{
				aggregationID: aggregation.DefaultID,
				storagePolicy: policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour),
				pipeline: applied.NewPipeline([]applied.OpUnion{
					{
						Type:           pipeline.TransformationOpType,
						Transformation: pipeline.TransformationOp{Type: transformation.Absolute},
					},
				}),
			},
			expectedIndex:    -1,
			expectedContains: false,
		},
	}
	for _, input := range inputs {
		require.Equal(t, input.expectedIndex, vals.index(input.key))
		require.Equal(t, input.expectedContains, vals.contains(input.key))
	}
}

func testEntry(ctrl *gomock.Controller) (*Entry, *metricLists, *time.Time) {
	now := time.Now()
	clockOpts := clock.NewOptions().SetNowFn(func() time.Time {
		return now
	})
	opts := testOptions(ctrl).
		SetClockOptions(clockOpts).
		SetDefaultStoragePolicies(testDefaultStoragePolicies)

	lists := newMetricLists(testShard, opts)
	runtimeOpts := runtime.NewOptions()
	e := NewEntry(nil, runtimeOpts, opts)
	e.ResetSetData(lists, runtimeOpts, opts)

	return e, lists, &now
}

func populateTestUntimedAggregations(
	t *testing.T,
	e *Entry,
	aggregationKeys []aggregationKey,
	typ metric.Type,
) {
	for _, aggKey := range aggregationKeys {
		var (
			newElem metricElem
			testID  id.RawID
		)
		switch typ {
		case metric.CounterType:
			newElem = e.opts.CounterElemPool().Get()
			testID = testCounterID
		case metric.TimerType:
			newElem = e.opts.TimerElemPool().Get()
			testID = testBatchTimerID
		case metric.GaugeType:
			newElem = e.opts.GaugeElemPool().Get()
			testID = testGaugeID
		default:
			require.Fail(t, fmt.Sprintf("unrecognized metric type: %v", typ))
		}
		aggTypes := e.decompressor.MustDecompress(aggKey.aggregationID)
		newElem.ResetSetData(testID, aggKey.storagePolicy, aggTypes, aggKey.pipeline, 0)
		listID := standardMetricListID{
			resolution: aggKey.storagePolicy.Resolution().Window,
		}.toMetricListID()
		list, err := e.lists.FindOrCreate(listID)
		require.NoError(t, err)
		newListElem, err := list.PushBack(newElem)
		require.NoError(t, err)
		e.aggregations = append(e.aggregations, aggregationValue{key: aggKey, elem: newListElem})
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

func testEntryAddUntimed(
	t *testing.T,
	ctrl *gomock.Controller,
	withPrePopulation bool,
	prePopulateData []aggregationKey,
	preAddFn testPreProcessFn,
	inputMetadatas metadata.StagedMetadatas,
	postAddFn testPostProcessFn,
	expectedShouldAdd bool,
	expectedCutoverNanos int64,
	expectedAggregationKeys []aggregationKey,
) {
	inputs := []testEntryData{
		{
			mu: testCounter,
			fn: func(t *testing.T, elem *list.Element, alignedStart time.Time) {
				id := elem.Value.(*CounterElem).ID()
				require.Equal(t, testCounterID, id)
				aggregations := elem.Value.(*CounterElem).values
				if !expectedShouldAdd {
					require.Equal(t, 0, len(aggregations))
				} else {
					require.Equal(t, 1, len(aggregations))
					require.Equal(t, alignedStart.UnixNano(), aggregations[0].startAtNanos)
					require.Equal(t, int64(1234), aggregations[0].lockedAgg.aggregation.Sum())
				}
			},
		},
		{
			mu: testBatchTimer,
			fn: func(t *testing.T, elem *list.Element, alignedStart time.Time) {
				id := elem.Value.(*TimerElem).ID()
				require.Equal(t, testBatchTimerID, id)
				aggregations := elem.Value.(*TimerElem).values
				if !expectedShouldAdd {
					require.Equal(t, 0, len(aggregations))
				} else {
					require.Equal(t, 1, len(aggregations))
					require.Equal(t, alignedStart.UnixNano(), aggregations[0].startAtNanos)
					require.Equal(t, 18.0, aggregations[0].lockedAgg.aggregation.Sum())
				}
			},
		},
		{
			mu: testGauge,
			fn: func(t *testing.T, elem *list.Element, alignedStart time.Time) {
				id := elem.Value.(*GaugeElem).ID()
				require.Equal(t, testGaugeID, id)
				aggregations := elem.Value.(*GaugeElem).values
				if !expectedShouldAdd {
					require.Equal(t, 0, len(aggregations))
				} else {
					require.Equal(t, 1, len(aggregations))
					require.Equal(t, alignedStart.UnixNano(), aggregations[0].startAtNanos)
					require.Equal(t, 123.456, aggregations[0].lockedAgg.aggregation.Last())
				}
			},
		},
	}

	for _, input := range inputs {
		e, _, now := testEntry(ctrl)
		if withPrePopulation {
			populateTestUntimedAggregations(t, e, prePopulateData, input.mu.Type)
		}

		preAddFn(e, now)
		require.NoError(t, e.AddUntimed(input.mu, inputMetadatas))
		require.Equal(t, now.UnixNano(), e.lastAccessNanos)

		require.Equal(t, len(expectedAggregationKeys), len(e.aggregations))
		for _, key := range expectedAggregationKeys {
			idx := e.aggregations.index(key)
			require.True(t, idx >= 0)
			elem := e.aggregations[idx].elem
			input.fn(t, elem, now.Truncate(key.storagePolicy.Resolution().Window))
		}
		require.Equal(t, expectedCutoverNanos, e.cutoverNanos)

		postAddFn(t)
	}
}

func aggregationKeys(pipelines []metadata.PipelineMetadata) []aggregationKey {
	var aggregationKeys []aggregationKey
	for _, pipeline := range pipelines {
		for _, storagePolicy := range pipeline.StoragePolicies {
			aggKey := aggregationKey{
				aggregationID: pipeline.AggregationID,
				storagePolicy: storagePolicy,
				pipeline:      pipeline.Pipeline,
			}
			aggregationKeys = append(aggregationKeys, aggKey)
		}
	}

	// De-duplicate aggregation keys.
	curr := 0
	for i := 0; i < len(aggregationKeys); i++ {
		found := false
		for j := 0; j < i; j++ {
			if aggregationKeys[i].Equal(aggregationKeys[j]) {
				found = true
				break
			}
		}
		if !found {
			aggregationKeys[curr] = aggregationKeys[i]
			curr++
		}
	}
	return aggregationKeys[:curr]
}

type testPreProcessFn func(e *Entry, now *time.Time)
type testElemValidateFn func(t *testing.T, elem *list.Element, alignedStart time.Time)
type testPostProcessFn func(t *testing.T)

type testEntryData struct {
	mu unaggregated.MetricUnion
	fn testElemValidateFn
}
