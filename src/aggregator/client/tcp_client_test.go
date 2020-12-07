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

//nolint:dupl,exhaustive
package client

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/pipeline"
	"github.com/m3db/m3/src/metrics/pipeline/applied"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"
	xtime "github.com/m3db/m3/src/x/time"
)

var (
	testNowNanos     = time.Now().UnixNano()
	testCutoverNanos = testNowNanos - int64(time.Minute)
	testCutoffNanos  = testNowNanos + int64(time.Hour)
	testCounter      = unaggregated.MetricUnion{
		Type:       metric.CounterType,
		ID:         []byte("foo"),
		CounterVal: 1234,
	}
	testBatchTimer = unaggregated.MetricUnion{
		Type:          metric.TimerType,
		ID:            []byte("foo"),
		BatchTimerVal: []float64{222.22, 345.67, 901.23345},
	}
	testGauge = unaggregated.MetricUnion{
		Type:     metric.GaugeType,
		ID:       []byte("foo"),
		GaugeVal: 123.456,
	}
	testTimed = aggregated.Metric{
		Type:      metric.CounterType,
		ID:        []byte("testTimed"),
		TimeNanos: 1234,
		Value:     178,
	}
	testForwarded = aggregated.ForwardedMetric{
		Type:      metric.CounterType,
		ID:        []byte("testForwarded"),
		TimeNanos: 1234,
		Values:    []float64{34567, 256, 178},
	}
	testPassthrough = aggregated.Metric{
		Type:      metric.CounterType,
		ID:        []byte("testPassthrough"),
		TimeNanos: 12345,
		Value:     123,
	}
	testStagedMetadatas = metadata.StagedMetadatas{
		{
			CutoverNanos: 100,
			Tombstoned:   false,
			Metadata: metadata.Metadata{
				Pipelines: []metadata.PipelineMetadata{
					{
						AggregationID: aggregation.DefaultID,
						StoragePolicies: []policy.StoragePolicy{
							policy.NewStoragePolicy(20*time.Second, xtime.Second, 6*time.Hour),
							policy.NewStoragePolicy(time.Minute, xtime.Minute, 2*24*time.Hour),
							policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 25*24*time.Hour),
						},
					},
				},
			},
		},
		{
			CutoverNanos: 200,
			Tombstoned:   true,
			Metadata: metadata.Metadata{
				Pipelines: []metadata.PipelineMetadata{
					{
						AggregationID: aggregation.DefaultID,
						StoragePolicies: []policy.StoragePolicy{
							policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
						},
					},
				},
			},
		},
	}
	testTimedMetadata = metadata.TimedMetadata{
		AggregationID: aggregation.DefaultID,
		StoragePolicy: policy.NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour),
	}
	testForwardMetadata = metadata.ForwardMetadata{
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
		SourceID:          1234,
		NumForwardedTimes: 3,
	}
	testPassthroughMetadata = policy.NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour)
	testPlacementInstances  = []placement.Instance{
		placement.NewInstance().
			SetID("instance1").
			SetEndpoint("instance1_endpoint").
			SetShards(shard.NewShards([]shard.Shard{
				shard.NewShard(0).
					SetState(shard.Initializing).
					SetCutoverNanos(testCutoverNanos).
					SetCutoffNanos(testCutoffNanos),
				shard.NewShard(1).
					SetState(shard.Initializing).
					SetCutoverNanos(testCutoverNanos).
					SetCutoffNanos(testCutoffNanos),
			})),
		placement.NewInstance().
			SetID("instance2").
			SetEndpoint("instance2_endpoint").
			SetShards(shard.NewShards([]shard.Shard{
				shard.NewShard(2).
					SetState(shard.Initializing).
					SetCutoverNanos(testCutoverNanos).
					SetCutoffNanos(testCutoffNanos),
				shard.NewShard(3).
					SetState(shard.Initializing).
					SetCutoverNanos(testCutoverNanos).
					SetCutoffNanos(testCutoffNanos),
			})),
		placement.NewInstance().
			SetID("instance3").
			SetEndpoint("instance3_endpoint").
			SetShards(shard.NewShards([]shard.Shard{
				shard.NewShard(0).
					SetState(shard.Initializing).
					SetCutoverNanos(testCutoverNanos).
					SetCutoffNanos(testCutoffNanos),
				shard.NewShard(1).
					SetState(shard.Initializing).
					SetCutoverNanos(testCutoverNanos).
					SetCutoffNanos(testCutoffNanos),
			})),
		placement.NewInstance().
			SetID("instance4").
			SetEndpoint("instance4_endpoint").
			SetShards(shard.NewShards([]shard.Shard{
				shard.NewShard(2).
					SetState(shard.Initializing).
					SetCutoverNanos(testCutoverNanos).
					SetCutoffNanos(testCutoffNanos),
				shard.NewShard(3).
					SetState(shard.Initializing).
					SetCutoverNanos(testCutoverNanos).
					SetCutoffNanos(testCutoffNanos),
			})),
	}
	testPlacement = placement.NewPlacement().
			SetVersion(1).
			SetCutoverNanos(12345).
			SetShards([]uint32{0, 1, 2, 3}).
			SetInstances(testPlacementInstances)
)

func TestTCPClientWriteUntimedMetricClosed(t *testing.T) {
	c := mustNewTestTCPClient(t, testOptions())
	require.NoError(t, c.Close())
	for _, input := range []unaggregated.MetricUnion{testCounter, testBatchTimer, testGauge} {
		var err error
		switch input.Type {
		case metric.CounterType:
			err = c.WriteUntimedCounter(input.Counter(), testStagedMetadatas)
		case metric.TimerType:
			err = c.WriteUntimedBatchTimer(input.BatchTimer(), testStagedMetadatas)
		case metric.GaugeType:
			err = c.WriteUntimedGauge(input.Gauge(), testStagedMetadatas)
		}
		require.Error(t, err)
	}
}

func TestTCPClientWriteUntimedMetricActiveStagedPlacementError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errActiveStagedPlacementError := errors.New("error active staged placement")
	watcher := placement.NewMockStagedPlacementWatcher(ctrl)
	watcher.EXPECT().ActiveStagedPlacement().
		Return(nil, nil, errActiveStagedPlacementError).
		MinTimes(1)
	c := mustNewTestTCPClient(t, testOptions())
	c.placementWatcher = watcher

	for _, input := range []unaggregated.MetricUnion{testCounter, testBatchTimer, testGauge} {
		var err error
		switch input.Type {
		case metric.CounterType:
			err = c.WriteUntimedCounter(input.Counter(), testStagedMetadatas)
		case metric.TimerType:
			err = c.WriteUntimedBatchTimer(input.BatchTimer(), testStagedMetadatas)
		case metric.GaugeType:
			err = c.WriteUntimedGauge(input.Gauge(), testStagedMetadatas)
		}
		require.Equal(t, errActiveStagedPlacementError, err)
	}
}

func TestTCPClientWriteUntimedMetricActivePlacementError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errActivePlacementError := errors.New("error active placement")
	stagedPlacement := placement.NewMockActiveStagedPlacement(ctrl)
	stagedPlacement.EXPECT().ActivePlacement().Return(nil, nil, errActivePlacementError).MinTimes(1)
	watcher := placement.NewMockStagedPlacementWatcher(ctrl)
	watcher.EXPECT().ActiveStagedPlacement().Return(stagedPlacement, func() {}, nil).MinTimes(1)
	c := mustNewTestTCPClient(t, testOptions())
	c.placementWatcher = watcher

	for _, input := range []unaggregated.MetricUnion{testCounter, testBatchTimer, testGauge} {
		var err error
		switch input.Type {
		case metric.CounterType:
			err = c.WriteUntimedCounter(input.Counter(), testStagedMetadatas)
		case metric.TimerType:
			err = c.WriteUntimedBatchTimer(input.BatchTimer(), testStagedMetadatas)
		case metric.GaugeType:
			err = c.WriteUntimedGauge(input.Gauge(), testStagedMetadatas)
		}
		require.Equal(t, errActivePlacementError, err)
	}
}

func TestTCPClientWriteUntimedMetricSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		instancesRes []placement.Instance
		shardRes     uint32
		payloadRes   payloadUnion
	)
	writerMgr := NewMockinstanceWriterManager(ctrl)
	writerMgr.EXPECT().
		Write(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			instance placement.Instance,
			shardID uint32,
			payload payloadUnion,
		) error {
			instancesRes = append(instancesRes, instance)
			shardRes = shardID
			payloadRes = payload
			return nil
		}).
		MinTimes(1)
	stagedPlacement := placement.NewMockActiveStagedPlacement(ctrl)
	stagedPlacement.EXPECT().ActivePlacement().Return(testPlacement, func() {}, nil).MinTimes(1)
	watcher := placement.NewMockStagedPlacementWatcher(ctrl)
	watcher.EXPECT().ActiveStagedPlacement().Return(stagedPlacement, func() {}, nil).MinTimes(1)
	c := mustNewTestTCPClient(t, testOptions())
	c.nowFn = func() time.Time { return time.Unix(0, testNowNanos) }
	c.writerMgr = writerMgr
	c.placementWatcher = watcher

	expectedInstances := []placement.Instance{
		testPlacementInstances[0],
		testPlacementInstances[2],
	}
	for _, input := range []unaggregated.MetricUnion{testCounter, testBatchTimer, testGauge} {
		// Reset states in each iteration.
		instancesRes = instancesRes[:0]
		shardRes = 0
		payloadRes = payloadUnion{}

		var err error
		switch input.Type {
		case metric.CounterType:
			err = c.WriteUntimedCounter(input.Counter(), testStagedMetadatas)
		case metric.TimerType:
			err = c.WriteUntimedBatchTimer(input.BatchTimer(), testStagedMetadatas)
		case metric.GaugeType:
			err = c.WriteUntimedGauge(input.Gauge(), testStagedMetadatas)
		}

		require.NoError(t, err)
		require.Equal(t, expectedInstances, instancesRes)
		require.Equal(t, uint32(1), shardRes)
		require.Equal(t, untimedType, payloadRes.payloadType)
		require.Equal(t, input, payloadRes.untimed.metric)
		require.Equal(t, testStagedMetadatas, payloadRes.untimed.metadatas)
	}
}

func TestTCPClientWriteUntimedMetricPartialError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		instancesRes     []placement.Instance
		shardRes         uint32
		payloadRes       payloadUnion
		errInstanceWrite = errors.New("instance write error")
	)
	writerMgr := NewMockinstanceWriterManager(ctrl)
	writerMgr.EXPECT().
		Write(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			instance placement.Instance,
			shardID uint32,
			payload payloadUnion,
		) error {
			if instance.ID() == testPlacementInstances[0].ID() {
				return errInstanceWrite
			}
			instancesRes = append(instancesRes, instance)
			shardRes = shardID
			payloadRes = payload
			return nil
		}).
		MinTimes(1)
	stagedPlacement := placement.NewMockActiveStagedPlacement(ctrl)
	stagedPlacement.EXPECT().ActivePlacement().Return(testPlacement, func() {}, nil).MinTimes(1)
	watcher := placement.NewMockStagedPlacementWatcher(ctrl)
	watcher.EXPECT().ActiveStagedPlacement().Return(stagedPlacement, func() {}, nil).MinTimes(1)
	c := mustNewTestTCPClient(t, testOptions())
	c.nowFn = func() time.Time { return time.Unix(0, testNowNanos) }
	c.writerMgr = writerMgr
	c.placementWatcher = watcher

	expectedInstances := []placement.Instance{
		testPlacementInstances[2],
	}
	err := c.WriteUntimedCounter(testCounter.Counter(), testStagedMetadatas)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), errInstanceWrite.Error()))
	require.Equal(t, expectedInstances, instancesRes)
	require.Equal(t, uint32(1), shardRes)
	require.Equal(t, untimedType, payloadRes.payloadType)
	require.Equal(t, testCounter, payloadRes.untimed.metric)
	require.Equal(t, testStagedMetadatas, payloadRes.untimed.metadatas)
}

func TestTCPClientWriteUntimedMetricBeforeShardCutover(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var instancesRes []placement.Instance
	stagedPlacement := placement.NewMockActiveStagedPlacement(ctrl)
	stagedPlacement.EXPECT().ActivePlacement().Return(testPlacement, func() {}, nil).MinTimes(1)
	watcher := placement.NewMockStagedPlacementWatcher(ctrl)
	watcher.EXPECT().ActiveStagedPlacement().Return(stagedPlacement, func() {}, nil).MinTimes(1)
	c := mustNewTestTCPClient(t, testOptions())
	c.shardCutoverWarmupDuration = time.Second
	c.nowFn = func() time.Time { return time.Unix(0, testCutoverNanos-1).Add(-time.Second) }
	c.writerMgr = nil
	c.placementWatcher = watcher

	err := c.WriteUntimedCounter(testCounter.Counter(), testStagedMetadatas)
	require.NoError(t, err)
	require.Nil(t, instancesRes)
}

func TestTCPClientWriteUntimedMetricAfterShardCutoff(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var instancesRes []placement.Instance
	stagedPlacement := placement.NewMockActiveStagedPlacement(ctrl)
	stagedPlacement.EXPECT().ActivePlacement().Return(testPlacement, func() {}, nil).MinTimes(1)
	watcher := placement.NewMockStagedPlacementWatcher(ctrl)
	watcher.EXPECT().ActiveStagedPlacement().Return(stagedPlacement, func() {}, nil).MinTimes(1)
	c := mustNewTestTCPClient(t, testOptions())
	c.shardCutoffLingerDuration = time.Second
	c.nowFn = func() time.Time { return time.Unix(0, testCutoffNanos+1).Add(time.Second) }
	c.writerMgr = nil
	c.placementWatcher = watcher

	err := c.WriteUntimedCounter(testCounter.Counter(), testStagedMetadatas)
	require.NoError(t, err)
	require.Nil(t, instancesRes)
}

func TestTCPClientWriteTimedMetricSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		instancesRes []placement.Instance
		shardRes     uint32
		payloadRes   payloadUnion
	)
	writerMgr := NewMockinstanceWriterManager(ctrl)
	writerMgr.EXPECT().
		Write(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			instance placement.Instance,
			shardID uint32,
			payload payloadUnion,
		) error {
			instancesRes = append(instancesRes, instance)
			shardRes = shardID
			payloadRes = payload
			return nil
		}).
		MinTimes(1)
	stagedPlacement := placement.NewMockActiveStagedPlacement(ctrl)
	stagedPlacement.EXPECT().ActivePlacement().Return(testPlacement, func() {}, nil).MinTimes(1)
	watcher := placement.NewMockStagedPlacementWatcher(ctrl)
	watcher.EXPECT().ActiveStagedPlacement().Return(stagedPlacement, func() {}, nil).MinTimes(1)
	c := mustNewTestTCPClient(t, testOptions())
	c.nowFn = func() time.Time { return time.Unix(0, testNowNanos) }
	c.writerMgr = writerMgr
	c.placementWatcher = watcher

	expectedInstances := []placement.Instance{
		testPlacementInstances[0],
		testPlacementInstances[2],
	}
	testMetric := testTimed
	testMetric.TimeNanos = testNowNanos
	err := c.WriteTimed(testMetric, testTimedMetadata)
	require.NoError(t, err)
	require.Equal(t, expectedInstances, instancesRes)
	require.Equal(t, uint32(1), shardRes)
	require.Equal(t, timedType, payloadRes.payloadType)
	require.Equal(t, testMetric, payloadRes.timed.metric)
	require.Equal(t, testTimedMetadata, payloadRes.timed.metadata)
}

func TestTCPClientWriteTimedMetricPartialError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		instancesRes     []placement.Instance
		shardRes         uint32
		payloadRes       payloadUnion
		errInstanceWrite = errors.New("instance write error")
	)
	writerMgr := NewMockinstanceWriterManager(ctrl)
	writerMgr.EXPECT().
		Write(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			instance placement.Instance,
			shardID uint32,
			payload payloadUnion,
		) error {
			if instance.ID() == testPlacementInstances[0].ID() {
				return errInstanceWrite
			}
			instancesRes = append(instancesRes, instance)
			shardRes = shardID
			payloadRes = payload
			return nil
		}).
		MinTimes(1)
	stagedPlacement := placement.NewMockActiveStagedPlacement(ctrl)
	stagedPlacement.EXPECT().ActivePlacement().Return(testPlacement, func() {}, nil).MinTimes(1)
	watcher := placement.NewMockStagedPlacementWatcher(ctrl)
	watcher.EXPECT().ActiveStagedPlacement().Return(stagedPlacement, func() {}, nil).MinTimes(1)
	c := mustNewTestTCPClient(t, testOptions())
	c.nowFn = func() time.Time { return time.Unix(0, testNowNanos) }
	c.writerMgr = writerMgr
	c.placementWatcher = watcher

	expectedInstances := []placement.Instance{
		testPlacementInstances[2],
	}
	testMetric := testTimed
	testMetric.TimeNanos = testNowNanos
	err := c.WriteTimed(testMetric, testTimedMetadata)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), errInstanceWrite.Error()))
	require.Equal(t, expectedInstances, instancesRes)
	require.Equal(t, uint32(1), shardRes)
	require.Equal(t, timedType, payloadRes.payloadType)
	require.Equal(t, testMetric, payloadRes.timed.metric)
	require.Equal(t, testTimedMetadata, payloadRes.timed.metadata)
}

func TestTCPClientWriteForwardedMetricSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		instancesRes []placement.Instance
		shardRes     uint32
		payloadRes   payloadUnion
	)
	writerMgr := NewMockinstanceWriterManager(ctrl)
	writerMgr.EXPECT().
		Write(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			instance placement.Instance,
			shardID uint32,
			payload payloadUnion,
		) error {
			instancesRes = append(instancesRes, instance)
			shardRes = shardID
			payloadRes = payload
			return nil
		}).
		MinTimes(1)
	stagedPlacement := placement.NewMockActiveStagedPlacement(ctrl)
	stagedPlacement.EXPECT().ActivePlacement().Return(testPlacement, func() {}, nil).MinTimes(1)
	watcher := placement.NewMockStagedPlacementWatcher(ctrl)
	watcher.EXPECT().ActiveStagedPlacement().Return(stagedPlacement, func() {}, nil).MinTimes(1)
	c := mustNewTestTCPClient(t, testOptions())
	c.nowFn = func() time.Time { return time.Unix(0, testNowNanos) }
	c.writerMgr = writerMgr
	c.placementWatcher = watcher

	expectedInstances := []placement.Instance{
		testPlacementInstances[0],
		testPlacementInstances[2],
	}
	testMetric := testForwarded
	testMetric.TimeNanos = testNowNanos
	err := c.WriteForwarded(testMetric, testForwardMetadata)
	require.NoError(t, err)
	require.Equal(t, expectedInstances, instancesRes)
	require.Equal(t, uint32(1), shardRes)
	require.Equal(t, forwardedType, payloadRes.payloadType)
	require.Equal(t, testMetric, payloadRes.forwarded.metric)
	require.Equal(t, testForwardMetadata, payloadRes.forwarded.metadata)
}

func TestTCPClientWriteForwardedMetricPartialError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		instancesRes     []placement.Instance
		shardRes         uint32
		payloadRes       payloadUnion
		errInstanceWrite = errors.New("instance write error")
	)
	writerMgr := NewMockinstanceWriterManager(ctrl)
	writerMgr.EXPECT().
		Write(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			instance placement.Instance,
			shardID uint32,
			payload payloadUnion,
		) error {
			if instance.ID() == testPlacementInstances[0].ID() {
				return errInstanceWrite
			}
			instancesRes = append(instancesRes, instance)
			shardRes = shardID
			payloadRes = payload
			return nil
		}).
		MinTimes(1)
	stagedPlacement := placement.NewMockActiveStagedPlacement(ctrl)
	stagedPlacement.EXPECT().ActivePlacement().Return(testPlacement, func() {}, nil).MinTimes(1)
	watcher := placement.NewMockStagedPlacementWatcher(ctrl)
	watcher.EXPECT().ActiveStagedPlacement().Return(stagedPlacement, func() {}, nil).MinTimes(1)
	c := mustNewTestTCPClient(t, testOptions())
	c.nowFn = func() time.Time { return time.Unix(0, testNowNanos) }
	c.writerMgr = writerMgr
	c.placementWatcher = watcher

	expectedInstances := []placement.Instance{
		testPlacementInstances[2],
	}
	testMetric := testForwarded
	testMetric.TimeNanos = testNowNanos
	err := c.WriteForwarded(testMetric, testForwardMetadata)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), errInstanceWrite.Error()))
	require.Equal(t, expectedInstances, instancesRes)
	require.Equal(t, uint32(1), shardRes)
	require.Equal(t, forwardedType, payloadRes.payloadType)
	require.Equal(t, testMetric, payloadRes.forwarded.metric)
	require.Equal(t, testForwardMetadata, payloadRes.forwarded.metadata)
}

func TestTCPClientWritePassthroughMetricSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		instancesRes []placement.Instance
		shardRes     uint32
		payloadRes   payloadUnion
	)
	writerMgr := NewMockinstanceWriterManager(ctrl)
	writerMgr.EXPECT().
		Write(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			instance placement.Instance,
			shardID uint32,
			payload payloadUnion,
		) error {
			instancesRes = append(instancesRes, instance)
			shardRes = shardID
			payloadRes = payload
			return nil
		}).
		MinTimes(1)
	stagedPlacement := placement.NewMockActiveStagedPlacement(ctrl)
	stagedPlacement.EXPECT().ActivePlacement().Return(testPlacement, func() {}, nil).MinTimes(1)
	watcher := placement.NewMockStagedPlacementWatcher(ctrl)
	watcher.EXPECT().ActiveStagedPlacement().Return(stagedPlacement, func() {}, nil).MinTimes(1)
	c := mustNewTestTCPClient(t, testOptions())
	c.nowFn = func() time.Time { return time.Unix(0, testNowNanos) }
	c.writerMgr = writerMgr
	c.placementWatcher = watcher

	expectedInstances := []placement.Instance{
		testPlacementInstances[0],
		testPlacementInstances[2],
	}
	testMetric := testPassthrough
	testMetric.TimeNanos = testNowNanos
	err := c.WritePassthrough(testMetric, testPassthroughMetadata)
	require.NoError(t, err)
	require.Equal(t, expectedInstances, instancesRes)
	require.Equal(t, uint32(1), shardRes)
	require.Equal(t, passthroughType, payloadRes.payloadType)
	require.Equal(t, testMetric, payloadRes.passthrough.metric)
	require.Equal(t, testPassthroughMetadata, payloadRes.passthrough.storagePolicy)
}

func TestTCPClientWritePassthroughMetricPartialError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		instancesRes     []placement.Instance
		shardRes         uint32
		payloadRes       payloadUnion
		errInstanceWrite = errors.New("instance write error")
	)
	writerMgr := NewMockinstanceWriterManager(ctrl)
	writerMgr.EXPECT().
		Write(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			instance placement.Instance,
			shardID uint32,
			payload payloadUnion,
		) error {
			if instance.ID() == testPlacementInstances[0].ID() {
				return errInstanceWrite
			}
			instancesRes = append(instancesRes, instance)
			shardRes = shardID
			payloadRes = payload
			return nil
		}).
		MinTimes(1)
	stagedPlacement := placement.NewMockActiveStagedPlacement(ctrl)
	stagedPlacement.EXPECT().ActivePlacement().Return(testPlacement, func() {}, nil).MinTimes(1)
	watcher := placement.NewMockStagedPlacementWatcher(ctrl)
	watcher.EXPECT().ActiveStagedPlacement().Return(stagedPlacement, func() {}, nil).MinTimes(1)
	c := mustNewTestTCPClient(t, testOptions())
	c.nowFn = func() time.Time { return time.Unix(0, testNowNanos) }
	c.writerMgr = writerMgr
	c.placementWatcher = watcher

	expectedInstances := []placement.Instance{
		testPlacementInstances[2],
	}
	testMetric := testPassthrough
	testMetric.TimeNanos = testNowNanos
	err := c.WritePassthrough(testMetric, testPassthroughMetadata)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), errInstanceWrite.Error()))
	require.Equal(t, expectedInstances, instancesRes)
	require.Equal(t, uint32(1), shardRes)
	require.Equal(t, passthroughType, payloadRes.payloadType)
	require.Equal(t, testMetric, payloadRes.passthrough.metric)
	require.Equal(t, testPassthroughMetadata, payloadRes.passthrough.storagePolicy)
}

func TestTCPClientFlushClosed(t *testing.T) {
	c := mustNewTestTCPClient(t, testOptions())
	require.NoError(t, c.Close())
	require.Equal(t, errInstanceWriterManagerClosed, c.Flush())
}

func TestTCPClientFlushError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errTestFlush := errors.New("test flush error")
	writerMgr := NewMockinstanceWriterManager(ctrl)
	writerMgr.EXPECT().Flush().Return(errTestFlush).MinTimes(1)
	c := mustNewTestTCPClient(t, testOptions())
	c.writerMgr = writerMgr
	require.Equal(t, errTestFlush, c.Flush())
}

func TestTCPClientFlushSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	writerMgr := NewMockinstanceWriterManager(ctrl)
	writerMgr.EXPECT().Flush().Return(nil).MinTimes(1)
	c := mustNewTestTCPClient(t, testOptions())
	c.writerMgr = writerMgr
	require.NoError(t, c.Flush())
}

func TestTCPClientClosed(t *testing.T) {
	c := mustNewTestTCPClient(t, testOptions())

	require.NoError(t, c.Close())
	require.Equal(t, errInstanceWriterManagerClosed, c.Close())
}

func TestTCPClientCloseSuccess(t *testing.T) {
	c := mustNewTestTCPClient(t, testOptions())
	require.NoError(t, c.Close())
}

func TestTCPClientWriteTimeRangeFor(t *testing.T) {
	c := mustNewTestTCPClient(t, testOptions())
	testShard := shard.NewShard(0).SetState(shard.Initializing)
	for _, input := range []struct {
		cutoverNanos     int64
		cutoffNanos      int64
		expectedEarliest int64
		expectedLatest   int64
	}{
		{
			cutoverNanos:     0,
			cutoffNanos:      int64(math.MaxInt64),
			expectedEarliest: 0,
			expectedLatest:   int64(math.MaxInt64),
		},
		{
			cutoverNanos:     testNowNanos,
			cutoffNanos:      int64(math.MaxInt64),
			expectedEarliest: testNowNanos - int64(time.Minute),
			expectedLatest:   int64(math.MaxInt64),
		},
		{
			cutoverNanos:     0,
			cutoffNanos:      testNowNanos,
			expectedEarliest: 0,
			expectedLatest:   testNowNanos + int64(10*time.Minute),
		},
	} {
		testShard = testShard.SetCutoverNanos(input.cutoverNanos).SetCutoffNanos(input.cutoffNanos)
		earliest, latest := c.writeTimeRangeFor(testShard)
		require.Equal(t, input.expectedEarliest, earliest)
		require.Equal(t, input.expectedLatest, latest)
	}
}

func TestTCPClientActivePlacement(t *testing.T) {
	var (
		c               = mustNewTestTCPClient(t, testOptions())
		emptyPl         = placement.NewPlacement()
		ctrl            = gomock.NewController(t)
		mockPl          = placement.NewMockPlacement(ctrl)
		stagedPlacement = placement.NewMockActiveStagedPlacement(ctrl)
		watcher         = placement.NewMockStagedPlacementWatcher(ctrl)
		doneCalls       int
	)

	c.placementWatcher = watcher
	watcher.EXPECT().ActiveStagedPlacement().Return(stagedPlacement, func() { doneCalls++ }, nil)
	stagedPlacement.EXPECT().Version().Return(42)
	stagedPlacement.EXPECT().ActivePlacement().Return(mockPl, func() { doneCalls++ }, nil)
	mockPl.EXPECT().Clone().Return(emptyPl)

	pl, v, err := c.ActivePlacement()
	assert.NoError(t, err)
	assert.Equal(t, 42, v)
	assert.Equal(t, 2, doneCalls)
	assert.Equal(t, emptyPl, pl)
}

func TestTCPClientInitAndClose(t *testing.T) {
	c := mustNewTestTCPClient(t, testOptions())
	require.NoError(t, c.Init())
	require.NoError(t, c.Close())
}

func mustNewTestTCPClient(t *testing.T, opts Options) *TCPClient {
	c, err := NewClient(opts)
	require.NoError(t, err)
	value, ok := c.(*TCPClient)
	require.True(t, ok)
	return value
}

// TODO: clean this up as it's in use by other test files
func testOptions() Options {
	return testTCPClientOptions()
}

func testTCPClientOptions() Options {
	const placementKey = "placement"
	pl, err := placement.NewPlacement().Proto()
	if err != nil {
		panic(err.Error())
	}

	store := mem.NewStore()
	if _, err := store.Set(placementKey, pl); err != nil {
		panic(err.Error())
	}

	plOpts := placement.NewStagedPlacementWatcherOptions().
		SetStagedPlacementStore(store).
		SetStagedPlacementKey(placementKey).
		SetInitWatchTimeout(time.Millisecond)
	return NewOptions().
		SetClockOptions(clock.NewOptions()).
		SetConnectionOptions(testConnectionOptions()).
		SetInstrumentOptions(instrument.NewOptions()).
		SetShardFn(func([]byte, uint32) uint32 { return 1 }).
		SetInstanceQueueSize(10).
		SetMaxTimerBatchSize(140).
		SetShardCutoverWarmupDuration(time.Minute).
		SetShardCutoffLingerDuration(10 * time.Minute).
		SetAggregatorClientType(TCPAggregatorClient).
		SetStagedPlacementWatcherOptions(plOpts)
}
