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

package client

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/metadata"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

var (
	testNowNanos     = time.Now().UnixNano()
	testCutoverNanos = testNowNanos - int64(time.Minute)
	testCutoffNanos  = testNowNanos + int64(time.Hour)
	testCounter      = unaggregated.MetricUnion{
		Type:       unaggregated.CounterType,
		ID:         []byte("foo"),
		CounterVal: 1234,
	}
	testBatchTimer = unaggregated.MetricUnion{
		Type:          unaggregated.BatchTimerType,
		ID:            []byte("foo"),
		BatchTimerVal: []float64{222.22, 345.67, 901.23345},
	}
	testGauge = unaggregated.MetricUnion{
		Type:     unaggregated.GaugeType,
		ID:       []byte("foo"),
		GaugeVal: 123.456,
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
	testPlacementInstances = []placement.Instance{
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

func TestClientInitUninitializedOrClosed(t *testing.T) {
	c := NewClient(testOptions()).(*client)

	c.state = clientInitialized
	require.Equal(t, errClientIsInitializedOrClosed, c.Init())

	c.state = clientClosed
	require.Equal(t, errClientIsInitializedOrClosed, c.Init())
}

func TestClientInitWatcherWatchError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errTestWatcherWatch := errors.New("error watching")
	watcher := placement.NewMockStagedPlacementWatcher(ctrl)
	watcher.EXPECT().Watch().Return(errTestWatcherWatch)
	c := NewClient(testOptions()).(*client)
	c.placementWatcher = watcher
	require.Equal(t, errTestWatcherWatch, c.Init())
}

func TestClientInitSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	watcher := placement.NewMockStagedPlacementWatcher(ctrl)
	watcher.EXPECT().Watch().Return(nil)
	c := NewClient(testOptions()).(*client)
	c.placementWatcher = watcher
	require.NoError(t, c.Init())
	require.Equal(t, clientInitialized, c.state)
}

func TestClientWriteUntimedMetricClosed(t *testing.T) {
	c := NewClient(testOptions()).(*client)
	c.state = clientUninitialized
	for _, input := range []unaggregated.MetricUnion{testCounter, testBatchTimer, testGauge} {
		var err error
		switch input.Type {
		case unaggregated.CounterType:
			err = c.WriteUntimedCounter(input.Counter(), testStagedMetadatas)
		case unaggregated.BatchTimerType:
			err = c.WriteUntimedBatchTimer(input.BatchTimer(), testStagedMetadatas)
		case unaggregated.GaugeType:
			err = c.WriteUntimedGauge(input.Gauge(), testStagedMetadatas)
		}
		require.Equal(t, errClientIsUninitializedOrClosed, err)
	}
}

func TestClientWriteUntimedMetricActiveStagedPlacementError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errActiveStagedPlacementError := errors.New("error active staged placement")
	watcher := placement.NewMockStagedPlacementWatcher(ctrl)
	watcher.EXPECT().ActiveStagedPlacement().Return(nil, nil, errActiveStagedPlacementError).MinTimes(1)
	c := NewClient(testOptions()).(*client)
	c.state = clientInitialized
	c.placementWatcher = watcher

	for _, input := range []unaggregated.MetricUnion{testCounter, testBatchTimer, testGauge} {
		var err error
		switch input.Type {
		case unaggregated.CounterType:
			err = c.WriteUntimedCounter(input.Counter(), testStagedMetadatas)
		case unaggregated.BatchTimerType:
			err = c.WriteUntimedBatchTimer(input.BatchTimer(), testStagedMetadatas)
		case unaggregated.GaugeType:
			err = c.WriteUntimedGauge(input.Gauge(), testStagedMetadatas)
		}
		require.Equal(t, errActiveStagedPlacementError, err)
	}
}

func TestClientWriteUntimedMetricActivePlacementError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errActivePlacementError := errors.New("error active placement")
	stagedPlacement := placement.NewMockActiveStagedPlacement(ctrl)
	stagedPlacement.EXPECT().ActivePlacement().Return(nil, nil, errActivePlacementError).MinTimes(1)
	watcher := placement.NewMockStagedPlacementWatcher(ctrl)
	watcher.EXPECT().ActiveStagedPlacement().Return(stagedPlacement, func() {}, nil).MinTimes(1)
	c := NewClient(testOptions()).(*client)
	c.state = clientInitialized
	c.placementWatcher = watcher

	for _, input := range []unaggregated.MetricUnion{testCounter, testBatchTimer, testGauge} {
		var err error
		switch input.Type {
		case unaggregated.CounterType:
			err = c.WriteUntimedCounter(input.Counter(), testStagedMetadatas)
		case unaggregated.BatchTimerType:
			err = c.WriteUntimedBatchTimer(input.BatchTimer(), testStagedMetadatas)
		case unaggregated.GaugeType:
			err = c.WriteUntimedGauge(input.Gauge(), testStagedMetadatas)
		}
		require.Equal(t, errActivePlacementError, err)
	}
}

func TestClientWriteUntimedMetricSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		instancesRes []placement.Instance
		shardRes     uint32
		muRes        unaggregated.MetricUnion
		smRes        metadata.StagedMetadatas
	)
	writerMgr := NewMockinstanceWriterManager(ctrl)
	writerMgr.EXPECT().
		WriteUntimed(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			instance placement.Instance,
			shardID uint32,
			metric unaggregated.MetricUnion,
			metadatas metadata.StagedMetadatas,
		) error {
			instancesRes = append(instancesRes, instance)
			shardRes = shardID
			muRes = metric
			smRes = metadatas
			return nil
		}).
		MinTimes(1)
	stagedPlacement := placement.NewMockActiveStagedPlacement(ctrl)
	stagedPlacement.EXPECT().ActivePlacement().Return(testPlacement, func() {}, nil).MinTimes(1)
	watcher := placement.NewMockStagedPlacementWatcher(ctrl)
	watcher.EXPECT().ActiveStagedPlacement().Return(stagedPlacement, func() {}, nil).MinTimes(1)
	c := NewClient(testOptions()).(*client)
	c.state = clientInitialized
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
		muRes = unaggregated.MetricUnion{}
		smRes = smRes[:0]

		var err error
		switch input.Type {
		case unaggregated.CounterType:
			err = c.WriteUntimedCounter(input.Counter(), testStagedMetadatas)
		case unaggregated.BatchTimerType:
			err = c.WriteUntimedBatchTimer(input.BatchTimer(), testStagedMetadatas)
		case unaggregated.GaugeType:
			err = c.WriteUntimedGauge(input.Gauge(), testStagedMetadatas)
		}

		require.NoError(t, err)
		require.Equal(t, expectedInstances, instancesRes)
		require.Equal(t, uint32(1), shardRes)
		require.Equal(t, input, muRes)
		require.Equal(t, testStagedMetadatas, smRes)
	}
}

func TestClientWriteUntimedMetricPartialError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		instancesRes     []placement.Instance
		shardRes         uint32
		muRes            unaggregated.MetricUnion
		smRes            metadata.StagedMetadatas
		errInstanceWrite = errors.New("instance write error")
	)
	writerMgr := NewMockinstanceWriterManager(ctrl)
	writerMgr.EXPECT().
		WriteUntimed(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			instance placement.Instance,
			shardID uint32,
			metric unaggregated.MetricUnion,
			metadatas metadata.StagedMetadatas,
		) error {
			if instance.ID() == testPlacementInstances[0].ID() {
				return errInstanceWrite
			}
			instancesRes = append(instancesRes, instance)
			shardRes = shardID
			muRes = metric
			smRes = metadatas
			return nil
		}).
		MinTimes(1)
	stagedPlacement := placement.NewMockActiveStagedPlacement(ctrl)
	stagedPlacement.EXPECT().ActivePlacement().Return(testPlacement, func() {}, nil).MinTimes(1)
	watcher := placement.NewMockStagedPlacementWatcher(ctrl)
	watcher.EXPECT().ActiveStagedPlacement().Return(stagedPlacement, func() {}, nil).MinTimes(1)
	c := NewClient(testOptions()).(*client)
	c.state = clientInitialized
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
	require.Equal(t, testCounter, muRes)
	require.Equal(t, testStagedMetadatas, smRes)
}

func TestClientWriteUntimedMetricBeforeShardCutover(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var instancesRes []placement.Instance
	stagedPlacement := placement.NewMockActiveStagedPlacement(ctrl)
	stagedPlacement.EXPECT().ActivePlacement().Return(testPlacement, func() {}, nil).MinTimes(1)
	watcher := placement.NewMockStagedPlacementWatcher(ctrl)
	watcher.EXPECT().ActiveStagedPlacement().Return(stagedPlacement, func() {}, nil).MinTimes(1)
	c := NewClient(testOptions()).(*client)
	c.shardCutoverWarmupDuration = time.Second
	c.state = clientInitialized
	c.nowFn = func() time.Time { return time.Unix(0, testCutoverNanos-1).Add(-time.Second) }
	c.writerMgr = nil
	c.placementWatcher = watcher

	err := c.WriteUntimedCounter(testCounter.Counter(), testStagedMetadatas)
	require.NoError(t, err)
	require.Nil(t, instancesRes)
}

func TestClientWriteUntimedMetricAfterShardCutoff(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var instancesRes []placement.Instance
	stagedPlacement := placement.NewMockActiveStagedPlacement(ctrl)
	stagedPlacement.EXPECT().ActivePlacement().Return(testPlacement, func() {}, nil).MinTimes(1)
	watcher := placement.NewMockStagedPlacementWatcher(ctrl)
	watcher.EXPECT().ActiveStagedPlacement().Return(stagedPlacement, func() {}, nil).MinTimes(1)
	c := NewClient(testOptions()).(*client)
	c.shardCutoffLingerDuration = time.Second
	c.state = clientInitialized
	c.nowFn = func() time.Time { return time.Unix(0, testCutoffNanos+1).Add(time.Second) }
	c.writerMgr = nil
	c.placementWatcher = watcher

	err := c.WriteUntimedCounter(testCounter.Counter(), testStagedMetadatas)
	require.NoError(t, err)
	require.Nil(t, instancesRes)
}

func TestClientFlushClosed(t *testing.T) {
	c := NewClient(testOptions()).(*client)
	c.state = clientClosed
	require.Equal(t, errClientIsUninitializedOrClosed, c.Flush())
}

func TestClientFlushError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errTestFlush := errors.New("test flush error")
	writerMgr := NewMockinstanceWriterManager(ctrl)
	writerMgr.EXPECT().Flush().Return(errTestFlush).MinTimes(1)
	opts := testOptions()
	c := NewClient(opts).(*client)
	c.state = clientInitialized
	c.writerMgr = writerMgr
	require.Equal(t, errTestFlush, c.Flush())
}

func TestClientFlushSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	writerMgr := NewMockinstanceWriterManager(ctrl)
	writerMgr.EXPECT().Flush().Return(nil).MinTimes(1)
	opts := testOptions()
	c := NewClient(opts).(*client)
	c.state = clientInitialized
	c.writerMgr = writerMgr
	require.NoError(t, c.Flush())
}

func TestClientCloseUninitializedOrClosed(t *testing.T) {
	c := NewClient(testOptions()).(*client)

	c.state = clientUninitialized
	require.Equal(t, errClientIsUninitializedOrClosed, c.Close())

	c.state = clientClosed
	require.Equal(t, errClientIsUninitializedOrClosed, c.Close())
}

func TestClientCloseSuccess(t *testing.T) {
	c := NewClient(testOptions()).(*client)
	c.state = clientInitialized
	require.NoError(t, c.Close())
}

func TestClientWriteTimeRangeFor(t *testing.T) {
	c := NewClient(testOptions()).(*client)
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

func testOptions() Options {
	return NewOptions().
		SetClockOptions(clock.NewOptions()).
		SetConnectionOptions(testConnectionOptions()).
		SetInstrumentOptions(instrument.NewOptions()).
		SetShardFn(func(id []byte, numShards int) uint32 { return 1 }).
		SetInstanceQueueSize(10).
		SetMaxTimerBatchSize(140).
		SetShardCutoverWarmupDuration(time.Minute).
		SetShardCutoffLingerDuration(10 * time.Minute)
}
