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
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3/src/aggregator/aggregator/handler"
	"github.com/m3db/m3/src/aggregator/aggregator/handler/writer"
	"github.com/m3db/m3/src/aggregator/client"
	schema "github.com/m3db/m3/src/aggregator/generated/proto/flush"
	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	"github.com/m3db/m3/src/cluster/kv"
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
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

const (
	testShardSetID       = uint32(1)
	testInstanceID       = "localhost:0"
	testPlacementKey     = "placement"
	testNumShards        = 4
	testPlacementCutover = 1234
)

var (
	testUntimedMetric = unaggregated.MetricUnion{
		Type:       metric.CounterType,
		ID:         []byte("foo"),
		CounterVal: 1234,
	}
	testTimedMetric = aggregated.Metric{
		Type:      metric.CounterType,
		ID:        []byte("testTimed"),
		TimeNanos: 12345,
		Value:     1000,
	}
	testForwardedMetric = aggregated.ForwardedMetric{
		Type:      metric.CounterType,
		ID:        []byte("testForwarded"),
		TimeNanos: 12345,
		Values:    []float64{76109, 23891},
	}
	testInvalidMetric = unaggregated.MetricUnion{
		Type: metric.UnknownType,
		ID:   []byte("testInvalid"),
	}
	testStagedMetadatas = metadata.StagedMetadatas{
		{
			CutoverNanos: 123,
			Tombstoned:   false,
			Metadata: metadata.Metadata{
				Pipelines: []metadata.PipelineMetadata{
					{
						AggregationID: aggregation.DefaultID,
						StoragePolicies: []policy.StoragePolicy{
							policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
							policy.NewStoragePolicy(time.Minute, xtime.Minute, 2*24*time.Hour),
						},
					},
				},
			},
		},
		{
			CutoverNanos: 456,
			Tombstoned:   false,
			Metadata: metadata.Metadata{
				Pipelines: []metadata.PipelineMetadata{
					{},
				},
			},
		},
	}
	testTimedMetadata = metadata.TimedMetadata{
		AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
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
)

func TestAggregatorOpenAlreadyOpen(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, _ := testAggregator(t, ctrl)
	agg.state = aggregatorOpen
	require.Equal(t, errAggregatorAlreadyOpenOrClosed, agg.Open())
}

func TestAggregatorOpenPlacementManagerOpenError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errPlacementManagerOpen := errors.New("error opening placement manager")
	placementManager := NewMockPlacementManager(ctrl)
	placementManager.EXPECT().Open().Return(errPlacementManagerOpen)

	agg, _ := testAggregator(t, ctrl)
	agg.placementManager = placementManager
	require.Equal(t, errPlacementManagerOpen, agg.Open())
}

func TestAggregatorOpenPlacementError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errPlacement := errors.New("error getting placement")
	placementManager := NewMockPlacementManager(ctrl)
	placementManager.EXPECT().Open().Return(nil)
	placementManager.EXPECT().Placement().Return(nil, nil, errPlacement)

	agg, _ := testAggregator(t, ctrl)
	agg.placementManager = placementManager
	require.Equal(t, errPlacement, agg.Open())
}

func TestAggregatorOpenInstanceFromError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testPlacement := placement.NewPlacement().SetCutoverNanos(5678)
	testStagedPlacement := placement.NewMockActiveStagedPlacement(ctrl)
	errInstanceFrom := errors.New("error getting instance from placement")
	placementManager := NewMockPlacementManager(ctrl)
	placementManager.EXPECT().Open().Return(nil)
	placementManager.EXPECT().Placement().Return(testStagedPlacement, testPlacement, nil)
	placementManager.EXPECT().InstanceFrom(testPlacement).Return(nil, errInstanceFrom)

	agg, _ := testAggregator(t, ctrl)
	agg.placementManager = placementManager
	require.Equal(t, errInstanceFrom, agg.Open())
}

func TestAggregatorOpenInstanceNotInPlacement(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	placementManager := NewMockPlacementManager(ctrl)

	agg, _ := testAggregator(t, ctrl)
	agg.placementManager = placementManager

	testPlacement := placement.NewPlacement().SetCutoverNanos(5678)
	testStagedPlacement := placement.NewMockActiveStagedPlacement(ctrl)

	placementManager.EXPECT().Open().Return(nil)
	placementManager.EXPECT().InstanceID().Return(agg.opts.PlacementManager().InstanceID())
	placementManager.EXPECT().Placement().Return(testStagedPlacement, testPlacement, nil)
	placementManager.EXPECT().InstanceFrom(testPlacement).Return(nil, ErrInstanceNotFoundInPlacement)

	require.NoError(t, agg.Open())
	require.Equal(t, uint32(0), agg.shardSetID)
	require.False(t, agg.shardSetOpen)
	require.Equal(t, 0, len(agg.shardIDs))
	require.Nil(t, agg.shards)
	require.Equal(t, agg.currStagedPlacement, testStagedPlacement)
	require.Equal(t, agg.currPlacement, testPlacement)
	require.Equal(t, aggregatorOpen, agg.state)
}

func TestAggregatorOpenSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, _ := testAggregator(t, ctrl)
	require.NoError(t, agg.Open())
	require.Equal(t, testShardSetID, agg.shardSetID)
	require.True(t, agg.shardSetOpen)
	require.Equal(t, aggregatorOpen, agg.state)
	require.Equal(t, []uint32{0, 1, 2, 3}, agg.shardIDs)
	for i := 0; i < testNumShards; i++ {
		require.NotNil(t, agg.shards[i])
	}
	require.NotNil(t, agg.currStagedPlacement)
	require.NotNil(t, agg.currPlacement)
	require.Equal(t, int64(testPlacementCutover), agg.currPlacement.CutoverNanos())
}

func TestAggregatorInstanceNotFoundThenFoundThenNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	placements := []*placementpb.PlacementSnapshots{
		&placementpb.PlacementSnapshots{
			Snapshots: []*placementpb.Placement{
				&placementpb.Placement{
					CutoverTime: 0,
				},
			},
		},
		testStagedPlacementProtoWithNumShards(t, testInstanceID, testShardSetID, 4),
		&placementpb.PlacementSnapshots{
			Snapshots: []*placementpb.Placement{
				&placementpb.Placement{
					CutoverTime: testPlacementCutover + 1000,
				},
			},
		},
	}

	// Instance not in the first placement.
	agg, store := testAggregatorWithCustomPlacements(t, ctrl, placements[0])
	require.NoError(t, agg.Open())
	require.Equal(t, uint32(0), agg.shardSetID)
	require.False(t, agg.shardSetOpen)
	require.Equal(t, 0, len(agg.shardIDs))
	require.Nil(t, agg.shards)
	require.Equal(t, int64(0), agg.currPlacement.CutoverNanos())
	require.Equal(t, aggregatorOpen, agg.state)

	// Update placement to add the instance and wait for it to propagate.
	_, err := store.Set(testPlacementKey, placements[1])
	require.NoError(t, err)
	for {
		_, p, err := agg.placementManager.Placement()
		require.NoError(t, err)
		if p.CutoverNanos() == testPlacementCutover {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Instance is now in the placement.
	agg.shardFn = func([]byte, uint32) uint32 { return 1 }
	require.NoError(t, agg.AddUntimed(testUntimedMetric, testStagedMetadatas))
	require.Equal(t, testShardSetID, agg.shardSetID)
	require.True(t, agg.shardSetOpen)
	require.Equal(t, aggregatorOpen, agg.state)
	require.Equal(t, []uint32{0, 1, 2, 3}, agg.shardIDs)
	for i := 0; i < testNumShards; i++ {
		require.NotNil(t, agg.shards[i])
	}
	require.Equal(t, int64(testPlacementCutover), agg.currPlacement.CutoverNanos())

	// Update placement to remove the instance and wait for it to propagate.
	_, err = store.Set(testPlacementKey, placements[2])
	require.NoError(t, err)
	for {
		_, p, err := agg.placementManager.Placement()
		require.NoError(t, err)
		if p.CutoverNanos() == testPlacementCutover+1000 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Instance is now removed from the placement.
	require.Error(t, agg.AddUntimed(testUntimedMetric, testStagedMetadatas))
	require.Equal(t, uint32(0), agg.shardSetID)
	require.False(t, agg.shardSetOpen)
	require.Equal(t, 0, len(agg.shardIDs))
	require.Nil(t, agg.shards)
	require.Equal(t, int64(testPlacementCutover+1000), agg.currPlacement.CutoverNanos())
	require.Equal(t, aggregatorOpen, agg.state)
}

func TestAggregatorAddUntimedInvalidMetricType(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, _ := testAggregator(t, ctrl)
	require.NoError(t, agg.Open())
	err := agg.AddUntimed(testInvalidMetric, testStagedMetadatas)
	require.Equal(t, errInvalidMetricType, err)
}

func TestAggregatorAddUntimedNotOpen(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, _ := testAggregator(t, ctrl)
	err := agg.AddUntimed(testUntimedMetric, testStagedMetadatas)
	require.Equal(t, errAggregatorNotOpenOrClosed, err)
}

func TestAggregatorAddUntimedNotResponsibleForShard(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, _ := testAggregator(t, ctrl)
	require.NoError(t, agg.Open())
	agg.shardFn = func([]byte, uint32) uint32 { return testNumShards }
	require.Equal(t, errShardNotOwned, agg.AddUntimed(testUntimedMetric, testStagedMetadatas))
}

func TestAggregatorAddUntimedSuccessNoPlacementUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, _ := testAggregator(t, ctrl)
	require.NoError(t, agg.Open())
	agg.shardFn = func([]byte, uint32) uint32 { return 1 }
	err := agg.AddUntimed(testUntimedMetric, testStagedMetadatas)
	require.NoError(t, err)
	require.Equal(t, 1, len(agg.shards[1].metricMap.entries))
}

func TestAggregatorAddUntimedSuccessWithPlacementUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, store := testAggregator(t, ctrl)
	now := time.Unix(0, 12345)
	nowFn := func() time.Time { return now }
	agg.opts = agg.opts.
		SetClockOptions(agg.opts.ClockOptions().SetNowFn(nowFn)).
		SetBufferDurationBeforeShardCutover(time.Duration(500)).
		SetBufferDurationAfterShardCutoff(time.Duration(1000))
	agg.shardFn = func([]byte, uint32) uint32 { return 1 }
	require.NoError(t, agg.Open())
	require.Equal(t, int64(testPlacementCutover), agg.currPlacement.CutoverNanos())

	newShardAssignment := []shard.Shard{
		shard.NewShard(0).SetState(shard.Initializing).SetCutoverNanos(5000).SetCutoffNanos(20000),
		shard.NewShard(1).SetState(shard.Initializing).SetCutoverNanos(5500).SetCutoffNanos(25000),
		shard.NewShard(2).SetState(shard.Initializing).SetCutoverNanos(6000).SetCutoffNanos(30000),
		shard.NewShard(4).SetState(shard.Initializing).SetCutoverNanos(6500).SetCutoffNanos(math.MaxInt64),
	}
	newPlacementCutoverNanos := int64(testPlacementCutover - 1)
	newStagedPlacementProto := testStagedPlacementProtoWithCustomShards(t, testInstanceID, testShardSetID, newShardAssignment, newPlacementCutoverNanos)
	_, err := store.Set(testPlacementKey, newStagedPlacementProto)
	require.NoError(t, err)

	// Wait for the placement to be updated.
	for {
		_, placement, err := agg.placementManager.Placement()
		require.NoError(t, err)
		if placement.CutoverNanos() == newPlacementCutoverNanos {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	existingShard := agg.shards[3]
	err = agg.AddUntimed(testUntimedMetric, testStagedMetadatas)
	require.NoError(t, err)
	require.Equal(t, 5, len(agg.shards))

	expectedShards := []struct {
		isNil         bool
		earliestNanos int64
		latestNanos   int64
	}{
		{isNil: false, earliestNanos: 4500, latestNanos: 21000},
		{isNil: false, earliestNanos: 5000, latestNanos: 26000},
		{isNil: false, earliestNanos: 5500, latestNanos: 31000},
		{isNil: true},
		{isNil: false, earliestNanos: 6000, latestNanos: math.MaxInt64},
	}
	for i, expected := range expectedShards {
		if expected.isNil {
			require.Nil(t, agg.shards[i])
		} else {
			require.NotNil(t, agg.shards[i])
			require.Equal(t, expected.earliestNanos, agg.shards[i].earliestWritableNanos)
			require.Equal(t, expected.latestNanos, agg.shards[i].latestWriteableNanos)
		}
	}
	require.Equal(t, 1, len(agg.shards[1].metricMap.entries))
	require.Equal(t, newPlacementCutoverNanos, agg.currPlacement.CutoverNanos())

	for {
		existingShard.RLock()
		closed := existingShard.closed
		existingShard.RUnlock()
		if closed {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestAggregatorAddTimedNotOpen(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, _ := testAggregator(t, ctrl)
	err := agg.AddTimed(testTimedMetric, testTimedMetadata)
	require.Equal(t, errAggregatorNotOpenOrClosed, err)
}

func TestAggregatorAddTimedNotResponsibleForShard(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, _ := testAggregator(t, ctrl)
	require.NoError(t, agg.Open())
	agg.shardFn = func([]byte, uint32) uint32 { return testNumShards }
	require.Equal(t, errShardNotOwned, agg.AddTimed(testTimedMetric, testTimedMetadata))
}

func TestAggregatorAddTimedSuccessNoPlacementUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, _ := testAggregator(t, ctrl)
	now := time.Unix(0, 12345)
	nowFn := func() time.Time { return now }
	agg.nowFn = nowFn

	require.NoError(t, agg.Open())
	agg.shardFn = func([]byte, uint32) uint32 { return 1 }
	err := agg.AddTimed(testTimedMetric, testTimedMetadata)
	require.NoError(t, err)
	require.Equal(t, 1, len(agg.shards[1].metricMap.entries))
}

func TestAggregatorAddTimedSuccessWithPlacementUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, store := testAggregator(t, ctrl)
	now := time.Unix(0, 12345)
	nowFn := func() time.Time { return now }
	agg.opts = agg.opts.
		SetClockOptions(agg.opts.ClockOptions().SetNowFn(nowFn)).
		SetBufferDurationBeforeShardCutover(time.Duration(500)).
		SetBufferDurationAfterShardCutoff(time.Duration(1000))
	agg.shardFn = func([]byte, uint32) uint32 { return 1 }
	require.NoError(t, agg.Open())
	require.Equal(t, int64(testPlacementCutover), agg.currPlacement.CutoverNanos())

	newShardAssignment := []shard.Shard{
		shard.NewShard(0).SetState(shard.Initializing).SetCutoverNanos(5000).SetCutoffNanos(20000),
		shard.NewShard(1).SetState(shard.Initializing).SetCutoverNanos(5500).SetCutoffNanos(25000),
		shard.NewShard(2).SetState(shard.Initializing).SetCutoverNanos(6000).SetCutoffNanos(30000),
		shard.NewShard(4).SetState(shard.Initializing).SetCutoverNanos(6500).SetCutoffNanos(math.MaxInt64),
	}
	newPlacementCutoverNanos := int64(testPlacementCutover - 1)
	newStagedPlacementProto := testStagedPlacementProtoWithCustomShards(t, testInstanceID, testShardSetID, newShardAssignment, newPlacementCutoverNanos)
	_, err := store.Set(testPlacementKey, newStagedPlacementProto)
	require.NoError(t, err)

	// Wait for the placement to be updated.
	for {
		_, placement, err := agg.placementManager.Placement()
		require.NoError(t, err)
		if placement.CutoverNanos() == newPlacementCutoverNanos {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	existingShard := agg.shards[3]
	err = agg.AddTimed(testTimedMetric, testTimedMetadata)
	require.NoError(t, err)
	require.Equal(t, 5, len(agg.shards))

	expectedShards := []struct {
		isNil         bool
		earliestNanos int64
		latestNanos   int64
	}{
		{isNil: false, earliestNanos: 4500, latestNanos: 21000},
		{isNil: false, earliestNanos: 5000, latestNanos: 26000},
		{isNil: false, earliestNanos: 5500, latestNanos: 31000},
		{isNil: true},
		{isNil: false, earliestNanos: 6000, latestNanos: math.MaxInt64},
	}
	for i, expected := range expectedShards {
		if expected.isNil {
			require.Nil(t, agg.shards[i])
		} else {
			require.NotNil(t, agg.shards[i])
			require.Equal(t, expected.earliestNanos, agg.shards[i].earliestWritableNanos)
			require.Equal(t, expected.latestNanos, agg.shards[i].latestWriteableNanos)
		}
	}
	require.Equal(t, 1, len(agg.shards[1].metricMap.entries))
	require.Equal(t, newPlacementCutoverNanos, agg.currPlacement.CutoverNanos())

	for {
		existingShard.RLock()
		closed := existingShard.closed
		existingShard.RUnlock()
		if closed {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestAggregatorAddForwardedNotOpen(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, _ := testAggregator(t, ctrl)
	err := agg.AddForwarded(testForwardedMetric, testForwardMetadata)
	require.Equal(t, errAggregatorNotOpenOrClosed, err)
}

func TestAggregatorAddForwardedNotResponsibleForShard(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, _ := testAggregator(t, ctrl)
	require.NoError(t, agg.Open())
	agg.shardFn = func([]byte, uint32) uint32 { return testNumShards }
	require.Equal(t, errShardNotOwned, agg.AddForwarded(testForwardedMetric, testForwardMetadata))
}

func TestAggregatorAddForwardedSuccessNoPlacementUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, _ := testAggregator(t, ctrl)
	now := time.Unix(0, 12345)
	nowFn := func() time.Time { return now }
	agg.nowFn = nowFn

	require.NoError(t, agg.Open())
	agg.shardFn = func([]byte, uint32) uint32 { return 1 }
	err := agg.AddForwarded(testForwardedMetric, testForwardMetadata)
	require.NoError(t, err)
	require.Equal(t, 1, len(agg.shards[1].metricMap.entries))
}

func TestAggregatorAddForwardedSuccessWithPlacementUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, store := testAggregator(t, ctrl)
	now := time.Unix(0, 12345)
	nowFn := func() time.Time { return now }
	agg.opts = agg.opts.
		SetClockOptions(agg.opts.ClockOptions().SetNowFn(nowFn)).
		SetBufferDurationBeforeShardCutover(time.Duration(500)).
		SetBufferDurationAfterShardCutoff(time.Duration(1000))
	agg.shardFn = func([]byte, uint32) uint32 { return 1 }
	require.NoError(t, agg.Open())
	require.Equal(t, int64(testPlacementCutover), agg.currPlacement.CutoverNanos())

	newShardAssignment := []shard.Shard{
		shard.NewShard(0).SetState(shard.Initializing).SetCutoverNanos(5000).SetCutoffNanos(20000),
		shard.NewShard(1).SetState(shard.Initializing).SetCutoverNanos(5500).SetCutoffNanos(25000),
		shard.NewShard(2).SetState(shard.Initializing).SetCutoverNanos(6000).SetCutoffNanos(30000),
		shard.NewShard(4).SetState(shard.Initializing).SetCutoverNanos(6500).SetCutoffNanos(math.MaxInt64),
	}
	newPlacementCutoverNanos := int64(testPlacementCutover - 1)
	newStagedPlacementProto := testStagedPlacementProtoWithCustomShards(t, testInstanceID, testShardSetID, newShardAssignment, newPlacementCutoverNanos)
	_, err := store.Set(testPlacementKey, newStagedPlacementProto)
	require.NoError(t, err)

	// Wait for the placement to be updated.
	for {
		_, placement, err := agg.placementManager.Placement()
		require.NoError(t, err)
		if placement.CutoverNanos() == newPlacementCutoverNanos {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	existingShard := agg.shards[3]
	err = agg.AddForwarded(testForwardedMetric, testForwardMetadata)
	require.NoError(t, err)
	require.Equal(t, 5, len(agg.shards))

	expectedShards := []struct {
		isNil         bool
		earliestNanos int64
		latestNanos   int64
	}{
		{isNil: false, earliestNanos: 4500, latestNanos: 21000},
		{isNil: false, earliestNanos: 5000, latestNanos: 26000},
		{isNil: false, earliestNanos: 5500, latestNanos: 31000},
		{isNil: true},
		{isNil: false, earliestNanos: 6000, latestNanos: math.MaxInt64},
	}
	for i, expected := range expectedShards {
		if expected.isNil {
			require.Nil(t, agg.shards[i])
		} else {
			require.NotNil(t, agg.shards[i])
			require.Equal(t, expected.earliestNanos, agg.shards[i].earliestWritableNanos)
			require.Equal(t, expected.latestNanos, agg.shards[i].latestWriteableNanos)
		}
	}
	require.Equal(t, 1, len(agg.shards[1].metricMap.entries))
	require.Equal(t, newPlacementCutoverNanos, agg.currPlacement.CutoverNanos())

	for {
		existingShard.RLock()
		closed := existingShard.closed
		existingShard.RUnlock()
		if closed {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestAggregatorResignError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errTestResign := errors.New("test resign")
	electionMgr := NewMockElectionManager(ctrl)
	electionMgr.EXPECT().Resign(gomock.Any()).Return(errTestResign)
	agg, _ := testAggregator(t, ctrl)
	agg.electionManager = electionMgr

	require.Equal(t, errTestResign, agg.Resign())
}

func TestAggregatorResignSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	electionMgr := NewMockElectionManager(ctrl)
	electionMgr.EXPECT().Resign(gomock.Any()).Return(nil)
	agg, _ := testAggregator(t, ctrl)
	agg.electionManager = electionMgr
	require.NoError(t, agg.Resign())
}

func TestAggregatorAddPassThroughNotOpen(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, _ := testAggregator(t, ctrl)
	err := agg.AddPassThrough(testTimedMetric, testTimedMetadata)
	require.Equal(t, errAggregatorNotOpenOrClosed, err)
}

func TestAggregatorAddPassThroughSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, _ := testAggregator(t, ctrl)
	require.NoError(t, agg.Open())
	err := agg.AddPassThrough(testTimedMetric, testTimedMetadata)
	require.NoError(t, err)
}

func TestAggregatorStatus(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	flushStatus := FlushStatus{
		ElectionState: LeaderState,
		CanLead:       true,
	}
	flushManager := NewMockFlushManager(ctrl)
	flushManager.EXPECT().Status().Return(flushStatus)
	agg, _ := testAggregator(t, ctrl)
	agg.flushManager = flushManager
	require.Equal(t, RuntimeStatus{FlushStatus: flushStatus}, agg.Status())
}

func TestAggregatorCloseAlreadyClosed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, _ := testAggregator(t, ctrl)
	require.Equal(t, errAggregatorNotOpenOrClosed, agg.Close())
}

func TestAggregatorCloseSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, _ := testAggregator(t, ctrl)
	require.NoError(t, agg.Open())
	require.NoError(t, agg.Close())
	require.Equal(t, aggregatorClosed, agg.state)
}

func TestAggregatorTick(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	flushTimesManager := NewMockFlushTimesManager(ctrl)
	flushTimesManager.EXPECT().Reset().Return(nil).AnyTimes()
	flushTimesManager.EXPECT().Open(gomock.Any()).Return(nil).AnyTimes()
	flushTimesManager.EXPECT().Get().Return(nil, nil).AnyTimes()
	flushTimesManager.EXPECT().Close().Return(nil).AnyTimes()

	agg, _ := testAggregator(t, ctrl)
	agg.flushTimesManager = flushTimesManager
	require.NoError(t, agg.Open())

	// Forcing a tick.
	agg.tickInternal()

	require.NoError(t, agg.Close())
}

func TestAggregatorShardSetNotOpenNilInstance(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, _ := testAggregator(t, ctrl)
	agg.shardSetOpen = false
	agg.Lock()
	defer agg.Unlock()
	require.NoError(t, agg.updateShardSetIDWithLock(nil))
}

func TestAggregatorShardSetNotOpenValidInstance(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		flushTimesManagerOpenID *uint32
		electionManagerOpenID   *uint32
		testInstance            = placement.NewInstance().SetShardSetID(testShardSetID)
	)
	electionMgr := NewMockElectionManager(ctrl)
	electionMgr.EXPECT().
		Open(testShardSetID).
		DoAndReturn(func(shardSetID uint32) error {
			electionManagerOpenID = &shardSetID
			return nil
		})
	flushTimesManager := NewMockFlushTimesManager(ctrl)
	flushTimesManager.EXPECT().
		Open(testShardSetID).
		DoAndReturn(func(shardSetID uint32) error {
			flushTimesManagerOpenID = &shardSetID
			return nil
		})
	agg, _ := testAggregator(t, ctrl)
	agg.shardSetOpen = false
	agg.flushTimesManager = flushTimesManager
	agg.electionManager = electionMgr

	agg.Lock()
	defer agg.Unlock()
	require.NoError(t, agg.updateShardSetIDWithLock(testInstance))
	require.Equal(t, testShardSetID, agg.shardSetID)
	require.True(t, agg.shardSetOpen)
	require.Equal(t, testShardSetID, *flushTimesManagerOpenID)
	require.Equal(t, testShardSetID, *electionManagerOpenID)
}

func TestAggregatorShardSetOpenShardSetIDUnchanged(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testInstance := placement.NewInstance().SetShardSetID(testShardSetID)
	agg, _ := testAggregator(t, ctrl)
	agg.shardSetOpen = true
	agg.shardSetID = testShardSetID
	agg.Lock()
	defer agg.Unlock()
	require.NoError(t, agg.updateShardSetIDWithLock(testInstance))
	require.Equal(t, testShardSetID, agg.shardSetID)
	require.True(t, agg.shardSetOpen)
}

func TestAggregatorShardSetOpenNilInstance(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, _ := testAggregator(t, ctrl)
	agg.shardSetOpen = true
	agg.shardSetID = testShardSetID
	agg.Lock()
	defer agg.Unlock()
	require.NoError(t, agg.updateShardSetIDWithLock(nil))
	require.Equal(t, uint32(0), agg.shardSetID)
	require.False(t, agg.shardSetOpen)
}

func TestAggregatorShardSetOpenValidInstance(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		flushTimesManagerOpenID *uint32
		electionManagerOpenID   *uint32
		newShardSetID           = uint32(2)
		testInstance            = placement.NewInstance().SetShardSetID(newShardSetID)
	)
	electionMgr := NewMockElectionManager(ctrl)
	electionMgr.EXPECT().Reset().Return(nil)
	electionMgr.EXPECT().
		Open(newShardSetID).
		DoAndReturn(func(shardSetID uint32) error {
			electionManagerOpenID = &shardSetID
			return nil
		})
	electionMgr.EXPECT().Close().Return(nil)
	flushTimesManager := NewMockFlushTimesManager(ctrl)
	flushTimesManager.EXPECT().Reset().Return(nil)
	flushTimesManager.EXPECT().
		Open(newShardSetID).
		DoAndReturn(func(shardSetID uint32) error {
			flushTimesManagerOpenID = &shardSetID
			return nil
		})
	flushTimesManager.EXPECT().Close().Return(nil)

	agg, _ := testAggregator(t, ctrl)
	agg.shardSetOpen = true
	agg.shardSetID = testShardSetID
	agg.flushTimesManager = flushTimesManager
	agg.electionManager = electionMgr

	agg.Lock()
	defer agg.Unlock()
	require.NoError(t, agg.updateShardSetIDWithLock(testInstance))
	require.Equal(t, newShardSetID, agg.shardSetID)
	require.True(t, agg.shardSetOpen)
	require.Equal(t, newShardSetID, *flushTimesManagerOpenID)
	require.Equal(t, newShardSetID, *electionManagerOpenID)
}

func TestAggregatorOwnedShards(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, _ := testAggregator(t, ctrl)
	now := time.Unix(0, 12345)
	nowFn := func() time.Time { return now }
	agg.nowFn = nowFn
	agg.opts = agg.opts.SetClockOptions(agg.opts.ClockOptions().SetNowFn(nowFn))

	inputs := []struct {
		cutoffNanos         int64
		latestWritableNanos int64
		flushedNanos        int64
	}{
		{
			cutoffNanos:         math.MaxInt64,
			latestWritableNanos: math.MaxInt64,
			flushedNanos:        12340,
		},
		{
			cutoffNanos:         1234,
			latestWritableNanos: 5678,
			flushedNanos:        1234,
		},
		{
			cutoffNanos:         56789,
			latestWritableNanos: 67890,
			flushedNanos:        12345,
		},
		{
			cutoffNanos:         8901,
			latestWritableNanos: 34567,
			flushedNanos:        7200,
		},
	}
	flushTimes := make(map[uint32]*schema.ShardFlushTimes, len(inputs))
	agg.shardIDs = make([]uint32, 0, len(inputs))
	agg.shards = make([]*aggregatorShard, 0, len(inputs))
	for i, input := range inputs {
		shardID := uint32(i)
		shard := newAggregatorShard(shardID, agg.opts)
		shard.cutoffNanos = input.cutoffNanos
		shard.latestWriteableNanos = input.latestWritableNanos
		flushTimes[shardID] = &schema.ShardFlushTimes{
			StandardByResolution: map[int64]int64{
				int64(time.Second): input.flushedNanos,
			},
		}
		agg.shardIDs = append(agg.shardIDs, shardID)
		agg.shards = append(agg.shards, shard)
	}
	flushTimesManager := NewMockFlushTimesManager(ctrl)
	flushTimesManager.EXPECT().
		Get().
		Return(&schema.ShardSetFlushTimes{ByShard: flushTimes}, nil)
	agg.flushTimesManager = flushTimesManager

	expectedOwned := []*aggregatorShard{agg.shards[0], agg.shards[3], agg.shards[2]}
	expectedToClose := []*aggregatorShard{agg.shards[1]}
	owned, toClose := agg.ownedShards()
	require.Equal(t, expectedOwned, owned)
	require.Equal(t, expectedToClose, toClose)

	expectedShardIDs := []uint32{0, 3, 2}
	shardIDs := make([]uint32, len(agg.shardIDs))
	copy(shardIDs, agg.shardIDs)
	sort.Sort(uint32Ascending(shardIDs))
	require.Equal(t, expectedShardIDs, agg.shardIDs)

	for i := 0; i < 4; i++ {
		if i == 1 {
			require.Nil(t, agg.shards[i])
		} else {
			require.NotNil(t, agg.shards[i])
		}
	}
}

func TestAggregatorAddMetricMetrics(t *testing.T) {
	s := tally.NewTestScope("testScope", nil)
	m := newAggregatorAddUntimedMetrics(s, 1.0)
	m.ReportSuccess(time.Second)
	m.ReportError(errInvalidMetricType)
	m.ReportError(errShardNotOwned)
	m.ReportError(errAggregatorShardNotWriteable)
	m.ReportError(errWriteNewMetricRateLimitExceeded)
	m.ReportError(errWriteValueRateLimitExceeded)
	m.ReportError(errors.New("foo"))

	snapshot := s.Snapshot()
	counters, timers, gauges := snapshot.Counters(), snapshot.Timers(), snapshot.Gauges()

	// Validate we count successes and errors correctly.
	require.Equal(t, 7, len(counters))
	for _, id := range []string{
		"testScope.success+",
		"testScope.errors+reason=invalid-metric-types",
		"testScope.errors+reason=shard-not-owned",
		"testScope.errors+reason=shard-not-writeable",
		"testScope.errors+reason=value-rate-limit-exceeded",
		"testScope.errors+reason=new-metric-rate-limit-exceeded",
		"testScope.errors+reason=not-categorized",
	} {
		c, exists := counters[id]
		require.True(t, exists)
		require.Equal(t, int64(1), c.Value())
	}

	// Validate we record times correctly.
	require.Equal(t, 1, len(timers))

	for _, id := range []string{
		"testScope.success-latency+",
	} {
		ti, exists := timers[id]
		require.True(t, exists)
		require.Equal(t, []time.Duration{time.Second}, ti.Values())
	}

	// Validate we do not have any gauges.
	require.Equal(t, 0, len(gauges))
}

func TestAggregatorAddTimedMetrics(t *testing.T) {
	s := tally.NewTestScope("testScope", nil)
	m := newAggregatorAddTimedMetrics(s, 1.0)
	m.ReportSuccess(time.Second)
	m.ReportError(errShardNotOwned)
	m.ReportError(errAggregatorShardNotWriteable)
	m.ReportError(errWriteNewMetricRateLimitExceeded)
	m.ReportError(errWriteValueRateLimitExceeded)
	m.ReportError(errTooFarInTheFuture)
	m.ReportError(errTooFarInThePast)
	m.ReportError(errors.New("foo"))

	snapshot := s.Snapshot()
	counters, timers, gauges := snapshot.Counters(), snapshot.Timers(), snapshot.Gauges()

	// Validate we count successes and errors correctly.
	require.Equal(t, 8, len(counters))
	for _, id := range []string{
		"testScope.success+",
		"testScope.errors+reason=shard-not-owned",
		"testScope.errors+reason=shard-not-writeable",
		"testScope.errors+reason=value-rate-limit-exceeded",
		"testScope.errors+reason=new-metric-rate-limit-exceeded",
		"testScope.errors+reason=too-far-in-the-future",
		"testScope.errors+reason=too-far-in-the-past",
		"testScope.errors+reason=not-categorized",
	} {
		c, exists := counters[id]
		require.True(t, exists)
		require.Equal(t, int64(1), c.Value())
	}

	// Validate we record times correctly.
	require.Equal(t, 1, len(timers))

	for _, id := range []string{
		"testScope.success-latency+",
	} {
		ti, exists := timers[id]
		require.True(t, exists)
		require.Equal(t, []time.Duration{time.Second}, ti.Values())
	}

	// Validate we do not have any gauges.
	require.Equal(t, 0, len(gauges))
}

func testAggregator(t *testing.T, ctrl *gomock.Controller) (*aggregator, kv.Store) {
	proto := testStagedPlacementProtoWithNumShards(t, testInstanceID, testShardSetID, testNumShards)
	return testAggregatorWithCustomPlacements(t, ctrl, proto)
}

func testAggregatorWithCustomPlacements(
	t *testing.T,
	ctrl *gomock.Controller,
	proto *placementpb.PlacementSnapshots,
) (*aggregator, kv.Store) {
	watcher, store := testPlacementWatcherWithPlacementProto(t, testPlacementKey, proto)
	placementManagerOpts := NewPlacementManagerOptions().
		SetInstanceID(testInstanceID).
		SetStagedPlacementWatcher(watcher)
	placementManager := NewPlacementManager(placementManagerOpts)
	opts := testOptions(ctrl).
		SetEntryCheckInterval(0).
		SetPlacementManager(placementManager)
	return NewAggregator(opts).(*aggregator), store
}

// nolint: unparam
func testPlacementWatcherWithPlacementProto(
	t *testing.T,
	placementKey string,
	proto *placementpb.PlacementSnapshots,
) (placement.StagedPlacementWatcher, kv.Store) {
	store := mem.NewStore()
	_, err := store.SetIfNotExists(placementKey, proto)
	require.NoError(t, err)
	placementWatcherOpts := placement.NewStagedPlacementWatcherOptions().
		SetStagedPlacementKey(placementKey).
		SetStagedPlacementStore(store)
	placementWatcher := placement.NewStagedPlacementWatcher(placementWatcherOpts)
	return placementWatcher, store
}

// nolint: unparam
func testStagedPlacementProtoWithNumShards(
	t *testing.T,
	instanceID string,
	shardSetID uint32,
	numShards int,
) *placementpb.PlacementSnapshots {
	shardSet := make([]shard.Shard, numShards)
	for i := 0; i < numShards; i++ {
		shardSet[i] = shard.NewShard(uint32(i)).
			SetState(shard.Initializing).
			SetCutoverNanos(0).
			SetCutoffNanos(math.MaxInt64)
	}
	return testStagedPlacementProtoWithCustomShards(t, instanceID, shardSetID, shardSet, testPlacementCutover)
}

// nolint: unparam
func testStagedPlacementProtoWithCustomShards(
	t *testing.T,
	instanceID string,
	shardSetID uint32,
	shardSet []shard.Shard,
	placementCutoverNanos int64,
) *placementpb.PlacementSnapshots {
	shards := shard.NewShards(shardSet)
	instance := placement.NewInstance().
		SetID(instanceID).
		SetShards(shards).
		SetShardSetID(shardSetID)
	testPlacement := placement.NewPlacement().
		SetInstances([]placement.Instance{instance}).
		SetShards(shards.AllIDs()).
		SetCutoverNanos(placementCutoverNanos)
	testStagedPlacement := placement.NewStagedPlacement().
		SetPlacements([]placement.Placement{testPlacement})
	stagedPlacementProto, err := testStagedPlacement.Proto()
	require.NoError(t, err)
	return stagedPlacementProto
}

func testOptions(ctrl *gomock.Controller) Options {
	placementManager := NewMockPlacementManager(ctrl)
	placementManager.EXPECT().Close().Return(nil).AnyTimes()

	flushTimesManager := NewMockFlushTimesManager(ctrl)
	flushTimesManager.EXPECT().Reset().Return(nil).AnyTimes()
	flushTimesManager.EXPECT().Open(gomock.Any()).Return(nil).AnyTimes()
	flushTimesManager.EXPECT().Close().Return(nil).AnyTimes()

	electionMgr := NewMockElectionManager(ctrl)
	electionMgr.EXPECT().Reset().Return(nil).AnyTimes()
	electionMgr.EXPECT().Open(gomock.Any()).Return(nil).AnyTimes()
	electionMgr.EXPECT().Close().Return(nil).AnyTimes()

	flushManager := NewMockFlushManager(ctrl)
	flushManager.EXPECT().Reset().Return(nil).AnyTimes()
	flushManager.EXPECT().Open().Return(nil).AnyTimes()
	flushManager.EXPECT().Register(gomock.Any()).Return(nil).AnyTimes()
	flushManager.EXPECT().Unregister(gomock.Any()).Return(nil).AnyTimes()
	flushManager.EXPECT().Close().Return(nil).AnyTimes()

	w := writer.NewMockWriter(ctrl)
	w.EXPECT().Write(gomock.Any()).Return(nil).AnyTimes()
	w.EXPECT().Flush().Return(nil).AnyTimes()
	w.EXPECT().Close().Return(nil).AnyTimes()

	h := handler.NewMockHandler(ctrl)
	h.EXPECT().NewWriter(gomock.Any()).Return(w, nil).AnyTimes()
	h.EXPECT().Close().AnyTimes()

	pw := writer.NewMockWriter(ctrl)
	pw.EXPECT().Write(gomock.Any()).Return(nil).AnyTimes()
	pw.EXPECT().Flush().Return(nil).AnyTimes()
	pw.EXPECT().Close().Return(nil).AnyTimes()

	cl := client.NewMockAdminClient(ctrl)
	cl.EXPECT().Flush().Return(nil).AnyTimes()
	cl.EXPECT().Close().AnyTimes()

	infiniteAllowedDelayFn := func(time.Duration, int) time.Duration {
		return math.MaxInt64
	}
	infiniteBufferForPastTimedMetricFn := func(time.Duration) time.Duration {
		return math.MaxInt64
	}
	return NewOptions().
		SetPlacementManager(placementManager).
		SetFlushTimesManager(flushTimesManager).
		SetElectionManager(electionMgr).
		SetFlushManager(flushManager).
		SetFlushHandler(h).
		SetPassThroughWriter(pw).
		SetAdminClient(cl).
		SetMaxAllowedForwardingDelayFn(infiniteAllowedDelayFn).
		SetBufferForFutureTimedMetric(math.MaxInt64).
		SetBufferForPastTimedMetricFn(infiniteBufferForPastTimedMetricFn)
}

type uint32Ascending []uint32

func (a uint32Ascending) Len() int           { return len(a) }
func (a uint32Ascending) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a uint32Ascending) Less(i, j int) bool { return a[i] < a[j] }
