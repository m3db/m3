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

	"go.uber.org/zap"

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
	"github.com/m3db/m3/src/metrics/transformation"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/gogo/protobuf/types"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
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
	testPassthroughMetric = aggregated.Metric{
		Type:      metric.CounterType,
		ID:        []byte("testPassthrough"),
		TimeNanos: 12345,
		Value:     1000,
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
	testPassthroughStroagePolicy = policy.NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour)
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
	placementManager.EXPECT().Placement().Return(nil, errPlacement)

	agg, _ := testAggregator(t, ctrl)
	agg.placementManager = placementManager
	require.Equal(t, errPlacement, agg.Open())
}

func TestAggregatorOpenInstanceFromError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testPlacement := placement.NewPlacement().SetCutoverNanos(5678)
	errInstanceFrom := errors.New("error getting instance from placement")
	placementManager := NewMockPlacementManager(ctrl)
	placementManager.EXPECT().Open().Return(nil)
	placementManager.EXPECT().Placement().Return(testPlacement, nil)
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

	placementManager.EXPECT().Open().Return(nil)
	placementManager.EXPECT().C().Return(make(chan struct{})).AnyTimes()
	placementManager.EXPECT().InstanceID().Return(agg.opts.PlacementManager().InstanceID())
	placementManager.EXPECT().Placement().Return(testPlacement, nil).AnyTimes()
	placementManager.EXPECT().InstanceFrom(testPlacement).Return(nil, ErrInstanceNotFoundInPlacement)

	require.NoError(t, agg.Open())
	require.Equal(t, uint32(0), agg.shardSetID)
	require.False(t, agg.shardSetOpen)
	require.Equal(t, 0, len(agg.shardIDs))
	require.Nil(t, agg.shards)
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
	require.NotNil(t, agg.currPlacement)
	require.Equal(t, int64(testPlacementCutover), agg.currPlacement.CutoverNanos())
}

func TestAggregatorUpdateStagedMetadatas(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cases := []struct {
		name       string
		addToReset bool
	}{
		{
			name:       "enabled",
			addToReset: true,
		},
		{
			name:       "disabled",
			addToReset: false,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			agg, _ := testAggregator(t, ctrl)
			agg.opts = agg.opts.SetAddToReset(tc.addToReset)
			require.NoError(t, agg.Open())
			agg.shardFn = func([]byte, uint32) uint32 { return 1 }
			sms := metadata.StagedMetadatas{
				{
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								Pipeline: applied.NewPipeline([]applied.OpUnion{
									{
										Type:           pipeline.TransformationOpType,
										Transformation: pipeline.TransformationOp{Type: transformation.Add},
									},
								}),
							},
						},
					},
				},
			}
			err := agg.AddUntimed(testUntimedMetric, sms)
			require.NoError(t, err)
			require.Equal(t, 1, len(agg.shards[1].metricMap.entries))
			for _, e := range agg.shards[1].metricMap.entries {
				actual := e.Value.(hashedEntry).entry.aggregations[0].key.pipeline
				if tc.addToReset {
					require.Equal(t, transformation.Reset, actual.Operations[0].Transformation.Type)
				} else {
					require.Equal(t, transformation.Add, actual.Operations[0].Transformation.Type)
				}
			}
		})
	}
}

func TestAggregatorInstanceNotFoundThenFoundThenNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	placements := []*placementpb.PlacementSnapshots{
		{
			Snapshots: []*placementpb.Placement{
				{
					CutoverTime: 0,
				},
			},
		},
		testStagedPlacementProtoWithNumShards(t, testInstanceID, testShardSetID, 4),
		{
			Snapshots: []*placementpb.Placement{
				{
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
		p, err := agg.placementManager.Placement()
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
		p, err := agg.placementManager.Placement()
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

func TestAggregatorAddUntimedShardNotOwned(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, _ := testAggregator(t, ctrl)
	err := agg.AddUntimed(testUntimedMetric, testStagedMetadatas)
	require.Equal(t, errShardNotOwned, err)
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

	existingShard := agg.shards[3]

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
		placement, err := agg.placementManager.Placement()
		require.NoError(t, err)
		if placement.CutoverNanos() == newPlacementCutoverNanos {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

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

func TestAggregatorAddUntimedWithShardRedirect(t *testing.T) {
	testAddWithShardRedirect(t, func(agg *aggregator) error {
		return agg.AddUntimed(testUntimedMetric, testStagedMetadatas)
	})
}

func TestAggregatorAddUntimedWithShardRedirectToNotOwned(t *testing.T) {
	testAddWithShardRedirectToNotOwned(t, func(agg *aggregator) error {
		return agg.AddUntimed(testUntimedMetric, testStagedMetadatas)
	})
}

func TestAggregatorAddTimedShardNotOwned(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, _ := testAggregator(t, ctrl)
	err := agg.AddTimed(testTimedMetric, testTimedMetadata)
	require.Equal(t, errShardNotOwned, err)
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

	existingShard := agg.shards[3]

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
		placement, err := agg.placementManager.Placement()
		require.NoError(t, err)
		if placement.CutoverNanos() == newPlacementCutoverNanos {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

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

func TestAggregatorAddTimedWithShardRedirect(t *testing.T) {
	testAddWithShardRedirect(t, func(agg *aggregator) error {
		return agg.AddTimed(testTimedMetric, testTimedMetadata)
	})
}

func TestAggregatorAddTimedWithShardRedirectToNotOwned(t *testing.T) {
	testAddWithShardRedirectToNotOwned(t, func(agg *aggregator) error {
		return agg.AddTimed(testTimedMetric, testTimedMetadata)
	})
}

func TestAggregatorAddForwardedShardNotOwned(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, _ := testAggregator(t, ctrl)
	err := agg.AddForwarded(testForwardedMetric, testForwardMetadata)
	require.Equal(t, errShardNotOwned, err)
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

	existingShard := agg.shards[3]

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
		placement, err := agg.placementManager.Placement()
		require.NoError(t, err)
		if placement.CutoverNanos() == newPlacementCutoverNanos {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

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

func TestAggregatorAddForwardedWithShardRedirect(t *testing.T) {
	testAddWithShardRedirect(t, func(agg *aggregator) error {
		return agg.AddForwarded(testForwardedMetric, testForwardMetadata)
	})
}

func TestAggregatorAddForwardedWithShardRedirectToNotOwned(t *testing.T) {
	testAddWithShardRedirectToNotOwned(t, func(agg *aggregator) error {
		return agg.AddForwarded(testForwardedMetric, testForwardMetadata)
	})
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

func TestAggregatorAddPassthroughNotOpen(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, _ := testAggregator(t, ctrl)
	err := agg.AddPassthrough(testPassthroughMetric, testPassthroughStroagePolicy)
	require.Equal(t, errAggregatorNotOpenOrClosed, err)
}

func TestAggregatorAddPassthroughSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, _ := testAggregator(t, ctrl)
	require.NoError(t, agg.Open())
	err := agg.AddPassthrough(testPassthroughMetric, testPassthroughStroagePolicy)
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

func TestAggregatorAddUntimedMetrics(t *testing.T) {
	s := tally.NewTestScope("testScope", nil)
	m := newAggregatorAddUntimedMetrics(s, instrument.TimerOptions{StandardSampleRate: 0.001})
	m.ReportSuccess()
	m.SuccessLatencyStopwatch().Stop()
	log := zap.NewNop()
	for _, state := range []ElectionState{LeaderState, FollowerState} {
		m.ReportError(errInvalidMetricType, state, log)
		m.ReportError(errShardNotOwned, state, log)
		m.ReportError(errAggregatorShardNotWriteable, state, log)
		m.ReportError(errWriteNewMetricRateLimitExceeded, state, log)
		m.ReportError(errWriteValueRateLimitExceeded, state, log)
		m.ReportError(xerrors.NewRenamedError(errArrivedTooLate, errors.New("errorrr")), state, log)
		m.ReportError(errAggregationClosed, state, log)
		m.ReportError(errors.New("foo"), state, log)
	}

	snapshot := s.Snapshot()
	counters, timers, gauges := snapshot.Counters(), snapshot.Timers(), snapshot.Gauges()

	// Validate we count successes and errors correctly.
	expectedCounters := []string{
		"testScope.success+",
		"testScope.errors+reason=invalid-metric-types,role=leader",
		"testScope.errors+reason=invalid-metric-types,role=non-leader",
		"testScope.errors+reason=shard-not-owned,role=leader",
		"testScope.errors+reason=shard-not-owned,role=non-leader",
		"testScope.errors+reason=shard-not-writeable,role=leader",
		"testScope.errors+reason=shard-not-writeable,role=non-leader",
		"testScope.errors+reason=value-rate-limit-exceeded,role=leader",
		"testScope.errors+reason=value-rate-limit-exceeded,role=non-leader",
		"testScope.errors+reason=new-metric-rate-limit-exceeded,role=leader",
		"testScope.errors+reason=new-metric-rate-limit-exceeded,role=non-leader",
		"testScope.errors+reason=arrived-too-late,role=leader",
		"testScope.errors+reason=arrived-too-late,role=non-leader",
		"testScope.errors+reason=aggregation-closed,role=leader",
		"testScope.errors+reason=aggregation-closed,role=non-leader",
		"testScope.errors+reason=not-categorized,role=leader",
		"testScope.errors+reason=not-categorized,role=non-leader",
	}
	require.Len(t, counters, len(expectedCounters))
	for _, id := range expectedCounters {
		c, exists := counters[id]
		require.True(t, exists)
		require.Equal(t, int64(1), c.Value())
	}

	// Validate we record times correctly.
	require.Equal(t, 1, len(timers))

	for _, id := range []string{
		"testScope.success-latency+",
	} {
		_, exists := timers[id]
		require.True(t, exists)
	}

	// Validate we do not have any gauges.
	require.Equal(t, 0, len(gauges))
}

func TestAggregatorAddTimedMetrics(t *testing.T) {
	s := tally.NewTestScope("testScope", nil)
	m := newAggregatorAddTimedMetrics(s, instrument.TimerOptions{})
	m.ReportSuccess()
	m.SuccessLatencyStopwatch().Stop()
	log := zap.NewNop()
	for _, state := range []ElectionState{LeaderState, FollowerState} {
		m.ReportError(errShardNotOwned, state, log)
		m.ReportError(errAggregatorShardNotWriteable, state, log)
		m.ReportError(errWriteNewMetricRateLimitExceeded, state, log)
		m.ReportError(errWriteValueRateLimitExceeded, state, log)
		m.ReportError(errTooFarInTheFuture, state, log)
		m.ReportError(errTooFarInThePast, state, log)
		m.ReportError(xerrors.NewRenamedError(errArrivedTooLate, errors.New("errorrr")), state, log)
		m.ReportError(errAggregationClosed, state, log)
		m.ReportError(errors.New("foo"), state, log)
	}

	snapshot := s.Snapshot()
	counters, timers, gauges := snapshot.Counters(), snapshot.Timers(), snapshot.Gauges()

	// Validate we count successes and errors correctly.
	expectedCounters := []string{
		"testScope.success+",
		"testScope.errors+reason=shard-not-owned,role=leader",
		"testScope.errors+reason=shard-not-owned,role=non-leader",
		"testScope.errors+reason=shard-not-writeable,role=leader",
		"testScope.errors+reason=shard-not-writeable,role=non-leader",
		"testScope.errors+reason=value-rate-limit-exceeded,role=leader",
		"testScope.errors+reason=value-rate-limit-exceeded,role=non-leader",
		"testScope.errors+reason=new-metric-rate-limit-exceeded,role=leader",
		"testScope.errors+reason=new-metric-rate-limit-exceeded,role=non-leader",
		"testScope.errors+reason=too-far-in-the-future,role=leader",
		"testScope.errors+reason=too-far-in-the-future,role=non-leader",
		"testScope.errors+reason=too-far-in-the-past,role=leader",
		"testScope.errors+reason=too-far-in-the-past,role=non-leader",
		"testScope.errors+reason=arrived-too-late,role=leader",
		"testScope.errors+reason=arrived-too-late,role=non-leader",
		"testScope.errors+reason=aggregation-closed,role=leader",
		"testScope.errors+reason=aggregation-closed,role=non-leader",
		"testScope.errors+reason=not-categorized,role=leader",
		"testScope.errors+reason=not-categorized,role=non-leader",
	}
	require.Len(t, counters, len(expectedCounters))
	for _, id := range expectedCounters {
		c, exists := counters[id]
		require.True(t, exists)
		require.Equal(t, int64(1), c.Value())
	}

	// Validate we record times correctly.
	require.Equal(t, 1, len(timers))

	for _, id := range []string{
		"testScope.success-latency+",
	} {
		_, exists := timers[id]
		require.True(t, exists)
	}

	// Validate we do not have any gauges.
	require.Equal(t, 0, len(gauges))
}

func TestAggregatorAddPassthroughMetrics(t *testing.T) {
	s := tally.NewTestScope("testScope", nil)
	m := newAggregatorAddPassthroughMetrics(s, instrument.TimerOptions{})
	m.ReportSuccess()
	m.SuccessLatencyStopwatch().Stop()
	m.ReportFollowerNoop()
	log := zap.NewNop()
	for _, state := range []ElectionState{LeaderState, FollowerState} {
		m.ReportError(errShardNotOwned, state, log)
		m.ReportError(errAggregatorShardNotWriteable, state, log)
		m.ReportError(errWriteNewMetricRateLimitExceeded, state, log)
		m.ReportError(errWriteValueRateLimitExceeded, state, log)
		m.ReportError(xerrors.NewRenamedError(errArrivedTooLate, errors.New("errorrr")), state, log)
		m.ReportError(errAggregationClosed, state, log)
		m.ReportError(errors.New("foo"), state, log)
	}

	snapshot := s.Snapshot()
	counters, timers, gauges := snapshot.Counters(), snapshot.Timers(), snapshot.Gauges()

	// Validate we count successes and errors correctly.
	expectedCounters := []string{
		"testScope.success+",
		"testScope.follower-noop+",
		"testScope.errors+reason=shard-not-owned,role=leader",
		"testScope.errors+reason=shard-not-owned,role=non-leader",
		"testScope.errors+reason=shard-not-writeable,role=leader",
		"testScope.errors+reason=shard-not-writeable,role=non-leader",
		"testScope.errors+reason=value-rate-limit-exceeded,role=leader",
		"testScope.errors+reason=value-rate-limit-exceeded,role=non-leader",
		"testScope.errors+reason=new-metric-rate-limit-exceeded,role=leader",
		"testScope.errors+reason=new-metric-rate-limit-exceeded,role=non-leader",
		"testScope.errors+reason=arrived-too-late,role=leader",
		"testScope.errors+reason=arrived-too-late,role=non-leader",
		"testScope.errors+reason=aggregation-closed,role=leader",
		"testScope.errors+reason=aggregation-closed,role=non-leader",
		"testScope.errors+reason=not-categorized,role=leader",
		"testScope.errors+reason=not-categorized,role=non-leader",
	}
	require.Len(t, counters, len(expectedCounters))
	for _, id := range expectedCounters {
		c, exists := counters[id]
		require.True(t, exists)
		require.Equal(t, int64(1), c.Value())
	}

	// Validate we record times correctly.
	require.Equal(t, 1, len(timers))

	for _, id := range []string{
		"testScope.success-latency+",
	} {
		_, exists := timers[id]
		require.True(t, exists)
	}

	// Validate we do not have any gauges.
	require.Equal(t, 0, len(gauges))
}

func TestAggregatorAddForwardedMetrics(t *testing.T) {
	s := tally.NewTestScope("testScope", nil)
	delayFunc := func(resolution time.Duration, numForwardedTimes int) time.Duration {
		return time.Second
	}
	m := newAggregatorAddForwardedMetrics(s, instrument.TimerOptions{}, delayFunc)
	m.ReportSuccess()
	m.SuccessLatencyStopwatch().Stop()
	m.ReportForwardingLatency(time.Second, 1, 100*time.Millisecond)
	m.ReportForwardingLatency(time.Second, 2, 100*time.Millisecond)
	log := zap.NewNop()
	for _, state := range []ElectionState{LeaderState, FollowerState} {
		m.ReportError(errShardNotOwned, state, log)
		m.ReportError(errAggregatorShardNotWriteable, state, log)
		m.ReportError(errWriteNewMetricRateLimitExceeded, state, log)
		m.ReportError(errWriteValueRateLimitExceeded, state, log)
		m.ReportError(xerrors.NewRenamedError(errArrivedTooLate, errors.New("errorrr")), state, log)
		m.ReportError(errAggregationClosed, state, log)
		m.ReportError(errors.New("foo"), state, log)
	}

	var (
		snapshot   = s.Snapshot()
		counters   = snapshot.Counters()
		timers     = snapshot.Timers()
		gauges     = snapshot.Gauges()
		histograms = snapshot.Histograms()
	)

	// Validate we count successes and errors correctly.
	expectedCounters := []string{
		"testScope.success+",
		"testScope.errors+reason=shard-not-owned,role=leader",
		"testScope.errors+reason=shard-not-owned,role=non-leader",
		"testScope.errors+reason=shard-not-writeable,role=leader",
		"testScope.errors+reason=shard-not-writeable,role=non-leader",
		"testScope.errors+reason=value-rate-limit-exceeded,role=leader",
		"testScope.errors+reason=value-rate-limit-exceeded,role=non-leader",
		"testScope.errors+reason=new-metric-rate-limit-exceeded,role=leader",
		"testScope.errors+reason=new-metric-rate-limit-exceeded,role=non-leader",
		"testScope.errors+reason=arrived-too-late,role=leader",
		"testScope.errors+reason=arrived-too-late,role=non-leader",
		"testScope.errors+reason=aggregation-closed,role=leader",
		"testScope.errors+reason=aggregation-closed,role=non-leader",
		"testScope.errors+reason=not-categorized,role=leader",
		"testScope.errors+reason=not-categorized,role=non-leader",
	}
	require.Len(t, counters, len(expectedCounters))
	for _, id := range expectedCounters {
		c, exists := counters[id]
		require.True(t, exists)
		require.Equal(t, int64(1), c.Value())
	}

	// Validate we record times correctly.
	require.Equal(t, 1, len(timers))

	for _, id := range []string{
		"testScope.success-latency+",
	} {
		_, exists := timers[id]
		require.True(t, exists)
	}

	// Validate we do not have any gauges.
	require.Equal(t, 0, len(gauges))

	// Validate we measure histograms correctly.
	expectedHistograms := []string{
		"testScope.forwarding-latency+bucket-version=2,num-forwarded-times=1,resolution=1s",
		"testScope.forwarding-latency+bucket-version=2,num-forwarded-times=2,resolution=1s",
	}
	require.Len(t, histograms, len(expectedHistograms))
	for _, id := range expectedHistograms {
		h, exists := histograms[id]
		require.True(t, exists, "missing series id %v", id)
		h.Durations()[100*time.Millisecond] = 1
	}
}

func TestAggregatorUpdateShardsIgnoreCutoffCutover(t *testing.T) {
	testAggregatorUpdateShards(t, true)
}

func TestAggregatorUpdateShardsRespectCutoffCutover(t *testing.T) {
	testAggregatorUpdateShards(t, false)
}

func testAggregatorUpdateShards(t *testing.T, ignoreCutoffCutover bool) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		now     = time.Now().Truncate(time.Hour)
		cutover = now.Add(-time.Hour)
		cutoff  = now.Add(time.Hour)
	)

	shard := shard.NewShard(0).
		SetState(shard.Initializing).
		SetCutoverNanos(cutover.UnixNano()).
		SetCutoffNanos(cutoff.UnixNano())

	placement, shards := testPlacementWithCustomShards(testInstanceID, 1, shard)

	agg, _ := testAggregator(t, ctrl)
	opts := agg.opts

	agg.opts = opts.SetWritesIgnoreCutoffCutover(ignoreCutoffCutover)
	agg.updateShardsWithLock(placement, shards)

	expectedEarliest, expectedLatest := int64(0), int64(math.MaxInt64)
	if !ignoreCutoffCutover {
		expectedEarliest = cutover.Add(-opts.BufferDurationBeforeShardCutover()).UnixNano()
		expectedLatest = cutoff.Add(opts.BufferDurationAfterShardCutoff()).UnixNano()
	}

	aggShard := agg.shards[0]
	assert.Equal(t, expectedEarliest, aggShard.earliestWritableNanos)
	assert.Equal(t, expectedLatest, aggShard.latestWriteableNanos)
}

func testAddWithShardRedirect(t *testing.T, addFn func(*aggregator) error) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, _ := testAggregatorWithShardRedirect(t, ctrl, 3, 1)
	require.NoError(t, agg.Open())
	agg.shardFn = func([]byte, uint32) uint32 { return 3 }
	err := addFn(agg)
	require.NoError(t, err)
	require.Equal(t, 1, len(agg.shards[1].metricMap.entries))
}

func testAddWithShardRedirectToNotOwned(t *testing.T, addFn func(*aggregator) error) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, _ := testAggregatorWithShardRedirect(t, ctrl, 3, 100)
	require.NoError(t, agg.Open())
	agg.shardFn = func([]byte, uint32) uint32 { return 3 }
	err := addFn(agg)
	require.Equal(t, errShardNotOwned, err)
}

func testAggregator(t *testing.T, ctrl *gomock.Controller) (*aggregator, kv.Store) {
	proto := testStagedPlacementProtoWithNumShards(t, testInstanceID, testShardSetID, testNumShards)
	return testAggregatorWithCustomPlacements(t, ctrl, proto)
}

func testAggregatorWithShardRedirect(
	t *testing.T,
	ctrl *gomock.Controller,
	fromShard, toShard uint32,
) (*aggregator, kv.Store) {
	proto := testStagedPlacementProtoWithNumShards(t, testInstanceID, testShardSetID, testNumShards)
	shards := proto.Snapshots[0].Instances[testInstanceID].Shards
	shards[fromShard].RedirectToShardId = &types.UInt32Value{Value: toShard}

	return testAggregatorWithCustomPlacements(t, ctrl, proto)
}

func testAggregatorWithCustomPlacements(
	t *testing.T,
	ctrl *gomock.Controller,
	proto *placementpb.PlacementSnapshots,
) (*aggregator, kv.Store) {
	watcherOpts, store := testWatcherOptsWithPlacementProto(t, testPlacementKey, proto)
	placementManagerOpts := NewPlacementManagerOptions().
		SetInstanceID(testInstanceID).
		SetWatcherOptions(watcherOpts)
	placementManager := NewPlacementManager(placementManagerOpts)
	opts := testOptions(ctrl).
		SetEntryCheckInterval(0).
		SetPlacementManager(placementManager)
	return NewAggregator(opts).(*aggregator), store
}

// nolint: unparam
func testWatcherOptsWithPlacementProto(
	t *testing.T,
	placementKey string,
	proto *placementpb.PlacementSnapshots,
) (placement.WatcherOptions, kv.Store) {
	t.Helper()
	store := mem.NewStore()
	_, err := store.SetIfNotExists(placementKey, proto)
	require.NoError(t, err)
	placementWatcherOpts := placement.NewWatcherOptions().
		SetStagedPlacementKey(placementKey).
		SetStagedPlacementStore(store)
	return placementWatcherOpts, store
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

func testPlacementWithCustomShards(
	instanceID string,
	shardSetID uint32,
	customShards ...shard.Shard,
) (placement.Placement, shard.Shards) {
	shards := shard.NewShards(customShards)
	instance := placement.NewInstance().
		SetID(instanceID).
		SetShards(shards).
		SetShardSetID(shardSetID)
	placement := placement.NewPlacement().
		SetInstances([]placement.Instance{instance}).
		SetShards(shards.AllIDs())

	return placement, shards
}

func testStagedPlacementProtoWithCustomShards(
	t *testing.T,
	instanceID string,
	shardSetID uint32,
	shardSet []shard.Shard,
	placementCutoverNanos int64,
) *placementpb.PlacementSnapshots {
	testPlacement, _ := testPlacementWithCustomShards(instanceID, shardSetID, shardSet...)
	testPlacement.SetCutoverNanos(placementCutoverNanos)
	testStagedPlacement, err := placement.NewPlacementsFromLatest(testPlacement)
	require.NoError(t, err)
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
	electionMgr.EXPECT().ElectionState().Return(LeaderState).AnyTimes()

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
	return newTestOptions().
		SetPlacementManager(placementManager).
		SetFlushTimesManager(flushTimesManager).
		SetElectionManager(electionMgr).
		SetFlushManager(flushManager).
		SetFlushHandler(h).
		SetPassthroughWriter(pw).
		SetAdminClient(cl).
		SetMaxAllowedForwardingDelayFn(infiniteAllowedDelayFn).
		SetBufferForFutureTimedMetric(math.MaxInt64).
		SetBufferForPastTimedMetricFn(infiniteBufferForPastTimedMetricFn)
}

type uint32Ascending []uint32

func (a uint32Ascending) Len() int           { return len(a) }
func (a uint32Ascending) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a uint32Ascending) Less(i, j int) bool { return a[i] < a[j] }
