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
	"testing"
	"time"

	placementproto "github.com/m3db/m3cluster/generated/proto/placement"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3cluster/proto/util"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

const (
	testInstanceID       = "localhost:0"
	testPlacementKey     = "placement"
	testNumShards        = 4
	testPlacementCutover = 1234
)

var (
	testValidMetric = unaggregated.MetricUnion{
		Type:       unaggregated.CounterType,
		ID:         []byte("foo"),
		CounterVal: 1234,
	}
	testInvalidMetric = unaggregated.MetricUnion{
		Type: unaggregated.UnknownType,
		ID:   []byte("testInvalid"),
	}
	testPoliciesList = policy.PoliciesList{
		policy.NewStagedPolicies(123, false, []policy.Policy{
			policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
			policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 2*24*time.Hour), policy.DefaultAggregationID),
		}),
		policy.NewStagedPolicies(456, true, nil),
	}
)

func TestAggregatorOpenAlreadyOpen(t *testing.T) {
	agg, _ := testAggregator(t)
	agg.state = aggregatorOpen
	require.Equal(t, errAggregatorIsOpenOrClosed, agg.Open())
}

func TestAggregatorOpenSuccess(t *testing.T) {
	agg, _ := testAggregator(t)
	require.NoError(t, agg.Open())
	require.Equal(t, aggregatorOpen, agg.state)
	require.Equal(t, []uint32{0, 1, 2, 3}, agg.shardIDs)
	for i := 0; i < testNumShards; i++ {
		require.NotNil(t, agg.shards[i])
	}
	require.Equal(t, int64(testPlacementCutover), agg.placementCutoverNanos)
}

func TestAggregatorAddMetricWithPoliciesListInvalidMetricType(t *testing.T) {
	agg, _ := testAggregator(t)
	require.NoError(t, agg.Open())
	err := agg.AddMetricWithPoliciesList(testInvalidMetric, testPoliciesList)
	require.Equal(t, errInvalidMetricType, err)
}

func TestAggregatorAddMetricWithPoliciesListNotOpen(t *testing.T) {
	agg, _ := testAggregator(t)
	err := agg.AddMetricWithPoliciesList(testValidMetric, testPoliciesList)
	require.Equal(t, errAggregatorIsNotOpenOrClosed, err)
}

func TestAggregatorAddMetricWithPoliciesListPlacementWatcherUnwatched(t *testing.T) {
	agg, _ := testAggregator(t)
	require.NoError(t, agg.Open())

	require.NoError(t, agg.placementWatcher.Unwatch())
	err := agg.AddMetricWithPoliciesList(testValidMetric, testPoliciesList)
	require.Error(t, err)
}

func TestAggregatorAddMetricWithPoliciesListNotResponsibleForShard(t *testing.T) {
	agg, _ := testAggregator(t)
	require.NoError(t, agg.Open())
	agg.shardFn = func([]byte, int) uint32 { return testNumShards }
	err := agg.AddMetricWithPoliciesList(testValidMetric, testPoliciesList)
	require.Error(t, err)
}

func TestAggregatorAddMetricWithPoliciesListSuccessNoPlacementUpdate(t *testing.T) {
	agg, _ := testAggregator(t)
	require.NoError(t, agg.Open())
	agg.shardFn = func([]byte, int) uint32 { return 1 }
	err := agg.AddMetricWithPoliciesList(testValidMetric, testPoliciesList)
	require.NoError(t, err)
	require.Equal(t, 1, len(agg.shards[1].metricMap.entries))
}

func TestAggregatorAddMetricWithPoliciesListSuccessWithPlacementUpdate(t *testing.T) {
	agg, store := testAggregator(t)
	require.NoError(t, agg.Open())
	agg.shardFn = func([]byte, int) uint32 { return 1 }

	newShardAssignment := []shard.Shard{
		shard.NewShard(0).SetState(shard.Initializing),
		shard.NewShard(1).SetState(shard.Initializing),
		shard.NewShard(2).SetState(shard.Initializing),
		shard.NewShard(4).SetState(shard.Initializing),
	}
	newStagedPlacementProto := testStagedPlacementProtoWithCustomShards(t, newShardAssignment, 5678)
	_, err := store.Set(testPlacementKey, newStagedPlacementProto)
	require.NoError(t, err)

	// Wait for the placement to be updated.
	for {
		stagedPlacement, stagedPlacementDoneFn, err := agg.placementWatcher.ActiveStagedPlacement()
		require.NoError(t, err)
		placement, placementDoneFn, err := stagedPlacement.ActivePlacement()
		require.NoError(t, err)
		cutoverNanos := placement.CutoverNanos()
		placementDoneFn()
		stagedPlacementDoneFn()
		if cutoverNanos == 5678 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	existingShard := agg.shards[3]
	err = agg.AddMetricWithPoliciesList(testValidMetric, testPoliciesList)
	require.NoError(t, err)
	require.Equal(t, 5, len(agg.shards))
	for i := 0; i < 5; i++ {
		if i == 3 {
			require.Nil(t, agg.shards[i])
		} else {
			require.NotNil(t, agg.shards[i])
		}
	}
	require.Equal(t, 1, len(agg.shards[1].metricMap.entries))
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

func TestAggregatorCloseAlreadyClosed(t *testing.T) {
	agg, _ := testAggregator(t)
	require.Equal(t, errAggregatorIsNotOpenOrClosed, agg.Close())
}

func TestAggregatorCloseSuccess(t *testing.T) {
	agg, _ := testAggregator(t)
	require.NoError(t, agg.Open())
	require.NoError(t, agg.Close())
	require.Equal(t, aggregatorClosed, agg.state)
}

func TestAggregatorTick(t *testing.T) {
	agg, _ := testAggregator(t)
	require.NoError(t, agg.Open())

	// Forcing a tick.
	agg.tickInternal()

	require.NoError(t, agg.Close())
}

func testAggregator(t *testing.T) (*aggregator, kv.Store) {
	opts := testOptions().SetEntryCheckInterval(0)

	testStagedPlacementProto := testStagedPlacementProtoWithNumShards(t, testNumShards)
	testPlacementStore := mem.NewStore()
	_, err := testPlacementStore.SetIfNotExists(testPlacementKey, testStagedPlacementProto)
	require.NoError(t, err)
	placementWatcherOpts := placement.NewStagedPlacementWatcherOptions().
		SetStagedPlacementKey(testPlacementKey).
		SetStagedPlacementStore(testPlacementStore)
	opts = opts.SetInstanceID(testInstanceID).SetStagedPlacementWatcherOptions(placementWatcherOpts)

	return NewAggregator(opts).(*aggregator), testPlacementStore
}

func testStagedPlacementProtoWithNumShards(t *testing.T, numShards int) *placementproto.PlacementSnapshots {
	shardSet := make([]shard.Shard, numShards)
	for i := 0; i < numShards; i++ {
		shardSet[i] = shard.NewShard(uint32(i)).SetState(shard.Initializing)
	}
	return testStagedPlacementProtoWithCustomShards(t, shardSet, testPlacementCutover)
}

func testStagedPlacementProtoWithCustomShards(
	t *testing.T,
	shardSet []shard.Shard,
	cutoverNanos int64,
) *placementproto.PlacementSnapshots {
	shards := shard.NewShards(shardSet)
	instance := placement.NewInstance().
		SetID(testInstanceID).
		SetShards(shards)
	testPlacement := placement.NewPlacement().
		SetInstances([]services.PlacementInstance{instance}).
		SetShards(shards.AllIDs()).
		SetCutoverNanos(cutoverNanos)
	testStagedPlacement := placement.NewStagedPlacement().
		SetPlacements([]services.Placement{testPlacement})
	stagedPlacementProto, err := util.StagedPlacementToProto(testStagedPlacement)
	require.NoError(t, err)
	return stagedPlacementProto
}

func testOptions() Options {
	return NewOptions().
		SetFlushManager(&mockFlushManager{
			registerFn: func(flusher PeriodicFlusher) error { return nil },
		}).
		SetFlushHandler(&mockHandler{
			handleFn: func(msgpack.Buffer) error { return nil },
		})
}

type registerFn func(flusher PeriodicFlusher) error

type mockFlushManager struct {
	registerFn registerFn
}

func (mgr *mockFlushManager) Register(flusher PeriodicFlusher) error {
	return mgr.registerFn(flusher)
}

func (mgr *mockFlushManager) Close() {}
