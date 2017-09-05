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
	"context"
	"errors"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3cluster/generated/proto/placementpb"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
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
	testShardSetID  = uint32(0)
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
	require.Equal(t, errAggregatorAlreadyOpenOrClosed, agg.Open())
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
	require.Equal(t, errAggregatorNotOpenOrClosed, err)
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
	now := time.Unix(0, 12345)
	nowFn := func() time.Time { return now }
	agg.opts = agg.opts.
		SetClockOptions(agg.opts.ClockOptions().SetNowFn(nowFn)).
		SetBufferDurationBeforeShardCutover(time.Duration(500)).
		SetBufferDurationAfterShardCutoff(time.Duration(1000))
	require.NoError(t, agg.Open())
	agg.shardFn = func([]byte, int) uint32 { return 1 }

	newShardAssignment := []shard.Shard{
		shard.NewShard(0).SetState(shard.Initializing).SetCutoverNanos(5000).SetCutoffNanos(20000),
		shard.NewShard(1).SetState(shard.Initializing).SetCutoverNanos(5500).SetCutoffNanos(25000),
		shard.NewShard(2).SetState(shard.Initializing).SetCutoverNanos(6000).SetCutoffNanos(30000),
		shard.NewShard(4).SetState(shard.Initializing).SetCutoverNanos(6500).SetCutoffNanos(math.MaxInt64),
	}
	newStagedPlacementProto := testStagedPlacementProtoWithCustomShards(t, testInstanceID, newShardAssignment, 5678)
	_, err := store.Set(testPlacementKey, newStagedPlacementProto)
	require.NoError(t, err)

	// Wait for the placement to be updated.
	for {
		placement, err := agg.placement()
		require.NoError(t, err)
		if placement.CutoverNanos() == 5678 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	existingShard := agg.shards[3]
	err = agg.AddMetricWithPoliciesList(testValidMetric, testPoliciesList)
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
	errTestResign := errors.New("test resign")
	agg, _ := testAggregator(t)
	agg.electionManager = &mockElectionManager{
		resignFn: func(context.Context) error { return errTestResign },
	}
	require.Equal(t, errTestResign, agg.Resign())
}

func TestAggregatorResignSuccess(t *testing.T) {
	agg, _ := testAggregator(t)
	agg.electionManager = &mockElectionManager{
		resignFn: func(context.Context) error { return nil },
	}
	require.NoError(t, agg.Resign())
}

func TestAggregatorStatus(t *testing.T) {
	flushStatus := FlushStatus{
		ElectionState: LeaderState,
		CanLead:       true,
	}
	agg, _ := testAggregator(t)
	agg.flushManager = &mockFlushManager{
		statusFn: func() FlushStatus {
			return flushStatus
		},
	}
	require.Equal(t, RuntimeStatus{FlushStatus: flushStatus}, agg.Status())
}

func TestAggregatorCloseAlreadyClosed(t *testing.T) {
	agg, _ := testAggregator(t)
	require.Equal(t, errAggregatorNotOpenOrClosed, agg.Close())
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

func TestAggregatorOwnedShards(t *testing.T) {
	agg, _ := testAggregator(t)
	now := time.Unix(0, 12345)
	nowFn := func() time.Time { return now }
	agg.nowFn = nowFn
	agg.opts = agg.opts.SetClockOptions(agg.opts.ClockOptions().SetNowFn(nowFn))

	shardCutoffNanos := []int64{math.MaxInt64, 1234, 56789, 8901}
	agg.shardIDs = make([]uint32, 0, len(shardCutoffNanos))
	agg.shards = make([]*aggregatorShard, 0, len(shardCutoffNanos))
	for i, cutoffNanos := range shardCutoffNanos {
		shardID := uint32(i)
		shard := newAggregatorShard(shardID, agg.opts)
		shard.latestWriteableNanos = cutoffNanos
		agg.shardIDs = append(agg.shardIDs, shardID)
		agg.shards = append(agg.shards, shard)
	}

	expectedOwned := []*aggregatorShard{agg.shards[0], agg.shards[2]}
	expectedToClose := []*aggregatorShard{agg.shards[1], agg.shards[3]}
	owned, toClose := agg.ownedShards()
	require.Equal(t, expectedOwned, owned)
	require.Equal(t, expectedToClose, toClose)

	expectedShardIDs := []uint32{0, 2}
	shardIDs := make([]uint32, len(agg.shardIDs))
	copy(shardIDs, agg.shardIDs)
	sort.Sort(uint32Ascending(shardIDs))
	require.Equal(t, expectedShardIDs, agg.shardIDs)

	for i := 0; i < 4; i++ {
		if i == 0 || i == 2 {
			require.NotNil(t, agg.shards[i])
		} else {
			require.Nil(t, agg.shards[i])
		}
	}
}

func testAggregator(t *testing.T) (*aggregator, kv.Store) {
	watcher, store := testPlacementWatcherWithNumShards(t, testInstanceID, testNumShards, testPlacementKey)
	opts := testOptions().
		SetEntryCheckInterval(0).
		SetInstanceID(testInstanceID).
		SetStagedPlacementWatcher(watcher)
	return NewAggregator(opts).(*aggregator), store
}

// nolint: unparam
func testPlacementWatcherWithNumShards(
	t *testing.T,
	instanceID string,
	numShards int,
	placementKey string,
) (placement.StagedPlacementWatcher, kv.Store) {
	proto := testStagedPlacementProtoWithNumShards(t, instanceID, numShards)
	return testPlacementWatcherWithPlacementProto(t, placementKey, proto)
}

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
	require.NoError(t, placementWatcher.Watch())
	return placementWatcher, store
}

// nolint: unparam
func testStagedPlacementProtoWithNumShards(
	t *testing.T,
	instanceID string,
	numShards int,
) *placementpb.PlacementSnapshots {
	shardSet := make([]shard.Shard, numShards)
	for i := 0; i < numShards; i++ {
		shardSet[i] = shard.NewShard(uint32(i)).
			SetState(shard.Initializing).
			SetCutoverNanos(0).
			SetCutoffNanos(math.MaxInt64)
	}
	return testStagedPlacementProtoWithCustomShards(t, instanceID, shardSet, testPlacementCutover)
}

func testStagedPlacementProtoWithCustomShards(
	t *testing.T,
	instanceID string,
	shardSet []shard.Shard,
	placementCutoverNanos int64,
) *placementpb.PlacementSnapshots {
	shards := shard.NewShards(shardSet)
	instance := placement.NewInstance().
		SetID(instanceID).
		SetShards(shards)
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

func testOptions() Options {
	electionManager := &mockElectionManager{
		openFn: func(shardSetID uint32) error { return nil },
	}
	return NewOptions().
		SetElectionManager(electionManager).
		SetFlushManager(&mockFlushManager{
			registerFn:   func(flusher PeriodicFlusher) error { return nil },
			unregisterFn: func(flusher PeriodicFlusher) error { return nil },
		}).
		SetFlushHandler(&mockHandler{
			handleFn: func(buf *RefCountedBuffer) error {
				buf.DecRef()
				return nil
			},
		})
}

type registerFn func(flusher PeriodicFlusher) error
type unregisterFn func(flusher PeriodicFlusher) error
type statusFn func() FlushStatus

type mockFlushManager struct {
	registerFn   registerFn
	unregisterFn unregisterFn
	statusFn     statusFn
}

func (mgr *mockFlushManager) Open(shardSetID uint32) error { return nil }

func (mgr *mockFlushManager) Register(flusher PeriodicFlusher) error {
	return mgr.registerFn(flusher)
}

func (mgr *mockFlushManager) Unregister(flusher PeriodicFlusher) error {
	return mgr.unregisterFn(flusher)
}

func (mgr *mockFlushManager) Status() FlushStatus { return mgr.statusFn() }

func (mgr *mockFlushManager) Close() error { return nil }

type uint32Ascending []uint32

func (a uint32Ascending) Len() int           { return len(a) }
func (a uint32Ascending) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a uint32Ascending) Less(i, j int) bool { return a[i] < a[j] }
