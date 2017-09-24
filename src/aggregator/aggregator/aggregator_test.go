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

	schema "github.com/m3db/m3aggregator/generated/proto/flush"
	"github.com/m3db/m3cluster/generated/proto/placementpb"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3metrics/metric/aggregated"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	xtime "github.com/m3db/m3x/time"

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

func TestAggregatorOpenPlacementManagerOpenError(t *testing.T) {
	errPlacementManagerOpen := errors.New("error opening placement manager")
	agg, _ := testAggregator(t)
	agg.placementManager = &mockPlacementManager{
		openFn: func() error { return errPlacementManagerOpen },
	}
	require.Equal(t, errPlacementManagerOpen, agg.Open())
}

func TestAggregatorOpenPlacementError(t *testing.T) {
	errPlacement := errors.New("error getting placement")
	agg, _ := testAggregator(t)
	agg.placementManager = &mockPlacementManager{
		openFn:      func() error { return nil },
		placementFn: func() (placement.Placement, error) { return nil, errPlacement },
	}
	require.Equal(t, errPlacement, agg.Open())
}

func TestAggregatorOpenInstanceFromError(t *testing.T) {
	testPlacement := placement.NewPlacement().SetCutoverNanos(5678)
	errInstanceFrom := errors.New("error getting instance from placement")
	agg, _ := testAggregator(t)
	agg.placementManager = &mockPlacementManager{
		openFn:      func() error { return nil },
		placementFn: func() (placement.Placement, error) { return testPlacement, nil },
		instanceFromFn: func(placement.Placement) (placement.Instance, error) {
			return nil, errInstanceFrom
		},
	}
	require.Equal(t, errInstanceFrom, agg.Open())
}

func TestAggregatorOpenInstanceNotInPlacement(t *testing.T) {
	testPlacement := placement.NewPlacement().SetCutoverNanos(5678)
	agg, _ := testAggregator(t)
	agg.placementManager = &mockPlacementManager{
		openFn:      func() error { return nil },
		placementFn: func() (placement.Placement, error) { return testPlacement, nil },
		instanceFromFn: func(placement.Placement) (placement.Instance, error) {
			return nil, ErrInstanceNotFoundInPlacement
		},
	}
	require.NoError(t, agg.Open())
	require.Equal(t, uint32(0), agg.shardSetID)
	require.False(t, agg.shardSetOpen)
	require.Equal(t, 0, len(agg.shardIDs))
	require.Nil(t, agg.shards)
	require.Equal(t, int64(5678), agg.placementCutoverNanos)
	require.Equal(t, aggregatorOpen, agg.state)
}

func TestAggregatorOpenSuccess(t *testing.T) {
	agg, _ := testAggregator(t)
	require.NoError(t, agg.Open())
	require.Equal(t, testShardSetID, agg.shardSetID)
	require.True(t, agg.shardSetOpen)
	require.Equal(t, aggregatorOpen, agg.state)
	require.Equal(t, []uint32{0, 1, 2, 3}, agg.shardIDs)
	for i := 0; i < testNumShards; i++ {
		require.NotNil(t, agg.shards[i])
	}
	require.Equal(t, int64(testPlacementCutover), agg.placementCutoverNanos)
}

func TestAggregatorInstanceNotFoundThenFoundThenNotFound(t *testing.T) {
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
	agg, store := testAggregatorWithCustomPlacements(t, placements[0])
	require.NoError(t, agg.Open())
	require.Equal(t, uint32(0), agg.shardSetID)
	require.False(t, agg.shardSetOpen)
	require.Equal(t, 0, len(agg.shardIDs))
	require.Nil(t, agg.shards)
	require.Equal(t, int64(0), agg.placementCutoverNanos)
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
	agg.shardFn = func([]byte, int) uint32 { return 1 }
	require.NoError(t, agg.AddMetricWithPoliciesList(testValidMetric, testPoliciesList))
	require.Equal(t, testShardSetID, agg.shardSetID)
	require.True(t, agg.shardSetOpen)
	require.Equal(t, aggregatorOpen, agg.state)
	require.Equal(t, []uint32{0, 1, 2, 3}, agg.shardIDs)
	for i := 0; i < testNumShards; i++ {
		require.NotNil(t, agg.shards[i])
	}
	require.Equal(t, int64(testPlacementCutover), agg.placementCutoverNanos)

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
	require.Error(t, agg.AddMetricWithPoliciesList(testValidMetric, testPoliciesList))
	require.Equal(t, uint32(0), agg.shardSetID)
	require.False(t, agg.shardSetOpen)
	require.Equal(t, 0, len(agg.shardIDs))
	require.Nil(t, agg.shards)
	require.Equal(t, int64(testPlacementCutover+1000), agg.placementCutoverNanos)
	require.Equal(t, aggregatorOpen, agg.state)
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
	newStagedPlacementProto := testStagedPlacementProtoWithCustomShards(t, testInstanceID, testShardSetID, newShardAssignment, 5678)
	_, err := store.Set(testPlacementKey, newStagedPlacementProto)
	require.NoError(t, err)

	// Wait for the placement to be updated.
	for {
		placement, err := agg.placementManager.Placement()
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
	agg.flushTimesManager = &mockFlushTimesManager{
		openShardSetIDFn: func(shardSetID uint32) error { return nil },
		getFlushTimesFn:  func() (*schema.ShardSetFlushTimes, error) { return nil, nil },
	}
	require.NoError(t, agg.Open())

	// Forcing a tick.
	agg.tickInternal()

	require.NoError(t, agg.Close())
}

func TestAggregatorShardSetNotOpenNilInstance(t *testing.T) {
	agg, _ := testAggregator(t)
	agg.shardSetOpen = false
	agg.Lock()
	defer agg.Unlock()
	require.NoError(t, agg.updateShardSetIDWithLock(nil))
}

func TestAggregatorShardSetNotOpenValidInstance(t *testing.T) {
	var (
		flushTimesManagerOpenID *uint32
		electionManagerOpenID   *uint32
		testInstance            = placement.NewInstance().SetShardSetID(testShardSetID)
	)
	agg, _ := testAggregator(t)
	agg.shardSetOpen = false
	agg.flushTimesManager = &mockFlushTimesManager{
		openShardSetIDFn: func(shardSetID uint32) error {
			flushTimesManagerOpenID = &shardSetID
			return nil
		},
	}
	agg.electionManager = &mockElectionManager{
		openFn: func(shardSetID uint32) error {
			electionManagerOpenID = &shardSetID
			return nil
		},
	}
	agg.Lock()
	defer agg.Unlock()
	require.NoError(t, agg.updateShardSetIDWithLock(testInstance))
	require.Equal(t, testShardSetID, agg.shardSetID)
	require.True(t, agg.shardSetOpen)
	require.Equal(t, testShardSetID, *flushTimesManagerOpenID)
	require.Equal(t, testShardSetID, *electionManagerOpenID)
}

func TestAggregatorShardSetOpenShardSetIDUnchanged(t *testing.T) {
	testInstance := placement.NewInstance().SetShardSetID(testShardSetID)
	agg, _ := testAggregator(t)
	agg.shardSetOpen = true
	agg.shardSetID = testShardSetID
	agg.Lock()
	defer agg.Unlock()
	require.NoError(t, agg.updateShardSetIDWithLock(testInstance))
	require.Equal(t, testShardSetID, agg.shardSetID)
	require.True(t, agg.shardSetOpen)
}

func TestAggregatorShardSetOpenNilInstance(t *testing.T) {
	agg, _ := testAggregator(t)
	agg.shardSetOpen = true
	agg.shardSetID = testShardSetID
	agg.Lock()
	defer agg.Unlock()
	require.NoError(t, agg.updateShardSetIDWithLock(nil))
	require.Equal(t, uint32(0), agg.shardSetID)
	require.False(t, agg.shardSetOpen)
}

func TestAggregatorShardSetOpenValidInstance(t *testing.T) {
	var (
		flushTimesManagerOpenID *uint32
		electionManagerOpenID   *uint32
		newShardSetID           = uint32(2)
		testInstance            = placement.NewInstance().SetShardSetID(newShardSetID)
	)
	agg, _ := testAggregator(t)
	agg.shardSetOpen = true
	agg.shardSetID = testShardSetID
	agg.flushTimesManager = &mockFlushTimesManager{
		openShardSetIDFn: func(shardSetID uint32) error {
			flushTimesManagerOpenID = &shardSetID
			return nil
		},
	}
	agg.electionManager = &mockElectionManager{
		openFn: func(shardSetID uint32) error {
			electionManagerOpenID = &shardSetID
			return nil
		},
	}
	agg.Lock()
	defer agg.Unlock()
	require.NoError(t, agg.updateShardSetIDWithLock(testInstance))
	require.Equal(t, uint32(2), agg.shardSetID)
	require.True(t, agg.shardSetOpen)
	require.Equal(t, newShardSetID, *flushTimesManagerOpenID)
	require.Equal(t, newShardSetID, *electionManagerOpenID)
}

func TestAggregatorOwnedShards(t *testing.T) {
	agg, _ := testAggregator(t)
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
			ByResolution: map[int64]int64{
				int64(time.Second): input.flushedNanos,
			},
		}
		agg.shardIDs = append(agg.shardIDs, shardID)
		agg.shards = append(agg.shards, shard)
	}
	agg.flushTimesManager = &mockFlushTimesManager{
		openShardSetIDFn: func(shardSetID uint32) error { return nil },
		getFlushTimesFn: func() (*schema.ShardSetFlushTimes, error) {
			return &schema.ShardSetFlushTimes{ByShard: flushTimes}, nil
		},
	}

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

func testAggregator(t *testing.T) (*aggregator, kv.Store) {
	proto := testStagedPlacementProtoWithNumShards(t, testInstanceID, testShardSetID, testNumShards)
	return testAggregatorWithCustomPlacements(t, proto)
}

func testAggregatorWithCustomPlacements(
	t *testing.T,
	proto *placementpb.PlacementSnapshots,
) (*aggregator, kv.Store) {
	watcher, store := testPlacementWatcherWithPlacementProto(t, testPlacementKey, proto)
	placementManagerOpts := NewPlacementManagerOptions().
		SetInstanceID(testInstanceID).
		SetStagedPlacementWatcher(watcher)
	placementManager := NewPlacementManager(placementManagerOpts)
	opts := testOptions().
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

func testOptions() Options {
	return NewOptions().
		SetPlacementManager(&mockPlacementManager{
			openFn: func() error { return nil },
		}).
		SetFlushTimesManager(&mockFlushTimesManager{
			openShardSetIDFn: func(shardSetID uint32) error { return nil },
		}).
		SetElectionManager(&mockElectionManager{
			openFn: func(shardSetID uint32) error { return nil },
		}).
		SetFlushManager(&mockFlushManager{
			registerFn:   func(flusher PeriodicFlusher) error { return nil },
			unregisterFn: func(flusher PeriodicFlusher) error { return nil },
		}).
		SetFlushHandler(&mockHandler{
			newWriterFn: func(tally.Scope) (Writer, error) {
				return &mockWriter{
					writeFn: func(mp aggregated.ChunkedMetricWithStoragePolicy) error { return nil },
					flushFn: func() error { return nil },
				}, nil
			},
		})
}

type uint32Ascending []uint32

func (a uint32Ascending) Len() int           { return len(a) }
func (a uint32Ascending) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a uint32Ascending) Less(i, j int) bool { return a[i] < a[j] }
