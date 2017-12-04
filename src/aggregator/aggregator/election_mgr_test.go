// Copyright (c) 2017 Uber Technologies, Inc.
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
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	schema "github.com/m3db/m3aggregator/generated/proto/flush"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/leader/campaign"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3x/retry"

	"github.com/stretchr/testify/require"
)

func TestElectionStateJSONMarshal(t *testing.T) {
	for _, input := range []struct {
		state    ElectionState
		expected string
	}{
		{state: LeaderState, expected: `"leader"`},
		{state: FollowerState, expected: `"follower"`},
		{state: PendingFollowerState, expected: `"pendingFollower"`},
		{state: UnknownState, expected: `"unknown"`},
		{state: ElectionState(100), expected: `"unknown"`},
	} {
		b, err := json.Marshal(input.state)
		require.NoError(t, err)
		require.Equal(t, input.expected, string(b))
	}
}

func TestElectionStateJSONUnMarshal(t *testing.T) {
	for _, input := range []struct {
		str      string
		expected ElectionState
	}{
		{str: `"leader"`, expected: LeaderState},
		{str: `"follower"`, expected: FollowerState},
		{str: `"pendingFollower"`, expected: PendingFollowerState},
	} {
		var actual ElectionState
		require.NoError(t, json.Unmarshal([]byte(input.str), &actual))
		require.Equal(t, input.expected, actual)
	}
}

func TestElectionStateJSONUnMarshalError(t *testing.T) {
	var state ElectionState
	require.Error(t, json.Unmarshal([]byte(`"foo"`), &state))
}

func TestElectionStateJSONRoundtrip(t *testing.T) {
	for _, input := range []ElectionState{LeaderState, FollowerState, PendingFollowerState} {
		var actual ElectionState
		b, err := json.Marshal(input)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(b, &actual))
		require.Equal(t, input, actual)
	}
}

func TestElectionManagerReset(t *testing.T) {
	opts := testElectionManagerOptions(t)
	mgr := NewElectionManager(opts).(*electionManager)

	// Reseting an unopened manager is a no op.
	require.NoError(t, mgr.Reset())
	require.NoError(t, mgr.Open(testShardSetID))
	require.NoError(t, mgr.Close())

	// Opening a closed manager causes an error.
	require.Error(t, mgr.Open(testShardSetID))

	// Resetting the manager allows the manager to be reopened.
	require.NoError(t, mgr.Reset())
	require.NoError(t, mgr.Open(testShardSetID))
	require.NoError(t, mgr.Close())

	// Resetting an open manager causes an error.
	mgr.state = electionManagerOpen
	require.Equal(t, errElectionManagerOpen, mgr.Reset())
}

func TestElectionManagerOpenAlreadyOpen(t *testing.T) {
	opts := testElectionManagerOptions(t)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.state = electionManagerOpen
	require.Equal(t, errElectionManagerAlreadyOpenOrClosed, mgr.Open(testShardSetID))
}

func TestElectionManagerOpenSuccess(t *testing.T) {
	leaderService := &mockLeaderService{
		campaignFn: func(
			electionID string,
			opts services.CampaignOptions,
		) (<-chan campaign.Status, error) {
			return make(chan campaign.Status), nil
		},
	}
	opts := testElectionManagerOptions(t).SetLeaderService(leaderService)
	mgr := NewElectionManager(opts).(*electionManager)
	require.NoError(t, mgr.Open(testShardSetID))
	require.NoError(t, mgr.Close())
}

func TestElectionManagerElectionState(t *testing.T) {
	opts := testElectionManagerOptions(t)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.electionStateWatchable.Update(LeaderState)
	require.Equal(t, LeaderState, mgr.ElectionState())
}

func TestElectionManagerIsCampaigning(t *testing.T) {
	opts := testElectionManagerOptions(t)
	mgr := NewElectionManager(opts).(*electionManager)

	inputs := []struct {
		state    campaignState
		expected bool
	}{
		{state: campaignDisabled, expected: false},
		{state: campaignPendingDisabled, expected: false},
		{state: campaignEnabled, expected: true},
	}
	for _, input := range inputs {
		mgr.campaignStateWatchable.Update(input.state)
		require.Equal(t, input.expected, mgr.IsCampaigning())
	}
}

func TestElectionManagerResignAlreadyClosed(t *testing.T) {
	opts := testElectionManagerOptions(t)
	mgr := NewElectionManager(opts).(*electionManager)
	require.Equal(t, errElectionManagerNotOpenOrClosed, mgr.Resign(context.Background()))
}

func TestElectionManagerFollowerResign(t *testing.T) {
	leaderService := &mockLeaderService{
		campaignFn: func(
			electionID string,
			opts services.CampaignOptions,
		) (<-chan campaign.Status, error) {
			return make(chan campaign.Status), nil
		},
	}
	opts := testElectionManagerOptions(t).SetLeaderService(leaderService)
	mgr := NewElectionManager(opts).(*electionManager)
	require.NoError(t, mgr.Open(testShardSetID))
	require.NoError(t, mgr.Resign(context.Background()))
}

func TestElectionManagerResignLeaderServiceResignError(t *testing.T) {
	iter := 0
	errLeaderServiceResign := errors.New("leader service resign error")
	opts := testElectionManagerOptions(t)
	mgr := NewElectionManager(opts).(*electionManager)
	leaderService := &mockLeaderService{
		campaignFn: func(
			electionID string,
			opts services.CampaignOptions,
		) (<-chan campaign.Status, error) {
			return make(chan campaign.Status), nil
		},
		resignFn: func(electionID string) error {
			iter++
			if iter < 3 {
				return errLeaderServiceResign
			}
			mgr.electionStateWatchable.Update(FollowerState)
			return nil
		},
	}
	mgr.leaderService = leaderService
	retryOpts := retry.NewOptions().
		SetInitialBackoff(10 * time.Millisecond).
		SetBackoffFactor(2).
		SetMaxBackoff(50 * time.Millisecond).
		SetForever(true)
	mgr.resignRetrier = retry.NewRetrier(retryOpts)
	mgr.sleepFn = func(time.Duration) {}
	mgr.electionStateWatchable.Update(LeaderState)
	require.NoError(t, mgr.Open(testShardSetID))
	require.NoError(t, mgr.Resign(context.Background()))
	require.Equal(t, 3, iter)
	require.Equal(t, electionManagerOpen, mgr.state)
	require.NoError(t, mgr.Close())
}

func TestElectionManagerResignTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()

	leaderService := &mockLeaderService{
		campaignFn: func(
			electionID string,
			opts services.CampaignOptions,
		) (<-chan campaign.Status, error) {
			return make(chan campaign.Status), nil
		},
		resignFn: func(electionID string) error {
			return nil
		},
	}
	opts := testElectionManagerOptions(t).SetLeaderService(leaderService)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.sleepFn = func(time.Duration) {}
	mgr.electionStateWatchable.Update(LeaderState)
	require.NoError(t, mgr.Open(testShardSetID))
	require.Error(t, mgr.Resign(ctx))
	require.Equal(t, electionManagerOpen, mgr.state)
	require.NoError(t, mgr.Close())
}

func TestElectionManagerResignSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var (
		statusCh = make(chan campaign.Status, 2)
		mgr      *electionManager
	)

	leaderValue := "myself"
	leaderService := &mockLeaderService{
		leaderFn: func(electionID string) (string, error) {
			return "someone else", nil
		},
		campaignFn: func(
			electionID string,
			opts services.CampaignOptions,
		) (<-chan campaign.Status, error) {
			return statusCh, nil
		},
		resignFn: func(electionID string) error {
			statusCh <- campaign.Status{State: campaign.Follower}
			return nil
		},
	}
	campaignOpts, err := services.NewCampaignOptions()
	require.NoError(t, err)
	campaignOpts = campaignOpts.SetLeaderValue(leaderValue)
	opts := testElectionManagerOptions(t).
		SetCampaignOptions(campaignOpts).
		SetLeaderService(leaderService)
	mgr = NewElectionManager(opts).(*electionManager)
	mgr.sleepFn = func(time.Duration) {}
	mgr.electionStateWatchable.Update(LeaderState)
	require.NoError(t, mgr.Open(testShardSetID))

	require.NoError(t, mgr.Resign(ctx))
	time.Sleep(time.Second)
	require.Equal(t, FollowerState, mgr.ElectionState())
	require.Equal(t, electionManagerOpen, mgr.state)
	require.NoError(t, mgr.Close())
}

func TestElectionManagerCloseNotOpenOrResigned(t *testing.T) {
	opts := testElectionManagerOptions(t)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.state = electionManagerNotOpen
	require.Equal(t, errElectionManagerNotOpenOrClosed, mgr.Close())
}

func TestElectionManagerCloseSuccess(t *testing.T) {
	opts := testElectionManagerOptions(t)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.state = electionManagerOpen
	require.NoError(t, mgr.Close())
}

func TestElectionManagerCampaignLoop(t *testing.T) {
	iter := 0
	var resigned int32
	leaderValue := "myself"
	campaignCh := make(chan campaign.Status)
	nextCampaignCh := make(chan campaign.Status)
	leaderService := &mockLeaderService{
		campaignFn: func(
			electionID string,
			opts services.CampaignOptions,
		) (<-chan campaign.Status, error) {
			iter++
			if iter == 1 {
				return campaignCh, nil
			}
			return nextCampaignCh, nil
		},
		leaderFn: func(electionID string) (string, error) {
			return "someone else", nil
		},
		resignFn: func(electionID string) error {
			atomic.StoreInt32(&resigned, 1)
			return nil
		},
	}
	campaignOpts, err := services.NewCampaignOptions()
	require.NoError(t, err)
	campaignOpts = campaignOpts.SetLeaderValue(leaderValue)
	opts := testElectionManagerOptions(t).
		SetCampaignOptions(campaignOpts).
		SetLeaderService(leaderService)
	mgr := NewElectionManager(opts).(*electionManager)

	var enabled = int32(1)
	mgr.campaignIsEnabledFn = func() (bool, error) {
		if atomic.LoadInt32(&enabled) == 1 {
			return true, nil
		}
		return false, nil
	}
	require.NoError(t, mgr.Open(testShardSetID))

	// Error status is ignored.
	campaignCh <- campaign.NewErrorStatus(errors.New("foo"))
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, FollowerState, mgr.ElectionState())

	// Same state is a no op.
	campaignCh <- campaign.NewStatus(campaign.Follower)
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, FollowerState, mgr.ElectionState())

	// Follower to leader.
	campaignCh <- campaign.NewStatus(campaign.Leader)
	for {
		if mgr.ElectionState() == LeaderState {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Disable campaigning.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		atomic.StoreInt32(&enabled, 0)
		campaignCh <- campaign.NewStatus(campaign.Follower)
		close(campaignCh)
	}()

	for {
		if mgr.ElectionState() == FollowerState {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	wg.Wait()

	for {
		if atomic.LoadInt32(&mgr.campaigning) == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Give it some time to go through the retry logic even though
	// no change is expected.
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, int32(0), atomic.LoadInt32(&mgr.campaigning))

	// Enable campaigning again.
	atomic.StoreInt32(&enabled, 1)
	for {
		if atomic.LoadInt32(&mgr.campaigning) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Verifying we are actually campaigning.
	nextCampaignCh <- campaign.NewStatus(campaign.Leader)

	require.NoError(t, mgr.Close())

	for {
		if atomic.LoadInt32(&resigned) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestElectionManagerVerifyLeaderDelay(t *testing.T) {
	var iter int
	leaderValue := "myself"
	leaderService := &mockLeaderService{
		leaderFn: func(electionID string) (string, error) {
			iter++
			if iter < 10 {
				return leaderValue, nil
			}
			return "someone else", nil
		},
	}
	campaignOpts, err := services.NewCampaignOptions()
	require.NoError(t, err)
	campaignOpts = campaignOpts.SetLeaderValue(leaderValue)
	opts := testElectionManagerOptions(t).
		SetCampaignOptions(campaignOpts).
		SetLeaderService(leaderService)
	mgr := NewElectionManager(opts).(*electionManager)
	retryOpts := retry.NewOptions().
		SetInitialBackoff(10 * time.Millisecond).
		SetBackoffFactor(2).
		SetMaxBackoff(50 * time.Millisecond).
		SetForever(true)
	mgr.changeRetrier = retry.NewRetrier(retryOpts)
	mgr.electionStateWatchable.Update(PendingFollowerState)
	mgr.campaignStateWatchable.Update(campaignEnabled)

	_, watch, err := mgr.goalStateWatchable.Watch()
	require.NoError(t, err)

	mgr.Add(1)
	go mgr.verifyPendingFollower(watch)
	mgr.goalStateWatchable.Update(goalState{state: PendingFollowerState})

	for {
		if mgr.goalStateWatchable.Get().(goalState).state == FollowerState {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	close(mgr.doneCh)
	require.Equal(t, 10, iter)
}

func TestElectionManagerVerifyWithLeaderErrors(t *testing.T) {
	iter := 0
	leaderValue := "myself"
	leaderService := &mockLeaderService{
		leaderFn: func(electionID string) (string, error) {
			iter++
			if iter == 1 {
				return "", errors.New("leader service error")
			}
			if iter == 2 {
				return leaderValue, nil
			}
			return "someone else", nil
		},
	}
	campaignOpts, err := services.NewCampaignOptions()
	require.NoError(t, err)
	campaignOpts = campaignOpts.SetLeaderValue(leaderValue)
	opts := testElectionManagerOptions(t).
		SetCampaignOptions(campaignOpts).
		SetLeaderService(leaderService)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.electionStateWatchable.Update(PendingFollowerState)
	mgr.changeRetrier = retry.NewRetrier(retry.NewOptions().SetInitialBackoff(100 * time.Millisecond))
	mgr.campaignStateWatchable.Update(campaignEnabled)

	_, watch, err := mgr.goalStateWatchable.Watch()
	require.NoError(t, err)

	mgr.Add(1)
	go mgr.verifyPendingFollower(watch)
	mgr.goalStateWatchable.Update(goalState{state: PendingFollowerState})

	for {
		if mgr.goalStateWatchable.Get().(goalState).state == FollowerState {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	close(mgr.doneCh)
	require.Equal(t, 3, iter)
}

func TestElectionManagerVerifyPendingFollowerStale(t *testing.T) {
	var called int32
	leaderValue := "myself"
	leaderService := &mockLeaderService{
		leaderFn: func(electionID string) (string, error) {
			atomic.StoreInt32(&called, 1)
			return leaderValue, nil
		},
	}
	campaignOpts, err := services.NewCampaignOptions()
	require.NoError(t, err)
	campaignOpts = campaignOpts.SetLeaderValue(leaderValue)
	opts := testElectionManagerOptions(t).
		SetCampaignOptions(campaignOpts).
		SetLeaderService(leaderService)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.electionStateWatchable.Update(PendingFollowerState)
	mgr.campaignStateWatchable.Update(campaignEnabled)

	_, watch, err := mgr.goalStateWatchable.Watch()
	require.NoError(t, err)

	mgr.Add(1)
	go mgr.verifyPendingFollower(watch)
	mgr.goalStateWatchable.Update(goalState{state: PendingFollowerState})

	for {
		if atomic.LoadInt32(&called) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Push a new goal state to preempt the previous pending follower state.
	mgr.goalStateWatchable.Update(goalState{state: LeaderState})

	// Verify the retrier has exited the infinite retry loop.
	mgr.doneCh <- struct{}{}
}

func TestElectionManagerVerifyCampaignDisabled(t *testing.T) {
	errLeaderService := errors.New("leader service error")
	leaderService := &mockLeaderService{
		leaderFn: func(electionID string) (string, error) {
			return "", errLeaderService
		},
	}
	opts := testElectionManagerOptions(t).
		SetLeaderService(leaderService)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.electionStateWatchable.Update(PendingFollowerState)
	mgr.campaignStateWatchable.Update(campaignEnabled)

	_, watch, err := mgr.goalStateWatchable.Watch()
	require.NoError(t, err)

	mgr.Add(1)
	go mgr.verifyPendingFollower(watch)
	mgr.goalStateWatchable.Update(goalState{state: PendingFollowerState})

	// Sleep a little and check nothing changes.
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, PendingFollowerState, mgr.goalStateWatchable.Get().(goalState).state)

	// Disable the campaign and wait for the goal state to update.
	mgr.campaignStateWatchable.Update(campaignDisabled)
	for {
		if mgr.goalStateWatchable.Get().(goalState).state == FollowerState {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	close(mgr.doneCh)
}

func TestElectionManagerCheckCampaignStateLoop(t *testing.T) {
	leaderValue := "myself"
	leaderService := &mockLeaderService{
		campaignFn: func(
			electionID string,
			opts services.CampaignOptions,
		) (<-chan campaign.Status, error) {
			return make(chan campaign.Status), nil
		},
		leaderFn: func(electionID string) (string, error) {
			return leaderValue, nil
		},
		resignFn: func(electionID string) error {
			return nil
		},
	}
	campaignOpts, err := services.NewCampaignOptions()
	require.NoError(t, err)
	campaignOpts = campaignOpts.SetLeaderValue(leaderValue)
	opts := testElectionManagerOptions(t).
		SetCampaignOptions(campaignOpts).
		SetLeaderService(leaderService)
	mgr := NewElectionManager(opts).(*electionManager)
	iterCh := make(chan enabledRes)
	mgr.campaignIsEnabledFn = func() (bool, error) {
		res := <-iterCh
		return res.result, res.err
	}
	require.NoError(t, mgr.Open(testShardSetID))

	ensureState := func(targetState campaignState) {
		for {
			if mgr.campaignState() == targetState {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Enable campaigning.
	iterCh <- enabledRes{result: true, err: nil}
	ensureState(campaignEnabled)

	// Disable campaigning with error.
	iterCh <- enabledRes{result: false, err: nil}
	iterCh <- enabledRes{result: false, err: errors.New("enabled error")}
	ensureState(campaignPendingDisabled)

	// Disable campaigning.
	iterCh <- enabledRes{result: false, err: nil}
	iterCh <- enabledRes{result: false, err: nil}
	iterCh <- enabledRes{result: false, err: nil}
	ensureState(campaignDisabled)

	// Re-enable campaigning.
	iterCh <- enabledRes{result: true, err: nil}
	ensureState(campaignEnabled)

	require.NoError(t, mgr.Close())
}

func TestElectionManagerCampaignIsEnabledInstanceNotFound(t *testing.T) {
	opts := testElectionManagerOptions(t)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.placementManager = &mockPlacementManager{
		shardsFn: func() (shard.Shards, error) {
			return nil, ErrInstanceNotFoundInPlacement
		},
	}
	enabled, err := mgr.campaignIsEnabled()
	require.False(t, enabled)
	require.NoError(t, err)
}

func TestElectionManagerCampaignIsEnabledShardsError(t *testing.T) {
	errShards := errors.New("shards error")
	opts := testElectionManagerOptions(t)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.placementManager = &mockPlacementManager{
		shardsFn: func() (shard.Shards, error) {
			return nil, errShards
		},
	}
	_, err := mgr.campaignIsEnabled()
	require.Equal(t, errShards, err)
}

func TestElectionManagerCampaignIsEnabledWithActiveShards(t *testing.T) {
	expectedShards := shard.NewShards([]shard.Shard{
		shard.NewShard(0).SetCutoverNanos(1000).SetCutoffNanos(7777),
		shard.NewShard(1).SetCutoverNanos(3333).SetCutoffNanos(8888),
	})
	opts := testElectionManagerOptions(t).SetShardCutoffCheckOffset(time.Duration(1000))
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.placementManager = &mockPlacementManager{
		shardsFn: func() (shard.Shards, error) {
			return expectedShards, nil
		},
	}
	now := time.Unix(0, 1234)
	mgr.nowFn = func() time.Time { return now }

	enabled, err := mgr.campaignIsEnabled()
	require.True(t, enabled)
	require.NoError(t, err)
}

func TestElectionManagerCampaignIsEnabledNoCutoverShards(t *testing.T) {
	expectedShards := shard.NewShards([]shard.Shard{
		shard.NewShard(0).SetCutoverNanos(1000).SetCutoffNanos(7777),
		shard.NewShard(1).SetCutoverNanos(3333).SetCutoffNanos(8888),
	})
	opts := testElectionManagerOptions(t).SetShardCutoffCheckOffset(time.Duration(1000))
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.placementManager = &mockPlacementManager{
		shardsFn: func() (shard.Shards, error) {
			return expectedShards, nil
		},
	}
	now := time.Unix(0, 123)
	mgr.nowFn = func() time.Time { return now }

	enabled, err := mgr.campaignIsEnabled()
	require.False(t, enabled)
	require.NoError(t, err)
}

func TestElectionManagerCampaignIsEnabledAllCutoffShardsFlushed(t *testing.T) {
	expectedShards := shard.NewShards([]shard.Shard{
		shard.NewShard(0).SetCutoverNanos(1000).SetCutoffNanos(7777),
		shard.NewShard(1).SetCutoverNanos(3333).SetCutoffNanos(8888),
	})
	opts := testElectionManagerOptions(t).SetShardCutoffCheckOffset(time.Duration(1000))
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.placementManager = &mockPlacementManager{
		shardsFn: func() (shard.Shards, error) {
			return expectedShards, nil
		},
	}
	mgr.flushTimesManager = &mockFlushTimesManager{
		getFlushTimesFn: func() (*schema.ShardSetFlushTimes, error) {
			return &schema.ShardSetFlushTimes{
				ByShard: map[uint32]*schema.ShardFlushTimes{
					0: &schema.ShardFlushTimes{
						ByResolution: map[int64]int64{
							int64(time.Second): 8000,
						},
					},
					1: &schema.ShardFlushTimes{
						ByResolution: map[int64]int64{
							int64(time.Minute): 9000,
						},
					},
				},
			}, nil
		},
	}
	now := time.Unix(0, 8000)
	mgr.nowFn = func() time.Time { return now }

	enabled, err := mgr.campaignIsEnabled()
	require.False(t, enabled)
	require.NoError(t, err)
}

func TestElectionManagerCampaignIsEnabledHasReplacementInstance(t *testing.T) {
	expectedShards := shard.NewShards([]shard.Shard{
		shard.NewShard(0).SetCutoverNanos(1000).SetCutoffNanos(7777),
		shard.NewShard(1).SetCutoverNanos(3333).SetCutoffNanos(8888),
	})
	opts := testElectionManagerOptions(t).SetShardCutoffCheckOffset(time.Duration(1000))
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.placementManager = &mockPlacementManager{
		shardsFn: func() (shard.Shards, error) {
			return expectedShards, nil
		},
		hasReplacementInstanceFn: func() (bool, error) {
			return true, nil
		},
	}
	mgr.flushTimesManager = &mockFlushTimesManager{
		getFlushTimesFn: func() (*schema.ShardSetFlushTimes, error) {
			return &schema.ShardSetFlushTimes{
				ByShard: map[uint32]*schema.ShardFlushTimes{
					0: &schema.ShardFlushTimes{
						ByResolution: map[int64]int64{
							int64(time.Second): 7000,
						},
					},
					1: &schema.ShardFlushTimes{
						ByResolution: map[int64]int64{
							int64(time.Minute): 9000,
						},
					},
				},
			}, nil
		},
	}
	now := time.Unix(0, 8000)
	mgr.nowFn = func() time.Time { return now }

	enabled, err := mgr.campaignIsEnabled()
	require.False(t, enabled)
	require.NoError(t, err)
}

func TestElectionManagerCampaignIsEnabledNoReplacementInstance(t *testing.T) {
	expectedShards := shard.NewShards([]shard.Shard{
		shard.NewShard(0).SetCutoverNanos(1000).SetCutoffNanos(7777),
		shard.NewShard(1).SetCutoverNanos(3333).SetCutoffNanos(8888),
	})
	opts := testElectionManagerOptions(t).SetShardCutoffCheckOffset(time.Duration(1000))
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.placementManager = &mockPlacementManager{
		shardsFn: func() (shard.Shards, error) {
			return expectedShards, nil
		},
		hasReplacementInstanceFn: func() (bool, error) {
			return false, nil
		},
	}
	mgr.flushTimesManager = &mockFlushTimesManager{
		getFlushTimesFn: func() (*schema.ShardSetFlushTimes, error) {
			return &schema.ShardSetFlushTimes{
				ByShard: map[uint32]*schema.ShardFlushTimes{
					0: &schema.ShardFlushTimes{
						ByResolution: map[int64]int64{
							int64(time.Second): 7000,
						},
					},
					1: &schema.ShardFlushTimes{
						ByResolution: map[int64]int64{
							int64(time.Minute): 9000,
						},
					},
				},
			}, nil
		},
	}
	now := time.Unix(0, 9000)
	mgr.nowFn = func() time.Time { return now }

	enabled, err := mgr.campaignIsEnabled()
	require.True(t, enabled)
	require.NoError(t, err)
}

func TestElectionManagerCampaignIsEnabledAllCutoffShardsWithError(t *testing.T) {
	errFlushTimesGet := errors.New("error getting flush times")
	errHasReplacementInstance := errors.New("error determining replacement instance")
	expectedShards := shard.NewShards([]shard.Shard{
		shard.NewShard(0).SetCutoverNanos(1000).SetCutoffNanos(7777),
		shard.NewShard(1).SetCutoverNanos(3333).SetCutoffNanos(8888),
	})
	opts := testElectionManagerOptions(t).SetShardCutoffCheckOffset(time.Duration(1000))
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.placementManager = &mockPlacementManager{
		shardsFn: func() (shard.Shards, error) {
			return expectedShards, nil
		},
		hasReplacementInstanceFn: func() (bool, error) {
			return false, errHasReplacementInstance
		},
	}
	mgr.flushTimesManager = &mockFlushTimesManager{
		getFlushTimesFn: func() (*schema.ShardSetFlushTimes, error) {
			return nil, errFlushTimesGet
		},
	}
	now := time.Unix(0, 9000)
	mgr.nowFn = func() time.Time { return now }

	_, err := mgr.campaignIsEnabled()
	require.Error(t, err)
}

func TestElectionManagerCampaignIsEnabledUnexpectedShardCutoverCutoffTimes(t *testing.T) {
	expectedShards := shard.NewShards([]shard.Shard{
		shard.NewShard(0).SetCutoverNanos(1000).SetCutoffNanos(3333),
		shard.NewShard(1).SetCutoverNanos(6666).SetCutoffNanos(8888),
	})
	opts := testElectionManagerOptions(t).SetShardCutoffCheckOffset(time.Duration(1000))
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.placementManager = &mockPlacementManager{
		shardsFn: func() (shard.Shards, error) {
			return expectedShards, nil
		},
	}
	now := time.Unix(0, 4444)
	mgr.nowFn = func() time.Time { return now }

	_, err := mgr.campaignIsEnabled()
	require.Equal(t, errUnexpectedShardCutoverCutoffTimes, err)
}

func testElectionManagerOptions(t *testing.T) ElectionManagerOptions {
	campaignOpts, err := services.NewCampaignOptions()
	require.NoError(t, err)
	placementManager := &mockPlacementManager{
		shardsFn: func() (shard.Shards, error) {
			return shard.NewShards([]shard.Shard{
				shard.NewShard(0),
			}), nil
		},
	}
	leaderService := &mockLeaderService{
		campaignFn: func(
			electionID string,
			opts services.CampaignOptions,
		) (<-chan campaign.Status, error) {
			return make(chan campaign.Status), nil
		},
		resignFn: func(electionID string) error {
			return nil
		},
	}
	return NewElectionManagerOptions().
		SetCampaignOptions(campaignOpts).
		SetPlacementManager(placementManager).
		SetLeaderService(leaderService)
}

type enabledRes struct {
	result bool
	err    error
}

type campaignFn func(
	electionID string,
	opts services.CampaignOptions,
) (<-chan campaign.Status, error)

type resignFn func(electionID string) error
type leaderFn func(electionID string) (string, error)

type mockLeaderService struct {
	campaignFn campaignFn
	resignFn   resignFn
	leaderFn   leaderFn
}

func (s *mockLeaderService) Campaign(
	electionID string,
	opts services.CampaignOptions,
) (<-chan campaign.Status, error) {
	return s.campaignFn(electionID, opts)
}

func (s *mockLeaderService) Resign(electionID string) error {
	return s.resignFn(electionID)
}

func (s *mockLeaderService) Leader(electionID string) (string, error) {
	return s.leaderFn(electionID)
}

func (s *mockLeaderService) Close() error { return nil }

type electionOpenFn func(shardSetID uint32) error
type isCampaigningFn func() bool
type electionResignFn func(ctx context.Context) error

type mockElectionManager struct {
	sync.RWMutex

	openFn          electionOpenFn
	isCampaigningFn isCampaigningFn
	electionState   ElectionState
	resignFn        electionResignFn
}

func (m *mockElectionManager) Reset() error                 { return nil }
func (m *mockElectionManager) Open(shardSetID uint32) error { return m.openFn(shardSetID) }
func (m *mockElectionManager) ElectionState() ElectionState {
	m.RLock()
	state := m.electionState
	m.RUnlock()
	return state
}
func (m *mockElectionManager) IsCampaigning() bool              { return m.isCampaigningFn() }
func (m *mockElectionManager) Resign(ctx context.Context) error { return m.resignFn(ctx) }
func (m *mockElectionManager) Close() error                     { return nil }
