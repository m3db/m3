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

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/leader/campaign"
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

func TestElectionManagerResignAlreadyClosed(t *testing.T) {
	opts := testElectionManagerOptions(t)
	mgr := NewElectionManager(opts).(*electionManager)
	require.Equal(t, errElectionManagerNotOpenOrClosed, mgr.Resign(context.Background()))
}

func TestElectionManagerResignAlreadyResigning(t *testing.T) {
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
	mgr.resigning = true
	require.Equal(t, errElectionManagerAlreadyResigning, mgr.Resign(context.Background()))
}

func TestElectionManagerResignLeaderServiceResignError(t *testing.T) {
	errLeaderServiceResign := errors.New("leader service resign error")
	leaderService := &mockLeaderService{
		campaignFn: func(
			electionID string,
			opts services.CampaignOptions,
		) (<-chan campaign.Status, error) {
			return make(chan campaign.Status), nil
		},
		resignFn: func(electionID string) error {
			return errLeaderServiceResign
		},
	}
	opts := testElectionManagerOptions(t).SetLeaderService(leaderService)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.sleepFn = func(time.Duration) {}
	mgr.electionStateWatchable.Update(LeaderState)
	require.NoError(t, mgr.Open(testShardSetID))
	require.Error(t, mgr.Resign(context.Background()))
	require.Equal(t, electionManagerOpen, mgr.state)
	require.False(t, mgr.resigning)
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

func TestElectionManagerResignSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var (
		statusCh = make(chan campaign.Status)
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
			close(statusCh)
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
	}
	campaignOpts, err := services.NewCampaignOptions()
	require.NoError(t, err)
	campaignOpts = campaignOpts.SetLeaderValue(leaderValue)
	opts := NewElectionManagerOptions().
		SetCampaignOptions(campaignOpts).
		SetLeaderService(leaderService)
	mgr := NewElectionManager(opts).(*electionManager)
	require.NoError(t, mgr.Open(testShardSetID))

	// Error status is ignored.
	campaignCh <- campaign.NewErrorStatus(errors.New("foo"))
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, FollowerState, mgr.ElectionState())

	// Same state is a no op.
	campaignCh <- campaign.NewStatus(campaign.Follower)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, FollowerState, mgr.ElectionState())

	// Follower to leader.
	campaignCh <- campaign.NewStatus(campaign.Leader)
	for {
		if mgr.ElectionState() == LeaderState {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Asynchronously resign.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		campaignCh <- campaign.NewStatus(campaign.Follower)
		close(campaignCh)
	}()

	for {
		if mgr.ElectionState() == LeaderState {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	wg.Wait()

	// Verifying we are still electing.
	nextCampaignCh <- campaign.NewStatus(campaign.Leader)

	require.NoError(t, mgr.Close())
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
	opts := NewElectionManagerOptions().
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

	_, watch, err := mgr.goalStateWatchable.Watch()
	require.NoError(t, err)

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
	opts := NewElectionManagerOptions().
		SetCampaignOptions(campaignOpts).
		SetLeaderService(leaderService)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.electionStateWatchable.Update(PendingFollowerState)
	mgr.changeRetrier = retry.NewRetrier(retry.NewOptions().SetInitialBackoff(100 * time.Millisecond))

	_, watch, err := mgr.goalStateWatchable.Watch()
	require.NoError(t, err)

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
	opts := NewElectionManagerOptions().
		SetCampaignOptions(campaignOpts).
		SetLeaderService(leaderService)
	mgr := NewElectionManager(opts).(*electionManager)
	mgr.electionStateWatchable.Update(PendingFollowerState)

	_, watch, err := mgr.goalStateWatchable.Watch()
	require.NoError(t, err)

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

func testElectionManagerOptions(t *testing.T) ElectionManagerOptions {
	campaignOpts, err := services.NewCampaignOptions()
	require.NoError(t, err)
	return NewElectionManagerOptions().SetCampaignOptions(campaignOpts)
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
type electionResignFn func(ctx context.Context) error

type mockElectionManager struct {
	sync.RWMutex

	openFn        electionOpenFn
	electionState ElectionState
	resignFn      electionResignFn
}

func (m *mockElectionManager) Open(shardSetID uint32) error { return m.openFn(shardSetID) }
func (m *mockElectionManager) ElectionState() ElectionState {
	m.RLock()
	state := m.electionState
	m.RUnlock()
	return state
}
func (m *mockElectionManager) Resign(ctx context.Context) error { return m.resignFn(ctx) }
func (m *mockElectionManager) Close() error                     { return nil }
