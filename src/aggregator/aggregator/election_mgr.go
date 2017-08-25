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
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/leader/campaign"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/retry"
	"github.com/m3db/m3x/watch"

	"github.com/uber-go/tally"
)

// ElectionManager manages leadership elections.
type ElectionManager interface {
	// Open opens the election manager for a given shard set.
	Open(shardSetID uint32) error

	// ElectionState returns the election state.
	ElectionState() ElectionState

	// Resign stops the election and resigns from the ongoing campaign if any, thereby
	// forcing the current instance to become a follower. If the provided context
	// expires before resignation is complete, the context error is returned, and the
	// election is restarted if necessary.
	Resign(ctx context.Context) error

	// Close the election manager.
	Close() error
}

const (
	backOffOnResignOrElectionError = time.Second
)

var (
	errElectionManagerAlreadyOpenOrClosed = errors.New("election manager is already open or closed")
	errElectionManagerNotOpenOrClosed     = errors.New("election manager is not open or closed")
	errElectionManagerAlreadyResigning    = errors.New("election manager is already resigning")
	errLeaderNotChanged                   = errors.New("leader has not changed")
)

type electionManagerState int

const (
	electionManagerNotOpen electionManagerState = iota
	electionManagerOpen
	electionManagerClosed
)

// ElectionState is the election state.
type ElectionState int

// A list of supported election states.
const (
	// Unknown election state.
	UnknownState ElectionState = iota

	// Follower state.
	FollowerState

	// Leader state.
	LeaderState

	// Pending follower state.
	PendingFollowerState
)

func newElectionState(state campaign.State) (ElectionState, error) {
	switch state {
	case campaign.Leader:
		return LeaderState, nil
	case campaign.Follower:
		// NB(xichen): Follower state from campaign is not verified and as such
		// we map it to pending follower state.
		return PendingFollowerState, nil
	default:
		return UnknownState, fmt.Errorf("unknown campaign state %s", state.String())
	}
}

func (state ElectionState) String() string {
	switch state {
	case FollowerState:
		return "follower"
	case PendingFollowerState:
		return "pendingFollower"
	case LeaderState:
		return "leader"
	default:
		return "unknown"
	}
}

// MarshalJSON returns state as the JSON encoding of state.
func (state ElectionState) MarshalJSON() ([]byte, error) {
	return json.Marshal(state.String())
}

// UnmarshalJSON unmarshals JSON-encoded data into state.
func (state *ElectionState) UnmarshalJSON(data []byte) error {
	var str string
	err := json.Unmarshal(data, &str)
	if err != nil {
		return err
	}
	switch str {
	case FollowerState.String():
		*state = FollowerState
	case PendingFollowerState.String():
		*state = PendingFollowerState
	case LeaderState.String():
		*state = LeaderState
	default:
		return fmt.Errorf("unexpected json-encoded state: %s", str)
	}
	return nil
}

type electionManagerMetrics struct {
	campaignCreateErrors      tally.Counter
	campaignErrors            tally.Counter
	campaignUnknownState      tally.Counter
	verifyLeaderErrors        tally.Counter
	verifyLeaderNotChanged    tally.Counter
	verifyPendingChangeStale  tally.Counter
	verifyNoKnownLeader       tally.Counter
	followerResign            tally.Counter
	resignTimeout             tally.Counter
	resignErrors              tally.Counter
	followerToPendingFollower tally.Counter
	stateChanges              tally.Counter
	electionState             tally.Gauge
}

func newElectionManagerMetrics(scope tally.Scope) electionManagerMetrics {
	return electionManagerMetrics{
		campaignCreateErrors:      scope.Counter("campaign-create-errors"),
		campaignErrors:            scope.Counter("campaign-errors"),
		campaignUnknownState:      scope.Counter("campaign-unknown-state"),
		verifyLeaderErrors:        scope.Counter("verify-leader-errors"),
		verifyLeaderNotChanged:    scope.Counter("verify-leader-not-changed"),
		verifyPendingChangeStale:  scope.Counter("verify-pending-change-stale"),
		verifyNoKnownLeader:       scope.Counter("verify-no-known-leader"),
		followerResign:            scope.Counter("follower-resign"),
		resignTimeout:             scope.Counter("resign-timeout"),
		resignErrors:              scope.Counter("resign-errors"),
		followerToPendingFollower: scope.Counter("follower-to-pending-follower"),
		stateChanges:              scope.Counter("state-changes"),
		electionState:             scope.Gauge("election-state"),
	}
}

type goalState struct {
	id    int64
	state ElectionState
}

type electionManager struct {
	sync.RWMutex

	nowFn           clock.NowFn
	logger          xlog.Logger
	reportInterval  time.Duration
	campaignOpts    services.CampaignOptions
	electionOpts    services.ElectionOptions
	campaignRetrier xretry.Retrier
	changeRetrier   xretry.Retrier
	electionKeyFmt  string
	leaderService   services.LeaderService
	leaderValue     string

	state                  electionManagerState
	doneCh                 chan struct{}
	electionKey            string
	electionStateWatchable xwatch.Watchable
	nextGoalStateID        int64
	goalStateLock          sync.RWMutex
	goalStateWatchable     xwatch.Watchable
	resigning              bool
	sleepFn                sleepFn
	metrics                electionManagerMetrics
}

// NewElectionManager creates a new election manager.
func NewElectionManager(opts ElectionManagerOptions) ElectionManager {
	instrumentOpts := opts.InstrumentOptions()
	campaignOpts := opts.CampaignOptions()
	campaignRetrier := xretry.NewRetrier(opts.CampaignRetryOptions().SetForever(true))
	changeRetrier := xretry.NewRetrier(opts.ChangeRetryOptions().SetForever(true))
	electionStateWatchable := xwatch.NewWatchable()
	electionStateWatchable.Update(FollowerState)
	return &electionManager{
		nowFn:                  opts.ClockOptions().NowFn(),
		logger:                 instrumentOpts.Logger(),
		reportInterval:         instrumentOpts.ReportInterval(),
		campaignOpts:           campaignOpts,
		electionOpts:           opts.ElectionOptions(),
		campaignRetrier:        campaignRetrier,
		changeRetrier:          changeRetrier,
		electionKeyFmt:         opts.ElectionKeyFmt(),
		leaderService:          opts.LeaderService(),
		leaderValue:            campaignOpts.LeaderValue(),
		state:                  electionManagerNotOpen,
		doneCh:                 make(chan struct{}),
		electionStateWatchable: electionStateWatchable,
		goalStateWatchable:     xwatch.NewWatchable(),
		sleepFn:                time.Sleep,
		metrics:                newElectionManagerMetrics(instrumentOpts.MetricsScope()),
	}
}

func (mgr *electionManager) Open(shardSetID uint32) error {
	mgr.Lock()
	defer mgr.Unlock()

	if mgr.state != electionManagerNotOpen {
		return errElectionManagerAlreadyOpenOrClosed
	}
	mgr.state = electionManagerOpen
	mgr.electionKey = fmt.Sprintf(mgr.electionKeyFmt, shardSetID)

	_, stateChangeWatch, err := mgr.goalStateWatchable.Watch()
	if err != nil {
		return err
	}

	_, verifyWatch, err := mgr.goalStateWatchable.Watch()
	if err != nil {
		return err
	}

	go mgr.watchGoalStateChanges(stateChangeWatch)
	go mgr.verifyPendingFollower(verifyWatch)
	go mgr.campaignLoop()
	go mgr.reportMetrics()
	return nil
}

func (mgr *electionManager) ElectionState() ElectionState {
	return mgr.electionStateWatchable.Get().(ElectionState)
}

func (mgr *electionManager) Resign(ctx context.Context) error {
	mgr.Lock()
	if mgr.state != electionManagerOpen {
		mgr.Unlock()
		return errElectionManagerNotOpenOrClosed
	}
	if mgr.resigning {
		mgr.Unlock()
		return errElectionManagerAlreadyResigning
	}
	if mgr.ElectionState() == FollowerState {
		mgr.Unlock()
		mgr.metrics.followerResign.Inc(1)
		return nil
	}
	mgr.resigning = true
	mgr.Unlock()

	defer func() {
		mgr.Lock()
		mgr.resigning = false
		mgr.Unlock()
	}()

	_, watch, err := mgr.electionStateWatchable.Watch()
	if err != nil {
		return fmt.Errorf("error creating watch when resigning: %v", err)
	}
	defer watch.Close()

	if err := mgr.leaderService.Resign(mgr.electionKey); err != nil {
		mgr.metrics.resignErrors.Inc(1)
		mgr.logError("resign error", err)
		return err
	}

	for {
		select {
		case <-watch.C():
			if state := watch.Get().(ElectionState); state == FollowerState {
				return nil
			}
		case <-ctx.Done():
			mgr.metrics.resignTimeout.Inc(1)
			mgr.logError("resign error", ctx.Err())
			return ctx.Err()
		}
	}
}

func (mgr *electionManager) Close() error {
	mgr.Lock()
	defer mgr.Unlock()

	if mgr.state != electionManagerOpen {
		return errElectionManagerNotOpenOrClosed
	}
	close(mgr.doneCh)
	mgr.state = electionManagerClosed
	return nil
}

func (mgr *electionManager) watchGoalStateChanges(watch xwatch.Watch) {
	defer watch.Close()

	for {
		select {
		case <-mgr.doneCh:
			return
		case <-watch.C():
			mgr.processGoalState(watch.Get().(goalState))
		}
	}
}

func (mgr *electionManager) processGoalState(goalState goalState) {
	currState := mgr.ElectionState()
	newState := goalState.state
	if currState == newState {
		return
	}
	// NB(xichen): the follower to pending follower transition is conceptually invalid
	// but because the leader election API currently does not verify states, if the client
	// receives a follower state, it can only treat it as a "pendingFollower" state. This
	// is no longer needed once the leader election logic is adapted to only notify clients
	// of states that have been verified.
	if currState == FollowerState && newState == PendingFollowerState {
		mgr.metrics.followerToPendingFollower.Inc(1)
		return
	}
	mgr.electionStateWatchable.Update(newState)
	mgr.metrics.stateChanges.Inc(1)
	mgr.logger.Info(fmt.Sprintf("election state changed from %v to %v", currState, newState))
}

func (mgr *electionManager) verifyPendingFollower(watch xwatch.Watch) {
	defer watch.Close()

	for {
		select {
		case <-mgr.doneCh:
			return
		case <-watch.C():
		}

		currState := watch.Get().(goalState)
		if currState.state != PendingFollowerState {
			continue
		}

		// Only continue verifying if the state has not changed.
		continueFn := func(int) bool {
			mgr.goalStateLock.RLock()
			latest := mgr.goalStateWatchable.Get().(goalState)
			mgr.goalStateLock.RUnlock()
			return currState.id == latest.id && currState.state == latest.state
		}

		// Do not change state if the follower state cannot be verified.
		if verifyErr := mgr.changeRetrier.AttemptWhile(continueFn, func() error {
			leader, err := mgr.leaderService.Leader(mgr.electionKey)
			if err != nil {
				mgr.metrics.verifyLeaderErrors.Inc(1)
				mgr.logError("error determining the leader", err)
				return err
			}
			if leader == mgr.leaderValue {
				mgr.metrics.verifyLeaderNotChanged.Inc(1)
				mgr.logError("leader has not changed", errLeaderNotChanged)
				return errLeaderNotChanged
			}
			return nil
		}); verifyErr != nil {
			mgr.logError("verify error", verifyErr)
			continue
		}

		mgr.goalStateLock.Lock()
		// If the latest goal state is different from the goal state before verification,
		// this means the goal state has been updated since verification started and the
		// pending follower to follower transition is no longer valid.
		state := mgr.goalStateWatchable.Get().(goalState)
		if !(state.id == currState.id && state.state == currState.state) {
			mgr.metrics.verifyPendingChangeStale.Inc(1)
			mgr.goalStateLock.Unlock()
			continue
		}
		mgr.setGoalStateWithLock(FollowerState)
		mgr.goalStateLock.Unlock()
	}
}

func (mgr *electionManager) campaignLoop() {
	var campaignStatusCh <-chan campaign.Status
	notDone := func(int) bool {
		select {
		case <-mgr.doneCh:
			return false
		default:
			return true
		}
	}

	for {
		if campaignStatusCh == nil {
			// NB(xichen): campaign retrier retries forever until either the Campaign call succeeds,
			// or the election manager has resigned.
			if err := mgr.campaignRetrier.AttemptWhile(notDone, func() error {
				var err error
				campaignStatusCh, err = mgr.leaderService.Campaign(mgr.electionKey, mgr.campaignOpts)
				if err == nil {
					return nil
				}
				mgr.metrics.campaignCreateErrors.Inc(1)
				mgr.logError("error creating campaign", err)
				return err
			}); err == xretry.ErrWhileConditionFalse {
				return
			}
		}

		var (
			campaignStatus campaign.Status
			ok             bool
		)
		select {
		case campaignStatus, ok = <-campaignStatusCh:
			// If the campaign status channel is closed, this is either because session has expired,
			// or we have resigned from the campaign, or there are issues with the underlying etcd
			// cluster, in which case we back off a little and restart the campaign.
			if !ok {
				campaignStatusCh = nil
				mgr.sleepFn(backOffOnResignOrElectionError)
				continue
			}
		case <-mgr.doneCh:
			return
		}
		mgr.processCampaignUpdate(campaignStatus)
	}
}

func (mgr *electionManager) processCampaignUpdate(campaignStatus campaign.Status) {
	if campaignStatus.State == campaign.Error {
		mgr.metrics.campaignErrors.Inc(1)
		mgr.logError("error campaigning", campaignStatus.Err)
		return
	}

	newState, err := newElectionState(campaignStatus.State)
	if err != nil {
		mgr.metrics.campaignUnknownState.Inc(1)
		mgr.logError("unknown campaign state", err)
		return
	}

	mgr.goalStateLock.Lock()
	mgr.setGoalStateWithLock(newState)
	mgr.goalStateLock.Unlock()
}

func (mgr *electionManager) setGoalStateWithLock(newState ElectionState) {
	goalStateID := mgr.nextGoalStateID
	mgr.nextGoalStateID++
	newGoalState := goalState{id: goalStateID, state: newState}
	mgr.goalStateWatchable.Update(newGoalState)
}

func (mgr *electionManager) reportMetrics() {
	ticker := time.NewTicker(mgr.reportInterval)
	for {
		select {
		case <-ticker.C:
			currState := mgr.ElectionState()
			mgr.metrics.electionState.Update(float64(currState))
		case <-mgr.doneCh:
			ticker.Stop()
			return
		}
	}
}

func (mgr *electionManager) logError(desc string, err error) {
	mgr.logger.WithFields(
		xlog.NewLogField("electionKey", mgr.electionKey),
		xlog.NewLogField("electionTTL", time.Duration(mgr.electionOpts.TTLSecs())*time.Second),
		xlog.NewLogField("leaderValue", mgr.campaignOpts.LeaderValue()),
		xlog.NewLogErrField(err),
	).Error(desc)
}
