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
	"sync/atomic"
	"time"

	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/cluster/services/leader/campaign"
	"github.com/m3db/m3/src/x/clock"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/retry"
	"github.com/m3db/m3/src/x/watch"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

// ElectionManager manages leadership elections.
type ElectionManager interface {
	// Reset resets the election manager.
	Reset() error

	// Open opens the election manager for a given shard set.
	Open(shardSetID uint32) error

	// ElectionState returns the election state.
	ElectionState() ElectionState

	// IsCampaigning returns true if the election manager is actively campaigning,
	// and false otherwise.
	IsCampaigning() bool

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
	errElectionManagerOpen                = errors.New("election manager is open")
	errLeaderNotChanged                   = errors.New("leader has not changed")
	errUnexpectedShardCutoverCutoffTimes  = errors.New("unexpected shard cutover and/or cutoff times")
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

	// Pending follower state.
	PendingFollowerState

	// Leader state.
	LeaderState
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

type campaignState int

const (
	campaignDisabled campaignState = iota
	campaignPendingDisabled
	campaignEnabled
)

func newCampaignState(enabled bool) campaignState {
	if enabled {
		return campaignEnabled
	}
	return campaignDisabled
}

type electionManagerMetrics struct {
	campaignCreateErrors                   tally.Counter
	campaignErrors                         tally.Counter
	campaignUnknownState                   tally.Counter
	campaignCheckErrors                    tally.Counter
	campaignCheckHasActiveShards           tally.Counter
	campaignCheckNoCutoverShards           tally.Counter
	campaignCheckFlushTimesErrors          tally.Counter
	campaignCheckReplacementInstanceErrors tally.Counter
	campaignCheckHasReplacementInstance    tally.Counter
	campaignCheckUnexpectedShardTimes      tally.Counter
	verifyLeaderErrors                     tally.Counter
	verifyLeaderNotChanged                 tally.Counter
	verifyCampaignDisabled                 tally.Counter
	verifyPendingChangeStale               tally.Counter
	verifyPlacementErrors                  tally.Counter
	verifyInstanceErrors                   tally.Counter
	verifyLeaderNotInPlacement             tally.Counter
	followerResign                         tally.Counter
	resignTimeout                          tally.Counter
	resignErrors                           tally.Counter
	resignOnCloseSuccess                   tally.Counter
	resignOnCloseErrors                    tally.Counter
	resignOnClose                          tally.Gauge
	followerToPendingFollower              tally.Counter
	electionState                          tally.Gauge
	campaignState                          tally.Gauge
	campaigning                            tally.Gauge
	leadersWithActiveShards                tally.Gauge
	followersWithActiveShards              tally.Gauge
}

func newElectionManagerMetrics(scope tally.Scope) electionManagerMetrics {
	campaignScope := scope.SubScope("campaign")
	campaignCheckScope := scope.SubScope("campaign-check")
	verifyScope := scope.SubScope("verify")
	resignScope := scope.SubScope("resign")
	return electionManagerMetrics{
		campaignCreateErrors:                   campaignScope.Counter("create-errors"),
		campaignErrors:                         campaignScope.Counter("errors"),
		campaignUnknownState:                   campaignScope.Counter("unknown-state"),
		campaignCheckErrors:                    campaignCheckScope.Counter("errors"),
		campaignCheckHasActiveShards:           campaignCheckScope.Counter("has-active-shards"),
		campaignCheckNoCutoverShards:           campaignCheckScope.Counter("no-cutover-shards"),
		campaignCheckFlushTimesErrors:          campaignCheckScope.Counter("flush-times-errors"),
		campaignCheckReplacementInstanceErrors: campaignCheckScope.Counter("repl-instance-errors"),
		campaignCheckHasReplacementInstance:    campaignCheckScope.Counter("has-repl-instance"),
		campaignCheckUnexpectedShardTimes:      campaignCheckScope.Counter("unexpected-shard-times"),
		verifyLeaderErrors:                     verifyScope.Counter("leader-errors"),
		verifyLeaderNotChanged:                 verifyScope.Counter("leader-not-changed"),
		verifyCampaignDisabled:                 verifyScope.Counter("campaign-disabled"),
		verifyPendingChangeStale:               verifyScope.Counter("pending-change-stale"),
		verifyPlacementErrors:                  verifyScope.Counter("placement-errors"),
		verifyInstanceErrors:                   verifyScope.Counter("instance-errors"),
		verifyLeaderNotInPlacement:             verifyScope.Counter("leader-not-in-placement"),
		followerResign:                         resignScope.Counter("follower-resign"),
		resignTimeout:                          resignScope.Counter("timeout"),
		resignErrors:                           resignScope.Counter("errors"),
		resignOnCloseSuccess:                   resignScope.Counter("on-close-success"),
		resignOnCloseErrors:                    resignScope.Counter("on-close-errors"),
		resignOnClose:                          resignScope.Gauge("on-close"),
		followerToPendingFollower:              scope.Counter("follower-to-pending-follower"),
		electionState:                          scope.Gauge("election-state"),
		campaignState:                          scope.Gauge("campaign-state"),
		campaigning:                            scope.Gauge("campaigning"),
		leadersWithActiveShards:                scope.Gauge("leaders-with-active-shards"),
		followersWithActiveShards:              scope.Gauge("follower-with-active-shards"),
	}
}

type goalState struct {
	id    int64
	state ElectionState
}

type campaignIsEnabledFn func() (bool, error)

// nolint: maligned
type electionManager struct {
	sync.RWMutex
	sync.WaitGroup

	nowFn                      clock.NowFn
	logger                     *zap.Logger
	reportInterval             time.Duration
	campaignOpts               services.CampaignOptions
	electionOpts               services.ElectionOptions
	campaignRetrier            retry.Retrier
	changeRetrier              retry.Retrier
	resignRetrier              retry.Retrier
	electionKeyFmt             string
	leaderService              services.LeaderService
	leaderValue                string
	placementManager           PlacementManager
	flushTimesManager          FlushTimesManager
	flushTimesChecker          flushTimesChecker
	campaignStateCheckInterval time.Duration
	shardCutoffCheckOffset     time.Duration

	state                  electionManagerState
	doneCh                 chan struct{}
	campaigning            int32
	campaignStateWatchable watch.Watchable
	electionKey            string
	electionStateWatchable watch.Watchable
	nextGoalStateID        int64
	goalStateLock          *sync.RWMutex
	goalStateWatchable     watch.Watchable
	campaignIsEnabledFn    campaignIsEnabledFn
	resignOnClose          int32
	sleepFn                sleepFn
	metrics                electionManagerMetrics
}

// NewElectionManager creates a new election manager.
func NewElectionManager(opts ElectionManagerOptions) ElectionManager {
	instrumentOpts := opts.InstrumentOptions()
	scope := instrumentOpts.MetricsScope()
	campaignOpts := opts.CampaignOptions()
	campaignRetrier := retry.NewRetrier(opts.CampaignRetryOptions().SetForever(true))
	changeRetrier := retry.NewRetrier(opts.ChangeRetryOptions().SetForever(true))
	resignRetrier := retry.NewRetrier(opts.ResignRetryOptions().SetForever(true))
	mgr := &electionManager{
		nowFn:                      opts.ClockOptions().NowFn(),
		logger:                     instrumentOpts.Logger(),
		reportInterval:             instrumentOpts.ReportInterval(),
		campaignOpts:               campaignOpts,
		electionOpts:               opts.ElectionOptions(),
		campaignRetrier:            campaignRetrier,
		changeRetrier:              changeRetrier,
		resignRetrier:              resignRetrier,
		electionKeyFmt:             opts.ElectionKeyFmt(),
		leaderService:              opts.LeaderService(),
		leaderValue:                campaignOpts.LeaderValue(),
		placementManager:           opts.PlacementManager(),
		flushTimesManager:          opts.FlushTimesManager(),
		flushTimesChecker:          newFlushTimesChecker(scope.SubScope("campaign-check")),
		campaignStateCheckInterval: opts.CampaignStateCheckInterval(),
		shardCutoffCheckOffset:     opts.ShardCutoffCheckOffset(),
		sleepFn:                    time.Sleep,
		metrics:                    newElectionManagerMetrics(scope),
	}
	mgr.campaignIsEnabledFn = mgr.campaignIsEnabled
	mgr.Lock()
	mgr.resetWithLock()
	mgr.Unlock()
	return mgr
}

func (mgr *electionManager) Reset() error {
	mgr.Lock()
	defer mgr.Unlock()

	switch mgr.state {
	case electionManagerNotOpen:
		return nil
	case electionManagerOpen:
		return errElectionManagerOpen
	default:
		mgr.resetWithLock()
		return nil
	}
}

func (mgr *electionManager) Open(shardSetID uint32) error {
	mgr.Lock()
	defer mgr.Unlock()

	if mgr.state != electionManagerNotOpen {
		return errElectionManagerAlreadyOpenOrClosed
	}
	mgr.electionKey = fmt.Sprintf(mgr.electionKeyFmt, shardSetID)
	_, stateChangeWatch, err := mgr.goalStateWatchable.Watch()
	if err != nil {
		return err
	}
	_, verifyWatch, err := mgr.goalStateWatchable.Watch()
	if err != nil {
		return err
	}
	_, campaignStateWatch, err := mgr.campaignStateWatchable.Watch()
	if err != nil {
		return err
	}
	mgr.state = electionManagerOpen

	mgr.Add(5)
	go mgr.watchGoalStateChanges(stateChangeWatch)
	go mgr.verifyPendingFollower(verifyWatch)
	go mgr.checkCampaignStateLoop()
	go mgr.campaignLoop(campaignStateWatch)
	go mgr.reportMetrics()

	mgr.logger.Info("election manager opened successfully")
	return nil
}

func (mgr *electionManager) ElectionState() ElectionState {
	return mgr.electionStateWatchable.Get().(ElectionState)
}

func (mgr *electionManager) IsCampaigning() bool {
	return mgr.campaignState() == campaignEnabled
}

func (mgr *electionManager) Resign(ctx context.Context) error {
	mgr.RLock()
	state := mgr.state
	mgr.RUnlock()
	if state != electionManagerOpen {
		return errElectionManagerNotOpenOrClosed
	}

	_, watch, err := mgr.electionStateWatchable.Watch()
	if err != nil {
		return fmt.Errorf("error creating watch when resigning: %v", err)
	}
	defer watch.Close()
	if electionState := watch.Get().(ElectionState); electionState == FollowerState {
		mgr.metrics.followerResign.Inc(1)
		return nil
	}

	ctxNotDone := func(int) bool {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	// Log the context error because the error returned from the retrier is not helpful.
	if err := mgr.resignWhile(ctxNotDone); err != nil {
		mgr.metrics.resignTimeout.Inc(1)
		mgr.logError("resign error", ctx.Err())
		return ctx.Err()
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
	if mgr.state != electionManagerOpen {
		mgr.Unlock()
		return errElectionManagerNotOpenOrClosed
	}
	close(mgr.doneCh)
	mgr.state = electionManagerClosed
	mgr.Unlock()

	mgr.Wait()
	mgr.campaignStateWatchable.Close()
	mgr.electionStateWatchable.Close()
	mgr.goalStateWatchable.Close()
	return nil
}

func (mgr *electionManager) watchGoalStateChanges(watch watch.Watch) {
	defer func() {
		watch.Close()
		mgr.Done()
	}()

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
	mgr.logger.Info(fmt.Sprintf("election state changed from %v to %v", currState, newState))
}

func (mgr *electionManager) verifyPendingFollower(watch watch.Watch) {
	defer func() {
		watch.Close()
		mgr.Done()
	}()

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

		stateUnchangedFn := func() bool {
			mgr.goalStateLock.RLock()
			latest := mgr.goalStateWatchable.Get().(goalState)
			mgr.goalStateLock.RUnlock()
			return currState.id == latest.id && currState.state == latest.state
		}

		// Only continue verifying if the state has not changed and campaigning is not disabled.
		continueFn := func(int) bool {
			return stateUnchangedFn() && mgr.campaignState() != campaignDisabled
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
			p, err := mgr.placementManager.Placement()
			if err != nil {
				mgr.metrics.verifyPlacementErrors.Inc(1)
				mgr.logError("error getting placement", err)
				return err
			}
			leaderInstance, exist := p.Instance(leader)
			if !exist {
				mgr.metrics.verifyLeaderNotInPlacement.Inc(1)
				err := fmt.Errorf("received invalid leader value: [%s], which is not available in placement", leader)
				mgr.logError("invalid leader value", err)
				return err
			}
			instance, err := mgr.placementManager.Instance()
			if err != nil {
				mgr.metrics.verifyInstanceErrors.Inc(1)
				mgr.logError("error getting instance", err)
				return err
			}
			if leaderInstance.ShardSetID() != instance.ShardSetID() {
				err := fmt.Errorf("received invalid leader value: [%s] which is in shardSet group %v",
					leader, leaderInstance.ShardSetID())
				mgr.logError("invalid leader value", err)
				return err
			}
			mgr.logger.Info("found valid new leader for the campaign", zap.String("leader", leader))
			return nil
		}); verifyErr != nil {
			// If state has changed, we skip this stale change.
			if !stateUnchangedFn() {
				continue
			}
			// Otherwise the campaign is disabled and there is no need to verify leader.
			mgr.metrics.verifyCampaignDisabled.Inc(1)
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
		mgr.logger.Info("goal state changed to follower")
		mgr.goalStateLock.Unlock()
	}
}

func (mgr *electionManager) campaignState() campaignState {
	return mgr.campaignStateWatchable.Get().(campaignState)
}

func (mgr *electionManager) checkCampaignState() {
	enabled, err := mgr.campaignIsEnabledFn()
	if err != nil {
		mgr.metrics.campaignCheckErrors.Inc(1)
		return
	}
	newState := newCampaignState(enabled)
	currState := mgr.campaignState()
	if currState == newState {
		return
	}
	mgr.processCampaignStateChange(newState)
}

func (mgr *electionManager) checkCampaignStateLoop() {
	defer mgr.Done()

	ticker := time.NewTicker(mgr.campaignStateCheckInterval)
	defer ticker.Stop()

	for {
		mgr.checkCampaignState()
		select {
		case <-ticker.C:
		case <-mgr.doneCh:
			return
		}
	}
}

func (mgr *electionManager) processCampaignStateChange(newState campaignState) {
	switch newState {
	case campaignEnabled:
		mgr.campaignStateWatchable.Update(campaignEnabled)
	case campaignDisabled:
		mgr.campaignStateWatchable.Update(campaignPendingDisabled)
		// NB(xichen): if campaign should be disabled, we need to resign from
		// any ongoing campaign and retry on errors until either we succeed or
		// the campaign becomes enabled.
		var (
			enabled     bool
			campaignErr error
		)
		shouldResignFn := func(int) bool {
			enabled, campaignErr = mgr.campaignIsEnabledFn()
			if campaignErr != nil {
				mgr.metrics.campaignCheckErrors.Inc(1)
				return false
			}
			return !enabled
		}
		if err := mgr.resignWhile(shouldResignFn); err == nil {
			mgr.campaignStateWatchable.Update(campaignDisabled)
		} else if enabled {
			mgr.campaignStateWatchable.Update(campaignEnabled)
		}
	}
}

func (mgr *electionManager) campaignIsEnabled() (bool, error) {
	// If the current instance is not found in the placement, campaigning is disabled.
	shards, err := mgr.placementManager.Shards()
	if err == ErrInstanceNotFoundInPlacement {
		mgr.logger.Warn("campaign is not enabled", zap.Error(ErrInstanceNotFoundInPlacement))
		return false, nil
	}
	if err != nil {
		mgr.logger.Warn("campaign is not enabled", zap.Error(err))
		return false, err
	}

	if shards.NumShards() == 0 {
		return true, nil
	}

	// NB(xichen): We apply an offset when checking if a shard has been cutoff in order
	// to detect if the campaign should be stopped before all the shards are cut off.
	// This is to avoid the situation where the campaign is stopped after the shards
	// are cut off, and the instance gets promoted to leader before the campaign is stopped,
	// causing incomplete data to be flushed.
	var (
		nowNanos        = mgr.nowFn().UnixNano()
		noCutoverShards = true
		allCutoffShards = true
		allShards       = shards.All()
	)
	for _, shard := range allShards {
		hasCutover := nowNanos >= shard.CutoverNanos()
		hasNotCutoff := nowNanos < shard.CutoffNanos()-int64(mgr.shardCutoffCheckOffset)
		if hasCutover && hasNotCutoff {
			mgr.metrics.campaignCheckHasActiveShards.Inc(1)
			if mgr.ElectionState() == LeaderState {
				mgr.metrics.leadersWithActiveShards.Update(float64(1))
			}
			if mgr.ElectionState() == FollowerState {
				mgr.metrics.followersWithActiveShards.Update(float64(1))
			}
			return true, nil
		}
		noCutoverShards = noCutoverShards && !hasCutover
		allCutoffShards = allCutoffShards && !hasNotCutoff
	}

	// If no shards have been cut over, campaign is disabled to avoid writing
	// incomplete data before shards are cut over.
	if noCutoverShards {
		mgr.metrics.campaignCheckNoCutoverShards.Inc(1)
		mgr.logger.Warn("campaign is not enabled, no cutover shards")
		return false, nil
	}

	// If all shards have been cut off, campaigning is disabled when:
	// * The flush times persisted in kv are no earlier than the shards' cutoff times
	//   indicating this instance's data have been consumed downstream and are no longer
	//   needed, or
	// * There is an instance with the same shard set id replacing the current instance,
	//   indicating the replacement instance has a copy of this instance's data and as
	//   such this instance's data are no longer needed.
	if allCutoffShards {
		multiErr := xerrors.NewMultiError()

		// Check flush times persisted in kv.
		flushTimes, err := mgr.flushTimesManager.Get()
		if err != nil {
			multiErr = multiErr.Add(err)
			mgr.metrics.campaignCheckFlushTimesErrors.Inc(1)
		} else {
			allFlushed := true
			for _, shard := range allShards {
				if hasFlushedTillCutoff := mgr.flushTimesChecker.HasFlushed(
					shard.ID(),
					shard.CutoffNanos(),
					flushTimes,
				); !hasFlushedTillCutoff {
					allFlushed = false
					break
				}
			}
			if allFlushed {
				mgr.logger.Warn("campaign is not enabled, all shards cutoff")
				return false, nil
			}
		}

		// Check if there is a replacement instance.
		hasReplacementInstance, err := mgr.placementManager.HasReplacementInstance()
		if err != nil {
			multiErr = multiErr.Add(err)
			mgr.metrics.campaignCheckReplacementInstanceErrors.Inc(1)
		} else if hasReplacementInstance {
			mgr.logger.Warn("campaign is not enabled, there is a replacement instance")
			mgr.metrics.campaignCheckHasReplacementInstance.Inc(1)
			return false, nil
		}

		if err := multiErr.FinalError(); err != nil {
			return false, err
		}
		return true, nil
	}

	// If we get here, it means no shards are active, and some shards have not yet
	// cut over, and the other shards have been cut off, which is not expected in
	// supported topology changes.
	mgr.metrics.campaignCheckUnexpectedShardTimes.Inc(1)
	return false, errUnexpectedShardCutoverCutoffTimes
}

func (mgr *electionManager) campaignLoop(campaignStateWatch watch.Watch) {
	defer mgr.Done()

	var campaignStatusCh <-chan campaign.Status
	shouldCampaignFn := func(int) bool {
		select {
		case <-mgr.doneCh:
			return false
		default:
			campaignState := campaignStateWatch.Get().(campaignState)
			return campaignState == campaignEnabled
		}
	}

	for {
		if campaignStatusCh == nil {
			if err := mgr.campaignRetrier.AttemptWhile(shouldCampaignFn, func() error {
				var err error
				campaignStatusCh, err = mgr.leaderService.Campaign(mgr.electionKey, mgr.campaignOpts)
				if err == nil {
					return nil
				}
				mgr.metrics.campaignCreateErrors.Inc(1)
				mgr.logError("error creating campaign", err)
				return err
			}); err == nil {
				atomic.StoreInt32(&mgr.campaigning, 1)
			} else {
				// If we get here, the campaign failed and either the manager is closed or
				// the campaign is disabled. If the manager is closed, we return immediately.
				// Otherwise we wait for a change in the campaign enabled status before continuing
				// campaigning.
				select {
				case <-mgr.doneCh:
					return
				case <-campaignStateWatch.C():
					continue
				}
			}
		}

		select {
		case campaignStatus, ok := <-campaignStatusCh:
			// If the campaign status channel is closed, this is either because session has expired,
			// or we have resigned from the campaign, or there are issues with the underlying etcd
			// cluster, in which case we back off a little and restart the campaign.
			if !ok {
				campaignStatusCh = nil
				atomic.StoreInt32(&mgr.campaigning, 0)
				mgr.sleepFn(backOffOnResignOrElectionError)
				continue
			}
			mgr.processCampaignUpdate(campaignStatus)
		case <-mgr.doneCh:
			electionKey := mgr.electionKey
			// Asynchronously resign from ongoing campaign on close to avoid blocking the close
			// call while still ensuring there are no lingering campaigns that are kept alive
			// after the campaign manager is closed.
			go func() {
				atomic.AddInt32(&mgr.resignOnClose, 1)
				if err := mgr.leaderService.Resign(electionKey); err != nil {
					mgr.metrics.resignOnCloseErrors.Inc(1)
				} else {
					mgr.metrics.resignOnCloseSuccess.Inc(1)
				}
				atomic.AddInt32(&mgr.resignOnClose, -1)
			}()
			return
		}
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

func (mgr *electionManager) resetWithLock() {
	mgr.state = electionManagerNotOpen
	mgr.doneCh = make(chan struct{})
	mgr.campaigning = 0
	mgr.campaignStateWatchable = watch.NewWatchable()
	mgr.campaignStateWatchable.Update(campaignDisabled)
	mgr.electionStateWatchable = watch.NewWatchable()
	mgr.electionStateWatchable.Update(FollowerState)
	mgr.nextGoalStateID = 0
	mgr.goalStateLock = &sync.RWMutex{}
	mgr.goalStateWatchable = watch.NewWatchable()
}

func (mgr *electionManager) resignWhile(continueFn retry.ContinueFn) error {
	return mgr.resignRetrier.AttemptWhile(continueFn, func() error {
		if err := mgr.leaderService.Resign(mgr.electionKey); err != nil {
			mgr.metrics.resignErrors.Inc(1)
			mgr.logError("resign error", err)
			return err
		}
		return nil
	})
}

func (mgr *electionManager) reportMetrics() {
	defer mgr.Done()

	ticker := time.NewTicker(mgr.reportInterval)
	for {
		select {
		case <-ticker.C:
			electionState := mgr.ElectionState()
			campaignState := mgr.campaignState()
			campaigning := atomic.LoadInt32(&mgr.campaigning)
			resignOnClose := atomic.LoadInt32(&mgr.resignOnClose)
			mgr.metrics.electionState.Update(float64(electionState))
			mgr.metrics.campaignState.Update(float64(campaignState))
			mgr.metrics.campaigning.Update(float64(campaigning))
			mgr.metrics.resignOnClose.Update(float64(resignOnClose))
		case <-mgr.doneCh:
			ticker.Stop()
			return
		}
	}
}

func (mgr *electionManager) logError(desc string, err error) {
	mgr.logger.Error(desc,
		zap.String("electionKey", mgr.electionKey),
		zap.Duration("electionTTL", time.Duration(mgr.electionOpts.TTLSecs())*time.Second),
		zap.String("leaderValue", mgr.campaignOpts.LeaderValue()),
		zap.Error(err),
	)
}
