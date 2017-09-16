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
	"time"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/retry"
)

const (
	defaultElectionKeyFormat          = "/shardset/%d/lock"
	defaultCampaignStateCheckInterval = time.Second
	defaultShardCutoffCheckOffset     = 30 * time.Second
)

// ElectionManagerOptions provide a set of options for the election manager.
type ElectionManagerOptions interface {
	// SetClockOptions sets the clock options.
	SetClockOptions(value clock.Options) ElectionManagerOptions

	// ClockOptions returns the clock options.
	ClockOptions() clock.Options

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) ElectionManagerOptions

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetElectionOptions sets the election options.
	SetElectionOptions(value services.ElectionOptions) ElectionManagerOptions

	// ElectionOptions returns the election options.
	ElectionOptions() services.ElectionOptions

	// SetCampaignOptions sets the campaign options.
	SetCampaignOptions(value services.CampaignOptions) ElectionManagerOptions

	// CampaignOptions returns the campaign options.
	CampaignOptions() services.CampaignOptions

	// SetCampaignRetryOptions sets the campaign retry options.
	SetCampaignRetryOptions(value retry.Options) ElectionManagerOptions

	// CampaignRetryOptions returns the campaign retry options.
	CampaignRetryOptions() retry.Options

	// SetChangeRetryOptions sets the change retry options.
	SetChangeRetryOptions(value retry.Options) ElectionManagerOptions

	// ChangeRetryOptions returns the change retry options.
	ChangeRetryOptions() retry.Options

	// SetResignRetryOptions sets the resign retry options.
	SetResignRetryOptions(value retry.Options) ElectionManagerOptions

	// ResignRetryOptions returns the resign retry options
	ResignRetryOptions() retry.Options

	// SetElectionKeyFmt sets the election key format.
	SetElectionKeyFmt(value string) ElectionManagerOptions

	// ElectionKeyFmt returns the election key format.
	ElectionKeyFmt() string

	// SetLeaderService sets the leader service.
	SetLeaderService(value services.LeaderService) ElectionManagerOptions

	// LeaderService returns the leader service.
	LeaderService() services.LeaderService

	// SetPlacementManager sets the placement manager.
	SetPlacementManager(value PlacementManager) ElectionManagerOptions

	// PlacementManager returns the placement manager.
	PlacementManager() PlacementManager

	// SetFlushTimesManager sets the flush times manager.
	SetFlushTimesManager(value FlushTimesManager) ElectionManagerOptions

	// FlushTimesManager returns the flush times manager.
	FlushTimesManager() FlushTimesManager

	// SetCampaignStateCheckInterval sets the interval to check campaign state.
	SetCampaignStateCheckInterval(value time.Duration) ElectionManagerOptions

	// CampaignStateCheckInterval returns the interval to check campaign state.
	CampaignStateCheckInterval() time.Duration

	// SetShardCutoffCheckOffset sets the offset when checking if a shard has been cutoff.
	// The cutoff time is applied in order to stop campaignining when necessary before all
	// shards are cut off avoiding incomplete data to be flushed.
	SetShardCutoffCheckOffset(value time.Duration) ElectionManagerOptions

	// ShardCutoffCheckOffset returns the offset when checking if a shard has been cutoff.
	// The cutoff time is applied in order to stop campaignining when necessary before all
	// shards are cut off avoiding incomplete data to be flushed.
	ShardCutoffCheckOffset() time.Duration
}

type electionManagerOptions struct {
	clockOpts                  clock.Options
	instrumentOpts             instrument.Options
	electionOpts               services.ElectionOptions
	campaignOpts               services.CampaignOptions
	campaignRetryOpts          retry.Options
	changeRetryOpts            retry.Options
	resignRetryOpts            retry.Options
	electionKeyFmt             string
	leaderService              services.LeaderService
	placementManager           PlacementManager
	flushTimesManager          FlushTimesManager
	campaignStateCheckInterval time.Duration
	shardCutoffCheckOffset     time.Duration
}

// NewElectionManagerOptions create a new set of options for the election manager.
func NewElectionManagerOptions() ElectionManagerOptions {
	return &electionManagerOptions{
		clockOpts:                  clock.NewOptions(),
		instrumentOpts:             instrument.NewOptions(),
		electionOpts:               services.NewElectionOptions(),
		campaignRetryOpts:          retry.NewOptions(),
		changeRetryOpts:            retry.NewOptions(),
		resignRetryOpts:            retry.NewOptions(),
		electionKeyFmt:             defaultElectionKeyFormat,
		campaignStateCheckInterval: defaultCampaignStateCheckInterval,
		shardCutoffCheckOffset:     defaultShardCutoffCheckOffset,
	}
}

func (o *electionManagerOptions) SetClockOptions(value clock.Options) ElectionManagerOptions {
	opts := *o
	opts.clockOpts = value
	return &opts
}

func (o *electionManagerOptions) ClockOptions() clock.Options {
	return o.clockOpts
}

func (o *electionManagerOptions) SetInstrumentOptions(value instrument.Options) ElectionManagerOptions {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *electionManagerOptions) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *electionManagerOptions) SetElectionOptions(value services.ElectionOptions) ElectionManagerOptions {
	opts := *o
	opts.electionOpts = value
	return &opts
}

func (o *electionManagerOptions) ElectionOptions() services.ElectionOptions {
	return o.electionOpts
}

func (o *electionManagerOptions) SetCampaignOptions(value services.CampaignOptions) ElectionManagerOptions {
	opts := *o
	opts.campaignOpts = value
	return &opts
}

func (o *electionManagerOptions) CampaignOptions() services.CampaignOptions {
	return o.campaignOpts
}

func (o *electionManagerOptions) SetCampaignRetryOptions(value retry.Options) ElectionManagerOptions {
	opts := *o
	opts.campaignRetryOpts = value
	return &opts
}

func (o *electionManagerOptions) CampaignRetryOptions() retry.Options {
	return o.campaignRetryOpts
}

func (o *electionManagerOptions) SetChangeRetryOptions(value retry.Options) ElectionManagerOptions {
	opts := *o
	opts.changeRetryOpts = value
	return &opts
}

func (o *electionManagerOptions) ChangeRetryOptions() retry.Options {
	return o.changeRetryOpts
}

func (o *electionManagerOptions) SetResignRetryOptions(value retry.Options) ElectionManagerOptions {
	opts := *o
	opts.resignRetryOpts = value
	return &opts
}

func (o *electionManagerOptions) ResignRetryOptions() retry.Options {
	return o.resignRetryOpts
}

func (o *electionManagerOptions) SetElectionKeyFmt(value string) ElectionManagerOptions {
	opts := *o
	opts.electionKeyFmt = value
	return &opts
}

func (o *electionManagerOptions) ElectionKeyFmt() string {
	return o.electionKeyFmt
}

func (o *electionManagerOptions) SetLeaderService(value services.LeaderService) ElectionManagerOptions {
	opts := *o
	opts.leaderService = value
	return &opts
}

func (o *electionManagerOptions) LeaderService() services.LeaderService {
	return o.leaderService
}

func (o *electionManagerOptions) SetPlacementManager(value PlacementManager) ElectionManagerOptions {
	opts := *o
	opts.placementManager = value
	return &opts
}

func (o *electionManagerOptions) PlacementManager() PlacementManager {
	return o.placementManager
}

func (o *electionManagerOptions) SetFlushTimesManager(value FlushTimesManager) ElectionManagerOptions {
	opts := *o
	opts.flushTimesManager = value
	return &opts
}

func (o *electionManagerOptions) FlushTimesManager() FlushTimesManager {
	return o.flushTimesManager
}

func (o *electionManagerOptions) SetCampaignStateCheckInterval(value time.Duration) ElectionManagerOptions {
	opts := *o
	opts.campaignStateCheckInterval = value
	return &opts
}

func (o *electionManagerOptions) CampaignStateCheckInterval() time.Duration {
	return o.campaignStateCheckInterval
}

func (o *electionManagerOptions) SetShardCutoffCheckOffset(value time.Duration) ElectionManagerOptions {
	opts := *o
	opts.shardCutoffCheckOffset = value
	return &opts
}

func (o *electionManagerOptions) ShardCutoffCheckOffset() time.Duration {
	return o.shardCutoffCheckOffset
}
