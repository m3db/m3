// Copyright (c) 2019 Uber Technologies, Inc.
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

package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/cluster/placement"
	xplacement "github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/services"

	"github.com/stretchr/testify/require"
)

func TestElections(t *testing.T) {
	opts := newTestServerOptions()

	// Placement setup.
	numShards := 1024
	cfg := placementInstanceConfig{
		instanceID:          opts.InstanceID(),
		shardSetID:          opts.ShardSetID(),
		shardStartInclusive: 0,
		shardEndExclusive:   uint32(numShards),
	}

	instance := cfg.newPlacementInstance()
	placement := newPlacement(numShards, []placement.Instance{instance})
	placementKey := opts.PlacementKVKey()
	placementStore := opts.KVStore()
	require.NoError(t, setPlacement(placementKey, placementStore, placement))

	// Set up placement manager.
	placementWatcherOpts := xplacement.NewStagedPlacementWatcherOptions().
		SetStagedPlacementKey(opts.PlacementKVKey()).
		SetStagedPlacementStore(opts.KVStore())
	placementWatcher := xplacement.NewStagedPlacementWatcher(placementWatcherOpts)
	placementManagerOpts := aggregator.NewPlacementManagerOptions().
		SetInstanceID(opts.InstanceID()).
		SetStagedPlacementWatcher(placementWatcher)
	placementManager := aggregator.NewPlacementManager(placementManagerOpts)

	// Set up election manager.
	leaderValue := opts.InstanceID()
	campaignOpts, err := services.NewCampaignOptions()
	require.NoError(t, err)
	campaignOpts = campaignOpts.SetLeaderValue(leaderValue)
	electionKey := fmt.Sprintf(opts.ShardSetID())

	electionCluster := newTestCluster(t)
	leaderService := electionCluster.LeaderService()
	electionManagerOpts := aggregator.NewElectionManagerOptions().
		SetCampaignOptions(campaignOpts).
		SetElectionKeyFmt("/shardset/%d/lock").
		SetLeaderService(leaderService).
		SetPlacementManager(placementManager)

	electionManager := aggregator.NewElectionManager(electionManagerOpts)
	electionManagerTwo := aggregator.NewElectionManager(electionManagerOpts)

	require.NoError(t, placementManager.Open())
	electionManager.Open(0)
	electionManagerTwo.Open(0)

	deadline := time.Now().Add(defaultElectionStateChangeTimeout)
	var str string
	for time.Now().Before(deadline) {
		str, err = leaderService.Leader(electionKey)
		if err == nil {
			break
		}

		time.Sleep(time.Second)
	}

	require.NoError(t, err)
	fmt.Println("STRING is", str)

	fmt.Println("election manager    ", electionManager.ElectionState())
	fmt.Println("election manager two", electionManagerTwo.ElectionState())
}
