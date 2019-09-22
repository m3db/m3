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

package aggregator

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/integration/etcd"

	"go.uber.org/zap"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/cluster/placement"
	xplacement "github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/services"

	// "github.com/m3db/m3/src/cluster/services/leader"
	"github.com/m3db/m3/src/cluster/shard"

	"github.com/stretchr/testify/require"
)

// newPlacement creates a new placement.
func newPlacement(
	id string,
	numShards int,
	shardSetID uint32,
) placement.Placement {
	shardSet := make([]shard.Shard, 0, numShards)
	for shardID := 0; shardID < numShards; shardID++ {
		shard := shard.NewShard(uint32(shardID)).
			SetState(shard.Available).
			SetCutoverNanos(0).
			SetCutoffNanos(math.MaxInt64)
		shardSet = append(shardSet, shard)
	}

	shards := shard.NewShards(shardSet)
	instance := placement.NewInstance().
		SetID(id).
		SetShards(shards).
		SetShardSetID(shardSetID).
		SetEndpoint(id)

	shardIDs := make([]uint32, numShards)
	for i := 0; i < numShards; i++ {
		shardIDs[i] = uint32(i)
	}

	return placement.NewPlacement().
		SetInstances([]placement.Instance{instance}).
		SetShards(shardIDs)
}

func setPlacement(
	key string,
	store kv.Store,
	pl placement.Placement,
) error {
	stagedPlacement := placement.NewStagedPlacement().
		SetPlacements([]placement.Placement{pl})
	stagedPlacementProto, err := stagedPlacement.Proto()
	if err != nil {
		return err
	}
	_, err = store.SetIfNotExists(key, stagedPlacementProto)
	return err
}

var (
	testEnvironment     = "testEnv"
	testServiceName     = "testSvc"
	testZone            = "testZone"
	testElectionTTLSecs = 5
)

func newLeaderService(t *testing.T, kv etcd.EmbeddedKV) services.LeaderService {
	etcdClient, err := kv.ConfigServiceClient()
	require.NoError(t, err)

	sid := services.NewServiceID().
		SetEnvironment(testEnvironment).
		SetName(testServiceName).
		SetZone(testZone)
	eopts := services.NewElectionOptions().
		SetTTLSecs(testElectionTTLSecs)
	svcs, err := etcdClient.Services(nil)
	require.NoError(t, err)

	leader, err := svcs.LeaderService(sid, eopts)
	require.NoError(t, err)
	return leader
}

func buildElectionManagerOptions(
	t *testing.T,
	placementKey string,
	store kv.Store,
	instanceID string,
	electionKeyFormat string,
	shardID uint32,
	leaderService services.LeaderService,
) ElectionManagerOptions {
	placementWatcherOpts := xplacement.NewStagedPlacementWatcherOptions().
		SetStagedPlacementKey(placementKey).
		SetStagedPlacementStore(store)
	placementWatcher := xplacement.NewStagedPlacementWatcher(placementWatcherOpts)
	placementManagerOpts := NewPlacementManagerOptions().
		SetInstanceID(instanceID).
		SetStagedPlacementWatcher(placementWatcher)
	placementManager := NewPlacementManager(placementManagerOpts)

	// Set up election manager.
	campaignOpts, err := services.NewCampaignOptions()
	require.NoError(t, err)
	campaignOpts = campaignOpts.SetLeaderValue(instanceID)

	require.NoError(t, placementManager.Open())
	return NewElectionManagerOptions().
		SetCampaignOptions(campaignOpts).
		SetElectionKeyFmt(electionKeyFormat).
		SetLeaderService(leaderService).
		SetPlacementManager(placementManager)
}

func TestElections(t *testing.T) {
	var (
		numShards         = 1024
		instanceID        = "localhost:6000"
		placementKey      = "/placement"
		electionKeyFormat = "/shardset/%d/lock"
		shardID           = uint32(0)
		store             = mem.NewStore()

		count            = 2
		electionManagers = make([]ElectionManager, 0, count)
	)

	// Placement setup.
	placement := newPlacement(instanceID, numShards, shardID)
	require.NoError(t, setPlacement(placementKey, store, placement))
	defaultElectionStateChangeTimeout := 30 * time.Second

	eOpts := etcd.
		NewOptions().
		SetZone(testZone).
		SetEnvironment(testEnvironment).
		SetServiceID(testServiceName)
	kv, err := etcd.New(eOpts)
	require.NoError(t, err)
	// etcdClient, err := kv.ConfigServiceClient()
	require.NoError(t, err)

	require.NoError(t, kv.Start())
	leaderService := newLeaderService(t, kv)
	electionManagerOpts := buildElectionManagerOptions(t, placementKey, store,
		instanceID, electionKeyFormat, shardID, leaderService)

	for i := 0; i < count; i++ {
		lg := electionManagerOpts.InstrumentOptions().Logger()
		lg = lg.With(zap.Int("instance", i))
		instrumentOptions := electionManagerOpts.InstrumentOptions().SetLogger(lg)

		opts := electionManagerOpts.SetInstrumentOptions(instrumentOptions)
		electionManagers = append(electionManagers,
			NewElectionManager(opts))
	}

	for _, manager := range electionManagers {
		//?
		manager.Open(shardID)
	}

	electionKey := fmt.Sprintf(electionKeyFormat, shardID)

	wait := func() error {
		time.Sleep(time.Second * 10)
		return nil
		deadline := time.Now().Add(defaultElectionStateChangeTimeout)
		var err error
		var str string
		fmt.Println("starting to wait", time.Now().Format("3:04:05PM"), deadline.Format("3:04:05PM"))
		for time.Now().Before(deadline) {
			str, err = leaderService.Leader(electionKey)
			fmt.Println("waiting", err, str, time.Now().Format("3:04:05PM"), deadline.Format("3:04:05PM"))
			if err == nil {
				fmt.Println(str)
				return nil
			}

			time.Sleep(time.Second)
			fmt.Println("waiting2", err, str, time.Now().Format("3:04:05PM"), deadline.Format("3:04:05PM"))
		}

		return err
	}

	runs := 5
	for j := 0; j < runs; j++ {
		require.NoError(t, wait())

		counts := 0
		for i, manager := range electionManagers {
			state := manager.ElectionState()
			if state == LeaderState {
				fmt.Println("Found a leader state", i)
				counts++
			}

			fmt.Println("election manager", i, manager.ElectionState())
		}

		if counts != 1 {
			fmt.Println("Multileader!", j, counts)
			t.FailNow()
		}

		fmt.Println("Terminating")
		require.NoError(t, kv.Close())
		fmt.Println("Terminated")
		require.NoError(t, kv.Start())
		fmt.Println("I'll be back")
	}
}
