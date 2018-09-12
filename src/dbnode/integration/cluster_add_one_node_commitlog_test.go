// +build integration

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

package integration

import (
	"strconv"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/integration/fake"
	"github.com/m3db/m3/src/dbnode/integration/generate"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/topology/testutil"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3x/ident"
	xlog "github.com/m3db/m3x/log"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func TestClusterAddOneNodeCommitlog(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// Test setups
	log := xlog.SimpleLogger

	namesp, err := namespace.NewMetadata(testNamespaces[0],
		namespace.NewOptions().SetRetentionOptions(
			retention.NewOptions().
				SetRetentionPeriod(6*time.Hour).
				SetBlockSize(2*time.Hour).
				SetBufferPast(10*time.Minute).
				SetBufferFuture(2*time.Minute)))
	require.NoError(t, err)
	opts := newTestOptions(t).
		SetNamespaces([]namespace.Metadata{namesp})

	minShard := uint32(0)
	maxShard := uint32(opts.NumShards()) - uint32(1)
	midShard := uint32((maxShard - minShard) / 2)

	instances := struct {
		start []services.ServiceInstance
		add   []services.ServiceInstance
		added []services.ServiceInstance
	}{
		start: []services.ServiceInstance{
			node(t, 0, newClusterShardsRange(minShard, maxShard, shard.Available)),
			node(t, 1, newClusterEmptyShardsRange()),
		},

		add: []services.ServiceInstance{
			node(t, 0, concatShards(
				newClusterShardsRange(minShard, midShard, shard.Available),
				newClusterShardsRange(midShard+1, maxShard, shard.Leaving))),
			node(t, 1, newClusterShardsRange(midShard+1, maxShard, shard.Initializing)),
		},
		added: []services.ServiceInstance{
			node(t, 0, newClusterShardsRange(minShard, midShard, shard.Available)),
			node(t, 1, newClusterShardsRange(midShard+1, maxShard, shard.Available)),
		},
	}

	svc := fake.NewM3ClusterService().
		SetInstances(instances.start).
		SetReplication(services.NewServiceReplication().SetReplicas(1)).
		SetSharding(services.NewServiceSharding().SetNumShards(opts.NumShards()))

	svcs := fake.NewM3ClusterServices()
	svcs.RegisterService("m3db", svc)

	topoOpts := topology.NewDynamicOptions().
		SetConfigServiceClient(fake.NewM3ClusterClient(svcs, nil))
	topoInit := topology.NewDynamicInitializer(topoOpts)
	setupOpts := []bootstrappableTestSetupOptions{
		{
			disablePeersBootstrapper: true,
			topologyInitializer:      topoInit,
		},
		{
			disablePeersBootstrapper: false,
			topologyInitializer:      topoInit,
		},
	}
	setups, closeFn := newDefaultBootstrappableTestSetups(t, opts, setupOpts)
	defer closeFn()

	// Write test data for first node
	topo, err := topoInit.Init()
	require.NoError(t, err)
	ids := []idShard{}

	// Boilerplate code to find two ID's that hash to the first half of the
	// shards, and one ID that hashes to the second half of the shards.
	shardSet := topo.Get().ShardSet()
	i := 0
	numFirstHalf := 0
	numSecondHalf := 0
	for {
		if numFirstHalf == 2 && numSecondHalf == 1 {
			break
		}
		idStr := strconv.Itoa(i)
		shard := shardSet.Lookup(ident.StringID(idStr))
		if shard < midShard && numFirstHalf < 2 {
			ids = append(ids, idShard{str: idStr, shard: shard})
			numFirstHalf++
		}
		if shard > midShard && numSecondHalf < 1 {
			ids = append(ids, idShard{idStr, shard})
			numSecondHalf++
		}
		i++
	}

	for _, id := range ids {
		// Verify IDs will map to halves of the shard space
		require.Equal(t, id.shard, shardSet.Lookup(ident.StringID(id.str)))
	}

	now := setups[0].getNowFn()
	blockSize := namesp.Options().RetentionOptions().BlockSize()
	seriesMaps := generate.BlocksByStart([]generate.BlockConfig{
		{IDs: []string{ids[0].str, ids[1].str}, NumPoints: 180, Start: now.Add(-blockSize)},
		{IDs: []string{ids[0].str, ids[2].str}, NumPoints: 90, Start: now},
	})
	err = writeTestDataToDisk(namesp, setups[0], seriesMaps)
	require.NoError(t, err)

	// Prepare verification of data on nodes
	expectedSeriesMaps := make([]map[xtime.UnixNano]generate.SeriesBlock, 2)
	expectedSeriesIDs := make([]map[string]struct{}, 2)
	for i := range expectedSeriesMaps {
		expectedSeriesMaps[i] = make(map[xtime.UnixNano]generate.SeriesBlock)
		expectedSeriesIDs[i] = make(map[string]struct{})
	}
	for start, series := range seriesMaps {
		list := make([]generate.SeriesBlock, 2)
		for j := range series {
			if shardSet.Lookup(series[j].ID) < midShard+1 {
				list[0] = append(list[0], series[j])
			} else {
				list[1] = append(list[1], series[j])
			}
		}
		for i := range expectedSeriesMaps {
			if len(list[i]) > 0 {
				expectedSeriesMaps[i][start] = list[i]
			}
		}
	}
	for i := range expectedSeriesMaps {
		for _, series := range expectedSeriesMaps[i] {
			for _, elem := range series {
				expectedSeriesIDs[i][elem.ID.String()] = struct{}{}
			}
		}
	}
	require.Equal(t, 2, len(expectedSeriesIDs[0]))
	require.Equal(t, 1, len(expectedSeriesIDs[1]))

	// Start the first server with filesystem bootstrapper
	require.NoError(t, setups[0].startServer())

	// Start the last server with peers and filesystem bootstrappers, no shards
	// are assigned at first
	require.NoError(t, setups[1].startServer())
	log.Debug("servers are now up")

	// Stop the servers at test completion
	defer func() {
		log.Debug("servers closing")
		setups.parallel(func(s *testSetup) {
			require.NoError(t, s.stopServer())
		})
		log.Debug("servers are now down")
	}()

	// Bootstrap the new shards
	log.Debug("resharding to initialize shards on second node")
	svc.SetInstances(instances.add)
	svcs.NotifyServiceUpdate("m3db")
	waitUntilHasBootstrappedShardsExactly(setups[1].db, testutil.Uint32Range(midShard+1, maxShard))

	log.Debug("waiting for shards to be marked initialized")
	allMarkedAvailable := func(
		fakePlacementService fake.M3ClusterPlacementService,
		instanceID string,
		shards []shard.Shard,
	) bool {
		markedAvailable := fakePlacementService.InstanceShardsMarkedAvailable()
		if len(markedAvailable) != 1 {
			return false
		}
		if len(markedAvailable[instanceID]) != len(shards) {
			return false
		}
		marked := shard.NewShards(nil)
		for _, id := range markedAvailable[instanceID] {
			marked.Add(shard.NewShard(id).SetState(shard.Available))
		}
		for _, shard := range shards {
			if !marked.Contains(shard.ID()) {
				return false
			}
		}
		return true
	}

	fps := svcs.FakePlacementService()
	shouldMark := instances.add[1].Shards().All()
	for !allMarkedAvailable(fps, "testhost1", shouldMark) {
		time.Sleep(100 * time.Millisecond)
	}
	log.Debug("all shards marked as initialized")

	// Shed the old shards from the first node
	log.Debug("resharding to shed shards from first node")
	svc.SetInstances(instances.added)
	svcs.NotifyServiceUpdate("m3db")
	waitUntilHasBootstrappedShardsExactly(setups[0].db, testutil.Uint32Range(minShard, midShard))
	waitUntilHasBootstrappedShardsExactly(setups[1].db, testutil.Uint32Range(midShard+1, maxShard))

	log.Debug("verifying data in servers matches expected data set")

	// Verify in-memory data match what we expect
	for i := range setups {
		verifySeriesMaps(t, setups[i], namesp.ID(), expectedSeriesMaps[i])
	}

	require.NoError(t, setups[1].stopServer())
	startServerWithNewInspection(t, opts, setups[1])
	verifySeriesMaps(t, setups[1], namesp.ID(), expectedSeriesMaps[1])
}
