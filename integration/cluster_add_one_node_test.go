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
	"testing"
	"time"

	"github.com/m3db/m3db/integration/fake"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/topology"
	"github.com/m3db/m3db/ts"
	xlog "github.com/m3db/m3x/log"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/shard"
	"github.com/stretchr/testify/require"
)

func TestClusterAddOneNode(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// Test setups
	log := xlog.SimpleLogger

	namesp := namespace.NewMetadata(testNamespaces[0], namespace.NewOptions())
	opts := newTestOptions().
		SetNamespaces([]namespace.Metadata{namesp})

	instances := struct {
		start []services.ServiceInstance
		add   []services.ServiceInstance
		added []services.ServiceInstance
	}{
		start: []services.ServiceInstance{
			node(t, 0, newClusterShardsRange(0, 1023, shard.Available)),
			node(t, 1, newClusterEmptyShardsRange()),
		},

		add: []services.ServiceInstance{
			node(t, 0, concatShards(
				newClusterShardsRange(0, 511, shard.Available),
				newClusterShardsRange(512, 1023, shard.Leaving))),
			node(t, 1, newClusterShardsRange(512, 1023, shard.Initializing)),
		},
		added: []services.ServiceInstance{
			node(t, 0, newClusterShardsRange(0, 511, shard.Available)),
			node(t, 1, newClusterShardsRange(512, 1023, shard.Available)),
		},
	}

	svc := fake.NewM3ClusterService().
		SetInstances(instances.start).
		SetReplication(services.NewServiceReplication().SetReplicas(1)).
		SetSharding(services.NewServiceSharding().SetNumShards(1024))

	svcs := fake.NewM3ClusterServices()
	svcs.RegisterService("m3db", svc)

	topoOpts := topology.NewDynamicOptions().
		SetConfigServiceClient(fake.NewM3ClusterClient(svcs, nil))
	topoInit := topology.NewDynamicInitializer(topoOpts)
	retentionOpts := retention.NewOptions().
		SetRetentionPeriod(6 * time.Hour).
		SetBlockSize(2 * time.Hour).
		SetBufferPast(10 * time.Minute).
		SetBufferFuture(2 * time.Minute).
		SetBufferDrain(3 * time.Second)
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
	setups, closeFn := newDefaultBootstrappableTestSetups(t, opts, retentionOpts, setupOpts)
	defer closeFn()

	// Write test data for first node
	topo, err := topoInit.Init()
	require.NoError(t, err)
	ids := []struct {
		str   string
		shard uint32
	}{
		{"foobarqux", 902},
		{"bar", 397},
		{"baz", 234},
	}
	shardSet := topo.Get().ShardSet()
	for _, id := range ids {
		// Verify IDs will map to halves of the shard space
		require.Equal(t, id.shard, shardSet.Lookup(ts.StringID(id.str)))
	}

	now := setups[0].getNowFn()
	blockSize := setups[0].storageOpts.RetentionOptions().BlockSize()
	seriesMaps := generateTestDataByStart([]testData{
		{ids: []string{ids[0].str, ids[1].str}, numPoints: 180, start: now.Add(-blockSize)},
		{ids: []string{ids[0].str, ids[2].str}, numPoints: 90, start: now},
	})
	err = writeTestDataToDisk(t, namesp.ID(), setups[0], seriesMaps)
	require.NoError(t, err)

	// Prepare verfication of data on nodes
	expectedSeriesMaps := make([]map[time.Time]seriesList, 2)
	expectedSeriesIDs := make([]map[string]struct{}, 2)
	for i := range expectedSeriesMaps {
		expectedSeriesMaps[i] = make(map[time.Time]seriesList)
		expectedSeriesIDs[i] = make(map[string]struct{})
	}
	for start, series := range seriesMaps {
		list := make([]seriesList, 2)
		for j := range series {
			if shardSet.Lookup(series[j].ID) < 512 {
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
	waitUntilHasBootstrappedShardsExactly(setups[1].db, newShardsRange(512, 1023))

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
	waitUntilHasBootstrappedShardsExactly(setups[0].db, newShardsRange(0, 511))
	waitUntilHasBootstrappedShardsExactly(setups[1].db, newShardsRange(512, 1023))

	log.Debug("verifying data in servers matches expected data set")

	// Verify in-memory data match what we expect
	for i := range setups {
		verifySeriesMaps(t, setups[i], namesp.ID(), expectedSeriesMaps[i])
	}
}
