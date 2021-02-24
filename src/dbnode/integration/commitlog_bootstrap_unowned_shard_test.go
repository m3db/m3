// +build integration

// Copyright (c) 2020 Uber Technologies, Inc.
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

	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/integration/fake"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/topology"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/stretchr/testify/require"
)

func TestCommitLogBootstrapUnownedShard(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	log := xtest.NewLogger(t)
	retentionOpts := retention.NewOptions().
		SetRetentionPeriod(20 * time.Hour).
		SetBlockSize(2 * time.Hour).
		SetBufferPast(10 * time.Minute).
		SetBufferFuture(10 * time.Minute)
	blockSize := retentionOpts.BlockSize()

	ns1, err := namespace.NewMetadata(testNamespaces[0], namespace.NewOptions().
		SetRetentionOptions(retentionOpts))
	require.NoError(t, err)
	numShards := 6

	// Helper function to create node instances for fake cluster service.
	node := func(index int, shards []uint32) services.ServiceInstance {
		id := fmt.Sprintf("testhost%d", index)
		endpoint := fmt.Sprintf("127.0.0.1:%d", multiAddrPortStart+(index*multiAddrPortEach))

		result := services.NewServiceInstance().
			SetInstanceID(id).
			SetEndpoint(endpoint)
		resultShards := make([]shard.Shard, len(shards))
		for i, id := range shards {
			resultShards[i] = shard.NewShard(id).SetState(shard.Available)
		}
		return result.SetShards(shard.NewShards(resultShards))
	}

	// Pretend there are two nodes sharing 6 shards (RF1).
	node0OwnedShards := []uint32{0, 1, 2}
	svc := fake.NewM3ClusterService().
		SetInstances([]services.ServiceInstance{
			node(0, node0OwnedShards),
			node(1, []uint32{3, 4, 5}),
		}).
		SetReplication(services.NewServiceReplication().SetReplicas(1)).
		SetSharding(services.NewServiceSharding().SetNumShards(numShards))
	svcs := fake.NewM3ClusterServices()
	svcs.RegisterService("m3db", svc)
	topoOpts := topology.NewDynamicOptions().
		SetConfigServiceClient(fake.NewM3ClusterClient(svcs, nil))
	topoInit := topology.NewDynamicInitializer(topoOpts)

	opts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{ns1}).
		SetNumShards(numShards)
	setupOpts := []BootstrappableTestSetupOptions{
		{DisablePeersBootstrapper: true, TopologyInitializer: topoInit},
		{DisablePeersBootstrapper: true, TopologyInitializer: topoInit},
	}

	setups, closeFn := NewDefaultBootstrappableTestSetups(t, opts, setupOpts)
	defer closeFn()

	// Only set this up for the first setup because we're only writing commit
	// logs for the first server.
	setup := setups[0]
	commitLogOpts := setup.StorageOpts().CommitLogOptions().
		SetFlushInterval(defaultIntegrationTestFlushInterval)
	setup.SetStorageOpts(setup.StorageOpts().SetCommitLogOptions(commitLogOpts))

	log.Info("generating data")
	now := setup.NowFn()()
	seriesMaps := generateSeriesMaps(30, nil, now.Add(-2*blockSize), now.Add(-blockSize))
	log.Info("writing data")
	// Write commit log with generated data that spreads across all shards
	// (including shards that this node should not own). This node should still
	// be able to bootstrap successfully with commit log entries from shards
	// that it does not own.
	writeCommitLogData(t, setup, commitLogOpts, seriesMaps, ns1, false)
	log.Info("finished writing data")

	// Setup bootstrapper after writing data so filesystem inspection can find it.
	setupCommitLogBootstrapperWithFSInspection(t, setup, commitLogOpts)

	// Start the servers.
	for _, setup := range setups {
		require.NoError(t, setup.StartServer())
	}

	// Defer stop the servers.
	defer func() {
		setups.parallel(func(s TestSetup) {
			require.NoError(t, s.StopServer())
		})
		log.Debug("servers are now down")
	}()

	// Only fetch blocks for shards owned by node 0.
	metadatasByShard, err := m3dbClientFetchBlocksMetadata(
		setup.M3DBVerificationAdminClient(), testNamespaces[0], node0OwnedShards,
		now.Add(-2*blockSize), now, topology.ReadConsistencyLevelMajority)
	require.NoError(t, err)

	observedSeriesMaps := testSetupToSeriesMaps(t, setup, ns1, metadatasByShard)
	// Filter out the written series that node 0 does not own.
	filteredSeriesMaps := filterSeriesByShard(setup, seriesMaps, node0OwnedShards)
	// Expect to only see data that node 0 owns.
	verifySeriesMapsEqual(t, filteredSeriesMaps, observedSeriesMaps)
}
