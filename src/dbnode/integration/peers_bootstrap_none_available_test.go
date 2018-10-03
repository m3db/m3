// +build integration

// Copyright (c) 2018 Uber Technologies, Inc.
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
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/topology/testutil"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/shard"
	xlog "github.com/m3db/m3x/log"

	"github.com/stretchr/testify/require"
)

// TestPeersBootstrapNoneAvailable makes sure that a cluster with the peer bootstrapper enabled can still turn on
// if none of the nodes peers shard states are available or leaving.
func TestPeersBootstrapNoneAvailable(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// Test setups
	log := xlog.SimpleLogger
	retentionOpts := retention.NewOptions().
		SetRetentionPeriod(20 * time.Hour).
		SetBlockSize(2 * time.Hour).
		SetBufferPast(10 * time.Minute).
		SetBufferFuture(2 * time.Minute)
	namesp, err := namespace.NewMetadata(testNamespaces[0], namespace.NewOptions().SetRetentionOptions(retentionOpts))
	require.NoError(t, err)
	opts := newTestOptions(t).
		SetNamespaces([]namespace.Metadata{namesp})

	minShard := uint32(0)
	maxShard := uint32(opts.NumShards()) - uint32(1)
	start := []services.ServiceInstance{
		node(t, 0, newClusterShardsRange(minShard, maxShard, shard.Initializing)),
		node(t, 1, newClusterShardsRange(minShard, maxShard, shard.Initializing)),
	}

	hostShardSets := []topology.HostShardSet{}
	for _, instance := range start {
		h, err := topology.NewHostShardSetFromServiceInstance(instance, sharding.DefaultHashFn(int(maxShard)))
		require.NoError(t, err)
		hostShardSets = append(hostShardSets, h)
	}

	shards := testutil.ShardsRange(minShard, maxShard, shard.Initializing)
	shardSet, err := sharding.NewShardSet(
		shards,
		sharding.DefaultHashFn(int(maxShard)),
	)
	require.NoError(t, err)

	topoOpts := topology.NewStaticOptions().
		SetReplicas(2).
		SetHostShardSets(hostShardSets).
		SetShardSet(shardSet)
	topoInit := topology.NewStaticInitializer(topoOpts)

	setupOpts := []bootstrappableTestSetupOptions{
		{
			disablePeersBootstrapper:           false,
			fetchBlocksMetadataEndpointVersion: client.FetchBlocksMetadataEndpointV2,
			topologyInitializer:                topoInit,
		},
		{
			disablePeersBootstrapper:           false,
			fetchBlocksMetadataEndpointVersion: client.FetchBlocksMetadataEndpointV2,
			topologyInitializer:                topoInit,
		},
	}
	setups, closeFn := newDefaultBootstrappableTestSetups(t, opts, setupOpts)
	defer closeFn()

	serversAreUp := &sync.WaitGroup{}
	serversAreUp.Add(2)

	// Start both servers "simultaneously"
	go func() {
		if err := setups[0].startServer(); err != nil {
			panic(err)
		}
		serversAreUp.Done()
	}()
	go func() {
		if err := setups[1].startServer(); err != nil {
			panic(err)
		}
		serversAreUp.Done()
	}()

	serversAreUp.Wait()
	log.Debug("servers are now up")

	// Stop the servers
	defer func() {
		setups.parallel(func(s *testSetup) {
			require.NoError(t, s.stopServer())
		})
		log.Debug("servers are now down")
	}()
}
