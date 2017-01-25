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

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/topology"
	xlog "github.com/m3db/m3x/log"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/shard"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalQuorum(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Test setups
	log := xlog.SimpleLogger

	nspaces := []namespace.Metadata{
		namespace.NewMetadata(testNamespaces[0], namespace.NewOptions()),
	}
	opts := newTestOptions().SetNamespaces(nspaces)

	instances := []services.ServiceInstance{
		node(t, 0, newClusterShardsRange(0, 1023, shard.Available)),
		node(t, 1, newClusterShardsRange(0, 1023, shard.Available)),
		node(t, 2, newClusterShardsRange(0, 1023, shard.Available)),
	}

	svc := NewFakeM3ClusterService().
		SetInstances(instances).
		SetReplication(services.NewServiceReplication().SetReplicas(3)).
		SetSharding(services.NewServiceSharding().SetNumShards(1024))

	svcs := NewFakeM3ClusterServices()
	svcs.RegisterService("m3db", svc)

	topoOpts := topology.NewDynamicOptions().
		SetConfigServiceClient(NewM3FakeClusterClient(svcs, nil))
	topoInit := topology.NewDynamicInitializer(topoOpts)
	retentionOpts := retention.NewOptions().SetRetentionPeriod(6 * time.Hour)

	nodeOpt := bootstrappableTestSetupOptions{
		disablePeersBootstrapper: true,
		topologyInitializer:      topoInit,
	}
	nodeOpts := []bootstrappableTestSetupOptions{nodeOpt, nodeOpt, nodeOpt}

	// nodes = m3db nodes
	nodes, closeFn := newDefaultBootstrappableTestSetups(t, opts, retentionOpts, nodeOpts)
	defer closeFn()

	require.NoError(t, nodes[0].startServer())
	now := nodes[0].getNowFn()

	testWrite := func(cLevel topology.ConsistencyLevel) error {
		opts := client.NewOptions().
			SetClusterConnectConsistencyLevel(client.ConnectConsistencyLevelAny).
			SetClusterConnectTimeout(10 * time.Millisecond).
			SetWriteRequestTimeout(100 * time.Millisecond).
			SetTopologyInitializer(topoInit).
			SetWriteConsistencyLevel(cLevel)

		c, err := client.NewClient(opts)
		require.NoError(t, err)

		s, err := c.NewSession()
		require.NoError(t, err)

		return s.Write(nspaces[0].ID().String(), "quorumTest", now, 42, xtime.Second, nil)
	}

	assert.NoError(t, testWrite(topology.ConsistencyLevelOne))
	assert.Error(t, testWrite(topology.ConsistencyLevelMajority))
	assert.Error(t, testWrite(topology.ConsistencyLevelAll))

	require.NoError(t, nodes[1].startServer())
	assert.NoError(t, testWrite(topology.ConsistencyLevelOne))
	assert.NoError(t, testWrite(topology.ConsistencyLevelMajority))
	assert.Error(t, testWrite(topology.ConsistencyLevelAll))

	require.NoError(t, nodes[2].startServer())
	assert.NoError(t, testWrite(topology.ConsistencyLevelOne))
	assert.NoError(t, testWrite(topology.ConsistencyLevelMajority))
	assert.NoError(t, testWrite(topology.ConsistencyLevelAll))

	// Stop the servers at test completion
	log.Debug("servers closing")
	nodes.parallel(func(s *testSetup) {
		require.NoError(t, s.stopServer())
	})
	log.Debug("servers are now down")

}

func TestAddNodeQuorum(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Test setups
	//	log := xlog.SimpleLogger

	nspaces := []namespace.Metadata{
		namespace.NewMetadata(testNamespaces[0], namespace.NewOptions()),
	}
	opts := newTestOptions().SetNamespaces(nspaces)

	instances := []services.ServiceInstance{
		node(t, 0, newClusterShardsRange(0, 1023, shard.Leaving)),
		node(t, 1, newClusterShardsRange(0, 1023, shard.Available)),
		node(t, 2, newClusterShardsRange(0, 1023, shard.Available)),
		node(t, 3, newClusterShardsRange(0, 1023, shard.Initializing)),
	}

	svc := NewFakeM3ClusterService().
		SetInstances(instances).
		SetReplication(services.NewServiceReplication().SetReplicas(3)).
		SetSharding(services.NewServiceSharding().SetNumShards(1024))

	svcs := NewFakeM3ClusterServices()
	svcs.RegisterService("m3db", svc)

	topoOpts := topology.NewDynamicOptions().
		SetConfigServiceClient(NewM3FakeClusterClient(svcs, nil))
	topoInit := topology.NewDynamicInitializer(topoOpts)
	retentionOpts := retention.NewOptions().SetRetentionPeriod(6 * time.Hour)

	nodeOpt := bootstrappableTestSetupOptions{
		disablePeersBootstrapper: true,
		topologyInitializer:      topoInit,
	}
	nodeOpts := []bootstrappableTestSetupOptions{nodeOpt, nodeOpt, nodeOpt, nodeOpt}

	// nodes = m3db nodes
	nodes, closeFn := newDefaultBootstrappableTestSetups(t, opts, retentionOpts, nodeOpts)
	defer closeFn()

	require.NoError(t, nodes[0].startServer())
	require.NoError(t, nodes[3].startServer())
	now := nodes[0].getNowFn()

	testWrite := func(cLevel topology.ConsistencyLevel) error {
		opts := client.NewOptions().
			SetClusterConnectConsistencyLevel(client.ConnectConsistencyLevelAny).
			SetClusterConnectTimeout(10 * time.Millisecond).
			SetWriteRequestTimeout(100 * time.Millisecond).
			SetTopologyInitializer(topoInit).
			SetWriteConsistencyLevel(cLevel)

		c, err := client.NewClient(opts)
		require.NoError(t, err)

		s, err := c.NewSession()
		require.NoError(t, err)

		return s.Write(nspaces[0].ID().String(), "quorumTest", now, 42, xtime.Second, nil)
	}

	assert.Error(t, testWrite(topology.ConsistencyLevelOne))
	assert.Error(t, testWrite(topology.ConsistencyLevelMajority))
	assert.Error(t, testWrite(topology.ConsistencyLevelAll))

	require.NoError(t, nodes[1].startServer())
	assert.NoError(t, testWrite(topology.ConsistencyLevelOne))
	assert.Error(t, testWrite(topology.ConsistencyLevelMajority))
	assert.Error(t, testWrite(topology.ConsistencyLevelAll))

	require.NoError(t, nodes[2].startServer())
	assert.NoError(t, testWrite(topology.ConsistencyLevelOne))
	assert.NoError(t, testWrite(topology.ConsistencyLevelMajority))
	assert.Error(t, testWrite(topology.ConsistencyLevelAll))

	// // Stop the servers at test completion
	// log.Debug("servers closing")
	// nodes.parallel(func(s *testSetup) {
	// 	require.NoError(t, s.stopServer())
	// })
	// log.Debug("servers are now down")
}
