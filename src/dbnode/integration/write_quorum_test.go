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

	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalQuorumOnlyOneUp(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	numShards := defaultNumShards
	minShard := uint32(0)
	maxShard := uint32(numShards - 1)

	// nodes = m3db nodes
	nodes, closeFn, testWrite := makeTestWrite(t, numShards, []services.ServiceInstance{
		node(t, 0, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 1, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 2, newClusterShardsRange(minShard, maxShard, shard.Available)),
	})
	defer closeFn()

	require.NoError(t, nodes[0].StartServer())
	defer func() { require.NoError(t, nodes[0].StopServer()) }()

	// Writes succeed to one node
	assert.NoError(t, testWrite(topology.ConsistencyLevelOne))
	assert.Error(t, testWrite(topology.ConsistencyLevelMajority))
	assert.Error(t, testWrite(topology.ConsistencyLevelAll))
}

func TestNormalQuorumOnlyTwoUp(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	numShards := defaultNumShards
	minShard := uint32(0)
	maxShard := uint32(numShards - 1)

	// nodes = m3db nodes
	nodes, closeFn, testWrite := makeTestWrite(t, numShards, []services.ServiceInstance{
		node(t, 0, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 1, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 2, newClusterShardsRange(minShard, maxShard, shard.Available)),
	})
	defer closeFn()

	require.NoError(t, nodes[0].StartServer())
	defer func() { require.NoError(t, nodes[0].StopServer()) }()
	require.NoError(t, nodes[1].StartServer())
	defer func() { require.NoError(t, nodes[1].StopServer()) }()

	// Writes succeed to two nodes
	assert.NoError(t, testWrite(topology.ConsistencyLevelOne))
	assert.NoError(t, testWrite(topology.ConsistencyLevelMajority))
	assert.Error(t, testWrite(topology.ConsistencyLevelAll))
}

func TestNormalQuorumAllUp(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	numShards := defaultNumShards
	minShard := uint32(0)
	maxShard := uint32(numShards - 1)

	// nodes = m3db nodes
	nodes, closeFn, testWrite := makeTestWrite(t, numShards, []services.ServiceInstance{
		node(t, 0, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 1, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 2, newClusterShardsRange(minShard, maxShard, shard.Available)),
	})
	defer closeFn()

	require.NoError(t, nodes[0].StartServer())
	defer func() { require.NoError(t, nodes[0].StopServer()) }()
	require.NoError(t, nodes[1].StartServer())
	defer func() { require.NoError(t, nodes[1].StopServer()) }()
	require.NoError(t, nodes[2].StartServer())
	defer func() { require.NoError(t, nodes[2].StopServer()) }()

	// Writes succeed to all nodes
	assert.NoError(t, testWrite(topology.ConsistencyLevelOne))
	assert.NoError(t, testWrite(topology.ConsistencyLevelMajority))
	assert.NoError(t, testWrite(topology.ConsistencyLevelAll))
}

func TestAddNodeQuorumOnlyLeavingInitializingUp(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	numShards := defaultNumShards
	minShard := uint32(0)
	maxShard := uint32(numShards - 1)

	// nodes = m3db nodes
	nodes, closeFn, testWrite := makeTestWrite(t, numShards, []services.ServiceInstance{
		node(t, 0, newClusterShardsRange(minShard, maxShard, shard.Leaving)),
		node(t, 1, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 2, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 3, newClusterShardsRange(minShard, maxShard, shard.Initializing)),
	})
	defer closeFn()

	require.NoError(t, nodes[0].StartServer())
	defer func() { require.NoError(t, nodes[0].StopServer()) }()

	require.NoError(t, nodes[3].StartServerDontWaitBootstrap())
	defer func() { require.NoError(t, nodes[3].StopServer()) }()

	// No writes succeed to available nodes
	assert.Error(t, testWrite(topology.ConsistencyLevelOne))
	assert.Error(t, testWrite(topology.ConsistencyLevelMajority))
	assert.Error(t, testWrite(topology.ConsistencyLevelAll))
}

func TestAddNodeQuorumOnlyOneNormalAndLeavingInitializingUp(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	numShards := defaultNumShards
	minShard := uint32(0)
	maxShard := uint32(numShards - 1)

	// nodes = m3db nodes
	nodes, closeFn, testWrite := makeTestWrite(t, numShards, []services.ServiceInstance{
		node(t, 0, newClusterShardsRange(minShard, maxShard, shard.Leaving)),
		node(t, 1, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 2, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 3, newClusterShardsRange(minShard, maxShard, shard.Initializing)),
	})
	defer closeFn()

	require.NoError(t, nodes[0].StartServer())
	defer func() { require.NoError(t, nodes[0].StopServer()) }()
	require.NoError(t, nodes[1].StartServer())
	defer func() { require.NoError(t, nodes[1].StopServer()) }()
	require.NoError(t, nodes[3].StartServerDontWaitBootstrap())
	defer func() { require.NoError(t, nodes[3].StopServer()) }()

	// Writes succeed to one available node
	assert.NoError(t, testWrite(topology.ConsistencyLevelOne))
	assert.Error(t, testWrite(topology.ConsistencyLevelMajority))
	assert.Error(t, testWrite(topology.ConsistencyLevelAll))
}

func TestAddNodeQuorumAllUp(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	numShards := defaultNumShards
	minShard := uint32(0)
	maxShard := uint32(numShards - 1)

	// nodes = m3db nodes
	nodes, closeFn, testWrite := makeTestWrite(t, numShards, []services.ServiceInstance{
		node(t, 0, newClusterShardsRange(minShard, maxShard, shard.Leaving)),
		node(t, 1, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 2, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 3, newClusterShardsRange(minShard, maxShard, shard.Initializing)),
	})
	defer closeFn()

	require.NoError(t, nodes[0].StartServer())
	defer func() { require.NoError(t, nodes[0].StopServer()) }()
	require.NoError(t, nodes[1].StartServer())
	defer func() { require.NoError(t, nodes[1].StopServer()) }()
	require.NoError(t, nodes[2].StartServer())
	defer func() { require.NoError(t, nodes[2].StopServer()) }()
	require.NoError(t, nodes[3].StartServerDontWaitBootstrap())
	defer func() { require.NoError(t, nodes[3].StopServer()) }()

	// Writes succeed to two available nodes
	assert.NoError(t, testWrite(topology.ConsistencyLevelOne))
	assert.NoError(t, testWrite(topology.ConsistencyLevelMajority))
	assert.Error(t, testWrite(topology.ConsistencyLevelAll))
}

type testWriteFn func(topology.ConsistencyLevel) error

func makeTestWrite(
	t *testing.T,
	numShards int,
	instances []services.ServiceInstance,
) (testSetups, closeFn, testWriteFn) {

	nsOpts := namespace.NewOptions()
	md, err := namespace.NewMetadata(testNamespaces[0],
		nsOpts.SetRetentionOptions(nsOpts.RetentionOptions().SetRetentionPeriod(6*time.Hour)))
	require.NoError(t, err)

	nspaces := []namespace.Metadata{md}
	nodes, topoInit, closeFn := newNodes(t, numShards, instances, nspaces, false)
	now := nodes[0].NowFn()()

	for _, node := range nodes {
		node.SetOpts(node.Opts().SetNumShards(numShards))
	}

	clientopts := client.NewOptions().
		SetClusterConnectConsistencyLevel(topology.ConnectConsistencyLevelNone).
		SetClusterConnectTimeout(2 * time.Second).
		SetWriteRequestTimeout(2 * time.Second).
		SetTopologyInitializer(topoInit)

	testWrite := func(cLevel topology.ConsistencyLevel) error {
		clientopts = clientopts.SetWriteConsistencyLevel(cLevel)
		c, err := client.NewClient(clientopts)
		require.NoError(t, err)

		s, err := c.NewSession()
		require.NoError(t, err)

		return s.Write(nspaces[0].ID(), ident.StringID("quorumTest"), now, 42, xtime.Second, nil)
	}

	return nodes, closeFn, testWrite
}
