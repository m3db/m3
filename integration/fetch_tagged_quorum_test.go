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

	"github.com/m3db/m3ninx/idx"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/storage/index"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/topology"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFetchTaggedNormalQuorumOnlyOneUp(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	numShards := defaultNumShards
	minShard := uint32(0)
	maxShard := uint32(numShards - 1)

	// nodes = m3db nodes
	nodes, closeFn, testFetch := makeTestFetchTagged(t, numShards, []services.ServiceInstance{
		node(t, 0, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 1, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 2, newClusterShardsRange(minShard, maxShard, shard.Available)),
	})
	defer closeFn()

	// fetch succeeds from one node
	require.NoError(t, nodes[0].startServer())
	defer func() { require.NoError(t, nodes[0].stopServer()) }()

	require.NoError(t, testFetch(topology.ReadConsistencyLevelOne))
	require.Error(t, testFetch(topology.ReadConsistencyLevelMajority))
	require.Error(t, testFetch(topology.ReadConsistencyLevelAll))
}

func TestFetchTaggedNormalQuorumOnlyTwoUp(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	numShards := defaultNumShards
	minShard := uint32(0)
	maxShard := uint32(numShards - 1)

	// nodes = m3db nodes
	nodes, closeFn, testFetch := makeTestFetchTagged(t, numShards, []services.ServiceInstance{
		node(t, 0, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 1, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 2, newClusterShardsRange(minShard, maxShard, shard.Available)),
	})
	defer closeFn()

	require.NoError(t, nodes[0].startServer())
	defer func() { require.NoError(t, nodes[0].stopServer()) }()
	require.NoError(t, nodes[1].startServer())
	defer func() { require.NoError(t, nodes[1].stopServer()) }()

	// Writes succeed to two nodes
	assert.NoError(t, testFetch(topology.ReadConsistencyLevelOne))
	assert.NoError(t, testFetch(topology.ReadConsistencyLevelMajority))
	assert.Error(t, testFetch(topology.ReadConsistencyLevelAll))
}

func TestFetchTaggedNormalQuorumAllUp(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	numShards := defaultNumShards
	minShard := uint32(0)
	maxShard := uint32(numShards - 1)

	// nodes = m3db nodes
	nodes, closeFn, testFetch := makeTestFetchTagged(t, numShards, []services.ServiceInstance{
		node(t, 0, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 1, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 2, newClusterShardsRange(minShard, maxShard, shard.Available)),
	})
	defer closeFn()

	require.NoError(t, nodes[0].startServer())
	defer func() { require.NoError(t, nodes[0].stopServer()) }()
	require.NoError(t, nodes[1].startServer())
	defer func() { require.NoError(t, nodes[1].stopServer()) }()
	require.NoError(t, nodes[2].startServer())
	defer func() { require.NoError(t, nodes[2].stopServer()) }()

	// Writes succeed to all nodes
	assert.NoError(t, testFetch(topology.ReadConsistencyLevelOne))
	assert.NoError(t, testFetch(topology.ReadConsistencyLevelMajority))
	assert.NoError(t, testFetch(topology.ReadConsistencyLevelAll))
}

func TestFetchTaggedAddNodeQuorumOnlyLeavingInitializingUp(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	numShards := defaultNumShards
	minShard := uint32(0)
	maxShard := uint32(numShards - 1)

	// nodes = m3db nodes
	nodes, closeFn, testFetch := makeTestFetchTagged(t, numShards, []services.ServiceInstance{
		node(t, 0, newClusterShardsRange(minShard, maxShard, shard.Leaving)),
		node(t, 1, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 2, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 3, newClusterShardsRange(minShard, maxShard, shard.Initializing)),
	})
	defer closeFn()

	require.NoError(t, nodes[0].startServer())
	defer func() { require.NoError(t, nodes[0].stopServer()) }()
	require.NoError(t, nodes[3].startServer())
	defer func() { require.NoError(t, nodes[3].stopServer()) }()

	// No writes succeed to available nodes
	assert.Error(t, testFetch(topology.ReadConsistencyLevelOne))
	assert.Error(t, testFetch(topology.ReadConsistencyLevelMajority))
	assert.Error(t, testFetch(topology.ReadConsistencyLevelAll))
}

func TestFetchTaggedAddNodeQuorumOnlyOneNormalAndLeavingInitializingUp(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	numShards := defaultNumShards
	minShard := uint32(0)
	maxShard := uint32(numShards - 1)

	// nodes = m3db nodes
	nodes, closeFn, testFetch := makeTestFetchTagged(t, numShards, []services.ServiceInstance{
		node(t, 0, newClusterShardsRange(minShard, maxShard, shard.Leaving)),
		node(t, 1, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 2, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 3, newClusterShardsRange(minShard, maxShard, shard.Initializing)),
	})
	defer closeFn()

	require.NoError(t, nodes[0].startServer())
	require.NoError(t, nodes[1].startServer())
	require.NoError(t, nodes[3].startServer())
	defer func() {
		require.NoError(t, nodes[0].stopServer())
		require.NoError(t, nodes[1].stopServer())
		require.NoError(t, nodes[3].stopServer())
	}()

	// Writes succeed to one available node
	assert.NoError(t, testFetch(topology.ReadConsistencyLevelOne))
	assert.Error(t, testFetch(topology.ReadConsistencyLevelMajority))
	assert.Error(t, testFetch(topology.ReadConsistencyLevelAll))
}

func TestFetchTaggedAddNodeQuorumAllUp(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	numShards := defaultNumShards
	minShard := uint32(0)
	maxShard := uint32(numShards - 1)

	// nodes = m3db nodes
	nodes, closeFn, testFetch := makeTestFetchTagged(t, numShards, []services.ServiceInstance{
		node(t, 0, newClusterShardsRange(minShard, maxShard, shard.Leaving)),
		node(t, 1, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 2, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 3, newClusterShardsRange(minShard, maxShard, shard.Initializing)),
	})
	defer closeFn()

	// Writes succeed to two available nodes
	require.NoError(t, nodes[0].startServer())
	require.NoError(t, nodes[1].startServer())
	require.NoError(t, nodes[2].startServer())
	require.NoError(t, nodes[3].startServer())
	defer func() {
		require.NoError(t, nodes[0].stopServer())
		require.NoError(t, nodes[1].stopServer())
		require.NoError(t, nodes[2].stopServer())
		require.NoError(t, nodes[3].stopServer())
	}()

	assert.NoError(t, testFetch(topology.ReadConsistencyLevelOne))
	assert.NoError(t, testFetch(topology.ReadConsistencyLevelMajority))
	assert.Error(t, testFetch(topology.ReadConsistencyLevelAll))
}

type testFetchFn func(topology.ReadConsistencyLevel) error

func makeTestFetchTagged(
	t *testing.T,
	numShards int,
	instances []services.ServiceInstance,
) (testSetups, closeFn, testFetchFn) {

	nsOpts := namespace.NewOptions()
	md, err := namespace.NewMetadata(testNamespaces[0],
		nsOpts.SetRetentionOptions(nsOpts.RetentionOptions().SetRetentionPeriod(6*time.Hour)))
	require.NoError(t, err)

	nspaces := []namespace.Metadata{md}
	nodes, topoInit, closeFn := newNodes(t, instances, nspaces, true)
	for _, node := range nodes {
		node.opts = node.opts.SetNumShards(numShards)
	}

	clientopts := client.NewOptions().
		SetClusterConnectConsistencyLevel(topology.ConnectConsistencyLevelNone).
		SetClusterConnectTimeout(2 * time.Second).
		SetWriteRequestTimeout(2 * time.Second).
		SetTopologyInitializer(topoInit)

	testFetch := func(cLevel topology.ReadConsistencyLevel) error {
		c, err := client.NewClient(clientopts.SetReadConsistencyLevel(cLevel))
		require.NoError(t, err)

		s, err := c.NewSession()
		require.NoError(t, err)

		q, err := idx.NewRegexpQuery([]byte("foo"), []byte("b.*"))
		require.NoError(t, err)

		_, _, err = s.FetchTagged(nspaces[0].ID(), index.Query{q}, index.QueryOptions{})
		return err
	}

	return nodes, closeFn, testFetch
}
