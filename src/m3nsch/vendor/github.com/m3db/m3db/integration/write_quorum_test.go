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
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/topology"
	xtime "github.com/m3db/m3x/time"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/shard"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalQuorumOnlyOneUp(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// nodes = m3db nodes
	nodes, closeFn, testWrite := makeTestWrite(t, []services.ServiceInstance{
		node(t, 0, newClusterShardsRange(0, 1023, shard.Available)),
		node(t, 1, newClusterShardsRange(0, 1023, shard.Available)),
		node(t, 2, newClusterShardsRange(0, 1023, shard.Available)),
	})
	defer closeFn()

	// Writes succeed to one node
	require.NoError(t, nodes[0].startServer())
	assert.NoError(t, testWrite(topology.ConsistencyLevelOne))
	assert.Error(t, testWrite(topology.ConsistencyLevelMajority))
	assert.Error(t, testWrite(topology.ConsistencyLevelAll))
}

func TestNormalQuorumOnlyTwoUp(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// nodes = m3db nodes
	nodes, closeFn, testWrite := makeTestWrite(t, []services.ServiceInstance{
		node(t, 0, newClusterShardsRange(0, 1023, shard.Available)),
		node(t, 1, newClusterShardsRange(0, 1023, shard.Available)),
		node(t, 2, newClusterShardsRange(0, 1023, shard.Available)),
	})
	defer closeFn()

	// Writes succeed to two nodes
	require.NoError(t, nodes[0].startServer())
	require.NoError(t, nodes[1].startServer())
	assert.NoError(t, testWrite(topology.ConsistencyLevelOne))
	assert.NoError(t, testWrite(topology.ConsistencyLevelMajority))
	assert.Error(t, testWrite(topology.ConsistencyLevelAll))
}

func TestNormalQuorumAllUp(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// nodes = m3db nodes
	nodes, closeFn, testWrite := makeTestWrite(t, []services.ServiceInstance{
		node(t, 0, newClusterShardsRange(0, 1023, shard.Available)),
		node(t, 1, newClusterShardsRange(0, 1023, shard.Available)),
		node(t, 2, newClusterShardsRange(0, 1023, shard.Available)),
	})
	defer closeFn()

	// Writes succeed to all nodes
	require.NoError(t, nodes[0].startServer())
	require.NoError(t, nodes[1].startServer())
	require.NoError(t, nodes[2].startServer())
	assert.NoError(t, testWrite(topology.ConsistencyLevelOne))
	assert.NoError(t, testWrite(topology.ConsistencyLevelMajority))
	assert.NoError(t, testWrite(topology.ConsistencyLevelAll))
}

func TestAddNodeQuorumOnlyLeavingInitializingUp(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// nodes = m3db nodes
	nodes, closeFn, testWrite := makeTestWrite(t, []services.ServiceInstance{
		node(t, 0, newClusterShardsRange(0, 1023, shard.Leaving)),
		node(t, 1, newClusterShardsRange(0, 1023, shard.Available)),
		node(t, 2, newClusterShardsRange(0, 1023, shard.Available)),
		node(t, 3, newClusterShardsRange(0, 1023, shard.Initializing)),
	})
	defer closeFn()

	// No writes succeed to available nodes
	require.NoError(t, nodes[0].startServer())
	require.NoError(t, nodes[3].startServer())
	assert.Error(t, testWrite(topology.ConsistencyLevelOne))
	assert.Error(t, testWrite(topology.ConsistencyLevelMajority))
	assert.Error(t, testWrite(topology.ConsistencyLevelAll))
}

func TestAddNodeQuorumOnlyOneNormalAndLeavingInitializingUp(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// nodes = m3db nodes
	nodes, closeFn, testWrite := makeTestWrite(t, []services.ServiceInstance{
		node(t, 0, newClusterShardsRange(0, 1023, shard.Leaving)),
		node(t, 1, newClusterShardsRange(0, 1023, shard.Available)),
		node(t, 2, newClusterShardsRange(0, 1023, shard.Available)),
		node(t, 3, newClusterShardsRange(0, 1023, shard.Initializing)),
	})
	defer closeFn()

	// Writes succeed to one available node
	require.NoError(t, nodes[0].startServer())
	require.NoError(t, nodes[1].startServer())
	require.NoError(t, nodes[3].startServer())
	assert.NoError(t, testWrite(topology.ConsistencyLevelOne))
	assert.Error(t, testWrite(topology.ConsistencyLevelMajority))
	assert.Error(t, testWrite(topology.ConsistencyLevelAll))
}

func TestAddNodeQuorumAllUp(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// nodes = m3db nodes
	nodes, closeFn, testWrite := makeTestWrite(t, []services.ServiceInstance{
		node(t, 0, newClusterShardsRange(0, 1023, shard.Leaving)),
		node(t, 1, newClusterShardsRange(0, 1023, shard.Available)),
		node(t, 2, newClusterShardsRange(0, 1023, shard.Available)),
		node(t, 3, newClusterShardsRange(0, 1023, shard.Initializing)),
	})
	defer closeFn()

	// Writes succeed to two available nodes
	require.NoError(t, nodes[0].startServer())
	require.NoError(t, nodes[1].startServer())
	require.NoError(t, nodes[2].startServer())
	require.NoError(t, nodes[3].startServer())
	assert.NoError(t, testWrite(topology.ConsistencyLevelOne))
	assert.NoError(t, testWrite(topology.ConsistencyLevelMajority))
	assert.Error(t, testWrite(topology.ConsistencyLevelAll))
}

type testWriteFn func(topology.ConsistencyLevel) error

func makeTestWrite(
	t *testing.T,
	instances []services.ServiceInstance,
) (testSetups, closeFn, testWriteFn) {

	nspaces := []namespace.Metadata{
		namespace.NewMetadata(testNamespaces[0], namespace.NewOptions()),
	}

	nodes, topoInit, closeFn := newNodes(t, instances, nspaces)

	now := nodes[0].getNowFn()

	clientopts := client.NewOptions().
		SetClusterConnectConsistencyLevel(client.ConnectConsistencyLevelNone).
		SetClusterConnectTimeout(2 * time.Second).
		SetWriteRequestTimeout(2 * time.Second).
		SetTopologyInitializer(topoInit)

	testWrite := func(cLevel topology.ConsistencyLevel) error {
		c, err := client.NewClient(clientopts.SetWriteConsistencyLevel(cLevel))
		require.NoError(t, err)

		s, err := c.NewSession()
		require.NoError(t, err)

		return s.Write(nspaces[0].ID().String(), "quorumTest", now, 42, xtime.Second, nil)
	}

	return nodes, closeFn, testWrite
}
