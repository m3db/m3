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

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/storage/index"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/topology"
	"github.com/m3db/m3ninx/index/segment"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTaggedNormalQuorumOnlyOneUp(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	numShards := defaultNumShards
	minShard := uint32(0)
	maxShard := uint32(numShards - 1)

	// nodes = m3db nodes
	nodes, closeFn, testWrite := makeTestWriteTagged(t, numShards, []services.ServiceInstance{
		node(t, 0, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 1, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 2, newClusterShardsRange(minShard, maxShard, shard.Available)),
	})
	defer closeFn()

	// Writes succeed to one node
	require.NoError(t, nodes[0].startServer())
	assert.NoError(t, testWrite(topology.ConsistencyLevelOne))
	assert.Equal(t, 1, numNodesWithTaggedWrite(t, nodes))
	assert.Error(t, testWrite(topology.ConsistencyLevelMajority))
	assert.Equal(t, 1, numNodesWithTaggedWrite(t, nodes))
	assert.Error(t, testWrite(topology.ConsistencyLevelAll))
	assert.Equal(t, 1, numNodesWithTaggedWrite(t, nodes))
}

func TestTaggedNormalQuorumOnlyTwoUp(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	numShards := defaultNumShards
	minShard := uint32(0)
	maxShard := uint32(numShards - 1)

	// nodes = m3db nodes
	nodes, closeFn, testWrite := makeTestWriteTagged(t, numShards, []services.ServiceInstance{
		node(t, 0, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 1, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 2, newClusterShardsRange(minShard, maxShard, shard.Available)),
	})
	defer closeFn()

	// Writes succeed to two nodes
	require.NoError(t, nodes[0].startServer())
	require.NoError(t, nodes[1].startServer())
	assert.NoError(t, testWrite(topology.ConsistencyLevelOne))
	assert.True(t, numNodesWithTaggedWrite(t, nodes) >= 1)
	assert.NoError(t, testWrite(topology.ConsistencyLevelMajority))
	assert.True(t, numNodesWithTaggedWrite(t, nodes) == 2)
	assert.Error(t, testWrite(topology.ConsistencyLevelAll))
}

func TestTaggedNormalQuorumAllUp(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	numShards := defaultNumShards
	minShard := uint32(0)
	maxShard := uint32(numShards - 1)

	// nodes = m3db nodes
	nodes, closeFn, testWrite := makeTestWriteTagged(t, numShards, []services.ServiceInstance{
		node(t, 0, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 1, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 2, newClusterShardsRange(minShard, maxShard, shard.Available)),
	})
	defer closeFn()

	// Writes succeed to all nodes
	require.NoError(t, nodes[0].startServer())
	require.NoError(t, nodes[1].startServer())
	require.NoError(t, nodes[2].startServer())
	assert.NoError(t, testWrite(topology.ConsistencyLevelOne))
	assert.True(t, numNodesWithTaggedWrite(t, nodes) >= 1)
	assert.NoError(t, testWrite(topology.ConsistencyLevelMajority))
	assert.True(t, numNodesWithTaggedWrite(t, nodes) >= 2)
	assert.NoError(t, testWrite(topology.ConsistencyLevelAll))
	assert.True(t, numNodesWithTaggedWrite(t, nodes) >= 3)
}

func TestTaggedAddNodeQuorumOnlyLeavingInitializingUp(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	numShards := defaultNumShards
	minShard := uint32(0)
	maxShard := uint32(numShards - 1)

	// nodes = m3db nodes
	nodes, closeFn, testWrite := makeTestWriteTagged(t, numShards, []services.ServiceInstance{
		node(t, 0, newClusterShardsRange(minShard, maxShard, shard.Leaving)),
		node(t, 1, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 2, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 3, newClusterShardsRange(minShard, maxShard, shard.Initializing)),
	})
	defer closeFn()

	// No writes succeed to available nodes
	require.NoError(t, nodes[0].startServer())
	require.NoError(t, nodes[3].startServer())

	assert.Error(t, testWrite(topology.ConsistencyLevelOne))
	numWrites := numNodesWithTaggedWrite(t, []*testSetup{nodes[1], nodes[2]})
	assert.True(t, numWrites == 0)

	assert.Error(t, testWrite(topology.ConsistencyLevelMajority))
	numWrites = numNodesWithTaggedWrite(t, []*testSetup{nodes[1], nodes[2]})
	assert.True(t, numWrites == 0)

	assert.Error(t, testWrite(topology.ConsistencyLevelAll))
	numWrites = numNodesWithTaggedWrite(t, []*testSetup{nodes[1], nodes[2]})
	assert.True(t, numWrites == 0)
}

func TestTaggedAddNodeQuorumOnlyOneNormalAndLeavingInitializingUp(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	numShards := defaultNumShards
	minShard := uint32(0)
	maxShard := uint32(numShards - 1)

	// nodes = m3db nodes
	nodes, closeFn, testWrite := makeTestWriteTagged(t, numShards, []services.ServiceInstance{
		node(t, 0, newClusterShardsRange(minShard, maxShard, shard.Leaving)),
		node(t, 1, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 2, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 3, newClusterShardsRange(minShard, maxShard, shard.Initializing)),
	})
	defer closeFn()

	// Writes succeed to one available node
	require.NoError(t, nodes[0].startServer())
	require.NoError(t, nodes[1].startServer())
	require.NoError(t, nodes[3].startServer())

	assert.NoError(t, testWrite(topology.ConsistencyLevelOne))
	numWrites := numNodesWithTaggedWrite(t, []*testSetup{nodes[1], nodes[2]})
	assert.True(t, numWrites == 1)

	assert.Error(t, testWrite(topology.ConsistencyLevelMajority))
	numWrites = numNodesWithTaggedWrite(t, []*testSetup{nodes[1], nodes[2]})
	assert.True(t, numWrites == 1)

	assert.Error(t, testWrite(topology.ConsistencyLevelAll))
	numWrites = numNodesWithTaggedWrite(t, []*testSetup{nodes[1], nodes[2]})
	assert.True(t, numWrites == 1)
}

func TestTaggedAddNodeQuorumAllUp(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	numShards := defaultNumShards
	minShard := uint32(0)
	maxShard := uint32(numShards - 1)

	// nodes = m3db nodes
	nodes, closeFn, testWrite := makeTestWriteTagged(t, numShards, []services.ServiceInstance{
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

	assert.NoError(t, testWrite(topology.ConsistencyLevelOne))
	numWrites := numNodesWithTaggedWrite(t, []*testSetup{nodes[1], nodes[2]})
	assert.True(t, numWrites == 2)

	assert.NoError(t, testWrite(topology.ConsistencyLevelMajority))
	numWrites = numNodesWithTaggedWrite(t, []*testSetup{nodes[1], nodes[2]})
	assert.True(t, numWrites == 2)

	assert.Error(t, testWrite(topology.ConsistencyLevelAll))
	numWrites = numNodesWithTaggedWrite(t, []*testSetup{nodes[1], nodes[2]})
	assert.True(t, numWrites == 2)
}

func makeTestWriteTagged(
	t *testing.T,
	numShards int,
	instances []services.ServiceInstance,
) (testSetups, closeFn, testWriteFn) {

	nsOpts := namespace.NewOptions()
	md, err := namespace.NewMetadata(testNamespaces[0],
		nsOpts.SetRetentionOptions(nsOpts.RetentionOptions().SetRetentionPeriod(6*time.Hour)))
	require.NoError(t, err)

	nspaces := []namespace.Metadata{md}
	nodes, topoInit, closeFn := newNodes(t, instances, nspaces, true)
	now := nodes[0].getNowFn()

	for _, node := range nodes {
		node.opts = node.opts.SetNumShards(numShards)
	}

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

		return s.WriteTagged(nspaces[0].ID(), ident.StringID("quorumTest"),
			ident.NewTagIterator(ident.StringTag("foo", "bar"), ident.StringTag("boo", "baz")),
			now, 42, xtime.Second, nil)
	}

	return nodes, closeFn, testWrite
}

func numNodesWithTaggedWrite(t *testing.T, setups testSetups) int {
	n := 0
	for _, s := range setups {
		if nodeHasTaggedWrite(t, s) {
			n++
		}
	}
	return n
}

func nodeHasTaggedWrite(t *testing.T, s *testSetup) bool {
	if s.db == nil {
		return false
	}

	ctx := context.NewContext()
	defer ctx.BlockingClose()

	results, err := s.db.QueryIDs(ctx, index.Query{
		segment.Query{
			Conjunction: segment.AndConjunction,
			Filters: []segment.Filter{
				segment.Filter{
					FieldName:        []byte("foo"),
					FieldValueFilter: []byte("b.*"),
					Regexp:           true,
				},
			},
		},
	}, index.QueryOptions{})

	require.NoError(t, err)
	iter := results.Iterator
	found := false
	for iter.Next() {
		_, id, tags := iter.Current()
		if id.String() == "quorumTest" && tags.Equal(
			ident.Tags{ident.StringTag("foo", "bar"), ident.StringTag("boo", "baz")}) {
			found = true
		}
	}
	require.NoError(t, iter.Err())

	// TODO(prateek): verify data point too
	return found
}
