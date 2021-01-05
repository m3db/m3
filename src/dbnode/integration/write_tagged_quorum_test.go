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
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/test"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/x/xio"
	m3ninxidx "github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteTaggedNormalQuorumOnlyOneUp(t *testing.T) {
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
	require.NoError(t, nodes[0].StartServer())
	defer func() { require.NoError(t, nodes[0].StopServer()) }()

	assert.NoError(t, testWrite(topology.ConsistencyLevelOne))
	assert.Equal(t, 1, numNodesWithTaggedWrite(t, nodes))
	assert.Error(t, testWrite(topology.ConsistencyLevelMajority))
	assert.Equal(t, 1, numNodesWithTaggedWrite(t, nodes))
	assert.Error(t, testWrite(topology.ConsistencyLevelAll))
	assert.Equal(t, 1, numNodesWithTaggedWrite(t, nodes))
}

func TestWriteTaggedNormalQuorumOnlyTwoUp(t *testing.T) {
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

	require.NoError(t, nodes[0].StartServer())
	defer func() { require.NoError(t, nodes[0].StopServer()) }()
	require.NoError(t, nodes[1].StartServer())
	defer func() { require.NoError(t, nodes[1].StopServer()) }()

	// Writes succeed to two nodes
	assert.NoError(t, testWrite(topology.ConsistencyLevelOne))
	assert.True(t, numNodesWithTaggedWrite(t, nodes) >= 1)
	assert.NoError(t, testWrite(topology.ConsistencyLevelMajority))
	assert.True(t, numNodesWithTaggedWrite(t, nodes) == 2)
	assert.Error(t, testWrite(topology.ConsistencyLevelAll))
}

func TestWriteTaggedNormalQuorumAllUp(t *testing.T) {
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

	require.NoError(t, nodes[0].StartServer())
	defer func() { require.NoError(t, nodes[0].StopServer()) }()
	require.NoError(t, nodes[1].StartServer())
	defer func() { require.NoError(t, nodes[1].StopServer()) }()
	require.NoError(t, nodes[2].StartServer())
	defer func() { require.NoError(t, nodes[2].StopServer()) }()

	// Writes succeed to all nodes
	assert.NoError(t, testWrite(topology.ConsistencyLevelOne))
	assert.True(t, numNodesWithTaggedWrite(t, nodes) >= 1)
	assert.NoError(t, testWrite(topology.ConsistencyLevelMajority))
	assert.True(t, numNodesWithTaggedWrite(t, nodes) >= 2)
	assert.NoError(t, testWrite(topology.ConsistencyLevelAll))
	assert.True(t, numNodesWithTaggedWrite(t, nodes) >= 3)
}

func TestWriteTaggedAddNodeQuorumOnlyLeavingInitializingUp(t *testing.T) {
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

	require.NoError(t, nodes[0].StartServer())
	defer func() { require.NoError(t, nodes[0].StopServer()) }()
	require.NoError(t, nodes[3].StartServerDontWaitBootstrap())
	defer func() { require.NoError(t, nodes[3].StopServer()) }()

	// No writes succeed to available nodes
	assert.Error(t, testWrite(topology.ConsistencyLevelOne))
	numWrites := numNodesWithTaggedWrite(t, []TestSetup{nodes[1], nodes[2]})
	assert.True(t, numWrites == 0)

	assert.Error(t, testWrite(topology.ConsistencyLevelMajority))
	numWrites = numNodesWithTaggedWrite(t, []TestSetup{nodes[1], nodes[2]})
	assert.True(t, numWrites == 0)

	assert.Error(t, testWrite(topology.ConsistencyLevelAll))
	numWrites = numNodesWithTaggedWrite(t, []TestSetup{nodes[1], nodes[2]})
	assert.True(t, numWrites == 0)
}

func TestWriteTaggedAddNodeQuorumOnlyOneNormalAndLeavingInitializingUp(t *testing.T) {
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

	require.NoError(t, nodes[0].StartServer())
	defer func() { require.NoError(t, nodes[0].StopServer()) }()
	require.NoError(t, nodes[1].StartServer())
	defer func() { require.NoError(t, nodes[1].StopServer()) }()
	require.NoError(t, nodes[3].StartServerDontWaitBootstrap())
	defer func() { require.NoError(t, nodes[3].StopServer()) }()

	// Writes succeed to one available node
	assert.NoError(t, testWrite(topology.ConsistencyLevelOne))
	numWrites := numNodesWithTaggedWrite(t, []TestSetup{nodes[1], nodes[2]})
	assert.True(t, numWrites == 1)

	assert.Error(t, testWrite(topology.ConsistencyLevelMajority))
	numWrites = numNodesWithTaggedWrite(t, []TestSetup{nodes[1], nodes[2]})
	assert.True(t, numWrites == 1)

	assert.Error(t, testWrite(topology.ConsistencyLevelAll))
	numWrites = numNodesWithTaggedWrite(t, []TestSetup{nodes[1], nodes[2]})
	assert.True(t, numWrites == 1)
}

func TestWriteTaggedAddNodeQuorumAllUp(t *testing.T) {
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
	numWrites := numNodesWithTaggedWrite(t, []TestSetup{nodes[1], nodes[2]})
	assert.True(t, numWrites >= 1, numWrites)

	assert.NoError(t, testWrite(topology.ConsistencyLevelMajority))
	numWrites = numNodesWithTaggedWrite(t, []TestSetup{nodes[1], nodes[2]})
	assert.Equal(t, 2, numWrites)

	assert.Error(t, testWrite(topology.ConsistencyLevelAll))
}

func makeTestWriteTagged(
	t *testing.T,
	numShards int,
	instances []services.ServiceInstance,
) (testSetups, closeFn, testWriteFn) {
	nodes, closeFn, clientopts := makeMultiNodeSetup(t, numShards, true, false, instances)

	testWrite := func(cLevel topology.ConsistencyLevel) error {
		clientopts = clientopts.SetWriteConsistencyLevel(cLevel)
		c, err := client.NewClient(clientopts)
		require.NoError(t, err)

		s, err := c.NewSession()
		require.NoError(t, err)

		now := nodes[0].NowFn()().Add(time.Minute)
		return s.WriteTagged(testNamespaces[0], ident.StringID("quorumTest"),
			ident.NewTagsIterator(ident.NewTags(ident.StringTag("foo", "bar"), ident.StringTag("boo", "baz"))),
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

func nodeHasTaggedWrite(t *testing.T, s TestSetup) bool {
	if s.DB() == nil {
		return false
	}

	ctx := context.NewContext()
	defer ctx.BlockingClose()
	nsCtx := namespace.NewContextFor(testNamespaces[0], s.SchemaRegistry())

	reQuery, err := m3ninxidx.NewRegexpQuery([]byte("foo"), []byte("b.*"))
	assert.NoError(t, err)

	now := s.NowFn()()
	res, err := s.DB().QueryIDs(ctx, nsCtx.ID, index.Query{Query: reQuery}, index.QueryOptions{
		StartInclusive: now.Add(-2 * time.Minute),
		EndExclusive:   now.Add(2 * time.Minute),
	})
	require.NoError(t, err)
	results := res.Results
	require.Equal(t, nsCtx.ID.String(), results.Namespace().String())
	doc, ok := results.Map().Get(ident.BytesID("quorumTest"))
	idxFound := false
	if ok {
		tags := test.DocumentToTagIter(t, doc)
		idxFound = ident.NewTagIterMatcher(ident.MustNewTagStringsIterator(
			"foo", "bar", "boo", "baz")).Matches(tags)
	}

	if !idxFound {
		return false
	}

	// NB(prateek): if index has id, verify data point too
	dpFound := false

	id := ident.StringID("quorumTest")
	start := s.NowFn()()
	end := s.NowFn()().Add(5 * time.Minute)
	readers, err := s.DB().ReadEncoded(ctx, nsCtx.ID, id, start, end)
	require.NoError(t, err)

	mIter := s.DB().Options().MultiReaderIteratorPool().Get()
	mIter.ResetSliceOfSlices(xio.NewReaderSliceOfSlicesFromBlockReadersIterator(readers), nsCtx.Schema)
	defer mIter.Close()
	for mIter.Next() {
		dp, _, _ := mIter.Current()
		dpFound = dpFound || 42. == dp.Value
	}
	require.NoError(t, mIter.Err())

	return dpFound
}
