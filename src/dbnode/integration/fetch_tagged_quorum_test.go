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
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFetchTaggedQuorumNormalOnlyOneUp(t *testing.T) {
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
	writeTagged(t, nodes[0])

	testFetch.assertContainsTaggedResult(t,
		topology.ReadConsistencyLevelOne, topology.ReadConsistencyLevelUnstrictMajority)
	testFetch.assertFailsTaggedResult(t,
		topology.ReadConsistencyLevelAll, topology.ReadConsistencyLevelMajority)
}

func TestFetchTaggedQuorumNormalOnlyTwoUp(t *testing.T) {
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
	require.NoError(t, nodes[1].startServer())
	writeTagged(t, nodes[0], nodes[1])

	// succeed to two nodes
	testFetch.assertContainsTaggedResult(t, topology.ReadConsistencyLevelOne,
		topology.ReadConsistencyLevelUnstrictMajority, topology.ReadConsistencyLevelMajority)
	testFetch.assertFailsTaggedResult(t, topology.ReadConsistencyLevelAll)
}

func TestFetchTaggedQuorumNormalAllUp(t *testing.T) {
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
	require.NoError(t, nodes[1].startServer())
	require.NoError(t, nodes[2].startServer())
	writeTagged(t, nodes...)

	// succeed to all nodes
	testFetch.assertContainsTaggedResult(t,
		topology.ReadConsistencyLevelOne, topology.ReadConsistencyLevelUnstrictMajority,
		topology.ReadConsistencyLevelMajority, topology.ReadConsistencyLevelAll)
}

func TestFetchTaggedQuorumAddNodeOnlyLeavingInitializingUp(t *testing.T) {
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
	require.NoError(t, nodes[3].startServerDontWaitBootstrap())
	writeTagged(t, nodes[0], nodes[3])

	// No fetches succeed to available nodes
	testFetch.assertFailsTaggedResult(t,
		topology.ReadConsistencyLevelOne, topology.ReadConsistencyLevelUnstrictMajority,
		topology.ReadConsistencyLevelMajority, topology.ReadConsistencyLevelAll)
}

func TestFetchTaggedQuorumAddNodeOnlyOneNormalAndLeavingInitializingUp(t *testing.T) {
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
	require.NoError(t, nodes[3].startServerDontWaitBootstrap())
	writeTagged(t, nodes[0], nodes[1], nodes[3])

	// fetches succeed to one available node
	testFetch.assertContainsTaggedResult(t,
		topology.ReadConsistencyLevelUnstrictMajority, topology.ReadConsistencyLevelOne)

	testFetch.assertFailsTaggedResult(t,
		topology.ReadConsistencyLevelMajority, topology.ReadConsistencyLevelAll)
}

func TestFetchTaggedQuorumAddNodeAllUp(t *testing.T) {
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

	// fetches succeed to one available node
	require.NoError(t, nodes[0].startServer())
	require.NoError(t, nodes[1].startServer())
	require.NoError(t, nodes[2].startServer())
	require.NoError(t, nodes[3].startServerDontWaitBootstrap())
	writeTagged(t, nodes...)

	testFetch.assertContainsTaggedResult(t, topology.ReadConsistencyLevelOne,
		topology.ReadConsistencyLevelUnstrictMajority, topology.ReadConsistencyLevelMajority)

	testFetch.assertFailsTaggedResult(t, topology.ReadConsistencyLevelAll)
}

type testFetchFn func(topology.ReadConsistencyLevel) (encoding.SeriesIterators, bool, error)

func (fn testFetchFn) assertContainsTaggedResult(t *testing.T, lvls ...topology.ReadConsistencyLevel) {
	for _, lvl := range lvls {
		iters, exhaust, err := fn(lvl)
		require.NoError(t, err)
		require.True(t, exhaust)
		require.Equal(t, 1, iters.Len())
		iter := iters.Iters()[0]
		require.Equal(t, testNamespaces[0].String(), iter.Namespace().String())
		require.Equal(t, "quorumTest", iter.ID().String())
		require.True(t, ident.NewTagIterMatcher(
			ident.MustNewTagStringsIterator("foo", "bar", "boo", "baz")).Matches(iter.Tags()))
		require.True(t, iter.Next())
		dp, _, _ := iter.Current()
		require.Equal(t, 42., dp.Value)
		require.False(t, iter.Next())
		require.NoError(t, iter.Err())
	}
}

func (fn testFetchFn) assertFailsTaggedResult(t *testing.T, lvls ...topology.ReadConsistencyLevel) {
	for _, lvl := range lvls {
		_, _, err := fn(lvl)
		assert.Error(t, err)
	}
}

func makeMultiNodeSetup(
	t *testing.T,
	numShards int,
	indexingEnabled bool,
	asyncInserts bool,
	instances []services.ServiceInstance,
) (testSetups, closeFn, client.Options) {
	var (
		nsOpts  = namespace.NewOptions()
		md, err = namespace.NewMetadata(testNamespaces[0],
			nsOpts.SetRetentionOptions(nsOpts.RetentionOptions().SetRetentionPeriod(6*time.Hour)).
				SetIndexOptions(namespace.NewIndexOptions().SetEnabled(indexingEnabled)))
	)
	require.NoError(t, err)

	nspaces := []namespace.Metadata{md}
	nodes, topoInit, closeFn := newNodes(t, numShards, instances, nspaces, asyncInserts)
	for _, node := range nodes {
		node.opts = node.opts.SetNumShards(numShards)
	}

	clientopts := client.NewOptions().
		SetClusterConnectConsistencyLevel(topology.ConnectConsistencyLevelNone).
		SetClusterConnectTimeout(2 * time.Second).
		SetWriteRequestTimeout(2 * time.Second).
		SetFetchRequestTimeout(2 * time.Second).
		SetTopologyInitializer(topoInit)

	return nodes, closeFn, clientopts
}

func makeTestFetchTagged(
	t *testing.T,
	numShards int,
	instances []services.ServiceInstance,
) (testSetups, closeFn, testFetchFn) {
	nodes, closeFn, clientopts := makeMultiNodeSetup(t, numShards, true, false, instances)
	testFetch := func(cLevel topology.ReadConsistencyLevel) (encoding.SeriesIterators, bool, error) {
		c, err := client.NewClient(clientopts.SetReadConsistencyLevel(cLevel))
		require.NoError(t, err)

		s, err := c.NewSession()
		require.NoError(t, err)

		q, err := idx.NewRegexpQuery([]byte("foo"), []byte("b.*"))
		require.NoError(t, err)

		startTime := nodes[0].getNowFn()
		return s.FetchTagged(testNamespaces[0],
			index.Query{q},
			index.QueryOptions{
				StartInclusive: startTime.Add(-time.Minute),
				EndExclusive:   startTime.Add(time.Minute),
				Limit:          1,
			})
	}

	return nodes, closeFn, testFetch
}

func writeTagged(
	t *testing.T,
	nodes ...*testSetup,
) {
	ctx := context.NewContext()
	defer ctx.BlockingClose()
	for _, n := range nodes {
		require.NoError(t, n.db.WriteTagged(ctx, testNamespaces[0], ident.StringID("quorumTest"),
			ident.NewTagsIterator(ident.NewTags(ident.StringTag("foo", "bar"), ident.StringTag("boo", "baz"))),
			n.getNowFn(), 42, xtime.Second, nil, series.WriteOptions{WriteTime: n.getNowFn()}))
	}
}
