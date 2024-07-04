//go:build integration
// +build integration

//
// Copyright (c) 2019 Uber Technologies, Inc.
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
	"github.com/m3db/m3/src/dbnode/topology"
	xenc "github.com/m3db/m3/src/dbnode/x/encoding"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestShallowCopySingleSeries(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	var (
		numShards = defaultNumShards
		minShard  = uint32(0)
		maxShard  = uint32(numShards - 1)
		instances = []services.ServiceInstance{
			node(t, 0, newClusterShardsRange(minShard, maxShard, shard.Available)),
			node(t, 1, newClusterShardsRange(minShard, maxShard, shard.Available)),
			node(t, 2, newClusterShardsRange(minShard, maxShard, shard.Available)),
		}
	)
	nodes, closeFn, clientopts := makeMultiNodeSetup(t, numShards, true, true, instances) //nolint:govet
	clientopts = clientopts.
		SetWriteConsistencyLevel(topology.ConsistencyLevelAll).
		SetReadConsistencyLevel(topology.ReadConsistencyLevelAll)

	defer closeFn()
	log := nodes[0].StorageOpts().InstrumentOptions().Logger()
	for _, n := range nodes {
		require.NoError(t, n.StartServer())
	}

	c, err := client.NewClient(clientopts)
	require.NoError(t, err)
	session, err := c.NewSession()
	require.NoError(t, err)
	defer session.Close() //nolint:errcheck

	now := xtime.ToUnixNano(nodes[0].DB().Options().ClockOptions().NowFn()())
	start := time.Now()
	log.Info("starting data write")

	id := ident.StringID("foo")

	for i := 0; i < 10; i++ {
		ts := now.Truncate(time.Second).Add(time.Duration(i * int(time.Second)))
		writeErr := session.Write(testNamespaces[0], id, ts, float64(i), xtime.Second, nil)
		require.NoError(t, writeErr)
	}
	log.Info("test data written", zap.Duration("took", time.Since(start)))

	expectedValues := []float64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	iter, err := session.Fetch(testNamespaces[0], id, now.Add(-time.Hour), now.Add(time.Hour))
	require.NoError(t, err)

	iterPools, err := session.IteratorPools()
	require.NoError(t, err)

	cloner := xenc.NewShallowCloner(iterPools)

	// ensure we're able to make multiple clones and iterate them without
	// affecting the ability to make further clones.
	iterCopyA, err := cloner.CloneSeriesIterator(iter)
	require.NoError(t, err)
	require.Equal(t, expectedValues, iterToValueSlice(t, iterCopyA))

	iterCopyB, err := cloner.CloneSeriesIterator(iter)
	require.NoError(t, err)
	require.Equal(t, expectedValues, iterToValueSlice(t, iterCopyB))

	log.Info("data is readable", zap.Duration("took", time.Since(start)))
}

func iterToValueSlice(
	t *testing.T,
	iter encoding.Iterator,
) []float64 {
	valueSlice := make([]float64, 0, 10)
	for iter.Next() {
		dp, _, _ := iter.Current()
		valueSlice = append(valueSlice, dp.Value)
	}
	require.NoError(t, iter.Err())
	iter.Close()
	return valueSlice
}
