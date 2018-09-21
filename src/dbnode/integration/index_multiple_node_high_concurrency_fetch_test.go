// +build integration
//
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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/shard"
	xclock "github.com/m3db/m3x/clock"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexMultipleNodeHighConcurrencyFetch(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}
	var (
		concurrency = 10
		writeEach   = 100
		numTags     = 10
	)

	numShards := defaultNumShards
	minShard := uint32(0)
	maxShard := uint32(numShards - 1)

	// nodes = m3db nodes
	nodes, closeFn, clientopts := makeMultiNodeSetup(t, numShards, true, true, []services.ServiceInstance{
		node(t, 0, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 1, newClusterShardsRange(minShard, maxShard, shard.Available)),
		node(t, 2, newClusterShardsRange(minShard, maxShard, shard.Available)),
	})

	defer closeFn()
	log := nodes[0].storageOpts.InstrumentOptions().Logger()
	// Start the nodes
	for _, n := range nodes {
		require.NoError(t, n.startServer())
	}

	c, err := client.NewClient(clientopts.SetReadConsistencyLevel(topology.ReadConsistencyLevelMajority))
	require.NoError(t, err)
	session, err := c.NewSession()
	require.NoError(t, err)
	defer session.Close()

	var (
		insertWg       sync.WaitGroup
		numTotalErrors uint32
	)
	now := nodes[0].db.Options().ClockOptions().NowFn()()
	start := time.Now()
	log.Info("starting data write")

	for i := 0; i < concurrency; i++ {
		insertWg.Add(1)
		idx := i
		go func() {
			numErrors := uint32(0)
			for j := 0; j < writeEach; j++ {
				id, tags := genIDTags(idx, j, numTags)
				err := session.WriteTagged(testNamespaces[0], id, tags, now, float64(1.0), xtime.Second, nil)
				if err != nil {
					numErrors++
				}
			}
			atomic.AddUint32(&numTotalErrors, numErrors)
			insertWg.Done()
		}()
	}

	insertWg.Wait()
	require.Zero(t, numTotalErrors)
	log.Infof("test data written in %v", time.Since(start))
	log.Infof("waiting to see if data is indexed")

	var (
		indexTimeout = 10 * time.Second
		fetchWg      sync.WaitGroup
	)
	for i := 0; i < concurrency; i++ {
		for j := 0; j < writeEach; j++ {
			id, tags := genIDTags(i, j, numTags)
			fetchWg.Add(1)
			go func() {
				indexed := xclock.WaitUntil(func() bool {
					found := isIndexed(t, session, testNamespaces[0], id, tags)
					return found
				}, indexTimeout)
				assert.True(t, indexed, "timed out waiting for index retrieval")
				fetchWg.Done()
			}()
		}
	}
	fetchWg.Wait()
	log.Infof("data is indexed in %v", time.Since(start))
}
