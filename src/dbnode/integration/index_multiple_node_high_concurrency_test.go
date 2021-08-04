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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/topology"
	xclock "github.com/m3db/m3/src/x/clock"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestIndexMultipleNodeHighConcurrency(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}
	var (
		concurrency = 10
		writeEach   = 100
		numTags     = 10
	)

	levels := []topology.ReadConsistencyLevel{
		topology.ReadConsistencyLevelOne,
		topology.ReadConsistencyLevelUnstrictMajority,
		topology.ReadConsistencyLevelMajority,
		topology.ReadConsistencyLevelUnstrictAll,
		topology.ReadConsistencyLevelAll,
	}
	for _, lvl := range levels {
		t.Run(
			fmt.Sprintf("running test for %v", lvl),
			func(t *testing.T) {
				numShards := defaultNumShards
				minShard := uint32(0)
				maxShard := uint32(numShards - 1)

				instances := []services.ServiceInstance{
					node(t, 0, newClusterShardsRange(minShard, maxShard, shard.Available)),
					node(t, 1, newClusterShardsRange(minShard, maxShard, shard.Available)),
					node(t, 2, newClusterShardsRange(minShard, maxShard, shard.Available)),
				}
				// nodes = m3db nodes
				nodes, closeFn, clientopts := makeMultiNodeSetup(t, numShards, true, true, instances) //nolint:govet
				clientopts = clientopts.SetReadConsistencyLevel(lvl)

				defer closeFn()
				log := nodes[0].StorageOpts().InstrumentOptions().Logger()
				// Start the nodes
				for _, n := range nodes {
					require.NoError(t, n.StartServer())
				}

				c, err := client.NewClient(clientopts)
				require.NoError(t, err)
				session, err := c.NewSession()
				require.NoError(t, err)
				defer session.Close()

				var (
					insertWg       sync.WaitGroup
					numTotalErrors uint32
				)
				now := xtime.ToUnixNano(nodes[0].DB().Options().ClockOptions().NowFn()())
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
				log.Info("test data written", zap.Duration("took", time.Since(start)))
				log.Info("waiting to see if data is indexed")

				var (
					indexTimeout = 10 * time.Second
					fetchWg      sync.WaitGroup
				)
				for i := 0; i < concurrency; i++ {
					fetchWg.Add(1)
					idx := i
					go func() {
						id, tags := genIDTags(idx, writeEach-1, numTags)
						indexed := xclock.WaitUntil(func() bool {
							found := isIndexed(t, session, testNamespaces[0], id, tags)
							return found
						}, indexTimeout)
						assert.True(t, indexed, "timed out waiting for index retrieval")
						fetchWg.Done()
					}()
				}
				fetchWg.Wait()
				log.Info("data is indexed", zap.Duration("took", time.Since(start)))
			})
	}
}
