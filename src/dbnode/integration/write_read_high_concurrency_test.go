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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/topology"
	xclock "github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestWriteReadHighConcurrencyTestMultiNS stress tests the conccurent write and read pathways in M3DB by spinning
// up 100s of gorotuines that all write/read to M3DB. It was added as a regression test to catch bugs in the M3DB
// client batching logic and lifecycles, but it is useful for detecting various kinds of concurrency issues at the
// integration level.
func TestWriteReadHighConcurrencyTestMultiNS(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}
	var (
		concurrency = 100
		writeEach   = 1000
		numShards   = defaultNumShards
		minShard    = uint32(0)
		maxShard    = uint32(numShards - 1)
		instances   = []services.ServiceInstance{
			node(t, 0, newClusterShardsRange(minShard, maxShard, shard.Available)),
			node(t, 1, newClusterShardsRange(minShard, maxShard, shard.Available)),
			node(t, 2, newClusterShardsRange(minShard, maxShard, shard.Available)),
		}
	)
	nodes, closeFn, clientopts := makeMultiNodeSetup(t, numShards, true, true, instances)
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
	defer session.Close()

	var insertWg sync.WaitGroup
	now := nodes[0].DB().Options().ClockOptions().NowFn()()
	start := time.Now()
	log.Info("starting data write")

	newNs1GenIDs := func(idx int) func(j int) ident.ID {
		return func(j int) ident.ID {
			id, _ := genIDTags(idx, j, 0)
			return id
		}
	}
	newNs2GenIDs := func(idx int) func(j int) ident.ID {
		return func(j int) ident.ID {
			id, _ := genIDTags(concurrency+idx, writeEach+j, 0)
			return id
		}
	}
	for i := 0; i < concurrency; i++ {
		insertWg.Add(2)
		idx := i
		ns1GenIDs := newNs1GenIDs(idx)
		ns2GenIDs := newNs2GenIDs(idx)
		go func() {
			defer insertWg.Done()
			for j := 0; j < writeEach; j++ {
				id := ns1GenIDs(j)
				err := session.Write(testNamespaces[0], id, now, float64(1.0), xtime.Second, nil)
				if err != nil {
					panic(err)
				}
			}
		}()
		go func() {
			defer insertWg.Done()
			for j := 0; j < writeEach; j++ {
				id := ns2GenIDs(j)
				err := session.Write(testNamespaces[1], id, now, float64(1.0), xtime.Second, nil)
				if err != nil {
					panic(err)
				}
			}
		}()
	}

	insertWg.Wait()
	log.Info("test data written", zap.Duration("took", time.Since(start)))

	var fetchWg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		fetchWg.Add(2)
		idx := i
		verify := func(genID func(j int) ident.ID, ns ident.ID) {
			defer fetchWg.Done()
			for j := 0; j < writeEach; j++ {
				id := genID(j)
				found := xclock.WaitUntil(func() bool {
					iter, err := session.Fetch(ns, id, now.Add(-time.Hour), now.Add(time.Hour))
					if err != nil {
						panic(err)
					}
					if !iter.Next() {
						return false
					}
					return true
				}, 10*time.Second)
				if !found {
					panic(fmt.Sprintf("timed out waiting to fetch id: %s", id))
				}
			}
		}
		go verify(newNs1GenIDs(idx), testNamespaces[0])
		go verify(newNs2GenIDs(idx), testNamespaces[1])
	}
	fetchWg.Wait()
	log.Info("data is readable", zap.Duration("took", time.Since(start)))
}
