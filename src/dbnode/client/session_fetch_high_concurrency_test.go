// Copyright (c) 2017 Uber Technologies, Inc.
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

package client

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

func TestSessionFetchIDsHighConcurrency(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	numShards := 1024
	numReplicas := 3
	numHosts := 8

	concurrency := 4
	fetchAllEach := 128

	maxIDs := 0
	fetchAllTypes := []struct {
		numIDs int
		ids    []string
	}{
		{numIDs: 16},
		{numIDs: 32},
	}
	for i := range fetchAllTypes {
		for j := 0; j < fetchAllTypes[i].numIDs; j++ {
			fetchAllTypes[i].ids = append(fetchAllTypes[i].ids, fmt.Sprintf("foo.%d", j))
		}
		if fetchAllTypes[i].numIDs > maxIDs {
			maxIDs = fetchAllTypes[i].numIDs
		}
	}

	healthCheckResult := &rpc.NodeHealthResult_{Ok: true, Status: "ok", Bootstrapped: true}

	start := time.Now().Truncate(time.Hour)
	end := start.Add(2 * time.Hour)

	encoder := m3tsz.NewEncoder(start, nil, true, nil)
	for at := start; at.Before(end); at = at.Add(30 * time.Second) {
		dp := ts.Datapoint{
			Timestamp: at,
			Value:     rand.Float64() * math.MaxFloat64,
		}
		encoder.Encode(dp, xtime.Second, nil)
	}
	seg := encoder.Discard()
	respSegments := []*rpc.Segments{&rpc.Segments{
		Merged: &rpc.Segment{Head: seg.Head.Bytes(), Tail: seg.Tail.Bytes()},
	}}
	respElements := make([]*rpc.FetchRawResult_, maxIDs)
	for i := range respElements {
		respElements[i] = &rpc.FetchRawResult_{Segments: respSegments}
	}
	respResult := &rpc.FetchBatchRawResult_{Elements: respElements}

	// Override the new connection function for connection pools
	// to be able to mock the entire end to end pipeline
	newConnFn := func(
		_ string, addr string, _ Options,
	) (PooledChannel, rpc.TChanNode, error) {
		mockClient := rpc.NewMockTChanNode(ctrl)
		mockClient.EXPECT().Health(gomock.Any()).
			Return(healthCheckResult, nil).
			AnyTimes()
		mockClient.EXPECT().FetchBatchRaw(gomock.Any(), gomock.Any()).
			Return(respResult, nil).
			AnyTimes()
		return &noopPooledChannel{}, mockClient, nil
	}
	shards := make([]shard.Shard, numShards)
	for i := range shards {
		shards[i] = shard.NewShard(uint32(i)).SetState(shard.Available)
	}

	shardSet, err := sharding.NewShardSet(shards, sharding.DefaultHashFn(numShards))
	require.NoError(t, err)

	hosts := make([]topology.Host, numHosts)
	for i := range hosts {
		id := testHostName(i)
		hosts[i] = topology.NewHost(id, fmt.Sprintf("%s:9000", id))
	}

	shardAssignments := make([][]shard.Shard, numHosts)
	allShards := shardSet.All()
	host := 0
	for i := 0; i < numReplicas; i++ {
		for shard := 0; shard < numShards; shard++ {
			placed := false
			for !placed {
				unique := true
				for _, existing := range shardAssignments[host] {
					if existing.ID() == uint32(shard) {
						unique = false
						break
					}
				}
				if unique {
					placed = true
					shardAssignments[host] = append(shardAssignments[host], allShards[shard])
				}
				host++
				if host >= len(hosts) {
					host = 0
				}
			}
			if !placed {
				assert.Fail(t, "could not place shard")
			}
		}
	}

	hostShardSets := make([]topology.HostShardSet, numHosts)
	for hostIdx, shards := range shardAssignments {
		shardsSubset, err := sharding.NewShardSet(shards, shardSet.HashFn())
		require.NoError(t, err)
		hostShardSets[hostIdx] = topology.NewHostShardSet(hosts[hostIdx], shardsSubset)
	}

	opts := newSessionTestOptions().
		SetFetchBatchSize(128).
		SetNewConnectionFn(newConnFn).
		SetTopologyInitializer(topology.NewStaticInitializer(
			topology.NewStaticOptions().
				SetReplicas(numReplicas).
				SetShardSet(shardSet).
				SetHostShardSets(sessionTestHostAndShards(shardSet))))

	s, err := newSession(opts)
	assert.NoError(t, err)
	session := s.(*session)

	require.NoError(t, session.Open())

	var wg, startWg sync.WaitGroup
	startWg.Add(1)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			startWg.Wait()
			defer wg.Done()

			for j := 0; j < fetchAllEach; j++ {
				ids := fetchAllTypes[j%len(fetchAllTypes)].ids
				iters, err := session.FetchIDs(ident.StringID(testNamespaceName),
					ident.NewStringIDsSliceIterator(ids), start, end)
				if err != nil {
					panic(err)
				}
				iters.Close()
			}
		}()
	}

	startWg.Done()

	wg.Wait()

	require.NoError(t, session.Close())
}
