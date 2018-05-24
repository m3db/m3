// Copyright (c) 2018 Uber Technologies, Inc.
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

package peers

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3db/src/dbnode/client"
	m3dbruntime "github.com/m3db/m3db/src/dbnode/runtime"
	"github.com/m3db/m3db/src/dbnode/sharding"
	"github.com/m3db/m3db/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3db/src/dbnode/topology"
	xtime "github.com/m3db/m3x/time"
	"github.com/stretchr/testify/require"
)

type sourceAvailableHost struct {
	host        string
	shards      []uint32
	shardStates shard.State
}

func TestPeersSourceAvailableDataAndIndex(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		replicaMajority            = 2
		blockSize                  = 2 * time.Hour
		nsMetadata                 = testNamespaceMetadata(t)
		shards                     = []uint32{0, 1, 2, 3}
		blockStart                 = time.Now().Truncate(blockSize)
		shardTimeRangesToBootstrap = result.ShardTimeRanges{}
		bootstrapRanges            = xtime.Ranges{}.AddRange(xtime.Range{
			Start: blockStart,
			End:   blockStart.Add(blockSize),
		})
	)

	for _, shard := range shards {
		shardTimeRangesToBootstrap[shard] = bootstrapRanges
	}

	shardTimeRangesToBootstrapOneExtra := shardTimeRangesToBootstrap.Copy()
	shardTimeRangesToBootstrapOneExtra[100] = bootstrapRanges

	testCases := []struct {
		title                             string
		hosts                             []sourceAvailableHost
		bootstrapReadConsistency          topology.ReadConsistencyLevel
		shardsTimeRangesToBootstrap       result.ShardTimeRanges
		expectedAvailableShardsTimeRanges result.ShardTimeRanges
	}{
		{
			title: "Returns empty if only self is available",
			hosts: []sourceAvailableHost{
				sourceAvailableHost{
					host:        "self",
					shards:      shards,
					shardStates: shard.Available,
				},
			},
			bootstrapReadConsistency:          topology.ReadConsistencyLevelMajority,
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: result.ShardTimeRanges{},
		},
		{
			title: "Returns empty if all other peers initializing/unknown",
			hosts: []sourceAvailableHost{
				sourceAvailableHost{
					host:        "self",
					shards:      shards,
					shardStates: shard.Available,
				},
				sourceAvailableHost{
					host:        "other1",
					shards:      shards,
					shardStates: shard.Initializing,
				},
				sourceAvailableHost{
					host:        "other2",
					shards:      shards,
					shardStates: shard.Unknown,
				},
			},
			bootstrapReadConsistency:          topology.ReadConsistencyLevelMajority,
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: result.ShardTimeRanges{},
		},
		{
			title: "Returns success if consistency can be met (available/leaving)",
			hosts: []sourceAvailableHost{
				sourceAvailableHost{
					host:        "self",
					shards:      shards,
					shardStates: shard.Initializing,
				},
				sourceAvailableHost{
					host:        "other1",
					shards:      shards,
					shardStates: shard.Available,
				},
				sourceAvailableHost{
					host:        "other2",
					shards:      shards,
					shardStates: shard.Leaving,
				},
			},
			bootstrapReadConsistency:          topology.ReadConsistencyLevelMajority,
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: shardTimeRangesToBootstrap,
		},
		{
			title: "Skips shards that were not in the topology at start",
			hosts: []sourceAvailableHost{
				sourceAvailableHost{
					host:        "self",
					shards:      shards,
					shardStates: shard.Initializing,
				},
				sourceAvailableHost{
					host:        "other1",
					shards:      shards,
					shardStates: shard.Available,
				},
				sourceAvailableHost{
					host:        "other2",
					shards:      shards,
					shardStates: shard.Available,
				},
			},
			bootstrapReadConsistency:          topology.ReadConsistencyLevelMajority,
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrapOneExtra,
			expectedAvailableShardsTimeRanges: shardTimeRangesToBootstrap,
		},
		{
			title: "Returns empty if consistency can not be met",
			hosts: []sourceAvailableHost{
				sourceAvailableHost{
					host:        "self",
					shards:      shards,
					shardStates: shard.Initializing,
				},
				sourceAvailableHost{
					host:        "other1",
					shards:      shards,
					shardStates: shard.Available,
				},
				sourceAvailableHost{
					host:        "other2",
					shards:      shards,
					shardStates: shard.Available,
				},
			},
			bootstrapReadConsistency:          topology.ReadConsistencyLevelAll,
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: result.ShardTimeRanges{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.title, func(t *testing.T) {
			hostShardSets := []topology.HostShardSet{}

			for _, host := range tc.hosts {
				shards := sharding.NewShards(host.shards, host.shardStates)
				shardSet, err := sharding.NewShardSet(shards, sharding.DefaultHashFn(0))
				require.NoError(t, err)

				hostShardSet := topology.NewHostShardSet(
					topology.NewHost(host.host, host.host+"address"), shardSet)
				hostShardSets = append(hostShardSets, hostShardSet)
			}

			mockMap := topology.NewMockMap(ctrl)
			mockMap.EXPECT().HostShardSets().Return(hostShardSets).AnyTimes()
			mockMap.EXPECT().MajorityReplicas().Return(replicaMajority).AnyTimes()

			mockTopology := topology.NewMockTopology(ctrl)
			mockTopology.EXPECT().
				Get().
				Return(mockMap)

			mockAdminSession := client.NewMockAdminSession(ctrl)
			mockAdminSession.EXPECT().
				Topology().
				Return(mockTopology, nil)

			mockClient := client.NewMockAdminClient(ctrl)
			mockClient.EXPECT().
				DefaultAdminSession().
				Return(mockAdminSession, nil)
			mockClient.EXPECT().
				Options().
				Return(
					client.NewAdminOptions().
						SetOrigin(topology.NewHost("self", "selfAddress")),
				).
				AnyTimes()

			mockRuntimeOpts := m3dbruntime.NewMockOptions(ctrl)
			mockRuntimeOpts.
				EXPECT().
				ClientBootstrapConsistencyLevel().
				Return(tc.bootstrapReadConsistency).
				AnyTimes()

			mockRuntimeOptsMgr := m3dbruntime.NewMockOptionsManager(ctrl)
			mockRuntimeOptsMgr.
				EXPECT().
				Get().
				Return(mockRuntimeOpts).
				AnyTimes()

			opts := testDefaultOpts.
				SetAdminClient(mockClient).
				SetRuntimeOptionsManager(mockRuntimeOptsMgr)

			src, err := newPeersSource(opts)
			require.NoError(t, err)

			dataRes := src.AvailableData(nsMetadata, tc.shardsTimeRangesToBootstrap)
			require.Equal(t, tc.expectedAvailableShardsTimeRanges, dataRes)

			indexRes := src.AvailableIndex(nsMetadata, tc.shardsTimeRangesToBootstrap)
			require.Equal(t, tc.expectedAvailableShardsTimeRanges, indexRes)
		})
	}
}
