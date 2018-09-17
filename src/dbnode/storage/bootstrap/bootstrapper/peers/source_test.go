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

	m3dbruntime "github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/topology"
	tu "github.com/m3db/m3/src/dbnode/topology/testutil"
	"github.com/m3db/m3cluster/shard"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const (
	notSelfID1 = "not-self1"
	notSelfID2 = "not-self2"
)

func TestPeersSourceAvailableDataAndIndex(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
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
		hosts                             tu.SourceAvailableHosts
		majorityReplicas                  int
		bootstrapReadConsistency          topology.ReadConsistencyLevel
		shardsTimeRangesToBootstrap       result.ShardTimeRanges
		expectedAvailableShardsTimeRanges result.ShardTimeRanges
	}{
		{
			title: "Returns empty if only self is available",
			hosts: []tu.SourceAvailableHost{
				tu.SourceAvailableHost{
					Name:        tu.SelfID,
					Shards:      shards,
					ShardStates: shard.Available,
				},
			},
			majorityReplicas:                  1,
			bootstrapReadConsistency:          topology.ReadConsistencyLevelMajority,
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: result.ShardTimeRanges{},
		},
		{
			title: "Returns empty if all other peers initializing/unknown",
			hosts: []tu.SourceAvailableHost{
				tu.SourceAvailableHost{
					Name:        tu.SelfID,
					Shards:      shards,
					ShardStates: shard.Available,
				},
				tu.SourceAvailableHost{
					Name:        notSelfID1,
					Shards:      shards,
					ShardStates: shard.Initializing,
				},
				tu.SourceAvailableHost{
					Name:        notSelfID2,
					Shards:      shards,
					ShardStates: shard.Unknown,
				},
			},
			majorityReplicas:                  2,
			bootstrapReadConsistency:          topology.ReadConsistencyLevelMajority,
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: result.ShardTimeRanges{},
		},
		{
			title: "Returns success if consistency can be met (available/leaving)",
			hosts: []tu.SourceAvailableHost{
				tu.SourceAvailableHost{
					Name:        tu.SelfID,
					Shards:      shards,
					ShardStates: shard.Initializing,
				},
				tu.SourceAvailableHost{
					Name:        notSelfID1,
					Shards:      shards,
					ShardStates: shard.Available,
				},
				tu.SourceAvailableHost{
					Name:        notSelfID2,
					Shards:      shards,
					ShardStates: shard.Leaving,
				},
			},
			bootstrapReadConsistency:          topology.ReadConsistencyLevelMajority,
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: shardTimeRangesToBootstrap,
		},
		{
			title: "Skips shards that were not in the topology at start",
			hosts: []tu.SourceAvailableHost{
				tu.SourceAvailableHost{
					Name:        tu.SelfID,
					Shards:      shards,
					ShardStates: shard.Initializing,
				},
				tu.SourceAvailableHost{
					Name:        notSelfID1,
					Shards:      shards,
					ShardStates: shard.Available,
				},
				tu.SourceAvailableHost{
					Name:        notSelfID2,
					Shards:      shards,
					ShardStates: shard.Available,
				},
			},
			majorityReplicas:                  2,
			bootstrapReadConsistency:          topology.ReadConsistencyLevelMajority,
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrapOneExtra,
			expectedAvailableShardsTimeRanges: shardTimeRangesToBootstrap,
		},
		{
			title: "Returns empty if consistency can not be met",
			hosts: []tu.SourceAvailableHost{
				tu.SourceAvailableHost{
					Name:        tu.SelfID,
					Shards:      shards,
					ShardStates: shard.Initializing,
				},
				tu.SourceAvailableHost{
					Name:        notSelfID1,
					Shards:      shards,
					ShardStates: shard.Available,
				},
				tu.SourceAvailableHost{
					Name:        notSelfID2,
					Shards:      shards,
					ShardStates: shard.Available,
				},
			},
			majorityReplicas:                  2,
			bootstrapReadConsistency:          topology.ReadConsistencyLevelAll,
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: result.ShardTimeRanges{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.title, func(t *testing.T) {
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
				SetRuntimeOptionsManager(mockRuntimeOptsMgr)

			src, err := newPeersSource(opts)
			require.NoError(t, err)

			var (
				topoState = tc.hosts.TopologyState(tc.majorityReplicas)
				runOpts   = testDefaultRunOpts.SetInitialTopologyState(topoState)
				dataRes   = src.AvailableData(nsMetadata, tc.shardsTimeRangesToBootstrap, runOpts)
			)
			require.Equal(t, tc.expectedAvailableShardsTimeRanges, dataRes)

			indexRes := src.AvailableIndex(nsMetadata, tc.shardsTimeRangesToBootstrap, runOpts)
			require.Equal(t, tc.expectedAvailableShardsTimeRanges, indexRes)
		})
	}
}
