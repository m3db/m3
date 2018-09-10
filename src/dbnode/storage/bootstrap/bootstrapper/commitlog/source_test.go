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

package commitlog

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	topotestutils "github.com/m3db/m3/src/dbnode/topology/testutil"
	"github.com/m3db/m3cluster/shard"
	xtime "github.com/m3db/m3x/time"
	"github.com/stretchr/testify/require"
)

var (
	testDefaultOpts = testOptions()
	notSelfID       = "not-self"
)

func TestAvailableData(t *testing.T) {
	var (
		nsMetadata                 = testNsMetadata(t)
		blockSize                  = 2 * time.Hour
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

	testCases := []struct {
		title                             string
		hosts                             topotestutils.SourceAvailableHosts
		majorityReplicas                  int
		shardsTimeRangesToBootstrap       result.ShardTimeRanges
		expectedAvailableShardsTimeRanges result.ShardTimeRanges
	}{
		{
			title: "Single node - Shard initializing",
			hosts: []topotestutils.SourceAvailableHost{
				topotestutils.SourceAvailableHost{
					Name:        topotestutils.SelfID,
					Shards:      shards,
					ShardStates: shard.Initializing,
				},
			},
			majorityReplicas:                  1,
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: result.ShardTimeRanges{},
		},
		{
			title: "Single node - Shard unknown",
			hosts: []topotestutils.SourceAvailableHost{
				topotestutils.SourceAvailableHost{
					Name:        topotestutils.SelfID,
					Shards:      shards,
					ShardStates: shard.Unknown,
				},
			},
			majorityReplicas:                  1,
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: result.ShardTimeRanges{},
		},
		{
			title: "Single node - Shard leaving",
			hosts: []topotestutils.SourceAvailableHost{
				topotestutils.SourceAvailableHost{
					Name:        topotestutils.SelfID,
					Shards:      shards,
					ShardStates: shard.Leaving,
				},
			},
			majorityReplicas:                  1,
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: shardTimeRangesToBootstrap,
		},
		{
			title: "Single node - Shard available",
			hosts: []topotestutils.SourceAvailableHost{
				topotestutils.SourceAvailableHost{
					Name:        topotestutils.SelfID,
					Shards:      shards,
					ShardStates: shard.Available,
				},
			},
			majorityReplicas:                  1,
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: shardTimeRangesToBootstrap,
		},
		{
			title: "Multi node - Origin available",
			hosts: []topotestutils.SourceAvailableHost{
				topotestutils.SourceAvailableHost{
					Name:        topotestutils.SelfID,
					Shards:      shards,
					ShardStates: shard.Available,
				},
				topotestutils.SourceAvailableHost{
					Name:        notSelfID,
					Shards:      shards,
					ShardStates: shard.Initializing,
				},
			},
			majorityReplicas:                  1,
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: shardTimeRangesToBootstrap,
		},
		{
			title: "Multi node - Origin not available",
			hosts: []topotestutils.SourceAvailableHost{
				topotestutils.SourceAvailableHost{
					Name:        topotestutils.SelfID,
					Shards:      shards,
					ShardStates: shard.Initializing,
				},
				topotestutils.SourceAvailableHost{
					Name:        notSelfID,
					Shards:      shards,
					ShardStates: shard.Available,
				},
			},
			majorityReplicas:                  1,
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: result.ShardTimeRanges{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.title, func(t *testing.T) {

			var (
				src       = newCommitLogSource(testOptions(), fs.Inspection{})
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
