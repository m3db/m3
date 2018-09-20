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
	"errors"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/topology"
	tu "github.com/m3db/m3/src/dbnode/topology/testutil"
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
		numShards                  = uint32(4)
		blockStart                 = time.Now().Truncate(blockSize)
		shardTimeRangesToBootstrap = result.ShardTimeRanges{}
		bootstrapRanges            = xtime.Ranges{}.AddRange(xtime.Range{
			Start: blockStart,
			End:   blockStart.Add(blockSize),
		})
	)

	for i := 0; i < int(numShards); i++ {
		shardTimeRangesToBootstrap[uint32(i)] = bootstrapRanges
	}

	testCases := []struct {
		title                             string
		topoState                         *topology.StateSnapshot
		shardsTimeRangesToBootstrap       result.ShardTimeRanges
		expectedAvailableShardsTimeRanges result.ShardTimeRanges
		expectedErr                       error
	}{
		{
			title: "Single node - Shard initializing",
			topoState: tu.NewStateSnapshot(1, tu.HostShardStates{
				tu.SelfID: tu.ShardsRange(0, numShards, shard.Initializing),
			}),
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: result.ShardTimeRanges{},
		},
		{
			title: "Single node - Shard unknown",
			topoState: tu.NewStateSnapshot(1, tu.HostShardStates{
				tu.SelfID: tu.ShardsRange(0, numShards, shard.Unknown),
			}),
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: result.ShardTimeRanges{},
			expectedErr:                       errors.New("unknown shard state: Unknown"),
		},
		{
			title: "Single node - Shard leaving",
			topoState: tu.NewStateSnapshot(1, tu.HostShardStates{
				tu.SelfID: tu.ShardsRange(0, numShards, shard.Leaving),
			}),
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: shardTimeRangesToBootstrap,
		},
		{
			title: "Single node - Shard available",
			topoState: tu.NewStateSnapshot(1, tu.HostShardStates{
				tu.SelfID: tu.ShardsRange(0, numShards, shard.Available),
			}),
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: shardTimeRangesToBootstrap,
		},
		{
			title: "Multi node - Origin available",
			topoState: tu.NewStateSnapshot(1, tu.HostShardStates{
				tu.SelfID: tu.ShardsRange(0, numShards, shard.Available),
				notSelfID: tu.ShardsRange(0, numShards, shard.Initializing),
			}),
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: shardTimeRangesToBootstrap,
		},
		{
			title: "Multi node - Origin not available",
			topoState: tu.NewStateSnapshot(1, tu.HostShardStates{
				tu.SelfID: tu.ShardsRange(0, numShards, shard.Initializing),
				notSelfID: tu.ShardsRange(0, numShards, shard.Available),
			}),
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: result.ShardTimeRanges{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.title, func(t *testing.T) {
			var (
				src          = newCommitLogSource(testOptions(), fs.Inspection{})
				runOpts      = testDefaultRunOpts.SetInitialTopologyState(tc.topoState)
				dataRes, err = src.AvailableData(nsMetadata, tc.shardsTimeRangesToBootstrap, runOpts)
			)

			if tc.expectedErr != nil {
				require.Equal(t, err, tc.expectedErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedAvailableShardsTimeRanges, dataRes)
			}

			indexRes, err := src.AvailableIndex(nsMetadata, tc.shardsTimeRangesToBootstrap, runOpts)
			if tc.expectedErr != nil {
				require.Equal(t, err, tc.expectedErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedAvailableShardsTimeRanges, indexRes)
			}
		})
	}
}
