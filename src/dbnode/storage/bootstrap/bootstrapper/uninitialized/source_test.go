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

package uninitialized

import (
	"errors"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/topology"
	tu "github.com/m3db/m3/src/dbnode/topology/testutil"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	testNamespaceID    = ident.StringID("testnamespace")
	testDefaultRunOpts = bootstrap.NewRunOptions()
	notSelfID1         = "not-self-1"
	notSelfID2         = "not-self-2"
	notSelfID3         = "not-self-3"
)

func TestUnitializedTopologySourceAvailableDataAndAvailableIndex(t *testing.T) {
	var (
		blockSize                  = 2 * time.Hour
		numShards                  = uint32(4)
		blockStart                 = time.Now().Truncate(blockSize)
		shardTimeRangesToBootstrap = result.ShardTimeRanges{}
		bootstrapRanges            = xtime.Ranges{}.AddRange(xtime.Range{
			Start: blockStart,
			End:   blockStart.Add(blockSize),
		})
	)
	nsMetadata, err := namespace.NewMetadata(testNamespaceID, namespace.NewOptions())
	require.NoError(t, err)

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
		// Snould return that it can bootstrap everything because
		// it's a new namespace.
		{
			title: "Single node - Shard initializing",
			topoState: tu.NewStateSnapshot(1, tu.HostShardStates{
				tu.SelfID: tu.ShardsRange(0, numShards, shard.Initializing),
			}),
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: shardTimeRangesToBootstrap,
		},
		// Snould return that it can't bootstrap anything because we don't
		// know how to handle unknown shard states.
		{
			title: "Single node - Shard unknown",
			topoState: tu.NewStateSnapshot(1, tu.HostShardStates{
				tu.SelfID: tu.ShardsRange(0, numShards, shard.Unknown),
			}),
			shardsTimeRangesToBootstrap: shardTimeRangesToBootstrap,
			expectedErr:                 errors.New("unknown shard state: Unknown"),
		},
		// Snould return that it can't bootstrap anything because it's not
		// a new namespace.
		{
			title: "Single node - Shard leaving",
			topoState: tu.NewStateSnapshot(1, tu.HostShardStates{
				tu.SelfID: tu.ShardsRange(0, numShards, shard.Leaving),
			}),
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: result.ShardTimeRanges{},
		},
		// Snould return that it can't bootstrap anything because it's not
		// a new namespace.
		{
			title: "Single node - Shard available",
			topoState: tu.NewStateSnapshot(1, tu.HostShardStates{
				tu.SelfID: tu.ShardsRange(0, numShards, shard.Available),
			}),
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: result.ShardTimeRanges{},
		},
		// Snould return that it can bootstrap everything because
		// it's a new namespace.
		{
			title: "Multi node - Brand new namespace (all nodes initializing)",
			topoState: tu.NewStateSnapshot(2, tu.HostShardStates{
				tu.SelfID:  tu.ShardsRange(0, numShards, shard.Initializing),
				notSelfID1: tu.ShardsRange(0, numShards, shard.Initializing),
				notSelfID2: tu.ShardsRange(0, numShards, shard.Initializing),
			}),
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: shardTimeRangesToBootstrap,
		},
		// Snould return that it can bootstrap everything because
		// it's a new namespace (one of the nodes hasn't completed
		// initializing yet.)
		{
			title: "Multi node - Recently created namespace (one node still initializing)",
			topoState: tu.NewStateSnapshot(2, tu.HostShardStates{
				tu.SelfID:  tu.ShardsRange(0, numShards, shard.Initializing),
				notSelfID1: tu.ShardsRange(0, numShards, shard.Available),
				notSelfID2: tu.ShardsRange(0, numShards, shard.Available),
			}),
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: shardTimeRangesToBootstrap,
		},
		// Snould return that it can't bootstrap anything because it's not
		// a new namespace.
		{
			title: "Multi node - Initialized namespace (no nodes initializing)",
			topoState: tu.NewStateSnapshot(2, tu.HostShardStates{
				tu.SelfID:  tu.ShardsRange(0, numShards, shard.Available),
				notSelfID1: tu.ShardsRange(0, numShards, shard.Available),
				notSelfID2: tu.ShardsRange(0, numShards, shard.Available),
			}),
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: result.ShardTimeRanges{},
		},
		// Snould return that it can't bootstrap anything because it's not
		// a new namespace, we're just doing a node replace.
		{
			title: "Multi node - Node replace (one node leaving, one initializing)",
			topoState: tu.NewStateSnapshot(2, tu.HostShardStates{
				tu.SelfID:  tu.ShardsRange(0, numShards, shard.Available),
				notSelfID1: tu.ShardsRange(0, numShards, shard.Leaving),
				notSelfID2: tu.ShardsRange(0, numShards, shard.Available),
				notSelfID3: tu.ShardsRange(0, numShards, shard.Initializing),
			}),
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: result.ShardTimeRanges{},
		},
		// Snould return that it can't bootstrap anything because we don't
		// know how to interpret the unknown host.
		{
			title: "Multi node - One node unknown",
			topoState: tu.NewStateSnapshot(2, tu.HostShardStates{
				tu.SelfID:  tu.ShardsRange(0, numShards, shard.Available),
				notSelfID1: tu.ShardsRange(0, numShards, shard.Available),
				notSelfID2: tu.ShardsRange(0, numShards, shard.Unknown),
			}),
			shardsTimeRangesToBootstrap: shardTimeRangesToBootstrap,
			expectedErr:                 errors.New("unknown shard state: Unknown"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.title, func(t *testing.T) {

			var (
				srcOpts = NewOptions().SetInstrumentOptions(instrument.NewOptions())
				src     = newTopologyUninitializedSource(srcOpts)
				runOpts = testDefaultRunOpts.SetInitialTopologyState(tc.topoState)
			)
			dataAvailabilityResult, dataErr := src.AvailableData(nsMetadata, tc.shardsTimeRangesToBootstrap, runOpts)
			indexAvailabilityResult, indexErr := src.AvailableIndex(nsMetadata, tc.shardsTimeRangesToBootstrap, runOpts)

			if tc.expectedErr != nil {
				require.Equal(t, tc.expectedErr, dataErr)
				require.Equal(t, tc.expectedErr, indexErr)
			} else {
				// Make sure AvailableData and AvailableIndex return the correct result
				require.Equal(t, tc.expectedAvailableShardsTimeRanges, dataAvailabilityResult)
				require.Equal(t, tc.expectedAvailableShardsTimeRanges, indexAvailabilityResult)

				// Make sure ReadData marks anything that AvailableData wouldn't return as unfulfilled
				dataResult, err := src.ReadData(nsMetadata, tc.shardsTimeRangesToBootstrap, runOpts)
				require.NoError(t, err)
				expectedDataUnfulfilled := tc.shardsTimeRangesToBootstrap.Copy()
				expectedDataUnfulfilled.Subtract(tc.expectedAvailableShardsTimeRanges)
				require.Equal(t, expectedDataUnfulfilled, dataResult.Unfulfilled())

				// Make sure ReadIndex marks anything that AvailableIndex wouldn't return as unfulfilled
				indexResult, err := src.ReadIndex(nsMetadata, tc.shardsTimeRangesToBootstrap, runOpts)
				require.NoError(t, err)
				expectedIndexUnfulfilled := tc.shardsTimeRangesToBootstrap.Copy()
				expectedIndexUnfulfilled.Subtract(tc.expectedAvailableShardsTimeRanges)
				require.Equal(t, expectedIndexUnfulfilled, indexResult.Unfulfilled())
			}
		})
	}
}
