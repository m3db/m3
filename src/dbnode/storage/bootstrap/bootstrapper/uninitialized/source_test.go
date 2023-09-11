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

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/topology"
	tu "github.com/m3db/m3/src/dbnode/topology/testutil"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xtime "github.com/m3db/m3/src/x/time"

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
		blockStart                 = xtime.Now().Truncate(blockSize)
		shardTimeRangesToBootstrap = result.NewShardTimeRanges()
		bootstrapRanges            = xtime.NewRanges(xtime.Range{
			Start: blockStart,
			End:   blockStart.Add(blockSize),
		})
		cacheOptions = bootstrap.NewCacheOptions().
				SetFilesystemOptions(fs.NewOptions()).
				SetInstrumentOptions(instrument.NewOptions())
	)
	nsOpts := namespace.NewOptions()
	nsOpts = nsOpts.SetIndexOptions(nsOpts.IndexOptions().SetEnabled(true))
	nsMetadata, err := namespace.NewMetadata(testNamespaceID, nsOpts)
	require.NoError(t, err)

	for i := 0; i < int(numShards); i++ {
		shardTimeRangesToBootstrap.Set(uint32(i), bootstrapRanges)
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
			expectedAvailableShardsTimeRanges: result.NewShardTimeRanges(),
		},
		// Snould return that it can't bootstrap anything because it's not
		// a new namespace.
		{
			title: "Single node - Shard available",
			topoState: tu.NewStateSnapshot(1, tu.HostShardStates{
				tu.SelfID: tu.ShardsRange(0, numShards, shard.Available),
			}),
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: result.NewShardTimeRanges(),
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
			expectedAvailableShardsTimeRanges: result.NewShardTimeRanges(),
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
			expectedAvailableShardsTimeRanges: result.NewShardTimeRanges(),
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
			var shards []uint32
			for shard := range tc.shardsTimeRangesToBootstrap.Iter() {
				shards = append(shards, shard)
			}
			cache, sErr := bootstrap.NewCache(cacheOptions.
				SetNamespaceDetails([]bootstrap.NamespaceDetails{
					{
						Namespace: nsMetadata,
						Shards:    shards,
					},
				}))
			require.NoError(t, sErr)

			dataAvailabilityResult, dataErr := src.AvailableData(nsMetadata, tc.shardsTimeRangesToBootstrap, cache, runOpts)
			indexAvailabilityResult, indexErr := src.AvailableIndex(nsMetadata, tc.shardsTimeRangesToBootstrap, cache, runOpts)

			if tc.expectedErr != nil {
				require.Equal(t, tc.expectedErr, dataErr)
				require.Equal(t, tc.expectedErr, indexErr)
			} else {
				// Make sure AvailableData and AvailableIndex return the correct result
				require.Equal(t, tc.expectedAvailableShardsTimeRanges, dataAvailabilityResult)
				require.Equal(t, tc.expectedAvailableShardsTimeRanges, indexAvailabilityResult)

				// Make sure Read marks anything that available ranges wouldn't return as unfulfilled
				tester := bootstrap.BuildNamespacesTester(t, runOpts, tc.shardsTimeRangesToBootstrap, nsMetadata)
				defer tester.Finish()
				tester.TestReadWith(src)

				expectedDataUnfulfilled := tc.shardsTimeRangesToBootstrap.Copy()
				expectedDataUnfulfilled.Subtract(tc.expectedAvailableShardsTimeRanges)
				expectedIndexUnfulfilled := tc.shardsTimeRangesToBootstrap.Copy()
				expectedIndexUnfulfilled.Subtract(tc.expectedAvailableShardsTimeRanges)
				tester.TestUnfulfilledForNamespace(
					nsMetadata,
					expectedDataUnfulfilled,
					expectedIndexUnfulfilled,
				)

				tester.EnsureNoLoadedBlocks()
				tester.EnsureNoWrites()
			}
		})
	}
}
