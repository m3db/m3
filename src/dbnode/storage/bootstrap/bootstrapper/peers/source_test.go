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
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	m3dbruntime "github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/topology"
	tu "github.com/m3db/m3/src/dbnode/topology/testutil"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/instrument"
	xtime "github.com/m3db/m3/src/x/time"

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
		numShards                  = uint32(4)
		blockStart                 = time.Now().Truncate(blockSize)
		shardTimeRangesToBootstrap = result.NewShardTimeRanges()
		bootstrapRanges            = xtime.NewRanges(xtime.Range{
			Start: blockStart,
			End:   blockStart.Add(blockSize),
		})
		cacheOptions = bootstrap.NewCacheOptions().
				SetFilesystemOptions(fs.NewOptions()).
				SetInstrumentOptions(instrument.NewOptions())
	)

	for i := 0; i < int(numShards); i++ {
		shardTimeRangesToBootstrap.Set(uint32(i), bootstrapRanges)
	}

	shardTimeRangesToBootstrapOneExtra := shardTimeRangesToBootstrap.Copy()
	shardTimeRangesToBootstrapOneExtra.Set(100, bootstrapRanges)

	testCases := []struct {
		title                             string
		topoState                         *topology.StateSnapshot
		bootstrapReadConsistency          topology.ReadConsistencyLevel
		shardsTimeRangesToBootstrap       result.ShardTimeRanges
		expectedAvailableShardsTimeRanges result.ShardTimeRanges
		expectedErr                       error
	}{
		{
			title: "Returns empty if only self is available",
			topoState: tu.NewStateSnapshot(1, tu.HostShardStates{
				tu.SelfID: tu.ShardsRange(0, numShards, shard.Available),
			}),
			bootstrapReadConsistency:          topology.ReadConsistencyLevelMajority,
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: result.NewShardTimeRanges(),
		},
		{
			title: "Returns empty if all other peers initializing/unknown",
			topoState: tu.NewStateSnapshot(2, tu.HostShardStates{
				tu.SelfID:  tu.ShardsRange(0, numShards, shard.Available),
				notSelfID1: tu.ShardsRange(0, numShards, shard.Initializing),
				notSelfID2: tu.ShardsRange(0, numShards, shard.Unknown),
			}),
			bootstrapReadConsistency:          topology.ReadConsistencyLevelMajority,
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: result.NewShardTimeRanges(),
			expectedErr:                       errors.New("unknown shard state: Unknown"),
		},
		{
			title: "Returns success if consistency can be met (available/leaving)",
			topoState: tu.NewStateSnapshot(2, tu.HostShardStates{
				tu.SelfID:  tu.ShardsRange(0, numShards, shard.Initializing),
				notSelfID1: tu.ShardsRange(0, numShards, shard.Available),
				notSelfID2: tu.ShardsRange(0, numShards, shard.Leaving),
			}),
			bootstrapReadConsistency:          topology.ReadConsistencyLevelMajority,
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: shardTimeRangesToBootstrap,
		},
		{
			title: "Skips shards that were not in the topology at start",
			topoState: tu.NewStateSnapshot(2, tu.HostShardStates{
				tu.SelfID:  tu.ShardsRange(0, numShards, shard.Initializing),
				notSelfID1: tu.ShardsRange(0, numShards, shard.Available),
				notSelfID2: tu.ShardsRange(0, numShards, shard.Available),
			}),
			bootstrapReadConsistency:          topology.ReadConsistencyLevelMajority,
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrapOneExtra,
			expectedAvailableShardsTimeRanges: shardTimeRangesToBootstrap,
		},
		{
			title: "Returns empty if consistency can not be met",
			topoState: tu.NewStateSnapshot(2, tu.HostShardStates{
				tu.SelfID:  tu.ShardsRange(0, numShards, shard.Initializing),
				notSelfID1: tu.ShardsRange(0, numShards, shard.Available),
				notSelfID2: tu.ShardsRange(0, numShards, shard.Available),
			}),
			bootstrapReadConsistency:          topology.ReadConsistencyLevelAll,
			shardsTimeRangesToBootstrap:       shardTimeRangesToBootstrap,
			expectedAvailableShardsTimeRanges: result.NewShardTimeRanges(),
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

			opts := newTestDefaultOpts(t, ctrl).
				SetRuntimeOptionsManager(mockRuntimeOptsMgr)

			src, err := newPeersSource(opts)
			require.NoError(t, err)

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

			runOpts := testDefaultRunOpts.SetInitialTopologyState(tc.topoState)
			dataRes, err := src.AvailableData(nsMetadata, tc.shardsTimeRangesToBootstrap, cache, runOpts)
			if tc.expectedErr != nil {
				require.Equal(t, tc.expectedErr, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedAvailableShardsTimeRanges, dataRes)
			}

			indexRes, err := src.AvailableIndex(nsMetadata, tc.shardsTimeRangesToBootstrap, cache, runOpts)
			if tc.expectedErr != nil {
				require.Equal(t, tc.expectedErr, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedAvailableShardsTimeRanges, indexRes)
			}
		})
	}
}

func TestPeersSourceReturnsErrorIfUnknownPersistenceFileSetType(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		testNsMd = testNamespaceMetadata(t)
		opts     = newTestDefaultOpts(t, ctrl)
		ropts    = testNsMd.Options().RetentionOptions()

		start = time.Now().Add(-ropts.RetentionPeriod()).Truncate(ropts.BlockSize())
		end   = start.Add(2 * ropts.BlockSize())
	)

	src, err := newPeersSource(opts)
	require.NoError(t, err)

	target := result.NewShardTimeRanges().Set(
		0,
		xtime.NewRanges(xtime.Range{Start: start, End: end}),
	).Set(
		1,
		xtime.NewRanges(xtime.Range{Start: start, End: end}),
	)

	runOpts := testRunOptsWithPersist.SetPersistConfig(bootstrap.PersistConfig{Enabled: true, FileSetType: 999})
	tester := bootstrap.BuildNamespacesTesterWithFilesystemOptions(t, runOpts, target, opts.FilesystemOptions(), testNsMd)
	defer tester.Finish()

	ctx := context.NewBackground()
	defer ctx.Close()

	_, err = src.Read(ctx, tester.Namespaces, tester.Cache)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "unknown persist config fileset file type"))
	tester.EnsureNoLoadedBlocks()
	tester.EnsureNoWrites()
}
