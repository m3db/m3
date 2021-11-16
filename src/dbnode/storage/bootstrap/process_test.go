// Copyright (c) 2021 Uber Technologies, Inc.
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

package bootstrap

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/topology"
	xcontext "github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xtime "github.com/m3db/m3/src/x/time"
)

func TestBootstrapProcessRunActiveBlockAdvanced(t *testing.T) {
	tests := []struct {
		name               string
		shardsInitializing bool
		expectErr          error
	}{
		{
			name:               "time shifted and shards initializing, should return error",
			shardsInitializing: true,
			expectErr:          ErrFileSetSnapshotTypeRangeAdvanced,
		},
		{
			name:               "time shifted and shards all available, should not return error",
			shardsInitializing: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			var (
				ctx          = xcontext.NewBackground()
				blockSize    = time.Hour
				startTime    = xtime.Now().Truncate(blockSize)
				bufferPast   = 30 * time.Minute
				bufferFuture = 30 * time.Minute
				// shift 'now' just enough so that after adding 'bufferFuture' it would reach the next block
				now           = startTime.Add(blockSize - bufferFuture)
				shards        = []uint32{0}
				retentionOpts = retention.NewOptions().
						SetBlockSize(blockSize).
						SetRetentionPeriod(12 * blockSize).
						SetBufferPast(bufferPast).
						SetBufferFuture(bufferFuture)
				nsOptions = namespace.NewOptions().SetRetentionOptions(retentionOpts)
				nsID      = ident.StringID("ns")
				ns, err   = namespace.NewMetadata(nsID, nsOptions)
			)
			require.NoError(t, err)

			processNs := []ProcessNamespace{
				{
					Metadata:        ns,
					Shards:          shards,
					DataAccumulator: NewMockNamespaceDataAccumulator(ctrl),
				},
			}

			bootstrapper := NewMockBootstrapper(ctrl)
			bootstrapper.EXPECT().String().Return("mock_bootstrapper").AnyTimes()
			bootstrapper.EXPECT().
				Bootstrap(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(_, _, _ interface{}) (NamespaceResults, error) {
					return NewNamespaceResults(NewNamespaces(processNs)), nil
				}).
				AnyTimes()

			shardState := shard.Available
			if test.shardsInitializing {
				shardState = shard.Initializing
			}
			shardSet, err := sharding.NewShardSet(sharding.NewShards(shards, shardState),
				sharding.DefaultHashFn(len(shards)))
			require.NoError(t, err)

			origin := topology.NewHost("self", "127.0.0.1:9000")
			hostShardSet := topology.NewHostShardSet(origin, shardSet)

			topoMapOpts := topology.NewStaticOptions().
				SetReplicas(1).
				SetShardSet(shardSet).
				SetHostShardSets([]topology.HostShardSet{hostShardSet})
			topoMap := topology.NewStaticMap(topoMapOpts)

			topoState, err := newInitialTopologyState(origin, topoMap)
			require.NoError(t, err)

			processOpts := NewProcessOptions().SetOrigin(origin)
			process := bootstrapProcess{
				processOpts:          processOpts,
				resultOpts:           result.NewOptions(),
				fsOpts:               fs.NewOptions(),
				nowFn:                func() time.Time { return now.ToTime() },
				log:                  instrument.NewOptions().Logger(),
				bootstrapper:         bootstrapper,
				initialTopologyState: topoState,
			}

			_, err = process.Run(ctx, startTime, processNs)
			require.Equal(t, test.expectErr, err)
		})
	}
}

func TestTargetRangesFileSetTypeForSnapshotDisabledNamespace(t *testing.T) {
	sut := bootstrapProcess{processOpts: NewProcessOptions()}
	nsOpts := namespace.NewOptions().SetSnapshotEnabled(false)

	rangesForData := sut.targetRangesForData(xtime.Now(), nsOpts)
	rangesForIndex := sut.targetRangesForIndex(xtime.Now(), nsOpts)

	requireFilesetTypes(t, rangesForData, persist.FileSetFlushType)
	requireFilesetTypes(t, rangesForIndex, persist.FileSetFlushType)
}

func TestTargetRangesFileSetTypeForSnapshotEnabledNamespace(t *testing.T) {
	sut := bootstrapProcess{processOpts: NewProcessOptions()}
	nsOpts := namespace.NewOptions().SetSnapshotEnabled(true)

	rangesForData := sut.targetRangesForData(xtime.Now(), nsOpts)
	rangesForIndex := sut.targetRangesForIndex(xtime.Now(), nsOpts)

	requireFilesetTypes(t, rangesForData, persist.FileSetSnapshotType)
	requireFilesetTypes(t, rangesForIndex, persist.FileSetSnapshotType)
}

func requireFilesetTypes(t *testing.T, ranges targetRangesResult, expectedSecond persist.FileSetType) {
	persistConfigFirstRange := ranges.firstRangeWithPersistTrue.RunOptions.PersistConfig()
	require.True(t, persistConfigFirstRange.Enabled)
	require.Equal(t, persist.FileSetFlushType, persistConfigFirstRange.FileSetType)

	persistConfigSecondRange := ranges.secondRange.RunOptions.PersistConfig()
	require.True(t, persistConfigSecondRange.Enabled)
	require.Equal(t, expectedSecond, persistConfigSecondRange.FileSetType)
}
