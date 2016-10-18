// Copyright (c) 2016 Uber Technologies, Inc.
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

package storage

import (
	"errors"
	"testing"
	"time"

	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/storage/repair"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

var testNamespaceID = ts.StringID("testNs")

var testShardIDs = []uint32{0, 1}

func newTestNamespace(t *testing.T) *dbNamespace {
	metadata := namespace.NewMetadata(testNamespaceID, namespace.NewOptions())
	hashFn := func(identifier ts.ID) uint32 { return testShardIDs[0] }
	shardSet, err := sharding.NewShardSet(testShardIDs, hashFn)
	require.NoError(t, err)
	dopts := testDatabaseOptions()
	ns := newDatabaseNamespace(metadata, shardSet, nil, nil, dopts).(*dbNamespace)
	for i := range ns.shards {
		ns.shards[i] = nil
	}
	return ns
}

func TestNamespaceName(t *testing.T) {
	ns := newTestNamespace(t)
	require.Equal(t, testNamespaceID, ns.ID())
}

func TestNamespaceTick(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ns := newTestNamespace(t)
	deadline := 100 * time.Millisecond
	expectedPerShardDeadline := deadline / time.Duration(len(testShardIDs))
	for i := range testShardIDs {
		shard := NewMockdatabaseShard(ctrl)
		shard.EXPECT().Tick(expectedPerShardDeadline)
		ns.shards[testShardIDs[i]] = shard
	}

	// Only asserting the expected methods are called
	ns.Tick(deadline)
}

func TestNamespaceWriteShardNotOwned(t *testing.T) {
	ctx := context.NewContext()
	defer ctx.Close()

	ns := newTestNamespace(t)
	err := ns.Write(ctx, ts.StringID("foo"), time.Now(), 0.0, xtime.Second, nil)
	require.Error(t, err)
	require.True(t, xerrors.IsInvalidParams(err))
}

func TestNamespaceWriteShardOwned(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	id := ts.StringID("foo")
	ts := time.Now()
	val := 0.0
	unit := xtime.Second
	ant := []byte(nil)

	ns := newTestNamespace(t)
	shard := NewMockdatabaseShard(ctrl)
	shard.EXPECT().Write(ctx, id, ts, val, unit, ant).Return(nil)
	ns.shards[testShardIDs[0]] = shard

	require.NoError(t, ns.Write(ctx, id, ts, val, unit, ant))
}

func TestNamespaceReadEncodedShardNotOwned(t *testing.T) {
	ctx := context.NewContext()
	defer ctx.Close()

	ns := newTestNamespace(t)
	_, err := ns.ReadEncoded(ctx, ts.StringID("foo"), time.Now(), time.Now())
	require.Error(t, err)
}

func TestNamespaceReadEncodedShardOwned(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	id := ts.StringID("foo")
	start := time.Now()
	end := time.Now().Add(time.Second)

	ns := newTestNamespace(t)
	shard := NewMockdatabaseShard(ctrl)
	shard.EXPECT().ReadEncoded(ctx, id, start, end).Return(nil, nil)
	ns.shards[testShardIDs[0]] = shard

	_, err := ns.ReadEncoded(ctx, id, start, end)
	require.NoError(t, err)
}

func TestNamespaceFetchBlocksShardNotOwned(t *testing.T) {
	ctx := context.NewContext()
	defer ctx.Close()

	ns := newTestNamespace(t)
	_, err := ns.FetchBlocks(ctx, testShardIDs[0], ts.StringID("foo"), nil)
	require.True(t, xerrors.IsInvalidParams(err))
}

func TestNamespaceFetchBlocksShardOwned(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	ns := newTestNamespace(t)
	shard := NewMockdatabaseShard(ctrl)
	shard.EXPECT().FetchBlocks(ctx, ts.StringID("foo"), nil).Return(nil)
	ns.shards[testShardIDs[0]] = shard

	res, err := ns.FetchBlocks(ctx, testShardIDs[0], ts.StringID("foo"), nil)
	require.Nil(t, res)
	require.NoError(t, err)
}

func TestNamespaceFetchBlocksMetadataShardNotOwned(t *testing.T) {
	ctx := context.NewContext()
	defer ctx.Close()

	ns := newTestNamespace(t)
	start := time.Now()
	end := start.Add(time.Hour)
	_, _, err := ns.FetchBlocksMetadata(ctx, testShardIDs[0], start, end, 100, 0, true, true)
	require.True(t, xerrors.IsInvalidParams(err))
}

func TestNamespaceFetchBlocksMetadataShardOwned(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	var (
		start            = time.Now()
		end              = start.Add(time.Hour)
		limit            = int64(100)
		pageToken        = int64(0)
		includeSizes     = true
		includeChecksums = true
		nextPageToken    = int64(100)
	)

	ns := newTestNamespace(t)
	shard := NewMockdatabaseShard(ctrl)
	shard.EXPECT().FetchBlocksMetadata(ctx, start, end, limit, pageToken, includeSizes, includeChecksums).Return(nil, &nextPageToken)
	ns.shards[testShardIDs[0]] = shard

	res, npt, err := ns.FetchBlocksMetadata(ctx, testShardIDs[0], start, end, limit, pageToken, includeSizes, includeChecksums)
	require.Nil(t, res)
	require.Equal(t, npt, &nextPageToken)
	require.NoError(t, err)
}

func TestNamespaceBootstrapAlreadyBootstrapped(t *testing.T) {
	ns := newTestNamespace(t)
	ns.bs = bootstrapped
	require.NoError(t, ns.Bootstrap(nil, nil, time.Now()))
}

func TestNamespaceBootstrapBootstrapping(t *testing.T) {
	ns := newTestNamespace(t)
	ns.bs = bootstrapping
	require.Equal(t, errNamespaceIsBootstrapping, ns.Bootstrap(nil, nil, time.Now()))
}

func TestNamespaceBootstrapDontNeedBootstrap(t *testing.T) {
	ns := newTestNamespace(t)
	ns.nopts = ns.nopts.SetNeedsBootstrap(false)
	require.NoError(t, ns.Bootstrap(nil, nil, time.Now()))
	require.Equal(t, bootstrapped, ns.bs)
}

func TestNamespaceBootstrapAllShards(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	writeStart := time.Now()
	ranges := xtime.NewRanges().AddRange(xtime.Range{
		Start: writeStart.Add(-time.Hour),
		End:   writeStart.Add(-10 * time.Minute),
	}).AddRange(xtime.Range{
		Start: writeStart.Add(-10 * time.Minute),
		End:   writeStart.Add(2 * time.Minute),
	})

	ns := newTestNamespace(t)
	errs := []error{nil, errors.New("foo")}
	bs := bootstrap.NewMockBootstrap(ctrl)
	bs.EXPECT().Run(ranges, ns.ID(), testShardIDs).Return(bootstrap.NewResult(), nil)
	for i := range errs {
		shard := NewMockdatabaseShard(ctrl)
		shard.EXPECT().ID().Return(uint32(i)).AnyTimes()
		shard.EXPECT().Bootstrap(nil, writeStart).Return(errs[i])
		ns.shards[testShardIDs[i]] = shard
	}

	require.Equal(t, "foo", ns.Bootstrap(bs, ranges, writeStart).Error())
	require.Equal(t, bootstrapped, ns.bs)
}

func TestNamespaceFlushNotBootstrapped(t *testing.T) {
	ns := newTestNamespace(t)
	require.Equal(t, errNamespaceNotBootstrapped, ns.Flush(time.Now(), nil))
}

func TestNamespaceFlushDontNeedFlush(t *testing.T) {
	ns := newTestNamespace(t)
	ns.bs = bootstrapped
	ns.nopts = ns.nopts.SetNeedsFlush(false)
	require.NoError(t, ns.Flush(time.Now(), nil))
}

func TestNamespaceFlushAllShards(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	blockStart := time.Now()

	ns := newTestNamespace(t)
	ns.bs = bootstrapped
	errs := []error{nil, errors.New("foo")}
	for i := range errs {
		shard := NewMockdatabaseShard(ctrl)
		shard.EXPECT().Flush(testNamespaceID, blockStart, nil).Return(errs[i])
		if errs[i] != nil {
			shard.EXPECT().ID().Return(testShardIDs[i])
		}
		ns.shards[testShardIDs[i]] = shard
	}

	require.Error(t, ns.Flush(blockStart, nil))
}

func TestNamespaceCleanupFilesetDontNeedCleanup(t *testing.T) {
	ns := newTestNamespace(t)
	ns.nopts = ns.nopts.SetNeedsFilesetCleanup(false)

	require.NoError(t, ns.CleanupFileset(time.Now()))
}

func TestNamespaceCleanupFilesetAllShards(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	earliestToRetain := time.Now()

	ns := newTestNamespace(t)
	errs := []error{nil, errors.New("foo")}
	for i := range errs {
		shard := NewMockdatabaseShard(ctrl)
		shard.EXPECT().CleanupFileset(testNamespaceID, earliestToRetain).Return(errs[i])
		ns.shards[testShardIDs[i]] = shard
	}

	require.Error(t, ns.CleanupFileset(earliestToRetain))
}

func TestNamespaceTruncate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ns := newTestNamespace(t)
	for _, shard := range testShardIDs {
		mockShard := NewMockdatabaseShard(ctrl)
		mockShard.EXPECT().NumSeries().Return(int64(shard))
		ns.shards[shard] = mockShard
	}

	res, err := ns.Truncate()
	require.NoError(t, err)
	require.Equal(t, int64(1), res)
	require.NotNil(t, ns.shards[testShardIDs[0]])
}

func TestNamespaceRepair(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ns := newTestNamespace(t)
	repairTime := time.Now()
	opts := repair.NewOptions().SetRepairThrottle(time.Duration(0))
	repairer := NewMockdatabaseShardRepairer(ctrl)
	repairer.EXPECT().Options().Return(opts).AnyTimes()

	errs := []error{nil, errors.New("foo")}
	for i := range errs {
		shard := NewMockdatabaseShard(ctrl)
		var res repair.MetadataComparisonResult
		if errs[i] == nil {
			res = repair.MetadataComparisonResult{
				NumSeries:           1,
				NumBlocks:           2,
				SizeDifferences:     repair.NewReplicaSeriesMetadata(),
				ChecksumDifferences: repair.NewReplicaSeriesMetadata(),
			}
		}
		shard.EXPECT().Repair(gomock.Any(), testNamespaceID, repairTime, repairer).Return(res, errs[i])
		ns.shards[testShardIDs[i]] = shard
	}

	require.Equal(t, "foo", ns.Repair(repairer, repairTime).Error())
}

func TestNamespaceShardAt(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ns := newTestNamespace(t)
	ns.shards[0] = NewMockdatabaseShard(ctrl)
	ns.shards[1] = NewMockdatabaseShard(ctrl)

	_, err := ns.shardAt(0)
	require.NoError(t, err)
	_, err = ns.shardAt(1)
	require.NoError(t, err)
	_, err = ns.shardAt(2)
	require.Error(t, err)
}
