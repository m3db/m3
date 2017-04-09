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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/storage/repair"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3db/x/metrics"
	"github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

var testNamespaceID = ts.StringID("testNs")

var testShardIDs = sharding.NewShards([]uint32{0, 1}, shard.Available)

func newTestNamespace(t *testing.T) *dbNamespace {
	metadata := namespace.NewMetadata(testNamespaceID, namespace.NewOptions())
	hashFn := func(identifier ts.ID) uint32 { return testShardIDs[0].ID() }
	shardSet, err := sharding.NewShardSet(testShardIDs, hashFn)
	require.NoError(t, err)
	dopts := testDatabaseOptions()
	return newDatabaseNamespace(metadata, shardSet, nil, nil, nil, dopts).(*dbNamespace)
}

func TestNamespaceName(t *testing.T) {
	ns := newTestNamespace(t)
	require.True(t, testNamespaceID.Equal(ns.ID()))
}

func TestNamespaceTick(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ns := newTestNamespace(t)
	deadline := 100 * time.Millisecond
	expectedPerShardDeadline := deadline / time.Duration(len(testShardIDs))
	for i := range testShardIDs {
		shard := NewMockdatabaseShard(ctrl)
		shard.EXPECT().Tick(context.NewNoOpCanncellable(), expectedPerShardDeadline)
		ns.shards[testShardIDs[i].ID()] = shard
	}

	// Only asserting the expected methods are called
	ns.Tick(context.NewNoOpCanncellable(), deadline)
}

func TestNamespaceWriteShardNotOwned(t *testing.T) {
	ctx := context.NewContext()
	defer ctx.Close()

	ns := newTestNamespace(t)
	for i := range ns.shards {
		ns.shards[i] = nil
	}
	err := ns.Write(ctx, ts.StringID("foo"), time.Now(), 0.0, xtime.Second, nil)
	require.Error(t, err)
	require.True(t, xerrors.IsRetryableError(err))
	require.Equal(t, "not responsible for shard 0", err.Error())
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
	ns.shards[testShardIDs[0].ID()] = shard

	require.NoError(t, ns.Write(ctx, id, ts, val, unit, ant))
}

func TestNamespaceReadEncodedShardNotOwned(t *testing.T) {
	ctx := context.NewContext()
	defer ctx.Close()

	ns := newTestNamespace(t)
	for i := range ns.shards {
		ns.shards[i] = nil
	}
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
	ns.shards[testShardIDs[0].ID()] = shard

	shard.EXPECT().IsBootstrapped().Return(true)
	_, err := ns.ReadEncoded(ctx, id, start, end)
	require.NoError(t, err)

	shard.EXPECT().IsBootstrapped().Return(false)
	_, err = ns.ReadEncoded(ctx, id, start, end)
	require.Error(t, err)
	require.True(t, xerrors.IsRetryableError(err))
	require.Equal(t, errShardNotBootstrappedToRead, xerrors.GetInnerRetryableError(err))
}

func TestNamespaceFetchBlocksShardNotOwned(t *testing.T) {
	ctx := context.NewContext()
	defer ctx.Close()

	ns := newTestNamespace(t)
	for i := range ns.shards {
		ns.shards[i] = nil
	}
	_, err := ns.FetchBlocks(ctx, testShardIDs[0].ID(), ts.StringID("foo"), nil)
	require.True(t, xerrors.IsRetryableError(err))
	require.Equal(t, "not responsible for shard 0", err.Error())
}

func TestNamespaceFetchBlocksShardOwned(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	ns := newTestNamespace(t)
	shard := NewMockdatabaseShard(ctrl)
	shard.EXPECT().FetchBlocks(ctx, ts.NewIDMatcher("foo"), nil).Return(nil, nil)
	ns.shards[testShardIDs[0].ID()] = shard

	shard.EXPECT().IsBootstrapped().Return(true)
	res, err := ns.FetchBlocks(ctx, testShardIDs[0].ID(), ts.StringID("foo"), nil)
	require.NoError(t, err)
	require.Nil(t, res)

	shard.EXPECT().IsBootstrapped().Return(false)
	_, err = ns.FetchBlocks(ctx, testShardIDs[0].ID(), ts.StringID("foo"), nil)
	require.Error(t, err)
	require.True(t, xerrors.IsRetryableError(err))
	require.Equal(t, errShardNotBootstrappedToRead, xerrors.GetInnerRetryableError(err))
}

func TestNamespaceFetchBlocksMetadataShardNotOwned(t *testing.T) {
	ctx := context.NewContext()
	defer ctx.Close()

	ns := newTestNamespace(t)
	for i := range ns.shards {
		ns.shards[i] = nil
	}
	start := time.Now()
	end := start.Add(time.Hour)
	_, _, err := ns.FetchBlocksMetadata(ctx, testShardIDs[0].ID(), start, end, 100, 0, true, true)
	require.True(t, xerrors.IsRetryableError(err))
	require.Equal(t, "not responsible for shard 0", err.Error())
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
	shard.EXPECT().
		FetchBlocksMetadata(ctx, start, end, limit, pageToken,
			includeSizes, includeChecksums).
		Return(nil, &nextPageToken)
	ns.shards[testShardIDs[0].ID()] = shard

	shard.EXPECT().IsBootstrapped().Return(true)
	res, npt, err := ns.FetchBlocksMetadata(ctx, testShardIDs[0].ID(),
		start, end, limit, pageToken, includeSizes, includeChecksums)
	require.Nil(t, res)
	require.Equal(t, npt, &nextPageToken)
	require.NoError(t, err)

	shard.EXPECT().IsBootstrapped().Return(false)
	_, _, err = ns.FetchBlocksMetadata(ctx, testShardIDs[0].ID(),
		start, end, limit, pageToken, includeSizes, includeChecksums)
	require.Error(t, err)
	require.True(t, xerrors.IsRetryableError(err))
	require.Equal(t, errShardNotBootstrappedToRead, xerrors.GetInnerRetryableError(err))
}

func TestNamespaceBootstrapBootstrapping(t *testing.T) {
	ns := newTestNamespace(t)
	ns.bs = bootstrapping
	require.Equal(t, errNamespaceIsBootstrapping, ns.Bootstrap(nil, nil))
}

func TestNamespaceBootstrapDontNeedBootstrap(t *testing.T) {
	ns := newTestNamespace(t)
	ns.nopts = ns.nopts.SetNeedsBootstrap(false)
	require.NoError(t, ns.Bootstrap(nil, nil))
	require.Equal(t, bootstrapped, ns.bs)
}

func TestNamespaceBootstrapAllShards(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	writeStart := time.Now()
	ranges := []bootstrap.TargetRange{
		{Range: xtime.Range{
			Start: writeStart.Add(-time.Hour),
			End:   writeStart.Add(-10 * time.Minute),
		}},
		{Range: xtime.Range{
			Start: writeStart.Add(-10 * time.Minute),
			End:   writeStart.Add(2 * time.Minute),
		}},
	}

	ns := newTestNamespace(t)
	errs := []error{nil, errors.New("foo")}
	bs := bootstrap.NewMockProcess(ctrl)
	bs.EXPECT().
		Run(ns.ID(), sharding.IDs(testShardIDs), ranges).
		Return(result.NewBootstrapResult(), nil)
	for i := range errs {
		shard := NewMockdatabaseShard(ctrl)
		shard.EXPECT().IsBootstrapped().Return(false)
		shard.EXPECT().ID().Return(uint32(i)).AnyTimes()
		shard.EXPECT().Bootstrap(nil).Return(errs[i])
		ns.shards[testShardIDs[i].ID()] = shard
	}

	require.Equal(t, "foo", ns.Bootstrap(bs, ranges).Error())
	require.Equal(t, bootstrapped, ns.bs)
}

func TestNamespaceBootstrapOnlyNonBootstrappedShards(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	writeStart := time.Now()
	ranges := []bootstrap.TargetRange{
		{Range: xtime.Range{
			Start: writeStart.Add(-time.Hour),
			End:   writeStart.Add(-10 * time.Minute),
		}},
		{Range: xtime.Range{
			Start: writeStart.Add(-10 * time.Minute),
			End:   writeStart.Add(2 * time.Minute),
		}},
	}

	var needsBootstrap, alreadyBootstrapped []shard.Shard
	for i, shard := range testShardIDs {
		if i%2 == 0 {
			needsBootstrap = append(needsBootstrap, shard)
		} else {
			alreadyBootstrapped = append(alreadyBootstrapped, shard)
		}
	}

	require.True(t, len(needsBootstrap) > 0)
	require.True(t, len(alreadyBootstrapped) > 0)

	ns := newTestNamespace(t)
	bs := bootstrap.NewMockProcess(ctrl)
	bs.EXPECT().
		Run(ns.ID(), sharding.IDs(needsBootstrap), ranges).
		Return(result.NewBootstrapResult(), nil)

	for _, testShard := range needsBootstrap {
		shard := NewMockdatabaseShard(ctrl)
		shard.EXPECT().IsBootstrapped().Return(false)
		shard.EXPECT().ID().Return(testShard.ID()).AnyTimes()
		shard.EXPECT().Bootstrap(nil).Return(nil)
		ns.shards[testShard.ID()] = shard
	}
	for _, testShard := range alreadyBootstrapped {
		shard := NewMockdatabaseShard(ctrl)
		shard.EXPECT().IsBootstrapped().Return(true)
		ns.shards[testShard.ID()] = shard
	}

	require.NoError(t, ns.Bootstrap(bs, ranges))
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
		shard.EXPECT().Flush(ts.NewIDMatcher(testNamespaceID.String()), blockStart, nil).Return(errs[i])
		if errs[i] != nil {
			shard.EXPECT().ID().Return(testShardIDs[i].ID())
		}
		ns.shards[testShardIDs[i].ID()] = shard
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
		shard.EXPECT().
			CleanupFileset(ts.NewIDMatcher(testNamespaceID.String()), earliestToRetain).
			Return(errs[i])
		ns.shards[testShardIDs[i].ID()] = shard
	}

	require.Error(t, ns.CleanupFileset(earliestToRetain))
}

func TestNamespaceTruncate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ns := newTestNamespace(t)
	for _, shard := range testShardIDs {
		mockShard := NewMockdatabaseShard(ctrl)
		mockShard.EXPECT().NumSeries().Return(int64(shard.ID()))
		ns.shards[shard.ID()] = mockShard
	}

	res, err := ns.Truncate()
	require.NoError(t, err)
	require.Equal(t, int64(1), res)
	require.NotNil(t, ns.shards[testShardIDs[0].ID()])
	require.True(t, ns.shards[testShardIDs[0].ID()].IsBootstrapped())
}

func TestNamespaceRepair(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ns := newTestNamespace(t)
	now := time.Now()
	repairTimeRange := xtime.Range{Start: now, End: now.Add(time.Hour)}
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
		shard.EXPECT().
			Repair(gomock.Any(), ts.NewIDMatcher(testNamespaceID.String()), repairTimeRange, repairer).
			Return(res, errs[i])
		ns.shards[testShardIDs[i].ID()] = shard
	}

	require.Equal(t, "foo", ns.Repair(repairer, repairTimeRange).Error())
}

func TestNamespaceShardAt(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ns := newTestNamespace(t)

	s0 := NewMockdatabaseShard(ctrl)
	s0.EXPECT().IsBootstrapped().Return(true)
	ns.shards[0] = s0

	s1 := NewMockdatabaseShard(ctrl)
	s1.EXPECT().IsBootstrapped().Return(false)
	ns.shards[1] = s1

	_, err := ns.readableShardAt(0)
	require.NoError(t, err)
	_, err = ns.readableShardAt(1)
	require.Error(t, err)
	require.True(t, xerrors.IsRetryableError(err))
	require.Equal(t, errShardNotBootstrappedToRead.Error(), err.Error())
	_, err = ns.readableShardAt(2)
	require.Error(t, err)
	require.True(t, xerrors.IsRetryableError(err))
	require.Equal(t, "not responsible for shard 2", err.Error())
}

func TestNamespaceAssignShardSet(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	shards := sharding.NewShards([]uint32{0, 1, 2, 3, 4}, shard.Available)
	prevAssignment := shard.NewShards([]shard.Shard{shards[0], shards[2], shards[3]})
	nextAssignment := shard.NewShards([]shard.Shard{shards[0], shards[4]})
	closing := shard.NewShards([]shard.Shard{shards[2], shards[3]})
	closingErrors := shard.NewShards([]shard.Shard{shards[3]})
	adding := shard.NewShards([]shard.Shard{shards[4]})

	metadata := namespace.NewMetadata(testNamespaceID, namespace.NewOptions())
	hashFn := func(identifier ts.ID) uint32 { return shards[0].ID() }
	shardSet, err := sharding.NewShardSet(prevAssignment.All(), hashFn)
	require.NoError(t, err)
	dopts := testDatabaseOptions()

	reporter := xmetrics.NewTestStatsReporter(xmetrics.NewTestStatsReporterOptions())
	scope, closer := tally.NewRootScope("", nil, reporter, time.Millisecond, tally.DefaultSeparator)
	defer closer.Close()

	dopts = dopts.SetInstrumentOptions(dopts.InstrumentOptions().
		SetMetricsScope(scope))
	ns := newDatabaseNamespace(metadata, shardSet, nil, nil, nil, dopts).(*dbNamespace)

	prevMockShards := make(map[uint32]*MockdatabaseShard)
	for _, testShard := range prevAssignment.All() {
		shard := NewMockdatabaseShard(ctrl)
		shard.EXPECT().ID().Return(testShard.ID()).AnyTimes()
		if closing.Contains(testShard.ID()) {
			if closingErrors.Contains(testShard.ID()) {
				shard.EXPECT().Close().Return(fmt.Errorf("an error"))
			} else {
				shard.EXPECT().Close().Return(nil)
			}
		}
		ns.shards[testShard.ID()] = shard
		prevMockShards[testShard.ID()] = shard
	}

	nextShardSet, err := sharding.NewShardSet(nextAssignment.All(), hashFn)
	require.NoError(t, err)

	ns.AssignShardSet(nextShardSet)

	waitForStats(reporter, func(r xmetrics.TestStatsReporter) bool {
		var (
			counts       = r.Counters()
			adds         = int64(adding.NumShards())
			closeSuccess = int64(closing.NumShards() - closingErrors.NumShards())
			closeErrors  = int64(closingErrors.NumShards())
		)
		return counts["database.dbnamespace.shards.add"] == adds &&
			counts["database.dbnamespace.shards.close"] == closeSuccess &&
			counts["database.dbnamespace.shards.close-errors"] == closeErrors
	})

	for _, shard := range shards {
		if nextAssignment.Contains(shard.ID()) {
			assert.NotNil(t, ns.shards[shard.ID()])
			if prevAssignment.Contains(shard.ID()) {
				assert.Equal(t, prevMockShards[shard.ID()], ns.shards[shard.ID()])
			} else {
				assert.True(t, adding.Contains(shard.ID()))
			}
		} else {
			assert.Nil(t, ns.shards[shard.ID()])
		}
	}
}

func TestNamespaceNeedsFlushAllSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	shards := sharding.NewShards([]uint32{0, 2, 4}, shard.Available)

	metadata := namespace.NewMetadata(testNamespaceID, namespace.NewOptions())
	hashFn := func(identifier ts.ID) uint32 { return shards[0].ID() }
	shardSet, err := sharding.NewShardSet(shards, hashFn)
	require.NoError(t, err)

	dopts := testDatabaseOptions()
	ropts := dopts.RetentionOptions()

	at := time.Unix(0, 0).Add(2 * ropts.RetentionPeriod())
	dopts = dopts.SetClockOptions(dopts.ClockOptions().SetNowFn(func() time.Time {
		return at
	}))

	blockStart := retention.FlushTimeEnd(ropts, at)

	ns := newDatabaseNamespace(metadata, shardSet, nil, nil, nil, dopts).(*dbNamespace)
	for _, s := range shards {
		shard := NewMockdatabaseShard(ctrl)
		shard.EXPECT().ID().Return(s.ID()).AnyTimes()
		shard.EXPECT().FlushState(blockStart).Return(fileOpState{
			Status: fileOpSuccess,
		}).AnyTimes()
		ns.shards[s.ID()] = shard
	}

	assert.False(t, ns.NeedsFlush(blockStart))
}

func TestNamespaceNeedsFlushCountsLeastNumFailures(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	shards := sharding.NewShards([]uint32{0, 2, 4}, shard.Available)

	metadata := namespace.NewMetadata(testNamespaceID, namespace.NewOptions())
	hashFn := func(identifier ts.ID) uint32 { return shards[0].ID() }
	shardSet, err := sharding.NewShardSet(shards, hashFn)
	require.NoError(t, err)

	maxRetries := 2

	dopts := testDatabaseOptions()
	dopts = dopts.SetMaxFlushRetries(2)
	ropts := dopts.RetentionOptions()

	at := time.Unix(0, 0).Add(2 * ropts.RetentionPeriod())
	dopts = dopts.SetClockOptions(dopts.ClockOptions().SetNowFn(func() time.Time {
		return at
	}))

	blockStart := retention.FlushTimeEnd(ropts, at)

	ns := newDatabaseNamespace(metadata, shardSet, nil, nil, nil, dopts).(*dbNamespace)
	for _, s := range shards {
		shard := NewMockdatabaseShard(ctrl)
		shard.EXPECT().ID().Return(s.ID()).AnyTimes()
		switch shard.ID() {
		case shards[0].ID():
			shard.EXPECT().FlushState(blockStart).Return(fileOpState{
				Status: fileOpSuccess,
			}).AnyTimes()
		case shards[1].ID():
			shard.EXPECT().FlushState(blockStart).Return(fileOpState{
				Status:      fileOpFailed,
				NumFailures: maxRetries,
			}).AnyTimes()
		case shards[2].ID():
			shard.EXPECT().FlushState(blockStart).Return(fileOpState{
				Status:      fileOpFailed,
				NumFailures: maxRetries - 1,
			}).AnyTimes()
		}
		ns.shards[s.ID()] = shard
	}

	assert.True(t, ns.NeedsFlush(blockStart))
}

func TestNamespaceNeedsFlushAnyNotStarted(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	shards := sharding.NewShards([]uint32{0, 2, 4}, shard.Available)

	metadata := namespace.NewMetadata(testNamespaceID, namespace.NewOptions())
	hashFn := func(identifier ts.ID) uint32 { return shards[0].ID() }
	shardSet, err := sharding.NewShardSet(shards, hashFn)
	require.NoError(t, err)

	dopts := testDatabaseOptions()
	ropts := dopts.RetentionOptions()

	at := time.Unix(0, 0).Add(2 * ropts.RetentionPeriod())
	dopts = dopts.SetClockOptions(dopts.ClockOptions().SetNowFn(func() time.Time {
		return at
	}))

	blockStart := retention.FlushTimeEnd(ropts, at)

	ns := newDatabaseNamespace(metadata, shardSet, nil, nil, nil, dopts).(*dbNamespace)
	for _, s := range shards {
		shard := NewMockdatabaseShard(ctrl)
		shard.EXPECT().ID().Return(s.ID()).AnyTimes()
		switch shard.ID() {
		case shards[0].ID():
			shard.EXPECT().FlushState(blockStart).Return(fileOpState{
				Status: fileOpSuccess,
			}).AnyTimes()
		case shards[1].ID():
			shard.EXPECT().FlushState(blockStart).Return(fileOpState{
				Status: fileOpNotStarted,
			}).AnyTimes()
		case shards[2].ID():
			shard.EXPECT().FlushState(blockStart).Return(fileOpState{
				Status: fileOpSuccess,
			}).AnyTimes()
		}
		ns.shards[s.ID()] = shard
	}

	assert.True(t, ns.NeedsFlush(blockStart))
}

func TestNamespaceNeedsFlushInProgress(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	shards := sharding.NewShards([]uint32{0, 2, 4}, shard.Available)

	metadata := namespace.NewMetadata(testNamespaceID, namespace.NewOptions())
	hashFn := func(identifier ts.ID) uint32 { return shards[0].ID() }
	shardSet, err := sharding.NewShardSet(shards, hashFn)
	require.NoError(t, err)

	dopts := testDatabaseOptions()
	ropts := dopts.RetentionOptions()

	at := time.Unix(0, 0).Add(2 * ropts.RetentionPeriod())
	dopts = dopts.SetClockOptions(dopts.ClockOptions().SetNowFn(func() time.Time {
		return at
	}))

	blockStart := retention.FlushTimeEnd(ropts, at)

	ns := newDatabaseNamespace(metadata, shardSet, nil, nil, nil, dopts).(*dbNamespace)
	for _, s := range shards {
		shard := NewMockdatabaseShard(ctrl)
		shard.EXPECT().ID().Return(s.ID()).AnyTimes()
		switch shard.ID() {
		case shards[0].ID():
			shard.EXPECT().FlushState(blockStart).Return(fileOpState{
				Status: fileOpSuccess,
			}).AnyTimes()
		case shards[1].ID():
			shard.EXPECT().FlushState(blockStart).Return(fileOpState{
				Status: fileOpInProgress,
			}).AnyTimes()
		}
		ns.shards[s.ID()] = shard
	}

	assert.False(t, ns.NeedsFlush(blockStart))
}

func waitForStats(
	reporter xmetrics.TestStatsReporter,
	check func(xmetrics.TestStatsReporter) bool,
) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for !check(reporter) {
			time.Sleep(100 * time.Millisecond)
		}
		wg.Done()
	}()

	wg.Wait()
}
