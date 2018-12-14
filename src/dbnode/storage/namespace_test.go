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

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/storage/repair"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/metrics"
	"github.com/m3db/m3x/context"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/ident"
	xtest "github.com/m3db/m3x/test"
	xtime "github.com/m3db/m3x/time"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

var (
	testShardIDs = sharding.NewShards([]uint32{0, 1}, shard.Available)
)

type closerFn func()

func newTestNamespace(t *testing.T) (*dbNamespace, closerFn) {
	return newTestNamespaceWithIDOpts(t, defaultTestNs1ID, defaultTestNs1Opts)
}

func newTestNamespaceWithIDOpts(
	t *testing.T,
	nsID ident.ID,
	opts namespace.Options,
) (*dbNamespace, closerFn) {
	metadata, err := namespace.NewMetadata(nsID, opts)
	require.NoError(t, err)
	hashFn := func(identifier ident.ID) uint32 { return testShardIDs[0].ID() }
	shardSet, err := sharding.NewShardSet(testShardIDs, hashFn)
	require.NoError(t, err)
	dopts := testDatabaseOptions().SetRuntimeOptionsManager(runtime.NewOptionsManager())
	ns, err := newDatabaseNamespace(metadata, shardSet, nil, nil, nil, dopts)
	require.NoError(t, err)
	closer := dopts.RuntimeOptionsManager().Close
	return ns.(*dbNamespace), closer
}

func newTestNamespaceWithIndex(t *testing.T, index namespaceIndex) (*dbNamespace, closerFn) {
	ns, closer := newTestNamespace(t)
	if index != nil {
		ns.reverseIndex = index
	}
	return ns, closer
}

func TestNamespaceName(t *testing.T) {
	ns, closer := newTestNamespace(t)
	defer closer()
	require.True(t, defaultTestNs1ID.Equal(ns.ID()))
}

func TestNamespaceTick(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ns, closer := newTestNamespace(t)
	defer closer()
	for i := range testShardIDs {
		shard := NewMockdatabaseShard(ctrl)
		shard.EXPECT().Tick(context.NewNoOpCanncellable(), gomock.Any()).Return(tickResult{}, nil)
		ns.shards[testShardIDs[i].ID()] = shard
	}

	// Only asserting the expected methods are called
	require.NoError(t, ns.Tick(context.NewNoOpCanncellable(), time.Now()))
}

func TestNamespaceTickError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fakeErr := errors.New("fake error")
	ns, closer := newTestNamespace(t)
	defer closer()

	for i := range testShardIDs {
		shard := NewMockdatabaseShard(ctrl)
		if i == 0 {
			shard.EXPECT().Tick(context.NewNoOpCanncellable(), gomock.Any()).Return(tickResult{}, fakeErr)
		} else {
			shard.EXPECT().Tick(context.NewNoOpCanncellable(), gomock.Any()).Return(tickResult{}, nil)
		}
		ns.shards[testShardIDs[i].ID()] = shard
	}

	err := ns.Tick(context.NewNoOpCanncellable(), time.Now())
	require.NotNil(t, err)
	require.Equal(t, fakeErr.Error(), err.Error())
}

func TestNamespaceWriteShardNotOwned(t *testing.T) {
	ctx := context.NewContext()
	defer ctx.Close()

	ns, closer := newTestNamespace(t)
	defer closer()
	for i := range ns.shards {
		ns.shards[i] = nil
	}
	_, err := ns.Write(ctx, ident.StringID("foo"), time.Now(), 0.0, xtime.Second, nil)
	require.Error(t, err)
	require.True(t, xerrors.IsRetryableError(err))
	require.Equal(t, "not responsible for shard 0", err.Error())
}

func TestNamespaceWriteShardOwned(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	id := ident.StringID("foo")
	now := time.Now()
	val := 0.0
	unit := xtime.Second
	ant := []byte(nil)

	ns, closer := newTestNamespace(t)
	defer closer()
	shard := NewMockdatabaseShard(ctrl)
	shard.EXPECT().Write(ctx, id, now, val, unit, ant).Return(ts.Series{}, nil)
	ns.shards[testShardIDs[0].ID()] = shard

	_, err := ns.Write(ctx, id, now, val, unit, ant)
	require.NoError(t, err)
}

func TestNamespaceReadEncodedShardNotOwned(t *testing.T) {
	ctx := context.NewContext()
	defer ctx.Close()

	ns, closer := newTestNamespace(t)
	defer closer()

	for i := range ns.shards {
		ns.shards[i] = nil
	}
	_, err := ns.ReadEncoded(ctx, ident.StringID("foo"), time.Now(), time.Now())
	require.Error(t, err)
}

func TestNamespaceReadEncodedShardOwned(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	id := ident.StringID("foo")
	start := time.Now()
	end := time.Now().Add(time.Second)

	ns, closer := newTestNamespace(t)
	defer closer()

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

	ns, closer := newTestNamespace(t)
	defer closer()

	for i := range ns.shards {
		ns.shards[i] = nil
	}
	_, err := ns.FetchBlocks(ctx, testShardIDs[0].ID(), ident.StringID("foo"), nil)
	require.True(t, xerrors.IsRetryableError(err))
	require.Equal(t, "not responsible for shard 0", err.Error())
}

func TestNamespaceFetchBlocksShardOwned(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	ns, closer := newTestNamespace(t)
	defer closer()
	shard := NewMockdatabaseShard(ctrl)
	shard.EXPECT().FetchBlocks(ctx, ident.NewIDMatcher("foo"), nil).Return(nil, nil)
	ns.shards[testShardIDs[0].ID()] = shard

	shard.EXPECT().IsBootstrapped().Return(true)
	res, err := ns.FetchBlocks(ctx, testShardIDs[0].ID(), ident.StringID("foo"), nil)
	require.NoError(t, err)
	require.Nil(t, res)

	shard.EXPECT().IsBootstrapped().Return(false)
	_, err = ns.FetchBlocks(ctx, testShardIDs[0].ID(), ident.StringID("foo"), nil)
	require.Error(t, err)
	require.True(t, xerrors.IsRetryableError(err))
	require.Equal(t, errShardNotBootstrappedToRead, xerrors.GetInnerRetryableError(err))
}

func TestNamespaceBootstrapBootstrapping(t *testing.T) {
	ns, closer := newTestNamespace(t)
	defer closer()
	ns.bootstrapState = Bootstrapping
	require.Equal(t, errNamespaceIsBootstrapping, ns.Bootstrap(time.Now(), nil))
}

func TestNamespaceBootstrapDontNeedBootstrap(t *testing.T) {
	ns, closer := newTestNamespaceWithIDOpts(t, defaultTestNs1ID,
		namespace.NewOptions().SetBootstrapEnabled(false))
	defer closer()
	require.NoError(t, ns.Bootstrap(time.Now(), nil))
	require.Equal(t, Bootstrapped, ns.bootstrapState)
}

func TestNamespaceBootstrapAllShards(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	ns, closer := newTestNamespace(t)
	defer closer()

	start := time.Now()

	errs := []error{nil, errors.New("foo")}
	bs := bootstrap.NewMockProcess(ctrl)
	bs.EXPECT().
		Run(start, ns.metadata, sharding.IDs(testShardIDs)).
		Return(bootstrap.ProcessResult{
			DataResult:  result.NewDataBootstrapResult(),
			IndexResult: result.NewIndexBootstrapResult(),
		}, nil)
	for i := range errs {
		shard := NewMockdatabaseShard(ctrl)
		shard.EXPECT().IsBootstrapped().Return(false)
		shard.EXPECT().ID().Return(uint32(i)).AnyTimes()
		shard.EXPECT().Bootstrap(gomock.Any()).Return(errs[i])
		ns.shards[testShardIDs[i].ID()] = shard
	}

	require.Equal(t, "foo", ns.Bootstrap(start, bs).Error())
	require.Equal(t, BootstrapNotStarted, ns.bootstrapState)
}

func TestNamespaceBootstrapOnlyNonBootstrappedShards(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

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

	ns, closer := newTestNamespace(t)
	defer closer()

	start := time.Now()

	bs := bootstrap.NewMockProcess(ctrl)
	bs.EXPECT().
		Run(start, ns.metadata, sharding.IDs(needsBootstrap)).
		Return(bootstrap.ProcessResult{
			DataResult:  result.NewDataBootstrapResult(),
			IndexResult: result.NewIndexBootstrapResult(),
		}, nil)

	for _, testShard := range needsBootstrap {
		shard := NewMockdatabaseShard(ctrl)
		shard.EXPECT().IsBootstrapped().Return(false)
		shard.EXPECT().ID().Return(testShard.ID()).AnyTimes()
		shard.EXPECT().Bootstrap(gomock.Any()).Return(nil)
		ns.shards[testShard.ID()] = shard
	}
	for _, testShard := range alreadyBootstrapped {
		shard := NewMockdatabaseShard(ctrl)
		shard.EXPECT().IsBootstrapped().Return(true)
		ns.shards[testShard.ID()] = shard
	}

	require.NoError(t, ns.Bootstrap(start, bs))
	require.Equal(t, Bootstrapped, ns.bootstrapState)
}

func TestNamespaceFlushNotBootstrapped(t *testing.T) {
	ns, closer := newTestNamespace(t)
	defer closer()
	require.Equal(t, errNamespaceNotBootstrapped, ns.Flush(time.Now(), nil, nil))
}

func TestNamespaceFlushDontNeedFlush(t *testing.T) {
	ns, close := newTestNamespaceWithIDOpts(t, defaultTestNs1ID,
		namespace.NewOptions().SetFlushEnabled(false))
	defer close()

	ns.bootstrapState = Bootstrapped
	require.NoError(t, ns.Flush(time.Now(), nil, nil))
}

func TestNamespaceFlushSkipFlushed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	ns, closer := newTestNamespace(t)
	defer closer()

	ns.bootstrapState = Bootstrapped
	blockStart := time.Now().Truncate(ns.Options().RetentionOptions().BlockSize())

	states := []fileOpState{
		{Status: fileOpNotStarted},
		{Status: fileOpSuccess},
	}
	for i, s := range states {
		shard := NewMockdatabaseShard(ctrl)
		shard.EXPECT().ID().Return(testShardIDs[i].ID())
		shard.EXPECT().FlushState(blockStart).Return(s)
		if s.Status != fileOpSuccess {
			shard.EXPECT().Flush(blockStart, nil).Return(nil)
		}
		ns.shards[testShardIDs[i].ID()] = shard
	}

	ShardBootstrapStates := ShardBootstrapStates{}
	for i := range states {
		ShardBootstrapStates[testShardIDs[i].ID()] = Bootstrapped
	}

	require.NoError(t, ns.Flush(blockStart, ShardBootstrapStates, nil))
}

func TestNamespaceFlushSkipShardNotBootstrappedBeforeTick(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	ns, closer := newTestNamespace(t)
	defer closer()

	ns.bootstrapState = Bootstrapped
	blockStart := time.Now().Truncate(ns.Options().RetentionOptions().BlockSize())

	shard := NewMockdatabaseShard(ctrl)
	shard.EXPECT().ID().Return(testShardIDs[0].ID()).AnyTimes()
	ns.shards[testShardIDs[0].ID()] = shard

	shardBootstrapStates := ShardBootstrapStates{}
	shardBootstrapStates[testShardIDs[0].ID()] = Bootstrapping

	require.NoError(t, ns.Flush(blockStart, shardBootstrapStates, nil))
}

type snapshotTestCase struct {
	isSnapshotting                bool
	expectSnapshot                bool
	shardBootstrapStateBeforeTick BootstrapState
	lastSnapshotTime              func(blockStart time.Time, blockSize time.Duration) time.Time
	shardSnapshotErr              error
}

func TestNamespaceSnapshotNotBootstrapped(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	ns, close := newTestNamespace(t)
	defer close()

	ns.bootstrapState = Bootstrapping

	blockSize := ns.Options().RetentionOptions().BlockSize()
	blockStart := time.Now().Truncate(blockSize)
	require.Equal(t, errNamespaceNotBootstrapped, ns.Snapshot(blockStart, blockStart, nil))
}

func TestNamespaceSnapshotShardIsSnapshotting(t *testing.T) {
	shardMethodResults := []snapshotTestCase{
		snapshotTestCase{
			isSnapshotting:                false,
			expectSnapshot:                true,
			shardBootstrapStateBeforeTick: Bootstrapped,
			shardSnapshotErr:              nil,
		},
		snapshotTestCase{
			isSnapshotting:                true,
			expectSnapshot:                false,
			shardBootstrapStateBeforeTick: Bootstrapped,
			shardSnapshotErr:              nil,
		},
	}
	require.NoError(t, testSnapshotWithShardSnapshotErrs(t, shardMethodResults))
}

func TestNamespaceSnapshotAllShardsSuccess(t *testing.T) {
	shardMethodResults := []snapshotTestCase{
		snapshotTestCase{
			isSnapshotting:                false,
			expectSnapshot:                true,
			shardBootstrapStateBeforeTick: Bootstrapped,
			shardSnapshotErr:              nil,
		},
		snapshotTestCase{
			isSnapshotting:                false,
			expectSnapshot:                true,
			shardBootstrapStateBeforeTick: Bootstrapped,
			shardSnapshotErr:              nil,
		},
	}
	require.NoError(t, testSnapshotWithShardSnapshotErrs(t, shardMethodResults))
}

func TestNamespaceSnapshotShardError(t *testing.T) {
	shardMethodResults := []snapshotTestCase{
		snapshotTestCase{
			isSnapshotting:                false,
			expectSnapshot:                true,
			shardBootstrapStateBeforeTick: Bootstrapped,
			shardSnapshotErr:              nil,
		},
		snapshotTestCase{
			isSnapshotting:                false,
			expectSnapshot:                true,
			shardBootstrapStateBeforeTick: Bootstrapped,
			shardSnapshotErr:              errors.New("err"),
		},
	}
	require.Error(t, testSnapshotWithShardSnapshotErrs(t, shardMethodResults))
}

func testSnapshotWithShardSnapshotErrs(t *testing.T, shardMethodResults []snapshotTestCase) error {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	ns, closer := newTestNamespaceWithIDOpts(t, defaultTestNs1ID,
		namespace.NewOptions().SetSnapshotEnabled(true))
	defer closer()
	ns.bootstrapState = Bootstrapped
	now := time.Now()
	ns.nowFn = func() time.Time {
		return now
	}

	var (
		shardBootstrapStates = ShardBootstrapStates{}
		blockSize            = ns.Options().RetentionOptions().BlockSize()
		blockStart           = now.Truncate(blockSize)
	)

	for i, tc := range shardMethodResults {
		shard := NewMockdatabaseShard(ctrl)
		var lastSnapshotTime time.Time
		if tc.lastSnapshotTime == nil {
			lastSnapshotTime = blockStart.Add(-blockSize)
		} else {
			lastSnapshotTime = tc.lastSnapshotTime(now, blockSize)
		}
		shard.EXPECT().SnapshotState().Return(tc.isSnapshotting, lastSnapshotTime)
		shardID := uint32(i)
		shard.EXPECT().ID().Return(uint32(i)).AnyTimes()
		if tc.expectSnapshot {
			shard.EXPECT().Snapshot(blockStart, now, nil).Return(tc.shardSnapshotErr)
		}
		ns.shards[testShardIDs[i].ID()] = shard
		shardBootstrapStates[shardID] = tc.shardBootstrapStateBeforeTick
	}

	return ns.Snapshot(blockStart, now, nil)
}

func TestNamespaceTruncate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ns, closer := newTestNamespace(t)
	defer closer()
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

	ns, closer := newTestNamespaceWithIDOpts(t, defaultTestNs1ID,
		namespace.NewOptions().SetRepairEnabled(true))
	defer closer()
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
			Repair(gomock.Any(), repairTimeRange, repairer).
			Return(res, errs[i])
		ns.shards[testShardIDs[i].ID()] = shard
	}

	require.Equal(t, "foo", ns.Repair(repairer, repairTimeRange).Error())
}

func TestNamespaceShardAt(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ns, closer := newTestNamespace(t)
	defer closer()

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

	metadata, err := namespace.NewMetadata(defaultTestNs1ID, namespace.NewOptions())
	require.NoError(t, err)
	hashFn := func(identifier ident.ID) uint32 { return shards[0].ID() }
	shardSet, err := sharding.NewShardSet(prevAssignment.All(), hashFn)
	require.NoError(t, err)
	dopts := testDatabaseOptions()

	reporter := xmetrics.NewTestStatsReporter(xmetrics.NewTestStatsReporterOptions())
	scope, closer := tally.NewRootScope(tally.ScopeOptions{Reporter: reporter}, time.Millisecond)
	defer closer.Close()

	dopts = dopts.SetInstrumentOptions(dopts.InstrumentOptions().
		SetMetricsScope(scope))
	oNs, err := newDatabaseNamespace(metadata, shardSet, nil, nil, nil, dopts)
	require.NoError(t, err)
	ns := oNs.(*dbNamespace)

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

type needsFlushTestCase struct {
	shardNum   uint32
	needsFlush map[xtime.UnixNano]bool
}

func newNeedsFlushNamespace(t *testing.T, shardNumbers []uint32) *dbNamespace {
	shards := sharding.NewShards(shardNumbers, shard.Available)
	dopts := testDatabaseOptions()

	var (
		hashFn = func(identifier ident.ID) uint32 { return shards[0].ID() }
	)
	metadata, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)
	ropts := metadata.Options().RetentionOptions()
	shardSet, err := sharding.NewShardSet(shards, hashFn)
	require.NoError(t, err)

	at := time.Unix(0, 0).Add(2 * ropts.RetentionPeriod())
	dopts = dopts.SetClockOptions(dopts.ClockOptions().SetNowFn(func() time.Time {
		return at
	}))

	ns, err := newDatabaseNamespace(metadata, shardSet, nil, nil, nil, dopts)
	require.NoError(t, err)
	return ns.(*dbNamespace)
}

func setShardExpects(ns *dbNamespace, ctrl *gomock.Controller, cases []needsFlushTestCase) {
	for _, cs := range cases {
		shard := NewMockdatabaseShard(ctrl)
		shard.EXPECT().ID().Return(cs.shardNum).AnyTimes()
		for t, needFlush := range cs.needsFlush {
			if needFlush {
				shard.EXPECT().FlushState(t.ToTime()).Return(fileOpState{
					Status: fileOpNotStarted,
				}).AnyTimes()
			} else {
				shard.EXPECT().FlushState(t.ToTime()).Return(fileOpState{
					Status: fileOpSuccess,
				}).AnyTimes()
			}
		}
		ns.shards[cs.shardNum] = shard
	}
}

func TestNamespaceNeedsFlushRange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		shards    = []uint32{0, 2, 4}
		ns        = newNeedsFlushNamespace(t, shards)
		ropts     = ns.Options().RetentionOptions()
		blockSize = ropts.BlockSize()
		t1        = retention.FlushTimeEnd(ropts, ns.opts.ClockOptions().NowFn()())
		t0        = t1.Add(-blockSize)
	)

	t0Nano := xtime.ToUnixNano(t0)
	t1Nano := xtime.ToUnixNano(t1)
	inputCases := []needsFlushTestCase{
		{0, map[xtime.UnixNano]bool{t0Nano: false, t1Nano: true}},
		{2, map[xtime.UnixNano]bool{t0Nano: false, t1Nano: true}},
		{4, map[xtime.UnixNano]bool{t0Nano: false, t1Nano: true}},
	}

	setShardExpects(ns, ctrl, inputCases)
	assert.False(t, ns.NeedsFlush(t0, t0))
	assert.True(t, ns.NeedsFlush(t0, t1))
	assert.True(t, ns.NeedsFlush(t1, t1))
	assert.False(t, ns.NeedsFlush(t1, t0))
}

func TestNamespaceNeedsFlushRangeMultipleShardConflict(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		shards    = []uint32{0, 2, 4}
		ns        = newNeedsFlushNamespace(t, shards)
		ropts     = ns.Options().RetentionOptions()
		blockSize = ropts.BlockSize()
		t2        = retention.FlushTimeEnd(ropts, ns.opts.ClockOptions().NowFn()())
		t1        = t2.Add(-blockSize)
		t0        = t1.Add(-blockSize)
	)

	t0Nano := xtime.ToUnixNano(t0)
	t1Nano := xtime.ToUnixNano(t1)
	t2Nano := xtime.ToUnixNano(t2)
	inputCases := []needsFlushTestCase{
		{0, map[xtime.UnixNano]bool{t0Nano: false, t1Nano: true, t2Nano: true}},
		{2, map[xtime.UnixNano]bool{t0Nano: true, t1Nano: false, t2Nano: true}},
		{4, map[xtime.UnixNano]bool{t0Nano: false, t1Nano: true, t2Nano: true}},
	}

	setShardExpects(ns, ctrl, inputCases)
	assert.True(t, ns.NeedsFlush(t0, t0))
	assert.True(t, ns.NeedsFlush(t1, t1))
	assert.True(t, ns.NeedsFlush(t2, t2))
	assert.True(t, ns.NeedsFlush(t0, t1))
	assert.True(t, ns.NeedsFlush(t0, t2))
	assert.True(t, ns.NeedsFlush(t1, t2))
	assert.False(t, ns.NeedsFlush(t2, t1))
	assert.False(t, ns.NeedsFlush(t2, t0))
}
func TestNamespaceNeedsFlushRangeSingleShardConflict(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		shards    = []uint32{0, 2, 4}
		ns        = newNeedsFlushNamespace(t, shards)
		ropts     = ns.Options().RetentionOptions()
		blockSize = ropts.BlockSize()
		t2        = retention.FlushTimeEnd(ropts, ns.opts.ClockOptions().NowFn()())
		t1        = t2.Add(-blockSize)
		t0        = t1.Add(-blockSize)
	)

	t0Nano := xtime.ToUnixNano(t0)
	t1Nano := xtime.ToUnixNano(t1)
	t2Nano := xtime.ToUnixNano(t2)
	inputCases := []needsFlushTestCase{
		{0, map[xtime.UnixNano]bool{t0Nano: false, t1Nano: false, t2Nano: true}},
		{2, map[xtime.UnixNano]bool{t0Nano: true, t1Nano: false, t2Nano: true}},
		{4, map[xtime.UnixNano]bool{t0Nano: false, t1Nano: false, t2Nano: true}},
	}

	setShardExpects(ns, ctrl, inputCases)
	assert.True(t, ns.NeedsFlush(t0, t0))
	assert.False(t, ns.NeedsFlush(t1, t1))
	assert.True(t, ns.NeedsFlush(t2, t2))
	assert.True(t, ns.NeedsFlush(t0, t1))
	assert.True(t, ns.NeedsFlush(t0, t2))
	assert.True(t, ns.NeedsFlush(t1, t2))
	assert.False(t, ns.NeedsFlush(t2, t1))
	assert.False(t, ns.NeedsFlush(t2, t0))
}

func TestNamespaceNeedsFlushAllSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		shards = sharding.NewShards([]uint32{0, 2, 4}, shard.Available)
		dopts  = testDatabaseOptions()
	)

	var (
		hashFn = func(identifier ident.ID) uint32 { return shards[0].ID() }
	)
	metadata, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)
	shardSet, err := sharding.NewShardSet(shards, hashFn)
	require.NoError(t, err)

	ropts := metadata.Options().RetentionOptions()
	at := time.Unix(0, 0).Add(2 * ropts.RetentionPeriod())
	dopts = dopts.SetClockOptions(dopts.ClockOptions().SetNowFn(func() time.Time {
		return at
	}))

	blockStart := retention.FlushTimeEnd(ropts, at)

	oNs, err := newDatabaseNamespace(metadata, shardSet, nil, nil, nil, dopts)
	require.NoError(t, err)
	ns := oNs.(*dbNamespace)

	for _, s := range shards {
		shard := NewMockdatabaseShard(ctrl)
		shard.EXPECT().ID().Return(s.ID()).AnyTimes()
		shard.EXPECT().FlushState(blockStart).Return(fileOpState{
			Status: fileOpSuccess,
		}).AnyTimes()
		ns.shards[s.ID()] = shard
	}

	assert.False(t, ns.NeedsFlush(blockStart, blockStart))
}

func TestNamespaceNeedsFlushAnyFailed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		shards = sharding.NewShards([]uint32{0, 2, 4}, shard.Available)
		dopts  = testDatabaseOptions()
	)
	testNs, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)

	var (
		ropts  = testNs.Options().RetentionOptions()
		hashFn = func(identifier ident.ID) uint32 { return shards[0].ID() }
	)
	shardSet, err := sharding.NewShardSet(shards, hashFn)
	require.NoError(t, err)

	at := time.Unix(0, 0).Add(2 * ropts.RetentionPeriod())
	dopts = dopts.SetClockOptions(dopts.ClockOptions().SetNowFn(func() time.Time {
		return at
	}))

	blockStart := retention.FlushTimeEnd(ropts, at)

	oNs, err := newDatabaseNamespace(testNs, shardSet, nil, nil, nil, dopts)
	require.NoError(t, err)
	ns := oNs.(*dbNamespace)
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
				Status: fileOpSuccess,
			}).AnyTimes()
		case shards[2].ID():
			shard.EXPECT().FlushState(blockStart).Return(fileOpState{
				Status:      fileOpFailed,
				NumFailures: 999,
			}).AnyTimes()
		}
		ns.shards[s.ID()] = shard
	}

	assert.True(t, ns.NeedsFlush(blockStart, blockStart))
}

func TestNamespaceNeedsFlushAnyNotStarted(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		shards = sharding.NewShards([]uint32{0, 2, 4}, shard.Available)
		dopts  = testDatabaseOptions()
	)
	testNs, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)

	var (
		ropts  = testNs.Options().RetentionOptions()
		hashFn = func(identifier ident.ID) uint32 { return shards[0].ID() }
	)
	shardSet, err := sharding.NewShardSet(shards, hashFn)
	require.NoError(t, err)

	at := time.Unix(0, 0).Add(2 * ropts.RetentionPeriod())
	dopts = dopts.SetClockOptions(dopts.ClockOptions().SetNowFn(func() time.Time {
		return at
	}))

	blockStart := retention.FlushTimeEnd(ropts, at)

	oNs, err := newDatabaseNamespace(testNs, shardSet, nil, nil, nil, dopts)
	require.NoError(t, err)
	ns := oNs.(*dbNamespace)
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

	assert.True(t, ns.NeedsFlush(blockStart, blockStart))
}

func TestNamespaceCloseWillCloseShard(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	// mock namespace + 1 shard
	ns, closer := newTestNamespace(t)
	defer closer()

	// specify a mock shard to test being closed
	shard := NewMockdatabaseShard(ctrl)
	shard.EXPECT().Close().Return(nil)
	ns.Lock()
	ns.shards[testShardIDs[0].ID()] = shard
	ns.Unlock()

	// Close the namespace
	require.NoError(t, ns.Close())

	// Check the namespace no long owns any shards
	require.Empty(t, ns.GetOwnedShards())
}

func TestNamespaceCloseDoesNotLeak(t *testing.T) {
	// Need to generate leaktest at top of test as that is when
	// goroutines that are interesting are captured
	leakCheck := leaktest.Check(t)
	defer leakCheck()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	// new namespace
	ns, closer := newTestNamespace(t)
	defer closer()

	// verify has shards it will need to close
	ns.RLock()
	assert.True(t, len(ns.shards) > 0)
	ns.RUnlock()

	// Close the namespace
	require.NoError(t, ns.Close())

	// Check the namespace no long owns any shards
	require.Empty(t, ns.GetOwnedShards())
}

func TestNamespaceIndexInsert(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	idx := NewMocknamespaceIndex(ctrl)
	ns, closer := newTestNamespaceWithIndex(t, idx)
	defer closer()

	ctx := context.NewContext()
	now := time.Now()

	shard := NewMockdatabaseShard(ctrl)
	shard.EXPECT().WriteTagged(ctx, ident.NewIDMatcher("a"), ident.EmptyTagIterator,
		now, 1.0, xtime.Second, nil).Return(ts.Series{}, nil)
	ns.shards[testShardIDs[0].ID()] = shard

	_, err := ns.WriteTagged(ctx, ident.StringID("a"),
		ident.EmptyTagIterator, now, 1.0, xtime.Second, nil)
	require.NoError(t, err)

	shard.EXPECT().Close()
	idx.EXPECT().Close().Return(nil)
	require.NoError(t, ns.Close())
}

func TestNamespaceIndexQuery(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	idx := NewMocknamespaceIndex(ctrl)
	ns, closer := newTestNamespaceWithIndex(t, idx)
	defer closer()

	ctx := context.NewContext()
	query := index.Query{}
	opts := index.QueryOptions{}

	idx.EXPECT().Query(ctx, query, opts)
	_, err := ns.QueryIDs(ctx, query, opts)
	require.NoError(t, err)

	idx.EXPECT().Close().Return(nil)
	require.NoError(t, ns.Close())
}

func TestNamespaceTicksIndex(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	idx := NewMocknamespaceIndex(ctrl)
	ns, closer := newTestNamespaceWithIndex(t, idx)
	defer closer()

	ctx := context.NewCancellable()
	idx.EXPECT().Tick(ctx, gomock.Any()).Return(namespaceIndexTickResult{}, nil)
	err := ns.Tick(ctx, time.Now())
	require.NoError(t, err)
}

func TestNamespaceIndexDisabledQuery(t *testing.T) {
	ns, closer := newTestNamespace(t)
	defer closer()

	ctx := context.NewContext()
	query := index.Query{}
	opts := index.QueryOptions{}

	_, err := ns.QueryIDs(ctx, query, opts)
	require.Error(t, err)

	require.NoError(t, ns.Close())
}

func TestNamespaceBootstrapState(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ns, closer := newTestNamespace(t)
	defer closer()

	shard0 := NewMockdatabaseShard(ctrl)
	shard0.EXPECT().ID().Return(uint32(0))
	shard0.EXPECT().BootstrapState().Return(Bootstrapped)
	ns.shards[0] = shard0

	shard1 := NewMockdatabaseShard(ctrl)
	shard1.EXPECT().ID().Return(uint32(1))
	shard1.EXPECT().BootstrapState().Return(Bootstrapping)
	ns.shards[1] = shard1

	require.Equal(t, ShardBootstrapStates{
		0: Bootstrapped,
		1: Bootstrapping,
	}, ns.BootstrapState())
}

func TestNamespaceIsCapturedBySnapshot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ns, closer := newTestNamespace(t)
	defer closer()

	var (
		testTime   = time.Now()
		blockSize  = ns.nopts.RetentionOptions().BlockSize()
		blockStart = time.Now().Truncate(blockSize)
		testCases  = []struct {
			title                 string
			alignedInclusiveStart time.Time
			alignedInclusiveEnd   time.Time
			shards                []uint32
			snapshotFilesByShard  map[uint32][]fs.FileSetFile
			expectedResult        bool
			expectedErr           error
		}{
			{
				title:                 "Returns true if no shards",
				shards:                nil,
				alignedInclusiveStart: blockStart,
				alignedInclusiveEnd:   blockStart,
				snapshotFilesByShard:  nil,
				expectedResult:        true,
				expectedErr:           nil,
			},
			{
				title:                 "Handles nil files",
				shards:                []uint32{0},
				alignedInclusiveStart: blockStart,
				alignedInclusiveEnd:   blockStart,
				snapshotFilesByShard: map[uint32][]fs.FileSetFile{
					0: nil,
				},
				expectedResult: false,
				expectedErr:    nil,
			},
			{
				title:                 "Handles no latest volume for block",
				shards:                []uint32{0},
				alignedInclusiveStart: blockStart,
				alignedInclusiveEnd:   blockStart,
				snapshotFilesByShard: map[uint32][]fs.FileSetFile{
					0: nil,
				},
				expectedResult: false,
				expectedErr:    nil,
			},
			{
				title:                 "Handles latest snapshot time before",
				shards:                []uint32{0},
				alignedInclusiveStart: blockStart,
				alignedInclusiveEnd:   blockStart,
				snapshotFilesByShard: map[uint32][]fs.FileSetFile{
					0: []fs.FileSetFile{
						fs.FileSetFile{
							ID: fs.FileSetFileIdentifier{
								BlockStart: testTime.Truncate(blockSize),
							},
							// Must contain checkpoint file to be "valid".
							AbsoluteFilepaths:  []string{"snapshots-checkpoint"},
							CachedSnapshotTime: testTime.Add(-1 * time.Second),
						},
					},
				},
				expectedResult: false,
				expectedErr:    nil,
			},
			{
				title:                 "Handles latest snapshot time after",
				shards:                []uint32{0},
				alignedInclusiveStart: blockStart,
				alignedInclusiveEnd:   blockStart,
				snapshotFilesByShard: map[uint32][]fs.FileSetFile{
					0: []fs.FileSetFile{
						fs.FileSetFile{
							ID: fs.FileSetFileIdentifier{
								BlockStart: blockStart,
							},
							// Must contain checkpoint file to be "valid".
							AbsoluteFilepaths:  []string{"snapshots-checkpoint"},
							CachedSnapshotTime: testTime.Add(1 * time.Second),
						},
					},
				},
				expectedResult: true,
				expectedErr:    nil,
			},
			{
				title:                 "Handles multiple blocks - One block with snapshot and one without",
				shards:                []uint32{0},
				alignedInclusiveStart: blockStart,
				// Will iterate over two blocks, but only one will have a snapshot
				// file.
				alignedInclusiveEnd: blockStart.Add(blockSize),
				snapshotFilesByShard: map[uint32][]fs.FileSetFile{
					0: []fs.FileSetFile{
						fs.FileSetFile{
							ID: fs.FileSetFileIdentifier{
								BlockStart: blockStart,
							},
							// Must contain checkpoint file to be "valid".
							AbsoluteFilepaths:  []string{"snapshots-checkpoint"},
							CachedSnapshotTime: testTime.Add(1 * time.Second),
						},
					},
				},
				expectedResult: false,
				expectedErr:    nil,
			},
			{
				title:                 "Handles multiple blocks - Both have snapshot",
				shards:                []uint32{0},
				alignedInclusiveStart: blockStart,
				// Will iterate over two blocks and both will have a snapshot.
				alignedInclusiveEnd: blockStart.Add(blockSize),
				snapshotFilesByShard: map[uint32][]fs.FileSetFile{
					0: []fs.FileSetFile{
						fs.FileSetFile{
							ID: fs.FileSetFileIdentifier{
								BlockStart: blockStart,
							},
							// Must contain checkpoint file to be "valid".
							AbsoluteFilepaths:  []string{"snapshots-checkpoint"},
							CachedSnapshotTime: testTime.Add(1 * time.Second),
						},
						fs.FileSetFile{
							ID: fs.FileSetFileIdentifier{
								BlockStart: blockStart.Add(blockSize),
							},
							// Must contain checkpoint file to be "valid".
							AbsoluteFilepaths:  []string{"snapshots-checkpoint"},
							CachedSnapshotTime: testTime.Add(1 * time.Second),
						},
					},
				},
				expectedResult: true,
				expectedErr:    nil,
			},
		}
	)

	for _, tc := range testCases {
		t.Run(tc.title, func(t *testing.T) {
			ns, closer := newTestNamespace(t)
			defer closer()

			ns.snapshotFilesFn = func(_ string, _ ident.ID, shard uint32) (fs.FileSetFilesSlice, error) {
				files, ok := tc.snapshotFilesByShard[shard]
				if !ok {
					return nil, nil
				}
				return files, nil
			}
			// Make sure to nil out all other shards before hand.
			for i := range ns.shards {
				ns.shards[i] = nil
			}
			for _, shard := range tc.shards {
				mockShard := NewMockdatabaseShard(ctrl)
				mockShard.EXPECT().ID().Return(shard).AnyTimes()
				ns.shards[shard] = mockShard
				result, err := ns.IsCapturedBySnapshot(
					tc.alignedInclusiveStart, tc.alignedInclusiveEnd, testTime)
				require.Equal(t, tc.expectedErr, err)
				require.Equal(t, tc.expectedResult, result)
			}
		})
	}
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
