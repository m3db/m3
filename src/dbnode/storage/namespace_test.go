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
	stdlibctx "context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/dbnode/storage/repair"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/tracepoint"
	xmetrics "github.com/m3db/m3/src/dbnode/x/metrics"
	xidx "github.com/m3db/m3/src/m3ninx/idx"
	idxpersist "github.com/m3db/m3/src/m3ninx/persist"
	"github.com/m3db/m3/src/x/context"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

var (
	sourceNsID = ident.StringID("source")
	targetNsID = ident.StringID("target")

	sourceBlockSize = time.Hour
	targetBlockSize = 2 * time.Hour

	insOpts = instrument.NewOptions()

	testShardIDs = sharding.NewShards([]uint32{0, 1}, shard.Available)
)

type closerFn func()

func newTestNamespace(t *testing.T) (*dbNamespace, closerFn) {
	return newTestNamespaceWithIDOpts(t, defaultTestNs1ID, defaultTestNs1Opts)
}

func newTestNamespaceMetadata(t *testing.T) namespace.Metadata {
	return newTestNamespaceMetadataWithIDOpts(t, defaultTestNs1ID, defaultTestNs1Opts)
}

func newTestNamespaceMetadataWithIDOpts(
	t *testing.T,
	nsID ident.ID,
	opts namespace.Options,
) namespace.Metadata {
	metadata, err := namespace.NewMetadata(nsID, opts)
	require.NoError(t, err)
	return metadata
}

func newTestNamespaceWithIDOpts(
	t *testing.T,
	nsID ident.ID,
	opts namespace.Options,
) (*dbNamespace, closerFn) {
	metadata := newTestNamespaceMetadataWithIDOpts(t, nsID, opts)
	hashFn := func(identifier ident.ID) uint32 { return testShardIDs[0].ID() }
	shardSet, err := sharding.NewShardSet(testShardIDs, hashFn)
	require.NoError(t, err)
	dopts := DefaultTestOptions().SetRuntimeOptionsManager(runtime.NewOptionsManager())
	ns, err := newDatabaseNamespace(metadata,
		namespace.NewRuntimeOptionsManager(metadata.ID().String()),
		shardSet, nil, nil, nil, dopts)
	require.NoError(t, err)
	closer := dopts.RuntimeOptionsManager().Close
	return ns.(*dbNamespace), closer
}

func newTestNamespaceWithOpts(
	t *testing.T,
	opts namespace.Options,
	dopts Options,
) (*dbNamespace, closerFn) {
	nsID := defaultTestNs1ID
	metadata := newTestNamespaceMetadataWithIDOpts(t, nsID, opts)
	hashFn := func(identifier ident.ID) uint32 { return testShardIDs[0].ID() }
	shardSet, err := sharding.NewShardSet(testShardIDs, hashFn)
	require.NoError(t, err)
	ns, err := newDatabaseNamespace(metadata,
		namespace.NewRuntimeOptionsManager(metadata.ID().String()),
		shardSet, nil, nil, nil, dopts)
	require.NoError(t, err)
	closer := dopts.RuntimeOptionsManager().Close
	return ns.(*dbNamespace), closer
}

func newTestNamespaceWithIndex(
	t *testing.T,
	index NamespaceIndex,
) (*dbNamespace, closerFn) {
	ns, closer := newTestNamespace(t)
	if index != nil {
		ns.reverseIndex = index
	}
	return ns, closer
}

func newTestNamespaceWithTruncateType(
	t *testing.T,
	index NamespaceIndex,
	truncateType series.TruncateType,
) (*dbNamespace, closerFn) {
	opts := DefaultTestOptions().
		SetRuntimeOptionsManager(runtime.NewOptionsManager()).
		SetTruncateType(truncateType)

	ns, closer := newTestNamespaceWithOpts(t, defaultTestNs1Opts, opts)
	ns.reverseIndex = index
	return ns, closer
}

func TestNamespaceName(t *testing.T) {
	ns, closer := newTestNamespace(t)
	defer closer()
	require.True(t, defaultTestNs1ID.Equal(ns.ID()))
}

func TestNamespaceTick(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	ns, closer := newTestNamespace(t)
	defer closer()
	for i := range testShardIDs {
		shard := NewMockdatabaseShard(ctrl)
		shard.EXPECT().Tick(context.NewNoOpCanncellable(), gomock.Any(), gomock.Any()).Return(tickResult{}, nil)
		ns.shards[testShardIDs[i].ID()] = shard
	}

	// Only asserting the expected methods are called
	require.NoError(t, ns.Tick(context.NewNoOpCanncellable(), xtime.Now()))
}

func TestNamespaceTickError(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	fakeErr := errors.New("fake error")
	ns, closer := newTestNamespace(t)
	defer closer()

	for i := range testShardIDs {
		shard := NewMockdatabaseShard(ctrl)
		if i == 0 {
			shard.EXPECT().Tick(context.NewNoOpCanncellable(), gomock.Any(), gomock.Any()).Return(tickResult{}, fakeErr)
		} else {
			shard.EXPECT().Tick(context.NewNoOpCanncellable(), gomock.Any(), gomock.Any()).Return(tickResult{}, nil)
		}
		ns.shards[testShardIDs[i].ID()] = shard
	}

	err := ns.Tick(context.NewNoOpCanncellable(), xtime.Now())
	require.NotNil(t, err)
	require.Equal(t, fakeErr.Error(), err.Error())
}

func TestNamespaceWriteShardNotOwned(t *testing.T) {
	ctx := context.NewBackground()
	defer ctx.Close()

	ns, closer := newTestNamespace(t)
	defer closer()
	for i := range ns.shards {
		ns.shards[i] = nil
	}
	now := xtime.Now()
	seriesWrite, err := ns.Write(ctx, ident.StringID("foo"), now, 0.0, xtime.Second, nil)
	require.Error(t, err)
	require.True(t, xerrors.IsRetryableError(err))
	require.Equal(t, "not responsible for shard 0", err.Error())
	require.False(t, seriesWrite.WasWritten)
}

func TestNamespaceReadOnlyRejectWrites(t *testing.T) {
	ctx := context.NewBackground()
	defer ctx.Close()

	ns, closer := newTestNamespace(t)
	defer closer()

	ns.SetReadOnly(true)

	id := ident.StringID("foo")
	now := xtime.Now()

	seriesWrite, err := ns.Write(ctx, id, now, 0, xtime.Second, nil)
	require.EqualError(t, err, errNamespaceReadOnly.Error())
	require.False(t, seriesWrite.WasWritten)

	seriesWrite, err = ns.WriteTagged(ctx, id, convert.EmptyTagMetadataResolver, now, 0, xtime.Second, nil)
	require.EqualError(t, err, errNamespaceReadOnly.Error())
	require.False(t, seriesWrite.WasWritten)
}

func TestNamespaceWriteShardOwned(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewBackground()
	defer ctx.Close()

	id := ident.StringID("foo")
	now := xtime.Now()
	val := 0.0
	unit := xtime.Second
	ant := []byte(nil)

	truncateTypes := []series.TruncateType{series.TypeBlock, series.TypeNone}
	for _, truncateType := range truncateTypes {
		ns, closer := newTestNamespaceWithTruncateType(t, nil, truncateType)
		defer closer()
		shard := NewMockdatabaseShard(ctrl)
		opts := series.WriteOptions{
			TruncateType: truncateType,
		}
		shard.EXPECT().Write(ctx, id, now, val, unit, ant, opts).
			Return(SeriesWrite{WasWritten: true}, nil).Times(1)
		shard.EXPECT().Write(ctx, id, now, val, unit, ant, opts).
			Return(SeriesWrite{WasWritten: false}, nil).Times(1)

		ns.shards[testShardIDs[0].ID()] = shard

		seriesWrite, err := ns.Write(ctx, id, now, val, unit, ant)
		require.NoError(t, err)
		require.True(t, seriesWrite.WasWritten)

		seriesWrite, err = ns.Write(ctx, id, now, val, unit, ant)
		require.NoError(t, err)
		require.False(t, seriesWrite.WasWritten)
	}
}

func TestNamespaceReadEncodedShardNotOwned(t *testing.T) {
	ctx := context.NewBackground()
	defer ctx.Close()

	ns, closer := newTestNamespace(t)
	defer closer()

	for i := range ns.shards {
		ns.shards[i] = nil
	}

	now := xtime.Now()
	_, err := ns.ReadEncoded(ctx, ident.StringID("foo"), now, now)
	require.Error(t, err)
}

func TestNamespaceReadEncodedShardOwned(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewBackground()
	defer ctx.Close()

	id := ident.StringID("foo")
	start := xtime.Now()
	end := start.Add(time.Second)

	ns, closer := newTestNamespace(t)
	defer closer()

	shard := NewMockdatabaseShard(ctrl)
	shard.EXPECT().ReadEncoded(ctx, id, start, end, gomock.Any()).Return(nil, nil)
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
	ctx := context.NewBackground()
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
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewBackground()
	defer ctx.Close()

	ns, closer := newTestNamespace(t)
	defer closer()
	shard := NewMockdatabaseShard(ctrl)
	shard.EXPECT().FetchBlocks(ctx, ident.NewIDMatcher("foo"), nil, gomock.Any()).Return(nil, nil)
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

	ctx := context.NewBackground()
	defer ctx.Close()

	err := ns.Bootstrap(ctx, bootstrap.NamespaceResult{})
	require.Equal(t, errNamespaceIsBootstrapping, err)
}

func TestNamespaceBootstrapDontNeedBootstrap(t *testing.T) {
	ns, closer := newTestNamespaceWithIDOpts(t, defaultTestNs1ID,
		namespace.NewOptions().SetBootstrapEnabled(false))
	defer closer()

	ctx := context.NewBackground()
	defer ctx.Close()

	require.NoError(t, ns.Bootstrap(ctx, bootstrap.NamespaceResult{}))
	require.Equal(t, Bootstrapped, ns.bootstrapState)
}

func TestNamespaceBootstrapAllShards(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	ns, closer := newTestNamespace(t)
	defer closer()

	errs := []error{nil, errors.New("foo")}
	shardIDs := make([]uint32, 0, len(errs))
	for i := range errs {
		shardID := uint32(i)
		shard := NewMockdatabaseShard(ctrl)
		shard.EXPECT().IsBootstrapped().Return(false)
		shard.EXPECT().ID().Return(shardID)
		shard.EXPECT().Bootstrap(gomock.Any(), gomock.Any()).Return(errs[i])
		ns.shards[testShardIDs[i].ID()] = shard
		shardIDs = append(shardIDs, shardID)
	}

	nsResult := bootstrap.NamespaceResult{
		DataResult: result.NewDataBootstrapResult(),
		Shards:     shardIDs,
	}

	ctx := context.NewBackground()
	defer ctx.Close()

	require.Equal(t, "foo", ns.Bootstrap(ctx, nsResult).Error())
	require.Equal(t, BootstrapNotStarted, ns.bootstrapState)
}

func TestNamespaceBootstrapOnlyNonBootstrappedShards(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		needsBootstrap, alreadyBootstrapped []shard.Shard
		needsBootstrapShardIDs              []uint32
	)
	for i, shard := range testShardIDs {
		if i%2 == 0 {
			needsBootstrap = append(needsBootstrap, shard)
			needsBootstrapShardIDs = append(needsBootstrapShardIDs, shard.ID())
		} else {
			alreadyBootstrapped = append(alreadyBootstrapped, shard)
		}
	}

	require.True(t, len(needsBootstrap) > 0)
	require.True(t, len(alreadyBootstrapped) > 0)

	ns, closer := newTestNamespace(t)
	defer closer()

	shardIDs := make([]uint32, 0, len(needsBootstrap))
	for _, testShard := range needsBootstrap {
		shard := NewMockdatabaseShard(ctrl)
		shard.EXPECT().IsBootstrapped().Return(false)
		shard.EXPECT().ID().Return(testShard.ID())
		shard.EXPECT().Bootstrap(gomock.Any(), gomock.Any()).Return(nil)
		ns.shards[testShard.ID()] = shard
		shardIDs = append(shardIDs, testShard.ID())
	}

	for _, testShard := range alreadyBootstrapped {
		shard := NewMockdatabaseShard(ctrl)
		shard.EXPECT().IsBootstrapped().Return(true)
		shard.EXPECT().ID().Return(testShard.ID())
		ns.shards[testShard.ID()] = shard
		shardIDs = append(shardIDs, testShard.ID())
	}

	nsResult := bootstrap.NamespaceResult{
		DataResult: result.NewDataBootstrapResult(),
		Shards:     shardIDs,
	}

	ctx := context.NewBackground()
	defer ctx.Close()

	// do not panic for invariant violation to test some shards are still bootstrapped.
	defer instrument.SetShouldPanicEnvironmentVariable(false)()
	require.Error(t, ns.Bootstrap(ctx, nsResult))
	require.Equal(t, BootstrapNotStarted, ns.bootstrapState)
}

func TestNamespaceBootstrapUnfulfilledShards(t *testing.T) {
	unfulfilledRangeForShards := func(ids ...uint32) result.IndexBootstrapResult {
		var (
			unfulfilledRange = result.NewIndexBootstrapResult()
			unfulfilledTo    = xtime.Now().Truncate(time.Hour)
			unfulfilledFrom  = unfulfilledTo.Add(-time.Hour)
		)
		unfulfilledRange.SetUnfulfilled(result.NewShardTimeRangesFromRange(
			unfulfilledFrom, unfulfilledTo, ids...))
		return unfulfilledRange
	}

	shardIDs := []uint32{0, 1}
	tests := []struct {
		name                string
		withIndex           bool
		unfulfilledShardIDs []uint32
		nsResult            bootstrap.NamespaceResult
	}{
		{
			name:                "no index, unfulfilled data",
			withIndex:           false,
			unfulfilledShardIDs: []uint32{0},
			nsResult: bootstrap.NamespaceResult{
				DataResult: unfulfilledRangeForShards(0),
				Shards:     shardIDs,
			},
		},
		{
			name:                "with index, unfulfilled data",
			withIndex:           true,
			unfulfilledShardIDs: []uint32{0, 1},
			nsResult: bootstrap.NamespaceResult{
				DataResult:  unfulfilledRangeForShards(0, 1),
				IndexResult: unfulfilledRangeForShards(),
				Shards:      shardIDs,
			},
		},
		{
			name:                "with index, unfulfilled index",
			withIndex:           true,
			unfulfilledShardIDs: []uint32{1},
			nsResult: bootstrap.NamespaceResult{
				DataResult:  unfulfilledRangeForShards(),
				IndexResult: unfulfilledRangeForShards(1),
				Shards:      shardIDs,
			},
		},
		{
			name:                "with index, unfulfilled data and index",
			withIndex:           true,
			unfulfilledShardIDs: []uint32{0, 1},
			nsResult: bootstrap.NamespaceResult{
				DataResult:  unfulfilledRangeForShards(0),
				IndexResult: unfulfilledRangeForShards(1),
				Shards:      shardIDs,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testNamespaceBootstrapUnfulfilledShards(t, shardIDs, tt.unfulfilledShardIDs,
				tt.withIndex, tt.nsResult)
		})
	}
}

func testNamespaceBootstrapUnfulfilledShards(
	t *testing.T,
	shardIDs, unfulfilledShardIDs []uint32,
	withIndex bool,
	nsResult bootstrap.NamespaceResult,
) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()
	ctx := context.NewBackground()
	defer ctx.Close()

	var (
		ns     *dbNamespace
		closer func()
	)
	if withIndex {
		idx := NewMockNamespaceIndex(ctrl)
		idx.EXPECT().Bootstrap(gomock.Any()).Return(nil)
		ns, closer = newTestNamespaceWithIndex(t, idx)
	} else {
		ns, closer = newTestNamespace(t)
	}
	defer closer()

	for _, id := range shardIDs {
		shard := NewMockdatabaseShard(ctrl)
		shard.EXPECT().ID().Return(id)
		shard.EXPECT().IsBootstrapped().Return(false)
		if !contains(unfulfilledShardIDs, id) {
			shard.EXPECT().Bootstrap(gomock.Any(), gomock.Any()).Return(nil)
		}
		ns.shards[id] = shard
	}

	require.Error(t, ns.Bootstrap(ctx, nsResult))
}

func TestNamespaceFlushNotBootstrapped(t *testing.T) {
	ns, closer := newTestNamespace(t)
	defer closer()
	err := ns.WarmFlush(xtime.Now(), nil)
	require.Equal(t, errNamespaceNotBootstrapped, err)
	require.Equal(t, errNamespaceNotBootstrapped, ns.ColdFlush(nil))
}

func TestNamespaceFlushDontNeedFlush(t *testing.T) {
	ns, close := newTestNamespaceWithIDOpts(t, defaultTestNs1ID,
		namespace.NewOptions().SetFlushEnabled(false))
	defer close()

	ns.bootstrapState = Bootstrapped
	err := ns.WarmFlush(xtime.Now(), nil)
	require.NoError(t, err)
	require.NoError(t, ns.ColdFlush(nil))
}

func TestNamespaceSkipFlushIfReadOnly(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	// Configure the namespace so that flushing would only be enabled/disabled by read-only property.
	indexOpts := namespace.NewIndexOptions().
		SetEnabled(true)
	nsOpts := namespace.NewOptions().
		SetFlushEnabled(true).
		SetColdWritesEnabled(true).
		SetIndexOptions(indexOpts)

	// Set mocked 'OnColdFlush' so that the test would fail if cold flush would happen.
	opts := DefaultTestOptions().
		SetOnColdFlush(NewMockOnColdFlush(ctrl)).
		SetRuntimeOptionsManager(runtime.NewOptionsManager())

	ns, closer := newTestNamespaceWithOpts(t, nsOpts, opts)
	defer closer()

	ns.bootstrapState = Bootstrapped
	ns.SetReadOnly(true)
	require.NoError(t, ns.WarmFlush(xtime.Now(), nil))
	require.NoError(t, ns.ColdFlush(nil))
	require.NoError(t, ns.FlushIndex(nil))
}

func TestNamespaceFlushSkipFlushed(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewBackground()
	defer ctx.Close()

	ns, closer := newTestNamespace(t)
	defer closer()

	ns.bootstrapState = Bootstrapped
	blockStart := xtime.Now().Truncate(ns.Options().RetentionOptions().BlockSize())

	states := []fileOpState{
		{WarmStatus: warmStatus{DataFlushed: fileOpNotStarted}},
		{WarmStatus: warmStatus{DataFlushed: fileOpSuccess}},
	}
	for i, s := range states {
		shard := NewMockdatabaseShard(ctrl)
		shard.EXPECT().IsBootstrapped().Return(true).AnyTimes()
		shard.EXPECT().FlushState(blockStart).Return(s, nil)
		if s.WarmStatus.DataFlushed != fileOpSuccess {
			shard.EXPECT().WarmFlush(blockStart, gomock.Any(), gomock.Any()).Return(nil)
		}
		ns.shards[testShardIDs[i].ID()] = shard
	}

	err := ns.WarmFlush(blockStart, nil)
	require.NoError(t, err)
}

func TestNamespaceFlushSkipShardNotBootstrapped(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewBackground()
	defer ctx.Close()

	ns, closer := newTestNamespace(t)
	defer closer()

	ns.bootstrapState = Bootstrapped
	blockStart := xtime.Now().Truncate(ns.Options().RetentionOptions().BlockSize())

	shard := NewMockdatabaseShard(ctrl)
	shard.EXPECT().ID().Return(testShardIDs[0].ID()).AnyTimes()
	shard.EXPECT().IsBootstrapped().Return(false)
	ns.shards[testShardIDs[0].ID()] = shard

	err := ns.WarmFlush(blockStart, nil)
	require.NoError(t, err)
	require.NoError(t, ns.ColdFlush(nil))
}

type snapshotTestCase struct {
	isSnapshotting                bool
	expectSnapshot                bool
	shardBootstrapStateBeforeTick BootstrapState
	lastSnapshotTime              func(blockStart time.Time, blockSize time.Duration) time.Time
	shardSnapshotErr              error
	isBootstrapped                bool
}

func TestNamespaceSnapshotNotBootstrapped(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewBackground()
	defer ctx.Close()

	ns, close := newTestNamespace(t)
	defer close()

	ns.bootstrapState = Bootstrapping

	blockSize := ns.Options().RetentionOptions().BlockSize()
	blockStart := xtime.Now().Truncate(blockSize)
	require.Equal(t, errNamespaceNotBootstrapped, ns.Snapshot(blockStart, blockStart, nil))
}

func TestNamespaceSnapshotAllShardsSuccess(t *testing.T) {
	shardMethodResults := []snapshotTestCase{
		{
			isSnapshotting:                false,
			expectSnapshot:                true,
			shardBootstrapStateBeforeTick: Bootstrapped,
			shardSnapshotErr:              nil,
			isBootstrapped:                true,
		},
		{
			isSnapshotting:                false,
			expectSnapshot:                true,
			shardBootstrapStateBeforeTick: Bootstrapped,
			shardSnapshotErr:              nil,
			isBootstrapped:                true,
		},
	}
	require.NoError(t, testSnapshotWithShardSnapshotErrs(t, shardMethodResults))
}

func TestNamespaceSnapshotShardError(t *testing.T) {
	shardMethodResults := []snapshotTestCase{
		{
			isSnapshotting:                false,
			expectSnapshot:                true,
			shardBootstrapStateBeforeTick: Bootstrapped,
			shardSnapshotErr:              nil,
			isBootstrapped:                true,
		},
		{
			isSnapshotting:                false,
			expectSnapshot:                true,
			shardBootstrapStateBeforeTick: Bootstrapped,
			shardSnapshotErr:              errors.New("err"),
			isBootstrapped:                true,
		},
	}
	require.Error(t, testSnapshotWithShardSnapshotErrs(t, shardMethodResults))
}

func TestNamespaceSnapshotShardSkipNotBootstrapped(t *testing.T) {
	shardMethodResults := []snapshotTestCase{
		{
			isSnapshotting:                false,
			expectSnapshot:                true,
			shardBootstrapStateBeforeTick: Bootstrapped,
			shardSnapshotErr:              nil,
			isBootstrapped:                true,
		},
		{
			isSnapshotting:                false,
			expectSnapshot:                true,
			shardBootstrapStateBeforeTick: Bootstrapped,
			// Skip this shard (not bootstrapped) so we do not see this error.
			shardSnapshotErr: errors.New("shard not bootstrapped"),
			isBootstrapped:   false,
		},
	}
	require.NoError(t, testSnapshotWithShardSnapshotErrs(t, shardMethodResults))
}

func testSnapshotWithShardSnapshotErrs(
	t *testing.T,
	shardMethodResults []snapshotTestCase,
) error {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewBackground()
	defer ctx.Close()

	ns, closer := newTestNamespaceWithIDOpts(t, defaultTestNs1ID,
		namespace.NewOptions().SetSnapshotEnabled(true))
	defer closer()
	ns.bootstrapState = Bootstrapped
	now := xtime.Now()
	ns.nowFn = func() time.Time {
		return now.ToTime()
	}

	var (
		shardBootstrapStates = ShardBootstrapStates{}
		blockSize            = ns.Options().RetentionOptions().BlockSize()
		blockStart           = now.Truncate(blockSize)
	)

	for i, tc := range shardMethodResults {
		shard := NewMockdatabaseShard(ctrl)
		shardID := uint32(i)
		shard.EXPECT().ID().Return(uint32(i)).AnyTimes()
		shard.EXPECT().IsBootstrapped().Return(tc.isBootstrapped).AnyTimes()
		if !tc.isBootstrapped {
			continue
		}
		if tc.expectSnapshot {
			shard.EXPECT().
				Snapshot(blockStart, now, gomock.Any(), gomock.Any()).
				Return(ShardSnapshotResult{}, tc.shardSnapshotErr)
		}
		ns.shards[testShardIDs[i].ID()] = shard
		shardBootstrapStates[shardID] = tc.shardBootstrapStateBeforeTick
	}

	return ns.Snapshot(blockStart, now, nil)
}

func TestNamespaceTruncate(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	ns, closer := newTestNamespace(t)
	defer closer()
	for _, shard := range testShardIDs {
		mockShard := NewMockdatabaseShard(ctrl)
		mockShard.EXPECT().NumSeries().Return(int64(shard.ID()))
		mockShard.EXPECT().ID().Return(shard.ID())
		ns.shards[shard.ID()] = mockShard
	}

	res, err := ns.Truncate()
	require.NoError(t, err)
	require.Equal(t, int64(1), res)
	require.NotNil(t, ns.shards[testShardIDs[0].ID()])
	require.True(t, ns.shards[testShardIDs[0].ID()].IsBootstrapped())
}

func TestNamespaceRepair(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	ns, closer := newTestNamespaceWithIDOpts(t, defaultTestNs1ID,
		namespace.NewOptions().SetRepairEnabled(true))
	defer closer()
	now := xtime.Now()
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
			Repair(gomock.Any(), gomock.Any(), gomock.Any(), repairTimeRange, repairer).
			Return(res, errs[i])
		ns.shards[testShardIDs[i].ID()] = shard
	}

	require.Equal(t, "foo", ns.Repair(repairer, repairTimeRange, NamespaceRepairOptions{}).Error())
}

func TestNamespaceShardAt(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	ns, closer := newTestNamespace(t)
	defer closer()

	s0 := NewMockdatabaseShard(ctrl)
	s0.EXPECT().IsBootstrapped().Return(true)
	ns.shards[0] = s0

	s1 := NewMockdatabaseShard(ctrl)
	s1.EXPECT().IsBootstrapped().Return(false)
	ns.shards[1] = s1

	_, _, err := ns.ReadableShardAt(0)
	require.NoError(t, err)
	_, _, err = ns.ReadableShardAt(1)
	require.Error(t, err)
	require.True(t, xerrors.IsRetryableError(err))
	require.Equal(t, errShardNotBootstrappedToRead.Error(), err.Error())
	_, _, err = ns.ReadableShardAt(2)
	require.Error(t, err)
	require.True(t, xerrors.IsRetryableError(err))
	require.Equal(t, "not responsible for shard 2", err.Error())
}

func TestNamespaceAssignShardSet(t *testing.T) {
	ctrl := xtest.NewController(t)
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
	dopts := DefaultTestOptions()

	reporter := xmetrics.NewTestStatsReporter(xmetrics.NewTestStatsReporterOptions())
	scope, closer := tally.NewRootScope(tally.ScopeOptions{Reporter: reporter}, time.Millisecond)
	defer closer.Close()

	dopts = dopts.SetInstrumentOptions(dopts.InstrumentOptions().
		SetMetricsScope(scope))
	oNs, err := newDatabaseNamespace(metadata,
		namespace.NewRuntimeOptionsManager(metadata.ID().String()),
		shardSet, nil, nil, nil, dopts)
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
	dopts := DefaultTestOptions()

	hashFn := func(identifier ident.ID) uint32 { return shards[0].ID() }
	metadata, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)
	ropts := metadata.Options().RetentionOptions()
	shardSet, err := sharding.NewShardSet(shards, hashFn)
	require.NoError(t, err)

	at := time.Unix(0, 0).Add(2 * ropts.RetentionPeriod())
	dopts = dopts.SetClockOptions(dopts.ClockOptions().SetNowFn(func() time.Time {
		return at
	}))

	ns, err := newDatabaseNamespace(metadata,
		namespace.NewRuntimeOptionsManager(metadata.ID().String()),
		shardSet, nil, nil, nil, dopts)
	require.NoError(t, err)
	return ns.(*dbNamespace)
}

func setShardExpects(ns *dbNamespace, ctrl *gomock.Controller, cases []needsFlushTestCase) {
	for _, cs := range cases {
		shard := NewMockdatabaseShard(ctrl)
		shard.EXPECT().ID().Return(cs.shardNum).AnyTimes()
		for t, needFlush := range cs.needsFlush {
			if needFlush {
				shard.EXPECT().FlushState(t).Return(fileOpState{
					WarmStatus: warmStatus{
						DataFlushed: fileOpNotStarted,
					},
				}, nil).AnyTimes()
			} else {
				shard.EXPECT().FlushState(t).Return(fileOpState{
					WarmStatus: warmStatus{
						DataFlushed: fileOpSuccess,
					},
				}, nil).AnyTimes()
			}
		}
		ns.shards[cs.shardNum] = shard
	}
}

func TestNamespaceNeedsFlushRange(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		shards    = []uint32{0, 2, 4}
		ns        = newNeedsFlushNamespace(t, shards)
		ropts     = ns.Options().RetentionOptions()
		blockSize = ropts.BlockSize()
		t1        = retention.FlushTimeEnd(ropts, xtime.ToUnixNano(ns.opts.ClockOptions().NowFn()()))
		t0        = t1.Add(-blockSize)
	)

	inputCases := []needsFlushTestCase{
		{0, map[xtime.UnixNano]bool{t0: false, t1: true}},
		{2, map[xtime.UnixNano]bool{t0: false, t1: true}},
		{4, map[xtime.UnixNano]bool{t0: false, t1: true}},
	}

	setShardExpects(ns, ctrl, inputCases)

	assertNeedsFlush(t, ns, t0, t0, false)
	assertNeedsFlush(t, ns, t0, t1, true)
	assertNeedsFlush(t, ns, t1, t1, true)
	assertNeedsFlush(t, ns, t1, t0, false)
}

func TestNamespaceNeedsFlushRangeMultipleShardConflict(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		shards    = []uint32{0, 2, 4}
		ns        = newNeedsFlushNamespace(t, shards)
		ropts     = ns.Options().RetentionOptions()
		blockSize = ropts.BlockSize()
		t2        = retention.FlushTimeEnd(ropts, xtime.ToUnixNano(ns.opts.ClockOptions().NowFn()()))
		t1        = t2.Add(-blockSize)
		t0        = t1.Add(-blockSize)
	)

	inputCases := []needsFlushTestCase{
		{0, map[xtime.UnixNano]bool{t0: false, t1: true, t2: true}},
		{2, map[xtime.UnixNano]bool{t0: true, t1: false, t2: true}},
		{4, map[xtime.UnixNano]bool{t0: false, t1: true, t2: true}},
	}

	setShardExpects(ns, ctrl, inputCases)
	assertNeedsFlush(t, ns, t0, t0, true)
	assertNeedsFlush(t, ns, t1, t1, true)
	assertNeedsFlush(t, ns, t2, t2, true)
	assertNeedsFlush(t, ns, t0, t1, true)
	assertNeedsFlush(t, ns, t0, t2, true)
	assertNeedsFlush(t, ns, t1, t2, true)
	assertNeedsFlush(t, ns, t2, t1, false)
	assertNeedsFlush(t, ns, t2, t0, false)
}

func TestNamespaceNeedsFlushRangeSingleShardConflict(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		shards    = []uint32{0, 2, 4}
		ns        = newNeedsFlushNamespace(t, shards)
		ropts     = ns.Options().RetentionOptions()
		blockSize = ropts.BlockSize()
		t2        = retention.FlushTimeEnd(ropts, xtime.ToUnixNano(ns.opts.ClockOptions().NowFn()()))
		t1        = t2.Add(-blockSize)
		t0        = t1.Add(-blockSize)
	)
	inputCases := []needsFlushTestCase{
		{0, map[xtime.UnixNano]bool{t0: false, t1: false, t2: true}},
		{2, map[xtime.UnixNano]bool{t0: true, t1: false, t2: true}},
		{4, map[xtime.UnixNano]bool{t0: false, t1: false, t2: true}},
	}

	setShardExpects(ns, ctrl, inputCases)
	assertNeedsFlush(t, ns, t0, t0, true)
	assertNeedsFlush(t, ns, t1, t1, false)
	assertNeedsFlush(t, ns, t2, t2, true)
	assertNeedsFlush(t, ns, t0, t1, true)
	assertNeedsFlush(t, ns, t0, t2, true)
	assertNeedsFlush(t, ns, t1, t2, true)
	assertNeedsFlush(t, ns, t2, t1, false)
	assertNeedsFlush(t, ns, t2, t0, false)
}

func TestNamespaceNeedsFlushAllSuccess(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		shards = sharding.NewShards([]uint32{0, 2, 4}, shard.Available)
		dopts  = DefaultTestOptions()
		hashFn = func(identifier ident.ID) uint32 { return shards[0].ID() }
	)

	metadata, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)
	shardSet, err := sharding.NewShardSet(shards, hashFn)
	require.NoError(t, err)

	ropts := metadata.Options().RetentionOptions()
	at := xtime.UnixNano(2 * ropts.RetentionPeriod())
	dopts = dopts.SetClockOptions(dopts.ClockOptions().SetNowFn(func() time.Time {
		return at.ToTime()
	}))

	blockStart := retention.FlushTimeEnd(ropts, at)

	oNs, err := newDatabaseNamespace(metadata,
		namespace.NewRuntimeOptionsManager(metadata.ID().String()),
		shardSet, nil, nil, nil, dopts)
	require.NoError(t, err)
	ns := oNs.(*dbNamespace)

	for _, s := range shards {
		shard := NewMockdatabaseShard(ctrl)
		shard.EXPECT().ID().Return(s.ID()).AnyTimes()
		shard.EXPECT().FlushState(blockStart).Return(fileOpState{
			WarmStatus: warmStatus{
				DataFlushed: fileOpSuccess,
			},
		}, nil).AnyTimes()
		ns.shards[s.ID()] = shard
	}

	assertNeedsFlush(t, ns, blockStart, blockStart, false)
}

func TestNamespaceNeedsFlushAnyFailed(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		shards = sharding.NewShards([]uint32{0, 2, 4}, shard.Available)
		dopts  = DefaultTestOptions()
	)
	testNs, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)

	var (
		ropts  = testNs.Options().RetentionOptions()
		hashFn = func(identifier ident.ID) uint32 { return shards[0].ID() }
	)
	shardSet, err := sharding.NewShardSet(shards, hashFn)
	require.NoError(t, err)

	at := xtime.UnixNano(2 * ropts.RetentionPeriod())
	dopts = dopts.SetClockOptions(dopts.ClockOptions().SetNowFn(func() time.Time {
		return at.ToTime()
	}))

	blockStart := retention.FlushTimeEnd(ropts, at)

	oNs, err := newDatabaseNamespace(testNs,
		namespace.NewRuntimeOptionsManager(testNs.ID().String()),
		shardSet, nil, nil, nil, dopts)
	require.NoError(t, err)
	ns := oNs.(*dbNamespace)
	for _, s := range shards {
		shard := NewMockdatabaseShard(ctrl)
		shard.EXPECT().ID().Return(s.ID()).AnyTimes()
		switch shard.ID() {
		case shards[0].ID():
			shard.EXPECT().FlushState(blockStart).Return(fileOpState{
				WarmStatus: warmStatus{
					DataFlushed: fileOpSuccess,
				},
			}, nil).AnyTimes()
		case shards[1].ID():
			shard.EXPECT().FlushState(blockStart).Return(fileOpState{
				WarmStatus: warmStatus{
					DataFlushed: fileOpSuccess,
				},
			}, nil).AnyTimes()
		case shards[2].ID():
			shard.EXPECT().FlushState(blockStart).Return(fileOpState{
				WarmStatus: warmStatus{
					DataFlushed: fileOpFailed,
				},
				NumFailures: 999,
			}, nil).AnyTimes()
		}
		ns.shards[s.ID()] = shard
	}

	assertNeedsFlush(t, ns, blockStart, blockStart, true)
}

func TestNamespaceNeedsFlushAnyNotStarted(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		shards = sharding.NewShards([]uint32{0, 2, 4}, shard.Available)
		dopts  = DefaultTestOptions()
	)
	testNs, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)

	var (
		ropts  = testNs.Options().RetentionOptions()
		hashFn = func(identifier ident.ID) uint32 { return shards[0].ID() }
	)
	shardSet, err := sharding.NewShardSet(shards, hashFn)
	require.NoError(t, err)

	at := xtime.UnixNano(2 * ropts.RetentionPeriod())
	dopts = dopts.SetClockOptions(dopts.ClockOptions().SetNowFn(func() time.Time {
		return at.ToTime()
	}))

	blockStart := retention.FlushTimeEnd(ropts, at)

	oNs, err := newDatabaseNamespace(testNs,
		namespace.NewRuntimeOptionsManager(testNs.ID().String()),
		shardSet, nil, nil, nil, dopts)
	require.NoError(t, err)
	ns := oNs.(*dbNamespace)
	for _, s := range shards {
		shard := NewMockdatabaseShard(ctrl)
		shard.EXPECT().ID().Return(s.ID()).AnyTimes()
		switch shard.ID() {
		case shards[0].ID():
			shard.EXPECT().FlushState(blockStart).Return(fileOpState{
				WarmStatus: warmStatus{
					DataFlushed: fileOpSuccess,
				},
			}, nil).AnyTimes()
		case shards[1].ID():
			shard.EXPECT().FlushState(blockStart).Return(fileOpState{
				WarmStatus: warmStatus{
					DataFlushed: fileOpNotStarted,
				},
			}, nil).AnyTimes()
		case shards[2].ID():
			shard.EXPECT().FlushState(blockStart).Return(fileOpState{
				WarmStatus: warmStatus{
					DataFlushed: fileOpSuccess,
				},
			}, nil).AnyTimes()
		}
		ns.shards[s.ID()] = shard
	}

	assertNeedsFlush(t, ns, blockStart, blockStart, true)
}

func TestNamespaceCloseWillCloseShard(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewBackground()
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
	require.Empty(t, ns.OwnedShards())
}

func TestNamespaceCloseDoesNotLeak(t *testing.T) {
	// Need to generate leaktest at top of test as that is when
	// goroutines that are interesting are captured
	leakCheck := leaktest.Check(t)
	defer leakCheck()

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewBackground()
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
	require.Empty(t, ns.OwnedShards())
}

func TestNamespaceIndexInsert(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	truncateTypes := []series.TruncateType{series.TypeBlock, series.TypeNone}
	for _, truncateType := range truncateTypes {
		idx := NewMockNamespaceIndex(ctrl)

		ns, closer := newTestNamespaceWithTruncateType(t, idx, truncateType)
		ns.reverseIndex = idx
		defer closer()

		ctx := context.NewBackground()
		now := xtime.Now()

		shard := NewMockdatabaseShard(ctrl)

		opts := series.WriteOptions{
			TruncateType: truncateType,
		}
		shard.EXPECT().
			WriteTagged(ctx, ident.NewIDMatcher("a"), convert.EmptyTagMetadataResolver,
				now, 1.0, xtime.Second, nil, opts).
			Return(SeriesWrite{WasWritten: true}, nil)
		shard.EXPECT().
			WriteTagged(ctx, ident.NewIDMatcher("a"), convert.EmptyTagMetadataResolver,
				now, 1.0, xtime.Second, nil, opts).
			Return(SeriesWrite{WasWritten: false}, nil)

		ns.shards[testShardIDs[0].ID()] = shard

		seriesWrite, err := ns.WriteTagged(ctx, ident.StringID("a"),
			convert.EmptyTagMetadataResolver, now, 1.0, xtime.Second, nil)
		require.NoError(t, err)
		require.True(t, seriesWrite.WasWritten)

		seriesWrite, err = ns.WriteTagged(ctx, ident.StringID("a"),
			convert.EmptyTagMetadataResolver, now, 1.0, xtime.Second, nil)
		require.NoError(t, err)
		require.False(t, seriesWrite.WasWritten)

		shard.EXPECT().Close()
		idx.EXPECT().Close().Return(nil)
		require.NoError(t, ns.Close())
	}
}

func TestNamespaceIndexQuery(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	idx := NewMockNamespaceIndex(ctrl)
	idx.EXPECT().Bootstrapped().Return(true)

	ns, closer := newTestNamespaceWithIndex(t, idx)
	defer closer()

	ctx := context.NewBackground()
	mtr := mocktracer.New()
	sp := mtr.StartSpan("root")
	ctx.SetGoContext(opentracing.ContextWithSpan(stdlibctx.Background(), sp))

	query := index.Query{
		Query: xidx.NewTermQuery([]byte("foo"), []byte("bar")),
	}
	opts := index.QueryOptions{}

	idx.EXPECT().Query(gomock.Any(), query, opts)
	_, err := ns.QueryIDs(ctx, query, opts)
	require.NoError(t, err)

	idx.EXPECT().Close().Return(nil)
	require.NoError(t, ns.Close())

	sp.Finish()
	spans := mtr.FinishedSpans()
	require.Len(t, spans, 2)
	assert.Equal(t, tracepoint.NSQueryIDs, spans[0].OperationName)
	assert.Equal(t, "root", spans[1].OperationName)
}

func TestNamespaceAggregateQuery(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	idx := NewMockNamespaceIndex(ctrl)
	idx.EXPECT().Bootstrapped().Return(true)

	ns, closer := newTestNamespaceWithIndex(t, idx)
	defer closer()

	ctx := context.NewBackground()
	query := index.Query{
		Query: xidx.NewTermQuery([]byte("foo"), []byte("bar")),
	}
	aggOpts := index.AggregationOptions{}

	idx.EXPECT().AggregateQuery(ctx, query, aggOpts)
	_, err := ns.AggregateQuery(ctx, query, aggOpts)
	require.NoError(t, err)

	idx.EXPECT().Close().Return(nil)
	require.NoError(t, ns.Close())
}

func TestNamespaceTicksIndex(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	idx := NewMockNamespaceIndex(ctrl)
	ns, closer := newTestNamespaceWithIndex(t, idx)
	defer closer()

	ns.RLock()
	nsCtx := ns.nsContextWithRLock()
	ns.RUnlock()

	ctx := context.NewBackground()
	defer ctx.Close()

	for _, s := range ns.shards {
		if s != nil {
			s.Bootstrap(ctx, nsCtx)
		}
	}

	cancel := context.NewCancellable()
	idx.EXPECT().Tick(cancel, gomock.Any()).Return(namespaceIndexTickResult{}, nil)
	err := ns.Tick(cancel, xtime.Now())
	require.NoError(t, err)
}

func TestNamespaceIndexDisabledQuery(t *testing.T) {
	ns, closer := newTestNamespace(t)
	defer closer()

	ctx := context.NewBackground()
	query := index.Query{
		Query: xidx.NewTermQuery([]byte("foo"), []byte("bar")),
	}
	opts := index.QueryOptions{}

	_, err := ns.QueryIDs(ctx, query, opts)
	require.Error(t, err)

	require.NoError(t, ns.Close())
}

func TestNamespaceBootstrapState(t *testing.T) {
	ns, closer := newTestNamespace(t)
	defer closer()

	require.Equal(t, BootstrapNotStarted, ns.BootstrapState())

	ns.bootstrapState = Bootstrapped
	require.Equal(t, Bootstrapped, ns.BootstrapState())
}

func TestNamespaceShardBootstrapState(t *testing.T) {
	ctrl := xtest.NewController(t)
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
	}, ns.ShardBootstrapState())
}

func TestNamespaceFlushState(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	ns, closer := newTestNamespace(t)
	defer closer()

	var (
		blockStart         = xtime.Now().Truncate(2 * time.Hour)
		expectedFlushState = fileOpState{
			ColdVersionRetrievable: 2,
		}
		shard0 = NewMockdatabaseShard(ctrl)
	)
	shard0.EXPECT().FlushState(blockStart).Return(expectedFlushState, nil)
	ns.shards[0] = shard0

	flushState, err := ns.FlushState(0, blockStart)
	require.NoError(t, err)
	require.Equal(t, expectedFlushState, flushState)
}

func TestNamespaceAggregateTilesFailUntilBootstrapped(t *testing.T) {
	ctx := context.NewBackground()
	defer ctx.Close()

	var (
		start = xtime.Now().Truncate(time.Hour)
		opts  = AggregateTilesOptions{Start: start, End: start.Add(time.Hour), InsOptions: insOpts}
	)

	sourceNs, sourceCloser := newTestNamespaceWithIDOpts(t, sourceNsID, namespace.NewOptions())
	defer sourceCloser()

	targetNs, targetCloser := newTestNamespaceWithIDOpts(t, targetNsID, namespace.NewOptions())
	defer targetCloser()

	_, err := targetNs.AggregateTiles(ctx, sourceNs, opts)
	require.Equal(t, errNamespaceNotBootstrapped, err)

	sourceNs.bootstrapState = Bootstrapped

	_, err = targetNs.AggregateTiles(ctx, sourceNs, opts)
	require.Equal(t, errNamespaceNotBootstrapped, err)
}

func TestNamespaceAggregateTiles(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewBackground()
	defer ctx.Close()

	var (
		start    = xtime.Now().Truncate(targetBlockSize)
		shard0ID = uint32(10)
		shard1ID = uint32(20)

		createdWarmIndexForBlockStart xtime.UnixNano
	)

	opts, err := NewAggregateTilesOptions(
		start, start.Add(targetBlockSize), time.Second, targetNsID, AggregateTilesRegular,
		false, false, nil, insOpts)
	require.NoError(t, err)

	sourceNs, sourceCloser := newTestNamespaceWithIDOpts(t, sourceNsID, namespace.NewOptions())
	defer sourceCloser()
	sourceNs.bootstrapState = Bootstrapped
	sourceRetentionOpts := sourceNs.nopts.RetentionOptions().SetBlockSize(sourceBlockSize)
	sourceNs.nopts = sourceNs.nopts.SetRetentionOptions(sourceRetentionOpts)

	targetNs, targetCloser := newTestNamespaceWithIDOpts(t, targetNsID, namespace.NewOptions())
	defer targetCloser()
	targetNs.bootstrapState = Bootstrapped
	targetNs.createEmptyWarmIndexIfNotExistsFn = func(blockStart xtime.UnixNano) error {
		createdWarmIndexForBlockStart = blockStart
		return nil
	}
	targetRetentionOpts := targetNs.nopts.RetentionOptions().SetBlockSize(targetBlockSize)
	targetNs.nopts = targetNs.nopts.SetColdWritesEnabled(true).SetRetentionOptions(targetRetentionOpts)

	// Pass in mock cold flusher and expect the cold flush ns process to finish.
	mockOnColdFlushNs := NewMockOnColdFlushNamespace(ctrl)
	mockOnColdFlushNs.EXPECT().Done().Return(nil)
	mockOnColdFlush := NewMockOnColdFlush(ctrl)
	cfOpts := NewColdFlushNsOpts(false)
	mockOnColdFlush.EXPECT().ColdFlushNamespace(gomock.Any(), cfOpts).Return(mockOnColdFlushNs, nil)
	targetNs.opts = targetNs.opts.SetOnColdFlush(mockOnColdFlush)

	targetShard0 := NewMockdatabaseShard(ctrl)
	targetShard1 := NewMockdatabaseShard(ctrl)
	targetNs.shards[0] = targetShard0
	targetNs.shards[1] = targetShard1

	targetShard0.EXPECT().IsBootstrapped().Return(true)
	targetShard1.EXPECT().IsBootstrapped().Return(true)

	targetShard0.EXPECT().ID().Return(shard0ID)
	targetShard1.EXPECT().ID().Return(shard1ID)

	targetShard0.EXPECT().
		AggregateTiles(ctx, sourceNs, targetNs, shard0ID, mockOnColdFlushNs, opts).
		Return(int64(3), nil)

	targetShard1.EXPECT().
		AggregateTiles(ctx, sourceNs, targetNs, shard1ID, mockOnColdFlushNs, opts).
		Return(int64(2), nil)

	processedTileCount, err := targetNs.AggregateTiles(ctx, sourceNs, opts)

	require.NoError(t, err)
	assert.Equal(t, int64(3+2), processedTileCount)
	assert.Equal(t, start, createdWarmIndexForBlockStart)
}

func TestNamespaceAggregateTilesSkipBootstrappingShards(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewBackground()
	defer ctx.Close()

	start := xtime.Now().Truncate(targetBlockSize)

	opts, err := NewAggregateTilesOptions(
		start, start.Add(targetBlockSize), time.Second, targetNsID, AggregateTilesRegular,
		false, false, nil, insOpts)
	require.NoError(t, err)

	sourceNs, sourceCloser := newTestNamespaceWithIDOpts(t, sourceNsID, namespace.NewOptions())
	defer sourceCloser()
	sourceNs.bootstrapState = Bootstrapped
	sourceRetentionOpts := sourceNs.nopts.RetentionOptions().SetBlockSize(sourceBlockSize)
	sourceNs.nopts = sourceNs.nopts.SetRetentionOptions(sourceRetentionOpts)

	targetNs, targetCloser := newTestNamespaceWithIDOpts(t, targetNsID, namespace.NewOptions())
	defer targetCloser()
	targetNs.bootstrapState = Bootstrapped
	targetNs.createEmptyWarmIndexIfNotExistsFn = func(blockStart xtime.UnixNano) error {
		return nil
	}
	targetRetentionOpts := targetNs.nopts.RetentionOptions().SetBlockSize(targetBlockSize)
	targetNs.nopts = targetNs.nopts.SetColdWritesEnabled(true).SetRetentionOptions(targetRetentionOpts)

	targetShard0 := NewMockdatabaseShard(ctrl)
	targetShard1 := NewMockdatabaseShard(ctrl)
	targetNs.shards[0] = targetShard0
	targetNs.shards[1] = targetShard1

	targetShard0.EXPECT().IsBootstrapped().Return(false)
	targetShard1.EXPECT().IsBootstrapped().Return(false)

	targetShard0.EXPECT().ID().Return(uint32(10))
	targetShard1.EXPECT().ID().Return(uint32(11))

	processedTileCount, err := targetNs.AggregateTiles(ctx, sourceNs, opts)

	require.NoError(t, err)
	assert.Zero(t, processedTileCount)
}

func TestNamespaceAggregateTilesSkipIfNoShardsOwned(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewBackground()
	defer ctx.Close()

	start := xtime.Now().Truncate(targetBlockSize)

	opts, err := NewAggregateTilesOptions(
		start, start.Add(targetBlockSize), time.Second, targetNsID, AggregateTilesRegular,
		false, false, nil, insOpts)
	require.NoError(t, err)

	sourceNs, sourceCloser := newTestNamespaceWithIDOpts(t, sourceNsID, namespace.NewOptions())
	defer sourceCloser()
	sourceNs.bootstrapState = Bootstrapped
	sourceRetentionOpts := sourceNs.nopts.RetentionOptions().SetBlockSize(sourceBlockSize)
	sourceNs.nopts = sourceNs.nopts.SetRetentionOptions(sourceRetentionOpts)

	targetNs, targetCloser := newTestNamespaceWithIDOpts(t, targetNsID, namespace.NewOptions())
	defer targetCloser()
	targetNs.bootstrapState = Bootstrapped
	targetNs.createEmptyWarmIndexIfNotExistsFn = func(blockStart xtime.UnixNano) error {
		return nil
	}
	targetRetentionOpts := targetNs.nopts.RetentionOptions().SetBlockSize(targetBlockSize)
	targetNs.nopts = targetNs.nopts.SetColdWritesEnabled(true).SetRetentionOptions(targetRetentionOpts)

	noShards := sharding.NewEmptyShardSet(sharding.DefaultHashFn(1))
	targetNs.AssignShardSet(noShards)

	processedTileCount, err := targetNs.AggregateTiles(ctx, sourceNs, opts)

	require.NoError(t, err)
	assert.Zero(t, processedTileCount)
}

func TestCreateEmptyWarmIndexIfNotExists(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		nsID       = ident.StringID("warm")
		nsOpts     = namespace.NewOptions()
		blockStart = xtime.Now().Truncate(nsOpts.IndexOptions().BlockSize())
	)

	ns, nsCloser := newTestNamespaceWithIDOpts(t, nsID, nsOpts)
	defer nsCloser()

	shard0 := NewMockdatabaseShard(ctrl)
	shard1 := NewMockdatabaseShard(ctrl)
	ns.shards[0] = shard0
	ns.shards[1] = shard1

	shard0.EXPECT().IsBootstrapped().Return(false).Times(2)

	shard1.EXPECT().IsBootstrapped().Return(true).Times(2)
	shard1.EXPECT().ID().Return(uint32(5)).Times(2)

	bootstrappedShardIds := map[uint32]struct{}{5: {}}

	err := ns.createEmptyWarmIndexIfNotExists(blockStart)
	require.NoError(t, err)

	err = ns.createEmptyWarmIndexIfNotExists(blockStart)
	require.NoError(t, err)

	reader, err := fs.NewIndexReader(ns.StorageOptions().CommitLogOptions().FilesystemOptions())
	require.NoError(t, err)

	openOpts := fs.IndexReaderOpenOptions{
		Identifier: fs.FileSetFileIdentifier{
			FileSetContentType: persist.FileSetIndexContentType,
			Namespace:          nsID,
			BlockStart:         blockStart,
			VolumeIndex:        0,
		},
	}

	idx, err := reader.Open(openOpts)
	require.NoError(t, err)

	assert.Equal(t, bootstrappedShardIds, idx.Shards)
	assert.Equal(t, idxpersist.DefaultIndexVolumeType, reader.IndexVolumeType())
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

func assertNeedsFlush(t *testing.T, ns *dbNamespace, t0, t1 xtime.UnixNano, assertTrue bool) {
	needsFlush, err := ns.NeedsFlush(t0, t1)
	require.NoError(t, err)
	require.Equal(t, assertTrue, needsFlush)
}

func contains(c []uint32, x uint32) bool {
	for _, y := range c {
		if x == y {
			return true
		}
	}
	return false
}
