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
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/storage/repair"
	"github.com/m3db/m3/src/dbnode/storage/series"
	xmetrics "github.com/m3db/m3/src/dbnode/x/metrics"
	"github.com/m3db/m3cluster/shard"
	xclock "github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/context"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	xtest "github.com/m3db/m3x/test"
	xtime "github.com/m3db/m3x/time"
	xwatch "github.com/m3db/m3x/watch"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

var (
	defaultTestNs1ID         = ident.StringID("testns1")
	defaultTestNs2ID         = ident.StringID("testns2")
	defaultTestRetentionOpts = retention.NewOptions().SetBufferFuture(10 * time.Minute).SetBufferPast(10 * time.Minute).
					SetBlockSize(2 * time.Hour).SetRetentionPeriod(2 * 24 * time.Hour)
	defaultTestNs2RetentionOpts = retention.NewOptions().SetBufferFuture(10 * time.Minute).SetBufferPast(10 * time.Minute).
					SetBlockSize(4 * time.Hour).SetRetentionPeriod(2 * 24 * time.Hour)
	defaultTestCommitlogBlockSize = 2 * time.Hour
	defaultTestNs1Opts            = namespace.NewOptions().SetRetentionOptions(defaultTestRetentionOpts)
	defaultTestNs2Opts            = namespace.NewOptions().SetRetentionOptions(defaultTestNs2RetentionOpts)
	defaultTestDatabaseOptions    Options
)

func init() {
	iopts := instrument.NewOptions().SetReportInterval(10 * time.Millisecond)

	opts := newOptions(pool.NewObjectPoolOptions().
		SetSize(16))

	// Use a no-op options manager to avoid spinning up a goroutine to listen
	// for updates, which causes problems with leaktest in individual test
	// executions
	runtimeOptionsMgr := runtime.NewNoOpOptionsManager(
		runtime.NewOptions())

	pm, err := fs.NewPersistManager(fs.NewOptions().
		SetRuntimeOptionsManager(runtimeOptionsMgr))
	if err != nil {
		panic(err)
	}

	defaultTestDatabaseOptions = opts.
		SetInstrumentOptions(iopts).
		SetSeriesCachePolicy(series.CacheAll).
		SetPersistManager(pm).
		SetRepairEnabled(false).
		SetCommitLogOptions(opts.CommitLogOptions().
			SetBlockSize(defaultTestCommitlogBlockSize)).
		SetIndexOptions(
			opts.IndexOptions().SetInstrumentOptions(iopts))
}

type nsMapCh chan namespace.Map

type mockNsInitializer struct {
	registry *namespace.MockRegistry
	updateCh chan struct{}
}

func (mi *mockNsInitializer) Init() (namespace.Registry, error) {
	return mi.registry, nil
}

func newMockNsInitializer(
	t *testing.T,
	ctrl *gomock.Controller,
	nsMapCh nsMapCh,
) namespace.Initializer {
	updateCh := make(chan struct{}, 10)
	watch := xwatch.NewWatchable()
	go func() {
		for {
			v, ok := <-nsMapCh
			if !ok { // closed channel
				return
			}

			watch.Update(v)
			updateCh <- struct{}{}
		}
	}()

	_, w, err := watch.Watch()
	require.NoError(t, err)

	nsWatch := namespace.NewWatch(w)
	reg := namespace.NewMockRegistry(ctrl)
	reg.EXPECT().Watch().Return(nsWatch, nil).AnyTimes()

	return &mockNsInitializer{
		registry: reg,
		updateCh: updateCh,
	}
}

func testNamespaceMap(t *testing.T) namespace.Map {
	md1, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)
	md2, err := namespace.NewMetadata(defaultTestNs2ID, defaultTestNs2Opts)
	require.NoError(t, err)
	nsMap, err := namespace.NewMap([]namespace.Metadata{md1, md2})
	require.NoError(t, err)
	return nsMap
}

func testDatabaseOptions() Options {
	// NB(r): We don't need to recreate the options multiple
	// times as they are immutable - we save considerable
	// memory by doing this avoiding creating default pools
	// several times.
	return defaultTestDatabaseOptions
}

func testRepairOptions(ctrl *gomock.Controller) repair.Options {
	return repair.NewOptions().
		SetAdminClient(client.NewMockAdminClient(ctrl)).
		SetRepairInterval(time.Second).
		SetRepairTimeOffset(500 * time.Millisecond).
		SetRepairTimeJitter(300 * time.Millisecond).
		SetRepairCheckInterval(100 * time.Millisecond)
}

func newMockdatabase(ctrl *gomock.Controller, ns ...databaseNamespace) *Mockdatabase {
	db := NewMockdatabase(ctrl)
	db.EXPECT().Options().Return(testDatabaseOptions()).AnyTimes()
	if len(ns) != 0 {
		db.EXPECT().GetOwnedNamespaces().Return(ns, nil).AnyTimes()
	}
	return db
}

func newTestDatabase(t *testing.T, ctrl *gomock.Controller, bs BootstrapState) (*db, nsMapCh, xmetrics.TestStatsReporter) {
	testReporter := xmetrics.NewTestStatsReporter(xmetrics.NewTestStatsReporterOptions())
	scope, _ := tally.NewRootScope(tally.ScopeOptions{
		Reporter: testReporter,
	}, 100*time.Millisecond)

	mapCh := make(nsMapCh, 10)
	mapCh <- testNamespaceMap(t)

	opts := testDatabaseOptions()
	opts = opts.SetInstrumentOptions(
		opts.InstrumentOptions().SetMetricsScope(scope)).
		SetRepairEnabled(false).
		SetRepairOptions(testRepairOptions(ctrl)).
		SetNamespaceInitializer(newMockNsInitializer(t, ctrl, mapCh))

	shards := sharding.NewShards([]uint32{0, 1}, shard.Available)
	shardSet, err := sharding.NewShardSet(shards, nil)
	require.NoError(t, err)

	database, err := NewDatabase(shardSet, opts)
	require.NoError(t, err)
	d := database.(*db)
	m := d.mediator.(*mediator)
	bsm := newBootstrapManager(d, m, opts).(*bootstrapManager)
	bsm.state = bs
	m.databaseBootstrapManager = bsm

	return d, mapCh, testReporter
}

func dbAddNewMockNamespace(
	ctrl *gomock.Controller,
	d *db,
	id string,
) *MockdatabaseNamespace {
	ns := ident.StringID(id)
	mockNamespace := NewMockdatabaseNamespace(ctrl)
	mockNamespace.EXPECT().ID().Return(ns).AnyTimes()
	d.namespaces.Set(ns, mockNamespace)
	return mockNamespace
}

func TestDatabaseOpen(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	d, mapCh, _ := newTestDatabase(t, ctrl, BootstrapNotStarted)
	defer func() {
		close(mapCh)
		leaktest.CheckTimeout(t, time.Second)()
	}()
	require.NoError(t, d.Open())
	require.Equal(t, errDatabaseAlreadyOpen, d.Open())
	require.NoError(t, d.Close())
}

func TestDatabaseClose(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	d, mapCh, _ := newTestDatabase(t, ctrl, Bootstrapped)
	defer func() {
		close(mapCh)
		leaktest.CheckTimeout(t, time.Second)()
	}()
	require.NoError(t, d.Open())
	require.NoError(t, d.Close())
	require.Equal(t, errDatabaseAlreadyClosed, d.Close())
}

func TestDatabaseTerminate(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	d, mapCh, _ := newTestDatabase(t, ctrl, Bootstrapped)
	defer func() {
		close(mapCh)
		leaktest.CheckTimeout(t, time.Second)()
	}()
	require.NoError(t, d.Open())
	require.NoError(t, d.Terminate())
	require.Equal(t, errDatabaseAlreadyClosed, d.Close())
}

func TestDatabaseReadEncodedNamespaceNotOwned(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	d, mapCh, _ := newTestDatabase(t, ctrl, Bootstrapped)
	defer func() {
		close(mapCh)
	}()
	_, err := d.ReadEncoded(ctx, ident.StringID("nonexistent"), ident.StringID("foo"), time.Now(), time.Now())
	require.Equal(t, "no such namespace nonexistent", err.Error())
}

func TestDatabaseReadEncodedNamespaceOwned(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	d, mapCh, _ := newTestDatabase(t, ctrl, Bootstrapped)
	defer func() {
		close(mapCh)
	}()

	ns := ident.StringID("testns1")
	id := ident.StringID("bar")
	end := time.Now()
	start := end.Add(-time.Hour)
	mockNamespace := NewMockdatabaseNamespace(ctrl)
	mockNamespace.EXPECT().ReadEncoded(ctx, id, start, end).Return(nil, nil)
	d.namespaces.Set(ns, mockNamespace)

	res, err := d.ReadEncoded(ctx, ns, id, start, end)
	require.Nil(t, res)
	require.Nil(t, err)
}

func TestDatabaseFetchBlocksNamespaceNotOwned(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	d, mapCh, _ := newTestDatabase(t, ctrl, Bootstrapped)
	defer func() {
		close(mapCh)
	}()

	now := time.Now()
	starts := []time.Time{now, now.Add(time.Second), now.Add(-time.Second)}
	res, err := d.FetchBlocks(ctx, ident.StringID("non-existent-ns"), 0, ident.StringID("foo"), starts)
	require.Nil(t, res)
	require.True(t, xerrors.IsInvalidParams(err))
}

func TestDatabaseFetchBlocksNamespaceOwned(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	d, mapCh, _ := newTestDatabase(t, ctrl, Bootstrapped)
	defer func() {
		close(mapCh)
	}()

	ns := ident.StringID("testns1")
	id := ident.StringID("bar")
	shardID := uint32(0)
	now := time.Now()
	starts := []time.Time{now, now.Add(time.Second), now.Add(-time.Second)}
	expected := []block.FetchBlockResult{block.NewFetchBlockResult(starts[0], nil, nil)}
	mockNamespace := NewMockdatabaseNamespace(ctrl)
	mockNamespace.EXPECT().FetchBlocks(ctx, shardID, id, starts).Return(expected, nil)
	d.namespaces.Set(ns, mockNamespace)

	res, err := d.FetchBlocks(ctx, ns, shardID, id, starts)
	require.Equal(t, expected, res)
	require.NoError(t, err)
}

func TestDatabaseFetchBlocksMetadataShardNotOwned(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	var (
		ns        = ident.StringID("testns1")
		shardID   = uint32(0)
		start     = time.Now()
		end       = start.Add(time.Hour)
		limit     = int64(100)
		pageToken = int64(0)
		opts      = block.FetchBlocksMetadataOptions{
			IncludeSizes:     true,
			IncludeChecksums: true,
			IncludeLastRead:  true,
		}
	)
	d, mapCh, _ := newTestDatabase(t, ctrl, Bootstrapped)
	defer func() {
		close(mapCh)
	}()
	res, nextPageToken, err := d.FetchBlocksMetadata(ctx, ns, shardID, start, end, limit, pageToken, opts)
	require.Nil(t, res)
	require.Nil(t, nextPageToken)
	require.Error(t, err)
}

func TestDatabaseFetchBlocksMetadataShardOwned(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	var (
		ns        = ident.StringID("testns1")
		shardID   = uint32(397)
		start     = time.Now()
		end       = start.Add(time.Hour)
		limit     = int64(100)
		pageToken = int64(0)
		opts      = block.FetchBlocksMetadataOptions{
			IncludeSizes:     true,
			IncludeChecksums: true,
			IncludeLastRead:  true,
		}
	)

	d, mapCh, _ := newTestDatabase(t, ctrl, Bootstrapped)
	defer func() {
		close(mapCh)
	}()

	expectedBlocks := block.NewFetchBlocksMetadataResults()
	expectedBlocks.Add(block.NewFetchBlocksMetadataResult(ident.StringID("bar"), nil, nil))
	expectedToken := new(int64)
	mockNamespace := NewMockdatabaseNamespace(ctrl)
	mockNamespace.
		EXPECT().
		FetchBlocksMetadata(ctx, shardID, start, end, limit, pageToken, opts).
		Return(expectedBlocks, expectedToken, nil)
	d.namespaces.Set(ns, mockNamespace)

	res, nextToken, err := d.FetchBlocksMetadata(ctx, ns, shardID, start, end, limit, pageToken, opts)
	require.Equal(t, expectedBlocks, res)
	require.Equal(t, expectedToken, nextToken)
	require.Nil(t, err)
}

func TestDatabaseNamespaces(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	d, mapCh, _ := newTestDatabase(t, ctrl, Bootstrapped)
	defer func() {
		close(mapCh)
	}()

	dbAddNewMockNamespace(ctrl, d, "testns1")
	dbAddNewMockNamespace(ctrl, d, "testns2")

	result := d.Namespaces()
	require.Equal(t, 2, len(result))

	sort.Sort(NamespacesByID(result))
	assert.Equal(t, "testns1", result[0].ID().String())
	assert.Equal(t, "testns2", result[1].ID().String())
}

func TestGetOwnedNamespacesErrorIfClosed(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	d, mapCh, _ := newTestDatabase(t, ctrl, Bootstrapped)
	defer func() {
		close(mapCh)
	}()

	require.NoError(t, d.Open())
	require.NoError(t, d.Terminate())

	_, err := d.GetOwnedNamespaces()
	require.Equal(t, errDatabaseIsClosed, err)
}

func TestDatabaseAssignShardSet(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	d, mapCh, _ := newTestDatabase(t, ctrl, Bootstrapped)
	defer func() {
		close(mapCh)
	}()

	var ns []*MockdatabaseNamespace
	ns = append(ns, dbAddNewMockNamespace(ctrl, d, "testns1"))
	ns = append(ns, dbAddNewMockNamespace(ctrl, d, "testns2"))

	shards := append(sharding.NewShards([]uint32{0, 1}, shard.Available),
		sharding.NewShards([]uint32{2}, shard.Initializing)...)
	shardSet, err := sharding.NewShardSet(shards, nil)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(len(ns))
	for _, n := range ns {
		n.EXPECT().AssignShardSet(shardSet).Do(func(_ sharding.ShardSet) {
			wg.Done()
		})
	}

	d.AssignShardSet(shardSet)

	wg.Wait()
}

func TestDatabaseBootstrappedAssignShardSet(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	d, mapCh, _ := newTestDatabase(t, ctrl, Bootstrapped)
	defer func() {
		close(mapCh)
	}()

	ns := dbAddNewMockNamespace(ctrl, d, "testns")

	mediator := NewMockdatabaseMediator(ctrl)
	mediator.EXPECT().Bootstrap().Return(nil)
	d.mediator = mediator

	assert.NoError(t, d.Bootstrap())

	shards := append(sharding.NewShards([]uint32{0, 1}, shard.Available),
		sharding.NewShards([]uint32{2}, shard.Initializing)...)
	shardSet, err := sharding.NewShardSet(shards, nil)
	require.NoError(t, err)

	ns.EXPECT().AssignShardSet(shardSet)

	var wg sync.WaitGroup
	wg.Add(1)
	mediator.EXPECT().Bootstrap().Return(nil).Do(func() {
		wg.Done()
	})

	d.AssignShardSet(shardSet)

	wg.Wait()
}

func TestDatabaseRemoveNamespace(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	d, mapCh, testReporter := newTestDatabase(t, ctrl, Bootstrapped)
	require.NoError(t, d.Open())
	defer func() {
		close(mapCh)
		require.NoError(t, d.Close())
		leaktest.CheckTimeout(t, time.Second)()
	}()

	// retrieve the update channel to track propatation
	updateCh := d.opts.NamespaceInitializer().(*mockNsInitializer).updateCh

	// check initial namespaces
	nses := d.Namespaces()
	require.Len(t, nses, 2)

	// construct new namespace Map
	md1, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)
	nsMap, err := namespace.NewMap([]namespace.Metadata{md1})
	require.NoError(t, err)

	// update the database watch with new Map
	mapCh <- nsMap

	// wait till the update has propagated
	<-updateCh
	<-updateCh

	// the updateCh gets are in-sufficient to determine the update has been applied
	// to the database, they only measure if the watch itself has been updated. It
	// can take a few ms until the DB finds those values.
	require.True(t, xclock.WaitUntil(func() bool {
		counter, ok := testReporter.Counters()["database.namespace-watch.updates"]
		return ok && counter == 1
	}, 2*time.Second))
	require.True(t, xclock.WaitUntil(func() bool {
		return len(d.Namespaces()) == 2
	}, 2*time.Second))
}

func TestDatabaseAddNamespace(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	d, mapCh, testReporter := newTestDatabase(t, ctrl, Bootstrapped)
	require.NoError(t, d.Open())
	defer func() {
		close(mapCh)
		require.NoError(t, d.Close())
		leaktest.CheckTimeout(t, time.Second)()
	}()

	// retrieve the update channel to track propatation
	updateCh := d.opts.NamespaceInitializer().(*mockNsInitializer).updateCh

	// check initial namespaces
	nses := d.Namespaces()
	require.Len(t, nses, 2)

	// construct new namespace Map
	md1, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)
	md2, err := namespace.NewMetadata(defaultTestNs2ID, defaultTestNs2Opts)
	require.NoError(t, err)
	md3, err := namespace.NewMetadata(ident.StringID("and1"), defaultTestNs1Opts)
	require.NoError(t, err)
	nsMap, err := namespace.NewMap([]namespace.Metadata{md1, md2, md3})
	require.NoError(t, err)

	// update the database watch with new Map
	mapCh <- nsMap

	// wait till the update has propagated
	<-updateCh
	<-updateCh

	// the updateCh gets are in-sufficient to determine the update has been applied
	// to the database, they only measure if the watch itself has been updated. It
	// can take a few ms until the DB finds those values.
	require.True(t, xclock.WaitUntil(func() bool {
		counter, ok := testReporter.Counters()["database.namespace-watch.updates"]
		return ok && counter == 1
	}, 2*time.Second))
	require.True(t, xclock.WaitUntil(func() bool {
		return len(d.Namespaces()) == 3
	}, 2*time.Second))

	// ensure the expected namespaces exist
	nses = d.Namespaces()
	require.Len(t, nses, 3)
	ns1, ok := d.Namespace(defaultTestNs1ID)
	require.True(t, ok)
	require.Equal(t, md1.Options(), ns1.Options())
	ns2, ok := d.Namespace(defaultTestNs2ID)
	require.True(t, ok)
	require.Equal(t, md2.Options(), ns2.Options())
	ns3, ok := d.Namespace(ident.StringID("and1"))
	require.True(t, ok)
	require.Equal(t, md1.Options(), ns3.Options())
}

func TestDatabaseUpdateNamespace(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	d, mapCh, _ := newTestDatabase(t, ctrl, Bootstrapped)
	require.NoError(t, d.Open())
	defer func() {
		close(mapCh)
		require.NoError(t, d.Close())
		leaktest.CheckTimeout(t, time.Second)()
	}()

	// retrieve the update channel to track propatation
	updateCh := d.opts.NamespaceInitializer().(*mockNsInitializer).updateCh

	// check initial namespaces
	nses := d.Namespaces()
	require.Len(t, nses, 2)

	// construct new namespace Map
	ropts := defaultTestNs1Opts.RetentionOptions().SetRetentionPeriod(2000 * time.Hour)
	md1, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts.SetRetentionOptions(ropts))
	require.NoError(t, err)
	md2, err := namespace.NewMetadata(defaultTestNs2ID, defaultTestNs2Opts)
	require.NoError(t, err)
	nsMap, err := namespace.NewMap([]namespace.Metadata{md1, md2})
	require.NoError(t, err)

	// update the database watch with new Map
	mapCh <- nsMap

	// wait till the update has propagated
	<-updateCh
	<-updateCh
	time.Sleep(10 * time.Millisecond)

	// ensure the namespaces have old properties
	nses = d.Namespaces()
	require.Len(t, nses, 2)
	ns1, ok := d.Namespace(defaultTestNs1ID)
	require.True(t, ok)
	require.Equal(t, defaultTestNs1Opts, ns1.Options())
	ns2, ok := d.Namespace(defaultTestNs2ID)
	require.True(t, ok)
	require.Equal(t, defaultTestNs2Opts, ns2.Options())
}

func TestDatabaseNamespaceIndexFunctions(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	d, mapCh, _ := newTestDatabase(t, ctrl, BootstrapNotStarted)
	defer func() {
		close(mapCh)
	}()

	ns := dbAddNewMockNamespace(ctrl, d, "testns")
	ns.EXPECT().GetOwnedShards().Return([]databaseShard{}).AnyTimes()
	ns.EXPECT().Tick(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	ns.EXPECT().BootstrapState().Return(ShardBootstrapStates{}).AnyTimes()
	require.NoError(t, d.Open())

	ctx := context.NewContext()
	ns.EXPECT().WriteTagged(ctx, ident.NewIDMatcher("foo"), gomock.Any(),
		time.Time{}, 1.0, xtime.Second, nil).Return(nil)
	require.NoError(t, d.WriteTagged(ctx, ident.StringID("testns"),
		ident.StringID("foo"), ident.EmptyTagIterator, time.Time{},
		1.0, xtime.Second, nil))

	ns.EXPECT().WriteTagged(ctx, ident.NewIDMatcher("foo"), gomock.Any(),
		time.Time{}, 1.0, xtime.Second, nil).Return(fmt.Errorf("random err"))
	require.Error(t, d.WriteTagged(ctx, ident.StringID("testns"),
		ident.StringID("foo"), ident.EmptyTagIterator, time.Time{},
		1.0, xtime.Second, nil))

	var (
		q    = index.Query{}
		opts = index.QueryOptions{}
		res  = index.QueryResults{}
		err  error
	)

	ns.EXPECT().QueryIDs(ctx, q, opts).Return(res, nil)
	_, err = d.QueryIDs(ctx, ident.StringID("testns"), q, opts)
	require.NoError(t, err)

	ns.EXPECT().QueryIDs(ctx, q, opts).Return(res, fmt.Errorf("random err"))
	_, err = d.QueryIDs(ctx, ident.StringID("testns"), q, opts)
	require.Error(t, err)

	ns.EXPECT().Close().Return(nil)
	require.NoError(t, d.Close())
}

func TestDatabaseBootstrapState(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	d, mapCh, _ := newTestDatabase(t, ctrl, Bootstrapped)
	defer func() {
		close(mapCh)
	}()

	ns1 := dbAddNewMockNamespace(ctrl, d, "testns1")
	ns1.EXPECT().BootstrapState().Return(ShardBootstrapStates{
		1: Bootstrapping,
	})
	ns2 := dbAddNewMockNamespace(ctrl, d, "testns2")
	ns2.EXPECT().BootstrapState().Return(ShardBootstrapStates{
		2: Bootstrapped,
	})

	dbBootstrapState := d.BootstrapState()
	require.Equal(t, DatabaseBootstrapState{
		NamespaceBootstrapStates: NamespaceBootstrapStates{
			"testns1": ShardBootstrapStates{
				1: Bootstrapping,
			},
			"testns2": ShardBootstrapStates{
				2: Bootstrapped,
			},
		},
	}, dbBootstrapState)
}
