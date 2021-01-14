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
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage/block"
	dberrors "github.com/m3db/m3/src/dbnode/storage/errors"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/repair"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/tracepoint"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/ts/writes"
	xmetrics "github.com/m3db/m3/src/dbnode/x/metrics"
	"github.com/m3db/m3/src/m3ninx/idx"
	xclock "github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/context"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"
	xwatch "github.com/m3db/m3/src/x/watch"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/dbnode/testdata/prototest"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
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
	defaultTestNs1Opts = namespace.NewOptions().SetRetentionOptions(defaultTestRetentionOpts)
	defaultTestNs2Opts = namespace.NewOptions().SetRetentionOptions(defaultTestNs2RetentionOpts)
	testSchemaHistory  = prototest.NewSchemaHistory()
	testClientOptions  = client.NewOptions()
)

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

func testRepairOptions(ctrl *gomock.Controller) repair.Options {
	var (
		origin     = topology.NewHost("some-id", "some-address")
		clientOpts = testClientOptions.(client.AdminOptions).SetOrigin(origin)
		mockClient = client.NewMockAdminClient(ctrl)
	)
	mockClient.EXPECT().Options().Return(clientOpts).AnyTimes()
	return repair.NewOptions().
		SetAdminClients([]client.AdminClient{mockClient}).
		SetRepairCheckInterval(100 * time.Millisecond)
}

func newMockdatabase(ctrl *gomock.Controller, ns ...databaseNamespace) *Mockdatabase {
	db := NewMockdatabase(ctrl)
	db.EXPECT().Options().Return(DefaultTestOptions()).AnyTimes()
	if len(ns) != 0 {
		db.EXPECT().OwnedNamespaces().Return(ns, nil).AnyTimes()
	}
	return db
}

type newTestDatabaseOpt struct {
	bs    BootstrapState
	nsMap namespace.Map
	dbOpt Options
}

func defaultTestDatabase(t *testing.T, ctrl *gomock.Controller, bs BootstrapState) (*db, nsMapCh, xmetrics.TestStatsReporter) {
	return newTestDatabase(t, ctrl, newTestDatabaseOpt{bs: bs, nsMap: testNamespaceMap(t), dbOpt: DefaultTestOptions()})
}

func newTestDatabase(
	t *testing.T,
	ctrl *gomock.Controller,
	opt newTestDatabaseOpt,
) (*db, nsMapCh, xmetrics.TestStatsReporter) {
	testReporter := xmetrics.NewTestStatsReporter(xmetrics.NewTestStatsReporterOptions())
	scope, _ := tally.NewRootScope(tally.ScopeOptions{
		Reporter: testReporter,
	}, 100*time.Millisecond)

	mapCh := make(nsMapCh, 10)
	mapCh <- opt.nsMap

	opts := opt.dbOpt
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
	bsm.state = opt.bs
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
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	d, mapCh, _ := defaultTestDatabase(t, ctrl, BootstrapNotStarted)
	defer func() {
		close(mapCh)
		leaktest.CheckTimeout(t, time.Second)()
	}()
	require.NoError(t, d.Open())
	require.Equal(t, errDatabaseAlreadyOpen, d.Open())
	require.NoError(t, d.Close())
}

func TestDatabaseClose(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	d, mapCh, _ := defaultTestDatabase(t, ctrl, Bootstrapped)
	defer func() {
		close(mapCh)
		leaktest.CheckTimeout(t, time.Second)()
	}()
	require.NoError(t, d.Open())
	require.NoError(t, d.Close())
	require.Equal(t, errDatabaseAlreadyClosed, d.Close())
}

func TestDatabaseTerminate(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	d, mapCh, _ := defaultTestDatabase(t, ctrl, Bootstrapped)
	defer func() {
		close(mapCh)
		leaktest.CheckTimeout(t, time.Second)()
	}()
	require.NoError(t, d.Open())
	require.NoError(t, d.Terminate())
	require.Equal(t, errDatabaseAlreadyClosed, d.Close())
}

func TestDatabaseReadEncodedNamespaceNonExistent(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	d, mapCh, _ := defaultTestDatabase(t, ctrl, Bootstrapped)
	defer func() {
		close(mapCh)
	}()
	_, err := d.ReadEncoded(ctx, ident.StringID("nonexistent"),
		ident.StringID("foo"), time.Now(), time.Now())
	require.True(t, dberrors.IsUnknownNamespaceError(err))
}

func TestDatabaseReadEncoded(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	d, mapCh, _ := defaultTestDatabase(t, ctrl, Bootstrapped)
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

func TestDatabaseWideQueryNamespaceNonExistent(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	d, mapCh, _ := defaultTestDatabase(t, ctrl, Bootstrapped)
	defer func() {
		close(mapCh)
	}()
	_, err := d.WideQuery(ctx, ident.StringID("nonexistent"),
		index.Query{}, time.Now(), nil, index.IterationOptions{})
	require.True(t, dberrors.IsUnknownNamespaceError(err))
}

func TestDatabaseFetchBlocksNamespaceNonExistent(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	d, mapCh, _ := defaultTestDatabase(t, ctrl, Bootstrapped)
	defer func() {
		close(mapCh)
	}()

	now := time.Now()
	starts := []time.Time{now, now.Add(time.Second), now.Add(-time.Second)}
	res, err := d.FetchBlocks(ctx, ident.StringID("non-existent-ns"), 0, ident.StringID("foo"), starts)
	require.Nil(t, res)
	require.True(t, xerrors.IsInvalidParams(err))
}

func TestDatabaseFetchBlocks(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	d, mapCh, _ := defaultTestDatabase(t, ctrl, Bootstrapped)
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

func TestDatabaseNamespaces(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	d, mapCh, _ := defaultTestDatabase(t, ctrl, Bootstrapped)
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

func TestOwnedNamespacesErrorIfClosed(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	d, mapCh, _ := defaultTestDatabase(t, ctrl, Bootstrapped)
	defer func() {
		close(mapCh)
	}()

	require.NoError(t, d.Open())
	require.NoError(t, d.Terminate())

	_, err := d.OwnedNamespaces()
	require.Equal(t, errDatabaseIsClosed, err)
}

func TestDatabaseAssignShardSet(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	d, mapCh, _ := defaultTestDatabase(t, ctrl, Bootstrapped)
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

	t1 := d.lastReceivedNewShards
	d.AssignShardSet(shardSet)
	require.True(t, d.lastReceivedNewShards.After(t1))

	wg.Wait()
}

func TestDatabaseAssignShardSetBehaviorNoNewShards(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	d, mapCh, _ := defaultTestDatabase(t, ctrl, Bootstrapped)
	defer func() {
		close(mapCh)
	}()

	// Set a mock mediator to be certain that bootstrap is not called when
	// no new shards are assigned.
	mediator := NewMockdatabaseMediator(ctrl)
	d.mediator = mediator

	var ns []*MockdatabaseNamespace
	ns = append(ns, dbAddNewMockNamespace(ctrl, d, "testns1"))
	ns = append(ns, dbAddNewMockNamespace(ctrl, d, "testns2"))

	var wg sync.WaitGroup
	wg.Add(len(ns))
	for _, n := range ns {
		n.EXPECT().AssignShardSet(d.shardSet).Do(func(_ sharding.ShardSet) {
			wg.Done()
		})
	}

	t1 := d.lastReceivedNewShards
	d.AssignShardSet(d.shardSet)
	// Ensure that lastReceivedNewShards is not updated if no new shards are assigned.
	require.True(t, d.lastReceivedNewShards.Equal(t1))

	wg.Wait()
}

func TestDatabaseBootstrappedAssignShardSet(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	d, mapCh, _ := defaultTestDatabase(t, ctrl, Bootstrapped)
	defer func() {
		close(mapCh)
	}()

	ns := dbAddNewMockNamespace(ctrl, d, "testns")

	mediator := NewMockdatabaseMediator(ctrl)
	mediator.EXPECT().Bootstrap().Return(BootstrapResult{}, nil)
	d.mediator = mediator

	assert.NoError(t, d.Bootstrap())

	shards := append(sharding.NewShards([]uint32{0, 1}, shard.Available),
		sharding.NewShards([]uint32{2}, shard.Initializing)...)
	shardSet, err := sharding.NewShardSet(shards, nil)
	require.NoError(t, err)

	ns.EXPECT().AssignShardSet(shardSet)

	var wg sync.WaitGroup
	wg.Add(1)
	mediator.EXPECT().Bootstrap().Return(BootstrapResult{}, nil).Do(func() {
		wg.Done()
	})

	d.AssignShardSet(shardSet)

	wg.Wait()
}

func TestDatabaseRemoveNamespace(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	d, mapCh, testReporter := defaultTestDatabase(t, ctrl, Bootstrapped)
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
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	d, mapCh, testReporter := defaultTestDatabase(t, ctrl, Bootstrapped)
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
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	d, mapCh, _ := defaultTestDatabase(t, ctrl, Bootstrapped)
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

	// Ensure schema is not set and no error
	require.Nil(t, ns1.Schema())
	require.Nil(t, ns2.Schema())
	schema, err := d.Options().SchemaRegistry().GetLatestSchema(defaultTestNs2ID)
	require.NoError(t, err)
	require.Nil(t, schema)
}

func TestDatabaseCreateSchemaNotSet(t *testing.T) {
	protoTestDatabaseOptions := DefaultTestOptions().
		SetSchemaRegistry(namespace.NewSchemaRegistry(true, nil))

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	// Start the database with two namespaces, one miss configured (missing schema).
	nsID1 := ident.StringID("testns1")
	md1, err := namespace.NewMetadata(nsID1, defaultTestNs1Opts.SetSchemaHistory(testSchemaHistory))
	require.NoError(t, err)
	nsID2 := ident.StringID("testns2")
	md2, err := namespace.NewMetadata(nsID2, defaultTestNs1Opts)
	require.NoError(t, err)
	nsMap, err := namespace.NewMap([]namespace.Metadata{md1, md2})
	require.NoError(t, err)

	d, mapCh, _ := newTestDatabase(t, ctrl, newTestDatabaseOpt{bs: Bootstrapped, nsMap: nsMap, dbOpt: protoTestDatabaseOptions})
	require.NoError(t, d.Open())
	defer func() {
		close(mapCh)
		require.NoError(t, d.Close())
		leaktest.CheckTimeout(t, time.Second)()
	}()

	// check initial namespaces
	nses := d.Namespaces()
	require.Len(t, nses, 1)

	_, ok := d.Namespace(nsID1)
	require.True(t, ok)
	_, ok = d.Namespace(nsID2)
	require.False(t, ok)
}

func TestDatabaseUpdateNamespaceSchemaNotSet(t *testing.T) {
	protoTestDatabaseOptions := DefaultTestOptions().
		SetSchemaRegistry(namespace.NewSchemaRegistry(true, nil))

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	// Start db with no namespaces.
	d, mapCh, _ := newTestDatabase(t, ctrl, newTestDatabaseOpt{bs: Bootstrapped, dbOpt: protoTestDatabaseOptions})
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
	require.Len(t, nses, 0)

	// construct new namespace Map
	nsID3 := ident.StringID("testns3")
	md3, err := namespace.NewMetadata(nsID3, defaultTestNs1Opts)
	require.NoError(t, err)
	nsMap, err := namespace.NewMap([]namespace.Metadata{md3})
	require.NoError(t, err)

	// update the database watch with new Map
	mapCh <- nsMap

	// wait till the update has propagated
	time.Sleep(10 * time.Millisecond)

	// ensure the namespace3 is not created successfully.
	nses = d.Namespaces()
	require.Len(t, nses, 0)

	// Update nsID3 schema
	md3, err = namespace.NewMetadata(nsID3, defaultTestNs1Opts.SetSchemaHistory(testSchemaHistory))
	require.NoError(t, err)
	nsMap, err = namespace.NewMap([]namespace.Metadata{md3})
	require.NoError(t, err)

	// update the database watch with new Map
	mapCh <- nsMap

	// wait till the update has propagated
	time.Sleep(10 * time.Millisecond)

	// Ensure the namespace3 is created successfully.
	nses = d.Namespaces()
	require.Len(t, nses, 1)
	ns3, ok := d.Namespace(nsID3)
	require.True(t, ok)

	// Ensure schema is set
	require.NotNil(t, ns3.Schema())

	schema, err := d.Options().SchemaRegistry().GetLatestSchema(nsID3)
	require.NoError(t, err)
	require.NotNil(t, schema)

	// Update nsID3 schema to empty
	md3, err = namespace.NewMetadata(nsID3, defaultTestNs1Opts)
	require.NoError(t, err)
	nsMap, err = namespace.NewMap([]namespace.Metadata{md3})
	require.NoError(t, err)

	// update the database watch with new Map
	mapCh <- nsMap

	// wait till the update has propagated
	<-updateCh
	time.Sleep(10 * time.Millisecond)

	// Ensure schema is not set to empty
	require.NotNil(t, ns3.Schema())

	schema, err = d.Options().SchemaRegistry().GetLatestSchema(nsID3)
	require.NoError(t, err)
	require.NotNil(t, schema)
}

func TestDatabaseNamespaceIndexFunctions(t *testing.T) {
	testDatabaseNamespaceIndexFunctions(t, true)
}

func TestDatabaseNamespaceIndexFunctionsNoCommitlog(t *testing.T) {
	testDatabaseNamespaceIndexFunctions(t, false)
}

func testDatabaseNamespaceIndexFunctions(t *testing.T, commitlogEnabled bool) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	d, mapCh, _ := defaultTestDatabase(t, ctrl, BootstrapNotStarted)
	defer func() {
		close(mapCh)
	}()

	commitLog := d.commitLog
	if !commitlogEnabled {
		// We don't mock the commitlog so set this to nil to ensure its
		// not being used as the test will panic if any methods are called
		// on it.
		d.commitLog = nil
	}

	ns := dbAddNewMockNamespace(ctrl, d, "testns")
	nsOptions := namespace.NewOptions().
		SetWritesToCommitLog(commitlogEnabled)

	ns.EXPECT().OwnedShards().Return([]databaseShard{}).AnyTimes()
	ns.EXPECT().Tick(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	ns.EXPECT().ShardBootstrapState().Return(ShardBootstrapStates{}).AnyTimes()
	ns.EXPECT().Options().Return(nsOptions).AnyTimes()
	require.NoError(t, d.Open())

	var (
		namespace   = ident.StringID("testns")
		ctx         = context.NewContext()
		id          = ident.StringID("foo")
		tagsIter    = ident.EmptyTagIterator
		seriesWrite = SeriesWrite{
			Series: ts.Series{
				ID:        id,
				Namespace: namespace,
			},
			WasWritten: true,
		}
	)

	// create initial span from a mock tracer and get ctx
	mtr := mocktracer.New()
	sp := mtr.StartSpan("root")
	ctx.SetGoContext(opentracing.ContextWithSpan(stdlibctx.Background(), sp))

	ns.EXPECT().WriteTagged(gomock.Any(), ident.NewIDMatcher("foo"), gomock.Any(),
		time.Time{}, 1.0, xtime.Second, nil).Return(seriesWrite, nil)
	require.NoError(t, d.WriteTagged(ctx, namespace,
		id, tagsIter, time.Time{},
		1.0, xtime.Second, nil))

	ns.EXPECT().WriteTagged(gomock.Any(), ident.NewIDMatcher("foo"), gomock.Any(),
		time.Time{}, 1.0, xtime.Second, nil).Return(SeriesWrite{}, fmt.Errorf("random err"))
	require.Error(t, d.WriteTagged(ctx, namespace,
		ident.StringID("foo"), ident.EmptyTagIterator, time.Time{},
		1.0, xtime.Second, nil))

	var (
		q = index.Query{
			Query: idx.NewTermQuery([]byte("foo"), []byte("bar")),
		}
		opts    = index.QueryOptions{}
		res     = index.QueryResult{}
		aggOpts = index.AggregationOptions{}
		aggRes  = index.AggregateQueryResult{}
		err     error
	)
	ctx.SetGoContext(opentracing.ContextWithSpan(stdlibctx.Background(), sp))
	ns.EXPECT().QueryIDs(gomock.Any(), q, opts).Return(res, nil)
	_, err = d.QueryIDs(ctx, ident.StringID("testns"), q, opts)
	require.NoError(t, err)

	ns.EXPECT().QueryIDs(gomock.Any(), q, opts).Return(res, fmt.Errorf("random err"))
	_, err = d.QueryIDs(ctx, ident.StringID("testns"), q, opts)
	require.Error(t, err)

	ns.EXPECT().AggregateQuery(gomock.Any(), q, aggOpts).Return(aggRes, nil)
	_, err = d.AggregateQuery(ctx, ident.StringID("testns"), q, aggOpts)
	require.NoError(t, err)

	ns.EXPECT().AggregateQuery(gomock.Any(), q, aggOpts).
		Return(aggRes, fmt.Errorf("random err"))
	_, err = d.AggregateQuery(ctx, ident.StringID("testns"), q, aggOpts)
	require.Error(t, err)

	ns.EXPECT().Close().Return(nil)

	// Ensure commitlog is set before closing because this will call commitlog.Close()
	d.commitLog = commitLog
	require.NoError(t, d.Close())

	sp.Finish()
	spans := mtr.FinishedSpans()
	spanStrs := make([]string, 0, len(spans))
	for _, s := range spans {
		spanStrs = append(spanStrs, s.OperationName)
	}
	exSpans := []string{
		tracepoint.DBQueryIDs,
		tracepoint.DBQueryIDs,
		tracepoint.DBAggregateQuery,
		tracepoint.DBAggregateQuery,
		"root",
	}

	assert.Equal(t, exSpans, spanStrs)
}

type wideQueryTestFn func(
	ctx context.Context, t *testing.T, ctrl *gomock.Controller,
	ns *MockdatabaseNamespace, d *db, q index.Query,
	now time.Time, shards []uint32, iterOpts index.IterationOptions,
)

func TestWideQuery(t *testing.T) {
	readMismatchTest := func(
		ctx context.Context, t *testing.T, ctrl *gomock.Controller,
		ns *MockdatabaseNamespace, d *db, q index.Query,
		now time.Time, shards []uint32, iterOpts index.IterationOptions) {
		ns.EXPECT().FetchWideEntry(gomock.Any(),
			ident.StringID("foo"), gomock.Any(), nil).
			Return(block.EmptyStreamedWideEntry, nil)

		_, err := d.WideQuery(ctx, ident.StringID("testns"), q, now, shards, iterOpts)
		require.NoError(t, err)

		_, err = d.WideQuery(ctx, ident.StringID("testns"), q, now, nil, iterOpts)
		require.Error(t, err)
	}

	exSpans := []string{
		tracepoint.DBWideQuery,
		tracepoint.DBWideQuery,
		"root",
	}

	testWideFunction(t, readMismatchTest, exSpans)
}

func testWideFunction(t *testing.T, testFn wideQueryTestFn, exSpans []string) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	d, mapCh, _ := defaultTestDatabase(t, ctrl, BootstrapNotStarted)
	defer func() {
		close(mapCh)
	}()

	commitLog := d.commitLog
	d.commitLog = nil
	ns := dbAddNewMockNamespace(ctrl, d, "testns")
	nsOptions := namespace.NewOptions()
	ns.EXPECT().Options().Return(nsOptions).AnyTimes()
	require.NoError(t, d.Open())

	ctx := context.NewContext()
	// create initial span from a mock tracer and get ctx
	mtr := mocktracer.New()
	sp := mtr.StartSpan("root")
	ctx.SetGoContext(opentracing.ContextWithSpan(stdlibctx.Background(), sp))

	var (
		q = index.Query{
			Query: idx.NewTermQuery([]byte("foo"), []byte("bar")),
		}

		now      = time.Now()
		start    = now.Truncate(2 * time.Hour)
		iterOpts = index.IterationOptions{}
		wideOpts = index.WideQueryOptions{
			StartInclusive:   start,
			EndExclusive:     now.Truncate(2 * time.Hour).Add(2 * time.Hour),
			IterationOptions: iterOpts,
			BatchSize:        1024,
		}
	)
	ctx.SetGoContext(opentracing.ContextWithSpan(stdlibctx.Background(), sp))

	shards := []uint32{1, 2, 3}
	ns.EXPECT().WideQueryIDs(gomock.Any(), q, gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			_ context.Context,
			_ index.Query,
			collector chan *ident.IDBatch,
			opts index.WideQueryOptions,
		) error {
			assert.Equal(t, opts.StartInclusive, wideOpts.StartInclusive)
			assert.Equal(t, opts.EndExclusive, wideOpts.EndExclusive)
			assert.Equal(t, opts.IterationOptions, wideOpts.IterationOptions)
			assert.Equal(t, opts.BatchSize, wideOpts.BatchSize)
			assert.Equal(t, opts.ShardsQueried, shards)
			go func() {
				batch := &ident.IDBatch{
					ShardIDs: []ident.ShardID{
						{ID: ident.StringID("foo")},
					},
				}
				batch.ReadyForProcessing()
				collector <- batch
				close(collector)
			}()
			return nil
		})

	ns.EXPECT().WideQueryIDs(gomock.Any(), q, gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("random err"))

	testFn(ctx, t, ctrl, ns, d, q, start, shards, iterOpts)
	ns.EXPECT().Close().Return(nil)
	// Ensure commitlog is set before closing because this will call commitlog.Close()
	d.commitLog = commitLog
	require.NoError(t, d.Close())

	sp.Finish()
	spans := mtr.FinishedSpans()
	spanStrs := make([]string, 0, len(spans))
	for _, s := range spans {
		spanStrs = append(spanStrs, s.OperationName)
	}

	assert.Equal(t, exSpans, spanStrs)
}

func TestDatabaseWriteBatchNoNamespace(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	d, mapCh, _ := defaultTestDatabase(t, ctrl, BootstrapNotStarted)
	defer func() {
		close(mapCh)
	}()
	require.NoError(t, d.Open())

	var (
		notExistNamespace = ident.StringID("not-exist-namespace")
		batchSize         = 100
	)
	_, err := d.BatchWriter(notExistNamespace, batchSize)
	require.Error(t, err)

	err = d.WriteBatch(context.NewContext(), notExistNamespace, nil, nil)
	require.Error(t, err)

	require.NoError(t, d.Close())
}

func TestDatabaseWriteTaggedBatchNoNamespace(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	d, mapCh, _ := defaultTestDatabase(t, ctrl, BootstrapNotStarted)
	defer func() {
		close(mapCh)
	}()
	require.NoError(t, d.Open())

	var (
		notExistNamespace = ident.StringID("not-exist-namespace")
		batchSize         = 100
	)
	_, err := d.BatchWriter(notExistNamespace, batchSize)
	require.Error(t, err)

	err = d.WriteTaggedBatch(context.NewContext(), notExistNamespace, nil, nil)
	require.Error(t, err)

	require.NoError(t, d.Close())
}

func TestDatabaseWrite(t *testing.T) {
	dbWriteTests := []struct {
		name                              string
		tagged, commitlogEnabled, skipAll bool
	}{
		{"batch", false, false, false},
		{"tagged batch", true, false, false},
		{"batch no commitlog", false, true, false},
		{"tagged batch no commitlog", true, true, false},
		{"batch skip all", false, false, true},
		{"tagged batch skip all", true, false, true},
		{"batch no commitlog skip all", false, true, true},
		{"tagged batch no commitlog skip all", true, true, true},
	}

	for _, tt := range dbWriteTests {
		t.Run(tt.name, func(t *testing.T) {
			testDatabaseWriteBatch(t, tt.tagged, tt.commitlogEnabled, tt.skipAll)
		})
	}
}

type fakeIndexedErrorHandler struct {
	errs []indexedErr
}

func (f *fakeIndexedErrorHandler) HandleError(index int, err error) {
	f.errs = append(f.errs, indexedErr{index, err})
}

type indexedErr struct {
	index int
	err   error
}

func testDatabaseWriteBatch(t *testing.T,
	tagged bool, commitlogEnabled bool, skipAll bool) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	d, mapCh, _ := defaultTestDatabase(t, ctrl, BootstrapNotStarted)
	defer func() {
		close(mapCh)
	}()

	commitLog := d.commitLog
	if !commitlogEnabled {
		// We don't mock the commitlog so set this to nil to ensure its
		// not being used as the test will panic if any methods are called
		// on it.
		d.commitLog = nil
	}

	ns := dbAddNewMockNamespace(ctrl, d, "testns")
	nsOptions := namespace.NewOptions().
		SetWritesToCommitLog(commitlogEnabled)

	ns.EXPECT().OwnedShards().Return([]databaseShard{}).AnyTimes()
	ns.EXPECT().Tick(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	ns.EXPECT().ShardBootstrapState().Return(ShardBootstrapStates{}).AnyTimes()
	ns.EXPECT().Options().Return(nsOptions).AnyTimes()
	ns.EXPECT().Close().Return(nil).Times(1)
	require.NoError(t, d.Open())

	var (
		namespace = ident.StringID("testns")
		ctx       = context.NewContext()
		tags      = ident.NewTags(ident.Tag{
			Name:  ident.StringID("foo"),
			Value: ident.StringID("bar"),
		}, ident.Tag{
			Name:  ident.StringID("baz"),
			Value: ident.StringID("qux"),
		})
		tagsIter = ident.NewTagsIterator(tags)
	)

	testTagEncodingPool := serialize.NewTagEncoderPool(serialize.NewTagEncoderOptions(),
		pool.NewObjectPoolOptions().SetSize(1))
	testTagEncodingPool.Init()
	encoder := testTagEncodingPool.Get()
	err := encoder.Encode(tagsIter)
	require.NoError(t, err)

	encodedTags, ok := encoder.Data()
	require.True(t, ok)

	testWrites := []struct {
		series string
		t      time.Time
		v      float64
		skip   bool
		err    error
	}{
		{
			series: "won't appear - always skipped",
			t:      time.Time{}.Add(0 * time.Second),
			skip:   true,
			v:      0.0,
		},
		{
			series: "foo",
			t:      time.Time{}.Add(10 * time.Second),
			skip:   skipAll,
			v:      1.0,
		},
		{
			series: "bar",
			t:      time.Time{}.Add(20 * time.Second),
			skip:   skipAll,
			v:      2.0,
		},
		{
			series: "baz",
			t:      time.Time{}.Add(20 * time.Second),
			skip:   skipAll,
			v:      3.0,
		},
		{
			series: "qux",
			t:      time.Time{}.Add(30 * time.Second),
			skip:   skipAll,
			v:      4.0,
		},
		{
			series: "won't appear - always skipped",
			t:      time.Time{}.Add(40 * time.Second),
			skip:   true,
			v:      5.0,
		},
		{
			series: "error-series",
			err:    errors.New("some-error"),
		},
	}

	batchWriter, err := d.BatchWriter(namespace, 10)
	require.NoError(t, err)

	var i int
	for _, write := range testWrites {
		// Write with the provided index as i*2 so we can assert later that the
		// ErrorHandler is called with the provided index, not the actual position
		// in the WriteBatch slice.
		if tagged {
			batchWriter.AddTagged(i*2, ident.StringID(write.series),
				tagsIter.Duplicate(), encodedTags.Bytes(), write.t, write.v, xtime.Second, nil)
			wasWritten := write.err == nil
			ns.EXPECT().
				WriteTagged(ctx, ident.NewIDMatcher(write.series), gomock.Any(),
					write.t, write.v, xtime.Second, nil).
				Return(SeriesWrite{
					Series: ts.Series{
						ID:        ident.StringID(write.series + "-updated"),
						Namespace: namespace,
					}, WasWritten: wasWritten,
				}, write.err)
		} else {
			batchWriter.Add(i*2, ident.StringID(write.series),
				write.t, write.v, xtime.Second, nil)
			wasWritten := write.err == nil
			ns.EXPECT().
				Write(ctx, ident.NewIDMatcher(write.series),
					write.t, write.v, xtime.Second, nil).
				Return(SeriesWrite{
					Series: ts.Series{
						ID:        ident.StringID(write.series + "-updated"),
						Namespace: namespace,
					},
					WasWritten: wasWritten,
				}, write.err)
		}
		i++
	}

	errHandler := &fakeIndexedErrorHandler{}
	if tagged {
		err = d.WriteTaggedBatch(ctx, namespace, batchWriter.(writes.WriteBatch),
			errHandler)
	} else {
		err = d.WriteBatch(ctx, namespace, batchWriter.(writes.WriteBatch),
			errHandler)
	}

	require.NoError(t, err)
	require.Len(t, errHandler.errs, 1)
	// Make sure it calls the error handler with the "original" provided index, not the position
	// of the write in the WriteBatch slice.
	require.Equal(t, (i-1)*2, errHandler.errs[0].index)

	// Ensure commitlog is set before closing because this will call commitlog.Close()
	d.commitLog = commitLog
	require.NoError(t, d.Close())
}

func TestDatabaseBootstrapState(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	d, mapCh, _ := defaultTestDatabase(t, ctrl, Bootstrapped)
	defer func() {
		close(mapCh)
	}()

	ns1 := dbAddNewMockNamespace(ctrl, d, "testns1")
	ns1.EXPECT().ShardBootstrapState().Return(ShardBootstrapStates{
		1: Bootstrapping,
	})
	ns2 := dbAddNewMockNamespace(ctrl, d, "testns2")
	ns2.EXPECT().ShardBootstrapState().Return(ShardBootstrapStates{
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

func TestDatabaseFlushState(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	d, mapCh, _ := defaultTestDatabase(t, ctrl, Bootstrapped)
	defer func() {
		close(mapCh)
	}()

	var (
		shardID            = uint32(0)
		blockStart         = time.Now().Truncate(2 * time.Hour)
		expectedFlushState = fileOpState{
			ColdVersionRetrievable: 2,
		}
		nsID = "testns1"
		ns   = dbAddNewMockNamespace(ctrl, d, nsID)
	)
	ns.EXPECT().FlushState(shardID, blockStart).Return(expectedFlushState, nil)

	flushState, err := d.FlushState(ident.StringID(nsID), shardID, blockStart)
	require.NoError(t, err)
	require.Equal(t, expectedFlushState, flushState)

	_, err = d.FlushState(ident.StringID("not-exist"), shardID, blockStart)
	require.Error(t, err)
}

func TestDatabaseIsBootstrapped(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	d, mapCh, _ := defaultTestDatabase(t, ctrl, Bootstrapped)
	defer func() {
		close(mapCh)
	}()

	mediator := NewMockdatabaseMediator(ctrl)
	mediator.EXPECT().IsBootstrapped().Return(true)
	mediator.EXPECT().IsBootstrapped().Return(false)
	d.mediator = mediator

	assert.True(t, d.IsBootstrapped())
	assert.False(t, d.IsBootstrapped())
}

func TestUpdateBatchWriterBasedOnShardResults(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	d, mapCh, _ := defaultTestDatabase(t, ctrl, BootstrapNotStarted)
	defer func() {
		close(mapCh)
	}()

	commitLog := d.commitLog
	d.commitLog = nil

	ns := dbAddNewMockNamespace(ctrl, d, "testns")
	nsOptions := namespace.NewOptions().
		SetWritesToCommitLog(false)
	ns.EXPECT().OwnedShards().Return([]databaseShard{}).AnyTimes()
	ns.EXPECT().Tick(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	ns.EXPECT().ShardBootstrapState().Return(ShardBootstrapStates{}).AnyTimes()
	ns.EXPECT().Options().Return(nsOptions).AnyTimes()
	ns.EXPECT().Close().Return(nil).Times(1)
	require.NoError(t, d.Open())

	var (
		namespace    = ident.StringID("testns")
		ctx          = context.NewContext()
		seriesWrite1 = SeriesWrite{Series: ts.Series{UniqueIndex: 0}, WasWritten: true}
		seriesWrite2 = SeriesWrite{Series: ts.Series{UniqueIndex: 1}, WasWritten: true}
		seriesWrite3 = SeriesWrite{Series: ts.Series{UniqueIndex: 2}, WasWritten: false}
		seriesWrite4 = SeriesWrite{Series: ts.Series{UniqueIndex: 3}, WasWritten: false}
		err          = fmt.Errorf("err")
	)

	gomock.InOrder(
		ns.EXPECT().
			Write(ctx, gomock.Any(), gomock.Any(), gomock.Any(),
				gomock.Any(), gomock.Any()).
			Return(seriesWrite1, nil),
		ns.EXPECT().
			Write(ctx, gomock.Any(), gomock.Any(), gomock.Any(),
				gomock.Any(), gomock.Any()).
			Return(seriesWrite2, err),
		ns.EXPECT().
			Write(ctx, gomock.Any(), gomock.Any(), gomock.Any(),
				gomock.Any(), gomock.Any()).
			Return(seriesWrite3, err),
		ns.EXPECT().
			Write(ctx, gomock.Any(), gomock.Any(), gomock.Any(),
				gomock.Any(), gomock.Any()).
			Return(seriesWrite4, nil),
	)

	write := writes.Write{
		Series: ts.Series{ID: ident.StringID("foo")},
	}

	iters := []writes.BatchWrite{
		{Write: write},
		{Write: write},
		{Write: write},
		{Write: write},
	}

	batchWriter := writes.NewMockWriteBatch(ctrl)
	gomock.InOrder(
		batchWriter.EXPECT().Iter().Return(iters),
		batchWriter.EXPECT().SetSeries(0, seriesWrite1.Series),
		batchWriter.EXPECT().SetError(1, err),
		batchWriter.EXPECT().SetError(2, err),
		batchWriter.EXPECT().SetSeries(3, seriesWrite4.Series),
		batchWriter.EXPECT().SetSkipWrite(3),
		batchWriter.EXPECT().PendingIndex().Return(nil),
		batchWriter.EXPECT().Finalize(),
	)

	errHandler := &fakeIndexedErrorHandler{}
	d.WriteBatch(ctx, namespace, batchWriter, errHandler)
	require.Equal(t, 2, len(errHandler.errs))
	require.Equal(t, err, errHandler.errs[0].err)
	require.Equal(t, err, errHandler.errs[1].err)
	d.commitLog = commitLog
	require.NoError(t, d.Close())
}

func TestDatabaseIsOverloaded(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	d, mapCh, _ := defaultTestDatabase(t, ctrl, BootstrapNotStarted)
	defer func() {
		close(mapCh)
	}()

	d.opts = d.opts.SetCommitLogOptions(
		d.opts.CommitLogOptions().SetBacklogQueueSize(100),
	)

	mockCL := commitlog.NewMockCommitLog(ctrl)
	d.commitLog = mockCL

	mockCL.EXPECT().QueueLength().Return(int64(89))
	require.Equal(t, false, d.IsOverloaded())

	mockCL.EXPECT().QueueLength().Return(int64(90))
	require.Equal(t, true, d.IsOverloaded())
}

func TestDatabaseAggregateTiles(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	d, mapCh, _ := defaultTestDatabase(t, ctrl, Bootstrapped)
	defer func() {
		close(mapCh)
	}()

	var (
		sourceNsID = ident.StringID("source")
		targetNsID = ident.StringID("target")
		start      = time.Now().Truncate(time.Hour)
	)

	opts, err := NewAggregateTilesOptions(start, start.Add(-time.Second), time.Minute, targetNsID, d.opts.InstrumentOptions())
	require.Error(t, err)
	opts.InsOptions = d.opts.InstrumentOptions()

	sourceNs := dbAddNewMockNamespace(ctrl, d, sourceNsID.String())
	targetNs := dbAddNewMockNamespace(ctrl, d, targetNsID.String())
	targetNs.EXPECT().AggregateTiles(ctx, sourceNs, opts).Return(int64(4), nil)

	processedTileCount, err := d.AggregateTiles(ctx, sourceNsID, targetNsID, opts)
	require.NoError(t, err)
	assert.Equal(t, int64(4), processedTileCount)
}

func TestNewAggregateTilesOptions(t *testing.T) {
	start := time.Now().Truncate(time.Hour)
	targetNs := ident.StringID("target")
	insOpts := instrument.NewOptions()

	_, err := NewAggregateTilesOptions(start, start.Add(-time.Second), time.Minute, targetNs, insOpts)
	assert.Error(t, err)

	_, err = NewAggregateTilesOptions(start, start, time.Minute, targetNs, insOpts)
	assert.Error(t, err)

	_, err = NewAggregateTilesOptions(start, start.Add(time.Second), -time.Minute, targetNs, insOpts)
	assert.Error(t, err)

	_, err = NewAggregateTilesOptions(start, start.Add(time.Second), 0, targetNs, insOpts)
	assert.Error(t, err)

	_, err = NewAggregateTilesOptions(start, start.Add(time.Second), time.Minute, targetNs, insOpts)
	assert.NoError(t, err)
}
