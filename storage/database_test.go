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
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/repair"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3db/x/io"
	"github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockDatabase struct {
	opts       Options
	namespaces map[string]databaseNamespace
	bs         bootstrapState
}

func newMockDatabase() *mockDatabase { return &mockDatabase{opts: testDatabaseOptions()} }

func (d *mockDatabase) Options() Options                          { return d.opts }
func (d *mockDatabase) AssignShardSet(shardSet sharding.ShardSet) {}
func (d *mockDatabase) Namespaces() []Namespace                   { return nil }
func (d *mockDatabase) Open() error                               { return nil }
func (d *mockDatabase) Close() error                              { return nil }
func (d *mockDatabase) Bootstrap() error                          { return nil }
func (d *mockDatabase) IsBootstrapped() bool                      { return d.bs == bootstrapped }
func (d *mockDatabase) IsOverloaded() bool                        { return false }
func (d *mockDatabase) Repair() error                             { return nil }
func (d *mockDatabase) Truncate(namespace ts.ID) (int64, error)   { return 0, nil }
func (d *mockDatabase) flush(t time.Time, async bool)             {}

func (d *mockDatabase) getOwnedNamespaces() []databaseNamespace {
	namespaces := make([]databaseNamespace, 0, len(d.namespaces))
	for _, n := range d.namespaces {
		namespaces = append(namespaces, n)
	}
	return namespaces
}

func (d *mockDatabase) Write(context.Context, ts.ID, ts.ID, time.Time, float64, xtime.Unit, []byte) error {
	return nil
}

func (d *mockDatabase) ReadEncoded(context.Context, ts.ID, ts.ID, time.Time, time.Time) ([][]xio.SegmentReader, error) {
	return nil, nil
}

func (d *mockDatabase) FetchBlocks(context.Context, ts.ID, uint32, ts.ID, []time.Time) ([]block.FetchBlockResult, error) {
	return nil, nil
}

func (d *mockDatabase) FetchBlocksMetadata(context.Context, ts.ID, uint32, time.Time, time.Time, int64, int64, bool, bool) (block.FetchBlocksMetadataResults, *int64, error) {
	return nil, nil, nil
}

var defaultTestDatabaseOptions = NewOptions().
	SetMaxFlushRetries(3).
	SetFileOpOptions(NewFileOpOptions().SetJitter(0)).
	SetRetentionOptions(retention.NewOptions().
		SetBufferFuture(10 * time.Minute).
		SetBufferPast(10 * time.Minute).
		SetBufferDrain(10 * time.Minute).
		SetBlockSize(2 * time.Hour).
		SetRetentionPeriod(2 * 24 * time.Hour))

func testDatabaseOptions() Options {
	// NB(r): We don't need to recreate the options multiple
	// times as they are immutable - we save considerable
	// memory by doing this avoiding creating default pools
	// several times.
	return defaultTestDatabaseOptions
}

func newTestDatabase(t *testing.T, bs bootstrapState) *db {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testDatabaseOptions()
	opts = opts.SetRepairOptions(repair.NewOptions().
		SetAdminClient(client.NewMockAdminClient(ctrl)).
		SetRepairInterval(time.Duration(0)).
		SetRepairTimeOffset(time.Duration(0)).
		SetRepairTimeJitter(time.Duration(0)).
		SetRepairCheckInterval(time.Duration(0)))

	database, err := NewDatabase(nil, nil, opts)
	require.NoError(t, err)
	d := database.(*db)
	m := d.mediator.(*mediator)
	bsm := newBootstrapManager(d, m, opts).(*bootstrapManager)
	bsm.state = bs
	m.databaseBootstrapManager = bsm
	return d
}

func dbAddNewMockNamespace(
	ctrl *gomock.Controller,
	d *db,
	id string,
) *MockdatabaseNamespace {
	ns := ts.StringID(id)
	mockNamespace := NewMockdatabaseNamespace(ctrl)
	mockNamespace.EXPECT().ID().Return(ns).AnyTimes()
	d.namespaces[ns.Hash()] = mockNamespace
	return mockNamespace
}

func TestDatabaseOpen(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	d := newTestDatabase(t, bootstrapNotStarted)
	require.NoError(t, d.Open())
	require.Equal(t, errDatabaseAlreadyOpen, d.Open())
	require.NoError(t, d.Close())
}

func TestDatabaseClose(t *testing.T) {
	d := newTestDatabase(t, bootstrapped)

	require.NoError(t, d.Open())
	require.NoError(t, d.Close())
	require.Equal(t, errDatabaseAlreadyClosed, d.Close())
}

func TestDatabaseReadEncodedNamespaceNotOwned(t *testing.T) {
	ctx := context.NewContext()
	defer ctx.Close()

	d := newTestDatabase(t, bootstrapped)
	_, err := d.ReadEncoded(ctx, ts.StringID("nonexistent"), ts.StringID("foo"), time.Now(), time.Now())
	require.Equal(t, "no such namespace nonexistent", err.Error())
}

func TestDatabaseReadEncodedNamespaceOwned(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	d := newTestDatabase(t, bootstrapped)
	ns := ts.StringID("testns1")
	id := ts.StringID("bar")
	end := time.Now()
	start := end.Add(-time.Hour)
	mockNamespace := NewMockdatabaseNamespace(ctrl)
	mockNamespace.EXPECT().ReadEncoded(ctx, id, start, end).Return(nil, nil)
	d.namespaces[ns.Hash()] = mockNamespace

	res, err := d.ReadEncoded(ctx, ns, id, start, end)
	require.Nil(t, res)
	require.Nil(t, err)
}

func TestDatabaseFetchBlocksNamespaceNotOwned(t *testing.T) {
	ctx := context.NewContext()
	defer ctx.Close()

	d := newTestDatabase(t, bootstrapped)
	now := time.Now()
	starts := []time.Time{now, now.Add(time.Second), now.Add(-time.Second)}
	res, err := d.FetchBlocks(ctx, ts.StringID("testns1"), 0, ts.StringID("foo"), starts)
	require.Nil(t, res)
	require.True(t, xerrors.IsInvalidParams(err))
}

func TestDatabaseFetchBlocksNamespaceOwned(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	d := newTestDatabase(t, bootstrapped)
	ns := ts.StringID("testns1")
	id := ts.StringID("bar")
	shardID := uint32(0)
	now := time.Now()
	starts := []time.Time{now, now.Add(time.Second), now.Add(-time.Second)}
	expected := []block.FetchBlockResult{block.NewFetchBlockResult(starts[0], nil, nil, nil)}
	mockNamespace := NewMockdatabaseNamespace(ctrl)
	mockNamespace.EXPECT().FetchBlocks(ctx, shardID, id, starts).Return(expected, nil)
	d.namespaces[ns.Hash()] = mockNamespace

	res, err := d.FetchBlocks(ctx, ns, shardID, id, starts)
	require.Equal(t, expected, res)
	require.NoError(t, err)
}

func TestDatabaseFetchBlocksMetadataShardNotOwned(t *testing.T) {
	ctx := context.NewContext()
	defer ctx.Close()

	var (
		ns               = ts.StringID("testns1")
		shardID          = uint32(0)
		start            = time.Now()
		end              = start.Add(time.Hour)
		limit            = int64(100)
		pageToken        = int64(0)
		includeSizes     = true
		includeChecksums = true
	)
	d := newTestDatabase(t, bootstrapped)
	res, nextPageToken, err := d.FetchBlocksMetadata(ctx, ns, shardID, start, end, limit, pageToken, includeSizes, includeChecksums)
	require.Nil(t, res)
	require.Nil(t, nextPageToken)
	require.Error(t, err)
}

func TestDatabaseFetchBlocksMetadataShardOwned(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	var (
		ns               = ts.StringID("testns1")
		shardID          = uint32(397)
		start            = time.Now()
		end              = start.Add(time.Hour)
		limit            = int64(100)
		pageToken        = int64(0)
		includeSizes     = true
		includeChecksums = true
	)

	d := newTestDatabase(t, bootstrapped)
	expectedBlocks := block.NewFetchBlocksMetadataResults()
	expectedBlocks.Add(block.NewFetchBlocksMetadataResult(ts.StringID("bar"), nil))
	expectedToken := new(int64)
	mockNamespace := NewMockdatabaseNamespace(ctrl)
	mockNamespace.
		EXPECT().
		FetchBlocksMetadata(ctx, shardID, start, end, limit, pageToken, includeSizes, includeChecksums).
		Return(expectedBlocks, expectedToken, nil)
	d.namespaces[ns.Hash()] = mockNamespace

	res, nextToken, err := d.FetchBlocksMetadata(ctx, ns, shardID, start, end, limit, pageToken, includeSizes, includeChecksums)
	require.Equal(t, expectedBlocks, res)
	require.Equal(t, expectedToken, nextToken)
	require.Nil(t, err)
}

func TestDatabaseNamespaces(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	d := newTestDatabase(t, bootstrapped)

	var ns []*MockdatabaseNamespace
	ns = append(ns, dbAddNewMockNamespace(ctrl, d, "testns1"))
	ns = append(ns, dbAddNewMockNamespace(ctrl, d, "testns2"))

	result := d.Namespaces()
	require.Equal(t, 2, len(result))

	sort.Sort(NamespacesByID(result))
	assert.Equal(t, "testns1", result[0].ID().String())
	assert.Equal(t, "testns2", result[1].ID().String())
}

func TestDatabaseAssignShardSet(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	d := newTestDatabase(t, bootstrapped)

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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	d := newTestDatabase(t, bootstrapped)
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
