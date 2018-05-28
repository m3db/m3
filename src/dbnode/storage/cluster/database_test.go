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

package cluster

import (
	"fmt"
	"sync"
	"testing"

	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3db/src/dbnode/sharding"
	"github.com/m3db/m3db/src/dbnode/storage"
	"github.com/m3db/m3db/src/dbnode/topology"
	"github.com/m3db/m3db/src/dbnode/topology/testutil"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestDatabase(
	t *testing.T,
	hostid string,
	topoInit topology.Initializer,
) (Database, error) {
	opts := storage.NewOptions()
	return NewDatabase(hostid, topoInit, opts)
}

func TestDatabaseOpenClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStorageDB, restore := mockNewStorageDatabase(ctrl)
	defer restore()

	viewsCh := make(chan testutil.TopologyView, 64)
	defer close(viewsCh)

	viewsCh <- testutil.NewTopologyView(1, map[string][]shard.Shard{
		"testhost": sharding.NewShards([]uint32{0, 1, 2}, shard.Available),
	})

	topoInit, _ := newMockTopoInit(t, ctrl, viewsCh)

	db, err := newTestDatabase(t, "testhost", topoInit)
	require.NoError(t, err)

	mockStorageDB.EXPECT().Open().Return(nil)
	err = db.Open()
	require.NoError(t, err)

	mockStorageDB.EXPECT().Close().Return(nil)
	err = db.Close()
	require.NoError(t, err)
}

func TestDatabaseMarksShardAsAvailableOnReshard(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStorageDB, restore := mockNewStorageDatabase(ctrl)
	defer restore()

	viewsCh := make(chan testutil.TopologyView, 64)
	defer close(viewsCh)

	viewsCh <- testutil.NewTopologyView(1, map[string][]shard.Shard{
		"testhost0": sharding.NewShards([]uint32{0, 1}, shard.Available),
		"testhost1": sharding.NewShards([]uint32{2, 3}, shard.Available),
	})

	topoInit, props := newMockTopoInit(t, ctrl, viewsCh)

	db, err := newTestDatabase(t, "testhost0", topoInit)
	require.NoError(t, err)

	// Now open the cluster database
	mockStorageDB.EXPECT().Open().Return(nil)
	err = db.Open()
	require.NoError(t, err)

	// Reshard by taking a leaving host's shards
	updatedView := map[string][]shard.Shard{
		"testhost0": append(sharding.NewShards([]uint32{0, 1}, shard.Available),
			sharding.NewShards([]uint32{2, 3}, shard.Initializing)...),
		"testhost1": sharding.NewShards([]uint32{2, 3}, shard.Leaving),
	}

	// Expect the assign shards call
	mockStorageDB.EXPECT().AssignShardSet(gomock.Any()).Do(
		func(shardSet sharding.ShardSet) {
			// Ensure updated shard set is as expected
			assert.Equal(t, 4, len(shardSet.AllIDs()))
			values := updatedView["testhost0"]
			hostShardSet, _ := sharding.NewShardSet(values, shardSet.HashFn())
			assert.Equal(t, hostShardSet.AllIDs(), shardSet.AllIDs())
		})

	// Expect the namespaces query from report shard state background query
	mockShards := []*storage.MockShard{
		storage.NewMockShard(ctrl),
		storage.NewMockShard(ctrl),
		storage.NewMockShard(ctrl),
		storage.NewMockShard(ctrl),
	}
	for i, s := range mockShards {
		s.EXPECT().ID().Return(uint32(i)).AnyTimes()
	}
	mockShards[2].EXPECT().IsBootstrapped().Return(true)
	mockShards[3].EXPECT().IsBootstrapped().Return(true)

	var expectShards []storage.Shard
	for _, s := range mockShards {
		expectShards = append(expectShards, s)
	}

	mockNamespace := storage.NewMockNamespace(ctrl)
	mockNamespace.EXPECT().Shards().Return(expectShards)

	expectNamespaces := []storage.Namespace{mockNamespace}
	mockStorageDB.EXPECT().Namespaces().Return(expectNamespaces)

	var wg sync.WaitGroup
	wg.Add(1)
	props.topology.EXPECT().
		MarkShardsAvailable("testhost0", uint32(2), uint32(3)).
		Do(func(hostID string, shardIDs ...uint32) {
			wg.Done()
		})

	// Enqueue the update
	viewsCh <- testutil.NewTopologyView(1, updatedView)

	// Wait for the update to propagate, consume the first notification
	// from the initial read and then the second that should come after
	// enqueing the view just prior to this read
	for i := 0; i < 2; i++ {
		<-props.propogateViewsCh
	}

	// Wait for shards to be marked available
	wg.Wait()

	mockStorageDB.EXPECT().Close().Return(nil)
	err = db.Close()
	require.NoError(t, err)
}

func TestDatabaseOpenUpdatesShardSetBeforeOpen(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStorageDB, restore := mockNewStorageDatabase(ctrl)
	defer restore()

	viewsCh := make(chan testutil.TopologyView, 64)
	defer close(viewsCh)

	viewsCh <- testutil.NewTopologyView(1, map[string][]shard.Shard{
		"testhost0": append(sharding.NewShards([]uint32{0, 1}, shard.Available),
			sharding.NewShards([]uint32{2, 3}, shard.Leaving)...),
		"testhost1": sharding.NewShards([]uint32{2, 3}, shard.Initializing),
	})

	topoInit, props := newMockTopoInit(t, ctrl, viewsCh)

	db, err := newTestDatabase(t, "testhost0", topoInit)
	require.NoError(t, err)

	updatedView := map[string][]shard.Shard{
		"testhost0": sharding.NewShards([]uint32{0, 1}, shard.Available),
		"testhost1": sharding.NewShards([]uint32{2, 3}, shard.Available),
	}

	// Expect the assign shards call before open
	mockStorageDB.EXPECT().AssignShardSet(gomock.Any()).Do(
		func(shardSet sharding.ShardSet) {
			// Ensure updated shard set is as expected
			assert.Equal(t, 2, len(shardSet.AllIDs()))
			values := updatedView["testhost0"]
			hostShardSet, _ := sharding.NewShardSet(values, shardSet.HashFn())
			assert.Equal(t, hostShardSet.AllIDs(), shardSet.AllIDs())
			// Now we can expect an open call
			mockStorageDB.EXPECT().Open().Return(nil)
		})

	// Enqueue the update
	viewsCh <- testutil.NewTopologyView(1, updatedView)

	// Wait for the update to propagate, consume the first notification
	// from the initial read and then the second that should come after
	// enqueing the view just prior to this read
	for i := 0; i < 2; i++ {
		<-props.propogateViewsCh
	}

	// Now open the cluster database
	err = db.Open()
	require.NoError(t, err)

	mockStorageDB.EXPECT().Close().Return(nil)
	err = db.Close()
	require.NoError(t, err)
}

func TestDatabaseEmptyShardSet(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	asserted := false
	defer func() {
		assert.True(t, asserted)
	}()
	restore := setNewStorageDatabase(func(
		shardSet sharding.ShardSet,
		opts storage.Options,
	) (storage.Database, error) {
		assert.Equal(t, 0, len(shardSet.AllIDs()))
		asserted = true
		return nil, nil
	})
	defer restore()

	viewsCh := make(chan testutil.TopologyView, 64)
	defer close(viewsCh)

	viewsCh <- testutil.NewTopologyView(1, map[string][]shard.Shard{
		"testhost0": sharding.NewShards([]uint32{0}, shard.Available),
		"testhost1": sharding.NewShards([]uint32{1}, shard.Available),
		"testhost2": sharding.NewShards([]uint32{2}, shard.Available),
	})

	topoInit, _ := newMockTopoInit(t, ctrl, viewsCh)

	_, err := newTestDatabase(t, "testhost_not_in_placement", topoInit)
	require.NoError(t, err)
}

func TestDatabaseOpenTwiceError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStorageDB, restore := mockNewStorageDatabase(ctrl)
	defer restore()

	viewsCh := make(chan testutil.TopologyView, 64)
	defer close(viewsCh)

	viewsCh <- testutil.NewTopologyView(1, map[string][]shard.Shard{
		"testhost": sharding.NewShards([]uint32{0, 1, 2}, shard.Available),
	})

	topoInit, _ := newMockTopoInit(t, ctrl, viewsCh)

	db, err := newTestDatabase(t, "testhost", topoInit)
	require.NoError(t, err)

	mockStorageDB.EXPECT().Open().Return(nil).AnyTimes()

	err = db.Open()
	require.NoError(t, err)

	err = db.Open()
	require.Error(t, err)
	assert.Equal(t, errAlreadyWatchingTopology, err)

	mockStorageDB.EXPECT().Close().Return(nil)
	err = db.Close()
	require.NoError(t, err)
}

func TestDatabaseCloseTwiceError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStorageDB, restore := mockNewStorageDatabase(ctrl)
	defer restore()

	viewsCh := make(chan testutil.TopologyView, 64)
	defer close(viewsCh)

	viewsCh <- testutil.NewTopologyView(1, map[string][]shard.Shard{
		"testhost": sharding.NewShards([]uint32{0, 1, 2}, shard.Available),
	})

	topoInit, _ := newMockTopoInit(t, ctrl, viewsCh)

	db, err := newTestDatabase(t, "testhost", topoInit)
	require.NoError(t, err)

	mockStorageDB.EXPECT().Open().Return(nil)

	err = db.Open()
	require.NoError(t, err)

	mockStorageDB.EXPECT().Close().Return(nil).AnyTimes()
	err = db.Close()
	require.NoError(t, err)

	err = db.Close()
	require.Error(t, err)
	assert.Equal(t, errNotWatchingTopology, err)
}

func TestDatabaseOpenCanRetry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStorageDB, restore := mockNewStorageDatabase(ctrl)
	defer restore()

	viewsCh := make(chan testutil.TopologyView, 64)
	defer close(viewsCh)

	viewsCh <- testutil.NewTopologyView(1, map[string][]shard.Shard{
		"testhost": sharding.NewShards([]uint32{0, 1, 2}, shard.Available),
	})

	topoInit, _ := newMockTopoInit(t, ctrl, viewsCh)

	db, err := newTestDatabase(t, "testhost", topoInit)
	require.NoError(t, err)

	expectedErr := fmt.Errorf("an error")
	mockStorageDB.EXPECT().Open().Return(expectedErr)

	err = db.Open()
	require.Error(t, err)
	assert.Equal(t, expectedErr, err)

	mockStorageDB.EXPECT().Open().Return(nil)

	err = db.Open()
	require.NoError(t, err)

	mockStorageDB.EXPECT().Close().Return(nil)
	err = db.Close()
	require.NoError(t, err)
}

func TestDatabaseCloseCanRetry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStorageDB, restore := mockNewStorageDatabase(ctrl)
	defer restore()

	viewsCh := make(chan testutil.TopologyView, 64)
	defer close(viewsCh)

	viewsCh <- testutil.NewTopologyView(1, map[string][]shard.Shard{
		"testhost": sharding.NewShards([]uint32{0, 1, 2}, shard.Available),
	})

	topoInit, _ := newMockTopoInit(t, ctrl, viewsCh)

	db, err := newTestDatabase(t, "testhost", topoInit)
	require.NoError(t, err)

	mockStorageDB.EXPECT().Open().Return(nil)

	err = db.Open()
	require.NoError(t, err)

	expectedErr := fmt.Errorf("an error")
	mockStorageDB.EXPECT().Close().Return(expectedErr)
	err = db.Close()
	require.Error(t, err)
	assert.Equal(t, expectedErr, err)

	mockStorageDB.EXPECT().Close().Return(nil)
	err = db.Close()
	require.NoError(t, err)
}
