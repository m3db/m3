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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/storage"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatabaseOpenClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStorageDB, restore := mockNewStorageDatabase(ctrl)
	defer restore()

	viewsCh := make(chan topoView, 64)
	defer close(viewsCh)

	viewsCh <- newTopoView(1, map[string][]uint32{
		"testhost": []uint32{0, 1, 2},
	})

	topoInit, _ := newMockTopoInit(ctrl, viewsCh)

	db, err := NewDatabase(testNamespaces, "testhost",
		topoInit, storage.NewOptions())
	require.NoError(t, err)

	mockStorageDB.EXPECT().Open().Return(nil)
	err = db.Open()
	require.NoError(t, err)

	mockStorageDB.EXPECT().Close().Return(nil)
	err = db.Close()
	require.NoError(t, err)
}

func TestDatabaseOpenTwiceError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStorageDB, restore := mockNewStorageDatabase(ctrl)
	defer restore()

	viewsCh := make(chan topoView, 64)
	defer close(viewsCh)

	viewsCh <- newTopoView(1, map[string][]uint32{
		"testhost": []uint32{0, 1, 2},
	})

	topoInit, _ := newMockTopoInit(ctrl, viewsCh)

	db, err := NewDatabase(testNamespaces, "testhost",
		topoInit, storage.NewOptions())
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

	viewsCh := make(chan topoView, 64)
	defer close(viewsCh)

	viewsCh <- newTopoView(1, map[string][]uint32{
		"testhost": []uint32{0, 1, 2},
	})

	topoInit, _ := newMockTopoInit(ctrl, viewsCh)

	db, err := NewDatabase(testNamespaces, "testhost",
		topoInit, storage.NewOptions())
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

func TestDatabaseOpenUpdatesShardSetBeforeOpen(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStorageDB, restore := mockNewStorageDatabase(ctrl)
	defer restore()

	viewsCh := make(chan topoView, 64)
	defer close(viewsCh)

	viewsCh <- newTopoView(1, map[string][]uint32{
		"testhost0": []uint32{0, 1, 2, 3},
	})

	topoInit, propogateViewsCh := newMockTopoInit(ctrl, viewsCh)

	db, err := NewDatabase(testNamespaces, "testhost0",
		topoInit, storage.NewOptions())
	require.NoError(t, err)

	updatedView := map[string][]uint32{
		"testhost0": []uint32{0, 1},
		"testhost1": []uint32{2, 3},
	}

	// Expect the assign shards call before open
	mockStorageDB.EXPECT().AssignShardSet(gomock.Any()).Do(
		func(shardSet sharding.ShardSet) {
			// Ensure updated shard set is as expected
			assert.Equal(t, 2, len(shardSet.Shards()))
			assert.Equal(t, updatedView["testhost0"], shardSet.Shards())
			// Now we can expect an open call
			mockStorageDB.EXPECT().Open().Return(nil)
		})

	// Enqueue the update
	viewsCh <- newTopoView(1, updatedView)

	// Wait for the update to propogate, consume the first notification
	// from the initial read and then the second that should come after
	// enqueing the view just prior to this read
	for i := 0; i < 2; i++ {
		<-propogateViewsCh
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
		namespaces []namespace.Metadata,
		shardSet sharding.ShardSet,
		opts storage.Options,
	) (storage.Database, error) {
		assert.Equal(t, 0, len(shardSet.Shards()))
		asserted = true
		return nil, nil
	})
	defer restore()

	viewsCh := make(chan topoView, 64)
	defer close(viewsCh)

	viewsCh <- newTopoView(1, map[string][]uint32{
		"testhost0": []uint32{0},
		"testhost1": []uint32{1},
		"testhost2": []uint32{2},
	})

	topoInit, _ := newMockTopoInit(ctrl, viewsCh)

	_, err := NewDatabase(testNamespaces, "testhost_not_in_placement",
		topoInit, storage.NewOptions())
	require.NoError(t, err)
}
