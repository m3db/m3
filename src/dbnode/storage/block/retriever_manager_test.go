// Copyright (c) 2024 Uber Technologies, Inc.
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

package block

import (
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

func TestBlockRetrieverManager(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create test data
	nsID := ident.StringID("test")
	md := namespace.NewMockMetadata(ctrl)
	md.EXPECT().ID().Return(nsID).AnyTimes()

	shardSet := sharding.NewMockShardSet(ctrl)
	retriever := NewMockDatabaseBlockRetriever(ctrl)

	// Test new retriever creation
	newRetrieverFn := func(md namespace.Metadata, shardSet sharding.ShardSet) (DatabaseBlockRetriever, error) {
		return retriever, nil
	}

	manager := NewDatabaseBlockRetrieverManager(newRetrieverFn)

	// Test getting retriever for the first time
	r, err := manager.Retriever(md, shardSet)
	require.NoError(t, err)
	require.Equal(t, retriever, r)

	// Test getting cached retriever
	r, err = manager.Retriever(md, shardSet)
	require.NoError(t, err)
	require.Equal(t, retriever, r)

	// Test error case
	expectedErr := errors.New("test error")
	newRetrieverFn = func(md namespace.Metadata, shardSet sharding.ShardSet) (DatabaseBlockRetriever, error) {
		return nil, expectedErr
	}

	manager = NewDatabaseBlockRetrieverManager(newRetrieverFn)
	_, err = manager.Retriever(md, shardSet)
	require.Equal(t, expectedErr, err)
}

func TestShardBlockRetriever(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create test data
	shard := uint32(1)
	retriever := NewMockDatabaseBlockRetriever(ctrl)
	shardRetriever := NewDatabaseShardBlockRetriever(shard, retriever)

	// Test Stream method
	ctx := context.NewBackground()
	id := ident.StringID("test")
	blockStart := xtime.Now()
	testNs := ident.StringID("test-ns")
	onRetrieve := NewMockOnRetrieveBlock(ctrl)
	mockSchemaRegistry := namespace.NewMockSchemaRegistry(ctrl)
	mockSchema := namespace.NewMockSchemaDescr(ctrl)
	mockSchemaRegistry.EXPECT().GetLatestSchema(testNs).Return(mockSchema, nil)
	nsCtx := namespace.NewContextFor(testNs, mockSchemaRegistry)
	reader := xio.EmptyBlockReader

	retriever.EXPECT().
		Stream(ctx, shard, id, blockStart, onRetrieve, nsCtx).
		Return(reader, nil)

	r, err := shardRetriever.Stream(ctx, id, blockStart, onRetrieve, nsCtx)
	require.NoError(t, err)
	require.Equal(t, reader, r)
}

func TestShardBlockRetrieverManager(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create test data
	retriever := NewMockDatabaseBlockRetriever(ctrl)
	manager := NewDatabaseShardBlockRetrieverManager(retriever)

	// Test getting shard retriever for the first time
	shard := uint32(1)
	r := manager.ShardRetriever(shard)
	require.NotNil(t, r)

	// Test getting cached shard retriever
	r2 := manager.ShardRetriever(shard)
	require.Equal(t, r, r2)

	// Test getting different shard retriever
	shard2 := uint32(2)
	r3 := manager.ShardRetriever(shard2)
	require.NotNil(t, r3)
	require.NotEqual(t, r, r3)

	// Test concurrent access
	shards := []uint32{3, 4, 5}
	for _, s := range shards {
		go func(shard uint32) {
			r := manager.ShardRetriever(shard)
			require.NotNil(t, r)
		}(s)
	}
}

func TestShardBlockRetrieverManagerConcurrentAccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	retriever := NewMockDatabaseBlockRetriever(ctrl)
	manager := NewDatabaseShardBlockRetrieverManager(retriever)

	// Test concurrent access to the same shard
	shard := uint32(1)
	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func() {
			r := manager.ShardRetriever(shard)
			require.NotNil(t, r)
			done <- struct{}{}
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestShardBlockRetrieverManagerEdgeCases(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	retriever := NewMockDatabaseBlockRetriever(ctrl)
	manager := NewDatabaseShardBlockRetrieverManager(retriever)

	// Test with zero shard
	r := manager.ShardRetriever(0)
	require.NotNil(t, r)

	// Test with max uint32 shard
	r = manager.ShardRetriever(^uint32(0))
	require.NotNil(t, r)
}
