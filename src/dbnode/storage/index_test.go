// Copyright (c) 2018 Uber Technologies, Inc.
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
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	xtest "github.com/m3db/m3x/test"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNamespaceIndexCleanupExpiredFilesets(t *testing.T) {
	md := testNamespaceMetadata(time.Hour, time.Hour*8)
	nsIdx, err := newNamespaceIndex(md, testDatabaseOptions())
	require.NoError(t, err)

	now := time.Now().Truncate(time.Hour)
	idx := nsIdx.(*nsIndex)

	oldestTime := now.Add(-time.Hour * 8)
	files := []string{"abc"}

	idx.indexFilesetsBeforeFn = func(dir string, nsID ident.ID, exclusiveTime time.Time) ([]string, error) {
		require.True(t, oldestTime.Equal(exclusiveTime), fmt.Sprintf("%v %v", exclusiveTime, oldestTime))
		return files, nil
	}
	idx.deleteFilesFn = func(s []string) error {
		require.Equal(t, files, s)
		return nil
	}
	require.NoError(t, idx.CleanupExpiredFileSets(now))
}

func TestNamespaceIndexCleanupExpiredFilesetsWithBlocks(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	md := testNamespaceMetadata(time.Hour, time.Hour*8)
	nsIdx, err := newNamespaceIndex(md, testDatabaseOptions())
	require.NoError(t, err)

	defer func() {
		require.NoError(t, nsIdx.Close())
	}()

	now := time.Now().Truncate(time.Hour)
	idx := nsIdx.(*nsIndex)

	mockBlock := index.NewMockBlock(ctrl)
	mockBlock.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	mockBlock.EXPECT().Close().Return(nil)
	oldestTime := now.Add(-time.Hour * 9)
	idx.state.blocksByTime[xtime.ToUnixNano(oldestTime)] = mockBlock

	idx.indexFilesetsBeforeFn = func(dir string, nsID ident.ID, exclusiveTime time.Time) ([]string, error) {
		require.True(t, exclusiveTime.Equal(oldestTime))
		return nil, nil
	}
	require.NoError(t, idx.CleanupExpiredFileSets(now))
}

func TestNamespaceIndexFlushSuccess(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	test := newTestIndex(t, ctrl)

	now := time.Now().Truncate(test.indexBlockSize)
	idx := test.index.(*nsIndex)

	defer func() {
		require.NoError(t, idx.Close())
	}()

	mockBlock := index.NewMockBlock(ctrl)
	mockBlock.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	blockTime := now.Add(-2 * test.indexBlockSize)
	mockBlock.EXPECT().StartTime().Return(blockTime).AnyTimes()
	mockBlock.EXPECT().EndTime().Return(blockTime.Add(test.indexBlockSize)).AnyTimes()
	idx.state.blocksByTime[xtime.ToUnixNano(blockTime)] = mockBlock

	mockBlock.EXPECT().IsSealed().Return(true)
	mockBlock.EXPECT().NeedsMutableSegmentsEvicted().Return(true)
	mockBlock.EXPECT().Close().Return(nil)

	mockShard := NewMockdatabaseShard(ctrl)
	mockShard.EXPECT().ID().Return(uint32(0)).AnyTimes()
	mockShard.EXPECT().FlushState(blockTime).Return(fileOpState{Status: fileOpSuccess})
	mockShard.EXPECT().FlushState(blockTime.Add(test.blockSize)).Return(fileOpState{Status: fileOpSuccess})
	shards := []databaseShard{mockShard}

	mockFlush := persist.NewMockIndexFlush(ctrl)

	persistClosed := false
	persistCalled := false
	closer := func() ([]segment.Segment, error) {
		persistClosed = true
		return nil, nil
	}
	persistFn := func(segment.Builder) error {
		persistCalled = true
		return nil
	}
	preparedPersist := persist.PreparedIndexPersist{
		Close:   closer,
		Persist: persistFn,
	}
	mockFlush.EXPECT().PrepareIndex(xtest.CmpMatcher(persist.IndexPrepareOptions{
		NamespaceMetadata: test.metadata,
		BlockStart:        blockTime,
		FileSetType:       persist.FileSetFlushType,
		Shards:            map[uint32]struct{}{0: struct{}{}},
	})).Return(preparedPersist, nil)

	results := block.NewMockFetchBlocksMetadataResults(ctrl)
	results.EXPECT().Results().Return(nil)
	results.EXPECT().Close()
	mockShard.EXPECT().FetchBlocksMetadataV2(gomock.Any(), blockTime, blockTime.Add(test.indexBlockSize),
		gomock.Any(), gomock.Any(), block.FetchBlocksMetadataOptions{}).Return(results, nil, nil)

	mockBlock.EXPECT().AddResults(gomock.Any()).Return(nil)
	mockBlock.EXPECT().EvictMutableSegments().Return(nil)

	require.NoError(t, idx.Flush(mockFlush, shards))
	require.True(t, persistCalled)
	require.True(t, persistClosed)
}

func TestNamespaceIndexFlushShardStateNotSuccess(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	test := newTestIndex(t, ctrl)

	now := time.Now().Truncate(test.indexBlockSize)
	idx := test.index.(*nsIndex)

	defer func() {
		require.NoError(t, idx.Close())
	}()

	mockBlock := index.NewMockBlock(ctrl)
	mockBlock.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	blockTime := now.Add(-2 * test.indexBlockSize)
	mockBlock.EXPECT().StartTime().Return(blockTime).AnyTimes()
	mockBlock.EXPECT().EndTime().Return(blockTime.Add(test.indexBlockSize)).AnyTimes()
	idx.state.blocksByTime[xtime.ToUnixNano(blockTime)] = mockBlock

	mockBlock.EXPECT().IsSealed().Return(true)
	mockBlock.EXPECT().NeedsMutableSegmentsEvicted().Return(true)
	mockBlock.EXPECT().Close().Return(nil)

	mockShard := NewMockdatabaseShard(ctrl)
	mockShard.EXPECT().ID().Return(uint32(0)).AnyTimes()
	mockShard.EXPECT().FlushState(blockTime).Return(fileOpState{Status: fileOpSuccess})
	mockShard.EXPECT().FlushState(blockTime.Add(test.blockSize)).Return(fileOpState{Status: fileOpFailed})
	shards := []databaseShard{mockShard}

	mockFlush := persist.NewMockIndexFlush(ctrl)

	require.NoError(t, idx.Flush(mockFlush, shards))
}

func TestNamespaceIndexFlushSuccessMultipleShards(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	test := newTestIndex(t, ctrl)

	now := time.Now().Truncate(test.indexBlockSize)
	idx := test.index.(*nsIndex)

	defer func() {
		require.NoError(t, idx.Close())
	}()

	mockBlock := index.NewMockBlock(ctrl)
	mockBlock.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	blockTime := now.Add(-2 * test.indexBlockSize)
	mockBlock.EXPECT().StartTime().Return(blockTime).AnyTimes()
	mockBlock.EXPECT().EndTime().Return(blockTime.Add(test.indexBlockSize)).AnyTimes()
	idx.state.blocksByTime[xtime.ToUnixNano(blockTime)] = mockBlock

	mockBlock.EXPECT().IsSealed().Return(true)
	mockBlock.EXPECT().NeedsMutableSegmentsEvicted().Return(true)
	mockBlock.EXPECT().Close().Return(nil)

	mockShard1 := NewMockdatabaseShard(ctrl)
	mockShard1.EXPECT().ID().Return(uint32(0)).AnyTimes()
	mockShard1.EXPECT().FlushState(blockTime).Return(fileOpState{Status: fileOpSuccess})
	mockShard1.EXPECT().FlushState(blockTime.Add(test.blockSize)).Return(fileOpState{Status: fileOpSuccess})

	mockShard2 := NewMockdatabaseShard(ctrl)
	mockShard2.EXPECT().ID().Return(uint32(1)).AnyTimes()
	mockShard2.EXPECT().FlushState(blockTime).Return(fileOpState{Status: fileOpSuccess})
	mockShard2.EXPECT().FlushState(blockTime.Add(test.blockSize)).Return(fileOpState{Status: fileOpSuccess})

	shards := []databaseShard{mockShard1, mockShard2}

	mockFlush := persist.NewMockIndexFlush(ctrl)

	persistClosed := false
	numPersistCalls := 0
	closer := func() ([]segment.Segment, error) {
		persistClosed = true
		return nil, nil
	}
	persistFn := func(segment.Builder) error {
		numPersistCalls++
		return nil
	}
	preparedPersist := persist.PreparedIndexPersist{
		Close:   closer,
		Persist: persistFn,
	}
	mockFlush.EXPECT().PrepareIndex(xtest.CmpMatcher(persist.IndexPrepareOptions{
		NamespaceMetadata: test.metadata,
		BlockStart:        blockTime,
		FileSetType:       persist.FileSetFlushType,
		Shards:            map[uint32]struct{}{0: struct{}{}, 1: struct{}{}},
	})).Return(preparedPersist, nil)

	results1 := block.NewMockFetchBlocksMetadataResults(ctrl)
	results1.EXPECT().Results().Return(nil)
	results1.EXPECT().Close()
	mockShard1.EXPECT().FetchBlocksMetadataV2(gomock.Any(), blockTime, blockTime.Add(test.indexBlockSize),
		gomock.Any(), gomock.Any(), block.FetchBlocksMetadataOptions{}).Return(results1, nil, nil)

	results2 := block.NewMockFetchBlocksMetadataResults(ctrl)
	results2.EXPECT().Results().Return(nil)
	results2.EXPECT().Close()
	mockShard2.EXPECT().FetchBlocksMetadataV2(gomock.Any(), blockTime, blockTime.Add(test.indexBlockSize),
		gomock.Any(), gomock.Any(), block.FetchBlocksMetadataOptions{}).Return(results2, nil, nil)

	mockBlock.EXPECT().AddResults(gomock.Any()).Return(nil)
	mockBlock.EXPECT().EvictMutableSegments().Return(nil)

	require.NoError(t, idx.Flush(mockFlush, shards))
	require.Equal(t, 2, numPersistCalls)
	require.True(t, persistClosed)
}

func TestNamespaceIndexQueryNoMatchingBlocks(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	test := newTestIndex(t, ctrl)

	now := time.Now().Truncate(test.indexBlockSize)
	query := index.Query{idx.NewTermQuery([]byte("foo"), []byte("bar"))}
	idx := test.index.(*nsIndex)

	defer func() {
		require.NoError(t, idx.Close())
	}()

	mockBlock := index.NewMockBlock(ctrl)
	mockBlock.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	blockTime := now.Add(-1 * test.indexBlockSize)
	mockBlock.EXPECT().StartTime().Return(blockTime).AnyTimes()
	mockBlock.EXPECT().EndTime().Return(blockTime.Add(test.indexBlockSize)).AnyTimes()
	mockBlock.EXPECT().Close().Return(nil)
	idx.state.blocksByTime[xtime.ToUnixNano(blockTime)] = mockBlock

	ctx := context.NewContext()
	defer ctx.Close()

	// Query non-overlapping range
	result, err := idx.Query(ctx, query, index.QueryOptions{
		StartInclusive: now.Add(-3 * test.indexBlockSize),
		EndExclusive:   now.Add(-2 * test.indexBlockSize),
	})
	require.NoError(t, err)
	assert.True(t, result.Exhaustive)
	assert.Equal(t, 0, result.Results.Size())
}

type testIndex struct {
	index          namespaceIndex
	metadata       namespace.Metadata
	opts           Options
	ropts          retention.Options
	blockSize      time.Duration
	indexBlockSize time.Duration
	retention      time.Duration
}

func newTestIndex(t *testing.T, ctrl *gomock.Controller) testIndex {
	blockSize := time.Hour
	indexBlockSize := 2 * time.Hour
	retentionPeriod := 24 * time.Hour
	ropts := retention.NewOptions().
		SetBlockSize(blockSize).
		SetRetentionPeriod(retentionPeriod).
		SetBufferPast(blockSize / 2)
	nopts := namespace.NewOptions().
		SetRetentionOptions(ropts).
		SetIndexOptions(namespace.NewIndexOptions().SetBlockSize(indexBlockSize))
	md, err := namespace.NewMetadata(ident.StringID("testns"), nopts)
	require.NoError(t, err)
	opts := testDatabaseOptions()
	index, err := newNamespaceIndex(md, opts)
	require.NoError(t, err)

	return testIndex{
		index:          index,
		metadata:       md,
		opts:           opts,
		blockSize:      blockSize,
		indexBlockSize: indexBlockSize,
		retention:      retentionPeriod,
	}
}
