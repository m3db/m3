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
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3x/ident"
	xtest "github.com/m3db/m3x/test"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
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

	now := time.Now().Truncate(time.Hour)
	idx := nsIdx.(*nsIndex)

	mockBlock := index.NewMockBlock(ctrl)
	mockBlock.EXPECT().Stats().Return(index.BlockStats{}).AnyTimes()
	oldestTime := now.Add(-time.Hour * 9)
	idx.state.Lock()
	idx.state.blocksByTime[xtime.ToUnixNano(oldestTime)] = mockBlock
	idx.state.Unlock()

	idx.indexFilesetsBeforeFn = func(dir string, nsID ident.ID, exclusiveTime time.Time) ([]string, error) {
		require.True(t, exclusiveTime.Equal(oldestTime))
		return nil, nil
	}
	require.NoError(t, idx.CleanupExpiredFileSets(now))
}

func TestNamespaceIndexFlushSuccess(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	blockSize := time.Hour
	indexBlockSize := 2 * time.Hour
	period := 8 * time.Hour
	nopts := namespace.NewOptions().
		SetRetentionOptions(retention.NewOptions().
			SetBlockSize(blockSize).
			SetRetentionPeriod(period)).
		SetIndexOptions(namespace.NewIndexOptions().SetBlockSize(indexBlockSize))
	md, err := namespace.NewMetadata(ident.StringID("testns"), nopts)
	require.NoError(t, err)
	nsIdx, err := newNamespaceIndex(md, testDatabaseOptions())
	require.NoError(t, err)

	now := time.Now().Truncate(indexBlockSize)
	idx := nsIdx.(*nsIndex)

	mockBlock := index.NewMockBlock(ctrl)
	mockBlock.EXPECT().Stats().Return(index.BlockStats{}).AnyTimes()
	blockTime := now.Add(-2 * indexBlockSize)
	mockBlock.EXPECT().StartTime().Return(blockTime).AnyTimes()
	mockBlock.EXPECT().EndTime().Return(blockTime.Add(indexBlockSize)).AnyTimes()
	mockBlock.EXPECT().IsSealed().Return(true)
	mockBlock.EXPECT().NeedsFlush().Return(true)
	idx.state.Lock()
	idx.state.blocksByTime[xtime.ToUnixNano(blockTime)] = mockBlock
	idx.state.Unlock()

	mockShard := NewMockdatabaseShard(ctrl)
	mockShard.EXPECT().ID().Return(uint32(0)).AnyTimes()
	mockShard.EXPECT().FlushState(blockTime).Return(fileOpState{Status: fileOpSuccess})
	mockShard.EXPECT().FlushState(blockTime.Add(blockSize)).Return(fileOpState{Status: fileOpSuccess})
	shards := []databaseShard{mockShard}

	mockFlush := persist.NewMockIndexFlush(ctrl)

	persistClosed := false
	persistCalled := false
	closer := func() ([]segment.Segment, error) {
		persistClosed = true
		return nil, nil
	}
	persistFn := func(segment.MutableSegment) error {
		persistCalled = true
		return nil
	}
	preparedPersist := persist.PreparedIndexPersist{
		Close:   closer,
		Persist: persistFn,
	}
	mockFlush.EXPECT().PrepareIndex(xtest.CmpMatcher(persist.IndexPrepareOptions{
		NamespaceMetadata: md,
		BlockStart:        blockTime,
		FileSetType:       persist.FileSetFlushType,
		Shards:            map[uint32]struct{}{0: struct{}{}},
	})).Return(preparedPersist, nil)

	results := block.NewMockFetchBlocksMetadataResults(ctrl)
	results.EXPECT().Results().Return(nil)
	results.EXPECT().Close()
	mockShard.EXPECT().FetchBlocksMetadataV2(gomock.Any(), blockTime, blockTime.Add(indexBlockSize),
		gomock.Any(), gomock.Any(), block.FetchBlocksMetadataOptions{}).Return(results, nil, nil)

	mockBlock.EXPECT().AddResults(gomock.Any()).Return(nil)
	mockBlock.EXPECT().EvictActiveSegments().Return(index.EvictActiveSegmentResults{}, nil)

	require.NoError(t, nsIdx.Flush(mockFlush, shards))
	require.True(t, persistCalled)
	require.True(t, persistClosed)
}

func TestNamespaceIndexFlushShardStateNotSuccess(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	blockSize := time.Hour
	indexBlockSize := 2 * time.Hour
	period := 8 * time.Hour
	nopts := namespace.NewOptions().
		SetRetentionOptions(retention.NewOptions().
			SetBlockSize(blockSize).
			SetRetentionPeriod(period)).
		SetIndexOptions(namespace.NewIndexOptions().SetBlockSize(indexBlockSize))
	md, err := namespace.NewMetadata(ident.StringID("testns"), nopts)
	require.NoError(t, err)
	nsIdx, err := newNamespaceIndex(md, testDatabaseOptions())
	require.NoError(t, err)

	now := time.Now().Truncate(indexBlockSize)
	idx := nsIdx.(*nsIndex)

	mockBlock := index.NewMockBlock(ctrl)
	mockBlock.EXPECT().Stats().Return(index.BlockStats{}).AnyTimes()
	blockTime := now.Add(-2 * indexBlockSize)
	mockBlock.EXPECT().StartTime().Return(blockTime).AnyTimes()
	mockBlock.EXPECT().EndTime().Return(blockTime.Add(indexBlockSize)).AnyTimes()
	idx.state.Lock()
	idx.state.blocksByTime[xtime.ToUnixNano(blockTime)] = mockBlock
	idx.state.Unlock()

	mockBlock.EXPECT().IsSealed().Return(true)
	mockBlock.EXPECT().NeedsFlush().Return(true)

	mockShard := NewMockdatabaseShard(ctrl)
	mockShard.EXPECT().ID().Return(uint32(0)).AnyTimes()
	mockShard.EXPECT().FlushState(blockTime).Return(fileOpState{Status: fileOpSuccess})
	mockShard.EXPECT().FlushState(blockTime.Add(blockSize)).Return(fileOpState{Status: fileOpFailed})
	shards := []databaseShard{mockShard}

	mockFlush := persist.NewMockIndexFlush(ctrl)

	require.NoError(t, nsIdx.Flush(mockFlush, shards))
}

func TestNamespaceIndexFlushSuccessMultipleShards(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	blockSize := time.Hour
	indexBlockSize := 2 * time.Hour
	period := 8 * time.Hour
	nopts := namespace.NewOptions().
		SetRetentionOptions(retention.NewOptions().
			SetBlockSize(blockSize).
			SetRetentionPeriod(period)).
		SetIndexOptions(namespace.NewIndexOptions().SetBlockSize(indexBlockSize))
	md, err := namespace.NewMetadata(ident.StringID("testns"), nopts)
	require.NoError(t, err)
	nsIdx, err := newNamespaceIndex(md, testDatabaseOptions())
	require.NoError(t, err)

	now := time.Now().Truncate(indexBlockSize)
	idx := nsIdx.(*nsIndex)

	mockBlock := index.NewMockBlock(ctrl)
	mockBlock.EXPECT().Stats().Return(index.BlockStats{}).AnyTimes()
	blockTime := now.Add(-2 * indexBlockSize)
	mockBlock.EXPECT().StartTime().Return(blockTime).AnyTimes()
	mockBlock.EXPECT().EndTime().Return(blockTime.Add(indexBlockSize)).AnyTimes()
	idx.state.Lock()
	idx.state.blocksByTime[xtime.ToUnixNano(blockTime)] = mockBlock
	idx.state.Unlock()

	mockBlock.EXPECT().IsSealed().Return(true)
	mockBlock.EXPECT().NeedsFlush().Return(true)

	mockShard1 := NewMockdatabaseShard(ctrl)
	mockShard1.EXPECT().ID().Return(uint32(0)).AnyTimes()
	mockShard1.EXPECT().FlushState(blockTime).Return(fileOpState{Status: fileOpSuccess})
	mockShard1.EXPECT().FlushState(blockTime.Add(blockSize)).Return(fileOpState{Status: fileOpSuccess})

	mockShard2 := NewMockdatabaseShard(ctrl)
	mockShard2.EXPECT().ID().Return(uint32(1)).AnyTimes()
	mockShard2.EXPECT().FlushState(blockTime).Return(fileOpState{Status: fileOpSuccess})
	mockShard2.EXPECT().FlushState(blockTime.Add(blockSize)).Return(fileOpState{Status: fileOpSuccess})

	shards := []databaseShard{mockShard1, mockShard2}

	mockFlush := persist.NewMockIndexFlush(ctrl)

	persistClosed := false
	numPersistCalls := 0
	closer := func() ([]segment.Segment, error) {
		persistClosed = true
		return nil, nil
	}
	persistFn := func(segment.MutableSegment) error {
		numPersistCalls++
		return nil
	}
	preparedPersist := persist.PreparedIndexPersist{
		Close:   closer,
		Persist: persistFn,
	}
	mockFlush.EXPECT().PrepareIndex(xtest.CmpMatcher(persist.IndexPrepareOptions{
		NamespaceMetadata: md,
		BlockStart:        blockTime,
		FileSetType:       persist.FileSetFlushType,
		Shards:            map[uint32]struct{}{0: struct{}{}, 1: struct{}{}},
	})).Return(preparedPersist, nil)

	results1 := block.NewMockFetchBlocksMetadataResults(ctrl)
	results1.EXPECT().Results().Return(nil)
	results1.EXPECT().Close()
	mockShard1.EXPECT().FetchBlocksMetadataV2(gomock.Any(), blockTime, blockTime.Add(indexBlockSize),
		gomock.Any(), gomock.Any(), block.FetchBlocksMetadataOptions{}).Return(results1, nil, nil)

	results2 := block.NewMockFetchBlocksMetadataResults(ctrl)
	results2.EXPECT().Results().Return(nil)
	results2.EXPECT().Close()
	mockShard2.EXPECT().FetchBlocksMetadataV2(gomock.Any(), blockTime, blockTime.Add(indexBlockSize),
		gomock.Any(), gomock.Any(), block.FetchBlocksMetadataOptions{}).Return(results2, nil, nil)

	mockBlock.EXPECT().AddResults(gomock.Any()).Return(nil)
	mockBlock.EXPECT().EvictActiveSegments().Return(index.EvictActiveSegmentResults{}, nil)

	require.NoError(t, nsIdx.Flush(mockFlush, shards))
	require.Equal(t, 2, numPersistCalls)
	require.True(t, persistClosed)
}
