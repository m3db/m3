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
	stdctx "context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"testing"
	"time"

	indexpb "github.com/m3db/m3/src/dbnode/generated/proto/index"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	idxpersist "github.com/m3db/m3/src/m3ninx/persist"
	"github.com/m3db/m3/src/x/context"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	protobuftypes "github.com/gogo/protobuf/types"
	"github.com/golang/mock/gomock"
	opentracinglog "github.com/opentracing/opentracing-go/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNamespaceIndexCleanupExpiredFilesets(t *testing.T) {
	md := testNamespaceMetadata(time.Hour, time.Hour*8)
	nsIdx, err := newNamespaceIndex(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, DefaultTestOptions())
	require.NoError(t, err)

	now := xtime.Now().Truncate(time.Hour)
	idx := nsIdx.(*nsIndex)

	oldestTime := now.Add(-time.Hour * 8)
	files := []string{"abc"}

	idx.indexFilesetsBeforeFn = func(
		dir string, nsID ident.ID, exclusiveTime xtime.UnixNano) ([]string, error) {
		require.True(t, oldestTime.Equal(exclusiveTime), fmt.Sprintf("%v %v", exclusiveTime, oldestTime))
		return files, nil
	}
	idx.deleteFilesFn = func(s []string) error {
		require.Equal(t, files, s)
		return nil
	}
	require.NoError(t, idx.CleanupExpiredFileSets(now))
}

func TestNamespaceIndexCleanupDuplicateFilesets(t *testing.T) {
	md := testNamespaceMetadata(time.Hour, time.Hour*8)
	nsIdx, err := newNamespaceIndex(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, DefaultTestOptions())
	require.NoError(t, err)

	idx := nsIdx.(*nsIndex)
	now := xtime.Now().Truncate(time.Hour)
	indexBlockSize := 2 * time.Hour
	blockTime := now.Add(-2 * indexBlockSize)

	dir, err := ioutil.TempDir("", t.Name())
	require.NoError(t, err)

	defer os.RemoveAll(dir)

	fset1, err := ioutil.TempFile(dir, "fileset-9000-0-")
	require.NoError(t, err)
	fset2, err := ioutil.TempFile(dir, "fileset-9000-1-")
	require.NoError(t, err)
	fset3, err := ioutil.TempFile(dir, "fileset-9000-2-")
	require.NoError(t, err)

	volumeType := "extra"
	infoFiles := []fs.ReadIndexInfoFileResult{
		{
			Info: indexpb.IndexVolumeInfo{
				BlockStart: int64(blockTime),
				BlockSize:  int64(indexBlockSize),
				Shards:     []uint32{0, 1, 2},
				IndexVolumeType: &protobuftypes.StringValue{
					Value: volumeType,
				},
			},
			AbsoluteFilePaths: []string{fset1.Name()},
		},
		{
			Info: indexpb.IndexVolumeInfo{
				BlockStart: int64(blockTime),
				BlockSize:  int64(indexBlockSize),
				Shards:     []uint32{0, 1, 2},
				IndexVolumeType: &protobuftypes.StringValue{
					Value: volumeType,
				},
			},
			AbsoluteFilePaths: []string{fset2.Name()},
		},
		{
			Info: indexpb.IndexVolumeInfo{
				BlockStart: int64(blockTime),
				BlockSize:  int64(indexBlockSize),
				Shards:     []uint32{0, 1, 2, 3},
				IndexVolumeType: &protobuftypes.StringValue{
					Value: volumeType,
				},
			},
			AbsoluteFilePaths: []string{fset3.Name()},
		},
	}

	idx.readIndexInfoFilesFn = func(_ fs.ReadIndexInfoFilesOptions) []fs.ReadIndexInfoFileResult {
		return infoFiles
	}

	idx.deleteFilesFn = func(s []string) error {
		require.Equal(t, []string{fset1.Name(), fset2.Name()}, s)
		multiErr := xerrors.NewMultiError()
		for _, file := range s {
			multiErr = multiErr.Add(os.Remove(file))
		}
		return multiErr.FinalError()
	}
	require.NoError(t, idx.CleanupDuplicateFileSets([]uint32{0, 1, 2, 3}))
}

func TestNamespaceIndexCleanupDuplicateFilesets_SortingByBlockStartAndVolumeType(t *testing.T) {
	blockStart1 := xtime.Now().Truncate(2 * time.Hour)
	blockStart2 := blockStart1.Add(-2 * time.Hour)

	filesets := []struct {
		blockStart   xtime.UnixNano
		volumeType   string
		volumeIndex  int
		shouldRemove bool
	}{
		{
			blockStart:   blockStart1,
			volumeType:   "default",
			volumeIndex:  0,
			shouldRemove: false,
		},
		{
			blockStart:   blockStart1,
			volumeType:   "extra",
			volumeIndex:  1,
			shouldRemove: false,
		},
		{
			blockStart:   blockStart1,
			volumeType:   "extra",
			volumeIndex:  0,
			shouldRemove: true,
		},
		{
			blockStart:   blockStart2,
			volumeType:   "default",
			volumeIndex:  1,
			shouldRemove: true,
		},
		{
			blockStart:   blockStart2,
			volumeType:   "default",
			volumeIndex:  2,
			shouldRemove: false,
		},
		{
			blockStart:   blockStart2,
			volumeType:   "default",
			volumeIndex:  0,
			shouldRemove: true,
		},
	}

	shards := []uint32{1, 2}
	expectedFilesToRemove := make([]string, 0)
	infoFiles := make([]fs.ReadIndexInfoFileResult, 0)
	for _, fileset := range filesets {
		infoFile := newReadIndexInfoFileResult(fileset.blockStart, fileset.volumeType, fileset.volumeIndex, shards)
		infoFiles = append(infoFiles, infoFile)
		if fileset.shouldRemove {
			expectedFilesToRemove = append(expectedFilesToRemove, infoFile.AbsoluteFilePaths...)
		}
	}

	md := testNamespaceMetadata(time.Hour, time.Hour*8)
	nsIdx, err := newNamespaceIndex(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, DefaultTestOptions())
	require.NoError(t, err)
	idx := nsIdx.(*nsIndex)
	idx.readIndexInfoFilesFn = func(_ fs.ReadIndexInfoFilesOptions) []fs.ReadIndexInfoFileResult {
		return infoFiles
	}
	idx.deleteFilesFn = func(s []string) error {
		require.Len(t, s, len(expectedFilesToRemove))
		for _, e := range expectedFilesToRemove {
			assert.Contains(t, s, e)
		}
		return nil
	}
	require.NoError(t, idx.CleanupDuplicateFileSets(shards))
}

func TestNamespaceIndexCleanupDuplicateFilesets_ChangingShardList(t *testing.T) {
	shardLists := []struct {
		shards       []uint32
		shouldRemove bool
	}{
		{
			shards:       []uint32{1, 2},
			shouldRemove: true,
		},
		{
			shards:       []uint32{1, 2, 3},
			shouldRemove: false,
		},
		{
			shards:       []uint32{1, 2, 4},
			shouldRemove: true,
		},
		{
			shards:       []uint32{1, 2, 4},
			shouldRemove: true,
		},
		{
			shards:       []uint32{1, 5},
			shouldRemove: true,
		},
		{
			shards:       []uint32{1, 2, 4, 5},
			shouldRemove: false,
		},
		{
			shards:       []uint32{1, 2},
			shouldRemove: false,
		},
	}

	blockStart := xtime.Now().Truncate(2 * time.Hour)
	expectedFilesToRemove := make([]string, 0)
	infoFiles := make([]fs.ReadIndexInfoFileResult, 0)
	for i, shardList := range shardLists {
		infoFile := newReadIndexInfoFileResult(blockStart, "default", i, shardList.shards)
		infoFiles = append(infoFiles, infoFile)
		if shardList.shouldRemove {
			expectedFilesToRemove = append(expectedFilesToRemove, infoFile.AbsoluteFilePaths...)
		}
	}

	md := testNamespaceMetadata(time.Hour, time.Hour*8)
	nsIdx, err := newNamespaceIndex(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, DefaultTestOptions())
	require.NoError(t, err)
	idx := nsIdx.(*nsIndex)
	idx.readIndexInfoFilesFn = func(_ fs.ReadIndexInfoFilesOptions) []fs.ReadIndexInfoFileResult {
		return infoFiles
	}
	idx.deleteFilesFn = func(s []string) error {
		require.Len(t, s, len(expectedFilesToRemove))
		for _, e := range expectedFilesToRemove {
			assert.Contains(t, s, e)
		}
		return nil
	}

	require.NoError(t, idx.CleanupDuplicateFileSets([]uint32{1, 2, 3, 4, 5}))
}

func TestNamespaceIndexCleanupDuplicateFilesets_IgnoreNonActiveShards(t *testing.T) {
	activeShards := []uint32{1, 2}
	shardLists := []struct {
		shards       []uint32
		shouldRemove bool
	}{
		{
			shards:       []uint32{1, 2, 3, 4},
			shouldRemove: true,
		},
		{
			shards:       []uint32{1, 2, 3},
			shouldRemove: true,
		},
		{
			shards:       []uint32{1, 2},
			shouldRemove: false,
		},
	}

	blockStart := xtime.Now().Truncate(2 * time.Hour)
	expectedFilesToRemove := make([]string, 0)
	infoFiles := make([]fs.ReadIndexInfoFileResult, 0)
	for i, shardList := range shardLists {
		infoFile := newReadIndexInfoFileResult(blockStart, "default", i, shardList.shards)
		infoFiles = append(infoFiles, infoFile)
		if shardList.shouldRemove {
			expectedFilesToRemove = append(expectedFilesToRemove, infoFile.AbsoluteFilePaths...)
		}
	}

	md := testNamespaceMetadata(time.Hour, time.Hour*8)
	nsIdx, err := newNamespaceIndex(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, DefaultTestOptions())
	require.NoError(t, err)
	idx := nsIdx.(*nsIndex)
	idx.readIndexInfoFilesFn = func(_ fs.ReadIndexInfoFilesOptions) []fs.ReadIndexInfoFileResult {
		return infoFiles
	}
	idx.deleteFilesFn = func(s []string) error {
		require.Len(t, s, len(expectedFilesToRemove))
		for _, e := range expectedFilesToRemove {
			assert.Contains(t, s, e)
		}
		return nil
	}

	require.NoError(t, idx.CleanupDuplicateFileSets(activeShards))
}

func TestNamespaceIndexCleanupDuplicateFilesets_NoActiveShards(t *testing.T) {
	activeShards := []uint32{}
	shardLists := []struct {
		shards       []uint32
		shouldRemove bool
	}{
		{
			shards:       []uint32{1, 2, 3, 4},
			shouldRemove: true,
		},
		{
			shards:       []uint32{1, 2, 3},
			shouldRemove: true,
		},
		{
			shards:       []uint32{1, 2},
			shouldRemove: false,
		},
	}

	blockStart := xtime.Now().Truncate(2 * time.Hour)
	expectedFilesToRemove := make([]string, 0)
	infoFiles := make([]fs.ReadIndexInfoFileResult, 0)
	for i, shardList := range shardLists {
		infoFile := newReadIndexInfoFileResult(blockStart, "default", i, shardList.shards)
		infoFiles = append(infoFiles, infoFile)
		if shardList.shouldRemove {
			expectedFilesToRemove = append(expectedFilesToRemove, infoFile.AbsoluteFilePaths...)
		}
	}

	md := testNamespaceMetadata(time.Hour, time.Hour*8)
	nsIdx, err := newNamespaceIndex(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, DefaultTestOptions())
	require.NoError(t, err)
	idx := nsIdx.(*nsIndex)
	idx.readIndexInfoFilesFn = func(_ fs.ReadIndexInfoFilesOptions) []fs.ReadIndexInfoFileResult {
		return infoFiles
	}
	idx.deleteFilesFn = func(s []string) error {
		require.Len(t, s, len(expectedFilesToRemove))
		for _, e := range expectedFilesToRemove {
			assert.Contains(t, s, e)
		}
		return nil
	}

	require.NoError(t, idx.CleanupDuplicateFileSets(activeShards))
}

func TestNamespaceIndexCleanupDuplicateFilesetsNoop(t *testing.T) {
	md := testNamespaceMetadata(time.Hour, time.Hour*8)
	nsIdx, err := newNamespaceIndex(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, DefaultTestOptions())
	require.NoError(t, err)

	idx := nsIdx.(*nsIndex)
	now := xtime.Now().Truncate(time.Hour)
	indexBlockSize := 2 * time.Hour
	blockTime := now.Add(-2 * indexBlockSize)

	dir, err := ioutil.TempDir("", t.Name())
	require.NoError(t, err)

	defer os.RemoveAll(dir)

	fset1, err := ioutil.TempFile(dir, "fileset-9000-0-")
	require.NoError(t, err)
	fset2, err := ioutil.TempFile(dir, "fileset-9000-1-")
	require.NoError(t, err)

	volumeType := string(idxpersist.DefaultIndexVolumeType)
	infoFiles := []fs.ReadIndexInfoFileResult{
		{
			Info: indexpb.IndexVolumeInfo{
				BlockStart: int64(blockTime),
				BlockSize:  int64(indexBlockSize),
				Shards:     []uint32{0, 1, 2},
				IndexVolumeType: &protobuftypes.StringValue{
					Value: volumeType,
				},
			},
			AbsoluteFilePaths: []string{fset1.Name()},
		},
		{
			Info: indexpb.IndexVolumeInfo{
				BlockStart: int64(blockTime),
				BlockSize:  int64(indexBlockSize),
				Shards:     []uint32{4},
				IndexVolumeType: &protobuftypes.StringValue{
					Value: volumeType,
				},
			},
			AbsoluteFilePaths: []string{fset2.Name()},
		},
	}

	idx.readIndexInfoFilesFn = func(_ fs.ReadIndexInfoFilesOptions) []fs.ReadIndexInfoFileResult {
		return infoFiles
	}

	idx.deleteFilesFn = func(s []string) error {
		require.Equal(t, []string{}, s)
		return nil
	}
	require.NoError(t, idx.CleanupDuplicateFileSets([]uint32{0, 1, 2, 4}))
}

func TestNamespaceIndexCleanupExpiredFilesetsWithBlocks(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	md := testNamespaceMetadata(time.Hour, time.Hour*8)
	nsIdx, err := newNamespaceIndex(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, DefaultTestOptions())
	require.NoError(t, err)

	defer func() {
		require.NoError(t, nsIdx.Close())
	}()

	now := xtime.Now().Truncate(time.Hour)
	idx := nsIdx.(*nsIndex)

	mockBlock := index.NewMockBlock(ctrl)
	mockBlock.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	mockBlock.EXPECT().Close().Return(nil)
	oldestTime := now.Add(-time.Hour * 9)
	idx.state.blocksByTime[oldestTime] = mockBlock

	idx.indexFilesetsBeforeFn = func(
		dir string, nsID ident.ID, exclusiveTime xtime.UnixNano) ([]string, error) {
		require.True(t, exclusiveTime.Equal(oldestTime))
		return nil, nil
	}
	require.NoError(t, idx.CleanupExpiredFileSets(now))
}

func TestNamespaceIndexCleanupCorruptedFilesets(t *testing.T) {
	md := testNamespaceMetadata(time.Hour, time.Hour*24)
	nsIdx, err := newNamespaceIndex(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, DefaultTestOptions())
	require.NoError(t, err)

	idx := nsIdx.(*nsIndex)
	now := xtime.Now().Truncate(time.Hour)
	indexBlockSize := 2 * time.Hour
	var (
		blockStarts = []xtime.UnixNano{
			now.Add(-6 * indexBlockSize),
			now.Add(-5 * indexBlockSize),
			now.Add(-4 * indexBlockSize),
			now.Add(-3 * indexBlockSize),
			now.Add(-2 * indexBlockSize),
			now.Add(-1 * indexBlockSize),
		}
		shards = []uint32{0, 1, 2} // has no effect on this test

		volumeTypeDefault = "default"
		volumeTypeExtra   = "extra"
	)

	filesetsForTest := []struct {
		infoFile     fs.ReadIndexInfoFileResult
		shouldRemove bool
	}{
		{newReadIndexInfoFileResult(blockStarts[0], volumeTypeDefault, 0, shards), false},
		{newReadIndexInfoFileResultForCorruptedFileset(blockStarts[0], volumeTypeDefault, 1), true},
		{newReadIndexInfoFileResultForCorruptedFileset(blockStarts[0], volumeTypeDefault, 2), true},
		{newReadIndexInfoFileResultForCorruptedFileset(blockStarts[0], volumeTypeExtra, 5), true},
		{newReadIndexInfoFileResultForCorruptedFileset(blockStarts[0], volumeTypeExtra, 6), false},
		{newReadIndexInfoFileResult(blockStarts[0], volumeTypeDefault, 11, shards), false},

		{newReadIndexInfoFileResultForCorruptedFileset(blockStarts[1], volumeTypeDefault, 1), false},
		{newReadIndexInfoFileResultForCorruptedInfoFile(blockStarts[1], 3), true},
		{newReadIndexInfoFileResultForCorruptedInfoFile(blockStarts[1], 4), true},
		{newReadIndexInfoFileResultForCorruptedFileset(blockStarts[1], volumeTypeExtra, 5), false},
		{newReadIndexInfoFileResultForCorruptedInfoFile(blockStarts[1], 6), true},
		{newReadIndexInfoFileResultForCorruptedInfoFile(blockStarts[1], 7), false},

		{newReadIndexInfoFileResultForCorruptedInfoFile(blockStarts[2], 0), true},
		{newReadIndexInfoFileResultForCorruptedFileset(blockStarts[2], volumeTypeDefault, 1), true},
		{newReadIndexInfoFileResultForCorruptedFileset(blockStarts[2], volumeTypeExtra, 2), true},
		{newReadIndexInfoFileResult(blockStarts[2], volumeTypeDefault, 3, shards), false},
		{newReadIndexInfoFileResult(blockStarts[2], volumeTypeExtra, 4, shards), false},

		{newReadIndexInfoFileResult(blockStarts[3], volumeTypeDefault, 0, shards), false},

		{newReadIndexInfoFileResultForCorruptedFileset(blockStarts[4], volumeTypeDefault, 0), false},

		{newReadIndexInfoFileResultForCorruptedInfoFile(blockStarts[5], 0), false},
	}

	var (
		infoFiles         = make([]fs.ReadIndexInfoFileResult, 0)
		expectedFilenames = make([]string, 0)
	)
	for _, f := range filesetsForTest {
		infoFiles = append(infoFiles, f.infoFile)
		if f.shouldRemove {
			expectedFilenames = append(expectedFilenames, f.infoFile.AbsoluteFilePaths...)
		}
	}

	idx.readIndexInfoFilesFn = func(_ fs.ReadIndexInfoFilesOptions) []fs.ReadIndexInfoFileResult {
		return infoFiles
	}

	deleteFilesFnInvoked := false
	idx.deleteFilesFn = func(s []string) error {
		sort.Strings(s)
		sort.Strings(expectedFilenames)
		require.Equal(t, expectedFilenames, s)
		deleteFilesFnInvoked = true
		return nil
	}
	require.NoError(t, idx.CleanupCorruptedFileSets())
	require.True(t, deleteFilesFnInvoked)
}

func TestNamespaceIndexFlushSuccess(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	test := newTestIndex(t, ctrl)

	idx := test.index.(*nsIndex)

	defer func() {
		require.NoError(t, idx.Close())
	}()

	verifyFlushForShards(
		t,
		ctrl,
		idx,
		test.blockSize,
		[]uint32{0},
	)
}

func TestNamespaceIndexFlushSuccessMultipleShards(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	test := newTestIndex(t, ctrl)

	idx := test.index.(*nsIndex)

	defer func() {
		require.NoError(t, idx.Close())
	}()

	verifyFlushForShards(
		t,
		ctrl,
		idx,
		test.blockSize,
		[]uint32{0, 1, 2},
	)
}

func TestNamespaceIndexFlushShardStateNotSuccess(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	test := newTestIndex(t, ctrl)

	now := xtime.Now().Truncate(test.indexBlockSize)
	idx := test.index.(*nsIndex)

	defer func() {
		require.NoError(t, idx.Close())
	}()

	// NB(bodu): We don't need to allocate a mock block for every block start we just need to
	// ensure that we aren't flushing index data if TSDB is not on disk and a single mock block is sufficient.
	mockBlock := index.NewMockBlock(ctrl)
	mockBlock.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	blockTime := now.Add(-2 * test.indexBlockSize)
	mockBlock.EXPECT().StartTime().Return(blockTime).AnyTimes()
	mockBlock.EXPECT().EndTime().Return(blockTime.Add(test.indexBlockSize)).AnyTimes()
	idx.state.blocksByTime[blockTime] = mockBlock

	mockBlock.EXPECT().IsSealed().Return(true)
	mockBlock.EXPECT().Close().Return(nil)

	mockShard := NewMockdatabaseShard(ctrl)
	mockShard.EXPECT().IsBootstrapped().Return(true).AnyTimes()
	mockShard.EXPECT().ID().Return(uint32(0)).AnyTimes()
	mockShard.EXPECT().FlushState(gomock.Any()).Return(fileOpState{WarmStatus: warmStatus{
		IndexFlushed: fileOpFailed,
	}}, nil).AnyTimes()
	shards := []databaseShard{mockShard}

	mockFlush := persist.NewMockIndexFlush(ctrl)

	err := idx.WarmFlush(mockFlush, shards)
	require.NoError(t, err)
}

func TestNamespaceIndexQueryNoMatchingBlocks(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	test := newTestIndex(t, ctrl)

	now := xtime.Now().Truncate(test.indexBlockSize)
	query := index.Query{Query: idx.NewTermQuery([]byte("foo"), []byte("bar"))}
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
	idx.state.blocksByTime[blockTime] = mockBlock

	ctx := context.NewBackground()
	defer ctx.Close()

	start := now.Add(-3 * test.indexBlockSize)
	end := now.Add(-2 * test.indexBlockSize)
	// Query non-overlapping range
	result, err := idx.Query(ctx, query, index.QueryOptions{
		StartInclusive: start,
		EndExclusive:   end,
	})
	require.NoError(t, err)
	assert.True(t, result.Exhaustive)
	assert.Equal(t, 0, result.Results.Size())

	// Aggregate query on the non-overlapping range
	aggResult, err := idx.AggregateQuery(ctx, query, index.AggregationOptions{
		QueryOptions: index.QueryOptions{
			StartInclusive: start,
			EndExclusive:   end,
		},
	})
	require.NoError(t, err)
	assert.True(t, aggResult.Exhaustive)
	assert.Equal(t, 0, aggResult.Results.Size())
}

func TestNamespaceIndexQueryTimeout(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	test := newTestIndex(t, ctrl)

	now := xtime.Now().Truncate(test.indexBlockSize)
	query := index.Query{Query: idx.NewTermQuery([]byte("foo"), []byte("bar"))}
	idx := test.index.(*nsIndex)

	defer func() {
		require.NoError(t, idx.Close())
	}()

	stdCtx, cancel := stdctx.WithTimeout(stdctx.Background(), time.Second)
	defer cancel()
	ctx := context.NewWithGoContext(stdCtx)
	defer ctx.Close()

	mockIter := index.NewMockQueryIterator(ctrl)
	mockIter.EXPECT().Done().Return(false).Times(2)
	mockIter.EXPECT().Close().Return(nil)

	mockBlock := index.NewMockBlock(ctrl)
	mockBlock.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	blockTime := now.Add(-1 * test.indexBlockSize)
	mockBlock.EXPECT().StartTime().Return(blockTime).AnyTimes()
	mockBlock.EXPECT().EndTime().Return(blockTime.Add(test.indexBlockSize)).AnyTimes()
	mockBlock.EXPECT().QueryIter(gomock.Any(), gomock.Any()).Return(mockIter, nil)
	mockBlock.EXPECT().
		QueryWithIter(gomock.Any(), gomock.Any(), mockIter, gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			ctx context.Context,
			opts index.QueryOptions,
			iter index.QueryIterator,
			r index.QueryResults,
			deadline time.Time,
			logFields []opentracinglog.Field,
		) error {
			<-ctx.GoContext().Done()
			return ctx.GoContext().Err()
		})
	mockBlock.EXPECT().Close().Return(nil)
	idx.state.blocksByTime[blockTime] = mockBlock
	idx.updateBlockStartsWithLock()

	start := blockTime
	end := blockTime.Add(test.indexBlockSize)

	// Query non-overlapping range
	_, err := idx.Query(ctx, query, index.QueryOptions{
		StartInclusive: start,
		EndExclusive:   end,
	})
	require.Error(t, err)
	var multiErr xerrors.MultiError
	require.True(t, errors.As(err, &multiErr))
	require.True(t, multiErr.Contains(stdctx.DeadlineExceeded))
}

func TestNamespaceIndexFlushSkipBootstrappingShards(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	test := newTestIndex(t, ctrl)

	now := xtime.Now().Truncate(test.indexBlockSize)
	idx := test.index.(*nsIndex)

	defer func() {
		require.NoError(t, idx.Close())
	}()

	// NB(bodu): We don't need to allocate a mock block for every block start we just need to
	// ensure that we aren't flushing index data if TSDB is not on disk and a single mock block is sufficient.
	mockBlock := index.NewMockBlock(ctrl)
	mockBlock.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	blockTime := now.Add(-2 * test.indexBlockSize)
	mockBlock.EXPECT().StartTime().Return(blockTime).AnyTimes()
	mockBlock.EXPECT().EndTime().Return(blockTime.Add(test.indexBlockSize)).AnyTimes()
	mockBlock.EXPECT().NeedsColdMutableSegmentsEvicted().Return(true).AnyTimes()
	mockBlock.EXPECT().RotateColdMutableSegments().Return(nil).AnyTimes()
	mockBlock.EXPECT().EvictColdMutableSegments().Return(nil).AnyTimes()
	idx.state.blocksByTime[blockTime] = mockBlock

	mockBlock.EXPECT().Close().Return(nil)

	shardInfos := []struct {
		id             uint32
		isBootstrapped bool
	}{
		{0, true},
		{1, false},
		{2, true},
		{3, false},
	}

	shards := make([]databaseShard, 0, len(shardInfos))
	for _, shardInfo := range shardInfos {
		mockShard := NewMockdatabaseShard(ctrl)
		mockShard.EXPECT().IsBootstrapped().Return(shardInfo.isBootstrapped).AnyTimes()
		mockShard.EXPECT().ID().Return(shardInfo.id).AnyTimes()
		if shardInfo.isBootstrapped {
			mockShard.EXPECT().FlushState(gomock.Any()).Return(fileOpState{WarmStatus: warmStatus{
				IndexFlushed: fileOpSuccess,
			}}, nil).AnyTimes()
		}
		shards = append(shards, mockShard)
	}

	done, err := idx.ColdFlush(shards)
	require.NoError(t, err)
	require.NoError(t, done())
}

func verifyFlushForShards(
	t *testing.T,
	ctrl *gomock.Controller,
	idx *nsIndex,
	blockSize time.Duration,
	shards []uint32,
) {
	var (
		mockFlush          = persist.NewMockIndexFlush(ctrl)
		shardMap           = make(map[uint32]struct{})
		now                = xtime.Now()
		warmBlockStart     = now.Add(-idx.bufferPast).Truncate(idx.blockSize)
		mockShards         []*MockdatabaseShard
		dbShards           []databaseShard
		numBlocks          int
		persistClosedTimes int
		persistCalledTimes int
		actualDocs         = make([]doc.Metadata, 0)
		expectedDocs       = make([]doc.Metadata, 0)
	)
	// NB(bodu): Always align now w/ the index's view of now.
	idx.nowFn = func() time.Time {
		return now.ToTime()
	}
	for _, shard := range shards {
		mockShard := NewMockdatabaseShard(ctrl)
		mockShard.EXPECT().ID().Return(uint32(0)).AnyTimes()
		mockShards = append(mockShards, mockShard)
		shardMap[shard] = struct{}{}
		dbShards = append(dbShards, mockShard)
	}
	earliestBlockStartToRetain := retention.FlushTimeStartForRetentionPeriod(idx.retentionPeriod, idx.blockSize, now)
	for blockStart := earliestBlockStartToRetain; blockStart.Before(warmBlockStart); blockStart = blockStart.Add(idx.blockSize) {
		numBlocks++

		mockBlock := index.NewMockBlock(ctrl)
		mockBlock.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
		mockBlock.EXPECT().StartTime().Return(blockStart).AnyTimes()
		mockBlock.EXPECT().EndTime().Return(blockStart.Add(idx.blockSize)).AnyTimes()
		idx.state.blocksByTime[blockStart] = mockBlock

		mockBlock.EXPECT().Close().Return(nil)

		closer := func() ([]segment.Segment, error) {
			persistClosedTimes++
			return nil, nil
		}
		persistFn := func(b segment.Builder) error {
			persistCalledTimes++
			actualDocs = append(actualDocs, b.Docs()...)
			return nil
		}
		preparedPersist := persist.PreparedIndexPersist{
			Close:   closer,
			Persist: persistFn,
		}
		mockFlush.EXPECT().PrepareIndex(xtest.CmpMatcher(persist.IndexPrepareOptions{
			NamespaceMetadata: idx.nsMetadata,
			BlockStart:        blockStart,
			FileSetType:       persist.FileSetFlushType,
			Shards:            map[uint32]struct{}{0: {}},
			IndexVolumeType:   idxpersist.DefaultIndexVolumeType,
		})).Return(preparedPersist, nil)

		results := block.NewMockFetchBlocksMetadataResults(ctrl)

		resultsID1 := ident.StringID("CACHED")
		resultsID2 := ident.StringID("NEW")
		doc1 := doc.Metadata{
			ID:     resultsID1.Bytes(),
			Fields: []doc.Field{},
		}
		doc2 := doc.Metadata{
			ID:     resultsID2.Bytes(),
			Fields: []doc.Field{},
		}
		expectedDocs = append(expectedDocs, doc1)
		expectedDocs = append(expectedDocs, doc2)

		for _, mockShard := range mockShards {
			mockShard.EXPECT().IsBootstrapped().Return(true)
			mockShard.EXPECT().FlushState(blockStart).Return(fileOpState{WarmStatus: warmStatus{
				// Index flushing requires data flush already happened.
				DataFlushed: fileOpSuccess,
			}}, nil)
			mockShard.EXPECT().FlushState(blockStart.Add(blockSize)).Return(fileOpState{WarmStatus: warmStatus{
				// Index flushing requires data flush already happened.
				DataFlushed: fileOpSuccess,
			}}, nil)

			resultsTags1 := ident.NewTagsIterator(ident.NewTags())
			resultsTags2 := ident.NewTagsIterator(ident.NewTags())
			resultsInShard := []block.FetchBlocksMetadataResult{
				{
					ID:   resultsID1,
					Tags: resultsTags1,
				},
				{
					ID:   resultsID2,
					Tags: resultsTags2,
				},
			}
			results.EXPECT().Results().Return(resultsInShard)
			results.EXPECT().Close()

			mockShard.EXPECT().DocRef(resultsID1).Return(doc1, true, nil)
			mockShard.EXPECT().DocRef(resultsID2).Return(doc.Metadata{}, false, nil)

			mockShard.EXPECT().FetchBlocksMetadataV2(gomock.Any(), blockStart, blockStart.Add(idx.blockSize),
				gomock.Any(), gomock.Any(), block.FetchBlocksMetadataOptions{OnlyDisk: true}).Return(results, nil, nil)

			// For a given index block, which in this test is 2x the size of a block, we expect that
			// we mark as flushed 2 blockStarts that fall within the index block.
			mockShard.EXPECT().MarkWarmIndexFlushStateSuccessOrError(blockStart, nil)
			mockShard.EXPECT().MarkWarmIndexFlushStateSuccessOrError(blockStart.Add(blockSize), nil)
		}

		mockBlock.EXPECT().IsSealed().Return(true)
		mockBlock.EXPECT().AddResults(gomock.Any()).Return(nil)
		mockBlock.EXPECT().EvictMutableSegments().Return(nil)
	}
	err := idx.WarmFlush(mockFlush, dbShards)
	require.NoError(t, err)
	require.Equal(t, numBlocks, persistClosedTimes)
	require.Equal(t, numBlocks, persistCalledTimes)
	require.Equal(t, expectedDocs, actualDocs)
}

func newReadIndexInfoFileResult(
	blockStart xtime.UnixNano,
	volumeType string,
	volumeIndex int,
	shards []uint32,
) fs.ReadIndexInfoFileResult {
	filenames := []string{
		// TODO: this may be an error/
		fmt.Sprintf("fileset-%v-%v-segement-1.db", blockStart, volumeIndex),
		fmt.Sprintf("fileset-%v-%v-segement-2.db", blockStart, volumeIndex),
	}
	return fs.ReadIndexInfoFileResult{
		ID: fs.FileSetFileIdentifier{
			BlockStart:  blockStart,
			VolumeIndex: volumeIndex,
		},
		Info: indexpb.IndexVolumeInfo{
			BlockStart: int64(blockStart),
			BlockSize:  int64(2 * time.Hour),
			Shards:     shards,
			IndexVolumeType: &protobuftypes.StringValue{
				Value: volumeType,
			},
		},
		AbsoluteFilePaths: filenames,
		Corrupted:         false,
	}
}

func newReadIndexInfoFileResultForCorruptedFileset(
	blockStart xtime.UnixNano,
	volumeType string,
	volumeIndex int,
) fs.ReadIndexInfoFileResult {
	res := newReadIndexInfoFileResult(blockStart, volumeType, volumeIndex, []uint32{})
	res.Corrupted = true
	return res
}

func newReadIndexInfoFileResultForCorruptedInfoFile(
	blockStart xtime.UnixNano,
	volumeIndex int,
) fs.ReadIndexInfoFileResult {
	res := newReadIndexInfoFileResultForCorruptedFileset(blockStart, "", volumeIndex)
	res.Info = indexpb.IndexVolumeInfo{}
	return res
}

type testIndex struct {
	index          NamespaceIndex
	metadata       namespace.Metadata
	opts           Options
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
	opts := DefaultTestOptions()
	index, err := newNamespaceIndex(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, opts)
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
