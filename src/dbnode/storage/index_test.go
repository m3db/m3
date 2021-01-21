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
	"io/ioutil"
	"os"
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNamespaceIndexCleanupExpiredFilesets(t *testing.T) {
	md := testNamespaceMetadata(time.Hour, time.Hour*8)
	nsIdx, err := newNamespaceIndex(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, DefaultTestOptions())
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

func TestNamespaceIndexCleanupDuplicateFilesets(t *testing.T) {
	md := testNamespaceMetadata(time.Hour, time.Hour*8)
	nsIdx, err := newNamespaceIndex(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, DefaultTestOptions())
	require.NoError(t, err)

	idx := nsIdx.(*nsIndex)
	now := time.Now().Truncate(time.Hour)
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
				BlockStart: blockTime.UnixNano(),
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
				BlockStart: blockTime.UnixNano(),
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
				BlockStart: blockTime.UnixNano(),
				BlockSize:  int64(indexBlockSize),
				Shards:     []uint32{0, 1, 2, 3},
				IndexVolumeType: &protobuftypes.StringValue{
					Value: volumeType,
				},
			},
			AbsoluteFilePaths: []string{fset3.Name()},
		},
	}

	idx.readIndexInfoFilesFn = func(
		filePathPrefix string,
		namespace ident.ID,
		readerBufferSize int,
	) []fs.ReadIndexInfoFileResult {
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
	require.NoError(t, idx.CleanupDuplicateFileSets())
}

func TestNamespaceIndexCleanupDuplicateFilesetsNoop(t *testing.T) {
	md := testNamespaceMetadata(time.Hour, time.Hour*8)
	nsIdx, err := newNamespaceIndex(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, DefaultTestOptions())
	require.NoError(t, err)

	idx := nsIdx.(*nsIndex)
	now := time.Now().Truncate(time.Hour)
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
				BlockStart: blockTime.UnixNano(),
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
				BlockStart: blockTime.UnixNano(),
				BlockSize:  int64(indexBlockSize),
				Shards:     []uint32{4},
				IndexVolumeType: &protobuftypes.StringValue{
					Value: volumeType,
				},
			},
			AbsoluteFilePaths: []string{fset2.Name()},
		},
	}

	idx.readIndexInfoFilesFn = func(
		filePathPrefix string,
		namespace ident.ID,
		readerBufferSize int,
	) []fs.ReadIndexInfoFileResult {
		return infoFiles
	}
	idx.deleteFilesFn = func(s []string) error {
		require.Equal(t, []string{}, s)
		return nil
	}
	require.NoError(t, idx.CleanupDuplicateFileSets())
}

func TestNamespaceIndexCleanupExpiredFilesetsWithBlocks(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{T: t})
	defer ctrl.Finish()

	md := testNamespaceMetadata(time.Hour, time.Hour*8)
	nsIdx, err := newNamespaceIndex(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, DefaultTestOptions())
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
	ctrl := gomock.NewController(xtest.Reporter{T: t})
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
	ctrl := gomock.NewController(xtest.Reporter{T: t})
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
	ctrl := gomock.NewController(xtest.Reporter{T: t})
	defer ctrl.Finish()

	test := newTestIndex(t, ctrl)

	now := time.Now().Truncate(test.indexBlockSize)
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
	idx.state.blocksByTime[xtime.ToUnixNano(blockTime)] = mockBlock

	mockBlock.EXPECT().IsSealed().Return(true)
	mockBlock.EXPECT().Close().Return(nil)

	mockShard := NewMockdatabaseShard(ctrl)
	mockShard.EXPECT().ID().Return(uint32(0)).AnyTimes()
	mockShard.EXPECT().FlushState(gomock.Any()).Return(fileOpState{WarmStatus: fileOpFailed}, nil).AnyTimes()
	shards := []databaseShard{mockShard}

	mockFlush := persist.NewMockIndexFlush(ctrl)

	require.NoError(t, idx.WarmFlush(mockFlush, shards))
}

func TestNamespaceIndexQueryNoMatchingBlocks(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{T: t})
	defer ctrl.Finish()

	test := newTestIndex(t, ctrl)

	now := time.Now().Truncate(test.indexBlockSize)
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
	idx.state.blocksByTime[xtime.ToUnixNano(blockTime)] = mockBlock

	ctx := context.NewContext()
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

	// Wide query on the non-overlapping range
	err = idx.WideQuery(ctx, query, make(chan *ident.IDBatch),
		index.WideQueryOptions{
			StartInclusive: start,
			EndExclusive:   end,
		})
	require.NoError(t, err)
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
		now                = time.Now()
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
		return now
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
		idx.state.blocksByTime[xtime.ToUnixNano(blockStart)] = mockBlock

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
			mockShard.EXPECT().FlushState(blockStart).Return(fileOpState{WarmStatus: fileOpSuccess}, nil)
			mockShard.EXPECT().FlushState(blockStart.Add(blockSize)).Return(fileOpState{WarmStatus: fileOpSuccess}, nil)

			resultsTags1 := ident.NewTagsIterator(ident.NewTags())
			resultsTags2 := ident.NewTagsIterator(ident.NewTags())
			resultsInShard := []block.FetchBlocksMetadataResult{
				block.FetchBlocksMetadataResult{
					ID:   resultsID1,
					Tags: resultsTags1,
				},
				block.FetchBlocksMetadataResult{
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
		}

		mockBlock.EXPECT().IsSealed().Return(true)
		mockBlock.EXPECT().AddResults(gomock.Any()).Return(nil)
		mockBlock.EXPECT().EvictMutableSegments().Return(nil)
	}
	require.NoError(t, idx.WarmFlush(mockFlush, dbShards))
	require.Equal(t, numBlocks, persistClosedTimes)
	require.Equal(t, numBlocks, persistCalledTimes)
	require.Equal(t, expectedDocs, actualDocs)
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
