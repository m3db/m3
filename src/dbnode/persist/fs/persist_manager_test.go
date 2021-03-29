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

package fs

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	m3ninxfs "github.com/m3db/m3/src/m3ninx/index/segment/fst"
	m3ninxpersist "github.com/m3db/m3/src/m3ninx/persist"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	m3test "github.com/m3db/m3/src/x/test"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPersistenceManagerPrepareDataFileExistsNoDelete(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pm, _, _, _ := testDataPersistManager(t, ctrl)
	defer os.RemoveAll(pm.filePathPrefix)

	var (
		shard              = uint32(0)
		blockStart         = time.Unix(1000, 0)
		shardDir           = createDataShardDir(t, pm.filePathPrefix, testNs1ID, shard)
		checkpointFilePath = filesetPathFromTimeLegacy(shardDir, blockStart, checkpointFileSuffix)
		checkpointFileBuf  = make([]byte, CheckpointFileSizeBytes)
	)
	createFile(t, checkpointFilePath, checkpointFileBuf)

	flush, err := pm.StartFlushPersist()
	require.NoError(t, err)

	defer func() {
		assert.NoError(t, flush.DoneFlush())
	}()

	prepareOpts := persist.DataPrepareOptions{
		NamespaceMetadata: testNs1Metadata(t),
		Shard:             shard,
		BlockStart:        blockStart,
	}
	prepared, err := flush.PrepareData(prepareOpts)
	require.Equal(t, errPersistManagerFileSetAlreadyExists, err)
	require.Nil(t, prepared.Persist)
	require.Nil(t, prepared.Close)
}

func TestPersistenceManagerPrepareDataFileExistsWithDelete(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pm, writer, _, _ := testDataPersistManager(t, ctrl)
	defer os.RemoveAll(pm.filePathPrefix)

	var (
		shard      = uint32(0)
		blockStart = time.Unix(1000, 0)
	)

	writerOpts := xtest.CmpMatcher(DataWriterOpenOptions{
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      shard,
			BlockStart: blockStart,
		},
		BlockSize: testBlockSize,
	}, m3test.IdentTransformer)
	writer.EXPECT().Open(writerOpts).Return(nil)

	var (
		shardDir           = createDataShardDir(t, pm.filePathPrefix, testNs1ID, shard)
		checkpointFilePath = filesetPathFromTimeLegacy(shardDir, blockStart, checkpointFileSuffix)
		checkpointFileBuf  = make([]byte, CheckpointFileSizeBytes)
	)
	createFile(t, checkpointFilePath, checkpointFileBuf)

	flush, err := pm.StartFlushPersist()
	require.NoError(t, err)

	defer func() {
		assert.NoError(t, flush.DoneFlush())
	}()

	prepareOpts := persist.DataPrepareOptions{
		NamespaceMetadata: testNs1Metadata(t),
		Shard:             shard,
		BlockStart:        blockStart,
		DeleteIfExists:    true,
	}
	prepared, err := flush.PrepareData(prepareOpts)
	require.NoError(t, err)
	require.NotNil(t, prepared.Persist)
	require.NotNil(t, prepared.Close)

	_, err = os.Open(checkpointFilePath)
	require.True(t, os.IsNotExist(err))
}

func TestPersistenceManagerPrepareOpenError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pm, writer, _, _ := testDataPersistManager(t, ctrl)
	defer os.RemoveAll(pm.filePathPrefix)

	ns1Md := testNs1Metadata(t)
	shard := uint32(0)
	blockStart := time.Unix(1000, 0)
	expectedErr := errors.New("foo")

	writerOpts := xtest.CmpMatcher(DataWriterOpenOptions{
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      shard,
			BlockStart: blockStart,
		},
		BlockSize: testBlockSize,
	}, m3test.IdentTransformer)
	writer.EXPECT().Open(writerOpts).Return(expectedErr)

	flush, err := pm.StartFlushPersist()
	require.NoError(t, err)

	defer func() {
		assert.NoError(t, flush.DoneFlush())
	}()

	prepareOpts := persist.DataPrepareOptions{
		NamespaceMetadata: ns1Md,
		Shard:             shard,
		BlockStart:        blockStart,
	}
	prepared, err := flush.PrepareData(prepareOpts)
	require.Equal(t, expectedErr, err)
	require.Nil(t, prepared.Persist)
	require.Nil(t, prepared.Close)
}

func TestPersistenceManagerPrepareSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pm, writer, _, _ := testDataPersistManager(t, ctrl)
	defer os.RemoveAll(pm.filePathPrefix)

	shard := uint32(0)
	blockStart := time.Unix(1000, 0)
	writerOpts := xtest.CmpMatcher(DataWriterOpenOptions{
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      shard,
			BlockStart: blockStart,
		},
		BlockSize: testBlockSize,
	}, m3test.IdentTransformer)
	writer.EXPECT().Open(writerOpts).Return(nil)

	var (
		id       = ident.StringID("foo")
		tags     = ident.NewTags(ident.StringTag("bar", "baz"))
		head     = checked.NewBytes([]byte{0x1, 0x2}, nil)
		tail     = checked.NewBytes([]byte{0x3, 0x4}, nil)
		segment  = ts.NewSegment(head, tail, 0, ts.FinalizeNone)
		checksum = segment.CalculateChecksum()
	)
	metadata := persist.NewMetadataFromIDAndTags(id, tags,
		persist.MetadataOptions{})
	writer.EXPECT().WriteAll(metadata, gomock.Any(), checksum).Return(nil)
	writer.EXPECT().Close()

	flush, err := pm.StartFlushPersist()
	require.NoError(t, err)

	defer func() {
		assert.NoError(t, flush.DoneFlush())
	}()

	now := time.Now()
	pm.start = now
	pm.count = 123
	pm.bytesWritten = 100

	prepareOpts := persist.DataPrepareOptions{
		NamespaceMetadata: testNs1Metadata(t),
		Shard:             shard,
		BlockStart:        blockStart,
	}
	prepared, err := flush.PrepareData(prepareOpts)
	defer prepared.Close()

	require.Nil(t, err)

	require.Nil(t, prepared.Persist(metadata, segment, checksum))

	require.True(t, pm.start.Equal(now))
	require.Equal(t, 124, pm.count)
	require.Equal(t, int64(104), pm.bytesWritten)
}

func TestPersistenceManagerPrepareSnapshotSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pm, writer, snapshotMetadataWriter, _ := testDataPersistManager(t, ctrl)
	defer os.RemoveAll(pm.filePathPrefix)

	shard := uint32(0)
	blockStart := time.Unix(1000, 0)
	writerOpts := xtest.CmpMatcher(DataWriterOpenOptions{
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      shard,
			BlockStart: blockStart,
		},
		BlockSize: testBlockSize,
		Snapshot: DataWriterSnapshotOptions{
			SnapshotID: testSnapshotID,
		},
	}, m3test.IdentTransformer)
	writer.EXPECT().Open(writerOpts).Return(nil)

	snapshotMetadataWriter.EXPECT().Write(SnapshotMetadataWriteArgs{
		ID: SnapshotMetadataIdentifier{
			Index: 0,
			UUID:  nil,
		},
		CommitlogIdentifier: persist.CommitLogFile{},
	}).Return(nil)

	var (
		id       = ident.StringID("foo")
		tags     = ident.NewTags(ident.StringTag("bar", "baz"))
		head     = checked.NewBytes([]byte{0x1, 0x2}, nil)
		tail     = checked.NewBytes([]byte{0x3, 0x4}, nil)
		segment  = ts.NewSegment(head, tail, 0, ts.FinalizeNone)
		checksum = segment.CalculateChecksum()
	)
	metadata := persist.NewMetadataFromIDAndTags(id, tags,
		persist.MetadataOptions{})
	writer.EXPECT().WriteAll(metadata, gomock.Any(), checksum).Return(nil)
	writer.EXPECT().Close()

	flush, err := pm.StartSnapshotPersist(testSnapshotID)
	require.NoError(t, err)

	defer func() {
		assert.NoError(t, flush.DoneSnapshot(nil, persist.CommitLogFile{}))
	}()

	now := time.Now()
	pm.start = now
	pm.count = 123
	pm.bytesWritten = 100

	prepareOpts := persist.DataPrepareOptions{
		NamespaceMetadata: testNs1Metadata(t),
		Shard:             shard,
		BlockStart:        blockStart,
	}
	prepared, err := flush.PrepareData(prepareOpts)
	defer prepared.Close()

	require.Nil(t, err)

	require.Nil(t, prepared.Persist(metadata, segment, checksum))

	require.True(t, pm.start.Equal(now))
	require.Equal(t, 124, pm.count)
	require.Equal(t, int64(104), pm.bytesWritten)
}

func TestPersistenceManagerCloseData(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pm, writer, _, _ := testDataPersistManager(t, ctrl)
	defer os.RemoveAll(pm.filePathPrefix)

	writer.EXPECT().Close()
	pm.closeData()
}

func TestPersistenceManagerPrepareIndexFileExists(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{T: t})
	defer ctrl.Finish()

	pm, writer, segWriter, _ := testIndexPersistManager(t, ctrl)
	defer os.RemoveAll(pm.filePathPrefix)

	blockStart := time.Unix(1000, 0)
	indexDir := createIndexDataDir(t, pm.filePathPrefix, testNs1ID)
	checkpointFilePath := filesetPathFromTimeAndIndex(indexDir, blockStart, 0, checkpointFileSuffix)

	digestBuf := digest.NewBuffer()
	digestBuf.WriteDigest(digest.Checksum([]byte("foo")))

	err := ioutil.WriteFile(checkpointFilePath, digestBuf, defaultNewFileMode)
	require.NoError(t, err)

	flush, err := pm.StartIndexPersist()
	require.NoError(t, err)

	defer func() {
		segWriter.EXPECT().Reset(nil)
		assert.NoError(t, flush.DoneIndex())
	}()
	volumeIndex, err := NextIndexFileSetVolumeIndex(
		pm.filePathPrefix,
		testNs1ID,
		blockStart,
	)
	require.NoError(t, err)

	prepareOpts := persist.IndexPrepareOptions{
		NamespaceMetadata: testNs1Metadata(t),
		BlockStart:        blockStart,
		VolumeIndex:       volumeIndex,
	}
	writer.EXPECT().Open(xtest.CmpMatcher(
		IndexWriterOpenOptions{
			BlockSize: testBlockSize,
			Identifier: FileSetFileIdentifier{
				FileSetContentType: persist.FileSetIndexContentType,
				BlockStart:         blockStart,
				Namespace:          testNs1ID,
				VolumeIndex:        1,
			},
		}, m3test.IdentTransformer),
	).Return(nil)
	prepared, err := flush.PrepareIndex(prepareOpts)
	require.NoError(t, err)
	require.NotNil(t, prepared.Persist)
	require.NotNil(t, prepared.Close)
}

func TestPersistenceManagerPrepareIndexOpenError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pm, writer, segWriter, _ := testIndexPersistManager(t, ctrl)
	defer os.RemoveAll(pm.filePathPrefix)

	ns1Md := testNs1Metadata(t)
	blockStart := time.Unix(1000, 0)
	expectedErr := errors.New("foo")

	writerOpts := xtest.CmpMatcher(IndexWriterOpenOptions{
		Identifier: FileSetFileIdentifier{
			FileSetContentType: persist.FileSetIndexContentType,
			Namespace:          testNs1ID,
			BlockStart:         blockStart,
		},
		BlockSize: testBlockSize,
	}, m3test.IdentTransformer)
	writer.EXPECT().Open(writerOpts).Return(expectedErr)

	flush, err := pm.StartIndexPersist()
	require.NoError(t, err)

	defer func() {
		segWriter.EXPECT().Reset(nil)
		assert.NoError(t, flush.DoneIndex())
	}()

	prepareOpts := persist.IndexPrepareOptions{
		NamespaceMetadata: ns1Md,
		BlockStart:        blockStart,
	}
	prepared, err := flush.PrepareIndex(prepareOpts)
	require.Equal(t, expectedErr, err)
	require.Nil(t, prepared.Persist)
	require.Nil(t, prepared.Close)
}

func TestPersistenceManagerPrepareIndexSuccess(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{T: t})
	defer ctrl.Finish()

	pm, writer, segWriter, _ := testIndexPersistManager(t, ctrl)
	defer os.RemoveAll(pm.filePathPrefix)

	flush, err := pm.StartIndexPersist()
	require.NoError(t, err)

	defer func() {
		segWriter.EXPECT().Reset(nil)
		assert.NoError(t, flush.DoneIndex())
	}()

	// We support preparing multiple index block writers for an index persist.
	numBlocks := 10
	blockStart := time.Unix(1000, 0)
	for i := 1; i < numBlocks; i++ {
		blockStart = blockStart.Add(time.Duration(i) * testBlockSize)
		writerOpts := IndexWriterOpenOptions{
			Identifier: FileSetFileIdentifier{
				FileSetContentType: persist.FileSetIndexContentType,
				Namespace:          testNs1ID,
				BlockStart:         blockStart,
			},
			BlockSize: testBlockSize,
		}
		writer.EXPECT().Open(xtest.CmpMatcher(writerOpts, m3test.IdentTransformer)).Return(nil)

		prepareOpts := persist.IndexPrepareOptions{
			NamespaceMetadata: testNs1Metadata(t),
			BlockStart:        blockStart,
		}
		prepared, err := flush.PrepareIndex(prepareOpts)
		require.NoError(t, err)

		seg := segment.NewMockMutableSegment(ctrl)
		segWriter.EXPECT().Reset(seg).Return(nil)
		writer.EXPECT().WriteSegmentFileSet(segWriter).Return(nil)
		require.NoError(t, prepared.Persist(seg))

		reader := NewMockIndexFileSetReader(ctrl)
		pm.indexPM.newReaderFn = func(Options) (IndexFileSetReader, error) {
			return reader, nil
		}

		reader.EXPECT().Open(xtest.CmpMatcher(IndexReaderOpenOptions{
			Identifier: writerOpts.Identifier,
		}, m3test.IdentTransformer)).Return(IndexReaderOpenResult{}, nil)

		file := NewMockIndexSegmentFile(ctrl)
		gomock.InOrder(
			reader.EXPECT().SegmentFileSets().Return(1),
			reader.EXPECT().ReadSegmentFileSet().Return(file, nil),
			reader.EXPECT().ReadSegmentFileSet().Return(nil, io.EOF),
		)
		fsSeg := m3ninxfs.NewMockSegment(ctrl)
		pm.indexPM.newPersistentSegmentFn = func(
			fset m3ninxpersist.IndexSegmentFileSet, opts m3ninxfs.Options,
		) (m3ninxfs.Segment, error) {
			require.Equal(t, file, fset)
			return fsSeg, nil
		}

		writer.EXPECT().Close().Return(nil)
		segs, err := prepared.Close()
		require.NoError(t, err)
		require.Len(t, segs, 1)
		require.Equal(t, fsSeg, segs[0])
	}
}

func TestPersistenceManagerNoRateLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pm, writer, _, _ := testDataPersistManager(t, ctrl)
	defer os.RemoveAll(pm.filePathPrefix)

	shard := uint32(0)
	blockStart := time.Unix(1000, 0)
	writerOpts := xtest.CmpMatcher(DataWriterOpenOptions{
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      shard,
			BlockStart: blockStart,
		},
		BlockSize: testBlockSize,
	}, m3test.IdentTransformer)
	writer.EXPECT().Open(writerOpts).Return(nil)

	var (
		now      time.Time
		slept    time.Duration
		id       = ident.StringID("foo")
		tags     = ident.NewTags(ident.StringTag("bar", "baz"))
		head     = checked.NewBytes([]byte{0x1, 0x2}, nil)
		tail     = checked.NewBytes([]byte{0x3}, nil)
		segment  = ts.NewSegment(head, tail, 0, ts.FinalizeNone)
		checksum = segment.CalculateChecksum()
	)

	pm.nowFn = func() time.Time { return now }
	pm.sleepFn = func(d time.Duration) { slept += d }

	metadata := persist.NewMetadataFromIDAndTags(id, tags,
		persist.MetadataOptions{})
	writer.EXPECT().
		WriteAll(metadata, pm.dataPM.segmentHolder, checksum).
		Return(nil).
		Times(2)

	flush, err := pm.StartFlushPersist()
	require.NoError(t, err)

	defer func() {
		assert.NoError(t, flush.DoneFlush())
	}()

	// prepare the flush
	prepareOpts := persist.DataPrepareOptions{
		NamespaceMetadata: testNs1Metadata(t),
		Shard:             shard,
		BlockStart:        blockStart,
	}
	prepared, err := flush.PrepareData(prepareOpts)
	require.NoError(t, err)

	// Start persistence
	now = time.Now()
	require.NoError(t, prepared.Persist(metadata, segment, checksum))

	// Advance time and write again
	now = now.Add(time.Millisecond)
	require.NoError(t, prepared.Persist(metadata, segment, checksum))

	// Check there is no rate limiting
	require.Equal(t, time.Duration(0), slept)
	require.Equal(t, int64(6), pm.bytesWritten)
}

func TestPersistenceManagerWithRateLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pm, writer, _, opts := testDataPersistManager(t, ctrl)
	defer os.RemoveAll(pm.filePathPrefix)

	shard := uint32(0)
	blockStart := time.Unix(1000, 0)

	var (
		now      time.Time
		slept    time.Duration
		iter     = 2
		id       = ident.StringID("foo")
		head     = checked.NewBytes([]byte{0x1, 0x2}, nil)
		tail     = checked.NewBytes([]byte{0x3}, nil)
		segment  = ts.NewSegment(head, tail, 0, ts.FinalizeNone)
		checksum = segment.CalculateChecksum()
	)

	pm.nowFn = func() time.Time { return now }
	pm.sleepFn = func(d time.Duration) { slept += d }

	writerOpts := xtest.CmpMatcher(DataWriterOpenOptions{
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      shard,
			BlockStart: blockStart,
		},
		BlockSize: testBlockSize,
	}, m3test.IdentTransformer)
	metadata := persist.NewMetadataFromIDAndTags(id, ident.Tags{},
		persist.MetadataOptions{})
	writer.EXPECT().Open(writerOpts).Return(nil).Times(iter)
	writer.EXPECT().
		WriteAll(metadata, pm.dataPM.segmentHolder, checksum).
		Return(nil).
		AnyTimes()
	writer.EXPECT().Close().Times(iter)

	// Enable rate limiting
	runtimeOpts := opts.RuntimeOptionsManager().Get()
	opts.RuntimeOptionsManager().Update(
		runtimeOpts.SetPersistRateLimitOptions(
			runtimeOpts.PersistRateLimitOptions().
				SetLimitEnabled(true).
				SetLimitCheckEvery(2).
				SetLimitMbps(16.0)))

	// Wait until enabled
	for func() bool {
		pm.Lock()
		defer pm.Unlock()
		return !pm.currRateLimitOpts.LimitEnabled()
	}() {
		time.Sleep(10 * time.Millisecond)
	}

	for i := 0; i < iter; i++ {
		// Reset
		slept = time.Duration(0)

		flush, err := pm.StartFlushPersist()
		require.NoError(t, err)

		// prepare the flush
		prepareOpts := persist.DataPrepareOptions{
			NamespaceMetadata: testNs1Metadata(t),
			Shard:             shard,
			BlockStart:        blockStart,
		}
		prepared, err := flush.PrepareData(prepareOpts)
		require.NoError(t, err)

		// Start persistence
		now = time.Now()
		require.NoError(t, prepared.Persist(metadata, segment, checksum))

		// Assert we don't rate limit if the count is not enough yet
		require.NoError(t, prepared.Persist(metadata, segment, checksum))
		require.Equal(t, time.Duration(0), slept)

		// Advance time and check we rate limit if the disk throughput exceeds the limit
		now = now.Add(time.Microsecond)
		require.NoError(t, prepared.Persist(metadata, segment, checksum))
		require.Equal(t, time.Duration(1861), slept)

		// Advance time and check we don't rate limit if the disk throughput is below the limit
		require.NoError(t, prepared.Persist(metadata, segment, checksum))
		now = now.Add(time.Second - time.Microsecond)
		require.NoError(t, prepared.Persist(metadata, segment, checksum))
		require.Equal(t, time.Duration(1861), slept)

		require.Equal(t, int64(15), pm.bytesWritten)

		require.NoError(t, prepared.Close())

		assert.NoError(t, flush.DoneFlush())
	}
}

func TestPersistenceManagerNamespaceSwitch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pm, writer, _, _ := testDataPersistManager(t, ctrl)
	defer os.RemoveAll(pm.filePathPrefix)

	shard := uint32(0)
	blockStart := time.Unix(1000, 0)

	flush, err := pm.StartFlushPersist()
	require.NoError(t, err)

	defer func() {
		assert.NoError(t, flush.DoneFlush())
	}()

	writerOpts := xtest.CmpMatcher(DataWriterOpenOptions{
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      shard,
			BlockStart: blockStart,
		},
		BlockSize: testBlockSize,
	}, m3test.IdentTransformer)
	writer.EXPECT().Open(writerOpts).Return(nil)
	prepareOpts := persist.DataPrepareOptions{
		NamespaceMetadata: testNs1Metadata(t),
		Shard:             shard,
		BlockStart:        blockStart,
	}
	prepared, err := flush.PrepareData(prepareOpts)
	require.NoError(t, err)
	require.NotNil(t, prepared.Persist)
	require.NotNil(t, prepared.Close)

	writerOpts = xtest.CmpMatcher(DataWriterOpenOptions{
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs2ID,
			Shard:      shard,
			BlockStart: blockStart,
		},
		BlockSize: testBlockSize,
	}, m3test.IdentTransformer)
	writer.EXPECT().Open(writerOpts).Return(nil)
	prepareOpts = persist.DataPrepareOptions{
		NamespaceMetadata: testNs2Metadata(t),
		Shard:             shard,
		BlockStart:        blockStart,
	}
	prepared, err = flush.PrepareData(prepareOpts)
	require.NoError(t, err)
	require.NotNil(t, prepared.Persist)
	require.NotNil(t, prepared.Close)
}

func createDataShardDir(t *testing.T, prefix string, namespace ident.ID, shard uint32) string {
	shardDirPath := ShardDataDirPath(prefix, namespace, shard)
	err := os.MkdirAll(shardDirPath, os.ModeDir|os.FileMode(0755))
	require.Nil(t, err)
	return shardDirPath
}

func createIndexDataDir(t *testing.T, prefix string, namespace ident.ID) string {
	path := NamespaceIndexDataDirPath(prefix, namespace)
	err := os.MkdirAll(path, os.ModeDir|os.FileMode(0755))
	require.Nil(t, err)
	return path
}

func testDataPersistManager(
	t *testing.T,
	ctrl *gomock.Controller,
) (*persistManager, *MockDataFileSetWriter, *MockSnapshotMetadataFileWriter, Options) {
	dir := createTempDir(t)

	opts := testDefaultOpts.
		SetFilePathPrefix(dir).
		SetWriterBufferSize(10)

	var (
		fileSetWriter          = NewMockDataFileSetWriter(ctrl)
		snapshotMetadataWriter = NewMockSnapshotMetadataFileWriter(ctrl)
	)

	mgr, err := NewPersistManager(opts)
	require.NoError(t, err)

	manager := mgr.(*persistManager)
	manager.dataPM.writer = fileSetWriter
	manager.dataPM.snapshotMetadataWriter = snapshotMetadataWriter
	manager.dataPM.nextSnapshotMetadataFileIndex = func(Options) (int64, error) {
		return 0, nil
	}

	return manager, fileSetWriter, snapshotMetadataWriter, opts
}

func testIndexPersistManager(t *testing.T, ctrl *gomock.Controller,
) (*persistManager, *MockIndexFileSetWriter, *m3ninxpersist.MockMutableSegmentFileSetWriter, Options) {
	dir := createTempDir(t)

	opts := testDefaultOpts.
		SetFilePathPrefix(dir).
		SetWriterBufferSize(10)

	writer := NewMockIndexFileSetWriter(ctrl)
	segmentWriter := m3ninxpersist.NewMockMutableSegmentFileSetWriter(ctrl)

	mgr, err := NewPersistManager(opts)
	require.NoError(t, err)

	manager := mgr.(*persistManager)
	manager.indexPM.newIndexWriterFn = func(opts Options) (IndexFileSetWriter, error) {
		return writer, nil
	}
	manager.indexPM.segmentWriter = segmentWriter
	return manager, writer, segmentWriter, opts
}
