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
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xresource "github.com/m3db/m3/src/x/resource"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testNs1ID       = ident.StringID("testNs")
	testNs2ID       = ident.StringID("testNs2")
	testNs1Metadata = func(t *testing.T) namespace.Metadata {
		md, err := namespace.NewMetadata(testNs1ID, namespace.NewOptions().
			SetCacheBlocksOnRetrieve(true).
			SetRetentionOptions(retention.NewOptions().SetBlockSize(testBlockSize)).
			SetIndexOptions(namespace.NewIndexOptions().SetEnabled(true).SetBlockSize(testBlockSize)))
		require.NoError(t, err)
		return md
	}
	testNs2Metadata = func(t *testing.T) namespace.Metadata {
		md, err := namespace.NewMetadata(testNs2ID, namespace.NewOptions().
			SetCacheBlocksOnRetrieve(true).
			SetRetentionOptions(retention.NewOptions().SetBlockSize(testBlockSize)).
			SetIndexOptions(namespace.NewIndexOptions().SetEnabled(true).SetBlockSize(testBlockSize)))
		require.NoError(t, err)
		return md
	}
)

func TestOpenFilesFails(t *testing.T) {
	testFilePath := "/not/a/real/path"
	expectedErr := errors.New("synthetic error")

	opener := func(filePath string) (*os.File, error) {
		assert.Equal(t, filePath, testFilePath)
		return nil, expectedErr
	}

	var fd *os.File
	err := openFiles(opener, map[string]**os.File{
		testFilePath: &fd,
	})
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
}

func TestCloseAllFails(t *testing.T) {
	file := createTempFile(t)
	defer os.Remove(file.Name())

	assert.NoError(t, file.Close())
	assert.Error(t, xresource.CloseAll(file))
}

func TestDeleteFiles(t *testing.T) {
	var files []string
	iter := 3

	for i := 0; i < iter; i++ {
		fd := createTempFile(t)
		fd.Close()
		files = append(files, fd.Name())
	}

	// Add a non-existent file path
	files = append(files, "/not/a/real/path")

	require.Error(t, DeleteFiles(files))
	for i := 0; i < iter; i++ {
		require.True(t, !mustFileExists(t, files[i]))
	}
}

func TestDeleteInactiveDirectories(t *testing.T) {
	tempPrefix, err := ioutil.TempDir("", "filespath")
	require.NoError(t, err)
	defer func() {
		os.RemoveAll(tempPrefix)
	}()
	namespaceDir := NamespaceDataDirPath(tempPrefix, testNs1ID)

	// Test shard deletion within a namespace
	shards := []uint32{uint32(4), uint32(5), uint32(6)}
	shardDirs := []string{"4", "5", "6"}
	for _, shard := range shards {
		shardDir := ShardDataDirPath(tempPrefix, testNs1ID, shard)
		err := os.MkdirAll(shardDir, defaultNewDirectoryMode)
		require.NoError(t, err)

		shardPath := path.Join(shardDir, "data.txt")
		_, err = os.Create(shardPath)
		require.NoError(t, err)
	}

	activeShards := shardDirs[1:]
	err = DeleteInactiveDirectories(namespaceDir, activeShards)
	require.NoError(t, err)
	dirs, err := ioutil.ReadDir(namespaceDir)
	require.NoError(t, err)
	require.Equal(t, 2, len(dirs))
	os.RemoveAll(namespaceDir)
}

func TestByTimeAscending(t *testing.T) {
	files := []string{"foo/fileset-1-info.db", "foo/fileset-12-info.db", "foo/fileset-2-info.db"}
	expected := []string{"foo/fileset-1-info.db", "foo/fileset-2-info.db", "foo/fileset-12-info.db"}
	sort.Sort(byTimeAscending(files))
	require.Equal(t, expected, files)
}

func TestForEachInfoFile(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shard := uint32(0)
	shardDir := ShardDataDirPath(dir, testNs1ID, shard)
	require.NoError(t, os.MkdirAll(shardDir, os.ModeDir|os.FileMode(0755)))

	blockStart := time.Unix(0, 0)
	buf := digest.NewBuffer()

	// No checkpoint file
	createDataFile(t, shardDir, blockStart, infoFileSuffix, nil)

	// No digest file
	blockStart = blockStart.Add(time.Nanosecond)
	createDataFile(t, shardDir, blockStart, infoFileSuffix, nil)
	createDataFile(t, shardDir, blockStart, checkpointFileSuffix, buf)

	// Digest of digest mismatch
	blockStart = blockStart.Add(time.Nanosecond)
	digests := []byte{0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc}
	buf.WriteDigest(digest.Checksum(append(digests, 0xd)))
	createDataFile(t, shardDir, blockStart, infoFileSuffix, nil)
	createDataFile(t, shardDir, blockStart, digestFileSuffix, digests)
	createDataFile(t, shardDir, blockStart, checkpointFileSuffix, buf)

	// Info file digest mismatch
	blockStart = blockStart.Add(time.Nanosecond)

	buf.WriteDigest(digest.Checksum(digests))
	createDataFile(t, shardDir, blockStart, infoFileSuffix, []byte{0x1})
	createDataFile(t, shardDir, blockStart, digestFileSuffix, digests)
	createDataFile(t, shardDir, blockStart, checkpointFileSuffix, buf)

	// All digests match
	blockStart = blockStart.Add(time.Nanosecond)
	infoData := []byte{0x1, 0x2, 0x3, 0x4}

	buf.WriteDigest(digest.Checksum(infoData))

	digestOfDigest := append(buf, []byte{0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc}...)
	buf.WriteDigest(digest.Checksum(digestOfDigest))
	createDataFile(t, shardDir, blockStart, infoFileSuffix, infoData)
	createDataFile(t, shardDir, blockStart, digestFileSuffix, digestOfDigest)
	createDataFile(t, shardDir, blockStart, checkpointFileSuffix, buf)

	var fnames []string
	var res []byte
	forEachInfoFile(
		forEachInfoFileSelector{
			fileSetType:    persist.FileSetFlushType,
			contentType:    persist.FileSetDataContentType,
			filePathPrefix: dir,
			namespace:      testNs1ID,
			shard:          shard,
		},
		testReaderBufferSize,
		func(file FileSetFile, data []byte, _ bool) {
			fname, ok := file.InfoFilePath()
			require.True(t, ok)
			fnames = append(fnames, fname)
			res = append(res, data...)
		})

	require.Equal(t, []string{filesetPathFromTimeLegacy(shardDir, blockStart, infoFileSuffix)}, fnames)
	require.Equal(t, infoData, res)
}

func TestTimeFromFileName(t *testing.T) {
	_, err := TimeFromFileName("foo/bar")
	require.Error(t, err)
	require.Equal(t, "unexpected filename: foo/bar", err.Error())

	_, err = TimeFromFileName("foo/bar-baz")
	require.Error(t, err)

	v, err := TimeFromFileName("foo-1-bar.db")
	expected := time.Unix(0, 1)
	require.Equal(t, expected, v)
	require.NoError(t, err)

	v, err = TimeFromFileName("foo-12345-6-bar.db")
	expected = time.Unix(0, 12345)
	require.Equal(t, expected, v)
	require.NoError(t, err)

	v, err = TimeFromFileName("foo/bar/foo-21234567890-bar.db")
	expected = time.Unix(0, 21234567890)
	require.Equal(t, expected, v)
	require.NoError(t, err)
}

func TestTimeAndIndexFromCommitlogFileName(t *testing.T) {
	_, _, err := TimeAndIndexFromCommitlogFilename("foo/bar")
	require.Error(t, err)
	require.Equal(t, "unexpected filename: foo/bar", err.Error())

	_, _, err = TimeAndIndexFromCommitlogFilename("foo/bar-baz")
	require.Error(t, err)

	type expected struct {
		t time.Time
		i int
	}
	ts, i, err := TimeAndIndexFromCommitlogFilename("foo-1-0.db")
	exp := expected{time.Unix(0, 1), 0}
	require.Equal(t, exp.t, ts)
	require.Equal(t, exp.i, i)
	require.NoError(t, err)

	ts, i, err = TimeAndIndexFromCommitlogFilename("foo/bar/foo-21234567890-1.db")
	exp = expected{time.Unix(0, 21234567890), 1}
	require.Equal(t, exp.t, ts)
	require.Equal(t, exp.i, i)
	require.NoError(t, err)
}

func TestTimeAndVolumeIndexFromFileSetFilename(t *testing.T) {
	_, _, err := TimeAndVolumeIndexFromFileSetFilename("foo/bar")
	require.Error(t, err)
	require.Equal(t, "unexpected filename: foo/bar", err.Error())

	_, _, err = TimeAndVolumeIndexFromFileSetFilename("foo/bar-baz")
	require.Error(t, err)

	type expected struct {
		t time.Time
		i int
	}
	ts, i, err := TimeAndVolumeIndexFromFileSetFilename("foo-1-0-data.db")
	exp := expected{time.Unix(0, 1), 0}
	require.Equal(t, exp.t, ts)
	require.Equal(t, exp.i, i)
	require.NoError(t, err)

	validName := "foo/bar/fileset-21234567890-1-data.db"
	ts, i, err = TimeAndVolumeIndexFromFileSetFilename(validName)
	exp = expected{time.Unix(0, 21234567890), 1}
	require.Equal(t, exp.t, ts)
	require.Equal(t, exp.i, i)
	require.NoError(t, err)
	require.Equal(t, filesetPathFromTimeAndIndex("foo/bar", exp.t, exp.i, "data"), validName)
}

func TestTimeAndVolumeIndexFromDataFileSetFilename(t *testing.T) {
	_, _, err := TimeAndVolumeIndexFromDataFileSetFilename("foo/bar")
	require.Error(t, err)
	require.Equal(t, "unexpected filename: foo/bar", err.Error())

	_, _, err = TimeAndVolumeIndexFromDataFileSetFilename("foo/bar-baz")
	require.Error(t, err)

	type expected struct {
		t time.Time
		i int
	}
	ts, i, err := TimeAndVolumeIndexFromDataFileSetFilename("foo-1-0-data.db")
	exp := expected{time.Unix(0, 1), 0}
	require.Equal(t, exp.t, ts)
	require.Equal(t, exp.i, i)
	require.NoError(t, err)

	validName := "foo/bar/fileset-21234567890-1-data.db"
	ts, i, err = TimeAndVolumeIndexFromDataFileSetFilename(validName)
	exp = expected{time.Unix(0, 21234567890), 1}
	require.Equal(t, exp.t, ts)
	require.Equal(t, exp.i, i)
	require.NoError(t, err)
	require.Equal(t, dataFilesetPathFromTimeAndIndex("foo/bar", exp.t, exp.i, "data", false), validName)

	unindexedName := "foo/bar/fileset-21234567890-data.db"
	ts, i, err = TimeAndVolumeIndexFromDataFileSetFilename(unindexedName)
	exp = expected{time.Unix(0, 21234567890), 0}
	require.Equal(t, exp.t, ts)
	require.Equal(t, exp.i, i)
	require.NoError(t, err)
	require.Equal(t, dataFilesetPathFromTimeAndIndex("foo/bar", exp.t, exp.i, "data", true), unindexedName)
}

func TestSnapshotMetadataFilePathFromIdentifierRoundTrip(t *testing.T) {
	idUUID := uuid.Parse("bf58eb3e-0582-42ee-83b2-d098c206260e")
	require.NotNil(t, idUUID)

	var (
		prefix = "/var/lib/m3db"
		id     = SnapshotMetadataIdentifier{
			Index: 10,
			UUID:  idUUID,
		}
	)

	var (
		expected = "/var/lib/m3db/snapshots/snapshot-bf58eb3e058242ee83b2d098c206260e-10-metadata.db"
		actual   = snapshotMetadataFilePathFromIdentifier(prefix, id)
	)
	require.Equal(t, expected, actual)

	idFromPath, err := snapshotMetadataIdentifierFromFilePath(expected)
	require.NoError(t, err)
	require.Equal(t, id, idFromPath)
}

func TestSnapshotMetadataCheckpointFilePathFromIdentifierRoundTrip(t *testing.T) {
	idUUID := uuid.Parse("bf58eb3e-0582-42ee-83b2-d098c206260e")
	require.NotNil(t, idUUID)

	var (
		prefix = "/var/lib/m3db"
		id     = SnapshotMetadataIdentifier{
			Index: 10,
			UUID:  idUUID,
		}
	)

	var (
		expected = "/var/lib/m3db/snapshots/snapshot-bf58eb3e058242ee83b2d098c206260e-10-metadata-checkpoint.db"
		actual   = snapshotMetadataCheckpointFilePathFromIdentifier(prefix, id)
	)
	require.Equal(t, expected, actual)

	idFromPath, err := snapshotMetadataIdentifierFromFilePath(expected)
	require.NoError(t, err)
	require.Equal(t, id, idFromPath)
}

func TestSanitizedUUIDsCanBeParsed(t *testing.T) {
	u := uuid.Parse("bf58eb3e-0582-42ee-83b2-d098c206260e")
	require.NotNil(t, u)

	parsedUUID, ok := parseUUID(sanitizeUUID(u))
	require.True(t, ok)
	require.Equal(t, u.String(), parsedUUID.String())
}

func TestFileExists(t *testing.T) {
	var (
		dir               = createTempDir(t)
		shard             = uint32(10)
		start             = time.Now()
		shardDir          = ShardDataDirPath(dir, testNs1ID, shard)
		checkpointFileBuf = make([]byte, CheckpointFileSizeBytes)
		err               = os.MkdirAll(shardDir, defaultNewDirectoryMode)
	)
	defer os.RemoveAll(dir)
	require.NoError(t, err)

	infoFilePath := filesetPathFromTimeLegacy(shardDir, start, infoFileSuffix)
	createDataFile(t, shardDir, start, infoFileSuffix, checkpointFileBuf)
	require.True(t, mustFileExists(t, infoFilePath))
	exists, err := DataFileSetExists(dir, testNs1ID, uint32(shard), start, 0)
	require.NoError(t, err)
	require.False(t, exists)

	checkpointFilePath := filesetPathFromTimeLegacy(shardDir, start, checkpointFileSuffix)
	createDataFile(t, shardDir, start, checkpointFileSuffix, checkpointFileBuf)
	exists, err = DataFileSetExists(dir, testNs1ID, uint32(shard), start, 0)
	require.NoError(t, err)
	require.True(t, exists)
	exists, err = DataFileSetExists(dir, testNs1ID, uint32(shard), start, 1)
	require.NoError(t, err)
	require.False(t, exists)

	exists, err = CompleteCheckpointFileExists(checkpointFilePath)
	require.NoError(t, err)
	require.True(t, exists)

	defer instrument.SetShouldPanicEnvironmentVariable(true)()
	require.Panics(t, func() {
		_, _ = FileExists(checkpointFilePath)
	})

	os.Remove(infoFilePath)
	require.False(t, mustFileExists(t, infoFilePath))
}

func TestCompleteCheckpointFileExists(t *testing.T) {
	var (
		dir                = createTempDir(t)
		shard              = uint32(10)
		start              = time.Now()
		shardDir           = ShardDataDirPath(dir, testNs1ID, shard)
		checkpointFilePath = filesetPathFromTimeLegacy(shardDir, start, checkpointFileSuffix)
		err                = os.MkdirAll(shardDir, defaultNewDirectoryMode)

		validCheckpointFileBuf   = make([]byte, CheckpointFileSizeBytes)
		invalidCheckpointFileBuf = make([]byte, CheckpointFileSizeBytes+1)
	)
	defer os.RemoveAll(dir)
	require.NoError(t, err)

	createDataFile(t, shardDir, start, checkpointFileSuffix, invalidCheckpointFileBuf)
	exists, err := CompleteCheckpointFileExists(checkpointFilePath)
	require.NoError(t, err)
	require.False(t, exists)

	createDataFile(t, shardDir, start, checkpointFileSuffix, validCheckpointFileBuf)
	exists, err = CompleteCheckpointFileExists(checkpointFilePath)
	require.NoError(t, err)
	require.True(t, exists)

	defer instrument.SetShouldPanicEnvironmentVariable(true)()
	require.Panics(t, func() { _, _ = CompleteCheckpointFileExists("some-arbitrary-file") })
}

func TestShardDirPath(t *testing.T) {
	require.Equal(t, "foo/bar/data/testNs/12", ShardDataDirPath("foo/bar", testNs1ID, 12))
	require.Equal(t, "foo/bar/data/testNs/12", ShardDataDirPath("foo/bar/", testNs1ID, 12))
}

func TestFilePathFromTime(t *testing.T) {
	start := time.Unix(1465501321, 123456789)
	inputs := []struct {
		prefix   string
		suffix   string
		expected string
	}{
		{"foo/bar", infoFileSuffix, "foo/bar/fileset-1465501321123456789-info.db"},
		{"foo/bar", indexFileSuffix, "foo/bar/fileset-1465501321123456789-index.db"},
		{"foo/bar", dataFileSuffix, "foo/bar/fileset-1465501321123456789-data.db"},
		{"foo/bar", checkpointFileSuffix, "foo/bar/fileset-1465501321123456789-checkpoint.db"},
		{"foo/bar/", infoFileSuffix, "foo/bar/fileset-1465501321123456789-info.db"},
	}
	for _, input := range inputs {
		require.Equal(t, input.expected, filesetPathFromTimeLegacy(input.prefix, start, input.suffix))
	}
}

func TestFileSetFilesBefore(t *testing.T) {
	shard := uint32(0)
	dir := createDataFlushInfoFilesDir(t, testNs1ID, shard, 20)
	defer os.RemoveAll(dir)

	cutoffIter := 8
	cutoff := time.Unix(0, int64(cutoffIter))
	res, err := DataFileSetsBefore(dir, testNs1ID, shard, cutoff)
	require.NoError(t, err)
	require.Equal(t, cutoffIter, len(res))

	shardDir := path.Join(dir, dataDirName, testNs1ID.String(), strconv.Itoa(int(shard)))
	for i := 0; i < len(res); i++ {
		ts := time.Unix(0, int64(i))
		require.Equal(t, filesetPathFromTimeLegacy(shardDir, ts, infoFileSuffix), res[i])
	}
}

func TestFileSetAt(t *testing.T) {
	shard := uint32(0)
	numIters := 20
	dir := createDataCheckpointFilesDir(t, testNs1ID, shard, numIters)
	defer os.RemoveAll(dir)

	for i := 0; i < numIters; i++ {
		timestamp := time.Unix(0, int64(i))
		res, ok, err := FileSetAt(dir, testNs1ID, shard, timestamp, 0)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, timestamp, res.ID.BlockStart)
	}
}

func TestFileSetAtNonLegacy(t *testing.T) {
	shard := uint32(0)
	numIters := 20
	dir := createDataFiles(t, dataDirName, testNs1ID, shard, numIters, true, checkpointFileSuffix)
	defer os.RemoveAll(dir)

	for i := 0; i < numIters; i++ {
		timestamp := time.Unix(0, int64(i))
		res, ok, err := FileSetAt(dir, testNs1ID, shard, timestamp, 0)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, timestamp, res.ID.BlockStart)
	}
}

func TestFileSetAtNotFirstVolumeIndex(t *testing.T) {
	shard := uint32(0)
	numIters := 20
	volumeIndex := 1
	dir := createDataFilesWithVolumeIndex(t, dataDirName, testNs1ID, shard, numIters, true,
		checkpointFileSuffix, volumeIndex)
	defer os.RemoveAll(dir)

	for i := 0; i < numIters; i++ {
		timestamp := time.Unix(0, int64(i))
		res, ok, err := FileSetAt(dir, testNs1ID, shard, timestamp, volumeIndex)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, timestamp, res.ID.BlockStart)
	}
}

func TestFileSetAtIgnoresWithoutCheckpoint(t *testing.T) {
	shard := uint32(0)
	numIters := 20
	dir := createDataFlushInfoFilesDir(t, testNs1ID, shard, numIters)
	defer os.RemoveAll(dir)

	for i := 0; i < numIters; i++ {
		timestamp := time.Unix(0, int64(i))
		_, ok, err := FileSetAt(dir, testNs1ID, shard, timestamp, 0)
		require.NoError(t, err)
		require.False(t, ok)
	}
}

func TestDeleteFileSetAt(t *testing.T) {
	shard := uint32(0)
	numIters := 20
	dir := createDataCheckpointFilesDir(t, testNs1ID, shard, numIters)
	defer os.RemoveAll(dir)

	for i := 0; i < numIters; i++ {
		timestamp := time.Unix(0, int64(i))
		res, ok, err := FileSetAt(dir, testNs1ID, shard, timestamp, 0)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, timestamp, res.ID.BlockStart)

		err = DeleteFileSetAt(dir, testNs1ID, shard, timestamp, 0)
		require.NoError(t, err)

		res, ok, err = FileSetAt(dir, testNs1ID, shard, timestamp, 0)
		require.NoError(t, err)
		require.False(t, ok)
	}
}

func TestFileSetAtNotExist(t *testing.T) {
	shard := uint32(0)
	dir := createDataFlushInfoFilesDir(t, testNs1ID, shard, 0)
	defer os.RemoveAll(dir)

	timestamp := time.Unix(0, 0)
	_, ok, err := FileSetAt(dir, testNs1ID, shard, timestamp, 0)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestFileSetFilesNoFiles(t *testing.T) {
	// Make empty directory
	shard := uint32(0)
	dir := createTempDir(t)
	shardDir := path.Join(dir, "data", testNs1ID.String(), strconv.Itoa(int(shard)))
	require.NoError(t, os.MkdirAll(shardDir, 0755))
	defer os.RemoveAll(shardDir)

	res, err := filesetFiles(filesetFilesSelector{
		filePathPrefix: dir,
		namespace:      testNs1ID,
		shard:          shard,
		pattern:        filesetFilePattern,
	})
	require.NoError(t, err)
	require.Equal(t, 0, len(res))
}

func TestSnapshotFiles(t *testing.T) {
	var (
		shard          = uint32(0)
		dir            = createTempDir(t)
		filePathPrefix = filepath.Join(dir, "")
	)
	defer os.RemoveAll(dir)

	// Write out snapshot file
	writeOutTestSnapshot(t, filePathPrefix, shard, testWriterStart, 0)

	// Load snapshot files
	snapshotFiles, err := SnapshotFiles(filePathPrefix, testNs1ID, shard)
	require.NoError(t, err)
	require.Equal(t, 1, len(snapshotFiles))
	snapshotTime, snapshotID, err := snapshotFiles[0].SnapshotTimeAndID()
	require.NoError(t, err)
	require.True(t, testWriterStart.Equal(snapshotTime))
	require.Equal(t, testSnapshotID, snapshotID)
	require.False(t, testWriterStart.IsZero())
}

func TestSnapshotFilesNoFiles(t *testing.T) {
	// Make empty directory
	shard := uint32(0)
	dir := createTempDir(t)
	shardDir := path.Join(dir, "snapshots", testNs1ID.String(), strconv.Itoa(int(shard)))
	require.NoError(t, os.MkdirAll(shardDir, 0755))
	defer os.RemoveAll(shardDir)

	files, err := SnapshotFiles(dir, testNs1ID, shard)
	require.NoError(t, err)
	require.Equal(t, 0, len(files))
	for i, snapshotFile := range files {
		require.Equal(t, int64(i), snapshotFile.ID.BlockStart.UnixNano())
	}

	require.Equal(t, 0, len(files.Filepaths()))
}

func TestNextSnapshotFileSetVolumeIndex(t *testing.T) {
	var (
		shard      = uint32(0)
		dir        = createTempDir(t)
		shardDir   = ShardSnapshotsDirPath(dir, testNs1ID, shard)
		blockStart = time.Now().Truncate(time.Hour)
	)
	require.NoError(t, os.MkdirAll(shardDir, 0755))
	defer os.RemoveAll(shardDir)

	index, err := NextSnapshotFileSetVolumeIndex(
		dir, testNs1ID, shard, blockStart)
	require.NoError(t, err)
	require.Equal(t, 0, index)

	// Check increments properly
	curr := -1
	for i := 0; i <= 10; i++ {
		index, err := NextSnapshotFileSetVolumeIndex(dir, testNs1ID,
			shard, blockStart)
		require.NoError(t, err)
		require.Equal(t, curr+1, index)
		curr = index

		writeOutTestSnapshot(t, dir, shard, blockStart, index)
	}
}

// TestSortedSnapshotMetadataFiles tests the SortedSnapshotMetadataFiles function by writing out
// a number of valid snapshot metadata files (along with their checkpoint files), as
// well as one invalid / corrupt one, and then asserts that the correct number of valid
// and corrupt files are returned.
func TestSortedSnapshotMetadataFiles(t *testing.T) {
	var (
		dir            = createTempDir(t)
		filePathPrefix = filepath.Join(dir, "")
		opts           = testDefaultOpts.
				SetFilePathPrefix(filePathPrefix)
		commitlogIdentifier = persist.CommitLogFile{
			FilePath: "some_path",
			Index:    0,
		}
		numMetadataFiles = 10
	)
	defer func() {
		os.RemoveAll(dir)
	}()

	// Shoulld be no files before we write them out.
	metadataFiles, errorsWithpaths, err := SortedSnapshotMetadataFiles(opts)
	require.NoError(t, err)
	require.Empty(t, errorsWithpaths)
	require.Empty(t, metadataFiles)

	// Write out a bunch of metadata files along with their corresponding checkpoints.
	for i := 0; i < numMetadataFiles; i++ {
		snapshotUUID := uuid.Parse("6645a373-bf82-42e7-84a6-f8452b137549")
		require.NotNil(t, snapshotUUID)

		snapshotMetadataIdentifier := SnapshotMetadataIdentifier{
			Index: int64(i),
			UUID:  snapshotUUID,
		}

		writer := NewSnapshotMetadataWriter(opts)
		err := writer.Write(SnapshotMetadataWriteArgs{
			ID:                  snapshotMetadataIdentifier,
			CommitlogIdentifier: commitlogIdentifier,
		})
		require.NoError(t, err)

		reader := NewSnapshotMetadataReader(opts)
		snapshotMetadata, err := reader.Read(snapshotMetadataIdentifier)
		require.NoError(t, err)

		require.Equal(t, SnapshotMetadata{
			ID:                  snapshotMetadataIdentifier,
			CommitlogIdentifier: commitlogIdentifier,
			MetadataFilePath: snapshotMetadataFilePathFromIdentifier(
				filePathPrefix, snapshotMetadataIdentifier),
			CheckpointFilePath: snapshotMetadataCheckpointFilePathFromIdentifier(
				filePathPrefix, snapshotMetadataIdentifier),
		}, snapshotMetadata)

		// Corrupt the last file.
		if i == numMetadataFiles-1 {
			os.Remove(snapshotMetadataCheckpointFilePathFromIdentifier(
				filePathPrefix, snapshotMetadataIdentifier))
		}
	}

	metadataFiles, errorsWithpaths, err = SortedSnapshotMetadataFiles(opts)
	require.NoError(t, err)
	require.Len(t, errorsWithpaths, 1)
	require.Len(t, metadataFiles, numMetadataFiles-1)

	// Assert that they're sorted.
	for i, file := range metadataFiles {
		require.Equal(t, int64(i), file.ID.Index)
	}
}

// TestNextSnapshotMetadataFileIndex tests the NextSnapshotMetadataFileIndex function by
// writing out a number of SnapshotMetadata files and then ensuring that the NextSnapshotMetadataFileIndex
// function returns the correct next index.
func TestNextSnapshotMetadataFileIndex(t *testing.T) {
	var (
		dir            = createTempDir(t)
		filePathPrefix = filepath.Join(dir, "")
		opts           = testDefaultOpts.
				SetFilePathPrefix(filePathPrefix)
		commitlogIdentifier = persist.CommitLogFile{
			FilePath: "some_path",
			Index:    0,
		}
		numMetadataFiles = 10
	)
	defer func() {
		os.RemoveAll(dir)
	}()

	// Shoulld be no files before we write them out.
	metadataFiles, errorsWithpaths, err := SortedSnapshotMetadataFiles(opts)
	require.NoError(t, err)
	require.Empty(t, errorsWithpaths)
	require.Empty(t, metadataFiles)

	writer := NewSnapshotMetadataWriter(opts)
	// Write out a bunch of metadata files along with their corresponding checkpoints.
	for i := 0; i < numMetadataFiles; i++ {
		snapshotUUID := uuid.Parse("6645a373-bf82-42e7-84a6-f8452b137549")
		require.NotNil(t, snapshotUUID)

		snapshotMetadataIdentifier := SnapshotMetadataIdentifier{
			Index: int64(i),
			UUID:  snapshotUUID,
		}

		err := writer.Write(SnapshotMetadataWriteArgs{
			ID:                  snapshotMetadataIdentifier,
			CommitlogIdentifier: commitlogIdentifier,
		})
		require.NoError(t, err)
	}

	nextIdx, err := NextSnapshotMetadataFileIndex(opts)
	require.NoError(t, err)
	// Snapshot metadata file indices are zero-based so if we wrote out
	// numMetadataFiles, then the last index should be numMetadataFiles-1
	// and the next one should be numMetadataFiles.
	require.Equal(t, int64(numMetadataFiles), nextIdx)
}

func TestNextIndexFileSetVolumeIndex(t *testing.T) {
	// Make empty directory
	dir := createTempDir(t)
	dataDir := NamespaceIndexDataDirPath(dir, testNs1ID)
	require.NoError(t, os.MkdirAll(dataDir, 0755))
	defer os.RemoveAll(dataDir)

	blockStart := time.Now().Truncate(time.Hour)

	// Check increments properly
	curr := -1
	for i := 0; i <= 10; i++ {
		index, err := NextIndexFileSetVolumeIndex(dir, testNs1ID, blockStart)
		require.NoError(t, err)
		require.Equal(t, curr+1, index)
		curr = index

		p := filesetPathFromTimeAndIndex(dataDir, blockStart, index, checkpointFileSuffix)

		digestBuf := digest.NewBuffer()
		digestBuf.WriteDigest(digest.Checksum([]byte("bar")))

		err = ioutil.WriteFile(p, digestBuf, defaultNewFileMode)
		require.NoError(t, err)
	}
}

func TestMultipleForBlockStart(t *testing.T) {
	numSnapshots := 20
	numSnapshotsPerBlock := 4
	shard := uint32(0)
	dir := createTempDir(t)
	defer os.RemoveAll(dir)
	shardDir := path.Join(dir, snapshotDirName, testNs1ID.String(), strconv.Itoa(int(shard)))
	require.NoError(t, os.MkdirAll(shardDir, 0755))

	// Write out many files with the same blockStart, but different indices
	ts := time.Unix(0, 0)
	for i := 0; i < numSnapshots; i++ {
		volume := i % numSnapshotsPerBlock
		// Periodically update the blockStart
		if volume == 0 {
			ts = time.Unix(0, int64(i))
		}

		writeOutTestSnapshot(t, dir, shard, ts, volume)
	}

	files, err := SnapshotFiles(dir, testNs1ID, shard)
	require.NoError(t, err)
	require.Equal(t, 20, len(files))
	require.Equal(t, 20*7, len(files.Filepaths()))

	// Make sure LatestForBlock works even if the input list is not sorted properly
	for i := range files {
		if i+1 < len(files) {
			files[i], files[i+1] = files[i+1], files[i]
		}
	}

	latestSnapshot, ok := files.LatestVolumeForBlock(ts)
	require.True(t, ok)
	require.Equal(t, numSnapshotsPerBlock-1, latestSnapshot.ID.VolumeIndex)
}

func TestSnapshotFileHasCompleteCheckpointFile(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	checkpointFilePath := path.Join(dir, "123-checkpoint-0.db")

	// Test a valid complete checkpoint file
	digestBuffer := digest.NewBuffer()
	digestBuffer.WriteDigest(digest.Checksum([]byte{1, 2, 3}))
	err := ioutil.WriteFile(checkpointFilePath, digestBuffer, defaultNewFileMode)
	require.NoError(t, err)

	// Check validates a valid checkpoint file
	f := FileSetFile{
		AbsoluteFilePaths: []string{checkpointFilePath},
	}
	require.Equal(t, true, f.HasCompleteCheckpointFile())

	// Check fails when checkpoint exists but not valid
	err = ioutil.WriteFile(checkpointFilePath, []byte{42}, defaultNewFileMode)
	require.NoError(t, err)
	f = FileSetFile{
		AbsoluteFilePaths: []string{checkpointFilePath},
	}
	require.Equal(t, false, f.HasCompleteCheckpointFile())

	// Check ignores index file path
	indexFilePath := path.Join(dir, "123-index-0.db")
	f = FileSetFile{
		AbsoluteFilePaths: []string{indexFilePath},
	}
	require.Equal(t, false, f.HasCompleteCheckpointFile())
}

func TestSnapshotDirPath(t *testing.T) {
	require.Equal(t, "prefix/snapshots", SnapshotDirPath("prefix"))
}

func TestNamespaceSnapshotsDirPath(t *testing.T) {
	expected := "prefix/snapshots/testNs"
	actual := NamespaceSnapshotsDirPath("prefix", testNs1ID)
	require.Equal(t, expected, actual)
}

func TestShardSnapshotsDirPath(t *testing.T) {
	expected := "prefix/snapshots/testNs/0"
	actual := ShardSnapshotsDirPath("prefix", testNs1ID, 0)
	require.Equal(t, expected, actual)
}

func TestSnapshotFileSetExistsAt(t *testing.T) {
	var (
		shard     = uint32(0)
		ts        = time.Unix(0, 0)
		dir       = createTempDir(t)
		shardPath = ShardSnapshotsDirPath(dir, testNs1ID, 0)
	)
	require.NoError(t, os.MkdirAll(shardPath, 0755))

	writeOutTestSnapshot(t, dir, shard, ts, 0)

	exists, err := SnapshotFileSetExistsAt(dir, testNs1ID, testSnapshotID, shard, ts)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestSortedCommitLogFiles(t *testing.T) {
	iter := 20
	dir := createCommitLogFiles(t, iter)
	defer os.RemoveAll(dir)

	files, err := SortedCommitLogFiles(CommitLogsDirPath(dir))
	require.NoError(t, err)
	require.Equal(t, iter, len(files))

	for i := 0; i < iter; i++ {
		require.Equal(
			t,
			path.Join(dir, "commitlogs", fmt.Sprintf("commitlog-0-%d.db", i)),
			files[i])
	}
}

func TestIndexFileSetAt(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	var (
		ns1     = ident.StringID("abc")
		now     = time.Now().Truncate(time.Hour)
		timeFor = func(n int) time.Time { return now.Add(time.Hour * time.Duration(n)) }
	)

	files := indexFileSetFileIdentifiers{
		indexFileSetFileIdentifier{
			FileSetFileIdentifier: FileSetFileIdentifier{
				BlockStart:         timeFor(1),
				Namespace:          ns1,
				VolumeIndex:        0,
				FileSetContentType: persist.FileSetIndexContentType,
			},
			Suffix: checkpointFileSuffix,
		},
	}
	files.create(t, dir)

	results, err := IndexFileSetsAt(dir, ns1, timeFor(1))
	require.NoError(t, err)
	require.Len(t, results, 1)
}

func TestIndexFileSetAtIgnoresLackOfCheckpoint(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	var (
		ns1     = ident.StringID("abc")
		now     = time.Now().Truncate(time.Hour)
		timeFor = func(n int) time.Time { return now.Add(time.Hour * time.Duration(n)) }
	)

	files := indexFileSetFileIdentifiers{
		indexFileSetFileIdentifier{
			FileSetFileIdentifier: FileSetFileIdentifier{
				BlockStart:         timeFor(1),
				Namespace:          ns1,
				VolumeIndex:        0,
				FileSetContentType: persist.FileSetIndexContentType,
			},
			Suffix: checkpointFileSuffix,
		},
		indexFileSetFileIdentifier{
			FileSetFileIdentifier: FileSetFileIdentifier{
				BlockStart:         timeFor(2),
				Namespace:          ns1,
				VolumeIndex:        0,
				FileSetContentType: persist.FileSetIndexContentType,
			},
			Suffix: infoFileSuffix,
		},
	}
	files.create(t, dir)

	results, err := IndexFileSetsAt(dir, ns1, timeFor(1))
	require.NoError(t, err)
	require.Len(t, results, 1)
}

func TestIndexFileSetAtMultiple(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	var (
		ns1     = ident.StringID("abc")
		now     = time.Now().Truncate(time.Hour)
		timeFor = func(n int) time.Time { return now.Add(time.Hour * time.Duration(n)) }
	)

	files := indexFileSetFileIdentifiers{
		indexFileSetFileIdentifier{
			FileSetFileIdentifier: FileSetFileIdentifier{
				BlockStart:         timeFor(1),
				Namespace:          ns1,
				VolumeIndex:        0,
				FileSetContentType: persist.FileSetIndexContentType,
			},
			Suffix: checkpointFileSuffix,
		},
		indexFileSetFileIdentifier{
			FileSetFileIdentifier: FileSetFileIdentifier{
				BlockStart:         timeFor(1),
				Namespace:          ns1,
				VolumeIndex:        1,
				FileSetContentType: persist.FileSetIndexContentType,
			},
			Suffix: checkpointFileSuffix,
		},
		indexFileSetFileIdentifier{
			FileSetFileIdentifier: FileSetFileIdentifier{
				BlockStart:         timeFor(1),
				Namespace:          ns1,
				VolumeIndex:        2,
				FileSetContentType: persist.FileSetIndexContentType,
			},
			Suffix: checkpointFileSuffix,
		},
	}
	files.create(t, dir)

	results, err := IndexFileSetsAt(dir, ns1, timeFor(1))
	require.NoError(t, err)
	require.Len(t, results, 3)
	for i := range files {
		require.Equal(t, files[i].Namespace.String(), results[i].ID.Namespace.String())
		require.Equal(t, files[i].BlockStart, results[i].ID.BlockStart)
		require.Equal(t, files[i].VolumeIndex, results[i].ID.VolumeIndex)
	}
}

func TestIndexFileSetsBefore(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	var (
		ns1     = ident.StringID("abc")
		now     = time.Now().Truncate(time.Hour)
		timeFor = func(n int) time.Time { return now.Add(time.Hour * time.Duration(n)) }
	)

	files := indexFileSetFileIdentifiers{
		indexFileSetFileIdentifier{
			FileSetFileIdentifier: FileSetFileIdentifier{
				BlockStart:         timeFor(1),
				Namespace:          ns1,
				VolumeIndex:        0,
				FileSetContentType: persist.FileSetIndexContentType,
			},
			Suffix: checkpointFileSuffix,
		},
		indexFileSetFileIdentifier{
			FileSetFileIdentifier: FileSetFileIdentifier{
				BlockStart:         timeFor(1),
				Namespace:          ns1,
				VolumeIndex:        1,
				FileSetContentType: persist.FileSetIndexContentType,
			},
			Suffix: checkpointFileSuffix,
		},
		indexFileSetFileIdentifier{
			FileSetFileIdentifier: FileSetFileIdentifier{
				BlockStart:         timeFor(2),
				Namespace:          ns1,
				VolumeIndex:        0,
				FileSetContentType: persist.FileSetIndexContentType,
			},
			Suffix: checkpointFileSuffix,
		},
		indexFileSetFileIdentifier{
			FileSetFileIdentifier: FileSetFileIdentifier{
				BlockStart:         timeFor(3),
				Namespace:          ns1,
				VolumeIndex:        0,
				FileSetContentType: persist.FileSetIndexContentType,
			},
			Suffix: checkpointFileSuffix,
		},
	}
	files.create(t, dir)

	results, err := IndexFileSetsBefore(dir, ns1, timeFor(3))
	require.NoError(t, err)
	require.Len(t, results, 3)
	for _, res := range results {
		require.False(t, strings.Contains(res, fmt.Sprintf("%d", timeFor(3).UnixNano())))
	}
}

func TestSnapshotFileSnapshotTimeAndID(t *testing.T) {
	var (
		dir            = createTempDir(t)
		filePathPrefix = filepath.Join(dir, "")
	)
	defer os.RemoveAll(dir)

	// Write out snapshot file
	writeOutTestSnapshot(t, filePathPrefix, 0, testWriterStart, 0)

	// Load snapshot files
	snapshotFiles, err := SnapshotFiles(filePathPrefix, testNs1ID, 0)
	require.NoError(t, err)
	require.Equal(t, 1, len(snapshotFiles))

	// Verify SnapshotTimeAndID() returns the expected time
	snapshotTime, snapshotID, err := SnapshotTimeAndID(filePathPrefix, snapshotFiles[0].ID)
	require.NoError(t, err)
	require.Equal(t, true, testWriterStart.Equal(snapshotTime))
	require.Equal(t, testSnapshotID, snapshotID)
}

func TestSnapshotFileSnapshotTimeAndIDZeroValue(t *testing.T) {
	f := FileSetFile{}
	_, _, err := f.SnapshotTimeAndID()
	require.Equal(t, errSnapshotTimeAndIDZero, err)
}

func TestSnapshotFileSnapshotTimeAndIDNotSnapshot(t *testing.T) {
	f := FileSetFile{}
	f.AbsoluteFilePaths = []string{"/var/lib/m3db/data/fileset-data.db"}
	_, _, err := f.SnapshotTimeAndID()
	require.Error(t, err)
}

func TestCommitLogFilePath(t *testing.T) {
	expected := "/var/lib/m3db/commitlogs/commitlog-0-1.db"
	actual := CommitLogFilePath("/var/lib/m3db", 1)
	require.Equal(t, expected, actual)
}

func createTempFile(t *testing.T) *os.File {
	fd, err := ioutil.TempFile("", "testfile")
	require.NoError(t, err)
	return fd
}

func createTempDir(t *testing.T) string {
	dir, err := ioutil.TempDir("", "testdir")
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func createFile(t *testing.T, filePath string, b []byte) {
	fd, err := os.Create(filePath)
	require.NoError(t, err)
	if b != nil {
		fd.Write(b)
	}
	fd.Close()
}

func createDataFlushInfoFilesDir(t *testing.T, namespace ident.ID, shard uint32, iter int) string {
	return createDataInfoFiles(t, dataDirName, namespace, shard, iter, false)
}

func createDataCheckpointFilesDir(t *testing.T, namespace ident.ID, shard uint32, iter int) string {
	return createDataCheckpointFiles(t, dataDirName, namespace, shard, iter, false)
}

func createDataInfoFiles(t *testing.T, subDirName string, namespace ident.ID, shard uint32, iter int, isSnapshot bool) string {
	return createDataFiles(t, subDirName, namespace, shard, iter, isSnapshot, infoFileSuffix)
}

func createDataCheckpointFiles(t *testing.T, subDirName string, namespace ident.ID, shard uint32, iter int, isSnapshot bool) string {
	return createDataFiles(t, subDirName, namespace, shard, iter, isSnapshot, checkpointFileSuffix)
}

func createDataFilesWithVolumeIndex(t *testing.T,
	subDirName string, namespace ident.ID, shard uint32, iter int, isSnapshot bool, fileSuffix string, volumeIndex int,
) string {
	dir := createTempDir(t)
	shardDir := path.Join(dir, subDirName, namespace.String(), strconv.Itoa(int(shard)))
	require.NoError(t, os.MkdirAll(shardDir, 0755))
	for i := 0; i < iter; i++ {
		ts := time.Unix(0, int64(i))
		var infoFilePath string
		if isSnapshot {
			infoFilePath = filesetPathFromTimeAndIndex(shardDir, ts, volumeIndex, fileSuffix)
		} else {
			infoFilePath = filesetPathFromTimeLegacy(shardDir, ts, fileSuffix)
		}
		var contents []byte
		if fileSuffix == checkpointFileSuffix {
			// If writing a checkpoint file then write out a checksum of contents
			// so that when code that validates the checkpoint file runs it returns
			// successfully
			digestBuf := digest.NewBuffer()
			digestBuf.WriteDigest(digest.Checksum(contents))
			contents = []byte(digestBuf)
		}
		createFile(t, infoFilePath, contents)
	}
	return dir
}

func createDataFiles(t *testing.T,
	subDirName string, namespace ident.ID, shard uint32, iter int, isSnapshot bool, fileSuffix string,
) string {
	return createDataFilesWithVolumeIndex(t, subDirName, namespace, shard, iter, isSnapshot, fileSuffix, 0)
}

type indexFileSetFileIdentifier struct {
	FileSetFileIdentifier
	Suffix string
}

type indexFileSetFileIdentifiers []indexFileSetFileIdentifier

func (indexFilesets indexFileSetFileIdentifiers) create(t *testing.T, prefixDir string) {
	for _, fileset := range indexFilesets {
		idents := fileSetFileIdentifiers{fileset.FileSetFileIdentifier}
		idents.create(t, prefixDir, persist.FileSetFlushType, fileset.Suffix)
	}
}

type fileSetFileIdentifiers []FileSetFileIdentifier

func (filesets fileSetFileIdentifiers) create(t *testing.T, prefixDir string, fileSetType persist.FileSetType, fileSuffixes ...string) {
	writeFile := func(t *testing.T, path string, contents []byte) {
		if strings.Contains(path, checkpointFileSuffix) {
			// If writing a checkpoint file then write out a checksum of contents
			// so that when code that validates the checkpoint file runs it returns
			// successfully
			digestBuf := digest.NewBuffer()
			digestBuf.WriteDigest(digest.Checksum(contents))
			contents = []byte(digestBuf)
		}
		createFile(t, path, contents)
	}

	for _, suffix := range fileSuffixes {
		for _, fileset := range filesets {
			switch fileset.FileSetContentType {
			case persist.FileSetDataContentType:
				ns := fileset.Namespace.String()
				shard := fileset.Shard
				blockStart := fileset.BlockStart
				shardDir := path.Join(prefixDir, dataDirName, ns, strconv.Itoa(int(shard)))
				require.NoError(t, os.MkdirAll(shardDir, 0755))
				var path string
				switch fileSetType {
				case persist.FileSetFlushType:
					path = filesetPathFromTimeLegacy(shardDir, blockStart, suffix)
					writeFile(t, path, nil)
				case persist.FileSetSnapshotType:
					path = filesetPathFromTimeAndIndex(shardDir, blockStart, 0, fileSuffix)
					writeFile(t, path, nil)
				default:
					panic("unknown FileSetType")
				}
			case persist.FileSetIndexContentType:
				ns := fileset.Namespace.String()
				blockStart := fileset.BlockStart
				volumeIndex := fileset.VolumeIndex
				indexDir := path.Join(prefixDir, indexDirName, dataDirName, ns)
				require.NoError(t, os.MkdirAll(indexDir, 0755))
				var path string
				switch fileSetType {
				case persist.FileSetFlushType:
					path = filesetPathFromTimeAndIndex(indexDir, blockStart, volumeIndex, suffix)
					writeFile(t, path, nil)
				case persist.FileSetSnapshotType:
					fallthrough
				default:
					panic("unknown FileSetType")
				}
			default:
				panic("unknown file type")
			}
		}
	}
}

func createDataFile(t *testing.T, shardDir string, blockStart time.Time, suffix string, b []byte) {
	filePath := filesetPathFromTimeLegacy(shardDir, blockStart, suffix)
	createFile(t, filePath, b)
}

func createCommitLogFiles(t *testing.T, iter int) string {
	dir := createTempDir(t)
	commitLogsDir := path.Join(dir, commitLogsDirName)
	assert.NoError(t, os.Mkdir(commitLogsDir, 0755))
	for i := 0; i < iter; i++ {
		filePath := CommitLogFilePath(dir, i)
		fd, err := os.Create(filePath)
		assert.NoError(t, err)
		assert.NoError(t, fd.Close())
	}
	return dir
}

func writeOutTestSnapshot(
	t *testing.T, filePathPrefix string,
	shard uint32, blockStart time.Time, volume int) {
	var (
		entries = []testEntry{
			{"foo", nil, []byte{1, 2, 3}},
			{"bar", nil, []byte{4, 5, 6}},
			{"baz", nil, make([]byte, 65536)},
			{"cat", nil, make([]byte, 100000)},
			{"echo", nil, []byte{7, 8, 9}},
		}
		w = newTestWriter(t, filePathPrefix)
	)
	defer w.Close()

	writeTestDataWithVolume(
		t, w, shard, blockStart, volume, entries, persist.FileSetSnapshotType)
}

func mustFileExists(t *testing.T, path string) bool {
	exists, err := FileExists(path)
	require.NoError(t, err)
	return exists
}
