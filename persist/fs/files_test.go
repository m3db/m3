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
	"hash/adler32"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTempFile(t *testing.T) *os.File {
	file, err := ioutil.TempFile("", "testfile")
	if err != nil {
		t.Fatal(err)
	}
	return file
}

func createTempDir(t *testing.T) string {
	dir, err := ioutil.TempDir("", "foo")
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func createFile(t *testing.T, shardDir string, blockStart time.Time, suffix string, b []byte) {
	filePath := filepathFromTime(shardDir, blockStart, suffix)
	fd, err := os.Create(filePath)
	require.NoError(t, err)
	if b != nil {
		fd.Write(b)
	}
	fd.Close()
}

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

func TestCloseFilesFails(t *testing.T) {
	file := createTempFile(t)
	defer os.Remove(file.Name())

	assert.NoError(t, file.Close())
	assert.Error(t, closeFiles(file))
}

func TestByTimeAscending(t *testing.T) {
	files := []string{"foo/1-info.db", "foo/12-info.db", "foo/2-info.db"}
	expected := []string{"foo/1-info.db", "foo/2-info.db", "foo/12-info.db"}
	sort.Sort(byTimeAscending(files))
	require.Equal(t, expected, files)
}

func TestReadCheckpointFileNonExistent(t *testing.T) {
	shardDir := "/non/existent"
	_, err := readCheckpointFile(shardDir, time.Now(), nil)
	require.Equal(t, errCheckpointFileNotFound, err)
}

func TestReadCheckpointFileSuccess(t *testing.T) {
	shardDir := createTempDir(t)
	defer os.RemoveAll(shardDir)

	blockStart := time.Now()
	path := filepathFromTime(shardDir, blockStart, checkpointFileSuffix)
	fd, err := os.Create(path)
	require.NoError(t, err)
	buf := make([]byte, digestLen)
	digest := adler32.New()
	digest.Write([]byte{0x1, 0x2})
	require.NoError(t, writeDigestToFile(fd, digest, buf))
	fd.Close()

	res, err := readCheckpointFile(shardDir, blockStart, buf)
	require.NoError(t, err)
	require.Equal(t, digest.Sum32(), res)
}

func TestForEachInfoFile(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shard := uint32(0)
	shardDir := shardDirPath(dir, shard)
	require.NoError(t, os.MkdirAll(shardDir, os.ModeDir|os.FileMode(0755)))

	blockStart := time.Unix(0, 0)
	buf := make([]byte, digestLen)
	digest := adler32.New()

	// No checkpoint file
	createFile(t, shardDir, blockStart, infoFileSuffix, nil)

	// No digest file
	blockStart = blockStart.Add(time.Nanosecond)
	createFile(t, shardDir, blockStart, infoFileSuffix, nil)
	createFile(t, shardDir, blockStart, checkpointFileSuffix, buf)

	// Digest of digest mismatch
	blockStart = blockStart.Add(time.Nanosecond)
	digests := []byte{0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc}
	digest.Write(append(digests, 0xd))
	writeDigest(digest, buf)
	createFile(t, shardDir, blockStart, infoFileSuffix, nil)
	createFile(t, shardDir, blockStart, digestFileSuffix, digests)
	createFile(t, shardDir, blockStart, checkpointFileSuffix, buf)

	// Info file digest mismatch
	blockStart = blockStart.Add(time.Nanosecond)
	digest.Reset()
	digest.Write(digests)
	writeDigest(digest, buf)
	createFile(t, shardDir, blockStart, infoFileSuffix, []byte{0x1})
	createFile(t, shardDir, blockStart, digestFileSuffix, digests)
	createFile(t, shardDir, blockStart, checkpointFileSuffix, buf)

	// All digests match
	blockStart = blockStart.Add(time.Nanosecond)
	infoData := []byte{0x1, 0x2, 0x3, 0x4}
	digest.Reset()
	digest.Write(infoData)
	writeDigest(digest, buf)
	digestOfDigest := append(buf, []byte{0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc}...)
	digest.Reset()
	digest.Write(digestOfDigest)
	writeDigest(digest, buf)
	createFile(t, shardDir, blockStart, infoFileSuffix, infoData)
	createFile(t, shardDir, blockStart, digestFileSuffix, digestOfDigest)
	createFile(t, shardDir, blockStart, checkpointFileSuffix, buf)

	var fnames []string
	var res []byte
	ForEachInfoFile(dir, shard, func(fname string, data []byte) {
		fnames = append(fnames, fname)
		res = append(res, data...)
	})

	require.Equal(t, []string{filepathFromTime(shardDir, blockStart, infoFileSuffix)}, fnames)
	require.Equal(t, infoData, res)
}

func TestTimeFromName(t *testing.T) {
	_, err := TimeFromFileName("foo/bar")
	require.Error(t, err)
	require.Equal(t, "unexpected file name foo/bar", err.Error())

	_, err = TimeFromFileName("foo/bar-baz")
	require.Error(t, err)

	v, err := TimeFromFileName("1-test.db")
	expected := time.Unix(0, 1)
	require.Equal(t, expected, v)
	require.NoError(t, err)

	v, err = TimeFromFileName("foo/bar/21234567890-test.db")
	expected = time.Unix(0, 21234567890)
	require.Equal(t, expected, v)
	require.NoError(t, err)
}

func TestShardDirPath(t *testing.T) {
	require.Equal(t, "foo/bar/12", shardDirPath("foo/bar", 12))
	require.Equal(t, "foo/bar/12", shardDirPath("foo/bar/", 12))
}

func TestFilePathFromTime(t *testing.T) {
	start := time.Unix(1465501321, 123456789)
	inputs := []struct {
		prefix   string
		suffix   string
		expected string
	}{
		{"foo/bar", infoFileSuffix, "foo/bar/1465501321123456789-info.db"},
		{"foo/bar", indexFileSuffix, "foo/bar/1465501321123456789-index.db"},
		{"foo/bar", dataFileSuffix, "foo/bar/1465501321123456789-data.db"},
		{"foo/bar", checkpointFileSuffix, "foo/bar/1465501321123456789-checkpoint.db"},
		{"foo/bar/", infoFileSuffix, "foo/bar/1465501321123456789-info.db"},
	}
	for _, input := range inputs {
		require.Equal(t, input.expected, filepathFromTime(input.prefix, start, input.suffix))
	}
}

func TestFileExists(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shard := 10
	start := time.Now()
	shardDir := path.Join(dir, strconv.Itoa(shard))
	err := os.Mkdir(shardDir, defaultNewDirectoryMode)
	require.NoError(t, err)

	infoFilePath := path.Join(shardDir, fmt.Sprintf("%d%s%s", start.UnixNano(), separator, infoFileSuffix))
	createFile(t, shardDir, start, infoFileSuffix, nil)
	require.True(t, fileExists(infoFilePath))
	require.False(t, FileExistsAt(shardDir, uint32(shard), start))

	checkpointFilePath := path.Join(shardDir, fmt.Sprintf("%d%s%s", start.UnixNano(), separator, checkpointFileSuffix))
	createFile(t, shardDir, start, checkpointFileSuffix, nil)
	require.True(t, fileExists(checkpointFilePath))
	require.False(t, FileExistsAt(shardDir, uint32(shard), start))

	os.Remove(infoFilePath)
	require.False(t, fileExists(infoFilePath))
}
