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
	"sort"
	"strconv"
	"testing"
	"time"

	schema "github.com/m3db/m3db/persist/fs/proto"

	"github.com/golang/protobuf/proto"
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

func createFile(t *testing.T, filePath string) {
	f, err := os.Create(filePath)
	require.NoError(t, err)
	f.Close()
}

func createInfoFiles(t *testing.T, iter int) string {
	dir := createTempDir(t)
	for i := 0; i < iter; i++ {
		infoFilePath := path.Join(dir, fmt.Sprintf("%d%s%s", i, separator, infoFileSuffix))
		createFile(t, infoFilePath)
		checkpointFilePath := path.Join(dir, fmt.Sprintf("%d%s%s", i, separator, checkpointFileSuffix))
		createFile(t, checkpointFilePath)
	}
	return dir
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

func TestInfoFiles(t *testing.T) {
	iter := 20
	dir := createInfoFiles(t, iter)
	defer os.RemoveAll(dir)

	createFile(t, path.Join(dir, "abcd"))
	createFile(t, path.Join(dir, separator+infoFileSuffix))
	createFile(t, path.Join(dir, strconv.Itoa(iter+1)+separator+infoFileSuffix))

	files, err := InfoFiles(dir)
	require.NoError(t, err)
	require.Equal(t, iter, len(files))
	for i := 0; i < iter; i++ {
		require.Equal(t, path.Join(dir, fmt.Sprintf("%d%s%s", i, separator, infoFileSuffix)), files[i])
	}
}

func TestReadInfo(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "example")
	if err != nil {
		t.Fatal(err)
	}

	defer os.Remove(tmpfile.Name())

	_, err = tmpfile.Write([]byte{0x1, 0x2})
	require.NoError(t, err)

	tmpfile.Seek(0, 0)
	_, err = ReadInfo(tmpfile)
	require.Error(t, err)

	data, err := proto.Marshal(&schema.IndexInfo{Start: 100, BlockSize: 10, Entries: 20})
	require.NoError(t, err)

	tmpfile.Truncate(0)
	tmpfile.Seek(0, 0)
	_, err = tmpfile.Write(data)
	require.NoError(t, err)

	tmpfile.Seek(0, 0)
	entry, err := ReadInfo(tmpfile)
	require.NoError(t, err)
	require.Equal(t, int64(100), entry.Start)
	require.Equal(t, int64(10), entry.BlockSize)
	require.Equal(t, int64(20), entry.Entries)
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
	require.Equal(t, "foo/bar/12", ShardDirPath("foo/bar", 12))
	require.Equal(t, "foo/bar/12", ShardDirPath("foo/bar/", 12))
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
	infoFilePath := path.Join(shardDir, fmt.Sprintf("%d%s%s", start, separator, infoFileSuffix))
	createFile(t, infoFilePath)
	require.True(t, fileExists(infoFilePath))
	require.False(t, FileExistsAt(shardDir, uint32(shard), start))

	checkpointFilePath := path.Join(shardDir, fmt.Sprintf("%d%s%s", start, separator, checkpointFileSuffix))
	createFile(t, checkpointFilePath)
	require.True(t, fileExists(checkpointFilePath))
	require.False(t, FileExistsAt(shardDir, uint32(shard), start))

	os.Remove(infoFilePath)
	require.False(t, fileExists(infoFilePath))
}
