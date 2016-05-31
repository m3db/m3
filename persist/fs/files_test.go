package fs

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"testing"

	schema "code.uber.internal/infra/memtsdb/persist/fs/proto"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestCloseFilesFails(t *testing.T) {
	file, err := ioutil.TempFile("", "testfile")
	if err != nil {
		t.Fatal(err)
	}

	defer os.Remove(file.Name())

	assert.NoError(t, file.Close())
	assert.Error(t, closeFiles(file))
}

func TestByVersion(t *testing.T) {
	files := []string{"foo/1-info.db", "foo/12-info.db", "foo/2-info.db"}
	expected := []string{"foo/1-info.db", "foo/2-info.db", "foo/12-info.db"}
	sort.Sort(byVersionAscending(files))
	require.Equal(t, expected, files)
}

func createFile(t *testing.T, filePath string) {
	f, err := os.Create(filePath)
	require.NoError(t, err)
	f.Close()
}

func createInfoFiles(t *testing.T, iter int) string {
	dir, err := ioutil.TempDir("", "foo")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < iter; i++ {
		filePath := path.Join(dir, fmt.Sprintf("%d%s%s", i, separator, infoFileSuffix))
		createFile(t, filePath)
	}
	return dir
}

func TestInfoFiles(t *testing.T) {
	iter := 20
	dir := createInfoFiles(t, iter)
	defer os.RemoveAll(dir)

	createFile(t, path.Join(dir, "abcd"))
	createFile(t, path.Join(dir, separator+infoFileSuffix))

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

	data, err := proto.Marshal(&schema.IndexInfo{Start: 100, Window: 10, Entries: 20})
	require.NoError(t, err)

	tmpfile.Truncate(0)
	tmpfile.Seek(0, 0)
	_, err = tmpfile.Write(data)
	require.NoError(t, err)

	tmpfile.Seek(0, 0)
	entry, err := ReadInfo(tmpfile)
	require.NoError(t, err)
	require.Equal(t, int64(100), entry.Start)
	require.Equal(t, int64(10), entry.Window)
	require.Equal(t, int64(20), entry.Entries)
}

func TestVersionFromName(t *testing.T) {
	_, err := VersionFromName("foo/bar")
	require.Error(t, err)

	_, err = VersionFromName("foo/bar-baz")
	require.Error(t, err)

	v, err := VersionFromName("1-test.db")
	require.Equal(t, 1, v)
	require.NoError(t, err)

	v, err = VersionFromName("foo/bar/20-test.db")
	require.Equal(t, 20, v)
	require.NoError(t, err)
}

func TestNextVersion(t *testing.T) {
	v, err := nextVersion("nonexistent")
	require.NoError(t, err)
	require.Equal(t, 0, v)

	dir := createInfoFiles(t, 20)
	defer os.RemoveAll(dir)
	v, err = nextVersion(dir)
	require.NoError(t, err)
	require.Equal(t, 20, v)
}

func TestShardDirPath(t *testing.T) {
	require.Equal(t, "foo/bar/12", ShardDirPath("foo/bar", 12))
	require.Equal(t, "foo/bar/12", ShardDirPath("foo/bar/", 12))
}

func TestFilePathFromVersion(t *testing.T) {
	require.Equal(t, "foo/bar/1-info.db", filepathFromVersion("foo/bar", 1, "info.db"))
	require.Equal(t, "foo/bar/20-info.db", filepathFromVersion("foo/bar/", 20, "info.db"))
}
