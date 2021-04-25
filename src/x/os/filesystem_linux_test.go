// +build linux

package xos

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	contents = []byte("0123456789")
)

func createTempFsForTests(t *testing.T) string {
	tempDir, err := ioutil.TempDir("", "fssizetest")
	assert.Nil(t, err)

	assert.Nil(t, os.Mkdir(filepath.Join(tempDir, "commitlogs"), 0755))
	assert.Nil(t, os.Mkdir(filepath.Join(tempDir, "snapshots"), 0755))
	assert.Nil(t, os.Mkdir(filepath.Join(tempDir, "data"), 0755))
	assert.Nil(t, os.Mkdir(filepath.Join(tempDir, "index"), 0755))
	return tempDir
}
func TestGetFileSystemStats(t *testing.T) {
	tempDir := createTempFsForTests(t)
	defer os.RemoveAll(tempDir)
	stats, err := GetFileSystemStats(tempDir)
	assert.Nil(t, err)
	assert.NotEqual(t, uint64(0), stats.Total)
}

func createFilesForTests(t *testing.T, path string) {
	commitlogsDir := filepath.Join(path, "commitlogs")
	commitlogsFileName := filepath.Join(commitlogsDir, "commitlog-0-77.db")
	err := ioutil.WriteFile(commitlogsFileName, contents, 0644)
	assert.Nil(t, err)
}

func checkDirectorySizes(t *testing.T, path string) {
	commitlogsDir := filepath.Join(path, "commitlogs")
	stats, err := GetDirectoryStats(commitlogsDir)
	assert.Nil(t, err)
	assert.Equal(t, uint64(len(contents)), stats.Size)
	snapshotsDir := filepath.Join(path, "snapshots")
	stats, err = GetDirectoryStats(snapshotsDir)
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), stats.Size)
}

func TestGetDirectoryStats(t *testing.T) {
	tempDir := createTempFsForTests(t)
	createFilesForTests(t, tempDir)
	defer os.RemoveAll(tempDir)
	checkDirectorySizes(t, tempDir)
}
