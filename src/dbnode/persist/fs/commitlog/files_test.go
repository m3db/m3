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

package commitlog

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/persist/fs"

	"github.com/stretchr/testify/require"
)

func TestFiles(t *testing.T) {
	dir, err := ioutil.TempDir("", "commitlogs")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	createTestCommitLogFiles(t, dir, 10*time.Minute, 5)

	var (
		minNumBlocks = 5
		opts         = NewOptions()
	)
	opts = opts.SetFilesystemOptions(
		opts.FilesystemOptions().
			SetFilePathPrefix(dir),
	)
	files, corruptFiles, err := Files(opts)
	require.NoError(t, err)
	require.True(t, len(corruptFiles) == 0)
	require.True(t, len(files) >= minNumBlocks)

	// Make sure its sorted.
	var lastFileIndex = -1
	for i, file := range files {
		require.Equal(t, int64(i), file.Index)
		require.True(t, strings.Contains(file.FilePath, dir))
		if lastFileIndex == -1 {
			lastFileIndex = int(file.Index)
			continue
		}

		require.True(t, int(file.Index) > lastFileIndex)
	}
}

// createTestCommitLogFiles creates at least the specified number of commit log files
// on disk with the appropriate block size. Commit log files will be valid and contain
// readable metadata.
func createTestCommitLogFiles(
	t *testing.T, filePathPrefix string, blockSize time.Duration, minNumBlocks int) {
	require.True(t, minNumBlocks >= 2)

	var (
		opts = NewOptions().
			SetBlockSize(blockSize).
			SetClockOptions(NewOptions().ClockOptions()).
			SetFilesystemOptions(fs.NewOptions().SetFilePathPrefix(filePathPrefix))
		commitLogsDir = fs.CommitLogsDirPath(filePathPrefix)
	)

	commitLog, err := NewCommitLog(opts)
	require.NoError(t, err)
	require.NoError(t, commitLog.Open())

	// Loop until we have enough commit log files.
	for {
		files, err := fs.SortedCommitLogFiles(commitLogsDir)
		require.NoError(t, err)
		if len(files) >= minNumBlocks {
			break
		}
		_, err = commitLog.RotateLogs()
		require.NoError(t, err)
	}

	require.NoError(t, commitLog.Close())
	files, err := fs.SortedCommitLogFiles(commitLogsDir)
	require.NoError(t, err)
	require.True(t, len(files) >= minNumBlocks)
}
