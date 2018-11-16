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
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

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

	// Make sure its sorted
	var lastFileStart time.Time
	for _, file := range files {
		require.Equal(t, 10*time.Minute, file.Duration)
		require.Equal(t, int64(0), file.Index)
		require.True(t, strings.Contains(file.FilePath, dir))
		if lastFileStart.IsZero() {
			lastFileStart = file.Start
			continue
		}

		require.True(t, file.Start.After(lastFileStart))
	}
}

// createTestCommitLogFiles creates at least the specified number of commit log files
// on disk with the appropriate block size. Commit log files will be valid and contain
// readable metadata.
func createTestCommitLogFiles(
	t *testing.T, filePathPrefix string, blockSize time.Duration, minNumBlocks int) {
	require.True(t, minNumBlocks >= 2)

	var (
		nowLock = sync.RWMutex{}
		now     = time.Now().Truncate(blockSize)
		nowFn   = func() time.Time {
			nowLock.RLock()
			n := now
			nowLock.RUnlock()
			return n
		}
		setNowFn = func(t time.Time) {
			nowLock.Lock()
			now = t
			nowLock.Unlock()
		}
		opts = NewOptions().
			SetBlockSize(blockSize).
			SetClockOptions(NewOptions().ClockOptions().SetNowFn(nowFn)).
			SetFilesystemOptions(fs.NewOptions().SetFilePathPrefix(filePathPrefix))
		commitLogsDir = fs.CommitLogsDirPath(filePathPrefix)
	)

	commitLog, err := NewCommitLog(opts)
	require.NoError(t, err)
	require.NoError(t, commitLog.Open())
	series := ts.Series{
		UniqueIndex: 0,
		Namespace:   ident.StringID("some-namespace"),
		ID:          ident.StringID("some-id"),
	}
	// Commit log writer is asynchronous and performs batching so getting the exact number
	// of files that we want is tricky. The implementation below loops infinitely, writing
	// a single datapoint and increasing the time after each iteration until minNumBlocks
	// files are on disk.
	for {
		files, err := fs.SortedCommitLogFiles(commitLogsDir)
		require.NoError(t, err)
		if len(files) >= minNumBlocks {
			break
		}
		err = commitLog.Write(context.NewContext(), series, ts.Datapoint{}, xtime.Second, nil)
		require.NoError(t, err)
		setNowFn(nowFn().Add(blockSize))
	}

	require.NoError(t, commitLog.Close())
	files, err := fs.SortedCommitLogFiles(commitLogsDir)
	require.NoError(t, err)
	require.True(t, len(files) >= minNumBlocks)
}
