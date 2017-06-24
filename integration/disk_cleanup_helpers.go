// +build integration

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

package integration

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage"
	"github.com/m3db/m3db/ts"
)

var (
	errDataCleanupTimedOut = errors.New("cleaning up data files took too long")
)

func createWriter(storageOpts storage.Options, ropts retention.Options) fs.FileSetWriter {
	fsOpts := storageOpts.CommitLogOptions().FilesystemOptions()
	blockSize := ropts.BlockSize()
	filePathPrefix := fsOpts.FilePathPrefix()
	writerBufferSize := fsOpts.WriterBufferSize()
	newFileMode := fsOpts.NewFileMode()
	newDirectoryMode := fsOpts.NewDirectoryMode()
	return fs.NewWriter(blockSize, filePathPrefix, writerBufferSize, newFileMode, newDirectoryMode)
}

func createFilesetFiles(t *testing.T, storageOpts storage.Options, namespace ts.ID, shard uint32, fileTimes []time.Time) {
	md, err := storageOpts.NamespaceRegistry().Get(namespace)
	require.NoError(t, err)
	writer := createWriter(storageOpts, md.Options().RetentionOptions())
	for _, start := range fileTimes {
		require.NoError(t, writer.Open(namespace, shard, start))
		require.NoError(t, writer.Close())
	}
}

func createCommitLogs(t *testing.T, filePathPrefix string, fileTimes []time.Time) {
	for _, start := range fileTimes {
		commitLogFile, _ := fs.NextCommitLogsFile(filePathPrefix, start)
		_, err := os.Create(commitLogFile)
		require.NoError(t, err)
	}
}

type cleanupTimesCommitLog struct {
	filePathPrefix string
	times          []time.Time
}

func (c *cleanupTimesCommitLog) anyExist() bool {
	for _, t := range c.times {
		_, index := fs.NextCommitLogsFile(c.filePathPrefix, t)
		if index != 0 {
			return true
		}
	}
	return false
}

func (c *cleanupTimesCommitLog) allExist() bool {
	for _, t := range c.times {
		_, index := fs.NextCommitLogsFile(c.filePathPrefix, t)
		if index == 0 {
			return false
		}
	}
	return true
}

type cleanupTimesFileset struct {
	filePathPrefix string
	namespace      ts.ID
	shard          uint32
	times          []time.Time
}

func (fset *cleanupTimesFileset) anyExist() bool {
	for _, t := range fset.times {
		if fs.FilesetExistsAt(fset.filePathPrefix, fset.namespace, fset.shard, t) {
			return true
		}
	}
	return false
}

func (fset *cleanupTimesFileset) allExist() bool {
	for _, t := range fset.times {
		if !fs.FilesetExistsAt(fset.filePathPrefix, fset.namespace, fset.shard, t) {
			return false
		}
	}
	return true
}

func waitUntilDataCleanedUpExtended(
	filesetFiles []cleanupTimesFileset,
	commitlog cleanupTimesCommitLog,
	timeout time.Duration,
) error {
	dataCleanedUp := func() bool {
		if commitlog.anyExist() {
			return false
		}

		for _, fset := range filesetFiles {
			if fset.anyExist() {
				return false
			}
		}

		return true
	}

	if waitUntil(dataCleanedUp, timeout) {
		return nil
	}
	return errDataCleanupTimedOut
}

func waitUntilDataCleanedUp(filePathPrefix string, namespace ts.ID, shard uint32, toDelete time.Time, timeout time.Duration) error {
	return waitUntilDataCleanedUpExtended(
		[]cleanupTimesFileset{
			cleanupTimesFileset{
				filePathPrefix: filePathPrefix,
				namespace:      namespace,
				shard:          shard,
				times:          []time.Time{toDelete},
			},
		},
		cleanupTimesCommitLog{
			filePathPrefix: filePathPrefix,
			times:          []time.Time{toDelete},
		},
		timeout)
}

func getTimes(start time.Time, end time.Time, intervalSize time.Duration) []time.Time {
	totalPeriod := end.Sub(start)
	numPeriods := int(totalPeriod.Nanoseconds() / intervalSize.Nanoseconds())

	times := make([]time.Time, 0, numPeriods)
	for i := 0; i < numPeriods; i++ {
		times = append(times, start.Add(time.Duration(i)*intervalSize))
	}

	return times
}
