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

func waitUntilDataCleanedUp(filePathPrefix string, namespace ts.ID, shard uint32, toDelete time.Time, timeout time.Duration) error {
	dataCleanedUp := func() bool {
		if fs.FilesetExistsAt(filePathPrefix, namespace, shard, toDelete) {
			return false
		}
		_, index := fs.NextCommitLogsFile(filePathPrefix, toDelete)
		if index != 0 {
			return false
		}
		return true
	}
	if waitUntil(dataCleanedUp, timeout) {
		return nil
	}
	return errDataCleanupTimedOut
}

func TestDiskCleanup(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}
	// Test setup
	tickInterval := 3 * time.Second
	testSetup, err := newTestSetup(newTestOptions().SetTickInterval(tickInterval))
	require.NoError(t, err)
	defer testSetup.close()

	var (
		ropts           = retention.NewOptions().SetRetentionPeriod(6 * time.Hour)
		blockSize       = ropts.BlockSize()
		retentionPeriod = ropts.RetentionPeriod()
	)
	require.NoError(t, testSetup.setRetentionOnAll(ropts))

	testSetup.storageOpts = testSetup.storageOpts.SetCommitLogOptions(
		testSetup.storageOpts.CommitLogOptions().
			SetRetentionOptions(ropts))

	filePathPrefix := testSetup.storageOpts.CommitLogOptions().FilesystemOptions().FilePathPrefix()

	// Start the server
	log := testSetup.storageOpts.InstrumentOptions().Logger()
	log.Debug("disk cleanup test")
	require.NoError(t, testSetup.startServer())
	log.Debug("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, testSetup.stopServer())
		log.Debug("server is now down")
	}()

	// Now create some fileset files and commit logs
	shard := uint32(0)
	numTimes := 10
	fileTimes := make([]time.Time, numTimes)
	now := testSetup.getNowFn()
	for i := 0; i < numTimes; i++ {
		fileTimes[i] = now.Add(time.Duration(i) * blockSize)
	}
	createFilesetFiles(t, testSetup.storageOpts, testNamespaces[0], shard, fileTimes)
	createCommitLogs(t, filePathPrefix, fileTimes)

	// Move now forward by retentionPeriod + 2 * blockSize so fileset files
	// and commit logs at now will be deleted
	newNow := now.Add(retentionPeriod).Add(2 * blockSize)
	testSetup.setNowFn(newNow)

	// Check if files have been deleted
	waitTimeout := tickInterval * 4
	require.NoError(t, waitUntilDataCleanedUp(filePathPrefix, testNamespaces[0], shard, now, waitTimeout))
}
