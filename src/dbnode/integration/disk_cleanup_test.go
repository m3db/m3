// +build integration

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

package integration

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDiskCleanup(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}
	// Test setup
	testOpts := newTestOptions(t)
	testSetup, err := newTestSetup(t, testOpts, nil)
	require.NoError(t, err)
	defer testSetup.close()

	md := testSetup.namespaceMetadataOrFail(testNamespaces[0])
	blockSize := md.Options().RetentionOptions().BlockSize()
	retentionPeriod := md.Options().RetentionOptions().RetentionPeriod()
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
	writeDataFileSetFiles(t, testSetup.storageOpts, md, shard, fileTimes)
	writeCommitLogs(t, filePathPrefix, fileTimes)

	// Move now forward by retentionPeriod + 2 * blockSize so fileset files
	// and commit logs at now will be deleted
	newNow := now.Add(retentionPeriod).Add(2 * blockSize)
	testSetup.setNowFn(newNow)

	// Check if files have been deleted
	waitTimeout := 30 * time.Second
	require.NoError(t, waitUntilDataCleanedUp(filePathPrefix, testNamespaces[0], shard, now, waitTimeout))
}
