//go:build integration
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

	"github.com/m3db/m3/src/dbnode/integration/generate"
	"github.com/m3db/m3/src/dbnode/namespace"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

func TestDiskCleanup(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}
	// Test setup
	testOpts := NewTestOptions(t)
	testSetup, err := NewTestSetup(t, testOpts, nil)
	require.NoError(t, err)
	defer testSetup.Close()

	md := testSetup.NamespaceMetadataOrFail(testNamespaces[0])
	blockSize := md.Options().RetentionOptions().BlockSize()
	retentionPeriod := md.Options().RetentionOptions().RetentionPeriod()

	// Create some fileset files and commit logs
	var (
		shard         = uint32(0)
		numTimes      = 10
		fileTimes     = make([]xtime.UnixNano, numTimes)
		now           = testSetup.NowFn()()
		commitLogOpts = testSetup.StorageOpts().CommitLogOptions().
				SetFlushInterval(defaultIntegrationTestFlushInterval)
	)
	ns1, err := namespace.NewMetadata(testNamespaces[0], namespace.NewOptions())
	require.NoError(t, err)
	for i := 0; i < numTimes; i++ {
		fileTimes[i] = now.Add(time.Duration(i) * blockSize)
	}
	writeDataFileSetFiles(t, testSetup.StorageOpts(), md, shard, fileTimes)
	for _, clTime := range fileTimes {
		data := map[xtime.UnixNano]generate.SeriesBlock{clTime: nil}
		writeCommitLogDataSpecifiedTS(
			t, testSetup, commitLogOpts,
			data, ns1, clTime, false)
	}

	// Now start the server
	log := testSetup.StorageOpts().InstrumentOptions().Logger()
	log.Debug("disk cleanup test")
	require.NoError(t, testSetup.StartServer())
	log.Debug("server is now up")

	defer func() {
		require.NoError(t, testSetup.StopServer())
		log.Debug("server is now down")
	}()

	// Move now forward by retentionPeriod + 2 * blockSize so fileset files
	// and commit logs at now will be deleted
	newNow := testSetup.NowFn()().Add(retentionPeriod).Add(2 * blockSize)
	testSetup.SetNowFn(newNow)

	// Check if files have been deleted
	waitTimeout := 30 * time.Second
	require.NoError(t, waitUntilDataCleanedUp(commitLogOpts, testNamespaces[0], shard, now, waitTimeout))
}
