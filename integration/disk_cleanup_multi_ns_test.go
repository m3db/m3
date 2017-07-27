// +build integration

// Copyright (c) 2017 Uber Technologies, Inc.
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

	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/namespace"

	"github.com/stretchr/testify/require"
)

// Consider a database running with two namespaces, and the following retention opts:
//
//           | BlockSize | Retention Period
// ns1       |    4h     |      8h
// ns2       |    2h     |      6h
// commitLog |   30m     |      8h
//
// We have a block for each of the three at each marker in the diagram below.
//
// time (flowing left --> right):
// time-label: t0  t1  t2  t3  t4  t5  t6
//  ns1        *       *       *       *   [blocksize * is 4h]
//  ns2        .   .   .   .   .   .   .   [blocksize . is 2h]
//  commitlog  ,,,,,,,,,,,,,,,,,,,,,,,,,   [blocksize , is 30min]
//                                     |
//      		     									current time
//
// The test creates the blocks above, sets the time to t6, and verifies the following:
// - we have removed the commit log blocks between [t0, t2-30m]
// - we have removed the ns1 fileset blocks at t0
// - we have removed the ns2 fileset blocks between [t0, t3)
//
// NB(prateek): the 30m offset in the times above is due to one commit log block on
// either side of the namespace block potentially having data for the block it stradles.
func TestDiskCleanupMultipleNamespace(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	// Test setup
	var (
		rOpts              = retention.NewOptions().SetRetentionPeriod(8 * time.Hour)
		commitLogBlockSize = 30 * time.Minute
		ns1BlockSize       = 4 * time.Hour
		ns2BlockSize       = 2 * time.Hour
		clROpts            = rOpts.SetBlockSize(commitLogBlockSize).SetBufferFuture(0).SetBufferPast(0)
		ns1ROpts           = rOpts.SetRetentionPeriod(8 * time.Hour).SetBlockSize(ns1BlockSize)
		ns2ROpts           = rOpts.SetRetentionPeriod(6 * time.Hour).SetBlockSize(ns2BlockSize)
		nsOpts             = namespace.NewOptions().SetNeedsFlush(false) // disabling flushing to ensure data isn't flushed during test
	)

	ns1, err := namespace.NewMetadata(testNamespaces[0], nsOpts.SetRetentionOptions(ns1ROpts))
	require.NoError(t, err)
	ns2, err := namespace.NewMetadata(testNamespaces[1], nsOpts.SetRetentionOptions(ns2ROpts))
	require.NoError(t, err)

	opts := newTestOptions(t).
		SetCommitLogRetention(clROpts).
		SetNamespaces([]namespace.Metadata{ns1, ns2})

	// Test setup
	testSetup, err := newTestSetup(t, opts)
	require.NoError(t, err)

	// logger
	log := testSetup.storageOpts.InstrumentOptions().Logger()
	log.Infof("disk cleanup multiple namespaces test")

	// close testSetup to release resources by default
	defer func() {
		log.Infof("testSetup closing")
		testSetup.close()
	}()

	filePathPrefix := testSetup.storageOpts.CommitLogOptions().FilesystemOptions().FilePathPrefix()

	// align to largest block size
	now := testSetup.getNowFn().Truncate(ns1BlockSize)
	testSetup.setNowFn(now)

	// Now create the files
	end := now.Add(12 * time.Hour)

	// generation times
	commitLogTimes := getTimes(now, end, commitLogBlockSize)
	ns1Times := getTimes(now, end, ns1BlockSize)
	ns2Times := getTimes(now, end, ns2BlockSize)

	// notice that ns2Times are the same as t0, t1, .. markers in the description above
	// files to remove
	commitLogTimesToRemove := getTimes(now, ns2Times[2].Add(-commitLogBlockSize), commitLogBlockSize)
	ns1TimesToRemove := []time.Time{ns2Times[0]}
	ns2TimesToRemove := getTimes(now, ns2Times[3], ns2BlockSize)

	// files to retain
	commitLogTimesToRetain := getTimes(ns2Times[2], end, commitLogBlockSize)
	ns1TimesToRetain := getTimes(now.Add(ns1BlockSize), end, ns1BlockSize)
	ns2TimesToRetain := getTimes(ns2Times[3].Add(ns2BlockSize), end, ns2BlockSize)

	// Start the server
	require.NoError(t, testSetup.startServer())
	log.Infof("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, testSetup.stopServer())
		log.Infof("server is now down")
	}()

	log.Infof("creating commit log and fileset files")
	shard := uint32(0)
	writeCommitLogs(t, filePathPrefix, commitLogTimes)
	writeFilesetFiles(t, testSetup.storageOpts, ns1, shard, ns1Times)
	writeFilesetFiles(t, testSetup.storageOpts, ns2, shard, ns2Times)

	// Move now forward by 12 hours, and see if the expected files have been deleted
	testSetup.setNowFn(end)

	// Check if expected files have been deleted
	log.Infof("waiting until data is cleaned up")
	waitTimeout := 60 * time.Second
	require.NoError(t, waitUntilDataCleanedUpExtended(
		[]cleanupTimesFileset{
			cleanupTimesFileset{
				filePathPrefix: filePathPrefix,
				namespace:      testNamespaces[0],
				shard:          shard,
				times:          ns1TimesToRemove,
			},
			cleanupTimesFileset{
				filePathPrefix: filePathPrefix,
				namespace:      testNamespaces[1],
				shard:          shard,
				times:          ns2TimesToRemove,
			},
		},
		cleanupTimesCommitLog{
			filePathPrefix: filePathPrefix,
			times:          commitLogTimesToRemove,
		},
		waitTimeout,
	))

	// check files we still expect exist
	log.Infof("asserting expected data files exist")
	ns1ExpectedFiles := cleanupTimesFileset{
		filePathPrefix: filePathPrefix,
		namespace:      testNamespaces[0],
		shard:          shard,
		times:          ns1TimesToRetain,
	}
	require.True(t, ns1ExpectedFiles.allExist(), "ns1 expected fileset files do not exist")

	ns2ExpectedFiles := cleanupTimesFileset{
		filePathPrefix: filePathPrefix,
		namespace:      testNamespaces[1],
		shard:          shard,
		times:          ns2TimesToRetain,
	}
	require.True(t, ns2ExpectedFiles.allExist(), "ns2 expected fileset files do not exist")

	commitLogFilesToRetain := cleanupTimesCommitLog{
		filePathPrefix: filePathPrefix,
		times:          commitLogTimesToRetain,
	}
	require.True(t, commitLogFilesToRetain.allExist(), "commit log expected files do not exist")
	log.Infof("done with data asserts")
}
