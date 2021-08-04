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
	"github.com/m3db/m3/src/dbnode/persist/fs"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestDiskSnapshotSimple(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}
	// Test setup
	var (
		nOpts = namespace.NewOptions().
			SetSnapshotEnabled(true)
		bufferPast   = 50 * time.Minute
		bufferFuture = 50 * time.Minute
		blockSize    = time.Hour
	)

	nOpts = nOpts.
		SetRetentionOptions(nOpts.RetentionOptions().
			SetBufferFuture(bufferFuture).
			SetBufferPast(bufferPast).
			SetBlockSize(blockSize))
	md1, err := namespace.NewMetadata(testNamespaces[0], nOpts)
	require.NoError(t, err)
	md2, err := namespace.NewMetadata(testNamespaces[1], nOpts)
	require.NoError(t, err)

	testOpts := NewTestOptions(t).
		SetTickMinimumInterval(time.Second).
		SetNamespaces([]namespace.Metadata{md1, md2})
	testSetup, err := NewTestSetup(t, testOpts, nil)
	require.NoError(t, err)
	defer testSetup.Close()

	shardSet := testSetup.ShardSet()

	// Start the server
	log := testSetup.StorageOpts().InstrumentOptions().Logger()
	log.Debug("disk flush test")
	require.NoError(t, testSetup.StartServer())
	log.Debug("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, testSetup.StopServer())
		log.Debug("server is now down")
	}()

	// Write test data
	var (
		currBlock                         = testSetup.NowFn()().Truncate(blockSize)
		now                               = currBlock.Add(11 * time.Minute)
		assertTimeAllowsWritesToAllBlocks = func(ti xtime.UnixNano) {
			// Make sure now is within bufferPast of the previous block
			require.True(t, ti.Before(ti.Truncate(blockSize).Add(bufferPast)))
			// Make sure now is within bufferFuture of the next block
			require.True(t, ti.After(ti.Truncate(blockSize).Add(blockSize).Add(-bufferFuture)))
		}
	)

	assertTimeAllowsWritesToAllBlocks(now)
	testSetup.SetNowFn(now)

	var (
		seriesMaps = make(map[xtime.UnixNano]generate.SeriesBlock)
		inputData  = []generate.BlockConfig{
			// Writes in the previous block which should be mutable due to bufferPast
			{IDs: []string{"foo", "bar", "baz"}, NumPoints: 5, Start: currBlock.Add(-10 * time.Minute)},
			// Writes in the current block
			{IDs: []string{"a", "b", "c"}, NumPoints: 5, Start: currBlock},
			// Writes in the next block which should be mutable due to bufferFuture
			{IDs: []string{"1", "2", "3"}, NumPoints: 5, Start: currBlock.Add(blockSize)},
		}
	)
	for _, input := range inputData {
		testData := generate.Block(input)
		seriesMaps[input.Start.Truncate(blockSize)] = testData
		for _, ns := range testSetup.Namespaces() {
			require.NoError(t, testSetup.WriteBatch(ns.ID(), testData))
		}
	}

	// Now that we've completed the writes, we need to make sure that we wait for
	// the next snapshot to guarantee that it should contain all the writes. We do
	// this by measuring the current highest snapshot volume index and then updating
	// the time (to allow another snapshot process to occur due to the configurable
	// minimum time between snapshots), and then waiting for the snapshot files with
	// the measured volume index + 1.
	var (
		snapshotsToWaitForByNS = make([][]snapshotID, 0, len(testSetup.Namespaces()))
		fsOpts                 = testSetup.StorageOpts().
					CommitLogOptions().
					FilesystemOptions()
	)
	for _, ns := range testSetup.Namespaces() {
		snapshotsToWaitForByNS = append(snapshotsToWaitForByNS, []snapshotID{
			{
				blockStart: currBlock.Add(-blockSize),
				minVolume: getLatestSnapshotVolumeIndex(
					fsOpts, shardSet, ns.ID(), currBlock.Add(-blockSize)),
			},
			{
				blockStart: currBlock,
				minVolume: getLatestSnapshotVolumeIndex(
					fsOpts, shardSet, ns.ID(), currBlock),
			},
			{
				blockStart: currBlock.Add(blockSize),
				minVolume: getLatestSnapshotVolumeIndex(
					fsOpts, shardSet, ns.ID(), currBlock.Add(blockSize)),
			},
		})
	}

	now = testSetup.NowFn()().Add(2 * time.Minute)
	assertTimeAllowsWritesToAllBlocks(now)
	testSetup.SetNowFn(now)

	maxWaitTime := time.Minute
	for i, ns := range testSetup.Namespaces() {
		log.Info("waiting for snapshot files to flush",
			zap.Any("ns", ns.ID()))
		_, err := waitUntilSnapshotFilesFlushed(fsOpts, shardSet, ns.ID(), snapshotsToWaitForByNS[i], maxWaitTime)
		require.NoError(t, err)
		log.Info("verifying snapshot files",
			zap.Any("ns", ns.ID()))
		verifySnapshottedDataFiles(t, shardSet, testSetup.StorageOpts(), ns.ID(), seriesMaps)
	}

	var (
		newTime = testSetup.NowFn()().Add(blockSize * 2)
	)
	testSetup.SetNowFn(newTime)

	for _, ns := range testSetup.Namespaces() {
		log.Info("waiting for new snapshot files to be written out",
			zap.Any("ns", ns.ID()))
		snapshotsToWaitFor := []snapshotID{{blockStart: currBlock.Add(blockSize)}}
		// NB(bodu): We need to check if a specific snapshot ID was deleted since snapshotting logic now changed
		// to always snapshotting every block start w/in retention.
		snapshotID, err := waitUntilSnapshotFilesFlushed(fsOpts, shardSet, ns.ID(), snapshotsToWaitFor, maxWaitTime)
		require.NoError(t, err)
		log.Info("waiting for old snapshot files to be deleted",
			zap.Any("ns", ns.ID()))
		// These should be flushed to disk and snapshots should have been cleaned up.
		flushedBlockStarts := []xtime.UnixNano{
			currBlock.Add(-blockSize),
			currBlock,
		}
		for _, shard := range shardSet.All() {
			waitUntil(func() bool {
				// Increase the time each check to ensure that the filesystem processes are able to progress (some
				// of them throttle themselves based on time elapsed since the previous time.)
				testSetup.SetNowFn(testSetup.NowFn()().Add(10 * time.Second))
				// Ensure that snapshots for flushed data blocks no longer exist.
				for _, blockStart := range flushedBlockStarts {
					exists, err := fs.SnapshotFileSetExistsAt(fsOpts.FilePathPrefix(), ns.ID(), snapshotID, shard.ID(), blockStart)
					require.NoError(t, err)
					if exists {
						return false
					}
				}
				return true
			}, maxWaitTime)
		}
	}
}
