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
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
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

	// md1.SetOptions()

	testOpts := newTestOptions(t).
		SetTickMinimumInterval(time.Second).
		SetNamespaces([]namespace.Metadata{md1, md2})
	testSetup, err := newTestSetup(t, testOpts, nil)
	require.NoError(t, err)
	defer testSetup.close()

	shardSet := testSetup.shardSet

	// Start the server
	log := testSetup.storageOpts.InstrumentOptions().Logger()
	log.Debug("disk flush test")
	require.NoError(t, testSetup.startServer())
	log.Debug("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, testSetup.stopServer())
		log.Debug("server is now down")
	}()

	// Write test data
	var (
		currBlock = testSetup.getNowFn().Truncate(blockSize)
		now       = currBlock.Add(11 * time.Minute)
	)
	// Make sure now is within bufferPast of the previous block
	require.True(t, now.Before(now.Truncate(blockSize).Add(bufferPast)))
	// Make sure now is within bufferFuture of the next block
	require.True(t, now.After(now.Truncate(blockSize).Add(blockSize).Add(-bufferFuture)))
	testSetup.setNowFn(now)

	var (
		seriesMaps = make(map[xtime.UnixNano]generate.SeriesBlock)
		inputData  = []generate.BlockConfig{
			// Writes in the previous block which should be mutable due to bufferPast
			{IDs: []string{"foo", "bar", "baz"}, NumPoints: 5, Start: currBlock.Add(-10 * time.Minute)},
			// Writes in the current block
			{IDs: []string{"a", "b", "c"}, NumPoints: 30, Start: currBlock},
			// Writes in the next block which should be mutable due to bufferFuture
			{IDs: []string{"1", "2", "3"}, NumPoints: 30, Start: currBlock.Add(blockSize)},
		}
	)
	for _, input := range inputData {
		// testSetup.setNowFn(input.Start)
		testData := generate.Block(input)
		seriesMaps[xtime.ToUnixNano(input.Start.Truncate(blockSize))] = testData
		for _, ns := range testSetup.namespaces {
			require.NoError(t, testSetup.writeBatch(ns.ID(), testData))
		}
	}

	now = testSetup.getNowFn().Add(2 * time.Minute)
	// TODO: Make this a function that can be repeated
	// Make sure now is within bufferPast of the previous block
	require.True(t, now.Before(now.Truncate(blockSize).Add(bufferPast)))
	// Make sure now is within bufferFuture of the next block
	require.True(t, now.After(now.Truncate(blockSize).Add(blockSize).Add(-bufferFuture)))
	testSetup.setNowFn(now)

	var (
		maxWaitTime    = time.Minute
		filePathPrefix = testSetup.storageOpts.
				CommitLogOptions().
				FilesystemOptions().
				FilePathPrefix()
	)

	for _, ns := range testSetup.namespaces {
		snapshotsToWaitFor := []snapshotID{
			{
				blockStart: currBlock.Add(-blockSize),
				minVolume: getLatestSnapshotVolumeIndex(
					filePathPrefix, shardSet, ns.ID(), currBlock.Add(-blockSize)),
			},
			{
				blockStart: currBlock,
				minVolume: getLatestSnapshotVolumeIndex(
					filePathPrefix, shardSet, ns.ID(), currBlock),
			},
			{
				blockStart: currBlock.Add(blockSize),
				minVolume: getLatestSnapshotVolumeIndex(
					filePathPrefix, shardSet, ns.ID(), currBlock.Add(blockSize)),
			},
		}

		log.Info("waiting for snapshot files to flush")
		require.NoError(t, waitUntilSnapshotFilesFlushed(
			filePathPrefix, shardSet, ns.ID(), snapshotsToWaitFor, maxWaitTime))
		log.Info("verifying snapshot files")
		verifySnapshottedDataFiles(t, shardSet, testSetup.storageOpts, ns.ID(), seriesMaps)
	}

	var (
		oldTime = testSetup.getNowFn()
		newTime = oldTime.Add(blockSize * 2)
	)
	testSetup.setNowFn(newTime)

	for _, ns := range testSetup.namespaces {
		log.Info("waiting for new snapshot files to be written out")
		snapshotsToWaitFor := []snapshotID{{blockStart: newTime.Truncate(blockSize)}}
		require.NoError(t, waitUntilSnapshotFilesFlushed(
			filePathPrefix, shardSet, ns.ID(), snapshotsToWaitFor, maxWaitTime))
		log.Info("waiting for old snapshot files to be deleted")
		for _, shard := range shardSet.All() {
			waitUntil(func() bool {
				exists, err := fs.SnapshotFileSetExistsAt(filePathPrefix, ns.ID(), shard.ID(), oldTime.Truncate(blockSize))
				require.NoError(t, err)
				return !exists
			}, maxWaitTime)
		}
	}
}
