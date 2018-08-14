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
	nOpts := namespace.NewOptions().SetSnapshotEnabled(true)
	md1, err := namespace.NewMetadata(testNamespaces[0], nOpts)
	require.NoError(t, err)
	md2, err := namespace.NewMetadata(testNamespaces[1], nOpts)
	require.NoError(t, err)

	testOpts := newTestOptions(t).
		SetTickMinimumInterval(time.Second).
		SetNamespaces([]namespace.Metadata{md1, md2})
	testSetup, err := newTestSetup(t, testOpts, nil)
	require.NoError(t, err)
	defer testSetup.close()

	blockSize := md1.Options().RetentionOptions().BlockSize()
	filePathPrefix := testSetup.storageOpts.CommitLogOptions().FilesystemOptions().FilePathPrefix()

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
	now := testSetup.getNowFn()
	seriesMaps := make(map[xtime.UnixNano]generate.SeriesBlock)
	inputData := []generate.BlockConfig{
		{IDs: []string{"foo", "bar", "baz"}, NumPoints: 100, Start: now},
	}
	for _, input := range inputData {
		testSetup.setNowFn(input.Start)
		testData := generate.Block(input)
		seriesMaps[xtime.ToUnixNano(input.Start)] = testData
		for _, ns := range testSetup.namespaces {
			require.NoError(t, testSetup.writeBatch(ns.ID(), testData))
		}
	}

	now = testSetup.getNowFn().Add(blockSize).Add(-10 * time.Minute)
	testSetup.setNowFn(now)
	maxWaitTime := time.Minute
	for _, ns := range testSetup.namespaces {
		require.NoError(t, waitUntilSnapshotFilesFlushed(filePathPrefix, testSetup.shardSet, ns.ID(), []time.Time{now.Truncate(blockSize)}, maxWaitTime))
		verifySnapshottedDataFiles(t, testSetup.shardSet, testSetup.storageOpts, ns.ID(), now.Truncate(blockSize), seriesMaps)
	}

	oldTime := testSetup.getNowFn()
	newTime := oldTime.Add(blockSize * 2)
	testSetup.setNowFn(newTime)

	testSetup.sleepFor10xTickMinimumInterval()
	for _, ns := range testSetup.namespaces {
		// Make sure new snapshotfiles are written out
		require.NoError(t, waitUntilSnapshotFilesFlushed(filePathPrefix, testSetup.shardSet, ns.ID(), []time.Time{newTime.Truncate(blockSize)}, maxWaitTime))
		for _, shard := range testSetup.shardSet.All() {
			waitUntil(func() bool {
				// Make sure old snapshot files are deleted
				exists, err := fs.SnapshotFileSetExistsAt(filePathPrefix, ns.ID(), shard.ID(), oldTime.Truncate(blockSize))
				require.NoError(t, err)
				return !exists
			}, maxWaitTime)
		}
	}
}
