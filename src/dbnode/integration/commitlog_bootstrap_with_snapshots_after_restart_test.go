// +build integration

// Copyright (c) 2020 Uber Technologies, Inc.
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
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3/src/dbnode/retention"
	xclock "github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

func TestCommitLogBootstrapWithSnapshotsAfterRestart(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	// Test setup
	var (
		ropts     = retention.NewOptions().SetRetentionPeriod(12 * time.Hour)
		blockSize = ropts.BlockSize()
	)
	ns, err := namespace.NewMetadata(testNamespaces[0], namespace.NewOptions().
		SetRetentionOptions(ropts).
		SetColdWritesEnabled(true))
	require.NoError(t, err)
	opts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{ns}).
		SetTickMinimumInterval(100 * time.Millisecond)

	setup, err := NewTestSetup(t, opts, nil)
	require.NoError(t, err)
	defer setup.Close()

	commitLogOpts := setup.StorageOpts().CommitLogOptions().
		SetFlushInterval(defaultIntegrationTestFlushInterval)
	setup.SetStorageOpts(setup.StorageOpts().
		SetCommitLogOptions(commitLogOpts).
		SetMediatorTickInterval(50 * time.Millisecond))

	log := setup.StorageOpts().InstrumentOptions().Logger()
	log.Info("commit log bootstrap with snapshots after restart test")

	// Start the server with filesystem bootstrapper
	require.NoError(t, setup.StartServer())
	log.Debug("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, setup.StopServer())
		log.Debug("server is now down")
	}()

	// Write test data
	log.Info("writing test data")
	now := setup.NowFn()().Truncate(blockSize)
	seriesMaps := make(map[xtime.UnixNano]generate.SeriesBlock)
	inputData := []generate.BlockConfig{
		{IDs: []string{"foo", "bar"}, NumPoints: 50, Start: now.Add(-5 * blockSize)},
		{IDs: []string{"foo", "qux"}, NumPoints: 50, Start: now.Add(-4 * blockSize)},
		{IDs: []string{"qux", "quux"}, NumPoints: 50, Start: now.Add(-3 * blockSize)},
		{IDs: []string{"corge", "porgie"}, NumPoints: 50, Start: now.Add(-2 * blockSize)},
	}
	for _, input := range inputData {
		testData := generate.Block(input)
		seriesMaps[input.Start] = testData
		require.NoError(t, setup.WriteBatch(testNamespaces[0], testData))
	}

	// Wait until snapshots are on disk.
	fsOpts := commitLogOpts.FilesystemOptions()
	expectedNumSeries := 0
	for _, data := range inputData {
		expectedNumSeries += len(data.IDs)
	}
	xclock.WaitUntil(func() bool {
		var totalNumEntries int
		for _, numEntries := range getNumEntriesPerBlockStart(ns.ID(), opts.NumShards(), fsOpts) {
			totalNumEntries += numEntries
		}
		return totalNumEntries == expectedNumSeries
	}, time.Minute)

	// Stop and restart server to allow bootstrapping from commit logs.
	require.NoError(t, setup.StopServer())
	// Setup commitlog bootstrapper after writing data so filesystem inspection can find it.
	require.NoError(t, setup.InitializeBootstrappers(InitializeBootstrappersOptions{
		CommitLogOptions: commitLogOpts,
		WithCommitLog:    true,
		// Also setup fs bootstrapper to be ensure correct behaviour on restart w/ fs bootstrapper enabled.
		WithFileSystem: true,
	}))
	require.NoError(t, setup.StartServer())
	log.Debug("server restarted")

	// Verify that data is what we expect.
	metadatasByShard := testSetupMetadatas(t, setup, testNamespaces[0], now.Add(-5*blockSize), now.Add(-blockSize))
	observedSeriesMaps := testSetupToSeriesMaps(t, setup, ns, metadatasByShard)
	verifySeriesMapsEqual(t, seriesMaps, observedSeriesMaps)

	// Wait until empty snapshots are on disk.
	xclock.WaitUntil(func() bool {
		var totalNumEntries int
		for _, numEntries := range getNumEntriesPerBlockStart(ns.ID(), opts.NumShards(), fsOpts) {
			totalNumEntries += numEntries
		}
		return totalNumEntries == 0
	}, time.Minute)

	// Verify that data is still what we expect.
	metadatasByShard = testSetupMetadatas(t, setup, testNamespaces[0], now.Add(-5*blockSize), now.Add(-blockSize))
	observedSeriesMaps = testSetupToSeriesMaps(t, setup, ns, metadatasByShard)
	verifySeriesMapsEqual(t, seriesMaps, observedSeriesMaps)
}

func getNumEntriesPerBlockStart(
	nsID ident.ID,
	numShards int,
	fsOpts fs.Options,
) map[xtime.UnixNano]int {
	numEntriesPerBlockStart := make(map[xtime.UnixNano]int)
	for shard := 0; shard < numShards; shard++ {
		infoFiles := fs.ReadInfoFiles(
			fsOpts.FilePathPrefix(),
			nsID,
			uint32(shard),
			fsOpts.InfoReaderBufferSize(),
			fsOpts.DecodingOptions(),
			persist.FileSetSnapshotType,
		)
		// Grab the latest snapshot file for each blockstart.
		latestSnapshotInfoPerBlockStart := make(map[xtime.UnixNano]schema.IndexInfo)
		for _, f := range infoFiles {
			info, ok := latestSnapshotInfoPerBlockStart[xtime.UnixNano(f.Info.BlockStart)]
			if !ok {
				latestSnapshotInfoPerBlockStart[xtime.UnixNano(f.Info.BlockStart)] = f.Info
				continue
			}

			if f.Info.VolumeIndex > info.VolumeIndex {
				latestSnapshotInfoPerBlockStart[xtime.UnixNano(f.Info.BlockStart)] = f.Info
			}
		}
		for blockStart, info := range latestSnapshotInfoPerBlockStart {
			numEntriesPerBlockStart[blockStart] += int(info.Entries)
		}
	}
	return numEntriesPerBlockStart
}
