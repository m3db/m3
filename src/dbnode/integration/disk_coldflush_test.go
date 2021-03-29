// +build integration

// Copyright (c) 2019 Uber Technologies, Inc.
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
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

func TestDiskColdFlushSimple(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run.
	}
	// Test setup with cold-writes-enabled namespace.
	nsOpts := namespace.NewOptions().
		SetRepairEnabled(false).
		SetRetentionOptions(DefaultIntegrationTestRetentionOpts.
			SetRetentionPeriod(12 * time.Hour)).
		SetColdWritesEnabled(true)
	nsID := ident.StringID("testColdWriteNs1")
	ns, err := namespace.NewMetadata(nsID, nsOpts)
	require.NoError(t, err)
	testOpts := NewTestOptions(t).
		SetTickMinimumInterval(time.Second).
		SetNamespaces([]namespace.Metadata{ns})

	testSetup, err := NewTestSetup(t, testOpts, nil)

	require.NoError(t, err)
	defer testSetup.Close()

	md := testSetup.NamespaceMetadataOrFail(nsID)
	ropts := md.Options().RetentionOptions()
	blockSize := ropts.BlockSize()
	filePathPrefix := testSetup.StorageOpts().CommitLogOptions().FilesystemOptions().FilePathPrefix()

	// Start the server.
	log := testSetup.StorageOpts().InstrumentOptions().Logger()
	log.Debug("disk coldflush test")
	require.NoError(t, testSetup.StartServer())
	log.Debug("server is now up")

	// Stop the server.
	defer func() {
		require.NoError(t, testSetup.StopServer())
		log.Debug("server is now down")
	}()

	// Write warm data first so that cold data will flush.
	start := testSetup.NowFn()()
	seriesMaps := make(map[xtime.UnixNano]generate.SeriesBlock)
	warmData := []generate.BlockConfig{
		{IDs: []string{"warm1", "warm2"}, NumPoints: 100, Start: start},
		// The `coldOverwrite` series data produced from this will later be
		// completely overwritten when cold data is flushed later in the test.
		// In order to satisfy this assumption, `coldOverwrite` needs to be on
		// the same block with equal or fewer NumPoints as its corresponding
		// cold data. Since `coldOverwrite` warm data is later overwritten,
		// we remove this from the expected `seriesMaps`.
		{IDs: []string{"warm1", "warm3", "coldOverwrite"}, NumPoints: 50, Start: start.Add(blockSize)},
	}

	expectedDataFiles := []fs.FileSetFileIdentifier{
		fs.FileSetFileIdentifier{
			// warm1, start
			Namespace:   nsID,
			Shard:       6,
			BlockStart:  start,
			VolumeIndex: 0,
		},
		fs.FileSetFileIdentifier{
			// warm1, start + 1
			Namespace:   nsID,
			Shard:       6,
			BlockStart:  start.Add(blockSize),
			VolumeIndex: 0,
		},
		fs.FileSetFileIdentifier{
			// warm2, start
			Namespace:   nsID,
			Shard:       11,
			BlockStart:  start,
			VolumeIndex: 0,
		},
		fs.FileSetFileIdentifier{
			// warm3, start + 1
			Namespace:   nsID,
			Shard:       2,
			BlockStart:  start.Add(blockSize),
			VolumeIndex: 0,
		},
		fs.FileSetFileIdentifier{
			// coldWrite, start + 1
			Namespace:   nsID,
			Shard:       8,
			BlockStart:  start.Add(blockSize),
			VolumeIndex: 0,
		},
	}
	for _, input := range warmData {
		testSetup.SetNowFn(input.Start)
		testData := generate.Block(input)
		seriesMaps[xtime.ToUnixNano(input.Start)] = testData
		require.NoError(t, testSetup.WriteBatch(nsID, testData))
	}
	startPlusOneBlockNano := xtime.ToUnixNano(start.Add(blockSize))
	// Remove warm data for `coldOverwrite`. See earlier comment for context.
	seriesMaps[startPlusOneBlockNano] =
		seriesMaps[startPlusOneBlockNano][:len(seriesMaps[startPlusOneBlockNano])-1]
	log.Debug("warm data is now written")

	// Advance time to make sure all data are flushed. Because data
	// are flushed to disk asynchronously, need to poll to check
	// when data are written.
	testSetup.SetNowFn(testSetup.NowFn()().Add(blockSize * 2))
	maxWaitTime := time.Minute
	require.NoError(t, waitUntilFileSetFilesExist(filePathPrefix, expectedDataFiles, maxWaitTime))

	// Verify on-disk data match what we expect.
	verifyFlushedDataFiles(t, testSetup.ShardSet(), testSetup.StorageOpts(), nsID, seriesMaps)

	coldData := []generate.BlockConfig{
		{IDs: []string{"cold0"}, NumPoints: 80, Start: start.Add(-blockSize)},
		{IDs: []string{"cold1", "cold2", "cold3"}, NumPoints: 30, Start: start},
		{IDs: []string{"cold1", "cold3", "coldOverwrite"}, NumPoints: 100, Start: start.Add(blockSize)},
	}
	// Set "now" to start + 3 * blockSize so that the above are cold writes.
	testSetup.SetNowFn(start.Add(blockSize * 3))
	for _, input := range coldData {
		testData := generate.Block(input)
		seriesMaps[xtime.ToUnixNano(input.Start)] = append(seriesMaps[xtime.ToUnixNano(input.Start)], testData...)
		require.NoError(t, testSetup.WriteBatch(nsID, testData))
	}
	log.Debug("cold data is now written")

	expectedDataFiles = []fs.FileSetFileIdentifier{
		fs.FileSetFileIdentifier{
			// warm1, start
			Namespace:   nsID,
			Shard:       6,
			BlockStart:  start,
			VolumeIndex: 0,
		},
		fs.FileSetFileIdentifier{
			// warm2, start (creating volume 0)
			// cold3, start (creating volume 1)
			Namespace:   nsID,
			Shard:       11,
			BlockStart:  start,
			VolumeIndex: 1,
		},
		fs.FileSetFileIdentifier{
			// warm1, start + 1
			Namespace:   nsID,
			Shard:       6,
			BlockStart:  start.Add(blockSize),
			VolumeIndex: 0,
		},
		fs.FileSetFileIdentifier{
			// warm3, start + 1
			Namespace:   nsID,
			Shard:       2,
			BlockStart:  start.Add(blockSize),
			VolumeIndex: 0,
		},
		fs.FileSetFileIdentifier{
			// cold0, start - 1
			Namespace:   nsID,
			Shard:       2,
			BlockStart:  start.Add(-blockSize),
			VolumeIndex: 1,
		},
		fs.FileSetFileIdentifier{
			// cold1, start
			Namespace:   nsID,
			Shard:       4,
			BlockStart:  start,
			VolumeIndex: 1,
		},
		fs.FileSetFileIdentifier{
			// cold2, start
			Namespace:   nsID,
			Shard:       7,
			BlockStart:  start,
			VolumeIndex: 1,
		},
		fs.FileSetFileIdentifier{
			// cold1, start + 1
			Namespace:   nsID,
			Shard:       4,
			BlockStart:  start.Add(blockSize),
			VolumeIndex: 1,
		},
		fs.FileSetFileIdentifier{
			// cold3, start + 1
			Namespace:   nsID,
			Shard:       11,
			BlockStart:  start.Add(blockSize),
			VolumeIndex: 1,
		},
		fs.FileSetFileIdentifier{
			// coldWrite, start + 1
			Namespace:   nsID,
			Shard:       8,
			BlockStart:  start.Add(blockSize),
			VolumeIndex: 1,
		},
	}

	require.NoError(t, waitUntilFileSetFilesExist(filePathPrefix, expectedDataFiles, maxWaitTime))

	// Verify on-disk data match what we expect
	verifyFlushedDataFiles(t, testSetup.ShardSet(), testSetup.StorageOpts(), nsID, seriesMaps)
}
