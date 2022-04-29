//go:build integration
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
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/integration/generate"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

func TestDiskFlushSimple(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}
	// Test setup
	testOpts := NewTestOptions(t).
		SetTickMinimumInterval(time.Second)
	testSetup, err := NewTestSetup(t, testOpts, nil)
	require.NoError(t, err)
	defer testSetup.Close()

	md := testSetup.NamespaceMetadataOrFail(testNamespaces[0])
	blockSize := md.Options().RetentionOptions().BlockSize()
	filePathPrefix := testSetup.StorageOpts().CommitLogOptions().FilesystemOptions().FilePathPrefix()

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
	now := testSetup.NowFn()()
	seriesMaps := make(map[xtime.UnixNano]generate.SeriesBlock)
	inputData := []generate.BlockConfig{
		{IDs: []string{"foo", "bar"}, NumPoints: 100, Start: now},
		{IDs: []string{"foo", "baz"}, NumPoints: 50, Start: now.Add(blockSize)},
	}
	for _, input := range inputData {
		testSetup.SetNowFn(input.Start)
		testData := generate.Block(input)
		seriesMaps[input.Start] = testData
		require.NoError(t, testSetup.WriteBatch(testNamespaces[0], testData))
	}
	log.Debug("test data is now written")

	// Advance time to make sure all data are flushed. Because data
	// are flushed to disk asynchronously, need to poll to check
	// when data are written.
	testSetup.SetNowFn(testSetup.NowFn()().Add(blockSize * 2))
	maxWaitTime := time.Minute
	require.NoError(t, waitUntilDataFilesFlushed(filePathPrefix, testSetup.ShardSet(), testNamespaces[0], seriesMaps, maxWaitTime))

	// Verify on-disk data match what we expect
	verifyFlushedDataFiles(t, testSetup.ShardSet(), testSetup.StorageOpts(), testNamespaces[0], seriesMaps)
}
