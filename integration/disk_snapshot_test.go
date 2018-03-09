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
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3db/integration/generate"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func TestDiskSnapshotSimple(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}
	// Test setup
	testOpts := newTestOptions(t).
		SetTickMinimumInterval(time.Second)
	testSetup, err := newTestSetup(t, testOpts, nil)
	require.NoError(t, err)
	defer testSetup.close()

	md := testSetup.namespaceMetadataOrFail(testNamespaces[0])
	blockSize := md.Options().RetentionOptions().BlockSize()
	fmt.Println("blockSize: ", blockSize)
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
		{[]string{"foo", "bar"}, 100, now},
		{[]string{"foo", "baz"}, 50, now.Add(blockSize)},
	}
	for _, input := range inputData {
		testSetup.setNowFn(input.Start)
		testData := generate.Block(input)
		seriesMaps[xtime.ToUnixNano(input.Start)] = testData
		require.NoError(t, testSetup.writeBatch(testNamespaces[0], testData))
	}
	log.Debug("test data is now written")
	maxWaitTime := time.Minute
	require.NoError(t, waitUntilSnapshotFilesFlushed(filePathPrefix, testSetup.shardSet, testNamespaces[0], []time.Time{now}, maxWaitTime))
	log.Debug("snapshot file is now written")

	// Advance time to make sure all data are flushed. Because data
	// are flushed to disk asynchronously, need to poll to check
	// when data are written.
	newTime := testSetup.getNowFn().Add(blockSize * 2)
	testSetup.setNowFn(newTime)
	require.NoError(t, waitUntilSnapshotFilesFlushed(filePathPrefix, testSetup.shardSet, testNamespaces[0], []time.Time{newTime}, maxWaitTime))
}
