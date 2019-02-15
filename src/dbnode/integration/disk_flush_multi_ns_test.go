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

	"github.com/m3db/m3/src/dbnode/integration/generate"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func TestDiskFlushMultipleNamespace(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	// Test setup
	var (
		rOpts        = retention.NewOptions().SetRetentionPeriod(18 * time.Hour)
		ns1BlockSize = 2 * time.Hour
		ns2BlockSize = 3 * time.Hour
		ns1ROpts     = rOpts.SetBlockSize(ns1BlockSize)
		ns2ROpts     = rOpts.SetBlockSize(ns2BlockSize)
	)

	ns1, err := namespace.NewMetadata(testNamespaces[0], namespace.NewOptions().SetRetentionOptions(ns1ROpts))
	require.NoError(t, err)
	ns2, err := namespace.NewMetadata(testNamespaces[1], namespace.NewOptions().SetRetentionOptions(ns2ROpts))
	require.NoError(t, err)
	opts := newTestOptions(t).
		SetNamespaces([]namespace.Metadata{ns1, ns2})

	// Test setup
	testSetup, err := newTestSetup(t, opts, nil)
	require.NoError(t, err)
	defer testSetup.close()

	clOpts := testSetup.storageOpts.CommitLogOptions()
	filePathPrefix := clOpts.FilesystemOptions().FilePathPrefix()

	// it's aligned to lcm of ns block sizes
	now := testSetup.getNowFn()

	// Start the server
	log := testSetup.storageOpts.InstrumentOptions().Logger()
	log.Info("disk flush multiple namespaces test")
	require.NoError(t, testSetup.startServer())
	log.Info("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, testSetup.stopServer())
		log.Info("server is now down")
	}()

	log.Info("generating test data")
	// test data for ns1
	ns1SeriesMaps := make(map[xtime.UnixNano]generate.SeriesBlock)
	ns1InputData := []generate.BlockConfig{
		{IDs: []string{"foo", "bar"}, NumPoints: 100, Start: now},
		{IDs: []string{"foo", "baz"}, NumPoints: 50, Start: now.Add(ns1BlockSize)},
	}

	// test data for ns2
	ns2SeriesMaps := make(map[xtime.UnixNano]generate.SeriesBlock)
	ns2InputData := []generate.BlockConfig{
		{IDs: []string{"foo", "bar"}, NumPoints: 20, Start: now},
	}

	for _, ns1Input := range ns1InputData {
		// write the data for ns1, always
		testSetup.setNowFn(ns1Input.Start)
		testData := generate.Block(ns1Input)
		ns1SeriesMaps[xtime.ToUnixNano(ns1Input.Start)] = testData
		require.NoError(t, testSetup.writeBatch(testNamespaces[0], testData))
		log.Infof("wrote ns1 for time %v", ns1Input.Start)

		// when applicable, write the data for ns2, too
		for _, ns2Input := range ns2InputData {
			if ns1Input.Start != ns2Input.Start {
				continue
			}
			testData = generate.Block(ns2Input)
			ns2SeriesMaps[xtime.ToUnixNano(ns2Input.Start)] = testData
			log.Infof("wrote ns2 for time %v", ns2Input.Start)
			require.NoError(t, testSetup.writeBatch(testNamespaces[1], testData))
		}
	}
	log.Infof("test data written successfully")

	// Advance time to make sure all data are flushed. Because data
	// are flushed to disk asynchronously, need to poll to check
	// when data are written.
	maxWaitTime := time.Minute
	log.Infof("waiting until data is flushed")
	testSetup.setNowFn(testSetup.getNowFn().Add(3 * ns1BlockSize))
	require.NoError(t, waitUntilDataFilesFlushed(filePathPrefix, testSetup.shardSet, testNamespaces[0], ns1SeriesMaps, maxWaitTime))
	require.NoError(t, waitUntilDataFilesFlushed(filePathPrefix, testSetup.shardSet, testNamespaces[1], ns2SeriesMaps, maxWaitTime))
	log.Infof("data has been flushed")

	// Verify on-disk data match what we expect
	log.Infof("verifying flushed data")
	verifyFlushedDataFiles(t, testSetup.shardSet, testSetup.storageOpts, testNamespaces[0], ns1SeriesMaps)
	verifyFlushedDataFiles(t, testSetup.shardSet, testSetup.storageOpts, testNamespaces[1], ns2SeriesMaps)
	log.Infof("flushed data verified")
}
