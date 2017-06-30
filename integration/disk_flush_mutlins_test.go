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

	"github.com/m3db/m3db/integration/generate"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/namespace"

	"github.com/stretchr/testify/require"
)

func TestDiskFlushMultipleNamespace(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	// Test setup
	var (
		tickInterval       = 3 * time.Second
		commitLogBlockSize = time.Hour
		clROpts            = retention.NewOptions().SetRetentionPeriod(18 * time.Hour).SetBlockSize(commitLogBlockSize)
		ns1BlockSize       = 2 * time.Hour
		ns1ROpts           = clROpts.SetBlockSize(ns1BlockSize)
		ns2BlockSize       = 3 * time.Hour
		ns2ROpts           = clROpts.SetBlockSize(ns2BlockSize)

		ns1  = namespace.NewMetadata(testNamespaces[0], namespace.NewOptions().SetRetentionOptions(ns1ROpts))
		ns2  = namespace.NewMetadata(testNamespaces[1], namespace.NewOptions().SetRetentionOptions(ns2ROpts))
		opts = newTestOptions().SetTickInterval(tickInterval).SetNamespaces([]namespace.Metadata{ns1, ns2})
	)

	// Test setup
	testSetup, err := newTestSetup(opts)
	require.NoError(t, err)
	defer testSetup.close()

	clOpts := testSetup.storageOpts.CommitLogOptions()
	testSetup.storageOpts = testSetup.storageOpts.SetCommitLogOptions(clOpts.SetRetentionOptions(clROpts))
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
	ns1SeriesMaps := make(map[time.Time]generate.SeriesBlock)
	ns1InputData := []generate.BlockConfig{
		{[]string{"foo", "bar"}, 100, now},
		{[]string{"foo", "baz"}, 50, now.Add(ns1BlockSize)},
	}

	// test data for ns2
	ns2SeriesMaps := make(map[time.Time]generate.SeriesBlock)
	ns2InputData := []generate.BlockConfig{
		{[]string{"foo", "bar"}, 20, now},
	}

	for _, ns1Input := range ns1InputData {
		// write the data for ns1, always
		testSetup.setNowFn(ns1Input.Start)
		testData := generate.Block(ns1Input)
		ns1SeriesMaps[ns1Input.Start] = testData
		require.NoError(t, testSetup.writeBatch(testNamespaces[0], testData))
		log.Infof("wrote ns1 for time %v", ns1Input.Start)

		// when applicable, write the data for ns2, too
		for _, ns2Input := range ns2InputData {
			if ns1Input.Start != ns2Input.Start {
				continue
			}
			testData = generate.Block(ns2Input)
			ns2SeriesMaps[ns2Input.Start] = testData
			log.Infof("wrote ns2 for time %v", ns2Input.Start)
			require.NoError(t, testSetup.writeBatch(testNamespaces[1], testData))
		}
	}
	log.Infof("test data written successfully")

	// Advance time to make sure all data are flushed. Because data
	// are flushed to disk asynchronously, need to poll to check
	// when data are written.
	log.Infof("waiting until data is flushed")
	testSetup.setNowFn(testSetup.getNowFn().Add(3 * ns1BlockSize))
	require.NoError(t, waitUntilDataFlushed(filePathPrefix, testSetup.shardSet, testNamespaces[0], ns1SeriesMaps, tickInterval*12))
	require.NoError(t, waitUntilDataFlushed(filePathPrefix, testSetup.shardSet, testNamespaces[1], ns2SeriesMaps, tickInterval*5))
	log.Infof("data has been flushed")

	// Verify on-disk data match what we expect
	log.Infof("verifying flushed data")
	verifyFlushed(t, testSetup.shardSet, testSetup.storageOpts, testNamespaces[0], ns1SeriesMaps)
	verifyFlushed(t, testSetup.shardSet, testSetup.storageOpts, testNamespaces[1], ns2SeriesMaps)
	log.Infof("flushed data verified")
}
