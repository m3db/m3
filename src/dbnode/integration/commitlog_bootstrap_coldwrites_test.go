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
	"github.com/m3db/m3/src/dbnode/retention"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

func TestCommitLogBootstrapColdWrites(t *testing.T) {
	testCommitLogBootstrapColdWrites(t, nil, nil)
}

func TestProtoCommitLogBootstrapColdWrites(t *testing.T) {
	testCommitLogBootstrapColdWrites(t, setProtoTestOptions, setProtoTestInputConfig)
}

func testCommitLogBootstrapColdWrites(t *testing.T, setTestOpts setTestOptions, updateInputConfig generate.UpdateBlockConfig) {
	// TODO(juchan): this test will periodically fail until this issue is
	// resolved: https://github.com/m3db/m3/issues/1747
	// When issuing a fetch, if commit log data is still in memory, all data
	// will be found. However, if commit log data has been evicted, the DB will
	// try to read from disk. However, since the namespace readers is hard coded
	// to look at volume 0, the commit log data which has been compacted to a
	// fileset with a higher volume number will not be found.
	t.SkipNow()

	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	// Test setup
	var (
		ropts     = retention.NewOptions().SetRetentionPeriod(12 * time.Hour)
		blockSize = ropts.BlockSize()
	)
	ns1, err := namespace.NewMetadata(testNamespaces[0], namespace.NewOptions().
		SetRetentionOptions(ropts).
		SetColdWritesEnabled(true))
	require.NoError(t, err)
	opts := newTestOptions(t).
		SetNamespaces([]namespace.Metadata{ns1})
	if setTestOpts != nil {
		opts = setTestOpts(t, opts)
		ns1 = opts.Namespaces()[0]
	}

	setup, err := newTestSetup(t, opts, nil)
	require.NoError(t, err)
	defer setup.close()

	commitLogOpts := setup.storageOpts.CommitLogOptions().
		SetFlushInterval(defaultIntegrationTestFlushInterval)
	setup.storageOpts = setup.storageOpts.SetCommitLogOptions(commitLogOpts)

	log := setup.storageOpts.InstrumentOptions().Logger()
	log.Info("commit log bootstrap test")

	start := setup.getNowFn()

	log.Info("writing data files")
	dataFilesData := []generate.BlockConfig{
		{IDs: []string{"foo", "bar"}, NumPoints: 100, Start: start.Add(-2 * blockSize)},
		{IDs: []string{"foo", "baz"}, NumPoints: 50, Start: start.Add(-blockSize)},
	}
	if updateInputConfig != nil {
		updateInputConfig(dataFilesData)
	}
	dataFilesSeriesMaps := generate.BlocksByStart(dataFilesData)
	require.NoError(t, writeTestDataToDisk(ns1, setup, dataFilesSeriesMaps, 0))
	log.Info("finished writing data files")

	log.Info("writing commit logs")
	commitLogData := []generate.BlockConfig{
		{IDs: []string{"commitlog1", "commitlog2"}, NumPoints: 120, Start: start.Add(-2 * blockSize)},
		{IDs: []string{"commitlog2", "commitlog3"}, NumPoints: 130, Start: start.Add(-blockSize)},
	}
	if updateInputConfig != nil {
		updateInputConfig(commitLogData)
	}
	commitLogSeriesMaps := generate.BlocksByStart(commitLogData)
	writeCommitLogData(t, setup, commitLogOpts, commitLogSeriesMaps, ns1, false)
	log.Info("finished writing commit logs")

	// Merge the two generated series maps together. We can only do this simply
	// here because we know that they span the same block starts and they do
	// not contains the same series.
	allSeriesMaps := make(map[xtime.UnixNano]generate.SeriesBlock, len(dataFilesSeriesMaps))
	for i := -2; i < 0; i++ {
		unixNano := xtime.ToUnixNano(start.Add(time.Duration(i) * blockSize))
		series := append(dataFilesSeriesMaps[unixNano], commitLogSeriesMaps[unixNano]...)
		allSeriesMaps[unixNano] = series
	}

	// Setup bootstrapper after writing data so filesystem inspection can find it.
	setupCommitLogBootstrapperWithFSInspection(t, setup, commitLogOpts)

	setup.setNowFn(start)
	// Start the server with filesystem bootstrapper
	require.NoError(t, setup.startServer())
	log.Debug("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, setup.stopServer())
		log.Debug("server is now down")
	}()

	// Verify in-memory data match what we expect - all writes from seriesMaps
	// should be present
	metadatasByShard := testSetupMetadatas(t, setup, testNamespaces[0], start.Add(-2*blockSize), start)
	observedSeriesMaps := testSetupToSeriesMaps(t, setup, ns1, metadatasByShard)
	verifySeriesMapsEqual(t, allSeriesMaps, observedSeriesMaps)
}
