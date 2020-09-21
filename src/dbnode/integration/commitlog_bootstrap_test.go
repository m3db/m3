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
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestCommitLogBootstrap(t *testing.T) {
	testCommitLogBootstrap(t, nil, nil)
}

func TestProtoCommitLogBootstrap(t *testing.T) {
	testCommitLogBootstrap(t, setProtoTestOptions, setProtoTestInputConfig)
}

func testCommitLogBootstrap(t *testing.T, setTestOpts setTestOptions, updateInputConfig generate.UpdateBlockConfig) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	// Test setup
	var (
		ropts     = retention.NewOptions().SetRetentionPeriod(12 * time.Hour)
		blockSize = ropts.BlockSize()
	)
	ns1, err := namespace.NewMetadata(testNamespaces[0], namespace.NewOptions().SetRetentionOptions(ropts))
	require.NoError(t, err)
	ns2, err := namespace.NewMetadata(testNamespaces[1], namespace.NewOptions().SetRetentionOptions(ropts))
	require.NoError(t, err)
	opts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{ns1, ns2})
	if setTestOpts != nil {
		opts = setTestOpts(t, opts)
		ns1 = opts.Namespaces()[0]
		ns2 = opts.Namespaces()[1]
	}

	setup, err := NewTestSetup(t, opts, nil)
	require.NoError(t, err)
	defer setup.Close()

	commitLogOpts := setup.StorageOpts().CommitLogOptions().
		SetFlushInterval(defaultIntegrationTestFlushInterval)
	setup.SetStorageOpts(setup.StorageOpts().SetCommitLogOptions(commitLogOpts))

	log := setup.StorageOpts().InstrumentOptions().Logger()
	log.Info("commit log bootstrap test")

	// Write test data
	log.Info("generating data")
	now := setup.NowFn()()
	seriesMaps := generateSeriesMaps(30, updateInputConfig, now.Add(-2*blockSize), now.Add(-blockSize))
	log.Info("writing data")
	writes := writeCommitLogData(t, setup, commitLogOpts, seriesMaps, ns1, false)
	log.Info("finished writing data")

	// Setup bootstrapper after writing data so filesystem inspection can find it.
	setupCommitLogBootstrapperWithFSInspection(t, setup, commitLogOpts)

	setup.SetNowFn(now)
	// Start the server with filesystem bootstrapper
	require.NoError(t, setup.StartServer())
	log.Debug("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, setup.StopServer())
		log.Debug("server is now down")
	}()

	// Verify in-memory data match what we expect - all writes from seriesMaps
	// should be present
	metadatasByShard := testSetupMetadatas(t, setup, testNamespaces[0], now.Add(-2*blockSize), now)
	observedSeriesMaps := testSetupToSeriesMaps(t, setup, ns1, metadatasByShard)
	verifySeriesMapsEqual(t, seriesMaps, observedSeriesMaps)

	// Verify in-memory data match what we expect - no writes should be present
	// because we didn't issue any writes for this namespace.
	emptySeriesMaps := make(generate.SeriesBlocksByStart)
	metadatasByShard2 := testSetupMetadatas(t, setup, testNamespaces[1], now.Add(-2*blockSize), now)
	observedSeriesMaps2 := testSetupToSeriesMaps(t, setup, ns2, metadatasByShard2)
	verifySeriesMapsEqual(t, emptySeriesMaps, observedSeriesMaps2)

	counters := commitLogOpts.InstrumentOptions().MetricsScope().(tally.TestScope).Snapshot().Counters()
	require.Equal(t, writes, int(counters["bootstrapper-commitlog.commitlog.entries-read+"].Value()))
}
