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

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"

	"github.com/stretchr/testify/require"
)

func TestCommitLogBootstrapOnlyReadsRequiredFiles(t *testing.T) {
	// TODO(rartoul): Temporarily disabled until a subsequent P.R that will
	// improve and simplify the commitlog bootstrapping logic. This is fine
	// because this integration test protects against performance regressions
	// not correctness.
	// https://github.com/m3db/m3/issues/1383
	t.SkipNow()

	// Test setup
	var (
		ropts     = retention.NewOptions().SetRetentionPeriod(12 * time.Hour)
		blockSize = ropts.BlockSize()
	)
	ns1, err := namespace.NewMetadata(testNamespaces[0], namespace.NewOptions().SetRetentionOptions(ropts))
	require.NoError(t, err)
	opts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{ns1})

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
	seriesMaps := generateSeriesMaps(30, nil, now.Add(-2*blockSize), now.Add(-blockSize))
	log.Info("writing data")
	writeCommitLogData(t, setup, commitLogOpts, seriesMaps, ns1, false)
	log.Info("finished writing data")

	// The datapoints in this generated data are within the retention period and
	// would ordinarily be bootstrapped, however, we intentionally write them to a
	// commitlog file that has a timestamp outside of the retention period. This
	// allows us to verify the commitlog bootstrapping logic will not waste time
	// reading commitlog files that are outside of the retention period.
	log.Info("generating data")
	seriesMapsExpiredCommitlog := generateSeriesMaps(30, nil, now.Add(-2*blockSize), now.Add(-blockSize))
	log.Info("writing data to commitlog file with out of range timestamp")
	writeCommitLogDataSpecifiedTS(
		t,
		setup,
		commitLogOpts,
		seriesMapsExpiredCommitlog,
		ns1,
		now.Add(-2*ropts.RetentionPeriod()),
		false,
	)
	log.Info("finished writing data to commitlog file with out of range timestamp")

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
	// should be present, and none of the writes from seriesMapsExpiredCommitlog
	// should be present.
	metadatasByShard := testSetupMetadatas(t, setup, testNamespaces[0], now.Add(-2*blockSize), now)
	observedSeriesMaps := testSetupToSeriesMaps(t, setup, ns1, metadatasByShard)
	verifySeriesMapsEqual(t, seriesMaps, observedSeriesMaps)
}
