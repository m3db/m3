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

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3db/integration/generate"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/bootstrapper"
	bcl "github.com/m3db/m3db/storage/bootstrap/bootstrapper/commitlog"
	"github.com/m3db/m3db/storage/namespace"
)

func TestCommitLogBootstrapMultipleNamespaces(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run

	}

	// Test setup
	var (
		tickInterval       = 3 * time.Second
		commitLogBlockSize = 15 * time.Minute
		clROpts            = retention.NewOptions().SetRetentionPeriod(48 * time.Hour).SetBlockSize(commitLogBlockSize)
		ns1BlockSize       = time.Hour
		ns1ROpts           = clROpts.SetRetentionPeriod(48 * time.Hour).SetBlockSize(ns1BlockSize)
		ns2BlockSize       = 30 * time.Minute
		ns2ROpts           = clROpts.SetRetentionPeriod(48 * time.Hour).SetBlockSize(ns2BlockSize)

		ns1 = namespace.NewMetadata(testNamespaces[0], namespace.NewOptions().SetRetentionOptions(ns1ROpts))
		ns2 = namespace.NewMetadata(testNamespaces[1], namespace.NewOptions().SetRetentionOptions(ns2ROpts))

		opts = newTestOptions().
			SetTickInterval(tickInterval).
			SetCommitLogRetention(clROpts).
			SetNamespaces([]namespace.Metadata{ns1, ns2})
	)

	// Test setup
	setup, err := newTestSetup(opts)
	require.NoError(t, err)
	defer setup.close()

	commitLogOpts := setup.storageOpts.CommitLogOptions().
		SetFlushInterval(defaultIntegrationTestFlushInterval)
	setup.storageOpts = setup.storageOpts.SetCommitLogOptions(commitLogOpts)

	noOpAll := bootstrapper.NewNoOpAllBootstrapper()
	bsOpts := newDefaulTestResultOptions(setup.storageOpts)
	bclOpts := bcl.NewOptions().
		SetResultOptions(bsOpts).
		SetCommitLogOptions(commitLogOpts)
	bs, err := bcl.NewCommitLogBootstrapper(bclOpts, noOpAll)
	require.NoError(t, err)
	process := bootstrap.NewProcess(bs, bsOpts)
	setup.storageOpts = setup.storageOpts.SetBootstrapProcess(process)

	log := setup.storageOpts.InstrumentOptions().Logger()

	// Write test data for ns1
	log.Info("generating data - ns1")
	now := setup.getNowFn()
	ns1SeriesMap := generate.BlocksByStart([]generate.BlockConfig{
		{[]string{"foo", "bar"}, 20, now.Add(ns1BlockSize)},
		{[]string{"bar", "baz"}, 50, now.Add(2 * ns1BlockSize)},
		{[]string{"and", "one"}, 40, now.Add(3 * ns1BlockSize)},
	})
	_, err = setup.storageOpts.NamespaceRegistry().Get(testNamespaces[0])
	require.NoError(t, err)
	log.Info("writing data - ns1")
	writeCommitLog(t, setup, ns1SeriesMap, testNamespaces[0])
	log.Info("written data - ns1")

	// Write test data for ns2
	log.Info("generating data - ns2")
	ns2SeriesMap := generate.BlocksByStart([]generate.BlockConfig{
		{[]string{"abc", "def"}, 20, now.Add(ns2BlockSize)},
		{[]string{"xyz", "lmn"}, 50, now.Add(2 * ns2BlockSize)},
		{[]string{"cat", "hax"}, 80, now.Add(3 * ns2BlockSize)},
		{[]string{"why", "this"}, 40, now.Add(4 * ns2BlockSize)},
	})
	_, err = setup.storageOpts.NamespaceRegistry().Get(testNamespaces[1])
	require.NoError(t, err)
	log.Info("writing data - ns2")
	writeCommitLog(t, setup, ns2SeriesMap, testNamespaces[1])
	log.Info("written data - ns2")

	later := now.Add(4 * ns1BlockSize)
	setup.setNowFn(later)
	// Start the server with filesystem bootstrapper
	require.NoError(t, setup.startServer())
	log.Debug("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, setup.stopServer())
		log.Debug("server is now down")
	}()

	log.Info("waiting until data is bootstrapped")
	bootstrapped := waitUntil(func() bool { return setup.db.IsBootstrapped() }, 20*time.Second)
	require.True(t, bootstrapped)
	log.Info("data bootstrapped")

	// Verify in-memory data match what we expect
	log.Info("verifying ns1 data")
	verifySeriesMaps(t, setup, testNamespaces[0], ns1SeriesMap)
	log.Info("verified ns1 data")

	log.Info("verifying ns2 data")
	verifySeriesMaps(t, setup, testNamespaces[1], ns2SeriesMap)
	log.Info("verified ns2 data")
}
