//go:build integration
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
)

func TestCommitLogBootstrapMultipleNamespaces(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	// Test setup
	var (
		rOpts        = retention.NewOptions().SetRetentionPeriod(48 * time.Hour)
		ns1BlockSize = time.Hour
		ns2BlockSize = 30 * time.Minute
		ns1ROpts     = rOpts.SetBlockSize(ns1BlockSize)
		ns2ROpts     = rOpts.SetBlockSize(ns2BlockSize)
	)

	ns1, err := namespace.NewMetadata(testNamespaces[0], namespace.NewOptions().SetRetentionOptions(ns1ROpts))
	require.NoError(t, err)
	ns2, err := namespace.NewMetadata(testNamespaces[1], namespace.NewOptions().SetRetentionOptions(ns2ROpts))
	require.NoError(t, err)

	opts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{ns1, ns2})

	// Test setup
	setup, err := NewTestSetup(t, opts, nil)
	require.NoError(t, err)
	defer setup.Close()

	commitLogOpts := setup.StorageOpts().CommitLogOptions().
		SetFlushInterval(defaultIntegrationTestFlushInterval)
	setup.SetStorageOpts(setup.StorageOpts().SetCommitLogOptions(commitLogOpts))

	log := setup.StorageOpts().InstrumentOptions().Logger()

	// Write test data for ns1
	log.Info("generating data - ns1")
	now := setup.NowFn()()
	ns1SeriesMap := generate.BlocksByStart([]generate.BlockConfig{
		{IDs: []string{"foo", "bar"}, NumPoints: 20, Start: now.Add(ns1BlockSize)},
		{IDs: []string{"bar", "baz"}, NumPoints: 50, Start: now.Add(2 * ns1BlockSize)},
		{IDs: []string{"and", "one"}, NumPoints: 40, Start: now.Add(3 * ns1BlockSize)},
	})

	setup.NamespaceMetadataOrFail(testNamespaces[0])
	log.Info("writing data - ns1")
	writeCommitLogData(t, setup, commitLogOpts, ns1SeriesMap, ns1, false)
	log.Info("written data - ns1")

	// Write test data for ns2
	log.Info("generating data - ns2")
	ns2SeriesMap := generate.BlocksByStart([]generate.BlockConfig{
		{IDs: []string{"abc", "def"}, NumPoints: 20, Start: now.Add(ns2BlockSize)},
		{IDs: []string{"xyz", "lmn"}, NumPoints: 50, Start: now.Add(2 * ns2BlockSize)},
		{IDs: []string{"cat", "hax"}, NumPoints: 80, Start: now.Add(3 * ns2BlockSize)},
		{IDs: []string{"why", "this"}, NumPoints: 40, Start: now.Add(4 * ns2BlockSize)},
	})
	setup.NamespaceMetadataOrFail(testNamespaces[1])
	log.Info("writing data - ns2")
	writeCommitLogData(t, setup, commitLogOpts, ns2SeriesMap, ns2, false)
	log.Info("written data - ns2")

	// Setup bootstrapper after writing data so filesystem inspection can find it
	setupCommitLogBootstrapperWithFSInspection(t, setup, commitLogOpts)

	later := now.Add(4 * ns1BlockSize)
	setup.SetNowFn(later)
	// Start the server with filesystem bootstrapper
	require.NoError(t, setup.StartServer())
	log.Debug("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, setup.StopServer())
		log.Debug("server is now down")
	}()

	log.Info("waiting until data is bootstrapped")
	bootstrapped := waitUntil(func() bool { return setup.DB().IsBootstrapped() }, 20*time.Second)
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
