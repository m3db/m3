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

func TestFilesystemBootstrapMultipleNamespaces(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	// Test setup
	var (
		rOpts        = retention.NewOptions().SetRetentionPeriod(6 * time.Hour)
		ns1BlockSize = 2 * time.Hour
		ns2BlockSize = 3 * time.Hour
		ns1ROpts     = rOpts.SetBlockSize(ns1BlockSize)
		ns2ROpts     = rOpts.SetBlockSize(ns2BlockSize)
	)

	ns1, err := namespace.NewMetadata(testNamespaces[0], namespace.NewOptions().SetRetentionOptions(ns1ROpts))
	require.NoError(t, err)
	ns2, err := namespace.NewMetadata(testNamespaces[1], namespace.NewOptions().SetRetentionOptions(ns2ROpts))
	require.NoError(t, err)
	opts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{ns1, ns2})

	setup, err := NewTestSetup(t, opts, nil)
	require.NoError(t, err)
	defer setup.Close()

	require.NoError(t, setup.InitializeBootstrappers(InitializeBootstrappersOptions{
		WithFileSystem: true,
	}))

	log := setup.StorageOpts().InstrumentOptions().Logger()

	log.Info("generating data")
	// Write test data
	now := setup.NowFn()()
	ns1SeriesMaps := generate.BlocksByStart([]generate.BlockConfig{
		{IDs: []string{"foo", "bar"}, NumPoints: 100, Start: now.Add(-ns1BlockSize)},
		{IDs: []string{"foo", "baz"}, NumPoints: 50, Start: now},
	})
	ns2SeriesMaps := generate.BlocksByStart([]generate.BlockConfig{
		{IDs: []string{"bar", "baz"}, NumPoints: 100, Start: now.Add(-2 * ns2BlockSize)},
		{IDs: []string{"foo", "bar"}, NumPoints: 100, Start: now.Add(-ns2BlockSize)},
		{IDs: []string{"foo", "baz"}, NumPoints: 50, Start: now},
	})
	require.NoError(t, writeTestDataToDiskWithIndex(ns1, setup, ns1SeriesMaps, 0))
	require.NoError(t, writeTestDataToDiskWithIndex(ns2, setup, ns2SeriesMaps, 0))
	log.Info("generated data")

	// Start the server with filesystem bootstrapper
	log.Info("filesystem bootstrap test")
	require.NoError(t, setup.StartServer())
	log.Info("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, setup.StopServer())
		log.Info("server is now down")
	}()

	// Verify in-memory data match what we expect
	verifySeriesMaps(t, setup, testNamespaces[0], ns1SeriesMaps)
	verifySeriesMaps(t, setup, testNamespaces[1], ns2SeriesMaps)
}
