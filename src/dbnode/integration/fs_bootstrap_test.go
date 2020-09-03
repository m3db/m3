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
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"

	"github.com/stretchr/testify/require"
)

func TestFilesystemBootstrap(t *testing.T) {
	testFilesystemBootstrap(t, nil, nil)
}

func TestProtoFilesystemBootstrap(t *testing.T) {
	testFilesystemBootstrap(t, setProtoTestOptions, setProtoTestInputConfig)
}

func testFilesystemBootstrap(t *testing.T, setTestOpts setTestOptions, updateInputConfig generate.UpdateBlockConfig) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	var (
		blockSize = 2 * time.Hour
		rOpts     = retention.NewOptions().SetRetentionPeriod(2 * time.Hour).SetBlockSize(blockSize)
	)
	ns1, err := namespace.NewMetadata(testNamespaces[0], namespace.NewOptions().SetRetentionOptions(rOpts))
	require.NoError(t, err)
	ns2, err := namespace.NewMetadata(testNamespaces[1], namespace.NewOptions().SetRetentionOptions(rOpts))
	require.NoError(t, err)

	opts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{ns1, ns2})
	if setTestOpts != nil {
		opts = setTestOpts(t, opts)
		ns1 = opts.Namespaces()[0]
		ns2 = opts.Namespaces()[1]
	}

	// Test setup
	setup, err := NewTestSetup(t, opts, nil)
	require.NoError(t, err)
	defer setup.Close()

	require.NoError(t, setup.InitializeBootstrappers(InitializeBootstrappersOptions{
		WithFileSystem: true,
	}))

	// Write test data
	now := setup.NowFn()()
	inputData := []generate.BlockConfig{
		{IDs: []string{"foo", "bar"}, NumPoints: 100, Start: now.Add(-blockSize)},
		{IDs: []string{"foo", "baz"}, NumPoints: 50, Start: now},
	}
	if updateInputConfig != nil {
		updateInputConfig(inputData)
	}
	seriesMaps := generate.BlocksByStart(inputData)
	require.NoError(t, writeTestDataToDisk(ns1, setup, seriesMaps, 0))
	require.NoError(t, writeTestDataToDisk(ns2, setup, nil, 0))

	// Start the server with filesystem bootstrapper
	log := setup.StorageOpts().InstrumentOptions().Logger()
	log.Debug("filesystem bootstrap test")
	require.NoError(t, setup.StartServer())
	log.Debug("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, setup.StopServer())
		log.Debug("server is now down")
	}()

	// Verify in-memory data match what we expect
	verifySeriesMaps(t, setup, testNamespaces[0], seriesMaps)
	verifySeriesMaps(t, setup, testNamespaces[1], nil)
}
