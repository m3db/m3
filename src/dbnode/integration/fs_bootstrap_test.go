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
	persistfs "github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper/fs"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/namespace"

	"github.com/stretchr/testify/require"
)

func TestFilesystemBootstrap(t *testing.T) {
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

	opts := newTestOptions(t).
		SetNamespaces([]namespace.Metadata{ns1, ns2})

	// Test setup
	setup, err := newTestSetup(t, opts, nil)
	require.NoError(t, err)
	defer setup.close()

	fsOpts := setup.storageOpts.CommitLogOptions().FilesystemOptions()

	persistMgr, err := persistfs.NewPersistManager(fsOpts)
	require.NoError(t, err)

	noOpAll := bootstrapper.NewNoOpAllBootstrapperProvider()
	bsOpts := result.NewOptions().
		SetSeriesCachePolicy(setup.storageOpts.SeriesCachePolicy())
	bfsOpts := fs.NewOptions().
		SetResultOptions(bsOpts).
		SetFilesystemOptions(fsOpts).
		SetDatabaseBlockRetrieverManager(setup.storageOpts.DatabaseBlockRetrieverManager()).
		SetPersistManager(persistMgr)
	bs, err := fs.NewFileSystemBootstrapperProvider(bfsOpts, noOpAll)
	require.NoError(t, err)
	processOpts := bootstrap.NewProcessOptions().
		SetTopologyMapProvider(setup).
		SetOrigin(setup.origin)
	processProvider, err := bootstrap.NewProcessProvider(bs, processOpts, bsOpts)
	require.NoError(t, err)

	setup.storageOpts = setup.storageOpts.
		SetBootstrapProcessProvider(processProvider)

	// Write test data
	now := setup.getNowFn()
	seriesMaps := generate.BlocksByStart([]generate.BlockConfig{
		{IDs: []string{"foo", "bar"}, NumPoints: 100, Start: now.Add(-blockSize)},
		{IDs: []string{"foo", "baz"}, NumPoints: 50, Start: now},
	})
	require.NoError(t, writeTestDataToDisk(ns1, setup, seriesMaps))
	require.NoError(t, writeTestDataToDisk(ns2, setup, nil))

	// Start the server with filesystem bootstrapper
	log := setup.storageOpts.InstrumentOptions().Logger()
	log.Debug("filesystem bootstrap test")
	require.NoError(t, setup.startServer())
	log.Debug("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, setup.stopServer())
		log.Debug("server is now down")
	}()

	// Verify in-memory data match what we expect
	verifySeriesMaps(t, setup, testNamespaces[0], seriesMaps)
	verifySeriesMaps(t, setup, testNamespaces[1], nil)
}
