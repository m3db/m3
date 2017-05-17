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

	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/bootstrapper"
	bfs "github.com/m3db/m3db/storage/bootstrap/bootstrapper/fs"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/log"

	"github.com/stretchr/testify/require"
)

func TestFilesystemDataExpiryBootstrap(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}
	// Test setup
	var (
		log    = xlog.SimpleLogger
		namesp = namespace.NewMetadata(testNamespaces[0], namespace.NewOptions())
		opts   = newTestOptions().
			SetNamespaces([]namespace.Metadata{namesp})
		setup *testSetup
		err   error
	)

	retrieverOpts := fs.NewBlockRetrieverOptions()

	blockRetrieverMgr := block.NewDatabaseBlockRetrieverManager(
		func(namespace ts.ID) (block.DatabaseBlockRetriever, error) {
			retriever := fs.NewBlockRetriever(retrieverOpts, setup.fsOpts)
			if err := retriever.Open(namespace); err != nil {
				return nil, err
			}
			return retriever, nil
		})

	opts = opts.SetDatabaseBlockRetrieverManager(blockRetrieverMgr)

	setup, err = newTestSetup(opts)
	require.NoError(t, err)
	defer setup.close()

	retentionOpts := retention.NewOptions().
		SetRetentionPeriod(6 * time.Hour).
		SetBlockSize(2 * time.Hour).
		SetBufferPast(10 * time.Minute).
		SetBufferFuture(2 * time.Minute).
		SetBufferDrain(3 * time.Second)

	fsOpts := setup.storageOpts.CommitLogOptions().FilesystemOptions()
	filePathPrefix := fsOpts.FilePathPrefix()
	noOpAll := bootstrapper.NewNoOpAllBootstrapper()
	bsOpts := result.NewOptions().
		SetRetentionOptions(setup.storageOpts.RetentionOptions())
	bfsOpts := bfs.NewOptions().
		SetResultOptions(bsOpts).
		SetFilesystemOptions(fsOpts).
		SetDatabaseBlockRetrieverManager(blockRetrieverMgr)
	bs := bfs.NewFileSystemBootstrapper(filePathPrefix, bfsOpts, noOpAll)
	process := bootstrap.NewProcess(bs, bsOpts)

	setup.storageOpts = setup.storageOpts.
		SetRetentionOptions(retentionOpts).
		SetBootstrapProcess(process)

	// Write test data
	now := setup.getNowFn()
	blockSize := setup.storageOpts.RetentionOptions().BlockSize()
	seriesMaps := generateTestDataByStart([]testData{
		{ids: []string{"foo", "bar"}, numPoints: 100, start: now.Add(-blockSize)},
	})
	require.NoError(t, writeTestDataToDisk(t, namesp.ID(), setup, seriesMaps))

	// Start the server with filesystem bootstrapper
	log.Debug("filesystem data expiry bootstrap test")
	require.NoError(t, setup.startServer())
	log.Debug("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, setup.stopServer())
		log.Debug("server is now down")
	}()

	// Verify in-memory data match what we expect
	verifySeriesMaps(t, setup, namesp.ID(), seriesMaps)
}
