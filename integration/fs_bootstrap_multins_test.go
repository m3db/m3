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

	"github.com/m3db/m3db/integration/generate"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/bootstrapper"
	"github.com/m3db/m3db/storage/bootstrap/bootstrapper/fs"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/storage/namespace"

	"github.com/stretchr/testify/require"
)

func TestFilesystemBootstrapMultipleNamespaces(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	// Test setup
	var (
		tickInterval       = 3 * time.Second
		commitLogBlockSize = time.Hour
		clROpts            = retention.NewOptions().SetRetentionPeriod(6 * time.Hour).SetBlockSize(commitLogBlockSize)
		ns1BlockSize       = 2 * time.Hour
		ns1ROpts           = clROpts.SetBlockSize(ns1BlockSize)
		ns2BlockSize       = 3 * time.Hour
		ns2ROpts           = clROpts.SetBlockSize(ns2BlockSize)

		ns1  = namespace.NewMetadata(testNamespaces[0], namespace.NewOptions().SetRetentionOptions(ns1ROpts))
		ns2  = namespace.NewMetadata(testNamespaces[1], namespace.NewOptions().SetRetentionOptions(ns2ROpts))
		opts = newTestOptions().SetTickInterval(tickInterval).SetNamespaces([]namespace.Metadata{ns1, ns2})
	)

	setup, err := newTestSetup(opts)
	require.NoError(t, err)
	defer setup.close()

	clOpts := setup.storageOpts.CommitLogOptions()
	setup.storageOpts = setup.storageOpts.SetCommitLogOptions(clOpts.SetRetentionOptions(clROpts))
	fsOpts := setup.storageOpts.CommitLogOptions().FilesystemOptions()
	filePathPrefix := fsOpts.FilePathPrefix()

	noOpAll := bootstrapper.NewNoOpAllBootstrapper()
	bsOpts := result.NewOptions()
	bfsOpts := fs.NewOptions().
		SetResultOptions(bsOpts).
		SetFilesystemOptions(fsOpts)

	bs, err := fs.NewFileSystemBootstrapper(filePathPrefix, bfsOpts, noOpAll)
	require.NoError(t, err)
	process := bootstrap.NewProcess(bs, bsOpts)

	setup.storageOpts = setup.storageOpts.
		SetBootstrapProcess(process)

	log := setup.storageOpts.InstrumentOptions().Logger()

	log.Info("generating data")
	// Write test data
	now := setup.getNowFn()
	ns1SeriesMaps := generate.BlocksByStart([]generate.BlockConfig{
		{[]string{"foo", "bar"}, 100, now.Add(-ns1BlockSize)},
		{[]string{"foo", "baz"}, 50, now},
	})
	ns2SeriesMaps := generate.BlocksByStart([]generate.BlockConfig{
		{[]string{"bar", "baz"}, 100, now.Add(-2 * ns2BlockSize)},
		{[]string{"foo", "bar"}, 100, now.Add(-ns2BlockSize)},
		{[]string{"foo", "baz"}, 50, now},
	})
	require.NoError(t, writeTestDataToDisk(ns1, setup, ns1SeriesMaps))
	require.NoError(t, writeTestDataToDisk(ns2, setup, ns2SeriesMaps))
	log.Info("generated data")

	// Start the server with filesystem bootstrapper
	log.Info("filesystem bootstrap test")
	require.NoError(t, setup.startServer())
	log.Info("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, setup.stopServer())
		log.Info("server is now down")
	}()

	// Verify in-memory data match what we expect
	verifySeriesMaps(t, setup, testNamespaces[0], ns1SeriesMaps)
	verifySeriesMaps(t, setup, testNamespaces[1], ns2SeriesMaps)
}
