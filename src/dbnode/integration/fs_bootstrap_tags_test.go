// +build integration

// Copyright (c) 2018 Uber Technologies, Inc.
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

	"github.com/m3db/m3db/src/dbnode/integration/generate"
	"github.com/m3db/m3db/src/dbnode/retention"
	"github.com/m3db/m3db/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3db/src/dbnode/storage/bootstrap/bootstrapper"
	"github.com/m3db/m3db/src/dbnode/storage/bootstrap/bootstrapper/fs"
	"github.com/m3db/m3db/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3db/src/dbnode/storage/namespace"
	"github.com/m3db/m3x/ident"

	"github.com/stretchr/testify/require"
)

func TestFilesystemBootstrapTagsWithIndexingDisabled(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	var (
		blockSize = 2 * time.Hour
		rOpts     = retention.NewOptions().SetRetentionPeriod(2 * time.Hour).SetBlockSize(blockSize)
		idxOpts   = namespace.NewIndexOptions().SetEnabled(false)
		nOpts     = namespace.NewOptions().SetRetentionOptions(rOpts).SetIndexOptions(idxOpts)
	)
	ns1, err := namespace.NewMetadata(testNamespaces[0], nOpts)
	require.NoError(t, err)
	ns2, err := namespace.NewMetadata(testNamespaces[1], nOpts)
	require.NoError(t, err)

	opts := newTestOptions(t).
		SetCommitLogRetentionPeriod(rOpts.RetentionPeriod()).
		SetCommitLogBlockSize(blockSize).
		SetNamespaces([]namespace.Metadata{ns1, ns2})

	// Test setup
	setup, err := newTestSetup(t, opts, nil)
	require.NoError(t, err)
	defer setup.close()

	fsOpts := setup.storageOpts.CommitLogOptions().FilesystemOptions()

	noOpAll := bootstrapper.NewNoOpAllBootstrapperProvider()
	bsOpts := result.NewOptions().
		SetSeriesCachePolicy(setup.storageOpts.SeriesCachePolicy())
	bfsOpts := fs.NewOptions().
		SetResultOptions(bsOpts).
		SetFilesystemOptions(fsOpts).
		SetDatabaseBlockRetrieverManager(setup.storageOpts.DatabaseBlockRetrieverManager())
	bs := fs.NewFileSystemBootstrapperProvider(bfsOpts, noOpAll)
	processProvider := bootstrap.NewProcessProvider(
		bs, bootstrap.NewProcessOptions(), bsOpts)

	setup.storageOpts = setup.storageOpts.
		SetBootstrapProcessProvider(processProvider)

	// Write test data
	now := setup.getNowFn()
	seriesMaps := generate.BlocksByStart([]generate.BlockConfig{
		{
			IDs:       []string{"foo"},
			Tags:      ident.NewTags(ident.StringTag("aaa", "bbb"), ident.StringTag("ccc", "ddd")),
			NumPoints: 100,
			Start:     now.Add(-blockSize),
		},
		{
			IDs:       []string{"bar"},
			Tags:      ident.NewTags(ident.StringTag("eee", "fff")),
			NumPoints: 100,
			Start:     now.Add(-blockSize),
		},
		{
			IDs:       []string{"foo"},
			Tags:      ident.NewTags(ident.StringTag("aaa", "bbb"), ident.StringTag("ccc", "ddd")),
			NumPoints: 50,
			Start:     now,
		},
		{
			IDs:       []string{"baz"},
			Tags:      ident.NewTags(ident.StringTag("ggg", "hhh")),
			NumPoints: 50,
			Start:     now,
		},
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
