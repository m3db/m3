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
	bcl "github.com/m3db/m3db/src/dbnode/storage/bootstrap/bootstrapper/commitlog"
	"github.com/m3db/m3db/src/dbnode/storage/index"
	"github.com/m3db/m3db/src/dbnode/storage/namespace"
	"github.com/m3db/m3db/src/m3ninx/idx"
	"github.com/m3db/m3x/ident"

	"github.com/stretchr/testify/require"
)

func TestCommitLogIndexBootstrap(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	// Test setup
	var (
		rOpts     = retention.NewOptions().SetRetentionPeriod(12 * time.Hour)
		blockSize = rOpts.BlockSize()
	)

	nsOpts := namespace.NewOptions().
		SetRetentionOptions(rOpts).
		SetIndexOptions(namespace.NewIndexOptions().
			SetEnabled(true).
			SetBlockSize(2 * blockSize),
		)
	ns1, err := namespace.NewMetadata(testNamespaces[0], nsOpts)
	require.NoError(t, err)
	ns2, err := namespace.NewMetadata(testNamespaces[1], nsOpts)
	require.NoError(t, err)
	opts := newTestOptions(t).
		SetCommitLogRetentionPeriod(rOpts.RetentionPeriod()).
		SetCommitLogBlockSize(blockSize).
		SetNamespaces([]namespace.Metadata{ns1, ns2})

	setup, err := newTestSetup(t, opts, nil)
	require.NoError(t, err)
	defer setup.close()

	commitLogOpts := setup.storageOpts.CommitLogOptions().
		SetFlushInterval(defaultIntegrationTestFlushInterval)
	setup.storageOpts = setup.storageOpts.SetCommitLogOptions(commitLogOpts)

	log := setup.storageOpts.InstrumentOptions().Logger()
	log.Info("commit log bootstrap test")

	// Write test data
	log.Info("generating data")
	now := setup.getNowFn()
	fooSeries := generate.Series{
		ID:   ident.StringID("foo"),
		Tags: ident.NewTags(ident.StringTag("city", "new_york"), ident.StringTag("foo", "foo")),
	}

	barSeries := generate.Series{
		ID:   ident.StringID("bar"),
		Tags: ident.NewTags(ident.StringTag("city", "new_jersey")),
	}

	bazSeries := generate.Series{
		ID:   ident.StringID("baz"),
		Tags: ident.NewTags(ident.StringTag("city", "seattle")),
	}

	unindexedSeries := generate.Series{
		ID: ident.StringID("unindexed"),
	}

	seriesMaps := generate.BlocksByStart([]generate.BlockConfig{
		{
			IDs:       []string{fooSeries.ID.String()},
			Tags:      fooSeries.Tags,
			NumPoints: 100,
			Start:     now.Add(-blockSize),
		},
		{
			IDs:       []string{barSeries.ID.String()},
			Tags:      barSeries.Tags,
			NumPoints: 100,
			Start:     now.Add(-blockSize),
		},
		{
			IDs:       []string{fooSeries.ID.String()},
			Tags:      fooSeries.Tags,
			NumPoints: 50,
			Start:     now,
		},
		{
			IDs:       []string{bazSeries.ID.String()},
			Tags:      bazSeries.Tags,
			NumPoints: 50,
			Start:     now,
		},
		{
			IDs:       []string{unindexedSeries.ID.String()},
			Tags:      ident.Tags{},
			NumPoints: 1,
			Start:     now,
		},
	})

	log.Info("writing data")
	writeCommitLogData(t, setup, commitLogOpts, seriesMaps, testNamespaces[0])
	writeCommitLogData(t, setup, commitLogOpts, seriesMaps, testNamespaces[1])
	log.Info("finished writing data")

	// Setup bootstrapper after writing data so filesystem inspection can find it.
	noOpAll := bootstrapper.NewNoOpAllBootstrapperProvider()
	bsOpts := newDefaulTestResultOptions(setup.storageOpts)
	bclOpts := bcl.NewOptions().
		SetResultOptions(bsOpts).
		SetCommitLogOptions(commitLogOpts)
	fsOpts := setup.storageOpts.CommitLogOptions().FilesystemOptions()
	bs, err := bcl.NewCommitLogBootstrapperProvider(
		bclOpts, mustInspectFilesystem(fsOpts), noOpAll)
	require.NoError(t, err)
	process := bootstrap.NewProcessProvider(
		bs, bootstrap.NewProcessOptions(), bsOpts)
	setup.storageOpts = setup.storageOpts.SetBootstrapProcessProvider(process)

	setup.setNowFn(now)
	// Start the server with filesystem bootstrapper
	require.NoError(t, setup.startServer())
	log.Debug("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, setup.stopServer())
		log.Debug("server is now down")
	}()

	// Verify in-memory data match what we expect - all writes from seriesMaps
	// should be present
	verifySeriesMaps(t, setup, testNamespaces[0], seriesMaps)
	verifySeriesMaps(t, setup, testNamespaces[1], seriesMaps)

	// Issue some index queries
	session, err := setup.m3dbClient.DefaultSession()
	require.NoError(t, err)

	start := now.Add(-rOpts.RetentionPeriod())
	end := now.Add(blockSize)
	queryOpts := index.QueryOptions{StartInclusive: start, EndExclusive: end}

	// Match all new_*r*
	regexpQuery, err := idx.NewRegexpQuery([]byte("city"), []byte("^new_.*r.*$"))
	require.NoError(t, err)
	iter, exhausitive, err := session.FetchTaggedIDs(ns1.ID(),
		index.Query{regexpQuery}, queryOpts)
	require.NoError(t, err)
	defer iter.Finalize()

	verifyQueryMetadataResults(t, iter, exhausitive, verifyQueryMetadataResultsOptions{
		namespace:   ns1.ID(),
		exhausitive: true,
		expected:    []generate.Series{fooSeries, barSeries},
	})

	// Match all *e*e*
	regexpQuery, err = idx.NewRegexpQuery([]byte("city"), []byte("^.*e.*e.*$"))
	require.NoError(t, err)
	iter, exhausitive, err = session.FetchTaggedIDs(ns1.ID(),
		index.Query{regexpQuery}, queryOpts)
	require.NoError(t, err)
	defer iter.Finalize()

	verifyQueryMetadataResults(t, iter, exhausitive, verifyQueryMetadataResultsOptions{
		namespace:   ns1.ID(),
		exhausitive: true,
		expected:    []generate.Series{barSeries, bazSeries},
	})
}
