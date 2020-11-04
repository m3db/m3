// +build integration

// Copyright (c) 2020 Uber Technologies, Inc.
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
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/x/ident"

	"github.com/stretchr/testify/require"
)

func TestCommitLogIndexBootstrapWithSnapshots(t *testing.T) {
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
			SetBlockSize(blockSize),
		).SetColdWritesEnabled(true)
	ns1, err := namespace.NewMetadata(testNamespaces[0], nsOpts)
	require.NoError(t, err)
	ns2, err := namespace.NewMetadata(testNamespaces[1], nsOpts)
	require.NoError(t, err)
	opts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{ns1, ns2})

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
			IDs: []string{bazSeries.ID.String()},
			// NB(bodu): Each dp adds 1 sec to the start time, therefore the baz series
			// only exists in snapshots due to the snapshot interval being 1 minute.
			// This tests whether or not we can properly bootstrap a series that we fully
			// rely on snapshots for.
			Tags:      bazSeries.Tags,
			NumPoints: 50,
			Start:     now.Truncate(blockSize),
		},
		{
			IDs:       []string{unindexedSeries.ID.String()},
			Tags:      ident.Tags{},
			NumPoints: 1,
			Start:     now,
		},
	})

	log.Info("writing data")
	var (
		snapshotInterval             = time.Minute
		numDatapointsNotInSnapshots  = 0
		numDatapointsNotInCommitLogs = 0
		snapshotsPred                = func(dp generate.TestValue) bool {
			blockStart := dp.Timestamp.Truncate(blockSize)
			if dp.Timestamp.Before(blockStart.Add(snapshotInterval)) {
				return true
			}

			numDatapointsNotInSnapshots++
			return false
		}
		commitLogPred = func(dp generate.TestValue) bool {
			blockStart := dp.Timestamp.Truncate(blockSize)
			if dp.Timestamp.Equal(blockStart.Add(snapshotInterval)) || dp.Timestamp.After(blockStart.Add(snapshotInterval)) {
				return true
			}

			numDatapointsNotInCommitLogs++
			return false
		}
	)
	for _, ns := range []namespace.Metadata{
		ns1,
		ns2,
	} {
		writeIndexSnapshotsWithPredicate(
			t, setup, seriesMaps, ns, snapshotsPred, snapshotInterval)
		writeSnapshotsWithPredicate(
			t, setup, seriesMaps, 0, ns, snapshotsPred, snapshotInterval)
		writeCommitLogDataWithPredicate(
			t, setup, commitLogOpts, seriesMaps, ns, commitLogPred)
	}
	// Ensure we've excluded some dps from data/index snapshot and commitlog files.
	require.True(t, numDatapointsNotInSnapshots > 0) // This num is 2x'ed but its fine.
	require.True(t, numDatapointsNotInCommitLogs > 0)
	log.Info("finished writing data")

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
	// should be present
	verifySeriesMaps(t, setup, testNamespaces[0], seriesMaps)
	verifySeriesMaps(t, setup, testNamespaces[1], seriesMaps)

	// Issue some index queries
	session, err := setup.M3DBClient().DefaultSession()
	require.NoError(t, err)

	start := now.Add(-rOpts.RetentionPeriod())
	end := now.Add(blockSize)
	queryOpts := index.QueryOptions{StartInclusive: start, EndExclusive: end}

	// Match all new_*r*
	regexpQuery, err := idx.NewRegexpQuery([]byte("city"), []byte("new_.*r.*"))
	require.NoError(t, err)
	iter, fetchResponse, err := session.FetchTaggedIDs(ns1.ID(),
		index.Query{Query: regexpQuery}, queryOpts)
	require.NoError(t, err)
	defer iter.Finalize()

	verifyQueryMetadataResults(t, iter, fetchResponse.Exhaustive, verifyQueryMetadataResultsOptions{
		namespace:  ns1.ID(),
		exhaustive: true,
		expected:   []generate.Series{fooSeries, barSeries},
	})

	// Match all *e*e*
	regexpQuery, err = idx.NewRegexpQuery([]byte("city"), []byte(".*e.*e.*"))
	require.NoError(t, err)
	iter, fetchResponse, err = session.FetchTaggedIDs(ns1.ID(),
		index.Query{Query: regexpQuery}, queryOpts)
	require.NoError(t, err)
	defer iter.Finalize()

	verifyQueryMetadataResults(t, iter, fetchResponse.Exhaustive, verifyQueryMetadataResultsOptions{
		namespace:  ns1.ID(),
		exhaustive: true,
		expected:   []generate.Series{barSeries, bazSeries},
	})
}
