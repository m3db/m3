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
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3ninx/idx"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/integration/generate"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/bootstrapper"
	"github.com/m3db/m3db/storage/bootstrap/bootstrapper/fs"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/storage/index"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3x/ident"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilesystemBootstrapIndexWithIndexingEnabled(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	var (
		blockSize = 2 * time.Hour
		rOpts     = retention.NewOptions().SetRetentionPeriod(2 * blockSize).SetBlockSize(blockSize)
		idxOpts   = namespace.NewIndexOptions().SetEnabled(true).SetBlockSize(2 * blockSize)
		nOpts     = namespace.NewOptions().SetRetentionOptions(rOpts).SetIndexOptions(idxOpts)
	)
	ns1, err := namespace.NewMetadata(testNamespaces[0], nOpts)
	require.NoError(t, err)
	ns2, err := namespace.NewMetadata(testNamespaces[1], nOpts)
	require.NoError(t, err)

	opts := newTestOptions(t).
		SetCommitLogRetentionPeriod(rOpts.RetentionPeriod()).
		SetCommitLogBlockSize(blockSize).
		SetNamespaces([]namespace.Metadata{ns1, ns2}).
		SetIndexingEnabled(true)

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
	processProvider := bootstrap.NewProcessProvider(bs, bsOpts)

	setup.storageOpts = setup.storageOpts.
		SetBootstrapProcessProvider(processProvider)

	// Write test data
	now := setup.getNowFn()

	fooSeries := generate.Series{
		ID:   ident.StringID("foo"),
		Tags: ident.Tags{ident.StringTag("city", "new_york"), ident.StringTag("foo", "foo")},
	}

	barSeries := generate.Series{
		ID:   ident.StringID("bar"),
		Tags: ident.Tags{ident.StringTag("city", "new_jersey")},
	}

	bazSeries := generate.Series{
		ID:   ident.StringID("baz"),
		Tags: ident.Tags{ident.StringTag("city", "seattle")},
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

	// Verify data matches what we expect
	verifySeriesMaps(t, setup, testNamespaces[0], seriesMaps)
	verifySeriesMaps(t, setup, testNamespaces[1], nil)

	// Issue some index queries
	session, err := setup.m3dbClient.DefaultSession()
	require.NoError(t, err)

	start := now.Add(-rOpts.RetentionPeriod())
	end := now.Add(blockSize)
	queryOpts := index.QueryOptions{StartInclusive: start, EndExclusive: end}

	// Match all new_*r* from namespace 1
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

	// Match all *e*e* from namespace 2
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

type verifyQueryMetadataResultsOptions struct {
	namespace   ident.ID
	exhausitive bool
	expected    []generate.Series
}

type verifyQueryMetadataResult struct {
	series  generate.Series
	matched bool
}

func verifyQueryMetadataResults(
	t *testing.T,
	iter client.TaggedIDsIterator,
	exhausitive bool,
	opts verifyQueryMetadataResultsOptions,
) {
	assert.Equal(t, opts.exhausitive, exhausitive)

	expected := map[string]*verifyQueryMetadataResult{}
	for _, series := range opts.expected {
		expected[series.ID.String()] = &verifyQueryMetadataResult{
			series:  series,
			matched: false,
		}
	}

	compared := 0
	for iter.Next() {
		compared++

		ns, id, tags := iter.Current()
		assert.True(t, opts.namespace.Equal(ns))

		idStr := id.String()
		result, ok := expected[idStr]
		require.True(t, ok,
			fmt.Sprintf("not expecting ID: %s", idStr))

		expectedTagsIter := ident.NewTagSliceIterator(result.series.Tags)
		matcher := ident.NewTagIterMatcher(expectedTagsIter)
		assert.True(t, matcher.Matches(tags),
			fmt.Sprintf("tags not matching for ID: %s", idStr))

		result.matched = true
	}
	require.NoError(t, iter.Err())

	var matched, notMatched []string
	for _, elem := range expected {
		if elem.matched {
			matched = append(matched, elem.series.ID.String())
			continue
		}
		notMatched = append(notMatched, elem.series.ID.String())
	}

	assert.Equal(t, len(expected), compared,
		fmt.Sprintf("matched: %v, not matched: %v", matched, notMatched))
}
