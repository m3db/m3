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

	"github.com/m3db/m3db/src/dbnode/client"
	"github.com/m3db/m3db/src/dbnode/integration/generate"
	"github.com/m3db/m3db/src/dbnode/retention"
	"github.com/m3db/m3db/src/dbnode/storage/index"
	"github.com/m3db/m3db/src/dbnode/storage/namespace"
	"github.com/m3db/m3ninx/idx"
	"github.com/m3db/m3x/ident"
	xlog "github.com/m3db/m3x/log"

	"github.com/stretchr/testify/require"
)

func TestPeersBootstrapIndexWithIndexingEnabled(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	log := xlog.SimpleLogger

	// Index metadata exchange is only possible with version 2
	version := client.FetchBlocksMetadataEndpointV2
	blockSize := 2 * time.Hour

	rOpts := retention.NewOptions().
		SetRetentionPeriod(20 * time.Hour).
		SetBlockSize(blockSize).
		SetBufferPast(10 * time.Minute).
		SetBufferFuture(2 * time.Minute)

	idxOpts := namespace.NewIndexOptions().
		SetEnabled(true).
		SetBlockSize(2 * blockSize)
	nOpts := namespace.NewOptions().
		SetRetentionOptions(rOpts).
		SetIndexOptions(idxOpts)
	ns1, err := namespace.NewMetadata(testNamespaces[0], nOpts)
	require.NoError(t, err)
	opts := newTestOptions(t).
		SetNamespaces([]namespace.Metadata{ns1})

	setupOpts := []bootstrappableTestSetupOptions{
		{disablePeersBootstrapper: true},
		{disablePeersBootstrapper: false, fetchBlocksMetadataEndpointVersion: version},
	}
	setups, closeFn := newDefaultBootstrappableTestSetups(t, opts, setupOpts)
	defer closeFn()

	// Write test data for first node
	// Write test data
	now := setups[0].getNowFn()

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
	require.NoError(t, writeTestDataToDisk(ns1, setups[0], seriesMaps))

	// Start the first server with filesystem bootstrapper
	require.NoError(t, setups[0].startServer())

	// Start the last server with peers and filesystem bootstrappers
	require.NoError(t, setups[1].startServer())
	log.Debug("servers are now up")

	// Stop the servers
	defer func() {
		setups.parallel(func(s *testSetup) {
			require.NoError(t, s.stopServer())
		})
		log.Debug("servers are now down")
	}()

	// Verify in-memory data match what we expect
	for _, setup := range setups {
		verifySeriesMaps(t, setup, ns1.ID(), seriesMaps)
	}

	// Issue some index queries to the second node which bootstrapped the metadata
	session, err := setups[1].m3dbClient.DefaultSession()
	require.NoError(t, err)

	start := now.Add(-rOpts.RetentionPeriod())
	end := now.Add(blockSize)
	queryOpts := index.QueryOptions{StartInclusive: start, EndExclusive: end}

	// Match all new_*r*
	regexpQuery, err := idx.NewRegexpQuery([]byte("city"), []byte("new_.*r.*"))
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
	regexpQuery, err = idx.NewRegexpQuery([]byte("city"), []byte(".*e.*e.*"))
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
