// +build integration

// Copyright (c) 2019 Uber Technologies, Inc.
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
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/stretchr/testify/require"
)

func TestPeersBootstrapIndexAggregateQuery(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	log := xtest.NewLogger(t)
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
	opts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{ns1}).
		// Use TChannel clients for writing / reading because we want to target individual nodes at a time
		// and not write/read all nodes in the cluster.
		SetUseTChannelClientForWriting(true).
		SetUseTChannelClientForReading(true)

	setupOpts := []BootstrappableTestSetupOptions{
		{DisablePeersBootstrapper: true},
		{DisablePeersBootstrapper: false},
	}
	setups, closeFn := NewDefaultBootstrappableTestSetups(t, opts, setupOpts)
	defer closeFn()

	// Write test data for first node
	// Write test data
	now := setups[0].NowFn()()

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
	require.NoError(t, writeTestDataToDisk(ns1, setups[0], seriesMaps, 0))

	// Start the first server with filesystem bootstrapper
	require.NoError(t, setups[0].StartServer())

	// Start the remaining servers with peers and filesystem bootstrappers
	setups[1:].parallel(func(s TestSetup) {
		require.NoError(t, s.StartServer())
	})
	log.Debug("servers are now up")

	// Stop the servers
	defer func() {
		setups.parallel(func(s TestSetup) {
			require.NoError(t, s.StopServer())
		})
		log.Debug("servers are now down")
	}()

	// Verify in-memory data match what we expect
	for _, setup := range setups {
		verifySeriesMaps(t, setup, ns1.ID(), seriesMaps)
	}

	// Issue aggregate index queries to the second node which bootstrapped the metadata
	session, err := setups[1].M3DBClient().DefaultSession()
	require.NoError(t, err)

	start := now.Add(-rOpts.RetentionPeriod())
	end := now.Add(blockSize)
	queryOpts := index.AggregationOptions{
		QueryOptions: index.QueryOptions{StartInclusive: start, EndExclusive: end},
	}

	// Match all new_*r*
	regexpQuery, err := idx.NewRegexpQuery([]byte("city"), []byte("new_.*r.*"))
	require.NoError(t, err)
	iter, fetchResponse, err := session.Aggregate(ns1.ID(),
		index.Query{Query: regexpQuery}, queryOpts)
	require.NoError(t, err)
	exhaustive := fetchResponse.Exhaustive
	require.True(t, exhaustive)
	defer iter.Finalize()

	verifyQueryAggregateMetadataResults(t, iter, exhaustive,
		verifyQueryAggregateMetadataResultsOptions{
			exhaustive: true,
			expected: map[tagName]aggregateTagValues{
				"city": aggregateTagValues{
					"new_jersey": struct{}{},
					"new_york":   struct{}{},
				},
				"foo": aggregateTagValues{
					"foo": struct{}{},
				},
			},
		})

	// Match all *e*e*
	regexpQuery, err = idx.NewRegexpQuery([]byte("city"), []byte(".*e.*e.*"))
	require.NoError(t, err)
	iter, fetchResponse, err = session.Aggregate(ns1.ID(),
		index.Query{Query: regexpQuery}, queryOpts)
	require.NoError(t, err)
	exhaustive = fetchResponse.Exhaustive
	defer iter.Finalize()

	verifyQueryAggregateMetadataResults(t, iter, exhaustive,
		verifyQueryAggregateMetadataResultsOptions{
			exhaustive: true,
			expected: map[tagName]aggregateTagValues{
				"city": aggregateTagValues{
					"new_jersey": struct{}{},
					"seattle":    struct{}{},
				},
			},
		})

	// Now test term filtering, match all new_*r*, filtering on `foo`
	regexpQuery, err = idx.NewRegexpQuery([]byte("city"), []byte("new_.*r.*"))
	require.NoError(t, err)
	queryOpts.FieldFilter = index.AggregateFieldFilter([][]byte{[]byte("foo")})
	iter, fetchResponse, err = session.Aggregate(ns1.ID(),
		index.Query{Query: regexpQuery}, queryOpts)
	require.NoError(t, err)
	exhaustive = fetchResponse.Exhaustive
	require.True(t, exhaustive)
	defer iter.Finalize()

	verifyQueryAggregateMetadataResults(t, iter, exhaustive,
		verifyQueryAggregateMetadataResultsOptions{
			exhaustive: true,
			expected: map[tagName]aggregateTagValues{
				"foo": aggregateTagValues{
					"foo": struct{}{},
				},
			},
		})

	// Now test term filter and tag name filtering, match all new_*r*, names only, filtering on `city`
	regexpQuery, err = idx.NewRegexpQuery([]byte("city"), []byte("new_.*r.*"))
	require.NoError(t, err)
	queryOpts.FieldFilter = index.AggregateFieldFilter([][]byte{[]byte("city")})
	queryOpts.Type = index.AggregateTagNames
	iter, fetchResponse, err = session.Aggregate(ns1.ID(),
		index.Query{Query: regexpQuery}, queryOpts)
	require.NoError(t, err)
	exhaustive = fetchResponse.Exhaustive
	require.True(t, exhaustive)
	defer iter.Finalize()

	verifyQueryAggregateMetadataResults(t, iter, exhaustive,
		verifyQueryAggregateMetadataResultsOptions{
			exhaustive: true,
			expected: map[tagName]aggregateTagValues{
				"city": nil,
			},
		})
}
