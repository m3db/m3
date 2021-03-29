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
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/idx"
	idxpersist "github.com/m3db/m3/src/m3ninx/persist"
	"github.com/m3db/m3/src/x/ident"

	"github.com/stretchr/testify/require"
)

func TestFilesystemBootstrapIndexVolumeTypes(t *testing.T) {
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

	opts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{ns1, ns2})

	// Test setup
	setup, err := NewTestSetup(t, opts, nil)
	require.NoError(t, err)
	defer setup.Close()

	require.NoError(t, setup.InitializeBootstrappers(InitializeBootstrappersOptions{
		WithFileSystem: true,
	}))

	// Write test data
	now := setup.NowFn()()

	fooSeries := generate.Series{
		ID:   ident.StringID("foo"),
		Tags: ident.NewTags(ident.StringTag("city", "new_york"), ident.StringTag("foo", "foo")),
	}
	fooDoc := doc.Metadata{
		ID: fooSeries.ID.Bytes(),
		Fields: []doc.Field{
			doc.Field{Name: []byte("city"), Value: []byte("new_york")},
			doc.Field{Name: []byte("foo"), Value: []byte("foo")},
		},
	}

	barSeries := generate.Series{
		ID:   ident.StringID("bar"),
		Tags: ident.NewTags(ident.StringTag("city", "new_jersey")),
	}
	barDoc := doc.Metadata{
		ID: barSeries.ID.Bytes(),
		Fields: []doc.Field{
			doc.Field{Name: []byte("city"), Value: []byte("new_jersey")},
		},
	}

	bazSeries := generate.Series{
		ID:   ident.StringID("baz"),
		Tags: ident.NewTags(ident.StringTag("city", "seattle")),
	}
	bazDoc := doc.Metadata{
		ID: bazSeries.ID.Bytes(),
		Fields: []doc.Field{
			doc.Field{Name: []byte("city"), Value: []byte("seattle")},
		},
	}

	quxSeries := generate.Series{
		ID:   ident.StringID("qux"),
		Tags: ident.NewTags(ident.StringTag("city", "new_harmony")),
	}
	quxDoc := doc.Metadata{
		ID: quxSeries.ID.Bytes(),
		Fields: []doc.Field{
			doc.Field{Name: []byte("city"), Value: []byte("new_harmony")},
		},
	}

	duxSeries := generate.Series{
		ID:   ident.StringID("dux"),
		Tags: ident.NewTags(ident.StringTag("city", "los_angeles")),
	}
	duxDoc := doc.Metadata{
		ID: duxSeries.ID.Bytes(),
		Fields: []doc.Field{
			doc.Field{Name: []byte("city"), Value: []byte("los_angeles")},
		},
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
			IDs:       []string{quxSeries.ID.String()},
			Tags:      quxSeries.Tags,
			NumPoints: 100,
			Start:     now.Add(-blockSize),
		},
		{
			IDs:       []string{duxSeries.ID.String()},
			Tags:      duxSeries.Tags,
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
	defaultIndexDocs := []doc.Metadata{
		fooDoc,
		barDoc,
		bazDoc,
	}
	extraIndexDocs := []doc.Metadata{
		quxDoc,
		duxDoc,
	}

	require.NoError(t, writeTestDataToDisk(ns1, setup, seriesMaps, 0))
	require.NoError(t, writeTestDataToDisk(ns2, setup, nil, 0))
	require.NoError(t, writeTestIndexDataToDisk(
		ns1,
		setup.StorageOpts(),
		idxpersist.DefaultIndexVolumeType,
		now.Add(-blockSize),
		setup.ShardSet().AllIDs(),
		defaultIndexDocs,
	))
	require.NoError(t, writeTestIndexDataToDisk(
		ns1,
		setup.StorageOpts(),
		idxpersist.IndexVolumeType("extra"),
		now.Add(-blockSize),
		setup.ShardSet().AllIDs(),
		extraIndexDocs,
	))

	// Start the server with filesystem bootstrapper
	log := setup.StorageOpts().InstrumentOptions().Logger()
	log.Debug("filesystem bootstrap multiple index volume types test")
	require.NoError(t, setup.StartServer())
	log.Debug("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, setup.StopServer())
		log.Debug("server is now down")
	}()

	// Verify data matches what we expect
	verifySeriesMaps(t, setup, testNamespaces[0], seriesMaps)
	verifySeriesMaps(t, setup, testNamespaces[1], nil)

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
		expected:   []generate.Series{fooSeries, barSeries, quxSeries},
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
		expected:   []generate.Series{barSeries, bazSeries, duxSeries},
	})
}
