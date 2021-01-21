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

	indexpb "github.com/m3db/m3/src/dbnode/generated/proto/index"
	"github.com/m3db/m3/src/dbnode/integration/generate"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/m3ninx/generated/proto/fswriter"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

func TestPeersBootstrapIndexWithIndexingEnabled(t *testing.T) {
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
		SetBlockSize(blockSize)
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

	quxSeries := generate.Series{
		ID:   ident.StringID("qux"),
		Tags: ident.NewTags(ident.StringTag("city", "new_orleans")),
	}

	seriesMaps := generate.BlocksByStart([]generate.BlockConfig{
		{
			IDs:       []string{quxSeries.ID.String()},
			Tags:      quxSeries.Tags,
			NumPoints: 100,
			Start:     now.Add(-2 * blockSize),
		},
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

	// Start the last server with peers and filesystem bootstrappers
	require.NoError(t, setups[1].StartServer())
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

	// Issue some index queries to the second node which bootstrapped the metadata
	session, err := setups[1].M3DBClient().DefaultSession()
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
		expected:   []generate.Series{barSeries, bazSeries, quxSeries},
	})

	// Ensure that the index data for qux has been written to disk.
	numDocsPerBlockStart, err := getNumDocsPerBlockStart(
		ns1.ID(),
		setups[1].FilesystemOpts(),
	)
	require.NoError(t, err)
	numDocs, ok := numDocsPerBlockStart[xtime.ToUnixNano(now.Add(-2*blockSize).Truncate(blockSize))]
	require.True(t, ok)
	require.Equal(t, numDocs, 1)
}

type indexInfo struct {
	Info        indexpb.IndexVolumeInfo
	VolumeIndex int
}

func getNumDocsPerBlockStart(
	nsID ident.ID,
	fsOpts fs.Options,
) (map[xtime.UnixNano]int, error) {
	numDocsPerBlockStart := make(map[xtime.UnixNano]int)
	infoFiles := fs.ReadIndexInfoFiles(
		fsOpts.FilePathPrefix(),
		nsID,
		fsOpts.InfoReaderBufferSize(),
	)
	// Grab the latest index info file for each blockstart.
	latestIndexInfoPerBlockStart := make(map[xtime.UnixNano]indexInfo)
	for _, f := range infoFiles {
		info, ok := latestIndexInfoPerBlockStart[xtime.UnixNano(f.Info.BlockStart)]
		if !ok {
			latestIndexInfoPerBlockStart[xtime.UnixNano(f.Info.BlockStart)] = indexInfo{
				Info:        f.Info,
				VolumeIndex: f.ID.VolumeIndex,
			}
			continue
		}

		if f.ID.VolumeIndex > info.VolumeIndex {
			latestIndexInfoPerBlockStart[xtime.UnixNano(f.Info.BlockStart)] = indexInfo{
				Info:        f.Info,
				VolumeIndex: f.ID.VolumeIndex,
			}
		}
	}
	for blockStart, info := range latestIndexInfoPerBlockStart {
		for _, segment := range info.Info.Segments {
			metadata := fswriter.Metadata{}
			if err := metadata.Unmarshal(segment.Metadata); err != nil {
				return nil, err
			}
			numDocsPerBlockStart[blockStart] += int(metadata.NumDocs)
		}
	}
	return numDocsPerBlockStart, nil
}
