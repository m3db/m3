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

	"github.com/m3db/m3/src/dbnode/integration/generate"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

func TestAdminSessionFetchBlocksFromPeers(t *testing.T) {
	testAdminSessionFetchBlocksFromPeers(t, nil, nil)
}

func TestProtoAdminSessionFetchBlocksFromPeers(t *testing.T) {
	testAdminSessionFetchBlocksFromPeers(t, setProtoTestOptions, setProtoTestInputConfig)
}

// This test writes data, and retrieves it using AdminSession endpoints
// FetchMetadataBlocksFromPeers and FetchBlocksFromPeers. Verifying the
// retrieved value is the same as the written.
func testAdminSessionFetchBlocksFromPeers(t *testing.T, setTestOpts setTestOptions, updateInputConfig generate.UpdateBlockConfig) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	// Test setup
	testOpts := NewTestOptions(t)
	if setTestOpts != nil {
		testOpts = setTestOpts(t, testOpts)
	}
	testSetup, err := NewTestSetup(t, testOpts, nil)
	require.NoError(t, err)
	defer testSetup.Close()

	md := testSetup.NamespaceMetadataOrFail(testNamespaces[0])
	blockSize := md.Options().RetentionOptions().BlockSize()

	// Start the server
	log := testSetup.StorageOpts().InstrumentOptions().Logger()
	require.NoError(t, testSetup.StartServer())

	// Stop the server
	defer func() {
		require.NoError(t, testSetup.StopServer())
		log.Debug("server is now down")
	}()

	// Write test data
	now := testSetup.NowFn()()
	seriesMaps := make(map[xtime.UnixNano]generate.SeriesBlock)
	inputData := []generate.BlockConfig{
		{IDs: []string{"foo", "bar"}, NumPoints: 100, Start: now},
		{IDs: []string{"foo", "baz"}, NumPoints: 50, Start: now.Add(blockSize)},
	}
	if updateInputConfig != nil {
		updateInputConfig(inputData)
	}
	for _, input := range inputData {
		start := input.Start
		testSetup.SetNowFn(start)
		testData := generate.Block(input)
		seriesMaps[start] = testData
		require.NoError(t, testSetup.WriteBatch(testNamespaces[0], testData))
	}
	log.Debug("test data is now written")

	// Advance time and sleep for a long enough time so data blocks are sealed during ticking
	testSetup.SetNowFn(testSetup.NowFn()().Add(blockSize * 2))
	later := testSetup.NowFn()()
	testSetup.SleepFor10xTickMinimumInterval()

	metadatasByShard := testSetupMetadatas(t, testSetup, testNamespaces[0], now, later)
	observedSeriesMaps := testSetupToSeriesMaps(t, testSetup, md, metadatasByShard)

	// Verify retrieved data matches what we've written
	verifySeriesMapsEqual(t, seriesMaps, observedSeriesMaps)
}

func testSetupMetadatas(
	t *testing.T,
	testSetup TestSetup,
	namespace ident.ID,
	start xtime.UnixNano,
	end xtime.UnixNano,
) map[uint32][]block.ReplicaMetadata {
	// Retrieve written data using the AdminSession APIs
	// FetchMetadataBlocksFromPeers/FetchBlocksFromPeers
	adminClient := testSetup.M3DBVerificationAdminClient()
	level := topology.ReadConsistencyLevelMajority
	metadatasByShard, err := m3dbClientFetchBlocksMetadata(adminClient,
		namespace, testSetup.ShardSet().AllIDs(), start, end, level)
	require.NoError(t, err)
	return metadatasByShard
}

func filterSeriesByShard(
	testSetup TestSetup,
	seriesMap map[xtime.UnixNano]generate.SeriesBlock,
	desiredShards []uint32,
) map[xtime.UnixNano]generate.SeriesBlock {
	filteredMap := make(map[xtime.UnixNano]generate.SeriesBlock)
	for blockStart, series := range seriesMap {
		filteredSeries := make([]generate.Series, 0, len(series))
		for _, serie := range series {
			shard := testSetup.ShardSet().Lookup(serie.ID)
			for _, ss := range desiredShards {
				if ss == shard {
					filteredSeries = append(filteredSeries, serie)
					break
				}
			}
		}

		if len(filteredSeries) > 0 {
			filteredMap[blockStart] = filteredSeries
		}
	}

	return filteredMap
}

func verifySeriesMapsEqual(
	t *testing.T,
	expectedSeriesMap map[xtime.UnixNano]generate.SeriesBlock,
	observedSeriesMap map[xtime.UnixNano]generate.SeriesBlock,
) {
	// ensure same length
	require.Equal(t, len(expectedSeriesMap), len(observedSeriesMap))

	// ensure same set of keys
	for i := range expectedSeriesMap {
		_, ok := observedSeriesMap[i]
		require.True(t, ok, "%v is expected but not observed", i.ToTime().String())
	}

	// given same set of keys, ensure same values too
	for i := range expectedSeriesMap {
		expectedSeries := expectedSeriesMap[i]
		observedSeries := observedSeriesMap[i]
		require.Equal(t, len(expectedSeries), len(observedSeries))
		for _, es := range expectedSeries {
			found := false

			for _, os := range observedSeries {
				if !es.ID.Equal(os.ID) {
					continue
				}
				found = true

				// compare all the values in the series
				require.Equal(t, len(es.Data), len(os.Data),
					"data length mismatch for series - [time: %v, seriesID: %v]", i.ToTime().String(), es.ID.String())
				for idx := range es.Data {
					expectedData := es.Data[idx]
					observedData := os.Data[idx]
					require.Equal(t, expectedData.TimestampNanos, observedData.TimestampNanos,
						"data mismatch for series - [time: %v, seriesID: %v, idx: %v]",
						i.ToTime().String(), es.ID.String(), idx)
					require.Equal(t, expectedData.Value, observedData.Value,
						"data mismatch for series - [time: %v, seriesID: %v, idx: %v]",
						i.ToTime().String(), es.ID.String(), idx)
				}
			}

			require.True(t, found, "unable to find expected series - [time: %v, seriesID: %v]",
				i.ToTime().String(), es.ID.String())
		}
	}
}

func testSetupToSeriesMaps(
	t *testing.T,
	testSetup TestSetup,
	nsMetadata namespace.Metadata,
	metadatasByShard map[uint32][]block.ReplicaMetadata,
) map[xtime.UnixNano]generate.SeriesBlock {
	blockSize := nsMetadata.Options().RetentionOptions().BlockSize()
	seriesMap := make(map[xtime.UnixNano]generate.SeriesBlock)
	resultOpts := newDefaulTestResultOptions(testSetup.StorageOpts())
	consistencyLevel := testSetup.StorageOpts().RepairOptions().RepairConsistencyLevel()
	iterPool := testSetup.StorageOpts().ReaderIteratorPool()
	session, err := testSetup.M3DBVerificationAdminClient().DefaultAdminSession()
	require.NoError(t, err)
	require.NotNil(t, session)
	nsCtx := namespace.NewContextFrom(nsMetadata)

	for shardID, metadatas := range metadatasByShard {
		blocksIter, err := session.FetchBlocksFromPeers(nsMetadata, shardID,
			consistencyLevel, metadatas, resultOpts)
		require.NoError(t, err)
		require.NotNil(t, blocksIter)

		for blocksIter.Next() {
			_, id, blk := blocksIter.Current()
			ctx := context.NewBackground()
			reader, err := blk.Stream(ctx)
			require.NoError(t, err)
			readerIter := iterPool.Get()
			readerIter.Reset(reader, nsCtx.Schema)

			var datapoints []generate.TestValue
			for readerIter.Next() {
				datapoint, _, ann := readerIter.Current()
				datapoints = append(datapoints, generate.TestValue{Datapoint: datapoint, Annotation: ann})
			}
			require.NoError(t, readerIter.Err())
			require.NotEmpty(t, datapoints)

			readerIter.Close()
			ctx.Close()

			firstTS := datapoints[0].TimestampNanos
			seriesMapList := seriesMap[firstTS.Truncate(blockSize)]
			seriesMapList = append(seriesMapList, generate.Series{
				ID:   id,
				Data: datapoints,
			})
			seriesMap[firstTS.Truncate(blockSize)] = seriesMapList
		}
		require.NoError(t, blocksIter.Err())
	}
	return seriesMap
}
