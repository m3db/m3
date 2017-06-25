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

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/integration/generate"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/ts"
)

// This test writes data, and retrieves it using AdminSession endpoints
// FetchMetadataBlocksFromPeers and FetchBlocksFromPeers. Verifying the
// retrieved value is the same as the written.
func TestAdminSessionFetchBlocksFromPeers(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	// Test setup
	tickInterval := time.Second
	testSetup, err := newTestSetup(newTestOptions().SetTickInterval(tickInterval))
	require.NoError(t, err)
	defer testSetup.close()

	var (
		ropts     = retention.NewOptions().SetRetentionPeriod(6 * time.Hour)
		blockSize = ropts.BlockSize()
	)
	require.NoError(t, testSetup.setRetentionOnAll(ropts))

	// Start the server
	log := testSetup.storageOpts.InstrumentOptions().Logger()
	require.NoError(t, testSetup.startServer())

	// Stop the server
	defer func() {
		require.NoError(t, testSetup.stopServer())
		log.Debug("server is now down")
	}()

	// Write test data
	now := testSetup.getNowFn()
	seriesMaps := make(map[time.Time]generate.SeriesBlock)
	inputData := []generate.BlockConfig{
		{[]string{"foo", "bar"}, 100, now},
		{[]string{"foo", "baz"}, 50, now.Add(blockSize)},
	}
	for _, input := range inputData {
		start := input.Start
		testSetup.setNowFn(start)
		testData := generate.Block(input)
		seriesMaps[start] = testData
		require.NoError(t, testSetup.writeBatch(testNamespaces[0], testData))
	}
	log.Debug("test data is now written")

	// Advance time and sleep for a long enough time so data blocks are sealed during ticking
	testSetup.setNowFn(testSetup.getNowFn().Add(blockSize * 2))
	later := testSetup.getNowFn()
	time.Sleep(tickInterval * 4)

	metadatasByShard := testSetupMetadatas(t, testSetup, testNamespaces[0], now, later)
	observedSeriesMaps := testSetupToSeriesMaps(t, testSetup, testNamespaces[0], metadatasByShard)

	// Verify retrieved data matches what we've written
	verifySeriesMapsEqual(t, seriesMaps, observedSeriesMaps)
}

func testSetupMetadatas(
	t *testing.T,
	testSetup *testSetup,
	namespace ts.ID,
	start time.Time,
	end time.Time,
) map[uint32][]block.ReplicaMetadata {
	// Retrieve written data using the AdminSession APIs
	// FetchMetadataBlocksFromPeers/FetchBlocksFromPeers
	adminClient := testSetup.m3dbAdminClient
	metadatasByShard, err := m3dbClientFetchBlocksMetadata(
		adminClient, namespace, testSetup.shardSet.AllIDs(), start, end)
	require.NoError(t, err)
	require.NotEmpty(t, metadatasByShard)
	return metadatasByShard
}

func verifySeriesMapsEqual(
	t *testing.T,
	expectedSeriesMap map[time.Time]generate.SeriesBlock,
	observedSeriesMap map[time.Time]generate.SeriesBlock,
) {
	// ensure same length
	require.Equal(t, len(expectedSeriesMap), len(observedSeriesMap))

	// ensure same set of keys
	for i := range expectedSeriesMap {
		_, ok := observedSeriesMap[i]
		require.True(t, ok, "%v is expected but not observed", i.String())
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
					"data length mismatch for series - [time: %v, seriesID: %v]", i.String(), es.ID.String())
				for idx := range es.Data {
					expectedData := es.Data[idx]
					observedData := os.Data[idx]
					require.Equal(t, expectedData.Timestamp, observedData.Timestamp,
						"data mismatch for series - [time: %v, seriesID: %v, idx: %v]",
						i.String(), es.ID.String(), idx)
					require.Equal(t, expectedData.Value, observedData.Value,
						"data mismatch for series - [time: %v, seriesID: %v, idx: %v]",
						i.String(), es.ID.String(), idx)
				}
			}

			require.True(t, found, "unable to find expected series - [time: %v, seriesID: %v]",
				i.String(), es.ID.String())
		}
	}
}

func testSetupToSeriesMaps(
	t *testing.T,
	testSetup *testSetup,
	namespace ts.ID,
	metadatasByShard map[uint32][]block.ReplicaMetadata,
) map[time.Time]generate.SeriesBlock {
	seriesMap := make(map[time.Time]generate.SeriesBlock)
	resultOpts := newDefaulTestResultOptions(testSetup.storageOpts)
	iterPool := testSetup.storageOpts.ReaderIteratorPool()
	session, err := testSetup.m3dbAdminClient.DefaultAdminSession()
	require.NoError(t, err)
	require.NotNil(t, session)

	for shardID, metadatas := range metadatasByShard {
		blocksIter, err := session.FetchBlocksFromPeers(namespace, shardID, metadatas, resultOpts)
		require.NoError(t, err)
		require.NotNil(t, blocksIter)

		for blocksIter.Next() {
			_, id, blk := blocksIter.Current()
			ctx := context.NewContext()
			defer ctx.Close()
			reader, err := blk.Stream(ctx)
			require.NoError(t, err)
			readerIter := iterPool.Get()
			readerIter.Reset(reader)

			var datapoints []ts.Datapoint
			for readerIter.Next() {
				datapoint, _, _ := readerIter.Current()
				datapoints = append(datapoints, datapoint)
			}
			require.NoError(t, readerIter.Err())
			require.NotEmpty(t, datapoints)

			firstTs := datapoints[0].Timestamp
			seriesMapList, _ := seriesMap[firstTs]
			seriesMapList = append(seriesMapList, generate.Series{
				ID:   id,
				Data: datapoints,
			})
			seriesMap[firstTs] = seriesMapList
		}
		require.NoError(t, blocksIter.Err())
	}
	return seriesMap
}
