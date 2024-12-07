// Copyright (c) 2024 Uber Technologies, Inc.
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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/integration/generate"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

func TestAdminSessionFetchBootstrapBlocksMetadataFromPeer(t *testing.T) {

	if testing.Short() {
		t.SkipNow()
	}

	numOfActiveSeries := 1000
	writeBatchSize := 7
	readBatchSize := 13

	testOpts := NewTestOptions(t).SetUseTChannelClientForWriting(true).
		SetNumShards(1).SetFetchSeriesBlocksBatchSize(readBatchSize)
	testSetup, err := NewTestSetup(t, testOpts, nil)
	require.NoError(t, err)
	defer testSetup.Close()

	// Start the server
	log := testSetup.StorageOpts().InstrumentOptions().Logger()
	require.NoError(t, testSetup.StartServer())

	// Stop the server
	defer func() {
		require.NoError(t, testSetup.StopServer())
		log.Debug("server is now down")
	}()

	start := testSetup.NowFn()()
	testSetup.SetNowFn(start)

	// Write test data
	writeTestData(t, testSetup, testNamespaces[0], start, numOfActiveSeries, writeBatchSize)

	testSetup.SetNowFn(testSetup.NowFn()().Add(10 * time.Minute))
	end := testSetup.NowFn()()

	// Fetch and verify metadata
	observedSeries := newTestSetupBootstrapBlocksMetadata(t, testSetup, testNamespaces[0],
		start, end)
	verifySeriesMetadata(t, numOfActiveSeries, observedSeries)
}

func writeTestData(t *testing.T, testSetup TestSetup, namespace ident.ID, start xtime.UnixNano,
	numOfSeries int, batchSize int) {
	var wg sync.WaitGroup

	for i := 0; i < numOfSeries; i += batchSize {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			size := batchSize
			if numOfSeries-i < batchSize {
				size = numOfSeries - i
			}
			for j := 0; j < size; j++ {
				id := fmt.Sprintf("foo_%d_%d", i, j)
				currInput := generate.BlockConfig{IDs: []string{id}, Start: start, NumPoints: 5}
				testData := generate.Block(currInput)
				require.NoError(t, testSetup.WriteBatch(namespace, testData))
			}
		}(i)
	}
	wg.Wait()
}

func newTestSetupBootstrapBlocksMetadata(t *testing.T,
	testSetup TestSetup,
	namespace ident.ID,
	start xtime.UnixNano,
	end xtime.UnixNano,
) map[string]int {
	adminClient := testSetup.M3DBVerificationAdminClient()
	metadatasByShard, err := m3dbClientFetchBootstrapBlocksMetadata(adminClient,
		namespace, testSetup.ShardSet().AllIDs(), start, end)
	require.NoError(t, err)

	// Setup only has one shard
	return newTestsSetupSeriesMetadataMap(metadatasByShard[0])
}

func newTestsSetupSeriesMetadataMap(metadatas []block.ReplicaMetadata) map[string]int {
	seriesMap := make(map[string]int, 100000)
	for _, block := range metadatas {
		idString := block.ID.String()
		seriesMap[idString]++
	}
	return seriesMap
}

func m3dbClientFetchBootstrapBlocksMetadata(
	c client.AdminClient,
	namespace ident.ID,
	shards []uint32,
	start, end xtime.UnixNano,
) (map[uint32][]block.ReplicaMetadata, error) {
	session, err := c.DefaultAdminSession()
	if err != nil {
		return nil, err
	}
	metadatasByShard := make(map[uint32][]block.ReplicaMetadata, 100000)
	for _, shardID := range shards {

		var metadatas []block.ReplicaMetadata
		iter, err := session.FetchBootstrapBlocksMetadataFromPeers(namespace,
			shardID, start, end, result.NewOptions())
		if err != nil {
			return nil, err
		}

		for iter.Next() {
			host, blockMetadata := iter.Current()
			metadatas = append(metadatas, block.ReplicaMetadata{
				Metadata: blockMetadata,
				Host:     host,
			})
		}
		if err := iter.Err(); err != nil {
			return nil, err
		}

		if metadatas != nil {
			metadatasByShard[shardID] = metadatas
		}
	}
	return metadatasByShard, nil
}

func verifySeriesMetadata(
	t *testing.T,
	expectedTotalSeries int,
	observed map[string]int,
) {
	require.Equal(t, expectedTotalSeries, len(observed))
	for expectedSeries, count := range observed {
		require.Equal(t, 1, count, "expected series %s metadata was expected to be observed once", expectedSeries)
	}
}
