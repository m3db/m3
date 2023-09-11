//go:build integration

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

	"github.com/m3db/m3/src/cluster/placement"
	maggregation "github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/policy"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

func TestOneClientMultiTypeTimedMetrics(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	serverOpts := newTestServerOptions(t)

	// Clock setup.
	clock := newTestClock(time.Now().Truncate(time.Hour))
	serverOpts = serverOpts.SetClockOptions(clock.Options())

	// Placement setup.
	numShards := 1024
	cfg := placementInstanceConfig{
		instanceID:          serverOpts.InstanceID(),
		shardSetID:          serverOpts.ShardSetID(),
		shardStartInclusive: 0,
		shardEndExclusive:   uint32(numShards),
	}
	instance := cfg.newPlacementInstance()
	placement := newPlacement(numShards, []placement.Instance{instance})
	placementKey := serverOpts.PlacementKVKey()
	setPlacement(t, placementKey, serverOpts.ClusterClient(), placement)

	serverOpts = setupTopic(t, serverOpts, placement)

	// Create server.
	testServer := newTestServerSetup(t, serverOpts)
	defer testServer.close()

	// Start the server.
	log := testServer.aggregatorOpts.InstrumentOptions().Logger()
	log.Info("test one client sending multiple types of timed metrics")
	require.NoError(t, testServer.startServer())
	log.Info("server is now up")
	require.NoError(t, testServer.waitUntilLeader())
	log.Info("server is now the leader")

	var (
		idPrefix = "full.id"
		numIDs   = 100
		start    = clock.Now()
		stop     = start.Add(10 * time.Second)
		interval = 2 * time.Second
	)
	client := testServer.newClient(t)
	require.NoError(t, client.connect())

	ids := generateTestIDs(idPrefix, numIDs)
	testTimedMetadataTemplate := metadata.TimedMetadata{
		AggregationID: maggregation.MustCompressTypes(maggregation.Sum),
		StoragePolicy: policy.NewStoragePolicy(2*time.Second, xtime.Second, time.Hour),
	}
	metadataFn := func(idx int) metadataUnion {
		timedMetadata := testTimedMetadataTemplate
		return metadataUnion{
			mType:         timedMetadataType,
			timedMetadata: timedMetadata,
		}
	}
	dataset := mustGenerateTestDataset(t, datasetGenOpts{
		start:        start,
		stop:         stop,
		interval:     interval,
		ids:          ids,
		category:     timedMetric,
		typeFn:       roundRobinMetricTypeFn,
		valueGenOpts: defaultValueGenOpts,
		metadataFn:   metadataFn,
	})
	for _, data := range dataset {
		clock.SetNow(data.timestamp)
		for _, mm := range data.metricWithMetadatas {
			require.NoError(t, client.writeTimedMetricWithMetadata(mm.metric.timed, mm.metadata.timedMetadata))
		}
		require.NoError(t, client.flush())

		// Give server some time to process the incoming packets.
		time.Sleep(100 * time.Millisecond)
	}

	// Move time forward and wait for flushing to happen.
	finalTime := stop.Add(time.Minute + 2*time.Second)
	clock.SetNow(finalTime)
	time.Sleep(waitForDataToFlush)

	require.NoError(t, client.close())

	// Stop the server.
	require.NoError(t, testServer.stopServer())
	log.Info("server is now down")

	// Validate results.
	expected := mustComputeExpectedResults(t, finalTime, dataset, testServer.aggregatorOpts)
	actual := testServer.sortedResults()
	require.Equal(t, expected, actual)
}
