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

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/metrics/metric"

	"github.com/stretchr/testify/require"
)

func TestSameIDMultiTypeWithStagedMetadatas(t *testing.T) {
	metadataFn := func(int) metadataUnion {
		return metadataUnion{
			mType:           stagedMetadatasType,
			stagedMetadatas: testStagedMetadatas,
		}
	}
	testSameIDMultiType(t, metadataFn)
}

func testSameIDMultiType(t *testing.T, metadataFn metadataFn) {
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
	log.Info("test sending same metric ID with different metric types")
	require.NoError(t, testServer.startServer())
	log.Info("server is now up")
	require.NoError(t, testServer.waitUntilLeader())
	log.Info("server is now the leader")

	var (
		idPrefix = "foo"
		numIDs   = 100
		start    = clock.Now()
		mid      = start.Add(4 * time.Second)
		stop     = start.Add(10 * time.Second)
		interval = time.Second
	)
	client := testServer.newClient(t)
	require.NoError(t, client.connect())

	ids := generateTestIDs(idPrefix, numIDs)
	metricTypeFn := func(ts time.Time, idx int) metric.Type {
		if ts.Before(mid) {
			switch idx % 3 {
			case 0:
				return metric.CounterType
			case 1:
				return metric.TimerType
			default:
				return metric.GaugeType
			}
		} else {
			switch idx % 3 {
			case 0:
				return metric.TimerType
			case 1:
				return metric.GaugeType
			default:
				return metric.CounterType
			}
		}
	}
	dataset := mustGenerateTestDataset(t, datasetGenOpts{
		start:        start,
		stop:         stop,
		interval:     interval,
		ids:          ids,
		category:     untimedMetric,
		typeFn:       metricTypeFn,
		valueGenOpts: defaultValueGenOpts,
		metadataFn:   metadataFn,
	})
	for _, data := range dataset {
		clock.SetNow(data.timestamp)
		for _, mm := range data.metricWithMetadatas {
			require.NoError(t, client.writeUntimedMetricWithMetadatas(mm.metric.untimed, mm.metadata.stagedMetadatas))
		}
		require.NoError(t, client.flush())

		// Give server some time to process the incoming packets.
		time.Sleep(100 * time.Millisecond)
	}

	// Move time forward and wait for ticking to happen. The sleep time
	// must be the longer than the lowest resolution across all policies.
	finalTime := stop.Add(6 * time.Second)
	clock.SetNow(finalTime)
	time.Sleep(4 * time.Second)

	require.NoError(t, client.close())

	// Stop the server.
	require.NoError(t, testServer.stopServer())
	log.Info("server is now down")

	// Validate results.
	expected := mustComputeExpectedResults(t, finalTime, dataset, testServer.aggregatorOpts)
	actual := testServer.sortedResults()
	require.Equal(t, expected, actual)
}
