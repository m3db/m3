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
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/x/clock"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

func TestOneClientPassthroughMetrics(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	serverOpts := newTestServerOptions()

	// Clock setup.
	var lock sync.RWMutex
	now := time.Now().Truncate(time.Hour)
	getNowFn := func() time.Time {
		lock.RLock()
		t := now
		lock.RUnlock()
		return t
	}
	setNowFn := func(t time.Time) {
		lock.Lock()
		now = t
		lock.Unlock()
	}
	clockOpts := clock.NewOptions().SetNowFn(getNowFn)
	serverOpts = serverOpts.SetClockOptions(clockOpts)

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
	placementStore := serverOpts.KVStore()
	require.NoError(t, setPlacement(placementKey, placementStore, placement))

	// Create server.
	testServer := newTestServerSetup(t, serverOpts)
	defer testServer.close()

	// Start the server.
	log := testServer.aggregatorOpts.InstrumentOptions().Logger()
	log.Info("test one client sending of passthrough metrics")
	require.NoError(t, testServer.startServer())
	log.Info("server is now up")
	require.NoError(t, testServer.waitUntilLeader())
	log.Info("server is now the leader")

	var (
		idPrefix = "full.passthru.id"
		numIDs   = 10
		start    = getNowFn()
		stop     = start.Add(10 * time.Second)
		interval = 2 * time.Second
	)
	client := testServer.newClient()
	require.NoError(t, client.connect())
	defer client.close()

	ids := generateTestIDs(idPrefix, numIDs)
	metadataFn := func(idx int) metadataUnion {
		return metadataUnion{
			mType:               passthroughMetadataType,
			passthroughMetadata: policy.NewStoragePolicy(2*time.Second, xtime.Second, time.Hour),
		}
	}
	dataset := mustGenerateTestDataset(t, datasetGenOpts{
		start:        start,
		stop:         stop,
		interval:     interval,
		ids:          ids,
		category:     passthroughMetric,
		typeFn:       constantMetricTypeFnFactory(metric.GaugeType),
		valueGenOpts: defaultValueGenOpts,
		metadataFn:   metadataFn,
	})

	for _, data := range dataset {
		setNowFn(data.timestamp)
		for _, mm := range data.metricWithMetadatas {
			require.NoError(t, client.writePassthroughMetricWithMetadata(mm.metric.passthrough, mm.metadata.passthroughMetadata))
		}
		require.NoError(t, client.flush())

		// Give server some time to process the incoming packets.
		time.Sleep(100 * time.Millisecond)
	}

	// Move time forward and wait for flushing to happen.
	finalTime := stop.Add(time.Minute + 2*time.Second)
	setNowFn(finalTime)
	time.Sleep(2 * time.Second)

	// Stop the server.
	require.NoError(t, testServer.stopServer())
	log.Info("server is now down")

	// Validate results.
	expected := computeExpectedPassthroughResults(t, dataset)
	actual := testServer.sortedResults()
	require.Equal(t, dedupResults(expected), dedupResults(actual))
}

func computeExpectedPassthroughResults(
	t *testing.T,
	dataset testDataset,
) []aggregated.MetricWithStoragePolicy {
	var expected []aggregated.MetricWithStoragePolicy
	for _, testData := range dataset {
		for _, metricWithMetadata := range testData.metricWithMetadatas {
			require.Equal(t, passthroughMetric, metricWithMetadata.metric.category)

			expectedPassthrough := aggregated.MetricWithStoragePolicy{
				Metric:        metricWithMetadata.metric.passthrough,
				StoragePolicy: metricWithMetadata.metadata.passthroughMetadata,
			}

			// The capturingWriter writes ChunkedMetricWithStoragePolicy which has no metric type defined.
			expectedPassthrough.Metric.Type = metric.UnknownType
			expected = append(expected, expectedPassthrough)
		}
	}
	// Sort the aggregated metrics.
	sort.Sort(byTimeIDPolicyAscending(expected))
	return expected
}

func dedupResults(
	results []aggregated.MetricWithStoragePolicy,
) []aggregated.MetricWithStoragePolicy {
	var deduped []aggregated.MetricWithStoragePolicy
	lenDeduped := 0
	for _, m := range results {
		if lenDeduped == 0 || !reflect.DeepEqual(deduped[lenDeduped-1], m) {
			deduped = append(deduped, m)
			lenDeduped++
		}
	}
	return deduped
}
