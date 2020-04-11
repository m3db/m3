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
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/cluster/placement"
	maggregation "github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/x/clock"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

func TestFlushJitterByShard(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// Create server opts with jitter enabled at 50%.
	jitterFactor := 0.8
	serverOpts := newTestServerOptions().
		SetJitterEnabled(true).
		SetFlushJitterType(aggregator.FlushJitterByShard).
		SetMaxJitterFn(
			aggregator.FlushJitterFn(func(interval time.Duration) time.Duration {
				return time.Duration(float64(interval) * jitterFactor)
			})).
		SetBufferForPastTimedMetricFn(
			aggregator.BufferForPastTimedMetricFn(func(resolution time.Duration) time.Duration {
				// Allow an extra second for data to arrive.
				return resolution + time.Second
			}))

	// Clock setup.
	var (
		lock  sync.RWMutex
		nowFn func() time.Time
	)
	now := time.Now()
	getNowFn := func() time.Time {
		lock.RLock()
		t := now
		if nowFn != nil {
			t = nowFn()
		}
		lock.RUnlock()
		return t
	}
	setNowFn := func(fn func() time.Time) {
		lock.Lock()
		nowFn = fn
		lock.Unlock()
	}
	setNow := func(t time.Time) {
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
	log.Info("test one client sending multiple types of timed metrics")
	require.NoError(t, testServer.startServer())
	log.Info("server is now up")
	require.NoError(t, testServer.waitUntilLeader())
	log.Info("server is now the leader")

	var (
		idPrefix   = "full.id"
		numIDs     = 100
		resolution = 10 * time.Second
		precision  = xtime.Second
		start      = getNowFn().Truncate(resolution).Add(resolution)
		stop       = start.Add(resolution)
	)
	client := testServer.newClient()
	require.NoError(t, client.connect())
	defer client.close()

	ids := generateTestIDs(idPrefix, numIDs)
	testTimedMetadataTemplate := metadata.TimedMetadata{
		AggregationID: maggregation.MustCompressTypes(maggregation.Sum),
		StoragePolicy: policy.NewStoragePolicy(resolution, precision, time.Hour),
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
		interval:     resolution,
		ids:          ids,
		category:     timedMetric,
		typeFn:       roundRobinMetricTypeFn,
		valueGenOpts: defaultValueGenOpts,
		metadataFn:   metadataFn,
	})
	for _, data := range dataset {
		setNow(data.timestamp)
		for _, mm := range data.metricWithMetadatas {
			require.NoError(t, client.writeTimedMetricWithMetadata(mm.metric.timed, mm.metadata.timedMetadata))
		}
		require.NoError(t, client.flush())
	}

	var (
		sampleExpectedAt = start.Add(time.Hour)
		expected         = mustComputeExpectedResults(t, sampleExpectedAt,
			dataset, testServer.aggregatorOpts)
		actual      []aggregated.MetricWithStoragePolicy
		verifyStart = time.Now()
		verifyFor   = 60 * time.Second
		verifyPause = 100 * time.Millisecond
	)
	// Make sure we're expecting results.
	require.True(t, len(expected) == numIDs)

	// Resume time and wait for flushing to happen.
	setNowFn(time.Now)

	for time.Since(verifyStart) <= verifyFor {
		// Validate results.
		actual = testServer.snapshotSortedResults()
		if reflect.DeepEqual(expected, actual) {
			break
		}
		time.Sleep(verifyPause)
	}
	require.Equal(t, expected, actual,
		xtest.Diff(xtest.MustPrettyJSONValue(t, newAggregatedMetrics(expected)),
			xtest.MustPrettyJSONValue(t, newAggregatedMetrics(actual))))

	var minRecv, maxRecv time.Time
	for _, m := range testServer.snapshotReceived() {
		if minRecv.IsZero() || m.receivedAt.Before(minRecv) {
			minRecv = m.receivedAt
		}
		if maxRecv.IsZero() || m.receivedAt.After(maxRecv) {
			maxRecv = m.receivedAt
		}
	}

	skew := maxRecv.Sub(minRecv)

	// Require at least 90% skew between received values, we have
	// 1024 shards so it should be very likely that with a good
	// random function that we get very close to our desired skew
	// across shards.
	minRequiredSkew := time.Duration(float64(resolution) * (0.9 * jitterFactor))
	require.True(t, skew >= minRequiredSkew,
		fmt.Sprintf("wanted min skew of at least %s, instead skew is %s\n",
			minRequiredSkew, skew))

	// Stop the server.
	require.NoError(t, testServer.stopServer())
	log.Info("server is now down")
}
