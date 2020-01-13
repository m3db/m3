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
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/placement"
	maggregation "github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/x/clock"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	m3msgServerAddress = "localhost:6099"
)

func TestOneClientPassThroughMetrics(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	serverOpts := newTestServerOptions().SetM3MsgAddr(m3msgServerAddress)
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
	log.Info("test one client sending passthrough metrics")
	require.NoError(t, testServer.startServer())
	log.Info("server is now up")
	require.NoError(t, testServer.waitUntilLeader())
	log.Info("server is now the leader")

	var (
		idPrefix = "full.id"
		numIDs   = 100
		start    = getNowFn()
		stop     = start.Add(10 * time.Second)
		interval = 2 * time.Second
	)
	client := testServer.newClient()
	require.NoError(t, client.connect())
	defer client.close()

	pClient, err := testServer.newPassthroughClient(ctrl)
	require.NoError(t, err)
	defer pClient.Close()

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
		setNowFn(data.timestamp)
		for _, mm := range data.metricWithMetadatas {
			require.NoError(t, pClient.writeMetricWithMetadata(mm.metric.timed, mm.metadata.timedMetadata))
		}
	}
	require.NoError(t, pClient.Close()) // ensure we flush all buffers

	// Move time forward and wait for flushing to happen.
	finalTime := stop.Add(time.Minute + 2*time.Second)
	setNowFn(finalTime)
	time.Sleep(2 * time.Second)

	// Stop the server.
	require.NoError(t, testServer.stopServer())
	log.Info("server is now down")

	// Validate results.
	actual := testServer.sortedResults()
	assertTestDatasetContainedInPassthroughResults(t, dataset, actual)
}

func assertTestDatasetContainedInPassthroughResults(
	t *testing.T,
	expected testDataset,
	observed []aggregated.MetricWithStoragePolicy,
) {
	// validate assumption expected data is all timed metrics,
	// which is a constraint on passthrough metrics.
	for _, exp := range expected {
		for _, md := range exp.metricWithMetadatas {
			assert.Equal(t, timedMetric, md.metric.category)
			assert.Equal(t, timedMetadataType, md.metadata.mType, md)
		}
	}

	// dedupe the observed data, as m3msg has at least once semantics.
	observedMap := make(map[string]aggregated.MetricWithStoragePolicy)
	for _, obs := range observed {
		observedMap[obs.String()] = obs
	}

	// compare expected v observed.
	for _, exp := range expected {
		for _, md := range exp.metricWithMetadatas {
			var found bool
			for _, obs := range observedMap {
				found = found || (md.metric.ID().String() == obs.Metric.ID.String() &&
					md.metric.timed.TimeNanos == obs.TimeNanos &&
					md.metric.timed.Value == obs.Value &&
					md.metadata.timedMetadata.StoragePolicy.String() == obs.StoragePolicy.String())
				if found {
					break
				}
			}
			assert.True(t, found, "unable to find: %+v in observed data", md)
		}
	}
}
