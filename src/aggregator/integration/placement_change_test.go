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
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	aggclient "github.com/m3db/m3/src/aggregator/client"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/cluster/placement"
	maggregation "github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/x/instrument"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"
)

func TestPlacementChange(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	aggregatorClientType, err := getAggregatorClientTypeFromEnv()
	require.NoError(t, err)
	if aggregatorClientType == aggclient.M3MsgAggregatorClient {
		// TODO(vilius) update this test to work with m3msg client
		t.SkipNow()
	}

	// Clock setup.
	clock := newTestClock(time.Now().Truncate(time.Hour))

	// Placement setup.
	var (
		numTotalShards = 2
		placementKey   = "/placement"
		kvStore        = mem.NewStore()
	)
	multiServerSetup := []struct {
		rawTCPAddr string
		httpAddr   string
		m3MsgAddr  string
	}{
		{
			rawTCPAddr: "localhost:6000",
			httpAddr:   "localhost:16000",
			m3MsgAddr:  "localhost:26000",
		},
		{
			rawTCPAddr: "localhost:6001",
			httpAddr:   "localhost:16001",
			m3MsgAddr:  "localhost:26001",
		},
	}
	initialInstanceConfig := []placementInstanceConfig{
		{
			shardSetID:          1,
			shardStartInclusive: 0,
			shardEndExclusive:   uint32(numTotalShards),
		},
		{
			shardSetID:          2,
			shardStartInclusive: 0,
			shardEndExclusive:   0,
		},
	}
	finalInstanceConfig := []placementInstanceConfig{
		{
			shardSetID:          1,
			shardStartInclusive: 0,
			shardEndExclusive:   uint32(numTotalShards / 2),
		},
		{
			shardSetID:          2,
			shardStartInclusive: uint32(numTotalShards / 2),
			shardEndExclusive:   uint32(numTotalShards),
		},
	}

	for i, mss := range multiServerSetup {
		initialInstanceConfig[i].instanceID = mss.rawTCPAddr
		finalInstanceConfig[i].instanceID = mss.rawTCPAddr
		if aggregatorClientType == aggclient.M3MsgAggregatorClient {
			initialInstanceConfig[i].instanceID = mss.m3MsgAddr
			finalInstanceConfig[i].instanceID = mss.m3MsgAddr
		}
	}

	initialPlacement := makePlacement(initialInstanceConfig, numTotalShards)
	finalPlacement := makePlacement(finalInstanceConfig, numTotalShards)
	require.NoError(t, setPlacement(placementKey, kvStore, initialPlacement))

	shardFn := newTestServerOptions(t).ShardFn()

	getServerIndex := func(metricId id.RawID, placement placement.Placement) int {
		instance := placement.InstancesForShard(shardFn(metricId, uint32(numTotalShards)))[0]
		for i, config := range initialInstanceConfig {
			if config.instanceID == instance.ID() {
				return i
			}
		}
		require.Fail(t, "could not find instance for")
		return -1
	}

	// Election cluster setup.
	electionCluster := newTestCluster(t)

	// Admin client connection options setup.
	connectionOpts := aggclient.NewConnectionOptions().
		SetInitReconnectThreshold(1).
		SetMaxReconnectThreshold(1).
		SetMaxReconnectDuration(2 * time.Second).
		SetWriteTimeout(time.Second)

	// Create servers.
	servers := make([]*testServerSetup, 0, len(multiServerSetup))
	for i, mss := range multiServerSetup {
		instrumentOpts := instrument.NewOptions()
		logger := instrumentOpts.Logger().With(
			zap.String("serverAddr", mss.rawTCPAddr),
		)
		instrumentOpts = instrumentOpts.SetLogger(logger)
		serverOpts := newTestServerOptions(t).
			SetClockOptions(clock.Options()).
			SetInstrumentOptions(instrumentOpts).
			SetElectionCluster(electionCluster).
			SetRawTCPAddr(mss.rawTCPAddr).
			SetHTTPAddr(mss.httpAddr).
			SetM3MsgAddr(mss.m3MsgAddr).
			SetInstanceID(initialInstanceConfig[i].instanceID).
			SetKVStore(kvStore).
			SetShardSetID(initialInstanceConfig[i].shardSetID).
			SetShardFn(shardFn).
			SetClientConnectionOptions(connectionOpts).
			SetPlacement(initialPlacement)
		server := newTestServerSetup(t, serverOpts)
		servers = append(servers, server)
	}

	// Start the servers.
	log := xtest.NewLogger(t)
	for i, server := range servers {
		require.NoError(t, server.startServer())
		log.Sugar().Infof("server %d is now up", i)
	}

	// Create clients for writing to the servers.
	clients := make([]*client, 0, len(servers))
	for _, server := range servers {
		client := server.newClient(t)
		require.NoError(t, client.connect())
		clients = append(clients, client)
	}

	for _, server := range servers {
		require.NoError(t, server.waitUntilLeader())
	}

	var (
		idPrefix = "metric.id"
		numIDs   = 100

		start1    = clock.Now()
		stop1     = start1.Add(10 * time.Second)
		start2    = stop1.Add(time.Minute + 2*time.Second)
		stop2     = start2.Add(10 * time.Second)
		finalTime = stop2.Add(time.Minute + 2*time.Second)
		interval  = 2 * time.Second

		sleepDuration = time.Second
	)
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
	datasets := []testDataset{
		mustGenerateTestDataset(t, datasetGenOpts{
			start:        start1,
			stop:         stop1,
			interval:     interval,
			ids:          ids,
			category:     timedMetric,
			typeFn:       roundRobinMetricTypeFn,
			valueGenOpts: defaultValueGenOpts,
			metadataFn:   metadataFn,
		}),
		mustGenerateTestDataset(t, datasetGenOpts{
			start:        start2,
			stop:         stop2,
			interval:     interval,
			ids:          ids,
			category:     timedMetric,
			typeFn:       roundRobinMetricTypeFn,
			valueGenOpts: defaultValueGenOpts,
			metadataFn:   metadataFn,
		}),
	}

	for _, data := range datasets[0] {
		clock.SetNow(data.timestamp)

		for _, mm := range data.metricWithMetadatas {
			idx := getServerIndex(mm.metric.ID(), initialPlacement)
			require.NoError(t, clients[idx].writeTimedMetricWithMetadata(mm.metric.timed, mm.metadata.timedMetadata))
		}
		for _, c := range clients {
			require.NoError(t, c.flush())
		}

		// Give server some time to process the incoming packets.
		time.Sleep(sleepDuration)
	}

	clock.SetNow(start2)
	time.Sleep(sleepDuration)
	require.NoError(t, setPlacement(placementKey, kvStore, finalPlacement))
	time.Sleep(sleepDuration)

	for _, data := range datasets[1] {
		clock.SetNow(data.timestamp)

		for _, mm := range data.metricWithMetadatas {
			idx := getServerIndex(mm.metric.ID(), finalPlacement)
			require.NoError(t, clients[idx].writeTimedMetricWithMetadata(mm.metric.timed, mm.metadata.timedMetadata))
		}
		for _, c := range clients {
			require.NoError(t, c.flush())
		}

		// Give server some time to process the incoming packets.
		time.Sleep(sleepDuration)
	}

	// Move time forward and wait for flushing to happen.
	clock.SetNow(finalTime)
	time.Sleep(sleepDuration)

	// Stop the clients.
	for _, client := range clients {
		require.NoError(t, client.close())
	}

	// Stop the servers.
	for i, server := range servers {
		require.NoError(t, server.stopServer())
		log.Sugar().Infof("server %d is now down", i)
	}

	actual := make([]aggregated.MetricWithStoragePolicy, 0)
	for _, server := range servers {
		actual = append(actual, server.sortedResults()...)
	}
	sort.Sort(byTimeIDPolicyAscending(actual))
	expected := make([]aggregated.MetricWithStoragePolicy, 0)
	for _, dataset := range datasets {
		results := mustComputeExpectedResults(t, finalTime, dataset, servers[0].aggregatorOpts)
		expected = append(expected, results...)
	}
	sort.Sort(byTimeIDPolicyAscending(expected))
	require.Equal(t, expected, actual)
}

func makePlacement(instanceConfig []placementInstanceConfig, numShards int) placement.Placement {
	instances := make([]placement.Instance, 0, len(instanceConfig))
	for _, config := range instanceConfig {
		instance := config.newPlacementInstance()
		instances = append(instances, instance)
	}
	return newPlacement(numShards, instances)
}
