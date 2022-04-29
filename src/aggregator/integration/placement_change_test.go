//go:build integration
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

	aggclient "github.com/m3db/m3/src/aggregator/client"
	"github.com/m3db/m3/src/cluster/kv"
	memcluster "github.com/m3db/m3/src/cluster/mem"
	"github.com/m3db/m3/src/cluster/placement"
	maggregation "github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/x/instrument"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestPlacementChange(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// Clock setup.
	clock := newTestClock(time.Now().Truncate(time.Hour))

	// Placement setup.
	var (
		numTotalShards = 4
		placementKey   = "/placement"
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
			shardEndExclusive:   uint32(numTotalShards) - 1,
		},
		{
			shardSetID:          2,
			shardStartInclusive: uint32(numTotalShards) - 1,
			shardEndExclusive:   uint32(numTotalShards),
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

	aggregatorClientType, err := getAggregatorClientTypeFromEnv()
	require.NoError(t, err)
	for i, mss := range multiServerSetup {
		initialInstanceConfig[i].instanceID = mss.rawTCPAddr
		finalInstanceConfig[i].instanceID = mss.rawTCPAddr
		if aggregatorClientType == aggclient.M3MsgAggregatorClient {
			initialInstanceConfig[i].instanceID = mss.m3MsgAddr
			finalInstanceConfig[i].instanceID = mss.m3MsgAddr
		}
	}

	clusterClient := memcluster.New(kv.NewOverrideOptions())
	initialPlacement := makePlacement(initialInstanceConfig, numTotalShards)
	finalPlacement := makePlacement(finalInstanceConfig, numTotalShards)
	setPlacement(t, placementKey, clusterClient, initialPlacement)
	topicService, err := initializeTopic(defaultTopicName, clusterClient, numTotalShards)
	require.NoError(t, err)

	// Election cluster setup.
	electionCluster := newTestCluster(t)

	// Admin client connection options setup.
	connectionOpts := aggclient.NewConnectionOptions().
		SetInitReconnectThreshold(1).
		SetMaxReconnectThreshold(1).
		SetMaxReconnectDuration(2 * time.Second).
		SetWriteTimeout(time.Second)

	// Create servers.
	servers := make(testServerSetups, 0, len(multiServerSetup))
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
			SetClusterClient(clusterClient).
			SetTopicService(topicService).
			SetShardSetID(initialInstanceConfig[i].shardSetID).
			SetClientConnectionOptions(connectionOpts)
		server := newTestServerSetup(t, serverOpts)
		servers = append(servers, server)
	}

	// Start the servers.
	log := xtest.NewLogger(t)
	for i, server := range servers {
		require.NoError(t, server.startServer())
		log.Sugar().Infof("server %d is now up", i)
	}

	// Create client for writing to the servers.
	client := servers.newClient(t)
	require.NoError(t, client.connect())

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
			require.NoError(t, client.writeTimedMetricWithMetadata(mm.metric.timed, mm.metadata.timedMetadata))
		}
		require.NoError(t, client.flush())

		// Give server some time to process the incoming packets.
		time.Sleep(sleepDuration)
	}

	clock.SetNow(start2)
	time.Sleep(6 * time.Second)
	setPlacement(t, placementKey, clusterClient, finalPlacement)
	time.Sleep(6 * time.Second)

	for _, data := range datasets[1] {
		clock.SetNow(data.timestamp)

		for _, mm := range data.metricWithMetadatas {
			require.NoError(t, client.writeTimedMetricWithMetadata(mm.metric.timed, mm.metadata.timedMetadata))
		}
		require.NoError(t, client.flush())

		// Give server some time to process the incoming packets.
		time.Sleep(sleepDuration)
	}

	// Move time forward and wait for flushing to happen.
	clock.SetNow(finalTime)
	time.Sleep(6 * time.Second)

	// Remove all the topic consumers before closing clients and servers. This allows to close the
	// connections between servers while they still are running. Otherwise, during server shutdown,
	// the yet-to-be-closed servers would repeatedly try to reconnect to recently closed ones, which
	// results in longer shutdown times.
	require.NoError(t, removeAllTopicConsumers(topicService, defaultTopicName))

	require.NoError(t, client.close())

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
	require.True(t, cmp.Equal(expected, actual, testCmpOpts...), cmp.Diff(expected, actual, testCmpOpts...))
}

func makePlacement(instanceConfig []placementInstanceConfig, numShards int) placement.Placement {
	instances := make([]placement.Instance, 0, len(instanceConfig))
	for _, config := range instanceConfig {
		instance := config.newPlacementInstance()
		instances = append(instances, instance)
	}
	return newPlacement(numShards, instances)
}
