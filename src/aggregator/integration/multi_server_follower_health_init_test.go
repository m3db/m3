//go:build integration
// +build integration

// Copyright (c) 2022 Uber Technologies, Inc.
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
	"sync/atomic"
	"testing"
	"time"

	aggclient "github.com/m3db/m3/src/aggregator/client"
	httpserver "github.com/m3db/m3/src/aggregator/server/http"
	"github.com/m3db/m3/src/cluster/kv"
	memcluster "github.com/m3db/m3/src/cluster/mem"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/x/instrument"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

//nolint:dupl
func TestMultiServerFollowerHealthInit(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	aggregatorClientType, err := getAggregatorClientTypeFromEnv()
	require.NoError(t, err)

	// Clock setup.
	clock := newTestClock(time.Now().Truncate(time.Hour))

	// Placement setup.
	var (
		numTotalShards = 1024
		placementKey   = "/placement"
	)
	multiServerSetup := []struct {
		rawTCPAddr     string
		httpAddr       string
		m3MsgAddr      string
		instanceConfig placementInstanceConfig
	}{
		{
			rawTCPAddr: "localhost:6000",
			httpAddr:   "localhost:16000",
			m3MsgAddr:  "localhost:26000",
			instanceConfig: placementInstanceConfig{
				instanceID:          "localhost:6000",
				shardSetID:          1,
				shardStartInclusive: 0,
				shardEndExclusive:   512,
			},
		},
		{
			rawTCPAddr: "localhost:6001",
			httpAddr:   "localhost:16001",
			m3MsgAddr:  "localhost:26001",
			instanceConfig: placementInstanceConfig{
				instanceID:          "localhost:6001",
				shardSetID:          1,
				shardStartInclusive: 0,
				shardEndExclusive:   512,
			},
		},
		{
			rawTCPAddr: "localhost:6002",
			httpAddr:   "localhost:16002",
			m3MsgAddr:  "localhost:26002",
			instanceConfig: placementInstanceConfig{
				instanceID:          "localhost:6002",
				shardSetID:          2,
				shardStartInclusive: 512,
				shardEndExclusive:   1024,
			},
		},
		{
			rawTCPAddr: "localhost:6003",
			httpAddr:   "localhost:16003",
			m3MsgAddr:  "localhost:26003",
			instanceConfig: placementInstanceConfig{
				instanceID:          "localhost:6003",
				shardSetID:          2,
				shardStartInclusive: 512,
				shardEndExclusive:   1024,
			},
		},
	}

	for i, mss := range multiServerSetup {
		multiServerSetup[i].instanceConfig.instanceID = mss.rawTCPAddr
		if aggregatorClientType == aggclient.M3MsgAggregatorClient {
			multiServerSetup[i].instanceConfig.instanceID = mss.m3MsgAddr
		}
	}

	clusterClient := memcluster.New(kv.NewOverrideOptions())
	instances := make([]placement.Instance, 0, len(multiServerSetup))
	for _, mss := range multiServerSetup {
		instance := mss.instanceConfig.newPlacementInstance()
		instances = append(instances, instance)
	}
	initPlacement := newPlacement(numTotalShards, instances).SetReplicaFactor(2)
	setPlacement(t, placementKey, clusterClient, initPlacement)
	topicService, err := initializeTopic(defaultTopicName, clusterClient, numTotalShards)
	require.NoError(t, err)

	// Election cluster setup.
	electionCluster := newTestCluster(t)

	// Sharding function maps all metrics to shard 0 except for the rollup metric,
	// which gets mapped to the last shard.
	pipelineRollupID := "pipelineRollup"
	shardFn := func(id []byte, numShards uint32) uint32 {
		if pipelineRollupID == string(id) {
			return numShards - 1
		}
		return 0
	}

	// Admin client connection options setup.
	connectionOpts := aggclient.NewConnectionOptions().
		SetInitReconnectThreshold(1).
		SetMaxReconnectThreshold(1).
		SetMaxReconnectDuration(2 * time.Second).
		SetWriteTimeout(time.Second)

	// Create servers.
	servers := make(testServerSetups, 0, len(multiServerSetup))
	for _, mss := range multiServerSetup {
		instrumentOpts := instrument.NewOptions()
		logger := instrumentOpts.Logger().With(
			zap.String("serverAddr", mss.rawTCPAddr),
		)
		instrumentOpts = instrumentOpts.SetLogger(logger)
		serverOpts := newTestServerOptions(t).
			SetBufferForPastTimedMetric(time.Minute).
			SetClockOptions(clock.Options()).
			SetInstrumentOptions(instrumentOpts).
			SetElectionCluster(electionCluster).
			SetHTTPAddr(mss.httpAddr).
			SetRawTCPAddr(mss.rawTCPAddr).
			SetM3MsgAddr(mss.m3MsgAddr).
			SetInstanceID(mss.instanceConfig.instanceID).
			SetClusterClient(clusterClient).
			SetTopicService(topicService).
			SetTopicName(defaultTopicName).
			SetShardFn(shardFn).
			SetShardSetID(mss.instanceConfig.shardSetID).
			SetClientConnectionOptions(connectionOpts).
			SetDiscardNaNAggregatedValues(false)
		server := newTestServerSetup(t, serverOpts)
		servers = append(servers, server)
	}

	// Start the servers.
	log := xtest.NewLogger(t)
	log.Info("test forwarding pipeline")
	for i, server := range servers {
		require.NoError(t, server.startServer())
		log.Sugar().Infof("server %d is now up", i)
	}

	// Stop the servers.
	for i, server := range servers {
		i := i
		server := server
		defer func() {
			require.NoError(t, server.stopServer())
			log.Sugar().Infof("server %d is now down", i)
		}()
	}

	// Waiting for two leaders to come up.
	var (
		leaders    = make(map[int]struct{})
		leaderCh   = make(chan int, len(servers)/2)
		numLeaders int32
		wg         sync.WaitGroup
	)
	wg.Add(len(servers) / 2)
	for i, server := range servers {
		i, server := i, server
		go func() {
			if err := server.waitUntilLeader(); err == nil {
				res := int(atomic.AddInt32(&numLeaders, 1))
				if res <= len(servers)/2 {
					leaderCh <- i
					wg.Done()
				}
			}
		}()
	}
	wg.Wait()
	close(leaderCh)

	for i := range leaderCh {
		leaders[i] = struct{}{}
		log.Sugar().Infof("server %d has become the leader", i)
	}
	log.Sugar().Infof("%d servers have become leaders", len(leaders))

	for _, server := range servers {
		var resp httpserver.StatusResponse
		require.NoError(t, server.getStatusResponse(httpserver.StatusPath, &resp))

		// No data has been written to the aggregators yet, but all servers (including the
		// followers) should be able to lead.
		assert.True(t, resp.Status.FlushStatus.CanLead)
	}
}
