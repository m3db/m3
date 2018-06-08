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
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3aggregator/aggregation"
	aggclient "github.com/m3db/m3aggregator/client"
	"github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3cluster/placement"
	maggregation "github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/metadata"
	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/op"
	"github.com/m3db/m3metrics/op/applied"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/transformation"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/log"
	xtime "github.com/m3db/m3x/time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
)

func TestMultiServerForwardingPipeline(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

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

	// Placement setup.
	var (
		numTotalShards = 1024
		placementKey   = "/placement"
		kvStore        = mem.NewStore()
	)
	multiServerSetup := []struct {
		rawTCPAddr     string
		httpAddr       string
		instanceConfig placementInstanceConfig
	}{
		{
			rawTCPAddr: "localhost:6000",
			httpAddr:   "localhost:16000",
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
			instanceConfig: placementInstanceConfig{
				instanceID:          "localhost:6003",
				shardSetID:          2,
				shardStartInclusive: 512,
				shardEndExclusive:   1024,
			},
		},
	}
	instances := make([]placement.Instance, 0, len(multiServerSetup))
	for _, mss := range multiServerSetup {
		instance := mss.instanceConfig.newPlacementInstance()
		instances = append(instances, instance)
	}
	initPlacement := newPlacement(numTotalShards, instances)
	require.NoError(t, setPlacement(placementKey, kvStore, initPlacement))

	// Election cluster setup.
	electionCluster := newTestCluster(t)

	// Sharding function maps all metrics to shard 0 except for the rollup metric,
	// which gets mapped to the last shard.
	pipelineRollupID := "pipelineRollup"
	shardFn := func(id []byte, numShards int) uint32 {
		if pipelineRollupID == string(id) {
			return uint32(numShards - 1)
		}
		return 0
	}

	// Admin client connection options setup.
	connectionOpts := aggclient.NewConnectionOptions().
		SetInitReconnectThreshold(1).
		SetMaxReconnectThreshold(1).
		SetMaxReconnectDuration(2 * time.Second)

	// Create servers.
	servers := make([]*testServerSetup, 0, len(multiServerSetup))
	for _, mss := range multiServerSetup {
		instrumentOpts := instrument.NewOptions()
		logger := instrumentOpts.Logger().WithFields(
			log.NewField("serverAddr", mss.rawTCPAddr),
		)
		instrumentOpts = instrumentOpts.SetLogger(logger)
		serverOpts := newTestServerOptions().
			SetClockOptions(clockOpts).
			SetInstrumentOptions(instrumentOpts).
			SetElectionCluster(electionCluster).
			SetHTTPAddr(mss.httpAddr).
			SetInstanceID(mss.instanceConfig.instanceID).
			SetKVStore(kvStore).
			SetRawTCPAddr(mss.rawTCPAddr).
			SetShardFn(shardFn).
			SetShardSetID(mss.instanceConfig.shardSetID).
			SetClientConnectionOptions(connectionOpts)
		server := newTestServerSetup(t, serverOpts)
		servers = append(servers, server)
	}

	// Start the servers.
	log := log.NewLevelLogger(log.SimpleLogger, log.LevelInfo)
	log.Info("test forwarding pipeline")
	for i, server := range servers {
		require.NoError(t, server.startServer())
		log.Infof("server %d is now up", i)
	}

	// Create clients for writing to the servers.
	clients := make([]*client, 0, len(servers))
	for _, server := range servers {
		client := server.newClient()
		require.NoError(t, client.connect())
		clients = append(clients, client)
	}

	// Waiting for two leaders to come up.
	var (
		leaders    = make(map[int]struct{})
		leaderCh   = make(chan int, len(servers)/2)
		numLeaders int32
	)
	for i, server := range servers {
		i, server := i, server
		go func() {
			if err := server.waitUntilLeader(); err == nil {
				res := int(atomic.AddInt32(&numLeaders, 1))
				if res <= len(servers)/2 {
					leaderCh <- i
				}
				if res == len(servers)/2 {
					close(leaderCh)
				}
			}
		}()
	}

	for i := range leaderCh {
		leaders[i] = struct{}{}
		log.Infof("server %d has become the leader", i)
	}
	log.Infof("%d servers have become leaders", len(leaders))

	var (
		idPrefix      = "foo"
		numIDs        = 2
		start         = getNowFn()
		stop          = start.Add(10 * time.Second)
		interval      = time.Second
		storagePolicy = policy.NewStoragePolicy(2*time.Second, xtime.Second, time.Hour)
	)

	ids := generateTestIDs(idPrefix, numIDs)
	stagedMetadatas := metadata.StagedMetadatas{
		{
			CutoverNanos: 0,
			Tombstoned:   false,
			Metadata: metadata.Metadata{
				Pipelines: []metadata.PipelineMetadata{
					{
						AggregationID:   maggregation.DefaultID,
						StoragePolicies: []policy.StoragePolicy{storagePolicy},
						Pipeline: applied.NewPipeline([]applied.Union{
							{
								Type:           op.TransformationType,
								Transformation: op.Transformation{Type: transformation.PerSecond},
							},
							{
								Type: op.RollupType,
								Rollup: applied.Rollup{
									ID:            []byte(pipelineRollupID),
									AggregationID: maggregation.MustCompressTypes(maggregation.Sum),
								},
							},
						}),
					},
				},
			},
		},
	}
	metricTypeFn := constantMetricTypeFnFactory(metric.GaugeType)
	valueGenOpts := valueGenOpts{
		untimed: untimedValueGenOpts{
			gaugeValueGenFn: func(intervalIdx, idIdx int) float64 {
				// Each gauge will have two datapoints within the same aggregation window.
				// The first value is 0.0 and should be ignored, and the second value will
				// be used for computing the `PerSecond` value and should result in a `PerSecond`
				// value of 1 that is then forwarded to the next aggregation server.
				if intervalIdx%2 == 0 {
					return 0.0
				}
				return float64(intervalIdx + idIdx)
			},
		},
	}
	metadataFn := func(idx int) metadataUnion {
		return metadataUnion{
			mType:           stagedMetadatasType,
			stagedMetadatas: stagedMetadatas,
		}
	}
	dataset := mustGenerateTestDataset(t, datasetGenOpts{
		start:        start,
		stop:         stop,
		interval:     interval,
		ids:          ids,
		category:     untimedMetric,
		typeFn:       metricTypeFn,
		valueGenOpts: valueGenOpts,
		metadataFn:   metadataFn,
	})

	writingClients := clients[:2]
	for _, data := range dataset {
		setNowFn(data.timestamp)

		for _, mm := range data.metricWithMetadatas {
			for _, c := range writingClients {
				require.NoError(t, c.writeUntimedMetricWithMetadatas(mm.metric.untimed, mm.metadata.stagedMetadatas))
			}
		}
		for _, c := range writingClients {
			require.NoError(t, c.flush())
		}

		// Give server some time to process the incoming packets.
		time.Sleep(time.Second)
	}

	// Move time forward and wait for flushing to happen at the originating server
	// (where the raw metrics are aggregated).
	originatingServerflushTime := stop.Add(storagePolicy.Resolution().Window)
	setNowFn(originatingServerflushTime)
	time.Sleep(2 * time.Second)

	// Move time forward again and wait for flushing to happen at the destination server
	// (where the rollup metrics are aggregated).
	destinationServerflushTime := originatingServerflushTime.Add(2 * time.Second)
	setNowFn(destinationServerflushTime)
	time.Sleep(2 * time.Second)

	// Stop the servers.
	for i, server := range servers {
		require.NoError(t, server.stopServer())
		log.Infof("server %d is now down", i)
	}

	// Stop the clients.
	for _, client := range clients {
		client.close()
	}

	// Validate results.
	var destinationServer *testServerSetup
	if _, exists := leaders[2]; exists {
		destinationServer = servers[2]
	} else if _, exists = leaders[3]; exists {
		destinationServer = servers[3]
	} else {
		require.Fail(t, "there must exist a leader between server 2 and server 3")
	}

	aggregatorOpts := destinationServer.aggregatorOpts
	expectedMetricKey := metricKey{
		category: forwardedMetric,
		typ:      metric.GaugeType,
		id:       pipelineRollupID,
	}
	expectedValuesByTime := make(valuesByTime)
	expectedValues := []float64{
		math.NaN(),
		float64(numIDs),
		float64(numIDs),
		float64(numIDs),
		float64(numIDs),
	}
	for i := 0; i < len(expectedValues); i++ {
		currTime := start.Add(time.Duration(i+1) * storagePolicy.Resolution().Window)
		agg := aggregation.NewGauge(aggregation.NewOptions())
		agg.Update(expectedValues[i])
		expectedValuesByTime[currTime.UnixNano()] = agg
	}
	expectedDatapointsByID := datapointsByID{
		expectedMetricKey: expectedValuesByTime,
	}
	expectedBuckets := []aggregationBucket{
		{
			key: aggregationKey{
				aggregationID: maggregation.MustCompressTypes(maggregation.Sum),
				storagePolicy: storagePolicy,
			},
			data: expectedDatapointsByID,
		},
	}
	expectedResults, err := computeExpectedAggregationOutput(
		destinationServerflushTime,
		expectedBuckets,
		aggregatorOpts,
	)
	require.NoError(t, err)
	require.True(t, cmp.Equal(expectedResults, destinationServer.sortedResults(), testCmpOpts...))
}
