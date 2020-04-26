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
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3/src/aggregator/aggregation"
	aggclient "github.com/m3db/m3/src/aggregator/client"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/cluster/placement"
	maggregation "github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/pipeline"
	"github.com/m3db/m3/src/metrics/pipeline/applied"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/transformation"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestMultiServerForwardingPipelineKeepNaNAggregatedValues(t *testing.T) {
	testMultiServerForwardingPipeline(t, false)
}

func TestMultiServerForwardingPipelineDiscardNaNAggregatedValues(t *testing.T) {
	testMultiServerForwardingPipeline(t, true)
}

func testMultiServerForwardingPipeline(t *testing.T, discardNaNAggregatedValues bool) {
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
	servers := make([]*testServerSetup, 0, len(multiServerSetup))
	for _, mss := range multiServerSetup {
		instrumentOpts := instrument.NewOptions()
		logger := instrumentOpts.Logger().With(
			zap.String("serverAddr", mss.rawTCPAddr),
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
			SetClientConnectionOptions(connectionOpts).
			SetDiscardNaNAggregatedValues(discardNaNAggregatedValues)
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

	var (
		idPrefix        = "foo"
		numIDs          = 2
		start           = getNowFn()
		stop            = start.Add(12 * time.Second)
		interval        = time.Second
		storagePolicies = policy.StoragePolicies{
			policy.NewStoragePolicy(2*time.Second, xtime.Second, time.Hour),
			policy.NewStoragePolicy(4*time.Second, xtime.Second, 24*time.Hour),
		}
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
						StoragePolicies: storagePolicies,
						Pipeline: applied.NewPipeline([]applied.OpUnion{
							{
								Type:           pipeline.TransformationOpType,
								Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
							},
							{
								Type: pipeline.RollupOpType,
								Rollup: applied.RollupOp{
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

	// Move time forward using the larger resolution and wait for flushing to happen
	// at the originating server (where the raw metrics are aggregated).
	originatingServerflushTime := stop.Add(2 * storagePolicies[1].Resolution().Window)
	for currTime := stop; !currTime.After(originatingServerflushTime); currTime = currTime.Add(time.Second) {
		setNowFn(currTime)
		time.Sleep(time.Second)
	}

	// Move time forward using the larger resolution again and wait for flushing to
	// happen at the destination server (where the rollup metrics are aggregated).
	destinationServerflushTime := originatingServerflushTime.Add(2 * storagePolicies[1].Resolution().Window)
	for currTime := originatingServerflushTime; !currTime.After(destinationServerflushTime); currTime = currTime.Add(time.Second) {
		setNowFn(currTime)
		time.Sleep(time.Second)
	}

	// Stop the servers.
	for i, server := range servers {
		require.NoError(t, server.stopServer())
		log.Sugar().Infof("server %d is now down", i)
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
	expectedMetricKeyList := []metricKey{
		{
			category:      forwardedMetric,
			typ:           metric.GaugeType,
			id:            pipelineRollupID,
			storagePolicy: storagePolicies[0],
		},
		{
			category:      forwardedMetric,
			typ:           metric.GaugeType,
			id:            pipelineRollupID,
			storagePolicy: storagePolicies[1],
		},
	}
	// Expected results for 2s:1h storage policy.
	expectedValuesByTimeList := []valuesByTime{
		make(valuesByTime),
		make(valuesByTime),
	}
	expectedValuesList := [][]float64{
		{
			math.NaN(),
			float64(numIDs),
			float64(numIDs),
			float64(numIDs),
			float64(numIDs),
			float64(numIDs),
		},
		{
			math.NaN(),
			float64(numIDs),
			float64(numIDs),
		},
	}
	for spIdx := 0; spIdx < len(storagePolicies); spIdx++ {
		storagePolicy := storagePolicies[spIdx]
		for i := 0; i < len(expectedValuesList[spIdx]); i++ {
			if discardNaNAggregatedValues && math.IsNaN(expectedValuesList[spIdx][i]) {
				continue
			}
			currTime := start.Add(time.Duration(i+1) * storagePolicy.Resolution().Window)
			instrumentOpts := aggregatorOpts.InstrumentOptions()
			agg := aggregation.NewGauge(aggregation.NewOptions(instrumentOpts))
			agg.Update(time.Now(), expectedValuesList[spIdx][i])
			expectedValuesByTimeList[spIdx][currTime.UnixNano()] = agg
		}
	}

	var expectedResultsFlattened []aggregated.MetricWithStoragePolicy
	for i := 0; i < len(storagePolicies); i++ {
		expectedDatapointsByID := datapointsByID{
			expectedMetricKeyList[i]: expectedValuesByTimeList[i],
		}
		expectedBuckets := []aggregationBucket{
			{
				key: aggregationKey{
					aggregationID: maggregation.MustCompressTypes(maggregation.Sum),
					storagePolicy: storagePolicies[i],
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
		expectedResultsFlattened = append(expectedResultsFlattened, expectedResults...)
	}
	sort.Sort(byTimeIDPolicyAscending(expectedResultsFlattened))
	require.True(t, cmp.Equal(expectedResultsFlattened, destinationServer.sortedResults(), testCmpOpts...))
}
