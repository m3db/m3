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
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/aggregator/aggregator"
	aggclient "github.com/m3db/m3/src/aggregator/client"
	"github.com/m3db/m3/src/cluster/placement"
	maggregation "github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	metricid "github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/pipeline"
	"github.com/m3db/m3/src/metrics/pipeline/applied"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/transformation"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"
)

//nolint
func TestResendAggregatedValueStress(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	aggregatorClientType, err := getAggregatorClientTypeFromEnv()
	require.NoError(t, err)

	serverOpts := newTestServerOptions(t)

	// Placement setup.
	var (
		numTotalShards = 1024
		placementKey   = "/placement"
	)
	serverSetup := struct {
		rawTCPAddr     string
		httpAddr       string
		m3MsgAddr      string
		instanceConfig placementInstanceConfig
	}{
		rawTCPAddr: "localhost:6000",
		httpAddr:   "localhost:16000",
		m3MsgAddr:  "localhost:26000",
		instanceConfig: placementInstanceConfig{
			instanceID:          "localhost:6000",
			shardSetID:          1,
			shardStartInclusive: 0,
			shardEndExclusive:   1024,
		},
	}

	serverSetup.instanceConfig.instanceID = serverSetup.rawTCPAddr
	if aggregatorClientType == aggclient.M3MsgAggregatorClient {
		serverSetup.instanceConfig.instanceID = serverSetup.m3MsgAddr
	}

	instance := serverSetup.instanceConfig.newPlacementInstance()
	initPlacement := newPlacement(numTotalShards, []placement.Instance{instance})
	setPlacement(t, placementKey, serverOpts.ClusterClient(), initPlacement)
	serverOpts = setupTopic(t, serverOpts, initPlacement)
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
	instrumentOpts := instrument.NewOptions()
	logger := xtest.NewLogger(t)
	instrumentOpts = instrumentOpts.SetLogger(logger)
	serverOpts = serverOpts.
		SetBufferForPastTimedMetric(time.Second * 5).
		SetMaxAllowedForwardingDelayFn(func(resolution time.Duration, numForwardedTimes int) time.Duration {
			return resolution
		}).
		// allow testing entry Closing
		SetEntryTTL(time.Second).
		SetInstrumentOptions(instrumentOpts).
		SetElectionCluster(electionCluster).
		SetHTTPAddr(serverSetup.httpAddr).
		SetInstanceID(serverSetup.instanceConfig.instanceID).
		SetTopicName(defaultTopicName).
		SetRawTCPAddr(serverSetup.rawTCPAddr).
		SetM3MsgAddr(serverSetup.m3MsgAddr).
		SetShardFn(shardFn).
		SetShardSetID(serverSetup.instanceConfig.shardSetID).
		SetClientConnectionOptions(connectionOpts).
		SetDiscardNaNAggregatedValues(false)
	server := newTestServerSetup(t, serverOpts)

	// Start the servers.
	log := xtest.NewLogger(t)
	require.NoError(t, server.startServer())
	log.Sugar().Info("server is now up")

	// Waiting for server to be leader.
	err = server.waitUntilLeader()
	require.NoError(t, err)

	var (
		storagePolicies = policy.StoragePolicies{
			policy.NewStoragePolicy(5*time.Millisecond, xtime.Second, time.Hour),
		}
	)

	stagedMetadatas := metadata.StagedMetadatas{
		{
			CutoverNanos: 0,
			Tombstoned:   false,
			Metadata: metadata.Metadata{
				Pipelines: []metadata.PipelineMetadata{
					{
						AggregationID:   maggregation.DefaultID,
						ResendEnabled:   true,
						StoragePolicies: storagePolicies,
						Pipeline: applied.NewPipeline([]applied.OpUnion{
							{
								Type:           pipeline.TransformationOpType,
								Transformation: pipeline.TransformationOp{Type: transformation.Increase},
							},
							{
								Type: pipeline.RollupOpType,
								Rollup: applied.RollupOp{
									ID:            []byte(pipelineRollupID),
									AggregationID: maggregation.MustCompressTypes(maggregation.Sum),
								},
							},
							{
								Type:           pipeline.TransformationOpType,
								Transformation: pipeline.TransformationOp{Type: transformation.Reset},
							},
						}),
					},
				},
			},
		},
	}

	start := time.Now().Truncate(time.Millisecond * 5)
	log.Sugar().Infof("Start time: %v", start)
	c1 := server.newClient(t)
	c2 := server.newClient(t)
	stop1 := make(chan struct{}, 1)
	stop2 := make(chan struct{}, 1)
	resolution := storagePolicies[0].Resolution().Window
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		writeMetrics(t, c1, stop1, "foo", start, resolution, stagedMetadatas)
		wg.Done()
	}()
	go func() {
		writeMetrics(t, c2, stop2, "bar", start, resolution, stagedMetadatas)
		wg.Done()
	}()
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Millisecond)
		expectedTs := start.Add(time.Millisecond * 5)
		zero := false
		rollupMetricID := metricid.ChunkedID{
			Prefix: aggregator.NewOptions(clock.NewOptions()).FullGaugePrefix(),
			Data:   []byte(pipelineRollupID),
		}.String()
		round := 0
		for range ticker.C {
			if zero {
				if server.removeIf(expectedTs.UnixNano(), rollupMetricID, storagePolicies[0], 0) {
					zero = false
					expectedTs = expectedTs.Add(time.Millisecond * 2).Add(time.Microsecond * 500)
					round++
				}
			} else {
				if server.removeIf(expectedTs.UnixNano(), rollupMetricID, storagePolicies[0], 10) {
					zero = true
					expectedTs = expectedTs.Add(time.Millisecond * 2).Add(time.Microsecond * 500)
				}
			}
			if time.Now().Sub(expectedTs) > time.Second*5 {
				actualVal := server.value(expectedTs.UnixNano(), rollupMetricID, storagePolicies[0])
				log.Sugar().Fatalf("failed for: ts:=%v value=%v zero=%v, round=%v",
					expectedTs, actualVal, zero, round)
			}

			if round > 2000 {
				stop1 <- struct{}{}
				stop2 <- struct{}{}
				ticker.Stop()
				return
			}
		}
	}()

	wg.Wait()
	log.Sugar().Infof("done. Ran for %v\n", time.Since(start))

	// give time for the aggregations to Close.
	time.Sleep(time.Second * 5)

	require.NoError(t, c1.close())
	require.NoError(t, c2.close())

	electionCluster.Close()

	// Stop the server.
	require.NoError(t, server.stopServer())
	log.Sugar().Info("server is now down")
}

func writeMetrics(
	t *testing.T,
	c *client,
	stop chan struct{},
	metricID string,
	start time.Time,
	resolution time.Duration,
	stagedMetadatas metadata.StagedMetadatas) {
	require.NoError(t, c.connect())
	value := 0.0
	ts := start.Add(time.Millisecond * -1)
	ticker := time.NewTicker(resolution)
	delay := time.Millisecond * 60
	rnd := rand.New(rand.NewSource(10)) //nolint:gosec
	var wg sync.WaitGroup
	for range ticker.C {
		select {
		case <-stop:
			wg.Wait()
			return
		default:
		}
		ts = ts.Add(resolution)
		value += 5.0
		wg.Add(1)
		go func(ts time.Time, value float64) {
			defer wg.Done()
			if rnd.Intn(3) == 0 {
				time.Sleep(delay)
			}
			m := unaggregated.MetricUnion{
				Type:            metric.GaugeType,
				ID:              metricid.RawID(metricID),
				ClientTimeNanos: xtime.ToUnixNano(ts),
				GaugeVal:        value,
			}
			require.NoError(t, c.writeUntimedMetricWithMetadatas(m, stagedMetadatas))
			require.NoError(t, c.flush())
		}(ts, value)
	}
}
