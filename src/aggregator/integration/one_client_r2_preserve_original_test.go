//go:build integration

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
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	metricid "github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	require "github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestOriginalMetricIsPreservedOnlyWithDefaultPipeline test emits two unique
// metrics: foo1 and foo2. Both the metrics rollup to the same metric: rollup1.
// The difference is that foo1 does not have a default pipeline
// whereas foo2 has a default pipeline.
// The output of the test should therefore be 2 metrics in total:
// foo2 and rollup1. foo1 should not be present in the output as it
// does not have a default pipeline.
// Context: When keepOriginal is false, we remove the default
// pipeline from the stagedMetadata of the original metric.
// This makes the aggregator drop the original metric but
// continues to aggregate and emit the rolled up metric which
// is what we expect.
func TestOriginalMetricIsPreservedOnlyWithDefaultPipeline(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	serverOpts := newTestServerOptions(t)
	log := serverOpts.InstrumentOptions().Logger()

	log.Info("initialized new test server options")

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
	log.Info("initializing placement")
	instance := cfg.newPlacementInstance()
	placement := newPlacement(numShards, []placement.Instance{instance})
	placementKey := serverOpts.PlacementKVKey()
	setPlacement(t, placementKey, serverOpts.ClusterClient(), placement)
	log.Info("initializing topic")

	serverOpts = setupTopic(t, serverOpts, placement)

	// Create server.
	log.Info("initializing testServer")
	testServer := newTestServerSetup(t, serverOpts)
	defer testServer.close()
	// Start the server.
	log.Info("test one client sending multiple types of untimed metrics")
	require.NoError(t, testServer.startServer())
	log.Info("server is now up")
	require.NoError(t, testServer.waitUntilLeader())
	log.Info("server is now the leader")

	client := testServer.newClient(t)
	log.Info(
		"created aggregator client",
		zap.String(
			"client_type",
			serverOpts.AggregatorClientType().String(),
		),
	)

	require.NoError(t, client.connect())

	ts := time.Now().Truncate(time.Hour)

	clock.SetNow(ts)

	testCases := []struct {
		name                     string
		inputMetric              unaggregated.MetricUnion
		metadatas                metadata.StagedMetadatas
		expectedInOutput         bool
		expectedRollupInOutput   bool
		expectedRollupMetricName string
	}{
		{
			name: "one metric with no default pipeline",
			inputMetric: unaggregated.MetricUnion{
				Type:       metric.CounterType,
				ID:         metricid.RawID("foo1"),
				CounterVal: 420,
			},
			metadatas:                testStagedMetadatasWithoutDefaultPipeline,
			expectedInOutput:         false,
			expectedRollupInOutput:   true,
			expectedRollupMetricName: "rollup1",
		},
		{
			name: "one metric with default pipeline",
			inputMetric: unaggregated.MetricUnion{
				Type:       metric.CounterType,
				ID:         metricid.RawID("foo2"),
				CounterVal: 120,
			},
			metadatas:                testStagedMetadatasWithDefaultPipeline,
			expectedInOutput:         true,
			expectedRollupInOutput:   true,
			expectedRollupMetricName: "rollup1",
		},
	}

	for _, m := range testCases {
		require.NoError(
			t,
			client.writeUntimedMetricWithMetadatas(
				m.inputMetric,
				m.metadatas,
			),
		)
	}

	require.NoError(t, client.flush())
	log.Info("wait for metrics to be ingested and aggregated")
	waitForMetricEmissions(clock, ts)
	require.NoError(t, client.close())

	// Stop the server.
	require.NoError(t, testServer.stopServer())
	log.Info("server is now down")

	actual := testServer.sortedResults()
	for _, m := range actual {
		log.Info("actual metric", zap.String("id", m.ID.String()))
	}

	for _, m := range testCases {
		found := isMetricFound(m.inputMetric.ID.String(), actual)
		require.Equal(t, m.expectedInOutput, found, "expectation not met for metric %s", m.inputMetric.ID.String())

		if m.expectedRollupInOutput {
			found := isMetricFound(m.expectedRollupMetricName, actual)
			require.True(t, found, "expected metric %s to be in the output.", m.expectedRollupMetricName)
		}
	}
}

func isMetricFound(id string, metrics []aggregated.MetricWithStoragePolicy) bool {
	found := false
	for _, m := range metrics {
		if strings.Contains(m.ID.String(), id) {
			found = true
			break
		}
	}
	return found
}

func waitForMetricEmissions(clock *testClock, now time.Time) {
	// Wait for original metrics to be emitted.
	time.Sleep(2 * time.Second)
	now = now.Add(2 * time.Second)
	clock.SetNow(now)

	// Wait for forwarded metrics to be emitted.
	time.Sleep(2 * time.Second)
	now = now.Add(2 * time.Second)
	clock.SetNow(now)

	// Wait additional time for FlushTask to be
	// prepared and executed to flush the final
	// rolled up metrics.
	time.Sleep(2 * time.Second)
}
