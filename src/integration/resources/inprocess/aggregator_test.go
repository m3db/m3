// +build test_harness
// Copyright (c) 2021  Uber Technologies, Inc.
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

package inprocess

import (
	"errors"
	"fmt"
	"testing"
	"time"

	m3agg "github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	"github.com/m3db/m3/src/cluster/placementhandler/handleroptions"
	"github.com/m3db/m3/src/integration/resources"
	"github.com/m3db/m3/src/msg/generated/proto/topicpb"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/storage"
	xtime "github.com/m3db/m3/src/x/time"
	"github.com/prometheus/common/model"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewAggregator(t *testing.T) {
	coord, closer := setupCoordinator(t)
	defer closer()
	require.NoError(t, coord.WaitForNamespace(""))

	agg, err := NewAggregatorFromYAML(defaultAggregatorConfig, AggregatorOptions{})
	require.NoError(t, err)
	setupPlacement(t, coord, resources.Aggregators{agg})
	setupM3msgTopic(t, coord)
	agg.Start()
	require.NoError(t, resources.Retry(agg.IsHealthy))
	require.NoError(t, agg.Close())

	// re-construct and restart an aggregator instance
	agg, err = NewAggregatorFromYAML(defaultAggregatorConfig, AggregatorOptions{Start: true})
	require.NoError(t, err)
	require.NoError(t, resources.Retry(agg.IsHealthy))
	require.NoError(t, agg.Close())
}

func TestMultiAggregators(t *testing.T) {
	coord, closer := setupCoordinator(t)
	defer closer()
	require.NoError(t, coord.WaitForNamespace(""))

	aggOpts := AggregatorOptions{
		GenerateHostID: true,
		GeneratePorts:  true,
		Start:          false,
	}

	agg1, err := NewAggregatorFromYAML(defaultAggregatorConfig, aggOpts)
	defer func() {
		assert.NoError(t, agg1.Close())
	}()
	require.NoError(t, err)

	agg2, err := NewAggregatorFromYAML(defaultAggregatorConfig, aggOpts)
	defer func() {
		assert.NoError(t, agg2.Close())
	}()
	require.NoError(t, err)

	setupPlacement(t, coord, resources.Aggregators{agg1, agg2})
	setupM3msgTopic(t, coord)

	agg1.Start()
	require.NoError(t, resources.Retry(agg1.IsHealthy))

	agg2.Start()
	require.NoError(t, resources.Retry(agg2.IsHealthy))
}

func TestAggregatorStatus(t *testing.T) {
	coord, closer := setupCoordinator(t)
	defer closer()
	require.NoError(t, coord.WaitForNamespace(""))

	agg, err := NewAggregatorFromYAML(
		defaultAggregatorConfig,
		AggregatorOptions{GenerateHostID: true, GeneratePorts: true},
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, agg.Close())
	}()

	setupPlacement(t, coord, resources.Aggregators{agg})
	setupM3msgTopic(t, coord)
	agg.Start()
	require.NoError(t, resources.Retry(agg.IsHealthy))

	followerStatus := m3agg.RuntimeStatus{
		FlushStatus: m3agg.FlushStatus{
			ElectionState: m3agg.FollowerState,
			CanLead:       false,
		},
	}

	status, err := agg.Status()
	require.NoError(t, err)
	require.Equal(t, followerStatus, status)

	// A follower remains a follower after resigning
	require.NoError(t, agg.Resign())
	status, err = agg.Status()
	require.NoError(t, err)
	require.Equal(t, followerStatus, status)
}

func TestAggregatorWriteWithCluster(t *testing.T) {
	cfgs, err := NewClusterConfigsFromYAML(defaultDBNodeConfig, aggregatorCoordConfig, defaultAggregatorConfig)
	require.NoError(t, err)

	cluster, err := NewCluster(cfgs,
		resources.ClusterOptions{
			DBNode: resources.NewDBNodeClusterOptions(),
		},
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, cluster.Cleanup())
	}()

	coord := cluster.Coordinator()
	agg, err := NewAggregatorFromYAML(defaultAggregatorConfig, AggregatorOptions{Start: false})
	defer func() {
		assert.NoError(t, agg.Close())
	}()
	require.NoError(t, err)

	setupPlacement(t, coord, resources.Aggregators{agg})
	setupM3msgTopic(t, coord)

	agg.Start()
	require.NoError(t, resources.Retry(agg.IsHealthy))

	testAggMetrics(t, coord)
}

func setupCoordinator(t *testing.T) (resources.Coordinator, func()) {
	dbnode, err := NewDBNodeFromYAML(defaultDBNodeConfig, DBNodeOptions{Start: true})
	require.NoError(t, err)

	coord, err := NewCoordinatorFromYAML(aggregatorCoordConfig, CoordinatorOptions{Start: true})
	require.NoError(t, err)

	return coord, func() {
		assert.NoError(t, coord.Close())
		assert.NoError(t, dbnode.Close())
	}
}

func setupM3msgTopic(t *testing.T, coord resources.Coordinator) {
	m3msgTopicOpts := resources.M3msgTopicOptions{
		Zone:      "embedded",
		Env:       "default_env",
		TopicName: "aggregator_ingest",
	}

	_, err := coord.InitM3msgTopic(m3msgTopicOpts, admin.TopicInitRequest{NumberOfShards: 4})
	require.NoError(t, err)

	_, err = coord.AddM3msgTopicConsumer(m3msgTopicOpts, admin.TopicAddRequest{
		ConsumerService: &topicpb.ConsumerService{
			ServiceId: &topicpb.ServiceID{
				Name:        handleroptions.M3AggregatorServiceName,
				Environment: m3msgTopicOpts.Env,
				Zone:        m3msgTopicOpts.Zone,
			},
			ConsumptionType: topicpb.ConsumptionType_REPLICATED,
			MessageTtlNanos: 600000000000, // 10 mins
		},
	})
	require.NoError(t, err)

	aggregatedTopicOpts := resources.M3msgTopicOptions{
		Zone:      "embedded",
		Env:       "default_env",
		TopicName: "aggregated_metrics",
	}
	_, err = coord.InitM3msgTopic(aggregatedTopicOpts, admin.TopicInitRequest{NumberOfShards: 4})
	require.NoError(t, err)

	_, err = coord.AddM3msgTopicConsumer(aggregatedTopicOpts, admin.TopicAddRequest{
		ConsumerService: &topicpb.ConsumerService{
			ServiceId: &topicpb.ServiceID{
				Name:        handleroptions.M3CoordinatorServiceName,
				Environment: aggregatedTopicOpts.Env,
				Zone:        aggregatedTopicOpts.Zone,
			},
			ConsumptionType: topicpb.ConsumptionType_SHARED,
			MessageTtlNanos: 600000000000, // 10 mins
		},
	})
	require.NoError(t, err)
}

func setupPlacement(t *testing.T, coord resources.Coordinator, aggs resources.Aggregators) {
	instances := make([]*placementpb.Instance, 0, len(aggs))
	for _, agg := range aggs {
		info, err := agg.HostDetails()
		require.NoError(t, err)
		instance := &placementpb.Instance{
			Id:             info.ID,
			IsolationGroup: info.ID,
			Zone:           info.Zone,
			Weight:         1,
			Endpoint:       fmt.Sprintf("%s:%d", info.M3msgAddress, info.M3msgPort),
			Hostname:       info.ID,
			Port:           info.M3msgPort,
		}

		instances = append(instances, instance)
	}

	_, err := coord.InitPlacement(
		resources.PlacementRequestOptions{
			Service: resources.ServiceTypeM3Aggregator,
			Zone:    "embedded",
			Env:     "default_env",
		},
		admin.PlacementInitRequest{
			NumShards:         4,
			ReplicationFactor: 1,
			Instances:         instances,
		},
	)
	require.NoError(t, err)

	_, err = coord.InitPlacement(
		resources.PlacementRequestOptions{
			Service: resources.ServiceTypeM3Coordinator,
			Zone:    "embedded",
			Env:     "default_env",
		},
		admin.PlacementInitRequest{
			Instances: []*placementpb.Instance{
				{
					Id:       "m3coordinator01",
					Zone:     "embedded",
					Endpoint: "0.0.0.0:7507",
					Hostname: "m3coordinator01",
					Port:     7507,
				},
			},
		},
	)
	require.NoError(t, err)
}

func testAggMetrics(t *testing.T, coord resources.Coordinator) {
	var (
		ts      = time.Now()
		ts1     = xtime.ToUnixNano(ts)
		ts2     = xtime.ToUnixNano(ts.Add(1 * time.Millisecond))
		ts3     = xtime.ToUnixNano(ts.Add(2 * time.Millisecond))
		samples = []prompb.Sample{
			{Value: 1, Timestamp: storage.TimeToPromTimestamp(ts1)},
			{Value: 2, Timestamp: storage.TimeToPromTimestamp(ts2)},
			{Value: 3, Timestamp: storage.TimeToPromTimestamp(ts3)},
		}
		// 6=1+2+3 is the sum of all three samples.
		expectedValue = model.SampleValue(6)
	)
	assert.NoError(t, resources.Retry(func() error {
		return coord.WriteProm("cpu", map[string]string{"host": "host1"}, samples, nil)
	}))

	queryHeaders := resources.Headers{"M3-Metrics-Type": {"aggregated"}, "M3-Storage-Policy": {"5s:6h"}}

	// Instant Query
	require.NoError(t, resources.Retry(func() error {
		result, err := coord.InstantQuery(resources.QueryRequest{Query: "cpu"}, queryHeaders)
		if err != nil {
			return err
		}
		if len(result) != 1 {
			return errors.New("wrong amount of datapoints")
		}
		if result[0].Value != expectedValue {
			return errors.New("wrong data point value")
		}
		return nil
	}))

	// Range Query
	require.NoError(t, resources.Retry(func() error {
		result, err := coord.RangeQuery(
			resources.RangeQueryRequest{
				Query: "cpu",
				Start: time.Now().Add(-30 * time.Second),
				End:   time.Now(),
				Step:  1 * time.Second,
			},
			queryHeaders,
		)
		if err != nil {
			return err
		}
		if len(result) != 1 {
			return errors.New("wrong amount of series in the range query result")
		}
		if len(result[0].Values) == 0 {
			return errors.New("empty range query result")
		}
		if result[0].Values[0].Value != expectedValue {
			return errors.New("wrong range query value")
		}
		return nil
	}))
}

const defaultAggregatorConfig = `{}`

const aggregatorCoordConfig = `
clusters:
  - client:
      config:
        service:
          env: default_env
          zone: embedded
          service: m3db
          etcdClusters:
            - zone: embedded
              endpoints:
                - 127.0.0.1:2379
downsample:
  rules:
    mappingRules:
      - name: "agged metrics"
        filter: "host:*"
        aggregations: ["Sum"]
        storagePolicies:
          - resolution: 5s
            retention: 6h
ingest:
  ingester:
    workerPoolSize: 10000
  m3msg:
    server:
      listenAddress: "0.0.0.0:7507"
`
