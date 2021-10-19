// +build integration_v2

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
	"encoding/json"
	"fmt"
	"testing"
	"time"

	m3agg "github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	"github.com/m3db/m3/src/cmd/services/m3aggregator/config"
	"github.com/m3db/m3/src/integration/resources"
	nettest "github.com/m3db/m3/src/integration/resources/net"
	"github.com/m3db/m3/src/msg/generated/proto/topicpb"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/x/config/hostid"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestNewAggregator(t *testing.T) {
	coord, closer := setupCoordinator(t)
	defer closer()
	require.NoError(t, coord.WaitForNamespace(""))

	setupPlacement(t, coord)
	setupM3msgTopic(t, coord)

	agg, err := NewAggregatorFromYAML(defaultAggregatorConfig, AggregatorOptions{GenerateHostID: true})
	require.NoError(t, err)
	require.NoError(t, resources.Retry(agg.IsHealthy))
	require.NoError(t, agg.Close())

	// restart an aggregator instance
	agg, err = NewAggregatorFromYAML(defaultAggregatorConfig, AggregatorOptions{GenerateHostID: true})
	require.NoError(t, err)
	require.NoError(t, resources.Retry(agg.IsHealthy))
	require.NoError(t, agg.Close())
}

func TestMultiAggregators(t *testing.T) {
	coord, closer := setupCoordinator(t)
	defer closer()
	require.NoError(t, coord.WaitForNamespace(""))

	args, err := generateTestAggregatorArgs(2)
	require.NoError(t, err)
	setupPlacementMultiAggs(t, coord, args)
	setupM3msgTopic(t, coord)

	cfg1, err := loadDefaultAggregatorConfig()
	require.NoError(t, err)
	updateTestAggConfig(&cfg1, args[0])

	agg1, err := NewAggregator(cfg1, AggregatorOptions{GeneratePorts: true})
	require.NoError(t, err)
	require.NoError(t, resources.Retry(agg1.IsHealthy))
	defer func() {
		assert.NoError(t, agg1.Close())
	}()

	cfg2, err := loadDefaultAggregatorConfig()
	require.NoError(t, err)
	updateTestAggConfig(&cfg2, args[1])

	agg2, err := NewAggregator(cfg2, AggregatorOptions{GeneratePorts: true})
	require.NoError(t, err)
	require.NoError(t, resources.Retry(agg2.IsHealthy))
	defer func() {
		assert.NoError(t, agg2.Close())
	}()
}

func TestAggregatorStatus(t *testing.T) {
	coord, closer := setupCoordinator(t)
	defer closer()
	require.NoError(t, coord.WaitForNamespace(""))

	setupPlacement(t, coord)
	setupM3msgTopic(t, coord)

	agg, err := NewAggregatorFromYAML(defaultAggregatorConfig, AggregatorOptions{})
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, agg.Close())
	}()

	followerStatus := m3agg.RuntimeStatus{
		FlushStatus: m3agg.FlushStatus{
			ElectionState: m3agg.FollowerState,
			CanLead:       false,
		},
	}

	require.NoError(t, resources.Retry(agg.IsHealthy))
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
		ClusterOptions{
			DBNode: NewDBNodeClusterOptions(),
		},
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, cluster.Cleanup())
	}()

	coord := cluster.Coordinator()

	setupPlacement(t, coord)
	setupM3msgTopic(t, coord)

	agg, err := NewAggregatorFromYAML(defaultAggregatorConfig, AggregatorOptions{})
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, agg.Close())
	}()

	require.NoError(t, resources.Retry(agg.IsHealthy))

	testAggMetrics(t, coord)
}

func setupCoordinator(t *testing.T) (resources.Coordinator, func()) {
	dbnode, err := NewDBNodeFromYAML(defaultDBNodeConfig, DBNodeOptions{})
	require.NoError(t, err)

	coord, err := NewCoordinatorFromYAML(aggregatorCoordConfig, CoordinatorOptions{})
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
				Name:        "m3aggregator",
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
				Name:        "m3coordinator",
				Environment: aggregatedTopicOpts.Env,
				Zone:        aggregatedTopicOpts.Zone,
			},
			ConsumptionType: topicpb.ConsumptionType_SHARED,
			MessageTtlNanos: 600000000000, // 10 mins
		},
	})
	require.NoError(t, err)
}

func setupPlacement(t *testing.T, coord resources.Coordinator) {
	_, err := coord.InitPlacement(
		resources.PlacementRequestOptions{
			Service: resources.ServiceTypeM3Aggregator,
			Zone:    "embedded",
			Env:     "default_env",
		},
		admin.PlacementInitRequest{
			NumShards:         4,
			ReplicationFactor: 1,
			Instances: []*placementpb.Instance{
				{
					Id:             "m3aggregator_local",
					IsolationGroup: "rack1",
					Zone:           "embedded",
					Weight:         1,
					Endpoint:       "0.0.0.0:6000",
					Hostname:       "m3aggregator_local",
					Port:           6000,
				},
			},
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

func setupPlacementMultiAggs(t *testing.T, coord resources.Coordinator, args []testAggregatorArgs) {
	require.Equal(t, 2, len(args))
	_, err := coord.InitPlacement(
		resources.PlacementRequestOptions{
			Service: resources.ServiceTypeM3Aggregator,
			Zone:    "embedded",
			Env:     "default_env",
		},
		admin.PlacementInitRequest{
			NumShards:         4,
			ReplicationFactor: 1,
			Instances: []*placementpb.Instance{
				{
					Id:             args[0].hostID,
					IsolationGroup: "rack1",
					Zone:           "embedded",
					Weight:         1,
					Endpoint:       args[0].m3msgAddr,
					Hostname:       args[0].hostID,
					Port:           args[0].m3msgPort,
				},
				{
					Id:             args[1].hostID,
					IsolationGroup: "rack2",
					Zone:           "embedded",
					Weight:         1,
					Endpoint:       args[1].m3msgAddr,
					Hostname:       args[1].hostID,
					Port:           args[1].m3msgPort,
				},
			},
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

func generateTestAggregatorArgs(numAggs int) ([]testAggregatorArgs, error) {
	opts := make([]testAggregatorArgs, 0, numAggs)
	for i := 1; i <= numAggs; i++ {
		addr, p, err := nettest.GeneratePort("0.0.0.0:0")
		if err != nil {
			return nil, err
		}
		opts = append(opts, testAggregatorArgs{
			hostID:    fmt.Sprintf("m3aggregator%02d", i),
			m3msgAddr: addr,
			m3msgPort: uint32(p),
		})
	}

	return opts, nil
}

func updateTestAggConfig(cfg *config.Configuration, args testAggregatorArgs) {
	aggCfg := cfg.AggregatorOrDefault()
	aggCfg.HostID = &hostid.Configuration{
		Resolver: hostid.ConfigResolver,
		Value:    &args.hostID,
	}
	cfg.Aggregator = &aggCfg

	m3msgCfg := cfg.M3MsgOrDefault()
	m3msgCfg.Server.ListenAddress = args.m3msgAddr
	cfg.M3Msg = &m3msgCfg
}

func loadDefaultAggregatorConfig() (config.Configuration, error) {
	var cfg config.Configuration
	if err := yaml.Unmarshal([]byte(defaultAggregatorConfig), &cfg); err != nil {
		return config.Configuration{}, err
	}

	return cfg, nil
}

type testAggregatorArgs struct {
	hostID    string
	m3msgAddr string
	m3msgPort uint32
}

func testAggMetrics(t *testing.T, coord resources.Coordinator) {
	var (
		ts  = time.Now()
		ts1 = xtime.ToUnixNano(ts)
		ts2 = xtime.ToUnixNano(ts.Add(1 * time.Millisecond))
		ts3 = xtime.ToUnixNano(ts.Add(2 * time.Millisecond))
	)
	samples := []prompb.Sample{
		{Value: 1, Timestamp: storage.TimeToPromTimestamp(ts1)},
		{Value: 2, Timestamp: storage.TimeToPromTimestamp(ts2)},
		{Value: 3, Timestamp: storage.TimeToPromTimestamp(ts3)},
	}
	assert.NoError(t, resources.Retry(func() error {
		return coord.WriteProm("cpu", map[string]string{"host": "host1"}, samples)
	}))
	queryHeaders := map[string][]string{"M3-Metrics-Type": {"aggregated"}, "M3-Storage-Policy": {"10s:6h"}}
	instantQ := "api/v1/query?query=cpu"
	assert.NoError(t, coord.RunQuery(verify, instantQ, queryHeaders))
}

type jsonResponse struct {
	Status string
	Data   QueryResult
}

type QueryResult struct {
	ResultType model.ValueType
	Result     model.Vector
}

func verify(
	status int,
	headers map[string][]string,
	resp string,
	err error,
) error {
	if err != nil {
		return err
	}

	if status != 200 {
		return fmt.Errorf("expected 200, received %v", status)
	}

	if contentType, ok := headers["Content-Type"]; !ok {
		return fmt.Errorf("missing Content-Type header")
	} else if len(contentType) != 1 || contentType[0] != "application/json" { //nolint:goconst
		return fmt.Errorf("expected json content type, got %v", contentType)
	}

	var parsedResp jsonResponse
	if err := json.Unmarshal([]byte(resp), &parsedResp); err != nil {
		return err
	}

	if parsedResp.Data.Result.Len() != 1 {
		return fmt.Errorf("wrong amount of query results")
	}

	if parsedResp.Data.Result[0].Value != 6 {
		return fmt.Errorf("wrong metric value")
	}

	return nil
}

const defaultAggregatorConfig = `{}`

const aggregatorCoordConfig = `
clusters:
  - namespaces:
      - namespace: default
        type: unaggregated
        retention: 1h
      - namespace: aggregated
        type: aggregated
        resolution: 10s
        retention: 6h
    client:
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
          - resolution: 10s
            retention: 6h
ingest:
  ingester:
    workerPoolSize: 10000
  m3msg:
    server:
      listenAddress: "0.0.0.0:7507"
`
