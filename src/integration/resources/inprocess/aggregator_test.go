//go:build integration_v2
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
	"testing"

	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	"github.com/m3db/m3/src/integration/resources"
	"github.com/m3db/m3/src/msg/generated/proto/topicpb"
	"github.com/m3db/m3/src/query/generated/proto/admin"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewAggregator(t *testing.T) {
	dbnode, err := NewDBNodeFromYAML(defaultDBNodeConfig, DBNodeOptions{})
	require.NoError(t, err)

	coord, err := NewCoordinatorFromYAML(aggregatorCoordConfig, CoordinatorOptions{})
	require.NoError(t, err)

	defer func() {
		assert.NoError(t, coord.Close())
		assert.NoError(t, dbnode.Close())
	}()

	require.NoError(t, coord.WaitForNamespace(""))

	setupPlacement(t, coord)
	setupM3msgTopic(t, coord)

	agg, err := NewAggregator(defaultAggregatorConfig, AggregatorOptions{})
	require.NoError(t, err)
	require.NoError(t, agg.Close())

	// restart an aggregator instance
	agg, err = NewAggregator(defaultAggregatorConfig, AggregatorOptions{})
	require.NoError(t, err)
	require.NoError(t, agg.Close())
}

func setupM3msgTopic(t *testing.T, coord resources.Coordinator) {
	m3msgTopicOpts := resources.M3msgTopicOptions{
		Zone:      "embedded",
		Env:       "default_env",
		TopicName: "aggregator_ingest",
	}

	_, err := coord.InitM3msgTopic(m3msgTopicOpts, admin.TopicInitRequest{NumberOfShards: 16})
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
	_, err = coord.InitM3msgTopic(aggregatedTopicOpts, admin.TopicInitRequest{NumberOfShards: 16})
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
			NumShards:         1,
			ReplicationFactor: 1,
			Instances: []*placementpb.Instance{
				{
					Id:             "m3aggregator01",
					IsolationGroup: "rack1",
					Zone:           "embedded",
					Weight:         1,
					Endpoint:       "m3aggregator01:6000",
					Hostname:       "m3aggregator01",
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
					Endpoint: "m3coordinator01:7507",
					Hostname: "m3coordinator01",
					Port:     7507,
				},
			},
		},
	)
	require.NoError(t, err)
}

const defaultAggregatorConfig = `
metrics:
  prometheus:
    handlerPath: /metrics
    listenAddress: 0.0.0.0:6002
    timerType: histogram
  sanitization: prometheus
  samplingRate: 1.0
http:
  listenAddress: 0.0.0.0:6001
  readTimeout: 60s
  writeTimeout: 60s
m3msg:
  server:
    listenAddress: 0.0.0.0:6000
    retry:
      maxBackoff: 10s
      jitter: true
  consumer:
    messagePool:
      size: 16384
      watermark:
        low: 0.2
        high: 0.5
kvClient:
  etcd:
    env: default_env
    zone: embedded
    service: m3aggregator
    cacheDir: "*"
    etcdClusters:
      - zone: embedded
        endpoints:
        - 127.0.0.1:2379
aggregator:
  instanceID:
    type: host_id
  stream:
    eps: 0.001
    capacity: 32
  client:
    type: m3msg
    m3msg:
      producer:
        writer:
          topicName: aggregator_ingest
          topicServiceOverride:
            zone: embedded
            environment: default_env
          placement:
            isStaged: true
          placementServiceOverride:
            namespaces:
              placement: /placement
          messagePool:
            size: 16384
            watermark:
              low: 0.2
              high: 0.5
          ignoreCutoffCutover: true
  placementManager:
    kvConfig:
      namespace: /placement
      environment: default_env
      zone: embedded
    placementWatcher:
      key: m3aggregator
  electionManager:
    serviceID:
      name: m3aggregator
      environment: default_env
      zone: embedded
  flush:
    handlers:
      - dynamicBackend:
          name: m3msg
          hashType: murmur32
          producer:
            writer:
              topicName: aggregated_metrics
              topicServiceOverride:
                zone: embedded
                environment: default_env
              messagePool:
                size: 16384
                watermark:
                  low: 0.2
                  high: 0.5
`

const aggregatorCoordConfig = `
clusters:
  - namespaces:
      - namespace: default
        type: unaggregated
        retention: 1h
    client:
      config:
        service:
          env: default_env
          zone: embedded
          service: m3db
          cacheDir: "*"
          etcdClusters:
            - zone: embedded
              endpoints:
                - 127.0.0.1:2379
ingest:
  ingester:
    workerPoolSize: 10000
    opPool:
      size: 10000
    retry:
      maxRetries: 3
      jitter: true
    logSampleRate: 0.01
  m3msg:
    server:
      listenAddress: "0.0.0.0:7507"
      retry:
        maxBackoff: 10s
        jitter: true
`
