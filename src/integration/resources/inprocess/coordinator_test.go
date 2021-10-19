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
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/integration/resources"
	"github.com/m3db/m3/src/msg/generated/proto/topicpb"
	"github.com/m3db/m3/src/msg/topic"
	"github.com/m3db/m3/src/query/generated/proto/admin"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCoordinator(t *testing.T) {
	dbnode, err := NewDBNodeFromYAML(defaultDBNodeConfig, DBNodeOptions{})
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, dbnode.Close())
	}()

	coord, err := NewCoordinatorFromYAML(defaultCoordConfig, CoordinatorOptions{})
	require.NoError(t, err)
	require.NoError(t, coord.Close())

	// Restart and shut down again to test restarting.
	coord, err = NewCoordinatorFromYAML(defaultCoordConfig, CoordinatorOptions{})
	require.NoError(t, err)
	require.NoError(t, coord.Close())
}

func TestNewEmbeddedCoordinator(t *testing.T) {
	dbnode, err := NewDBNodeFromYAML(embeddedCoordConfig, DBNodeOptions{})
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, dbnode.Close())
	}()

	d, ok := dbnode.(*DBNode)
	require.True(t, ok)
	require.True(t, d.started)

	_, err = NewEmbeddedCoordinator(d)
	require.NoError(t, err)
}

func TestNewEmbeddedCoordinatorNotStarted(t *testing.T) {
	var dbnode DBNode
	_, err := NewEmbeddedCoordinator(&dbnode)
	require.Error(t, err)
}

func TestM3msgTopicFunctions(t *testing.T) {
	dbnode, err := NewDBNodeFromYAML(defaultDBNodeConfig, DBNodeOptions{})
	require.NoError(t, err)

	coord, err := NewCoordinatorFromYAML(defaultCoordConfig, CoordinatorOptions{})
	require.NoError(t, err)

	require.NoError(t, coord.WaitForNamespace(""))

	// init an m3msg topic
	m3msgTopicOpts := resources.M3msgTopicOptions{
		Zone:      "embedded",
		Env:       "default_env",
		TopicName: "testtopic",
	}
	initResp, err := coord.InitM3msgTopic(
		m3msgTopicOpts,
		admin.TopicInitRequest{NumberOfShards: 16},
	)
	expectedInitResp := admin.TopicGetResponse{
		Topic: &topicpb.Topic{
			Name:             "testtopic",
			NumberOfShards:   16,
			ConsumerServices: nil,
		},
		Version: 1,
	}
	require.NoError(t, err)
	validateEqualTopicResp(t, expectedInitResp, initResp)

	// add a consumer service
	consumer := topicpb.ConsumerService{
		ServiceId: &topicpb.ServiceID{
			Name:        "testservice",
			Environment: m3msgTopicOpts.Env,
			Zone:        m3msgTopicOpts.Zone,
		},
		ConsumptionType: topicpb.ConsumptionType_SHARED,
		MessageTtlNanos: 1,
	}
	addResp, err := coord.AddM3msgTopicConsumer(
		m3msgTopicOpts,
		admin.TopicAddRequest{ConsumerService: &consumer},
	)
	expectedAddResp := admin.TopicGetResponse{
		Topic: &topicpb.Topic{
			Name:           "testtopic",
			NumberOfShards: 16,
			ConsumerServices: []*topicpb.ConsumerService{
				&consumer,
			},
		},
		Version: 2,
	}
	require.NoError(t, err)
	validateEqualTopicResp(t, expectedAddResp, addResp)

	// get an m3msg topic
	getResp, err := coord.GetM3msgTopic(m3msgTopicOpts)
	require.NoError(t, err)
	validateEqualTopicResp(t, expectedAddResp, getResp)

	assert.NoError(t, coord.Close())
	assert.NoError(t, dbnode.Close())
}

func validateEqualTopicResp(t *testing.T, expected, actual admin.TopicGetResponse) {
	require.Equal(t, expected.Version, actual.Version)

	t1, err := topic.NewTopicFromProto(expected.Topic)
	require.NoError(t, err)
	t2, err := topic.NewTopicFromProto(actual.Topic)
	require.NoError(t, err)
	require.Equal(t, t1, t2)
}

func TestAggPlacementFunctions(t *testing.T) {
	dbnode, err := NewDBNodeFromYAML(defaultDBNodeConfig, DBNodeOptions{})
	require.NoError(t, err)

	coord, err := NewCoordinatorFromYAML(defaultCoordConfig, CoordinatorOptions{})
	require.NoError(t, err)

	require.NoError(t, coord.WaitForNamespace(""))

	placementOpts := resources.PlacementRequestOptions{
		Service: resources.ServiceTypeM3Aggregator,
		Env:     "default_env",
		Zone:    "embedded",
	}
	initRequest := admin.PlacementInitRequest{
		Instances: []*placementpb.Instance{
			{
				Id:             "host1",
				IsolationGroup: "rack1",
				Zone:           "embedded",
				Weight:         1,
				Endpoint:       "http://host1:1234",
				Hostname:       "host1",
				Port:           1234,
				Metadata: &placementpb.InstanceMetadata{
					DebugPort: 0,
				},
			},
			{
				Id:             "host2",
				IsolationGroup: "rack2",
				Zone:           "embedded",
				Weight:         1,
				Endpoint:       "http://host2:1234",
				Hostname:       "host2",
				Port:           1234,
				Metadata: &placementpb.InstanceMetadata{
					DebugPort: 0,
				},
			},
		},
		NumShards:         1,
		ReplicationFactor: 2,
	}
	instanceMap := make(map[string]*placementpb.Instance, len(initRequest.Instances))
	for _, ins := range initRequest.Instances {
		newIns := *ins
		newIns.ShardSetId = 1 // initialized
		newIns.Shards = []*placementpb.Shard{
			{
				Id:                0,
				State:             placementpb.ShardState_INITIALIZING,
				SourceId:          "",
				CutoverNanos:      0,
				CutoffNanos:       0,
				RedirectToShardId: nil,
			},
		}
		instanceMap[ins.Hostname] = &newIns
	}
	initResp, err := coord.InitPlacement(placementOpts, initRequest)
	require.NoError(t, err)
	expectedPlacement := &placementpb.Placement{
		Instances:     instanceMap,
		ReplicaFactor: uint32(initRequest.ReplicationFactor),
		NumShards:     uint32(initRequest.NumShards),
		MaxShardSetId: uint32(initRequest.NumShards),
		IsSharded:     true,
		IsMirrored:    true,
	}
	require.Equal(t, int32(0), initResp.Version)
	validateEqualAggPlacement(t, expectedPlacement, initResp.Placement)

	getResp, err := coord.GetPlacement(placementOpts)

	require.NoError(t, err)
	require.Equal(t, int32(1), getResp.Version)
	validateEqualAggPlacement(t, expectedPlacement, getResp.Placement)

	wrongPlacementOpts := resources.PlacementRequestOptions{
		Service: resources.ServiceTypeM3Aggregator,
		Env:     "default_env_wrong",
		Zone:    "embedded",
	}
	_, err = coord.GetPlacement(wrongPlacementOpts)
	require.NotNil(t, err)

	assert.NoError(t, coord.Close())
	assert.NoError(t, dbnode.Close())
}

func validateEqualAggPlacement(t *testing.T, expected, actual *placementpb.Placement) {
	p1, err := placement.NewPlacementFromProto(expected)
	require.NoError(t, err)
	p2, err := placement.NewPlacementFromProto(actual)
	require.NoError(t, err)
	require.Equal(t, p1.String(), p2.String())
}

const defaultCoordConfig = `
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
          etcdClusters:
            - zone: embedded
              endpoints:
                - 127.0.0.1:2379
`

const embeddedCoordConfig = `
coordinator:
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
            etcdClusters:
              - zone: embedded
                endpoints:
                  - 127.0.0.1:2379

db: {}
`
