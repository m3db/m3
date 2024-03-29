// Copyright (c) 2021 Uber Technologies, Inc.
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
	"testing"
	"time"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/msg/topic"

	"github.com/stretchr/testify/require"
)

func initializeTopic(
	topicName string, //nolint:unparam
	clusterClient clusterclient.Client,
	numShards int,
) (topic.Service, error) {
	serviceID := services.NewServiceID().SetName(defaultServiceName)
	cs := topic.NewConsumerService().
		SetServiceID(serviceID).
		SetConsumptionType(topic.Replicated).
		SetMessageTTLNanos(time.Minute.Nanoseconds())
	ingestTopic := topic.NewTopic().
		SetName(topicName).
		SetNumberOfShards(uint32(numShards)).
		SetConsumerServices([]topic.ConsumerService{cs})

	topicServiceOpts := topic.NewServiceOptions().
		SetConfigService(clusterClient)
	topicService, err := topic.NewService(topicServiceOpts)
	if err != nil {
		return topicService, err
	}

	_, err = topicService.CheckAndSet(ingestTopic, 0)

	return topicService, err
}

func removeAllTopicConsumers(
	topicService topic.Service,
	topicName string, //nolint:unparam
) error {
	topic, err := topicService.Get(topicName)
	if err != nil {
		return err
	}
	for len(topic.ConsumerServices()) > 0 {
		topic, err = topic.RemoveConsumerService(topic.ConsumerServices()[0].ServiceID())
		if err != nil {
			return err
		}
	}
	_, err = topicService.CheckAndSet(topic, topic.Version())
	return err
}

func setupTopic(t *testing.T, serverOpts testServerOptions, placement placement.Placement) testServerOptions {
	topicService, err := initializeTopic(defaultTopicName, serverOpts.ClusterClient(), placement.NumShards())
	require.NoError(t, err)
	return serverOpts.
		SetTopicService(topicService).
		SetTopicName(defaultTopicName)
}
