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

package topic

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/services"

	"github.com/stretchr/testify/require"
)

func TestTopicAddConsumer(t *testing.T) {
	cs1 := NewConsumerService().
		SetConsumptionType(Shared).
		SetServiceID(services.NewServiceID().
			SetName("s1").
			SetEnvironment("env1").
			SetZone("zone1"),
		)
	cs2 := NewConsumerService().
		SetConsumptionType(Shared).
		SetServiceID(services.NewServiceID().
			SetName("s2").
			SetEnvironment("env2").
			SetZone("zone2"),
		)
	tpc := NewTopic().
		SetName("testName").
		SetNumberOfShards(1024).
		SetVersion(5).
		SetConsumerServices(
			[]ConsumerService{cs1},
		)

	_, err := tpc.AddConsumerService(
		NewConsumerService().
			SetConsumptionType(Shared).
			SetServiceID(services.NewServiceID().
				SetName("s1").
				SetEnvironment("env1").
				SetZone("zone1"),
			),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), cs1.ServiceID().String())

	tpc, err = tpc.AddConsumerService(cs2)
	require.NoError(t, err)
	require.Equal(t, []ConsumerService{cs1, cs2}, tpc.ConsumerServices())
}

func TestTopicRemoveConsumer(t *testing.T) {
	cs1 := NewConsumerService().
		SetConsumptionType(Shared).
		SetServiceID(services.NewServiceID().
			SetName("s1").
			SetEnvironment("env1").
			SetZone("zone1"),
		)
	cs2 := NewConsumerService().
		SetConsumptionType(Shared).
		SetServiceID(services.NewServiceID().
			SetName("s2").
			SetEnvironment("env2").
			SetZone("zone2"),
		)
	tpc := NewTopic().
		SetName("testName").
		SetNumberOfShards(1024).
		SetVersion(5).
		SetConsumerServices(
			[]ConsumerService{cs1},
		)

	_, err := tpc.RemoveConsumerService(cs2.ServiceID())
	require.Error(t, err)
	require.Contains(t, err.Error(), cs2.ServiceID().String())

	tpc, err = tpc.RemoveConsumerService(
		services.NewServiceID().
			SetName("s1").
			SetEnvironment("env1").
			SetZone("zone1"),
	)
	require.NoError(t, err)
	require.Empty(t, tpc.ConsumerServices())
}

func TestTopicUpdateConsumer(t *testing.T) {
	cs1 := NewConsumerService().
		SetConsumptionType(Shared).
		SetServiceID(services.NewServiceID().
			SetName("s1").
			SetEnvironment("env1").
			SetZone("zone1"),
		)
	tpc := NewTopic().
		SetName("testName").
		SetNumberOfShards(1024).
		SetConsumerServices(
			[]ConsumerService{cs1},
		)

	_, err := tpc.UpdateConsumerService(cs1.SetConsumptionType(Replicated))
	require.Error(t, err)
	require.Contains(t, err.Error(), "could not change consumption type")

	_, err = tpc.UpdateConsumerService(cs1.SetServiceID(services.NewServiceID().SetName("foo")))
	require.Error(t, err)
	require.Contains(t, err.Error(), "could not find consumer service")

	require.Equal(t, []ConsumerService{cs1}, tpc.ConsumerServices())
	cs2 := NewConsumerService().
		SetConsumptionType(Shared).
		SetServiceID(services.NewServiceID().
			SetName("s1").
			SetEnvironment("env1").
			SetZone("zone1"),
		).SetMessageTTLNanos(500)
	tpc, err = tpc.UpdateConsumerService(cs2)
	require.NoError(t, err)
	require.Equal(t, []ConsumerService{cs2}, tpc.ConsumerServices())
	require.Equal(t, int64(0), cs1.MessageTTLNanos())
	require.Equal(t, int64(500), cs2.MessageTTLNanos())
}

func TestTopicString(t *testing.T) {
	cs1 := NewConsumerService().
		SetConsumptionType(Shared).
		SetServiceID(services.NewServiceID().
			SetName("s1").
			SetEnvironment("env1").
			SetZone("zone1"),
		)
	cs2 := NewConsumerService().
		SetConsumptionType(Shared).
		SetServiceID(services.NewServiceID().
			SetName("s2").
			SetEnvironment("env2").
			SetZone("zone2"),
		).
		SetMessageTTLNanos(int64(time.Minute))
	tpc := NewTopic().
		SetName("testName").
		SetNumberOfShards(1024).
		SetVersion(5).
		SetConsumerServices(
			[]ConsumerService{cs1, cs2},
		)
	str := `
{
	version: 5
	name: testName
	numOfShards: 1024
	consumerServices: {
		{service: [name: s1, env: env1, zone: zone1], consumption type: shared}
		{service: [name: s2, env: env2, zone: zone2], consumption type: shared, ttl: 1m0s}
	}
}
`
	require.Equal(t, str, tpc.String())
}

func TestTopicValidation(t *testing.T) {
	topic := NewTopic()
	err := topic.Validate()
	require.Error(t, err)
	require.Equal(t, errEmptyName, err)

	topic = topic.SetName("name")
	err = topic.Validate()
	require.Error(t, err)
	require.Equal(t, errZeroShards, err)

	topic = topic.SetNumberOfShards(1024)
	err = topic.Validate()
	require.NoError(t, err)

	cs1 := NewConsumerService().
		SetConsumptionType(Shared).
		SetServiceID(services.NewServiceID().
			SetName("s1").
			SetEnvironment("env1").
			SetZone("zone1"),
		)
	topic = topic.SetConsumerServices([]ConsumerService{
		cs1, cs1,
	})
	err = topic.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicated consumer")

	topic = topic.SetConsumerServices([]ConsumerService{
		cs1, cs1.SetConsumptionType(Replicated),
	})
	err = topic.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicated consumer")

	topic = topic.SetConsumerServices([]ConsumerService{
		cs1,
	})
	err = topic.Validate()
	require.NoError(t, err)
}

func TestConsumerService(t *testing.T) {
	sid := services.NewServiceID().SetName("s").SetEnvironment("env").SetZone("zone")
	cs := NewConsumerService().SetConsumptionType(Shared).SetServiceID(sid).SetMessageTTLNanos(int64(time.Second))
	require.Equal(t, sid, cs.ServiceID())
	require.Equal(t, Shared, cs.ConsumptionType())
	require.Equal(t, int64(time.Second), cs.MessageTTLNanos())
	require.Equal(t, "{service: [name: s, env: env, zone: zone], consumption type: shared, ttl: 1s}", cs.String())
}
